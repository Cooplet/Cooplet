mod transport;

use std::{
    error::Error,
    fs,
    io::{self, Write},
    net::SocketAddr,
    path::PathBuf,
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
use cooplet_client::{BearerToken, ClientConfig, CoopletClient, Destination};
use cooplet_platform::MonotonicClock;
use cooplet_protocol::{HubId, RequestId, validate_idempotency_key};
use serde::Serialize;
use tokio::{
    net::TcpListener,
    signal::unix::{Signal, SignalKind, signal},
    sync::{oneshot, watch},
    time,
};
use url::{Host, Url};

use crate::{
    AppState,
    admin::{IssueEnrollmentRequest, IssuedEnrollmentResponse},
    bridge::LiveBridge,
    credentials::{EnrollmentCredential, enrollment_node_id},
    http::{DEFAULT_BRIDGE_CONCURRENCY, RequestLimits, router},
    openapi::openapi_bytes,
    storage::{self, HubStorage},
};
use transport::{HttpServerConfig, serve_http1};

const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const FORCE_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const BACKGROUND_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const BRIDGE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ADMIN_TOKEN_BYTES: u64 = 1024;
const MAX_ENROLLMENT_TTL_MS: i64 = 86_400_000;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    name = "cooplet-hub",
    version,
    about = "Run or administer a Cooplet Hub"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a private Hub state directory and its first admin credential.
    Init {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long, default_value = "Cooplet Hub")]
        display_name: String,
    },
    /// Serve the Hub in the foreground.
    Run(RunArgs),
    /// Manage node enrollment credentials.
    EnrollmentToken {
        #[command(subcommand)]
        command: EnrollmentCommand,
    },
    /// Print the Hub's OpenAPI contract.
    Openapi,
}

#[derive(Args)]
struct RunArgs {
    #[arg(long)]
    state_dir: PathBuf,
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    public_origin: Url,
    #[arg(long, default_value_t = cooplet_storage::DEFAULT_MAX_BLOB_BYTES)]
    max_blob_bytes: u64,
    #[arg(long, default_value_t = 10 * 1024 * 1024 * 1024)]
    max_total_blob_bytes: u64,
    #[arg(long, default_value_t = 1_048_576)]
    max_page_count: u32,
    #[arg(long, default_value_t = 256)]
    max_concurrent_requests: usize,
    #[arg(long, default_value_t = 64)]
    max_active_event_streams: usize,
    #[arg(long, default_value_t = 32)]
    max_concurrent_blob_transfers: usize,
    #[arg(long, default_value_t = DEFAULT_BRIDGE_CONCURRENCY)]
    handshake_permits: usize,
    #[arg(long, default_value_t = DEFAULT_BRIDGE_CONCURRENCY)]
    max_active_bridges: usize,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    agents_can_create_threads: bool,
}

#[derive(Subcommand)]
enum EnrollmentCommand {
    /// Issue a one-shot enrollment credential using an existing admin bearer.
    Create(EnrollmentArgs),
}

#[derive(Args)]
struct EnrollmentArgs {
    #[arg(long)]
    hub: Url,
    #[arg(long)]
    hub_id: HubId,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long)]
    name: String,
    /// Lifetime, using ms, s, m or h (up to 24h).
    #[arg(long, default_value = "15m", value_parser = enrollment_ttl)]
    ttl: i64,
    #[arg(long)]
    idempotency_key: Option<String>,
    #[arg(long)]
    ca_cert: Option<PathBuf>,
}

#[tokio::main]
pub(crate) async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let status = if error.use_stderr() {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            };
            return if error.print().is_ok() {
                status
            } else {
                ExitCode::FAILURE
            };
        }
    };
    match execute(cli.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cooplet-hub: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(command: Command) -> CliResult<()> {
    match command {
        Command::Init {
            state_dir,
            display_name,
        } => {
            let initialized = storage::init(&state_dir, &display_name).await?;
            let token_path = initialized
                .token_path
                .as_deref()
                .map(|path| path.display().to_string());
            print_json(&InitOutput {
                status: if token_path.is_some() {
                    "initialized"
                } else {
                    "already_initialized"
                },
                hub_id: initialized.hub_id,
                token_path,
            })
        }
        Command::Run(args) => run(args).await,
        Command::EnrollmentToken {
            command: EnrollmentCommand::Create(args),
        } => issue_enrollment(args).await,
        Command::Openapi => {
            io::stdout().lock().write_all(openapi_bytes()?)?;
            Ok(())
        }
    }
}

#[derive(Serialize)]
struct InitOutput {
    status: &'static str,
    hub_id: HubId,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_path: Option<String>,
}

async fn issue_enrollment(args: EnrollmentArgs) -> CliResult<()> {
    let key = match args.idempotency_key {
        Some(key) => {
            validate_idempotency_key(&key)?;
            key
        }
        None => RequestId::generate()?.to_string(),
    };
    let bytes =
        cooplet_platform::PrivateFs::read_private_file(&args.token_file, MAX_ADMIN_TOKEN_BYTES)?;
    let token = BearerToken::new(std::str::from_utf8(&bytes)?.trim())?;
    let mut config = ClientConfig::new(
        args.hub,
        Destination::Hub {
            expected_hub_id: args.hub_id,
        },
        Some(token),
    );
    if let Some(path) = args.ca_cert {
        config.root_certificates_pem.push(fs::read(path)?);
    }
    let client = CoopletClient::new(config)?;
    writeln!(io::stderr().lock(), "idempotency_key={key}")?;
    let response: IssuedEnrollmentResponse = client
        .post_json(
            "/v1/enrollment-tokens",
            &IssueEnrollmentRequest {
                name: args.name,
                ttl_ms: Some(args.ttl),
            },
            Some(&key),
        )
        .await?;
    if enrollment_node_id(&response.summary.token_id)? != response.summary.node_id
        || match &response.token {
            Some(token) => {
                response.already_issued
                    || EnrollmentCredential::parse(token)
                        .is_none_or(|credential| credential.token_id != response.summary.token_id)
            }
            None => !response.already_issued,
        }
    {
        return Err("invalid enrollment receipt from Hub".into());
    }
    print_json(&response)
}

fn print_json(value: &impl Serialize) -> CliResult<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn enrollment_ttl(value: &str) -> Result<i64, &'static str> {
    let (digits, multiplier) = if let Some(digits) = value.strip_suffix("ms") {
        (digits, 1)
    } else if let Some(digits) = value.strip_suffix('s') {
        (digits, 1_000)
    } else if let Some(digits) = value.strip_suffix('m') {
        (digits, 60_000)
    } else if let Some(digits) = value.strip_suffix('h') {
        (digits, 3_600_000)
    } else {
        (value, 1)
    };
    if digits.bytes().all(|byte| byte.is_ascii_digit())
        && let Ok(quantity) = digits.parse::<i64>()
        && let Some(milliseconds) = quantity.checked_mul(multiplier)
        && (1..=MAX_ENROLLMENT_TTL_MS).contains(&milliseconds)
    {
        Ok(milliseconds)
    } else {
        Err("enrollment TTL must be between 1ms and 24h")
    }
}

fn validate_public_origin(listen: SocketAddr, origin: &Url) -> io::Result<()> {
    if origin.host().is_none()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
        || !origin.username().is_empty()
        || origin.password().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "public origin must be a bare HTTP or HTTPS origin",
        ));
    }
    let loopback = match origin.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if origin.scheme() == "https"
        || (origin.scheme() == "http" && listen.ip().is_loopback() && loopback)
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "public origin requires HTTPS unless both listener and origin use loopback addresses",
        ))
    }
}

struct ShutdownSignals {
    interrupt: Signal,
    terminate: Signal,
}

impl ShutdownSignals {
    fn register() -> io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
    }
}

async fn run(args: RunArgs) -> CliResult<()> {
    validate_public_origin(args.listen, &args.public_origin)?;
    let limits = RequestLimits::new(
        args.max_concurrent_requests,
        args.max_active_event_streams,
        args.max_concurrent_blob_transfers,
    )?
    .with_bridges(args.handshake_permits, args.max_active_bridges)?;
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .try_init()?;
    let signals = ShutdownSignals::register()?;
    let HubStorage {
        engine,
        meta,
        master,
    } = storage::open(
        &args.state_dir,
        args.max_blob_bytes,
        args.max_total_blob_bytes,
        args.max_page_count,
    )
    .await?;
    let served = async {
        let (shutdown, shutdown_rx) = watch::channel(());
        let mut state = AppState::new(
            &engine,
            meta,
            master,
            Arc::new(LiveBridge::new(
                limits.active_bridges(),
                Arc::new(MonotonicClock),
            )),
            shutdown_rx,
            args.max_blob_bytes,
        )?;
        state.agents_can_create_threads = args.agents_can_create_threads;
        let socket = TcpListener::bind(args.listen).await?;
        writeln!(
            io::stderr().lock(),
            "Hub listening on {}",
            socket.local_addr()?
        )?;
        serve(socket, state, limits, shutdown, signals).await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    }
    .await;
    let stopped = engine.shutdown().await;
    if let Err(error) = &stopped {
        tracing::error!(%error, "Hub storage shutdown failed");
    }
    served?;
    stopped?;
    Ok(())
}

async fn serve(
    socket: TcpListener,
    state: AppState,
    limits: RequestLimits,
    shutdown: watch::Sender<()>,
    signals: ShutdownSignals,
) -> io::Result<()> {
    let background = state.background.clone();
    let bridge = state.bridge.clone();
    let config = HttpServerConfig {
        header_read_timeout: HEADER_READ_TIMEOUT,
        write_idle_timeout: WRITE_IDLE_TIMEOUT,
        drain_timeout: DRAIN_TIMEOUT,
        force_close_timeout: FORCE_CLOSE_TIMEOUT,
        max_connections: limits.max_connections(),
    };
    let (stop_accepting, stopped) = oneshot::channel();
    let app = router(state, limits);
    let server = serve_http1(socket, app, config, stopped);
    tokio::pin!(server);
    let served = tokio::select! {
        biased;
        () = signals.wait() => {
            background.close();
            shutdown.send_replace(());
            bridge.shutdown();
            let _ = stop_accepting.send(());
            server.await
        }
        result = &mut server => {
            background.close();
            shutdown.send_replace(());
            bridge.shutdown();
            result
        }
    };
    if time::timeout(BRIDGE_DRAIN_TIMEOUT, bridge.drain())
        .await
        .is_err()
    {
        tracing::warn!("Bridge drain timed out during Hub shutdown");
    }
    if time::timeout(BACKGROUND_DRAIN_TIMEOUT, background.drain())
        .await
        .is_err()
    {
        tracing::warn!("Background write drain timed out during Hub shutdown");
    }
    served
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_public_origin_against_listener() -> CliResult<()> {
        let loopback = "127.0.0.1:8200".parse()?;
        let external = "0.0.0.0:8200".parse()?;
        for (listen, origin, valid) in [
            (loopback, "http://127.0.0.1:8200", true),
            (loopback, "http://[::1]:8200", true),
            (loopback, "http://localhost:8200", false),
            (loopback, "http://192.0.2.2", false),
            (external, "http://127.0.0.1:8200", false),
            (external, "https://hub.example", true),
            (external, "https://hub.example/api", false),
            (external, "https://hub.example/?q=1", false),
            (external, "https://hub.example/#fragment", false),
            (external, "https://private@hub.example", false),
            (external, "ftp://hub.example", false),
        ] {
            assert_eq!(
                validate_public_origin(listen, &Url::parse(origin)?).is_ok(),
                valid
            );
        }
        Ok(())
    }

    #[test]
    fn parses_enrollment_ttl_with_bounds_and_overflow_checks() {
        assert_eq!(enrollment_ttl("15m"), Ok(900_000));
        assert_eq!(enrollment_ttl("24h"), Ok(86_400_000));
        assert_eq!(enrollment_ttl("2s"), Ok(2_000));
        assert_eq!(enrollment_ttl("100ms"), Ok(100));
        assert_eq!(enrollment_ttl("100"), Ok(100));
        for invalid in [
            "",
            "0",
            "25h",
            "24h1ms",
            "+1s",
            "-1s",
            "18446744073709551615h",
        ] {
            assert!(enrollment_ttl(invalid).is_err(), "accepted {invalid}");
        }
    }
}
