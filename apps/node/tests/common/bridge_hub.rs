use std::{
    fs,
    io::{self, BufRead, BufReader},
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use cooplet_client::{BearerToken, ClientConfig, CoopletClient, Destination};
use cooplet_protocol::{HubId, InfoResponse, NodeId};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader, copy_bidirectional},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
    time::timeout,
};

use crate::test_node::TestResult;

const WAIT: Duration = Duration::from_secs(15);

pub struct BridgeHub {
    process: Process,
    pub root: Scratch,
    pub id: HubId,
    pub origin: String,
    pub admin: CoopletClient,
}

#[derive(Deserialize)]
pub struct Enrollment {
    pub node_id: NodeId,
    pub token: String,
}

impl BridgeHub {
    pub async fn start() -> TestResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let program = tokio::task::spawn_blocking(hub_binary).await??;
        let root = Scratch::new()?;
        let init = Command::new(program)
            .arg("init")
            .arg("--state-dir")
            .arg(&root.0)
            .output()?;
        assert!(
            init.status.success(),
            "Hub init failed: {}",
            String::from_utf8_lossy(&init.stderr)
        );
        let mut process = Process(
            Command::new(program)
                .arg("run")
                .arg("--state-dir")
                .arg(&root.0)
                .args([
                    "--listen",
                    "127.0.0.1:0",
                    "--public-origin",
                    "http://127.0.0.1:0",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?,
        );
        let stderr = process.0.stderr.take().ok_or("Hub stderr was not piped")?;
        let (ready, listening) = oneshot::channel();
        std::thread::spawn(move || {
            let mut ready = Some(ready);
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Some(address) = line.strip_prefix("Hub listening on ")
                    && let Some(ready) = ready.take()
                {
                    let _ = ready.send(address.parse::<SocketAddr>());
                } else {
                    eprintln!("{line}");
                }
            }
        });
        let address = timeout(WAIT, listening).await???;
        let origin = format!("http://{address}");
        let url = origin.parse()?;
        let InfoResponse::Hub(info) = CoopletClient::discover_info(&url).await? else {
            return Err("the subprocess did not identify itself as a Hub".into());
        };
        let token = fs::read_to_string(root.0.join("admin.token"))?;
        let admin = CoopletClient::new(ClientConfig::new(
            url,
            Destination::Hub {
                expected_hub_id: info.hub_id.clone(),
            },
            Some(BearerToken::new(token.trim())?),
        ))?;
        Ok(Self {
            process,
            root,
            id: info.hub_id,
            origin,
            admin,
        })
    }

    pub async fn issue(&self, name: &str) -> TestResult<Enrollment> {
        let key = format!("enroll-{name}");
        Ok(self
            .admin
            .post_json("/v1/enrollment-tokens", &json!({"name":name}), Some(&key))
            .await?)
    }

    pub fn client(&self, token: &str) -> TestResult<CoopletClient> {
        Ok(CoopletClient::new(ClientConfig::new(
            self.origin.parse()?,
            Destination::Hub {
                expected_hub_id: self.id.clone(),
            },
            Some(BearerToken::new(token)?),
        ))?)
    }

    pub fn binding(&self, enrollment: &Enrollment) -> Value {
        json!({"hub_url":self.origin,"hub_id":self.id,"token":enrollment.token})
    }

    /// Count blob requests at the real Hub's HTTP boundary, including cache misses.
    pub async fn observe_blob_requests(&mut self) -> TestResult<BlobRequests> {
        let upstream: SocketAddr = self.origin.trim_start_matches("http://").parse()?;
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        self.origin = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((peer, _)) = accepted else { break };
                        connections.spawn(observe_connection(peer, upstream, observed.clone()));
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(BlobRequests {
            requests,
            _tasks: tasks,
        })
    }

    pub async fn stop(&mut self) -> TestResult {
        let status = Command::new("sh")
            .args([
                "-c",
                r#"kill -TERM "$1""#,
                "bridge-hub",
                &self.process.0.id().to_string(),
            ])
            .status()?;
        assert!(status.success());
        timeout(WAIT, async {
            loop {
                if let Some(status) = self.process.0.try_wait()? {
                    assert!(status.success(), "Hub did not drain cleanly: {status}");
                    return Ok::<_, io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        Ok(())
    }
}

pub struct BlobRequests {
    requests: Arc<Mutex<Vec<String>>>,
    _tasks: JoinSet<()>,
}

impl BlobRequests {
    pub fn snapshot(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

async fn observe_connection(
    peer: TcpStream,
    upstream: SocketAddr,
    observed: Arc<Mutex<Vec<String>>>,
) -> io::Result<()> {
    peer.set_nodelay(true)?;
    let mut peer = AsyncBufReader::new(peer);
    let mut line = String::new();
    if peer.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let mut fields = line.split_ascii_whitespace();
    let method = fields.next().unwrap_or_default();
    let path = fields.next().unwrap_or_default();
    if path == "/v1/blobs" || path.starts_with("/v1/blobs/") {
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!("{method} {path}"));
    }
    let websocket = path == "/v1/bridge/connect";
    let mut hub = TcpStream::connect(upstream).await?;
    hub.set_nodelay(true)?;
    hub.write_all(line.as_bytes()).await?;
    loop {
        line.clear();
        if peer.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if line == "\r\n" {
            break;
        }
        if websocket
            || !line
                .split_once(':')
                .is_some_and(|(name, _)| name.eq_ignore_ascii_case("connection"))
        {
            hub.write_all(line.as_bytes()).await?;
        }
    }
    // Keep each HTTP request observable without parsing streamed bodies. The
    // upgraded connection continues to carry every frame of the same bridge.
    if !websocket {
        hub.write_all(b"Connection: close\r\n").await?;
    }
    hub.write_all(b"\r\n").await?;
    copy_bidirectional(&mut peer, &mut hub).await?;
    Ok(())
}

pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "cooplet-bridge-{}",
            HubId::generate().map_err(io::Error::other)?
        ));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub struct Process(pub Child);

impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

pub fn write_private(path: &Path, body: &[u8]) -> io::Result<()> {
    use std::io::Write;
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?
        .write_all(body)
}

fn hub_binary() -> io::Result<&'static Path> {
    static BINARY: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    match BINARY.get_or_init(|| build_hub_binary().map_err(|error| error.to_string())) {
        Ok(path) => Ok(path),
        Err(error) => Err(io::Error::other(error.clone())),
    }
}

fn build_hub_binary() -> io::Result<PathBuf> {
    let path = if let Some(override_path) = std::env::var_os("COOPLET_HUB_BIN") {
        PathBuf::from(override_path)
    } else {
        let executable = std::env::current_exe()?;
        let profile_dir = executable.parent().and_then(Path::parent).ok_or_else(|| {
            io::Error::other("the bridge test executable is missing its Cargo profile directory")
        })?;
        let target_dir = profile_dir.parent().ok_or_else(|| {
            io::Error::other("the bridge test executable is missing its Cargo target directory")
        })?;
        let mut cargo = Command::new(env!("CARGO"));
        cargo.args([
            "build",
            "-p",
            "cooplet-hub",
            "--bin",
            "cooplet-hub",
            "--offline",
            "--locked",
        ]);
        if profile_dir.ends_with("release") {
            cargo.arg("--release");
        }
        let status = cargo.env("CARGO_TARGET_DIR", target_dir).status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "nested Cargo build of cooplet-hub failed: {status}"
            )));
        }
        profile_dir.join("cooplet-hub")
    };
    fs::canonicalize(path)
}
