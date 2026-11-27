use std::{
    error::Error,
    fs,
    io::{self, BufRead, BufReader},
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use cooplet_client::{BearerToken, ClientConfig, ClientError, CoopletClient, Destination};
use cooplet_protocol::{ErrorCode, HubId, InfoResponse, NodeId, ProblemDetails};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use url::Url;

pub type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

pub struct Hub {
    child: HubProcess,
    state: Scratch,
    address: SocketAddr,
    pub origin: Url,
    pub id: HubId,
    pub secret: String,
}

impl Hub {
    pub async fn start() -> TestResult<Self> {
        Self::start_with_args(&[]).await
    }

    pub async fn start_with_args(server_args: &[&str]) -> TestResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let state = Scratch::new()?;
        let init = hub_command()
            .arg("init")
            .arg("--state-dir")
            .arg(&state.0)
            .output()?;
        assert!(
            init.status.success(),
            "init failed: {}",
            String::from_utf8_lossy(&init.stderr)
        );

        let secret_file = state.0.join("admin.token");
        let secret = fs::read_to_string(&secret_file)?.trim().to_owned();
        assert!(!String::from_utf8_lossy(&init.stdout).contains(&secret));
        for name in ["hub.master.key", "admin.token"] {
            assert_eq!(
                fs::metadata(state.0.join(name))?.permissions().mode() & 0o777,
                0o600
            );
        }
        let repeated = hub_command()
            .arg("init")
            .arg("--state-dir")
            .arg(&state.0)
            .output()?;
        assert!(repeated.status.success());
        assert!(fs::read_to_string(secret_file)?.trim() == secret);

        let mut child = HubProcess(
            hub_command()
                .arg("run")
                .arg("--state-dir")
                .arg(&state.0)
                .arg("--listen")
                .arg("127.0.0.1:0")
                .arg("--public-origin")
                .arg("http://127.0.0.1:0")
                .args(server_args)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?,
        );
        let stderr = child.0.stderr.take().ok_or("Hub stderr is not piped")?;
        let (ready, address) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut ready = Some(ready);
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Some(address) = line.strip_prefix("Hub listening on ")
                    && let Some(sender) = ready.take()
                {
                    let _ = sender.send(address.parse());
                } else {
                    eprintln!("{line}");
                }
            }
        });
        let address = timeout(Duration::from_secs(8), address).await???;
        let origin = Url::parse(&format!("http://{address}"))?;
        let InfoResponse::Hub(info) = CoopletClient::discover_info(&origin).await? else {
            return Err("expected Hub info".into());
        };
        Ok(Self {
            child,
            state,
            address,
            origin,
            id: info.hub_id,
            secret,
        })
    }

    pub fn client(&self, secret: Option<&str>) -> TestResult<CoopletClient> {
        let mut config = ClientConfig::new(
            self.origin.clone(),
            Destination::Hub {
                expected_hub_id: self.id.clone(),
            },
            secret.map(BearerToken::new).transpose()?,
        );
        config.request_timeout = Duration::from_secs(3);
        Ok(CoopletClient::new(config)?)
    }

    pub fn admin(&self) -> TestResult<CoopletClient> {
        self.client(Some(&self.secret))
    }

    pub fn admin_token_path(&self) -> PathBuf {
        self.state.0.join("admin.token")
    }

    pub fn issue_enrollment_cli(&self, label: &str, key: &str) -> TestResult<IssuedEnrollment> {
        let output = hub_command()
            .args(["enrollment-token", "create", "--hub", self.origin.as_str()])
            .args(["--hub-id", self.id.as_str(), "--token-file"])
            .arg(self.state.0.join("admin.token"))
            .args(["--name", label, "--idempotency-key", key])
            .output()?;
        assert!(output.status.success(), "enrollment-token create failed");
        let issued: IssuedEnrollment = serde_json::from_slice(&output.stdout)?;
        let diagnostic = std::str::from_utf8(&output.stderr)?;
        assert!(diagnostic.contains(&format!("idempotency_key={key}")));
        assert!(!diagnostic.contains(&issued.token));
        Ok(issued)
    }

    pub async fn raw(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> TestResult<RawResponse> {
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.address
        );
        for (key, value) in headers {
            request.push_str(&format!("{key}: {value}\r\n"));
        }
        request.push_str("\r\n");
        let mut socket = TcpStream::connect(self.address).await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(3), async {
            socket.write_all(request.as_bytes()).await?;
            socket.write_all(body).await?;
            socket.read_to_end(&mut response).await?;
            Ok::<_, io::Error>(())
        })
        .await??;

        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or("missing HTTP response header")?;
        let header = std::str::from_utf8(&response[..header_end])?;
        let status: u16 = header
            .split_whitespace()
            .nth(1)
            .ok_or("missing HTTP status")?
            .parse()?;
        let mut headers = http::HeaderMap::new();
        for line in header.split("\r\n").skip(1) {
            let (name, value) = line.split_once(':').ok_or("invalid HTTP response header")?;
            headers.append(name.parse::<http::HeaderName>()?, value.trim().parse()?);
        }
        let body = response
            .get(header_end + 4..)
            .ok_or("missing HTTP body")?
            .to_vec();
        Ok(RawResponse {
            status,
            body,
            headers,
        })
    }

    /// Send only the HTTP request headers and keep the socket open to verify immediate rejection.
    pub async fn raw_open(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
    ) -> TestResult<RawResponse> {
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.address
        );
        for (key, value) in headers {
            request.push_str(&format!("{key}: {value}\r\n"));
        }
        request.push_str("\r\n");
        let mut socket = TcpStream::connect(self.address).await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), async {
            socket.write_all(request.as_bytes()).await?;
            socket.read_to_end(&mut response).await?;
            Ok::<_, io::Error>(())
        })
        .await??;

        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or("missing HTTP response header")?;
        let header = std::str::from_utf8(&response[..header_end])?;
        let status: u16 = header
            .split_whitespace()
            .nth(1)
            .ok_or("missing HTTP status")?
            .parse()?;
        let mut headers = http::HeaderMap::new();
        for line in header.split("\r\n").skip(1) {
            let (name, value) = line.split_once(':').ok_or("invalid HTTP response header")?;
            headers.append(name.parse::<http::HeaderName>()?, value.trim().parse()?);
        }
        let body = response
            .get(header_end + 4..)
            .ok_or("missing HTTP body")?
            .to_vec();
        Ok(RawResponse {
            status,
            body,
            headers,
        })
    }

    pub async fn stop_gracefully(&mut self) -> TestResult {
        let status = Command::new("sh")
            .args([
                "-c",
                r#"kill -TERM "$1""#,
                "hub-test",
                &self.child.0.id().to_string(),
            ])
            .status()?;
        assert!(status.success());
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            if let Some(status) = self.child.0.try_wait()? {
                assert!(status.success(), "unexpected Hub shutdown status {status}");
                break;
            }
            if Instant::now() >= deadline {
                return Err("Hub did not drain after SIGTERM".into());
            }
            sleep(Duration::from_millis(15)).await;
        }

        let reopened = hub_command()
            .arg("init")
            .arg("--state-dir")
            .arg(&self.state.0)
            .output()?;
        assert!(
            reopened.status.success(),
            "Hub did not release the database sidecar"
        );
        Ok(())
    }
}

struct HubProcess(Child);

impl Drop for HubProcess {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|result| result.is_none()) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

pub fn hub_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_cooplet-hub"))
}

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> TestResult<Self> {
        let root = std::env::temp_dir();
        let path = root.join(format!("cooplet-hub-test-{id}", id = HubId::generate()?));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub struct RawResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: http::HeaderMap,
}

impl RawResponse {
    pub fn problem(&self, status: u16, code: ErrorCode) -> TestResult<ProblemDetails> {
        assert_eq!(self.status, status);
        let problem: ProblemDetails = serde_json::from_slice(&self.body)?;
        assert_eq!(problem.code, code);
        assert_eq!(problem.status, self.status);
        Ok(problem)
    }
}

pub fn expect_problem<T>(response: Result<T, ClientError>, code: ErrorCode) -> TestResult {
    let problem = match response {
        Err(ClientError::Problem(problem)) => problem,
        Err(error) => return Err(format!("expected {code}: {error}").into()),
        Ok(_) => return Err(format!("expected {code}, received success").into()),
    };
    assert_eq!(problem.code, code);
    assert_eq!(problem.status, code.status());
    Ok(())
}

#[derive(Deserialize)]
pub struct IssuedEnrollment {
    pub token_id: String,
    pub node_id: NodeId,
    pub token: String,
}

#[derive(Deserialize)]
pub struct Enrolled {
    pub node_id: NodeId,
    pub generation: u64,
    pub node_token: String,
}

pub async fn issue_enrollment(
    admin: &CoopletClient,
    label: &str,
    key: &str,
) -> TestResult<IssuedEnrollment> {
    Ok(admin
        .post_json("/v1/enrollment-tokens", &json!({"name": label}), Some(key))
        .await?)
}

pub async fn redeem(
    unauthenticated: &CoopletClient,
    enrollment: &IssuedEnrollment,
) -> Result<Enrolled, ClientError> {
    unauthenticated
        .post_json(
            "/v1/enrollment/token",
            &json!({"token": enrollment.token}),
            None,
        )
        .await
}

pub fn string(object: &Value, key: &str) -> TestResult<String> {
    Ok(object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("response missing {key}"))?
        .to_owned())
}
