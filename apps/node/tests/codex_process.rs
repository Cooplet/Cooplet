use std::{
    env,
    error::Error,
    fs, io,
    io::{BufRead, BufReader, Write},
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use cooplet_client::{ClientError, CoopletClient};
use cooplet_node::{auth::Credential, config::operator_path};
use cooplet_protocol::{ParticipantId, PrincipalKind, RequestId};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::{UnixListener, UnixStream},
    time::{sleep, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

#[path = "common/codex_backend.rs"]
mod codex_backend;

#[path = "common/doctor.rs"]
mod doctor;
use doctor::adapter_report;

type Test<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const NODE: &str = env!("CARGO_BIN_EXE_cooplet");
const WAIT: Duration = Duration::from_secs(15);
const THREAD: &str = "fixture-thread";

struct Fixture {
    root: PathBuf,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Test<Self> {
        let root = env::temp_dir().join(format!("cx-{}", &RequestId::generate()?.as_str()[4..16]));
        fs::create_dir(&root)?;
        for dir in ["work", "home"] {
            fs::create_dir(root.join(dir))?;
        }
        fs::write(root.join("codex"), include_bytes!("fixtures/codex-cli.sh"))?;
        fs::set_permissions(root.join("codex"), fs::Permissions::from_mode(0o755))?;
        Ok(Self {
            state: root.join("s"),
            root,
        })
    }

    fn command(&self, mode: &str) -> Test<Command> {
        let mut command = Command::new(node_program()?);
        command
            .arg("--state-dir")
            .arg(&self.state)
            .env("COOPLET_CODEX_BIN", self.root.join("codex"))
            .env("CODEX_HOME", self.root.join("home"))
            .env("COOPLET_FIXTURE_SELF", env::current_exe()?)
            .env("COOPLET_FIXTURE_MODE", mode)
            .env("COOPLET_FIXTURE_ROOT", &self.root)
            .env("COOPLET_FIXTURE_FOREIGN", self.root.join("f.sock"));
        Ok(command)
    }

    async fn start(&self, mode: &str) -> Test<(Daemon, CoopletClient)> {
        self.start_with(self.command(mode)?).await
    }

    async fn start_with(&self, mut command: Command) -> Test<(Daemon, CoopletClient)> {
        let mut daemon = Daemon(
            command
                .args(["node", "run", "--workspace-dir"])
                .arg(self.root.join("work"))
                .stdout(Stdio::null())
                .stderr(fs::File::create(self.root.join("daemon.log"))?)
                .spawn()?,
        );
        let client = timeout(WAIT, async {
            loop {
                if daemon.0.try_wait()?.is_some() {
                    return Err(io::Error::other(fs::read_to_string(
                        self.root.join("daemon.log"),
                    )?));
                }
                if let Ok(credential) = Credential::read(&operator_path(&self.state))
                    && let Ok((client, _)) = credential.connect(PrincipalKind::Operator).await
                    && client.health().await.is_ok()
                {
                    return Ok(client);
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        Ok((daemon, client))
    }

    fn doctor(&self, mode: &str) -> Test<Value> {
        let output = self
            .command(mode)?
            .env("PATH", &self.root)
            .args(["node", "doctor"])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn records(&self) -> Test<Vec<Value>> {
        fs::read_to_string(self.root.join("record.jsonl"))?
            .lines()
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }
}

fn node_program() -> Test<PathBuf> {
    let built = PathBuf::from(NODE);
    if built.is_file() {
        return Ok(built);
    }
    // The Devkit test binary can also be run on the host for the native smoke.
    let test = env::current_exe()?;
    let debug = test
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::other("cannot locate the built Cooplet binary"))?;
    Ok(debug.join("cooplet"))
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = Command::new("kill")
                .args(["-TERM", &self.0.id().to_string()])
                .status();
            for _ in 0..50 {
                if !matches!(self.0.try_wait(), Ok(None)) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn create(
    client: &CoopletClient,
    key: &str,
    template: Option<&str>,
) -> Result<Value, ClientError> {
    let mut request = json!({"client_kind":"codex","name":"coder"});
    if let Some(template) = template {
        request["template_id"] = json!(template);
    }
    client.post_json("/v1/agents", &request, Some(key)).await
}

async fn participant_state(client: &CoopletClient, participant: &str, expected: &str) -> Test {
    timeout(WAIT, async {
        let path = format!("/v1/participants/{participant}");
        loop {
            let row: Value = client.get_json(&path, &[]).await?;
            if row["state"] == expected {
                return Ok::<_, ClientError>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    Ok(())
}

async fn stop_agent(client: &CoopletClient, id: &str, key: &str) -> Test {
    let _: Value = client
        .post_json(&format!("/v1/agents/{id}/stop"), &json!({}), Some(key))
        .await?;
    participant_state(client, id, "stopped").await
}

#[tokio::test]
async fn cli_runs_codex_with_mcp_and_resumes_the_same_thread_in_a_new_group() -> Test {
    let fixture = Fixture::new()?;
    // A multicall executable dispatches on its invocation name.
    fs::rename(fixture.root.join("codex"), fixture.root.join("dispatcher"))?;
    std::os::unix::fs::symlink("dispatcher", fixture.root.join("codex"))?;
    let (daemon, client) = fixture.start("ok").await?;
    let doctor = fixture.doctor("ok")?;
    let codex = adapter_report(&doctor, "codex")?;
    assert_eq!(codex["available"], true);
    assert_eq!(codex["version"], "codex-fixture/99.0.0");

    let template = fixture.root.join("work/.cooplet/agents/demo");
    fs::create_dir_all(&template)?;
    fs::write(
        template.join("agent.json"),
        r#"{"schema":1,"client":"codex","model":"fixture-model","effort":"low","approval":"on-request","sandbox":"read-only"}"#,
    )?;
    fs::write(
        template.join("instructions.md"),
        "Frozen fixture instructions",
    )?;
    let opened = create(&client, "create-codex", Some("demo")).await?;
    let id = opened["participant_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing participant id"))?;
    participant_state(&client, id, "idle").await?;

    let path = format!("/v1/participants/{id}/attach-plan");
    let before: Value = client.get_json(&path, &[]).await?;
    assert_eq!(before["mode"], "unsupported");

    let receipt: Value = client
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"Hello from the operator"}),
            Some("send-codex"),
        )
        .await?;
    assert_eq!(receipt["delivery_state"], "delivered");
    participant_state(&client, id, "idle").await?;
    let attach: Value = client.get_json(&path, &[]).await?;
    assert_eq!(attach["mode"], "exec");
    assert_eq!(attach["program"], json!(fixture.root.join("codex")));
    assert_eq!(attach["args"][0], "--remote");
    assert_eq!(attach["args"][2], "resume");
    assert_eq!(attach["args"][3], THREAD);
    assert_eq!(
        attach["env"]["CODEX_HOME"],
        fixture.root.join("home").display().to_string()
    );
    let first = fixture.records()?;
    let first_open = &first[0];
    assert!(
        first_open["params"]["developerInstructions"]
            .as_str()
            .is_some_and(|s| {
                s.starts_with("Cooplet collaboration rules")
                    && s.ends_with("Frozen fixture instructions")
            })
    );
    assert_eq!(first_open["params"]["model"], "fixture-model");
    assert_eq!(
        first_open["params"]["config"]["model_reasoning_effort"],
        "low"
    );
    assert_eq!(first_open["params"]["approvalPolicy"], "on-request");
    assert_eq!(first_open["params"]["sandbox"], "read-only");
    assert!(first[1]["whoami"].to_string().contains(id));
    assert!(
        first[2]["params"]["input"][0]["text"]
            .as_str()
            .is_some_and(
                |s| s.contains("author_kind=human") && s.contains("Hello from the operator")
            )
    );

    let socket = Path::new(
        attach["args"][1]
            .as_str()
            .and_then(|s| s.strip_prefix("unix://"))
            .ok_or_else(|| io::Error::other("missing Unix socket"))?,
    );
    let first_runtime = socket
        .parent()
        .ok_or_else(|| io::Error::other("missing runtime directory"))?;
    assert_eq!(
        fs::metadata(first_runtime)?.permissions().mode() & 0o777,
        0o700
    );
    stop_agent(&client, id, "stop-codex").await?;
    assert!(!first_runtime.exists());
    let first_cred = first_open["mcpArgs"][3]
        .as_str()
        .ok_or_else(|| io::Error::other("missing credential path"))?;
    assert!(!Path::new(first_cred).exists());

    fs::write(
        template.join("instructions.md"),
        "Replacement must not be injected",
    )?;
    drop(daemon);
    let mut restarted = fixture.command("ok")?;
    restarted.env("CODEX_HOME", fixture.root.join("missing-new-home"));
    let (_daemon, client) = fixture.start_with(restarted).await?;
    let _: Value = client
        .post_json(
            &format!("/v1/agents/{id}/resume"),
            &json!({}),
            Some("resume-codex"),
        )
        .await?;
    let second = fixture.records()?;
    let resumed = &second[3];
    assert_eq!(resumed["method"], "thread/resume");
    assert_eq!(resumed["params"]["threadId"], THREAD);
    assert_eq!(
        resumed["params"]["developerInstructions"],
        first_open["params"]["developerInstructions"]
    );
    assert_ne!(resumed["mcpArgs"][3], first_open["mcpArgs"][3]);
    assert_ne!(resumed["socket"], first_open["socket"]);
    assert_ne!(resumed["pid"], first_open["pid"]);
    stop_agent(&client, id, "stop-resumed-codex").await
}

#[tokio::test]
async fn invalid_codex_peer_home_mcp_and_paths_fail_closed() -> Test {
    for (mode, expected) in [
        ("foreign-peer", "codex_peer_identity_mismatch"),
        ("wrong-home", "codex_home_mismatch"),
        ("bad-mcp", "codex_required_mcp_unavailable"),
        ("mcp-not-ready", "codex_required_mcp_unavailable"),
        ("long", "socket_path_too_long"),
        ("non-utf8", "native_path_not_utf8"),
    ] {
        let mut fixture = Fixture::new()?;
        let _foreign = if mode == "foreign-peer" {
            Some(UnixListener::bind(fixture.root.join("f.sock"))?)
        } else {
            None
        };
        if mode == "long" {
            fixture.state = fixture.root.join("s".repeat(100));
        } else if mode == "non-utf8" {
            fixture.state = fixture.root.join(std::ffi::OsStr::from_bytes(b"s-\xff"));
        }
        let (_daemon, client) = fixture.start(mode).await?;
        let error = create(&client, "rejected-codex", None)
            .await
            .err()
            .ok_or_else(|| io::Error::other("unexpected create acknowledgement"))?;
        assert!(matches!(&error, ClientError::Problem(problem)
            if problem.extensions.get("reason") == Some(&json!("native_open_failed"))));
        let message = error.to_string();
        assert!(!message.contains("private-fixture-marker"), "{message}");
        let log = fs::read_to_string(fixture.root.join("daemon.log"))?;
        assert!(log.contains(expected), "{mode}: {log}");
        assert!(!log.contains("private-fixture-marker"), "{log}");
        assert!(
            fs::read_dir(fixture.state.join("runtime"))?
                .next()
                .is_none(),
            "{mode}"
        );
        if matches!(mode, "long" | "non-utf8") {
            assert!(!fixture.root.join("managed-spawned").exists());
        } else {
            assert!(fixture.root.join("managed-spawned").exists());
        }
    }
    Ok(())
}

#[tokio::test]
async fn doctor_rejects_missing_flags_and_bounds_a_hung_probe() -> Test {
    let fixture = Fixture::new()?;
    let (_daemon, _client) = fixture.start("ok").await?;
    let no_schema = fixture.doctor("no-schema")?;
    let codex = adapter_report(&no_schema, "codex")?;
    assert_eq!(codex["available"], true);
    assert_eq!(codex["capabilities"]["supports_structured_output"], false);
    for (mode, detail) in [
        ("no-remote", "--remote"),
        ("probe-timeout", "stdio initialize"),
    ] {
        let start = std::time::Instant::now();
        let report = fixture.doctor(mode)?;
        let codex = adapter_report(&report, "codex")?;
        assert_eq!(codex["available"], false);
        assert!(
            codex["detail"].as_str().is_some_and(|s| s.contains(detail)),
            "{report}"
        );
        assert!(start.elapsed() < Duration::from_secs(8));
    }
    Ok(())
}

#[tokio::test]
async fn doctor_skips_an_inaccessible_path_candidate_and_open_rechecks_the_executable() -> Test {
    let fixture = Fixture::new()?;
    let (_daemon, client) = fixture.start("ok").await?;
    let denied = fixture.root.join("denied");
    fs::create_dir(&denied)?;
    fs::copy(fixture.root.join("codex"), denied.join("codex"))?;
    let mode = if cooplet_platform::current_euid() == 0 {
        0o600
    } else {
        0o601
    };
    fs::set_permissions(denied.join("codex"), fs::Permissions::from_mode(mode))?;
    let path = env::join_paths([denied, fixture.root.clone()])?;
    let report = fixture
        .command("ok")?
        .env_remove("COOPLET_CODEX_BIN")
        .env("PATH", path)
        .args(["node", "doctor"])
        .output()?;
    assert!(report.status.success());
    let report: Value = serde_json::from_slice(&report.stdout)?;
    assert_eq!(
        adapter_report(&report, "codex")?["binary_path"],
        fixture
            .root
            .join("codex")
            .canonicalize()?
            .display()
            .to_string()
    );
    assert_eq!(adapter_report(&report, "codex")?["available"], true);

    let opened = create(&client, "path-positive-cache", None).await?;
    let id = opened["participant_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing participant id"))?;
    stop_agent(&client, id, "path-positive-stop").await?;
    fs::remove_file(fixture.root.join("codex"))?;
    let resumed = client
        .post_json::<_, Value>(
            &format!("/v1/agents/{id}/resume"),
            &json!({}),
            Some("path-removed"),
        )
        .await;
    assert!(
        matches!(resumed, Err(ClientError::Problem(problem)) if problem.extensions.get("reason") == Some(&json!("native_open_failed")))
    );
    let log = fs::read_to_string(fixture.root.join("daemon.log"))?;
    assert!(log.contains("codex_executable_unavailable"));
    participant_state(&client, id, "stopped").await
}

#[tokio::test]
async fn absent_rollout_refuses_resume_with_a_sanitized_reason() -> Test {
    let fixture = Fixture::new()?;
    let (_daemon, client) = fixture.start("no-rollout").await?;
    let open = create(&client, "create-empty-codex", None).await?;
    let id = open["participant_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing participant id"))?;
    stop_agent(&client, id, "stop-empty-codex").await?;
    let result = client
        .post_json::<_, Value>(
            &format!("/v1/agents/{id}/resume"),
            &json!({}),
            Some("resume-empty-codex"),
        )
        .await;
    let error = result
        .err()
        .ok_or_else(|| io::Error::other("empty resume unexpectedly succeeded"))?;
    assert!(!error.to_string().contains("private-fixture-marker"));
    let log = fs::read_to_string(fixture.root.join("daemon.log"))?;
    assert!(
        log.contains("codex_rollout_unavailable") && log.contains(id),
        "{log}"
    );
    assert!(!log.contains("private-fixture-marker"));
    participant_state(&client, id, "stopped").await?;
    assert!(
        fs::read_dir(fixture.state.join("runtime"))?
            .next()
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn external_turn_keeps_the_queue_closed_until_its_terminal_event() -> Test {
    let fixture = Fixture::new()?;
    let (_daemon, client) = fixture.start("external-idle").await?;
    let opened = create(&client, "tui-external-start", None).await?;
    let id = opened["participant_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing participant id"))?;
    // The fixture sends TUI turn/started then native Idle during this read.
    let _: Value = client
        .get_json(&format!("/v1/participants/{id}/attach-plan"), &[])
        .await?;
    participant_state(&client, id, "working").await?;
    let queued: Value = client
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"queued behind TUI","delivery_mode":"queue_until_idle"}),
            Some("queued-behind-tui"),
        )
        .await?;
    assert_eq!(queued["delivery_state"], "queued");
    fs::write(fixture.root.join("allow-tui-terminal"), "")?;
    let message = queued["message_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing message id"))?;
    timeout(WAIT, async {
        let path = format!("/v1/messages/{message}");
        loop {
            let result: Value = client.get_json(&path, &[]).await?;
            if result["delivery_state"] == "delivered" {
                return Ok::<_, ClientError>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert_eq!(
        fixture
            .records()?
            .iter()
            .filter(|record| record["method"] == "turn/start")
            .count(),
        1
    );
    stop_agent(&client, id, "external-tui-stop").await
}

#[tokio::test]
async fn overloaded_native_interrupt_reports_retryable_and_accepts_a_new_request() -> Test {
    let fixture = Fixture::new()?;
    let (_daemon, client) = fixture.start("cancel-overloaded").await?;
    let opened = create(&client, "overload-create", None).await?;
    let id = opened["participant_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing participant id"))?;
    let send: Value = client
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"Keep working"}),
            Some("overload-turn"),
        )
        .await?;
    assert_eq!(send["delivery_state"], "delivered");
    participant_state(&client, id, "working").await?;
    let path = format!("/v1/agents/{id}/stop");
    let cancel = json!({"cancel_turn_only":true});
    let overloaded = client
        .post_json::<_, Value>(&path, &cancel, Some("overload-cancel"))
        .await;
    assert!(matches!(overloaded, Err(ClientError::Problem(problem))
        if problem.retryable && problem.extensions.get("reason") == Some(&json!("cancel_unavailable"))));
    let retried: Value = client
        .post_json(&path, &cancel, Some("overload-cancel-retry"))
        .await?;
    assert_eq!(retried["stop_outcome"], "turn_cancelled");
    participant_state(&client, id, "idle").await?;
    stop_agent(&client, id, "overload-stop").await
}

#[tokio::test]
async fn app_server_fixture() -> Test {
    let Some(socket) = env::var_os("COOPLET_FIXTURE_SOCKET").map(PathBuf::from) else {
        return Ok(());
    };
    let mode = env::var("COOPLET_FIXTURE_MODE")?;
    let root = PathBuf::from(
        env::var_os("COOPLET_FIXTURE_ROOT")
            .ok_or_else(|| io::Error::other("missing fixture root"))?,
    );
    let home = PathBuf::from(
        env::var_os("CODEX_HOME").ok_or_else(|| io::Error::other("missing Codex home"))?,
    );
    let mcp: String = serde_json::from_str(&env::var("COOPLET_FIXTURE_MCP_COMMAND")?)?;
    let args: Vec<String> = serde_json::from_str(&env::var("COOPLET_FIXTURE_MCP_ARGS")?)?;
    assert_eq!(env::var("COOPLET_FIXTURE_MCP_REQUIRED")?, "1");
    assert_eq!(&args[..3], ["node", "mcp", "--participant-cred"]);
    assert_eq!(fs::metadata(&args[3])?.permissions().mode() & 0o777, 0o600);
    let listener = UnixListener::bind(&socket)?;
    let (stream, _) = listener.accept().await?;
    let mut ws = accept_async(stream).await?;
    let cwd = env::current_dir()?;
    let mut saved = false;
    let mut overload_interrupt = mode == "cancel-overloaded";
    while let Some(frame) = ws.next().await {
        let frame = frame?;
        if frame.is_close() {
            return Ok(());
        }
        let req: Value = serde_json::from_str(frame.to_text()?)?;
        let response = match req["method"].as_str() {
            Some("initialize") => {
                let home = if mode == "wrong-home" {
                    root.join("other")
                } else {
                    home.clone()
                };
                json!({"codexHome":home})
            }
            Some("initialized") => continue,
            Some("thread/start" | "thread/resume") => {
                saved = req["method"] == "thread/resume";
                if saved && mode == "no-rollout" {
                    send(
                        &mut ws,
                        json!({"id":req["id"],"error":{
                            "code":-32600,"message":"no rollout found for private-fixture-marker"
                        }}),
                    )
                    .await?;
                    continue;
                }
                if mode == "bad-mcp" {
                    send(&mut ws, json!({"id":req["id"], "error":{
                        "code":-32603,"message":"required MCP servers failed to initialize: private-fixture-marker"
                    }})).await?;
                    continue;
                }
                record(
                    &root,
                    &json!({"method":req["method"], "params":req["params"],
                    "mcpArgs":args, "socket":socket, "pid":std::process::id()}),
                )?;
                thread(&cwd, saved)
            }
            Some("mcpServerStatus/list") if mode == "mcp-not-ready" => {
                json!({"data":[{"name":"cooplet","runtimeStatus":"failed","tools":{},"toolsError":"private-fixture-marker"}]})
            }
            Some("mcpServerStatus/list") => {
                json!({"data":[{"name":"cooplet","tools":{"whoami":{}}}]})
            }
            Some("thread/read") if mode == "external-idle" => {
                send(&mut ws, json!({"method":"turn/started","params":{"threadId":THREAD,"turn":{"id":"tui-turn"}}})).await?;
                send(&mut ws, json!({"method":"thread/status/changed","params":{"threadId":THREAD,"status":{"type":"idle"}}})).await?;
                send(
                    &mut ws,
                    json!({"id":req["id"],"result":thread(&cwd, saved)}),
                )
                .await?;
                timeout(WAIT, async {
                    while !root.join("allow-tui-terminal").exists() {
                        sleep(Duration::from_millis(20)).await;
                    }
                })
                .await?;
                send(
                    &mut ws,
                    json!({"method":"turn/completed","params":{"threadId":THREAD,
                    "turn":{"id":"tui-turn","status":"completed","items":[]}}}),
                )
                .await?;
                continue;
            }
            Some("thread/read") => thread(&cwd, saved),
            Some("turn/start") => {
                let whoami = mcp_whoami(&mcp, &args)?;
                record(&root, &json!({"whoami":whoami}))?;
                record(&root, &req)?;
                saved = true;
                fs::write(cwd.join("fixture-rollout"), "saved fixture")?;
                send(
                    &mut ws,
                    json!({"id":req["id"],"result":{"turn":{"id":"t"}}}),
                )
                .await?;
                if mode == "cancel-overloaded" {
                    continue;
                }
                send(&mut ws, json!({"method":"turn/completed","params":{"threadId":THREAD,
                    "turn":{"id":"t","status":"completed","items":[{"type":"agentMessage","phase":"final_answer","text":"Hello"}]}}})).await?;
                continue;
            }
            Some("turn/interrupt") if mode == "cancel-overloaded" => {
                assert_eq!(req["params"]["turnId"], "t");
                if std::mem::take(&mut overload_interrupt) {
                    send(
                        &mut ws,
                        json!({"id":req["id"],"error":{
                            "code":-32001,"message":"Server overloaded; retry later."
                        }}),
                    )
                    .await?;
                } else {
                    send(&mut ws, json!({"id":req["id"],"result":{}})).await?;
                    send(
                        &mut ws,
                        json!({"method":"turn/completed","params":{"threadId":THREAD,
                        "turn":{"id":"t","status":"interrupted","items":[]}}}),
                    )
                    .await?;
                }
                continue;
            }
            _ => return Err(io::Error::other("unsupported fixture request").into()),
        };
        send(&mut ws, json!({"id":req["id"],"result":response})).await?;
    }
    Ok(())
}

fn thread(cwd: &Path, saved: bool) -> Value {
    json!({"cwd":cwd,"modelProvider":"fixture",
        "thread":{"id":THREAD,"cwd":cwd,"modelProvider":"fixture","status":{"type":"idle"},"path":saved.then(|| cwd.join("fixture-rollout"))}})
}

async fn send(ws: &mut WebSocketStream<UnixStream>, packet: Value) -> Test {
    Ok(ws.send(Message::Text(packet.to_string().into())).await?)
}

fn record(root: &Path, value: &Value) -> Test {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("record.jsonl"))?;
    Ok(writeln!(file, "{value}")?)
}

fn mcp_whoami(program: &str, args: &[String]) -> Test<Value> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("missing MCP stdin"))?;
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2025-03-26","capabilities":{{}},"clientInfo":{{"name":"fixture","version":"0"}}}}}}"#
    )?;
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
    )?;
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"whoami","arguments":{{}}}}}}"#
    )?;
    drop(stdin);
    let stdout = BufReader::new(
        child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("missing MCP stdout"))?,
    );
    let replies: Vec<Value> = stdout
        .lines()
        .map(|line| Ok::<_, Box<dyn Error + Send + Sync>>(serde_json::from_str(&line?)?))
        .collect::<Test<_>>()?;
    assert!(child.wait()?.success());
    replies
        .into_iter()
        .find(|reply| reply["id"] == 2)
        .ok_or_else(|| io::Error::other("missing MCP whoami reply").into())
}

#[tokio::test]
#[ignore = "requires an installed Codex CLI on the host"]
async fn native_codex_smoke() -> Test {
    use tokio_tungstenite::client_async;

    let binary = env::var_os("COOPLET_NATIVE_CODEX_BIN")
        .ok_or_else(|| io::Error::other("set COOPLET_NATIVE_CODEX_BIN to the native CLI path"))?;
    let fixture = Fixture::new()?;
    let mut backend = codex_backend::Backend::start(&fixture.root.join("home")).await?;
    let mut command = fixture.command("native")?;
    command
        .env("COOPLET_CODEX_BIN", &binary)
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env("HTTP_PROXY", "http://127.0.0.1:1")
        .env("HTTPS_PROXY", "http://127.0.0.1:1")
        .env("ALL_PROXY", "http://127.0.0.1:1")
        .env("NO_PROXY", "localhost,127.0.0.1");
    let (_daemon, client) = fixture.start_with(command).await?;
    let doctor = fixture
        .command("native")?
        .env("COOPLET_CODEX_BIN", &binary)
        .args(["node", "doctor"])
        .output()?;
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let diagnosis: Value = serde_json::from_slice(&doctor.stdout)?;
    assert_eq!(adapter_report(&diagnosis, "codex")?["available"], true);

    let opened = create(&client, "native-create", None)
        .await
        .map_err(|error| {
            let log = fs::read_to_string(fixture.root.join("daemon.log")).unwrap_or_default();
            io::Error::other(format!("{error}\n{log}"))
        })?;
    let id = opened["participant_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing participant"))?;
    assert_eq!(id.parse::<ParticipantId>()?.as_str(), id);
    participant_state(&client, id, "idle").await?;
    let before: Value = client
        .get_json(&format!("/v1/participants/{id}/attach-plan"), &[])
        .await?;
    assert_eq!(before["mode"], "unsupported");
    let send_message = |key: &'static str| {
        let client = client.clone();
        async move {
            client
                .post_json::<_, Value>(
                    "/v1/messages",
                    &json!({"to":id,"body":"Native smoke fixture"}),
                    Some(key),
                )
                .await
        }
    };
    let receipt = send_message("native-first-turn").await?;
    assert_eq!(receipt["delivery_state"], "delivered");
    let first = backend.request().await?;
    assert!(first["input"].to_string().contains("Native smoke fixture"));
    participant_state(&client, id, "idle").await?;
    let path = format!("/v1/participants/{id}/attach-plan");
    let attach: Value = client.get_json(&path, &[]).await?;
    assert_eq!(attach["mode"], "exec", "{attach}");
    let socket = attach["args"][1]
        .as_str()
        .and_then(|url| url.strip_prefix("unix://"))
        .ok_or_else(|| io::Error::other("missing native attach address"))?;
    let thread = attach["args"][3]
        .as_str()
        .ok_or_else(|| io::Error::other("missing native thread"))?;
    let (mut ws, _) = client_async("ws://localhost/", UnixStream::connect(socket).await?).await?;
    rpc(
        &mut ws,
        1,
        "initialize",
        json!({"clientInfo":{"name":"smoke-peer","version":"0"}}),
    )
    .await?;
    send(&mut ws, json!({"method":"initialized"})).await?;
    let resumed = rpc(
        &mut ws,
        2,
        "thread/resume",
        json!({"threadId":thread,"excludeTurns":true}),
    )
    .await?;
    assert_eq!(resumed["thread"]["id"], thread);

    drop(ws);

    stop_agent(&client, id, "native-stop-first").await?;
    let _: Value = client
        .post_json(
            &format!("/v1/agents/{id}/resume"),
            &json!({}),
            Some("native-resume"),
        )
        .await?;
    let attach2: Value = client.get_json(&path, &[]).await?;
    assert_eq!(attach2["args"][3], thread);
    assert_ne!(attach2["args"][1], attach["args"][1]);
    assert_eq!(
        send_message("native-second-turn").await?["delivery_state"],
        "delivered"
    );
    let _ = backend.request().await?;
    participant_state(&client, id, "idle").await?;
    stop_agent(&client, id, "native-stop-second").await
}

async fn rpc(
    ws: &mut WebSocketStream<UnixStream>,
    id: u32,
    method: &str,
    params: Value,
) -> Test<Value> {
    send(ws, json!({"id":id,"method":method,"params":params})).await?;
    timeout(WAIT, async {
        while let Some(frame) = ws.next().await {
            let packet: Value = serde_json::from_str(frame?.to_text()?)?;
            if packet["id"] == id {
                if packet.get("error").is_some() {
                    return Err(io::Error::other(format!("{method}: {}", packet["error"])).into());
                }
                return Ok(packet["result"].clone());
            }
        }
        Err(io::Error::other("WS closed before reply").into())
    })
    .await?
}
