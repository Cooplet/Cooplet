use std::{
    error::Error,
    fs, io,
    io::{BufRead, BufReader, Write},
    os::unix::{ffi::OsStrExt as _, fs::PermissionsExt as _},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
    time::Duration,
};

use cooplet_client::{ClientError, CoopletClient, api_path};
use cooplet_node::{
    auth::{Credential, MASTER_FILE},
    clients::AdapterRegistry,
    config::{NodeConfig, operator_path},
    daemon::Node,
    http::openapi_json,
    now_ms,
};
use cooplet_protocol::{ErrorCode, MessageId, ParticipantId, PrincipalKind, RequestId};
use cooplet_storage::WriteOutcome;
use rusqlite::params;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

#[path = "common/doctor.rs"]
mod doctor;
use doctor::adapter_report;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const COOPLET: &str = env!("CARGO_BIN_EXE_cooplet");
const WAIT: Duration = Duration::from_secs(10);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> TestResult<Self> {
        let path =
            std::env::temp_dir().join(format!("cooplet-node-test-{}", RequestId::generate()?));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn state(&self) -> PathBuf {
        self.0.join("state")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Process(Child);

impl Process {
    fn new(command: &mut Command) -> TestResult<Self> {
        Ok(Self(command.spawn()?))
    }

    fn stdout(&mut self) -> TestResult<BufReader<ChildStdout>> {
        self.0
            .stdout
            .take()
            .map(BufReader::new)
            .ok_or_else(|| io::Error::other("Missing child stdout").into())
    }

    async fn wait(&mut self) -> TestResult<std::process::ExitStatus> {
        Ok(tokio::time::timeout(WAIT, async {
            loop {
                if let Some(status) = self.0.try_wait()? {
                    return Ok::<_, io::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await??)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn json_line(
    mut reader: BufReader<ChildStdout>,
) -> TestResult<(BufReader<ChildStdout>, Value)> {
    let (reader, line) = tokio::time::timeout(
        WAIT,
        tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            Ok::<_, io::Error>((reader, line))
        }),
    )
    .await???;
    Ok((reader, serde_json::from_str(&line)?))
}

async fn run_process(state_dir: &Path) -> TestResult<(Process, Value)> {
    let mut child = Process::new(
        Command::new(COOPLET)
            .args(["node", "run", "--display-name", "Node Test", "--state-dir"])
            .arg(state_dir)
            .stdout(Stdio::piped()),
    )?;
    let (_stdout, ready) = json_line(child.stdout()?).await?;
    Ok((child, ready))
}

async fn operator(state: &Path) -> TestResult<CoopletClient> {
    let (client, _) = Credential::read(&operator_path(state))?
        .connect(PrincipalKind::Operator)
        .await?;
    Ok(client)
}

fn command(state: &Path, args: &[&str]) -> TestResult<std::process::Output> {
    let output = Command::new(COOPLET)
        .env("COOPLET_CODEX_BIN", state.join("missing-codex"))
        .arg("--state-dir")
        .arg(state)
        .args(args)
        .env("PATH", state.join("empty-path"))
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

#[tokio::test]
async fn doctor_and_run_use_the_registered_agy_adapter() -> TestResult {
    use std::{
        ffi::OsStr,
        os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    };

    let directory = TestDirectory::new()?;
    let state = directory.state();
    let home = directory.0.join("home");
    fs::create_dir_all(home.join(".gemini/antigravity-cli"))?;
    let binary = directory.0.join("agy");
    fs::write(
        &binary,
        r#"#!/bin/sh
[ -z "${COOPLET_AGY_TEST_SENTINEL:-}" ] || exit 2
IFS= read -r request || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"version":"2.3.4"},"agentCapabilities":{"loadSession":true,"_meta":{"antigravity":{"systemPrompt":{"append":true}}}}}}'
if [ "$5" = "--dangerously-skip-permissions" ]; then
    IFS= read -r request || exit 1
    printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"cli-fixture"}}'
    while IFS= read -r request; do :; done
fi
"#,
    )?;
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755))?;

    let mut daemon = Process::new(
        Command::new(COOPLET)
            .args(["node", "run", "--state-dir"])
            .arg(&state)
            .env_remove("COOPLET_CODEX_BIN")
            .env("PATH", &directory.0)
            .env("HOME", &home)
            .env("COOPLET_AGY_TEST_SENTINEL", "must be removed")
            .stdout(Stdio::piped()),
    )?;
    let (_reader, _) = json_line(daemon.stdout()?).await?;
    let unavailable: Value = serde_json::from_slice(&command(&state, &["node", "doctor"])?.stdout)?;
    let report = adapter_report(&unavailable, "agy")?;
    assert_eq!(report["available"], false);
    let output = Command::new(COOPLET)
        .args(["node", "doctor", "--state-dir"])
        .arg(&state)
        .env_remove("COOPLET_CODEX_BIN")
        .env("PATH", &directory.0)
        .env("HOME", &home)
        .env("COOPLET_AGY_TEST_SENTINEL", "must be removed")
        .output()?;
    assert!(output.status.success());
    let available: Value = serde_json::from_slice(&output.stdout)?;
    let report = adapter_report(&available, "agy")?;
    assert_eq!(report["available"], true);
    assert_eq!(report["capabilities"]["supports_resume"], true);
    assert_eq!(report["capabilities"]["supports_tui_attach"], false);
    assert_eq!(report["binary_path"], json!(binary.canonicalize()?));
    assert_eq!(report["version"], "2.3.4");

    let client = operator(&state).await?;
    let created: Value = client
        .post_json(
            "/v1/agents",
            &json!({"client_kind":"agy","name":"agy-cli"}),
            Some("cli-create-agy"),
        )
        .await?;
    let id = created["participant_id"]
        .as_str()
        .ok_or("Missing participant ID")?;
    let participant: Value = client
        .get_json(&format!("/v1/participants/{id}"), &[])
        .await?;
    assert_eq!(participant["state"], "idle");

    let raw = directory.0.join(OsStr::from_bytes(b"bin-\xff"));
    fs::create_dir(&raw)?;
    fs::rename(&binary, raw.join("agy"))?;
    let output = Command::new(COOPLET)
        .args(["node", "doctor", "--state-dir"])
        .arg(&state)
        .env_remove("COOPLET_CODEX_BIN")
        .env("PATH", &raw)
        .output()?;
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(adapter_report(&report, "agy")?["available"], true);
    assert_eq!(
        adapter_report(&report, "agy")?["binary_path"],
        raw.join("agy").display().to_string()
    );

    command(&state, &["node", "stop"])?;
    assert!(daemon.wait().await?.success());
    Ok(())
}

#[tokio::test]
async fn startup_announces_the_exact_operator_path_for_non_utf8_state() -> TestResult {
    use std::ffi::OsStr;

    let directory = TestDirectory::new()?;
    let raw = directory.0.join(OsStr::from_bytes(b"state-\xff"));
    let unicode = directory.0.join("state-\u{fffd}");
    let (mut first, first_ready) = run_process(&raw).await?;
    let (mut second, second_ready) = run_process(&unicode).await?;
    assert_ne!(first_ready["node_id"], second_ready["node_id"]);
    assert_eq!(first_ready.get("operator_credential"), Some(&Value::Null));
    let bytes: Vec<u8> = serde_json::from_value(first_ready["operator_credential_bytes"].clone())?;
    let path = Path::new(OsStr::from_bytes(&bytes));
    assert_eq!(path, operator_path(&raw).canonicalize()?);
    assert!(path.is_file());
    assert_eq!(
        second_ready["operator_credential"],
        json!(operator_path(&unicode).canonicalize()?)
    );
    assert!(second_ready.get("operator_credential_bytes").is_none());

    for (dir, daemon) in [(&raw, &mut first), (&unicode, &mut second)] {
        command(dir, &["node", "stop"])?;
        assert!(daemon.wait().await?.success());
    }
    Ok(())
}

#[tokio::test]
async fn foreground_cli_restarts_with_same_identity_and_replays_stop_without_stopping() -> TestResult
{
    let directory = TestDirectory::new()?;
    let state = directory.state();
    let (mut daemon, ready) = run_process(&state).await?;
    assert_eq!(
        ready["operator_credential"],
        operator_path(&state).canonicalize()?.display().to_string()
    );

    let duplicate = Command::new(COOPLET)
        .args(["node", "run", "--state-dir"])
        .arg(&state)
        .output()?;
    assert!(!duplicate.status.success());
    assert!(duplicate.stdout.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&command(&state, &["node", "status"])?.stdout)?["status"],
        "ok"
    );
    let client_path = directory
        .0
        .join(std::ffi::OsStr::from_bytes(b"client-binaries-\xff"));
    fs::create_dir(&client_path)?;
    let first_bin = directory.0.join("first-bin");
    fs::create_dir(&first_bin)?;
    let shadow = first_bin.join("pi");
    fs::write(&shadow, "#!/bin/sh\nexit 1\n")?;
    // For the owning non-root user, execute permission for others does not apply.
    let mode = if cooplet_platform::current_euid() == 0 {
        0o644
    } else {
        0o645
    };
    fs::set_permissions(&shadow, fs::Permissions::from_mode(mode))?;
    let path = std::env::join_paths([&first_bin, &client_path])?;
    let diagnose = || -> TestResult<Value> {
        let output = Command::new(COOPLET)
            .args(["node", "doctor", "--state-dir"])
            .arg(&state)
            .env("PATH", &path)
            .env_remove("COOPLET_CODEX_BIN")
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    };
    let diagnosis = diagnose()?;
    assert_eq!(diagnosis["running"], true);
    assert_eq!(adapter_report(&diagnosis, "agy")?["available"], false);
    assert_eq!(adapter_report(&diagnosis, "codex")?["available"], false);
    assert_eq!(adapter_report(&diagnosis, "pi")?["available"], false);
    assert_eq!(
        diagnosis["workspace_dir"],
        json!(std::env::current_dir()?.canonicalize()?)
    );
    let pi = client_path.join("pi");
    fs::write(
        &pi,
        r#"#!/bin/sh
case "$1" in
  --help) printf '%s\n' '  --mode <mode> text, json, or rpc' '  --session <path>' '  --extension <path>' '  --model <id>' '  --thinking <level>' ;;
  --version) printf '%s\n' '9.0.0' ;;
  *) exit 1 ;;
esac
"#,
    )?;
    fs::set_permissions(&pi, fs::Permissions::from_mode(0o755))?;
    let diagnosis = diagnose()?;
    let installed = adapter_report(&diagnosis, "pi")?;
    assert_eq!(installed["available"], true);
    assert_eq!(installed["version"], "9.0.0");
    assert_eq!(installed["binary_path"], pi.display().to_string());
    assert_eq!(installed["capabilities"]["supports_resume"], true);
    assert_eq!(installed["capabilities"]["supports_tui_attach"], false);
    let workspace = directory
        .0
        .join(std::ffi::OsStr::from_bytes(b"separate-workspace-\xff"));
    let template = workspace.join(".cooplet/agents/example");
    fs::create_dir_all(&template)?;
    fs::write(
        template.join("agent.json"),
        br#"{"schema":1,"client":"example"}"#,
    )?;
    let selected = Command::new(COOPLET)
        .env("COOPLET_CODEX_BIN", state.join("missing-codex"))
        .arg("--state-dir")
        .arg(&state)
        .args(["node", "doctor", "--workspace-dir"])
        .env("PATH", directory.0.join("empty-path"))
        .arg(&workspace)
        .output()?;
    assert!(selected.status.success());
    let report: Value = serde_json::from_slice(&selected.stdout)?;
    assert_eq!(
        report["workspace_dir"],
        workspace.canonicalize()?.display().to_string()
    );
    assert!(
        report["templates"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["template_id"] == "example"))
    );

    let file = directory.0.join("upload.txt");
    fs::write(&file, "loopback data")?;
    let file_arg = file
        .to_str()
        .ok_or_else(|| io::Error::other("Invalid fixture path"))?;
    let uploaded = command(
        &state,
        &[
            "blob",
            "put",
            file_arg,
            "--mime-type",
            "text/plain",
            "--operation-key",
            "cli-blob-upload",
        ],
    )?;
    let blob: Value = serde_json::from_slice(&uploaded.stdout)?;
    let blob_id = blob["blob_id"]
        .as_str()
        .ok_or_else(|| io::Error::other("Missing BlobId"))?;
    let destination = directory.0.join("download.txt");
    let destination_arg = destination
        .to_str()
        .ok_or_else(|| io::Error::other("Invalid fixture path"))?;
    command(
        &state,
        &["blob", "get", blob_id, "--output", destination_arg],
    )?;
    assert_eq!(fs::read(&destination)?, b"loopback data");

    let stopped = command(
        &state,
        &["node", "stop", "--operation-key", "stop-original-node"],
    )?;
    let receipt: Value = serde_json::from_slice(&stopped.stdout)?;
    assert!(daemon.wait().await?.success());
    let (mut daemon, restarted) = run_process(&state).await?;
    assert_eq!(ready, restarted);
    let client = operator(&state).await?;
    let replay: Value = client
        .post_json("/v1/node/stop", &json!({}), Some("stop-original-node"))
        .await?;
    assert_eq!(replay, receipt);
    assert_eq!(client.health().await?.status, "ok");
    let cli_replay = command(
        &state,
        &["node", "stop", "--operation-key", "stop-original-node"],
    )?;
    assert_eq!(
        serde_json::from_slice::<Value>(&cli_replay.stdout)?,
        receipt
    );
    assert_eq!(client.health().await?.status, "ok");

    command(&state, &["node", "stop"])?;
    assert!(daemon.wait().await?.success());
    let offline: Value = serde_json::from_slice(&command(&state, &["node", "doctor"])?.stdout)?;
    assert_eq!(offline["running"], false);
    assert_eq!(offline["node_id"], ready["node_id"]);
    let before = fs::read(state.join("node.sqlite3"))?;
    let wal = state.join("node.sqlite3-wal");
    let shm = state.join("node.sqlite3-shm");
    assert!(!wal.try_exists()?);
    assert!(!shm.try_exists()?);
    let master = state.join(MASTER_FILE);
    let saved_key = fs::read(&master)?;
    let mut wrong_key = saved_key.clone();
    wrong_key[0] ^= 1;
    fs::write(&master, &wrong_key)?;
    let denied_doctor = Command::new(COOPLET)
        .args(["node", "doctor", "--state-dir"])
        .env("PATH", directory.0.join("empty-path"))
        .arg(&state)
        .output()?;
    assert!(!denied_doctor.status.success());
    assert!(denied_doctor.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&denied_doctor.stderr).contains("does not match this database")
    );
    assert_eq!(fs::read(state.join("node.sqlite3"))?, before);
    assert!(!wal.try_exists()?);
    assert!(!shm.try_exists()?);
    fs::write(&master, saved_key)?;
    fs::rename(&master, state.join("saved.master.key"))?;
    let denied = Command::new(COOPLET)
        .args(["node", "run", "--state-dir"])
        .arg(&state)
        .output()?;
    assert!(!denied.status.success());
    assert!(denied.stdout.is_empty());
    assert_eq!(fs::read(state.join("node.sqlite3"))?, before);
    assert!(!wal.try_exists()?);
    assert!(!shm.try_exists()?);

    fs::rename(state.join("saved.master.key"), master)?;
    let (mut daemon, _) = run_process(&state).await?;
    let signal = Command::new("sh")
        .args(["-c", "kill -TERM \"$1\"", "signal-daemon"])
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(signal.success());
    assert!(daemon.wait().await?.success());
    Ok(())
}

#[tokio::test]
async fn gate_openapi_drift() -> TestResult {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contracts/openapi-node.json");
    let expected = fs::read(path)?;
    assert_eq!(openapi_json()?.as_bytes(), expected);
    let definition: Value = serde_json::from_slice(&expected)?;
    assert_eq!(definition["openapi"], "3.1.0");
    assert_eq!(
        definition["components"]["securitySchemes"]["bearer"]["scheme"],
        "bearer"
    );
    let paths = definition["paths"].as_object().ok_or("Missing API paths")?;
    for content in [
        &definition["paths"]["/v1/blobs"]["post"]["requestBody"]["content"],
        &definition["paths"]["/v1/blobs/{id}"]["get"]["responses"]["200"]["content"],
    ] {
        for mime in ["application/octet-stream", "*/*"] {
            assert_eq!(
                content[mime]["schema"],
                json!({"type": "string", "format": "binary"})
            );
        }
    }
    for (path, item) in paths {
        for method in ["get", "post"] {
            let operation = &item[method];
            if operation.is_null() {
                continue;
            }
            let mut expected = vec!["400", "413", "503", "default"];
            if operation["security"].is_array() {
                expected.push("401");
            }
            if method == "post" {
                expected.extend(["409", "507"]);
            }
            for status in expected {
                assert_eq!(
                    operation["responses"][status]["content"]["application/problem+json"]["schema"]
                        ["$ref"],
                    "#/components/schemas/ProblemDetails",
                    "{method} {path} {status}",
                );
            }
        }
    }
    let export = Command::new(COOPLET).arg("openapi").output()?;
    assert!(export.status.success());
    assert_eq!(export.stdout, expected);

    let directory = TestDirectory::new()?;
    let state = directory.state();
    let (mut daemon, ready) = run_process(&state).await?;
    let address = ready["endpoint"]
        .as_str()
        .and_then(|endpoint| endpoint.strip_prefix("http://"))
        .ok_or_else(|| io::Error::other("Missing Node address"))?;
    let response = raw_http(
        address,
        "GET /v1/openapi.json HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        &[],
    )
    .await?;
    assert_eq!(body(&response)?, expected);
    command(&state, &["node", "stop"])?;
    assert!(daemon.wait().await?.success());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_stdio_mcp_preserves_author_and_operator_can_answer() -> TestResult {
    let directory = TestDirectory::new()?;
    let state = directory.state();
    let node = Node::open(
        NodeConfig::new(state.clone(), directory.0.clone()),
        AdapterRegistry::default(),
    )
    .await?;
    let id = ParticipantId::generate()?;
    let node_id = node.state.info.node_id.clone();
    let std::net::SocketAddr::V4(address) = node.address()? else {
        return Err("Expected IPv4".into());
    };
    let credential = Credential::new(node_id, id.clone(), address)?;
    let cred_filename = format!("{id}.g1.json");
    let creds = state.join("creds");
    credential.publish(&creds, &cred_filename)?;
    let auth = node.state.auth.clone();
    let workspace = directory.0.to_string_lossy().into_owned();
    node.state.db.write_sqlite(move |tx| {
        let now = now_ms().map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        tx.execute(
            "INSERT INTO participants (participant_id,kind,name,state,workspace_dir,created_at_ms,updated_at_ms)
             VALUES (?1,'agent','peer','idle',?2,?3,?3)",
            params![credential.participant_id.as_str(), workspace, now],
        )?;
        auth.insert_credential(tx, &credential, PrincipalKind::Participant, now)?;
        Ok(WriteOutcome::new(()))
    }).await?;

    let shutdown = node.state.shutdown.clone();
    let server = tokio::spawn(node.serve());
    let operator = operator(&state).await?;
    let operator_id = operator
        .whoami()
        .await?
        .participant_id
        .ok_or("Missing operator ID")?;
    let bad_credential = Command::new(COOPLET)
        .args(["node", "mcp", "--participant-cred"])
        .arg(operator_path(&state))
        .output()?;
    assert!(!bad_credential.status.success());
    assert!(bad_credential.stdout.is_empty());

    let mut mcp = Process::new(
        Command::new(COOPLET)
            .args(["node", "mcp", "--participant-cred"])
            .arg(creds.join(&cred_filename))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped()),
    )?;
    let mut input = mcp.0.stdin.take().ok_or("Missing stdio input")?;
    let output = mcp.stdout()?;
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"node-test","version":"1"}
    }});
    writeln!(input, "{initialize}")?;
    let (output, reply) = json_line(output).await?;
    assert_eq!(reply["result"]["protocolVersion"], "2025-11-25");
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","method":"notifications/initialized"})
    )?;
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})
    )?;
    let (output, reply) = json_line(output).await?;
    let tools = reply["result"]["tools"].as_array().ok_or("Missing tools")?;
    assert_eq!(tools.len(), 5);
    assert!(tools.iter().all(|tool| tool["name"] != "respond_approval"));

    send_tool(&mut input, 3, "whoami", json!({}))?;
    let (output, reply) = json_line(output).await?;
    let identity = tool_payload(&reply, false)?;
    assert_eq!(identity["participant_id"], id.as_str());
    assert_eq!(identity["principal_kind"], "participant");

    for (number, field) in [(4, "from"), (5, "author")] {
        let mut spoofed =
            json!({"to":operator_id,"body":"wrong author","operation_key":"spoofed-field"});
        spoofed[field] = json!(operator_id);
        send_tool(&mut input, number, "send_message", spoofed)?;
    }
    let (output, reply) = json_line(output).await?;
    assert_eq!(tool_payload(&reply, true)?["code"], "invalid_request");
    let (output, reply) = json_line(output).await?;
    assert_eq!(tool_payload(&reply, true)?["code"], "invalid_request");

    send_tool(
        &mut input,
        6,
        "blob_put",
        json!({"path":format!("state/creds/{cred_filename}"),"operation_key":"forbidden-blob"}),
    )?;
    let (output, reply) = json_line(output).await?;
    assert_eq!(reply["result"]["isError"], true);

    send_tool(
        &mut input,
        7,
        "ask_user",
        json!({
            "operation_key":"real-question",
            "question":{"mode":"text","prompt":"Confirm the result?","expires_at_ms":now_ms()?+60_000}
        }),
    )?;
    let (_output, reply) = json_line(output).await?;
    let question = tool_payload(&reply, false)?;
    let message_id: MessageId = serde_json::from_value(question["message_id"].clone())?;
    let message_path = api_path(&["messages", message_id.as_str()])?;
    let message: Value = operator.get_json(&message_path, &[]).await?;
    assert_eq!(message["sender_id"], id.as_str());
    assert_eq!(message["recipient_id"], operator_id.as_str());

    let reply = json!({
        "to":id,"body":"","in_reply_to":message_id,
        "answer":{"text":"Confirmed"}
    });
    let (first, second): (Result<Value, _>, Result<Value, _>) = tokio::join!(
        operator.post_json("/v1/messages", &reply, Some("answer-key-one")),
        operator.post_json("/v1/messages", &reply, Some("answer-key-two"))
    );
    let (accepted, rejected) = match (first, second) {
        (Ok(accepted), Err(rejected)) | (Err(rejected), Ok(accepted)) => (accepted, rejected),
        other => return Err(format!("Expected one answer to win: {other:?}").into()),
    };
    assert_eq!(accepted["delivery_state"], "unavailable");
    assert!(matches!(rejected, ClientError::Problem(problem)
        if problem.code == ErrorCode::QuestionAlreadyAnswered &&
        problem.extensions.get("existing_answer_id") == accepted.get("message_id")));

    drop(input);
    assert!(mcp.wait().await?.success());
    shutdown.send_replace(true);
    tokio::time::timeout(WAIT, server).await???;
    Ok(())
}

fn send_tool(
    stdin: &mut std::process::ChildStdin,
    id: i32,
    name: &str,
    arguments: Value,
) -> TestResult {
    let message = json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}});
    writeln!(stdin, "{message}")?;
    stdin.flush()?;
    Ok(())
}

fn tool_payload(response: &Value, expect_error: bool) -> TestResult<Value> {
    assert_eq!(response["result"]["isError"], expect_error, "{response}");
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .ok_or("Missing tool content")?;
    Ok(serde_json::from_str(text)?)
}

#[tokio::test]
async fn paused_download_does_not_hold_daemon_shutdown() -> TestResult {
    let directory = TestDirectory::new()?;
    let state = directory.state();
    let (mut daemon, ready) = run_process(&state).await?;
    let address = ready["endpoint"]
        .as_str()
        .and_then(|endpoint| endpoint.strip_prefix("http://"))
        .ok_or("Missing Node address")?;
    let file = directory.0.join("large.bin");
    fs::File::create(&file)?.set_len(cooplet_storage::DEFAULT_MAX_BLOB_BYTES)?;
    let upload = command(
        &state,
        &["blob", "put", file.to_str().ok_or("Invalid fixture path")?],
    )?;
    let blob: Value = serde_json::from_slice(&upload.stdout)?;
    let blob = blob["blob_id"].as_str().ok_or("Missing blob identifier")?;
    let credential: Value = serde_json::from_slice(&fs::read(operator_path(&state))?)?;
    let token = credential["token"].as_str().ok_or("Missing test bearer")?;

    let mut socket = tokio::net::TcpStream::connect(address).await?;
    socket.write_all(format!(
        "GET /v1/blobs/{blob} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\r\n",
    ).as_bytes()).await?;
    let mut download = tokio::io::BufReader::new(socket);
    let mut status = String::new();
    tokio::time::timeout(WAIT, download.read_line(&mut status)).await??;
    assert!(status.starts_with("HTTP/1.1 200 "), "{status}");

    let started = std::time::Instant::now();
    let state_dir = state.clone();
    let stop = tokio::task::spawn_blocking(move || command(&state_dir, &["node", "stop"]));
    tokio::time::timeout(WAIT, stop).await???;
    assert!(daemon.wait().await?.success());
    assert!(started.elapsed() < WAIT);
    drop(download);
    Ok(())
}

async fn raw_http(address: &str, headers: &str, body: &[u8]) -> TestResult<Vec<u8>> {
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut response = Vec::new();
    tokio::time::timeout(WAIT, stream.read_to_end(&mut response)).await??;
    Ok(response)
}

fn body(response: &[u8]) -> TestResult<&[u8]> {
    let start = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .ok_or("Missing HTTP headers")?;
    Ok(&response[start + 4..])
}

#[tokio::test]
async fn http_rejections_use_problems_and_auth_reads_leave_wal_unchanged() -> TestResult {
    let directory = TestDirectory::new()?;
    let state = directory.state();
    let (mut daemon, ready) = run_process(&state).await?;
    let address = ready["endpoint"]
        .as_str()
        .and_then(|endpoint| endpoint.strip_prefix("http://"))
        .ok_or("Missing Node address")?;
    let credential: Value = serde_json::from_slice(&fs::read(operator_path(&state))?)?;
    let token = credential["token"].as_str().ok_or("Missing test bearer")?;

    let wal = state.join("node.sqlite3-wal");
    let before = fs::read(&wal)?;
    let client = operator(&state).await?;
    for _ in 0..10 {
        client.health().await?;
        client.whoami().await?;
        client.list_events(&Default::default()).await?;
    }
    assert_eq!(fs::read(&wal)?, before);

    let cases = [
        ("GET /v1/absent", "", "", "404", "not_found"),
        ("DELETE /v1/messages", "", "", "405", "invalid_request"),
        (
            "GET /v1/participants?limit=99&extraneous=true",
            "",
            "",
            "400",
            "invalid_request",
        ),
        (
            "GET /v1/health",
            "Authorization: Bearer repeated\r\n",
            "",
            "401",
            "unauthorized",
        ),
        (
            "POST /v1/messages",
            "Idempotency-Key: duplicate-key-a\r\nIdempotency-Key: duplicate-key-b\r\n",
            r#"{"to":"invalid","body":"test"}"#,
            "400",
            "invalid_request",
        ),
        (
            "POST /v1/messages",
            "Idempotency-Key: positional\r\n",
            "[1]",
            "400",
            "invalid_request",
        ),
    ];
    for (request, extra_headers, payload, status, code) in cases {
        let headers = format!(
            "{request} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {token}\r\n{extra_headers}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            payload.len(),
        );
        assert_problem(
            &raw_http(address, &headers, payload.as_bytes()).await?,
            status,
            code,
        )?;
    }
    for (request, cap) in [
        ("GET /v1/blobs/blob_00000000000000000000000000", 64 * 1024),
        ("POST /v1/node/stop", 64 * 1024),
        ("POST /v1/messages", 2 * 1024 * 1024),
    ] {
        let headers = format!(
            "{request} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nIdempotency-Key: oversize-request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            cap + 1,
        );
        assert_problem(
            &raw_http(address, &headers, &[]).await?,
            "413",
            "payload_too_large",
        )?;
    }
    for extra in ["Content-Length: 5\r\n", "Transfer-Encoding: chunked\r\n"] {
        let headers = format!(
            "GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {token}\r\n{extra}\r\n"
        );
        assert_problem(
            &raw_http(address, &headers, &[]).await?,
            "400",
            "invalid_request",
        )?;
    }
    command(&state, &["node", "stop"])?;
    assert!(daemon.wait().await?.success());
    Ok(())
}

fn assert_problem(response: &[u8], status: &str, code: &str) -> TestResult {
    let header_size = response.len() - body(response)?.len();
    let headers = std::str::from_utf8(&response[..header_size])?;
    assert!(
        headers.starts_with(&format!("HTTP/1.1 {status} ")),
        "{headers}"
    );
    assert!(
        headers.contains("content-type: application/problem+json\r\n"),
        "{headers}"
    );
    assert!(headers.contains("cache-control: no-store\r\n"), "{headers}");
    assert!(
        headers.contains("x-content-type-options: nosniff\r\n"),
        "{headers}"
    );
    let document: Value = serde_json::from_slice(body(response)?)?;
    assert_eq!(document["code"], code);
    assert_eq!(document["status"], status.parse::<u16>()?);
    Ok(())
}
