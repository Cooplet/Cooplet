use std::{
    fs, io,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

use cooplet_client::CoopletClient;
use cooplet_node::{
    auth::Credential,
    clients::{AdapterRegistry, SubmitOutcome, TurnInput},
    config::{NodeConfig, operator_path},
    daemon::Node,
};
use cooplet_protocol::{ParticipantId, PrincipalKind};
use serde_json::{Value, json};

use super::{
    bridge_hub::{BridgeHub, Process, Scratch},
    field, items, member_when,
    mock_adapter::MockAdapter,
    online, participant_state, post,
    test_node::TestResult,
    wait_for,
};

const NODE_ROOT: &str = "COOPLET_TEST_THREAD_CRASH_ROOT";
const NODE_PASS: &str = "COOPLET_TEST_THREAD_CRASH_PASS";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_thread_delivery_waits_for_an_explicit_offline_resume() -> TestResult {
    let mut hub = BridgeHub::start().await?;
    let scratch = Scratch::new()?;
    let root = &scratch.0;
    fs::create_dir(root.join("workspace"))?;
    let mut first = crash_node(root, "initial").await?;
    let client = crash_client(root).await?;
    let identity = client.whoami().await?;
    let created: Value = post(
        &client,
        "/v1/agents",
        &json!({"client_kind":"mock","name":"recoverer"}),
        "create-recoverer",
    )
    .await?;
    let agent: ParticipantId = field(&created, "participant_id")?.parse()?;
    let enrolled = hub.issue("crash-node").await?;
    let _: Value = client
        .post_json("/v1/bridge/enroll", &hub.binding(&enrolled), None)
        .await?;
    online(&hub.admin, &enrolled.node_id).await?;
    participant_state(&hub.admin, &enrolled.node_id, &agent, "idle").await?;
    let member = format!("{}/{agent}", enrolled.node_id);
    let board = post(
        &hub.admin,
        "/v1/threads",
        &json!({
            "title":"Crash boundary", "members":[{"member_key":member,"handle":"recoverer"}],
        }),
        "crash-board",
    )
    .await?;
    let thread = field(&board, "thread_id")?;
    let posts = format!("/v1/threads/{thread}/posts");
    post(
        &hub.admin,
        &posts,
        &json!({"body":"@recoverer work before the crash"}),
        "crash-post",
    )
    .await?;
    wait_for_marker(root, &mut first, "native-initial").await?;
    assert_eq!(
        read_fence(root, thread, &agent)?,
        ("injecting".into(), 0, 1)
    );
    first.0.kill()?;
    assert_eq!(first.0.wait()?.signal(), Some(9));

    // Roster on restart must preserve the unresolved delivery. Reviving the
    // native agent while still paused must not cause a second adapter call.
    let mut reconnect = crash_node(root, "reconnect").await?;
    let client = crash_client(root).await?;
    assert_eq!(client.whoami().await?, identity);
    online(&hub.admin, &enrolled.node_id).await?;
    let paused = member_when(&hub.admin, thread, &member, |m| {
        m["state"] == "paused_unknown"
    })
    .await?;
    assert_eq!(paused["delivery_generation"], 0);
    assert_eq!(read_fence(root, thread, &agent)?, ("unknown".into(), 0, 1));
    revive(&client, &agent, "reconnect").await?;
    participant_state(&hub.admin, &enrolled.node_id, &agent, "idle").await?;
    assert!(!root.join("native-reconnect").exists());
    let paused = member_when(&hub.admin, thread, &member, |m| {
        m["state"] == "paused_unknown"
    })
    .await?;
    assert_eq!(paused["confirmed_seq"], 0);
    stop_crash_node(&client, &mut reconnect, "stop-reconnect").await?;
    wait_for(async || {
        let nodes: Value = hub.admin.get_json("/v1/nodes", &[]).await?;
        Ok(items(&nodes)?
            .iter()
            .any(|node| node["node_id"] == enrolled.node_id.as_str() && node["status"] == "offline")
            .then_some(()))
    })
    .await?;

    let resume = format!("/v1/threads/{thread}/members/resume");
    let active = post(
        &hub.admin,
        &resume,
        &json!({"member_key":member,"after_seq":0}),
        "authorize-redelivery",
    )
    .await?;
    assert_eq!(active["state"], "active");
    assert_eq!(active["delivery_generation"], 1);
    let mut resumed = crash_node(root, "resumed").await?;
    let client = crash_client(root).await?;
    revive(&client, &agent, "resumed").await?;
    wait_for_marker(root, &mut resumed, "native-resumed").await?;
    member_when(&hub.admin, thread, &member, |m| m["confirmed_seq"] == 1).await?;
    assert_eq!(
        read_fence(root, thread, &agent)?,
        ("confirmed".into(), 1, 1)
    );
    stop_crash_node(&client, &mut resumed, "stop-resumed").await?;
    hub.stop().await?;
    Ok(())
}

async fn crash_node(root: &Path, pass: &str) -> TestResult<Process> {
    let log = fs::File::create(root.join(format!("log-{pass}")))?;
    let mut child = Process(
        Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "bridge_crash::bridge_crash_node",
                "--nocapture",
            ])
            .env(NODE_ROOT, root)
            .env(NODE_PASS, pass)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?,
    );
    wait_for_marker(root, &mut child, &format!("ready-{pass}")).await?;
    Ok(child)
}

async fn wait_for_marker(root: &Path, process: &mut Process, marker: &str) -> TestResult {
    let path = root.join(marker);
    wait_for(async || {
        if let Some(status) = process.0.try_wait()? {
            return Err(io::Error::other(format!(
                "Node subprocess ended with {status} before {marker}"
            ))
            .into());
        }
        Ok(path.exists().then_some(()))
    })
    .await
}

async fn crash_client(root: &Path) -> TestResult<CoopletClient> {
    let credential = Credential::read(&operator_path(&root.join("state")))?;
    Ok(credential.connect(PrincipalKind::Operator).await?.0)
}

async fn revive(client: &CoopletClient, agent: &ParticipantId, pass: &str) -> TestResult {
    let entity = format!("/v1/agents/{agent}");
    let stopped = post(
        client,
        &format!("{entity}/stop"),
        &json!({}),
        &format!("stop-agent-{pass}"),
    )
    .await?;
    assert_eq!(stopped["state"], "stopped");
    let resumed = post(
        client,
        &format!("{entity}/resume"),
        &json!({}),
        &format!("resume-agent-{pass}"),
    )
    .await?;
    assert_eq!(resumed["participant_id"], agent.as_str());
    Ok(())
}

fn read_fence(
    root: &Path,
    thread: &str,
    participant: &ParticipantId,
) -> TestResult<(String, u64, u64)> {
    let path = root.join("state/node.sqlite3");
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    Ok(connection.query_row(
        "SELECT state, delivery_generation, highest_dispatched_seq FROM thread_inject_fences WHERE thread_id = ?1 AND participant_id = ?2",
        [thread, participant.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?)
}

async fn stop_crash_node(client: &CoopletClient, process: &mut Process, key: &str) -> TestResult {
    let stop = post(client, "/v1/node/stop", &json!({}), key).await?;
    assert_eq!(stop["stopping"], true);
    let status = wait_for(async || Ok(process.0.try_wait()?)).await?;
    assert!(status.success(), "Node shutdown failed: {status}");
    Ok(())
}

#[tokio::test]
#[ignore = "started by the SIGKILL acceptance test"]
async fn bridge_crash_node() -> TestResult {
    let root = std::env::var_os(NODE_ROOT)
        .map(PathBuf::from)
        .ok_or("missing fixture root")?;
    let pass = std::env::var(NODE_PASS)?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockAdapter::default();
    let mut registry = AdapterRegistry::default();
    registry.register(Arc::new(mock.clone()))?;
    let node = Node::open(
        NodeConfig::new(root.join("state"), root.join("workspace")),
        registry,
    )
    .await?;
    let mut server = tokio::spawn(node.serve());
    fs::write(root.join(format!("ready-{pass}")), b"")?;
    let received = tokio::select! {
        result = &mut server => return result?,
        received = mock.next_submit() => received,
    };
    match received {
        Ok(submit) => {
            assert!(matches!(submit.request.input, TurnInput::Thread(_)));
            fs::File::create(root.join(format!("native-{pass}")))?.sync_all()?;
            if pass == "resumed" {
                submit.finish(SubmitOutcome::Accepted {
                    native_turn_id: None,
                })?;
            } else if pass == "reconnect" {
                return Err(
                    "an unresolved thread delivery reached the native adapter again".into(),
                );
            } else {
                // Keep the acknowledgment withheld until the parent sends SIGKILL.
                return server.await?.and_then(|()| {
                    drop(submit);
                    Err("Node stopped before SIGKILL".into())
                });
            }
        }
        Err(error) if pass != "reconnect" => return Err(error.into()),
        Err(_) => {}
    }
    server.await?
}
