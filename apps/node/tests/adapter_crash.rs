#[path = "common/mock_adapter.rs"]
mod mock_adapter;

use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use cooplet_client::{ClientError, CoopletClient};
use cooplet_node::{
    auth::Credential,
    clients::{AdapterRegistry, SubmitOutcome},
    config::{NodeConfig, operator_path},
    daemon::Node,
};
use cooplet_protocol::{ErrorCode, MessageId, ParticipantId, PrincipalKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use mock_adapter::MockAdapter;

type TestError = Box<dyn Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

const ROOT_ENV: &str = "COOPLET_TEST_CRASH_ROOT";
const PASS_ENV: &str = "COOPLET_TEST_CRASH_PASS";
const WAIT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(10);
const DIRECT: &str = "crash-test-direct";
const QUEUED: &str = "crash-test-queued";

struct Tree(PathBuf);

impl Tree {
    fn new() -> TestResult<Self> {
        let root =
            std::env::temp_dir().join(format!("cooplet-crash-{}", ParticipantId::generate()?));
        fs::create_dir_all(root.join("workspace"))?;
        fs::create_dir(root.join("queue-workspace"))?;
        Ok(Self(root))
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct TestProcess {
    child: Child,
    log: PathBuf,
}

impl TestProcess {
    fn spawn(root: &Path, pass: &str) -> TestResult<Self> {
        let log = root.join(format!("child-{pass}.log"));
        let output = fs::File::create(&log)?;
        let child = Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", "crash_helper", "--nocapture"])
            .env(ROOT_ENV, root)
            .env(PASS_ENV, pass)
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output)
            .spawn()?;
        Ok(Self { child, log })
    }

    async fn ready(&mut self, root: &Path, pass: &str) -> TestResult {
        let ready = root.join(format!("ready-{pass}"));
        tokio::time::timeout(WAIT, async {
            while !ready.is_file() {
                if let Some(status) = self.child.try_wait()? {
                    return Err(io::Error::other(format!(
                        "Child exited before readiness ({status}): {}",
                        fs::read_to_string(&self.log).unwrap_or_default(),
                    )));
                }
                tokio::time::sleep(POLL).await;
            }
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn await_exit(&mut self) -> TestResult {
        let status = tokio::time::timeout(WAIT, async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    return Ok::<_, io::Error>(status);
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await??;
        if !status.success() {
            return Err(io::Error::other(format!(
                "Child did not stop cleanly ({status}): {}",
                fs::read_to_string(&self.log).unwrap_or_default(),
            ))
            .into());
        }
        Ok(())
    }
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NativeReceipt {
    participant_id: ParticipantId,
    generation: u64,
    message_id: MessageId,
}

async fn connect(root: &Path) -> TestResult<CoopletClient> {
    let descriptor = Credential::read(&operator_path(&root.join("state")))?;
    Ok(descriptor.connect(PrincipalKind::Operator).await?.0)
}

async fn create(
    client: &CoopletClient,
    key: &str,
    workspace_dir: &Path,
) -> TestResult<ParticipantId> {
    let result: Value = client
        .post_json(
            "/v1/agents",
            &json!({
                "client_kind":"mock",
                "workspace_dir":workspace_dir,
            }),
            Some(key),
        )
        .await?;
    Ok(serde_json::from_value(result["participant_id"].clone())?)
}

fn read_native_receipts(root: &Path) -> TestResult<Vec<NativeReceipt>> {
    fs::read_to_string(root.join("native.jsonl"))?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

#[tokio::test]
async fn sigkill_preserves_unknown_without_repeating_direct_or_dispatched_queue() -> TestResult {
    let tree = Tree::new()?;
    let root = &tree.0;
    let mut first = TestProcess::spawn(root, "first")?;
    first.ready(root, "first").await?;
    let client = connect(root).await?;
    let direct_agent = create(&client, "create-direct", &root.join("workspace")).await?;
    let queued_agent = create(&client, "create-queued", &root.join("queue-workspace")).await?;
    let direct_request = json!({"to":direct_agent,"body":"direct before crash"});
    let queued_request = json!({
        "to":queued_agent,"body":"queue before crash","delivery_mode":"queue_until_idle",
    });

    let direct_client = client.clone();
    let pending_request = direct_request.clone();
    let pending = tokio::spawn(async move {
        direct_client
            .post_json::<_, Value>("/v1/messages", &pending_request, Some(DIRECT))
            .await
    });
    let queued: Value = client
        .post_json("/v1/messages", &queued_request, Some(QUEUED))
        .await?;
    assert_eq!(queued["delivery_state"], "queued");

    tokio::time::timeout(WAIT, async {
        while !(root.join(format!("written-{direct_agent}")).is_file()
            && root.join(format!("written-{queued_agent}")).is_file())
        {
            tokio::time::sleep(POLL).await;
        }
    })
    .await?;
    let first_receipts = read_native_receipts(root)?;
    assert_eq!(first_receipts.len(), 2);
    assert!(first_receipts.iter().all(|receipt| receipt.generation == 1));

    first.child.kill()?;
    let status = first.child.wait()?;
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    assert!(tokio::time::timeout(WAIT, pending).await??.is_err());

    let mut second = TestProcess::spawn(root, "second")?;
    second.ready(root, "second").await?;
    let client = connect(root).await?;
    for receipt in &first_receipts {
        let message: Value = client
            .get_json(&format!("/v1/messages/{}", receipt.message_id), &[])
            .await?;
        assert_eq!(message["delivery_state"], "unknown");
    }

    for (id, suffix) in [(&direct_agent, "direct"), (&queued_agent, "queued")] {
        let stopped: Value = client
            .post_json(
                &format!("/v1/agents/{id}/stop"),
                &json!({}),
                Some(&format!("stop-{suffix}")),
            )
            .await?;
        assert_eq!(stopped["state"], "stopped");
        let resumed: Value = client
            .post_json(
                &format!("/v1/agents/{id}/resume"),
                &json!({}),
                Some(&format!("resume-{suffix}")),
            )
            .await?;
        assert_eq!(resumed["participant_id"], id.as_str());
    }

    match client
        .post_json::<_, Value>("/v1/messages", &direct_request, Some(DIRECT))
        .await
    {
        Err(ClientError::Problem(problem)) => {
            assert_eq!(problem.code, ErrorCode::DeliveryUnknown);
            assert_eq!(problem.status, 503);
            assert!(!problem.retryable);
            let direct = first_receipts
                .iter()
                .find(|receipt| receipt.participant_id == direct_agent)
                .ok_or("Missing native direct receipt")?;
            assert_eq!(problem.extensions["message_id"], direct.message_id.as_str());
        }
        other => {
            return Err(io::Error::other(format!("Unexpected direct replay: {other:?}")).into());
        }
    }

    let replay: Value = client
        .post_json("/v1/messages", &queued_request, Some(QUEUED))
        .await?;
    assert_eq!(replay, queued);
    let stopping: Value = client
        .post_json("/v1/node/stop", &json!({}), Some("stop-restarted-node"))
        .await?;
    assert_eq!(stopping["stopping"], true);
    second.await_exit().await?;
    let final_receipts = read_native_receipts(root)?;
    assert_eq!(final_receipts.len(), 2);
    assert_eq!(
        final_receipts
            .iter()
            .filter(|receipt| receipt.participant_id == direct_agent)
            .count(),
        1
    );
    assert_eq!(
        final_receipts
            .iter()
            .filter(|receipt| receipt.participant_id == queued_agent)
            .count(),
        1
    );
    Ok(())
}

// The parent controls the crash boundary via the fsynced native receipt, then
// kills this separate process while both native acknowledgments are withheld.
#[tokio::test]
#[ignore = "spawned by the SIGKILL acceptance test"]
async fn crash_helper() -> TestResult {
    let root = std::env::var_os(ROOT_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("Missing crash fixture root"))?;
    let pass = std::env::var(PASS_ENV)?;
    let mock = MockAdapter::default();
    let mut registry = AdapterRegistry::default();
    registry.register(Arc::new(mock.clone()))?;
    let node = Node::open(
        NodeConfig::new(root.join("state"), root.join("workspace")),
        registry,
    )
    .await?;
    fs::write(root.join(format!("ready-{pass}")), [])?;
    tokio::select! {
        result = node.serve() => result,
        result = native_driver(&root, &mock, pass == "first") => result,
    }
}

async fn native_driver(root: &Path, mock: &MockAdapter, withhold_ack: bool) -> TestResult {
    let mut pending = Vec::new();
    loop {
        let submit = match mock.next_submit().await {
            Ok(submit) => submit,
            Err(error) if error.kind() == io::ErrorKind::Other => continue,
            Err(error) => return Err(error.into()),
        };
        if submit.request.admission.is_expired() {
            submit.finish(SubmitOutcome::Expired)?;
            continue;
        }
        let message_id = submit
            .request
            .message_id
            .clone()
            .ok_or_else(|| io::Error::other("Native request has no message ID"))?;
        let receipt = NativeReceipt {
            participant_id: submit.participant_id.clone(),
            generation: submit.generation,
            message_id,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("native.jsonl"))?;
        let mut line = serde_json::to_vec(&receipt)?;
        line.push(b'\n');
        file.write_all(&line)?;
        file.sync_all()?;
        fs::write(root.join(format!("written-{}", receipt.participant_id)), [])?;
        if withhold_ack {
            pending.push(submit);
        } else {
            submit.finish(SubmitOutcome::Accepted {
                native_turn_id: None,
            })?;
        }
    }
}
