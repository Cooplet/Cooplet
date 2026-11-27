#[path = "common/mock_adapter.rs"]
mod mock_adapter;
#[path = "common/test_node.rs"]
mod test_node;

use std::{fs, io, path::PathBuf, sync::Arc, time::Duration};

use cooplet_client::ClientError;
use cooplet_node::clients::{
    AdapterConfig, AdapterEventKind, AdapterRegistry, ApprovalAction, ApprovalDecision,
    ApprovalRespondOutcome, ApprovalWait, ApprovalWaitKind, ParticipantPage,
    ParticipantRuntimeState, ParticipantView, StopMode, SubmitOutcome, TuiAttachPlan,
    TurnFinishStatus,
};
use cooplet_protocol::{ErrorCode, ParticipantState};
use cooplet_storage::WriteOutcome;
use serde_json::{Value, json};

use mock_adapter::MockAdapter;
use test_node::{TestNode, TestResult};

const WAIT: Duration = Duration::from_secs(5);

fn present<T>(value: Option<T>) -> TestResult<T> {
    value.ok_or_else(|| io::Error::other("The expected fixture value is missing").into())
}

fn check_problem<T>(response: Result<T, ClientError>, expected: ErrorCode) -> TestResult {
    match present(response.err())? {
        ClientError::Problem(problem) => {
            assert_eq!(problem.code, expected, "{}", problem.detail);
            assert_eq!(problem.status, expected.status());
            Ok(())
        }
        other => Err(other.into()),
    }
}

fn change(state: ParticipantRuntimeState) -> AdapterEventKind {
    AdapterEventKind::StateChanged(state)
}

#[tokio::test]
async fn registry_probes_once_for_launches_and_rejects_duplicate_kind() -> TestResult {
    let mock = MockAdapter::default();
    let duplicate = MockAdapter::default();
    let mut registry = AdapterRegistry::default();
    registry.register(Arc::new(mock.clone()))?;
    assert!(registry.register(Arc::new(duplicate.clone())).is_err());
    assert!(registry.get("missing").is_err());

    let config = AdapterConfig {
        state_dir: PathBuf::new(),
        runtime_dir: PathBuf::new(),
    };
    let cloned = registry.clone();
    let (first, second) = tokio::join!(
        registry.discover("mock", &config),
        cloned.discover("mock", &config)
    );
    assert_eq!(first?, second?);
    assert_eq!(mock.discoveries(), 1);
    assert_eq!(duplicate.discoveries(), 0);
    assert_eq!(registry.discover_all(&config).await?.len(), 1);
    assert_eq!(mock.discoveries(), 2);
    Ok(())
}

#[tokio::test]
async fn create_stop_and_resume_preserve_native_identity_and_rotate_credentials() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let template_dir = node.workspace_dir.join(".cooplet/agents/developer");
    fs::create_dir_all(&template_dir)?;
    fs::write(
        template_dir.join("agent.json"),
        br#"{"schema":1,"client":"mock","model":"default","effort":"medium"}"#,
    )?;
    fs::write(
        template_dir.join("instructions.md"),
        "Keep the workspace clean",
    )?;
    let first = node
        .create(
            "create-template",
            json!({
                "name":"first", "template_id":"developer", "overrides": {"model":"operator"},
            }),
        )
        .await?;
    let initial = present(mock.session(&first, 1))?;
    let agent_client = node.agent_client(&initial).await?;
    assert_eq!(
        agent_client.whoami().await?.participant_id.as_ref(),
        Some(&first)
    );
    assert_eq!(initial.options.model.as_deref(), Some("operator"));
    assert_eq!(initial.options.effort.as_deref(), Some("medium"));
    assert_eq!(
        initial.instructions.as_deref(),
        Some("Keep the workspace clean")
    );
    assert_eq!(initial.template_id.as_deref(), Some("developer"));
    assert_eq!(
        initial.target.adapter_binding,
        json!({"overrides":"adapter-owned"})
    );

    let conflict: Result<Value, _> = node
        .client
        .post_json(
            "/v1/agents",
            &json!({
                "name":"second", "client_kind":"mock",
            }),
            Some("conflicting-native-identity"),
        )
        .await;
    check_problem(conflict, ErrorCode::ParticipantBusy)?;
    let second_workspace = node.root.join("another-workspace");
    fs::create_dir(&second_workspace)?;
    let second = node
        .create(
            "create-second",
            json!({
                "name":"second", "client_kind":"mock", "workspace_dir":second_workspace,
            }),
        )
        .await?;
    let second_native = present(present(mock.session(&second, 1))?.target.native_key)?;
    let first_native = present(initial.target.native_key.clone())?;
    assert_eq!(
        first_native.native_session_id,
        second_native.native_session_id
    );
    assert_ne!(first_native.store_namespace, second_native.store_namespace);
    assert_eq!(mock.discoveries(), 1);

    let stop_path = format!("/v1/agents/{first}/stop");
    let cancelled: Value = node
        .client
        .post_json(
            &stop_path,
            &json!({"cancel_turn_only": true}),
            Some("cancel-turn"),
        )
        .await?;
    assert_eq!(cancelled["stop_outcome"], "turn_cancelled");
    assert_eq!(
        agent_client.whoami().await?.participant_id.as_ref(),
        Some(&first)
    );
    let stopped: Value = node
        .client
        .post_json(&stop_path, &json!({}), Some("stop-first"))
        .await?;
    assert_eq!(stopped["state"], "stopped");
    assert_eq!(mock.stop_modes(), [StopMode::CancelTurn]);
    check_problem(agent_client.whoami().await, ErrorCode::Unauthorized)?;

    let attach_plan: TuiAttachPlan = node
        .client
        .get_json(&format!("/v1/participants/{first}/attach-plan"), &[])
        .await?;
    assert_eq!(
        attach_plan,
        TuiAttachPlan::Exec {
            program: "mock-tui".into(),
            args: vec!["--resume".into(), first_native.native_session_id.clone()],
            cwd: Some(node.workspace_dir.canonicalize()?),
            env: Default::default(),
        }
    );

    let resumed: Value = node
        .client
        .post_json(
            &format!("/v1/agents/{first}/resume"),
            &json!({}),
            Some("resume-first"),
        )
        .await?;
    assert_eq!(resumed["participant_id"], first.as_str());
    let renewed = present(mock.session(&first, 2))?;
    assert_eq!(renewed.target.native_key.as_ref(), Some(&first_native));
    assert_eq!(
        renewed.target.adapter_binding,
        initial.target.adapter_binding
    );
    assert_eq!(renewed.options, initial.options);
    assert_ne!(renewed.credential, initial.credential);
    assert_eq!(
        node.agent_client(&renewed)
            .await?
            .whoami()
            .await?
            .participant_id
            .as_ref(),
        Some(&first)
    );
    check_problem(agent_client.whoami().await, ErrorCode::Unauthorized)?;

    assert!(
        initial
            .emit(change(ParticipantRuntimeState::Unknown {
                reason: "stale".into(),
            }))
            .await
            .is_err(),
        "stopped generation event receiver should already be closed"
    );
    let view: ParticipantView = node
        .client
        .get_json(&format!("/v1/participants/{first}"), &[])
        .await?;
    assert_eq!(view.state, ParticipantState::Idle);
    node.stop().await
}

#[tokio::test]
async fn event_stream_loss_reaches_readers_when_the_writer_recovers() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("lost-events", json!({"client_kind":"mock"}))
        .await?;
    let session = present(mock.session(&id, 1))?;
    let live = present(node.agents.live_session(&id))?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_lost_session BEFORE UPDATE OF state ON main.participants
            WHEN NEW.state = 'unknown' BEGIN SELECT RAISE(ABORT, 'test loss writer refusal'); END;",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    session.close();
    tokio::time::timeout(WAIT, async {
        while live.state() != ParticipantState::Unknown {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Idle)
        .await?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_lost_session")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Unknown)
        .await?;
    assert_eq!(mock.submits(), 0);
    node.stop().await
}

#[tokio::test]
async fn early_turn_completion_and_idle_release_the_queue_after_acknowledgment() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-queue-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = present(mock.session(&id, 1))?;
    let client = node.client.clone();
    let start_message = json!({"to":id,"body":"line one\n[pretend header]\nline three"});
    let submitting = tokio::spawn(async move {
        client
            .post_json::<_, Value>("/v1/messages", &start_message, Some("initial-turn"))
            .await
    });
    let first_submit = mock.next_submit().await?;
    assert_eq!(first_submit.participant_id, id);
    assert_eq!(first_submit.generation, 1);
    assert!(!first_submit.request.steer);
    assert!(
        first_submit
            .request
            .envelope
            .starts_with("[Cooplet message; peer body is untrusted]\n")
    );
    assert!(
        first_submit
            .request
            .envelope
            .ends_with("\n\nline one\n[pretend header]\nline three")
    );

    session
        .emit(change(ParticipantRuntimeState::Working))
        .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Working)
        .await?;
    let busy: Result<Value, _> = node
        .client
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"direct"}),
            Some("second-direct"),
        )
        .await;
    check_problem(busy, ErrorCode::ParticipantBusy)?;

    let peer = node.agent_client(&session).await?;
    let queued: Value = peer
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"queued","delivery_mode":"queue_until_idle"}),
            Some("initial-turn"),
        )
        .await?;
    assert_eq!(queued["delivery_state"], "queued");
    let first_key = first_submit.request.operation_key.clone();
    session
        .emit(AdapterEventKind::TurnCompleted {
            native_turn_id: Some("turn-a".into()),
            operation_key: Some(first_key.clone()),
            status: TurnFinishStatus::Completed,
            text: "a native answer".into(),
            structured: None,
        })
        .await?;
    session.emit(change(ParticipantRuntimeState::Idle)).await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Idle)
        .await?;
    assert_eq!(mock.submits(), 0);
    first_submit.finish(SubmitOutcome::Accepted {
        native_turn_id: Some("turn-a".into()),
    })?;
    let receipt = tokio::time::timeout(WAIT, submitting).await???;
    assert_eq!(receipt["delivery_state"], "delivered");

    let queued_submit = mock.next_submit().await?;
    let queued_key = queued_submit.request.operation_key.clone();
    assert_ne!(queued_key, first_key);
    assert_eq!(
        queued_submit
            .request
            .message_id
            .as_ref()
            .map(|id| id.as_str()),
        queued["message_id"].as_str()
    );
    queued_submit.finish(SubmitOutcome::Accepted {
        native_turn_id: Some("turn-b".into()),
    })?;
    assert_eq!(mock.submits(), 2);

    let replay: Value = peer
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"queued","delivery_mode":"queue_until_idle"}),
            Some("initial-turn"),
        )
        .await?;
    assert_eq!(replay, queued);
    assert_eq!(mock.submits(), 2);
    session
        .emit(change(ParticipantRuntimeState::Working))
        .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Working)
        .await?;
    let next: Value = node
        .client
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"next","delivery_mode":"queue_until_idle"}),
            Some("next-turn"),
        )
        .await?;
    let completion = |key: String| AdapterEventKind::TurnCompleted {
        native_turn_id: None,
        operation_key: Some(key),
        status: TurnFinishStatus::Completed,
        text: String::new(),
        structured: None,
    };
    // Both authors used the same HTTP key. A late event for the first author's
    // turn must leave the second author's slot occupied, even after a fresh Idle.
    session.emit(completion(first_key)).await?;
    session.emit(change(ParticipantRuntimeState::Idle)).await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Idle)
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), mock.next_submit())
            .await
            .is_err()
    );
    session.emit(completion(queued_key)).await?;
    let next_submit = mock.next_submit().await?;
    assert_eq!(next_submit.request.operation_key, next["message_id"]);
    next_submit.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    node.stop().await
}

fn waiting(native: &str, remote: bool) -> AdapterEventKind {
    AdapterEventKind::ApprovalRequested(ApprovalWait {
        approval_id: Some(native.into()),
        kind: ApprovalWaitKind::Permission,
        summary: Some("tool permission".into()),
        can_respond_remotely: remote,
        action_hint: Some("\"\n".repeat(40_000)),
    })
}

fn decide(approval_id: String) -> ApprovalDecision {
    ApprovalDecision {
        approval_id,
        action: ApprovalAction::Approve,
    }
}

#[tokio::test]
async fn create_replays_explicit_default_overrides() -> TestResult {
    let mut node = TestNode::start(MockAdapter::default()).await?;
    let key = "create-default-overrides";
    let mut request = json!({"client_kind":"mock","name":"defaults"});
    let created: Value = node
        .client
        .post_json("/v1/agents", &request, Some(key))
        .await?;
    for defaults in [json!({}), json!({"model":null,"approval":null})] {
        request["overrides"] = defaults;
        let replay: Value = node
            .client
            .post_json("/v1/agents", &request, Some(key))
            .await?;
        assert_eq!(replay, created);
    }
    request["overrides"] = json!({"model":"different"});
    check_problem(
        node.client
            .post_json::<_, Value>("/v1/agents", &request, Some(key))
            .await,
        ErrorCode::OperationConflict,
    )?;
    node.stop().await
}

#[tokio::test]
async fn stop_recovers_an_open_left_starting_after_rollback_fails() -> TestResult {
    let mut node = TestNode::start(MockAdapter::default()).await?;
    node.create(
        "existing-native-session",
        json!({"client_kind":"mock","name":"existing"}),
    )
    .await?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_open_rollback
             BEFORE UPDATE OF state ON main.participants
             WHEN OLD.state = 'starting' AND NEW.state = 'stopped'
             BEGIN SELECT RAISE(ABORT, 'test open rollback refusal'); END;",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let input = json!({"client_kind":"mock","name":"interrupted-open"});
    let create_key = "open-without-final-commit";
    check_problem(
        node.client
            .post_json::<_, Value>("/v1/agents", &input, Some(create_key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    let page: ParticipantPage = node.client.get_json("/v1/participants", &[]).await?;
    let agent = present(
        page.items
            .into_iter()
            .find(|view| view.name == "interrupted-open"),
    )?;
    assert_eq!(agent.state, ParticipantState::Starting);
    let id = agent.participant_id;
    assert!(
        !node
            .state_dir
            .join("creds")
            .join(format!("{id}.g1.json"))
            .try_exists()?
    );

    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_open_rollback")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let stop_path = format!("/v1/agents/{id}/stop");
    let unverified: Value = node
        .client
        .post_json(&stop_path, &json!({}), Some("stop-interrupted-open"))
        .await?;
    assert_eq!(unverified["state"], "unknown");
    assert_eq!(unverified["stop_outcome"], "cleanup_unverified");
    let participant = id.clone();
    let active_credentials: u64 = node.reads.read(move |conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM participant_credentials WHERE participant_id = ?1 AND revoked_at_ms IS NULL",
            [participant.as_str()],
            |row| row.get(0),
        )?)
    }).await?;
    assert_eq!(active_credentials, 0);
    let acknowledged: Value = node
        .client
        .post_json(
            &stop_path,
            &json!({"acknowledge_manual_cleanup":true}),
            Some("acknowledge-interrupted-open"),
        )
        .await?;
    assert_eq!(acknowledged["state"], "stopped");
    assert_eq!(acknowledged["stop_outcome"], "acknowledged_manual_cleanup");
    let workspace = node.root.join("different-workspace");
    fs::create_dir(&workspace)?;
    let replacement = node
        .create(
            "replace-interrupted-open",
            json!({"client_kind":"mock","name":"interrupted-open","workspace_dir":workspace}),
        )
        .await?;
    assert_ne!(replacement, id);
    check_problem(
        node.client
            .post_json::<_, Value>("/v1/agents", &input, Some(create_key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    node.stop().await
}

#[tokio::test]
async fn stop_can_continue_after_an_unconfirmed_final_commit() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-stop-fault-agent", json!({"client_kind":"mock"}))
        .await?;
    let path = format!("/v1/agents/{id}/stop");
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_stop_receipt
             BEFORE UPDATE OF state ON main.participants
             WHEN OLD.state = 'stopping' AND NEW.state = 'stopped'
             BEGIN SELECT RAISE(ABORT, 'test stop receipt refusal'); END;",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let key = "stop-without-final-commit";
    check_problem(
        node.client
            .post_json::<_, Value>(&path, &json!({}), Some(key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    assert!(node.agents.live_session(&id).is_none());
    let view: ParticipantView = node
        .client
        .get_json(&format!("/v1/participants/{id}"), &[])
        .await?;
    assert_eq!(view.state, ParticipantState::Stopping);
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_stop_receipt")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    check_problem(
        node.client
            .post_json::<_, Value>(&path, &json!({}), Some(key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    let finished: Value = node
        .client
        .post_json(&path, &json!({}), Some("continue-stop"))
        .await?;
    assert_eq!(finished["state"], "stopped");
    assert_eq!(finished["stop_outcome"], "stopped");
    let resumed: Value = node
        .client
        .post_json(
            &format!("/v1/agents/{id}/resume"),
            &json!({}),
            Some("resume-after-continuing-stop"),
        )
        .await?;
    assert_eq!(resumed["participant_id"], id.as_str());
    assert!(mock.session(&id, 2).is_some());
    node.stop().await
}

#[tokio::test]
async fn approval_uses_public_ids_and_replays_after_the_pending_wait_changes() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-approval-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = present(mock.session(&id, 1))?;
    let participant_path = format!("/v1/participants/{id}");
    let approval_path = format!("{participant_path}/approval");
    let agent = node.agent_client(&session).await?;
    let native_id = "opaque-native-A-".repeat(300);
    session.emit(waiting(&native_id, false)).await?;
    let disabled = node
        .participant_when(&id, |view| view.can_respond_remotely == Some(false))
        .await?;
    let public = present(disabled.approval_id)?;
    assert!(public.starts_with("apr_") && public.len() <= 32);
    session.emit(waiting(&native_id, true)).await?;
    let enabled = node
        .participant_when(&id, |view| view.can_respond_remotely == Some(true))
        .await?;
    assert_eq!(enabled.approval_id.as_deref(), Some(public.as_str()));
    assert_eq!(enabled.action_hint.as_ref().map(String::len), Some(512));

    let visible: ParticipantView = agent.get_json(&participant_path, &[]).await?;
    assert_eq!(visible.state, ParticipantState::AwaitingApproval);
    assert_eq!(visible.approval_id, None);
    assert_eq!(visible.action_hint, None);
    let unauthorized: Result<Value, _> = agent
        .post_json(
            &approval_path,
            &decide(public.clone()),
            Some("agent-approval"),
        )
        .await;
    check_problem(unauthorized, ErrorCode::Forbidden)?;

    let request = decide(public.clone());
    let client = node.client.clone();
    let path = approval_path.clone();
    let reply = tokio::spawn(async move {
        client
            .post_json::<_, Value>(&path, &request, Some("operator-approval"))
            .await
    });
    let calling = mock.next_approval().await?;
    assert_eq!(calling.participant_id, id);
    assert_eq!(calling.generation, 1);
    assert_eq!(calling.decision.approval_id, native_id);
    assert_eq!(calling.decision.action, ApprovalAction::Approve);

    session.emit(waiting("native-B", true)).await?;
    let next = node
        .participant_when(&id, |view| {
            view.approval_id.as_ref().is_some_and(|id| id != &public)
        })
        .await?;
    let next_request = decide(present(next.approval_id)?);
    let busy: Result<Value, _> = node
        .client
        .post_json(&approval_path, &next_request, Some("busy-approval"))
        .await;
    check_problem(busy, ErrorCode::ParticipantBusy)?;

    session
        .emit(change(ParticipantRuntimeState::Working))
        .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Working)
        .await?;
    calling.finish(ApprovalRespondOutcome::Accepted)?;
    let first = tokio::time::timeout(WAIT, reply).await???;
    let replay: Value = node
        .client
        .post_json(&approval_path, &decide(public), Some("operator-approval"))
        .await?;
    assert_eq!(replay, first);

    session.emit(waiting("native-C", true)).await?;
    let next = node
        .participant_when(&id, |view| view.can_respond_remotely == Some(true))
        .await?;
    let next_request = decide(present(next.approval_id)?);
    let client = node.client.clone();
    let path = approval_path.clone();
    let request = next_request.clone();
    let reply = tokio::spawn(async move {
        client
            .post_json::<_, Value>(&path, &request, Some("unknown-approval"))
            .await
    });
    let calling = mock.next_approval().await?;
    assert_eq!(calling.decision.approval_id, "native-C");
    calling.finish(ApprovalRespondOutcome::Unknown(
        "Native ACK was lost".into(),
    ))?;
    check_problem(
        tokio::time::timeout(WAIT, reply).await??,
        ErrorCode::DeliveryUnknown,
    )?;
    let no_replay: Result<Value, _> = node
        .client
        .post_json(&approval_path, &next_request, Some("new-key-approval"))
        .await;
    check_problem(no_replay, ErrorCode::DeliveryUnknown)?;
    node.stop().await
}

#[tokio::test]
async fn approval_receipt_recovers_from_a_failed_final_commit() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-approval-fault-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = present(mock.session(&id, 1))?;
    let path = format!("/v1/participants/{id}/approval");
    let key = "approval-receipt-fault";
    session.emit(waiting("native-interrupted", true)).await?;
    let view = node
        .participant_when(&id, |view| view.can_respond_remotely == Some(true))
        .await?;
    let request = decide(present(view.approval_id)?);
    let pending = {
        let client = node.client.clone();
        let path = path.clone();
        let request = request.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, Value>(&path, &request, Some(key))
                .await
        })
    };
    let native = mock.next_approval().await?;
    assert_eq!(native.decision.approval_id, "native-interrupted");
    check_problem(
        node.client
            .post_json::<_, Value>(&path, &request, Some(key))
            .await,
        ErrorCode::OperationInProgress,
    )?;
    check_problem(
        node.client
            .post_json::<_, Value>(&path, &request, Some("approval-live-other-key"))
            .await,
        ErrorCode::ParticipantBusy,
    )?;

    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_approval_receipt
             BEFORE UPDATE OF state ON main.operations
             WHEN OLD.kind = 'approval.respond'
             BEGIN SELECT RAISE(ABORT, 'test receipt writer refusal'); END;",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    native.finish(ApprovalRespondOutcome::Accepted)?;
    check_problem(
        tokio::time::timeout(WAIT, pending).await??,
        ErrorCode::DeliveryUnknown,
    )?;
    check_problem(
        node.client
            .post_json::<_, Value>(&path, &request, Some(key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_approval_receipt")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    check_problem(
        node.client
            .post_json::<_, Value>(&path, &request, Some(key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    check_problem(
        node.client
            .post_json::<_, Value>(&path, &request, Some("approval-recovered-other-key"))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    let state: String = node
        .reads
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT state FROM operations WHERE operation_key = ?1",
                [key],
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(state, "unknown");

    session.emit(waiting("native-next", true)).await?;
    let next = node
        .participant_when(&id, |view| {
            view.approval_id
                .as_deref()
                .is_some_and(|id| id != request.approval_id)
        })
        .await?;
    let next_request = decide(present(next.approval_id)?);
    let client = node.client.clone();
    let next_path = path.clone();
    let reply = tokio::spawn(async move {
        client
            .post_json::<_, Value>(&next_path, &next_request, Some("approval-next"))
            .await
    });
    let native = mock.next_approval().await?;
    assert_eq!(native.decision.approval_id, "native-next");
    native.finish(ApprovalRespondOutcome::Accepted)?;
    tokio::time::timeout(WAIT, reply).await???;
    let mut old = request;
    old.action = ApprovalAction::Deny;
    check_problem(
        node.client
            .post_json::<_, Value>(&path, &old, Some("older-unknown-approval"))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    node.stop().await
}

#[tokio::test]
async fn stop_abandons_inflight_approval_and_cancel_turn_only_rejects_unknown_session() -> TestResult
{
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-abandon-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = present(mock.session(&id, 1))?;

    // 1. Seed a participant with a stuck in-flight approval_response and pending operation,
    //    then issue `stop`: stop must transition both approval_response and the operation
    //    to `unknown` atomically so neither stays stuck in `in_flight`/`in_progress`.
    let pid = id.clone();
    let op_id = cooplet_protocol::OperationId::generate()?;
    let seed_op = op_id.clone();
    node.db
        .write_sqlite(move |tx| {
            let binding = json!({
                "core": {
                    "pending_approval": {
                        "public_id": "apr_stuck",
                        "native_id": "nat_stuck",
                        "kind": "permission",
                        "summary": null,
                        "action_hint": null,
                        "can_respond_remotely": true
                    },
                    "approval_response": {
                        "public_id": "apr_stuck",
                        "operation_id": seed_op,
                        "state": "in_flight"
                    }
                },
                "adapter": {"overrides": "adapter-owned"}
            });
            tx.execute(
                "UPDATE participants SET state = 'awaiting_approval', adapter_binding_json = ?2
                 WHERE participant_id = ?1",
                rusqlite::params![pid.as_str(), binding.to_string()],
            )?;
            tx.execute(
                "INSERT INTO operations(operation_id, actor_id, operation_key, kind, request_sha256,
                                        state, resource_id, created_at_ms, updated_at_ms, expires_at_ms)
                 VALUES (?1, 'operator', 'stuck-apr-key', 'approval.respond', 'hash',
                         'in_progress', ?2, 1, 1, 9999999999999)",
                rusqlite::params![seed_op.as_str(), pid.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let stopped: Value = node
        .client
        .post_json(
            &format!("/v1/agents/{id}/stop"),
            &json!({}),
            Some("stop-abandons-apr"),
        )
        .await?;
    assert_eq!(stopped["state"], "stopped");
    let pid = id.clone();
    let check_op = op_id.clone();
    let (binding_raw, op_state): (String, String) = node
        .reads
        .read(move |conn| {
            let binding: String = conn.query_row(
                "SELECT adapter_binding_json FROM participants WHERE participant_id = ?1",
                [pid.as_str()],
                |row| row.get(0),
            )?;
            let op_state: String = conn.query_row(
                "SELECT state FROM operations WHERE operation_id = ?1",
                [check_op.as_str()],
                |row| row.get(0),
            )?;
            Ok((binding, op_state))
        })
        .await?;
    let binding: Value = serde_json::from_str(&binding_raw)?;
    assert_eq!(binding["core"]["approval_response"]["state"], "unknown");
    assert!(binding["core"].get("pending_approval").is_none());
    assert_eq!(op_state, "unknown");

    // 2. Resume into generation 2, force session to unknown via event stream close,
    //    and verify `cancel_turn_only: true` rejects with 409 participant_busy without
    //    calling the adapter.
    let _: Value = node
        .client
        .post_json(
            &format!("/v1/agents/{id}/resume"),
            &json!({}),
            Some("resume-for-cancel"),
        )
        .await?;
    let gen2 = present(mock.session(&id, 2))?;
    gen2.close();
    node.participant_when(&id, |view| view.state == ParticipantState::Unknown)
        .await?;
    let modes_before = mock.stop_modes().len();
    check_problem(
        node.client
            .post_json::<_, Value>(
                &format!("/v1/agents/{id}/stop"),
                &json!({"cancel_turn_only": true}),
                Some("cancel-unknown-session"),
            )
            .await,
        ErrorCode::ParticipantBusy,
    )?;
    assert_eq!(mock.stop_modes().len(), modes_before);
    let _ = session;
    node.stop().await
}
