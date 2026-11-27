#[path = "common/mock_adapter.rs"]
mod mock_adapter;
#[path = "common/test_node.rs"]
mod test_node;

use std::time::Duration;

use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, ClientError, CoopletClient, UploadVerification,
};
use cooplet_node::{
    clients::{
        AdapterEventKind, ApprovalWait, ApprovalWaitKind, ParticipantRuntimeState, SubmitOutcome,
        TurnInput,
    },
    delivery::{MessageView, SendMessageResponse},
};
use cooplet_protocol::{
    AuthorKind, DeliveryState, ErrorCode, MessageId, ParticipantState, ProblemDetails,
};
use cooplet_storage::WriteOutcome;
use serde_json::{Value, json};

use mock_adapter::MockAdapter;
use test_node::{TestNode, TestResult};
const WAIT: Duration = Duration::from_secs(5);

fn problem<T>(
    response: Result<T, ClientError>,
    expected: ErrorCode,
) -> TestResult<Box<ProblemDetails>> {
    match response {
        Err(ClientError::Problem(problem)) => {
            assert_eq!(problem.code, expected, "{}", problem.detail);
            assert_eq!(problem.status, expected.status());
            Ok(problem)
        }
        Err(other) => Err(other.into()),
        Ok(_) => Err(format!("Expected {expected}").into()),
    }
}

async fn message_state(
    client: &CoopletClient,
    id: &MessageId,
    state: DeliveryState,
) -> TestResult<MessageView> {
    let path = format!("/v1/messages/{id}");
    tokio::time::timeout(WAIT, async {
        loop {
            let message: MessageView = client.get_json(&path, &[]).await?;
            if message.delivery_state == state {
                return Ok(message);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn adapter_receives_readable_attachment_paths_for_both_delivery_modes() -> TestResult {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::Path};

    for state_name in [OsStr::new("state"), OsStr::from_bytes(b"state-\xff")] {
        for mode in ["direct_now", "queue_until_idle"] {
            let mock = MockAdapter::default();
            let mut node = TestNode::start_with_state_name(mock.clone(), state_name).await?;
            let recipient = node
                .create("attachment-target", json!({"client_kind":"mock"}))
                .await?;
            let spec = BlobUploadSpec {
                filename: "file.txt".into(),
                mime_type: "text/plain".into(),
                verification: UploadVerification::LocalSha256,
                expected_sha256: None,
                idempotency_key: Some("attachment-upload".into()),
                content_length: None,
            };
            let BlobUploadOutcome::VerifiedLocal(attachment) =
                node.client.upload_blob(spec, b"hello".as_slice()).await?
            else {
                return Err("Expected a verified attachment".into());
            };
            let input = json!({"to":recipient,"body":"Read the file","delivery_mode":mode,"attachments":[attachment]});
            let client = node.client.clone();
            let submit = tokio::spawn(async move {
                client
                    .post_json::<_, SendMessageResponse>(
                        "/v1/messages",
                        &input,
                        Some("send-attachment"),
                    )
                    .await
            });
            let held = mock.next_submit().await?;
            assert!(held.request.input.is_direct_human());
            let TurnInput::Direct(ref inbound) = held.request.input else {
                return Err("Expected direct TurnInput".into());
            };
            assert_eq!(inbound.author.kind, AuthorKind::Human);
            assert_eq!(inbound.attachments.len(), 1);
            assert_eq!(inbound.attachments[0].blob_id, attachment.blob_id);

            let rendered = held.request.input.render_canonical()?;
            let meta_line = rendered
                .lines()
                .nth(2)
                .ok_or("Missing bounded metadata line")?;
            let meta: Value = serde_json::from_str(meta_line)?;
            let file = &meta["attachments"][0];
            assert_eq!(file["blob_id"], attachment.blob_id.as_str());
            let path = if state_name.to_str().is_some() {
                assert!(file.get("local_path_bytes").is_none());
                Path::new(file["local_path"].as_str().ok_or("Missing UTF-8 path")?).to_owned()
            } else {
                assert_eq!(file.get("local_path"), Some(&Value::Null));
                let bytes: Vec<u8> = serde_json::from_value(file["local_path_bytes"].clone())?;
                Path::new(OsStr::from_bytes(&bytes)).to_owned()
            };
            assert!(path.starts_with(node.state_dir.canonicalize()?.join("blobs")));
            assert_eq!(std::fs::read(path)?, b"hello");
            held.finish(SubmitOutcome::Accepted {
                native_turn_id: None,
            })?;
            let receipt = submit.await??;
            let expected = if mode == "direct_now" {
                DeliveryState::Delivered
            } else {
                DeliveryState::Queued
            };
            assert_eq!(receipt.delivery_state, expected);
            assert_eq!(mock.submits(), 1);
            node.stop().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn delivery_replays_receipts_and_does_not_repeat_an_unknown_effect() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-delivery-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = mock
        .session(&recipient, 1)
        .ok_or("Missing opened session")?;
    let input = json!({"to": recipient, "body": "Review this change"});
    let key = "delivery-accepted";
    let first = {
        let client = node.client.clone();
        let input = input.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, SendMessageResponse>("/v1/messages", &input, Some(key))
                .await
        })
    };
    let held = mock.next_submit().await?;
    let delivery_id = held
        .request
        .message_id
        .clone()
        .ok_or("Missing native message correlation")?;
    assert_eq!(held.request.operation_key, delivery_id.as_str());
    assert_eq!(held.participant_id, recipient);
    let inflight = problem(
        node.client
            .post_json::<_, Value>("/v1/messages", &input, Some(key))
            .await,
        ErrorCode::OperationInProgress,
    )?;
    assert!(inflight.extensions.contains_key("operation_id"));
    problem(
        node.client
            .post_json::<_, Value>(
                "/v1/messages",
                &json!({"to": recipient, "body": "Changed"}),
                Some(key),
            )
            .await,
        ErrorCode::OperationConflict,
    )?;

    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Idle)
        .await?;
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    let receipt = tokio::time::timeout(WAIT, first).await???;
    assert_eq!(receipt.message_id.as_ref(), Some(&delivery_id));
    assert_eq!(receipt.delivery_state, DeliveryState::Delivered);
    let replay: SendMessageResponse = node.client.post_json(
        "/v1/messages",
        &json!({"body":"Review this change", "to": recipient, "delivery_mode":"direct_now", "attachments":[]}),
        Some(key),
    ).await?;
    assert_eq!(replay, receipt);
    assert_eq!(mock.submits(), 1);

    let key = "delivery-unknown";
    let unknown = {
        let client = node.client.clone();
        let input = input.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, SendMessageResponse>("/v1/messages", &input, Some(key))
                .await
        })
    };
    let held = mock.next_submit().await?;
    let unknown_id = held
        .request
        .message_id
        .clone()
        .ok_or("Missing unknown message correlation")?;
    held.finish(SubmitOutcome::Unknown("native acknowledgement lost".into()))?;
    let first_problem = problem(
        tokio::time::timeout(WAIT, unknown).await??,
        ErrorCode::DeliveryUnknown,
    )?;
    let replay_problem = problem(
        node.client
            .post_json::<_, Value>("/v1/messages", &input, Some(key))
            .await,
        ErrorCode::DeliveryUnknown,
    )?;
    assert_eq!(first_problem.extensions, replay_problem.extensions);
    assert_eq!(replay_problem.extensions["message_id"], unknown_id.as_str());
    message_state(&node.client, &unknown_id, DeliveryState::Unknown).await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Unknown)
        .await?;
    assert_eq!(mock.submits(), 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), mock.next_submit())
            .await
            .is_err()
    );
    node.stop().await
}

#[tokio::test]
async fn queue_expires_the_oldest_and_waits_for_fresh_idle_after_busy() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-queued-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = mock
        .session(&recipient, 1)
        .ok_or("Missing opened session")?;
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;

    let input =
        json!({"to": recipient, "body": "queued input", "delivery_mode": "queue_until_idle"});
    let first: SendMessageResponse = node
        .client
        .post_json("/v1/messages", &input, Some("queued-entry-0"))
        .await?;
    let second: SendMessageResponse = node
        .client
        .post_json("/v1/messages", &input, Some("queued-entry-1"))
        .await?;
    let first_id = first
        .message_id
        .clone()
        .ok_or("Missing first queued message")?;
    let second_id = second
        .message_id
        .clone()
        .ok_or("Missing second queued message")?;
    for index in 2..63 {
        let receipt: SendMessageResponse = node
            .client
            .post_json(
                "/v1/messages",
                &input,
                Some(&format!("queued-entry-{index}")),
            )
            .await?;
        assert_eq!(receipt.delivery_state, DeliveryState::Queued);
    }

    let (a, b) = tokio::join!(
        node.client.post_json::<_, SendMessageResponse>(
            "/v1/messages",
            &input,
            Some("queued-last-a")
        ),
        node.client.post_json::<_, SendMessageResponse>(
            "/v1/messages",
            &input,
            Some("queued-last-b")
        ),
    );
    let (last, rejected, full) = match (a, b) {
        (Ok(last), Err(full)) => (last, "queued-last-b", full),
        (Err(full), Ok(last)) => (last, "queued-last-a", full),
        _ => return Err("The last slot was not assigned to exactly one request".into()),
    };
    assert_eq!(last.delivery_state, DeliveryState::Queued);
    let full = problem::<SendMessageResponse>(Err(full), ErrorCode::RateLimited)?;
    assert_eq!(full.extensions["reason"], "queue_full");
    assert_eq!(mock.submits(), 0);

    // Move the persisted deadline to the current boundary while the adapter
    // is working. A later Idle must skip that entry before touching the wire.
    let expired = first_id.clone();
    node.db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE intake_queue SET deadline_at_ms = enqueued_at_ms WHERE message_id = ?1",
                [expired.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let busy = mock.next_submit().await?;
    assert_eq!(busy.request.message_id.as_ref(), Some(&second_id));
    busy.finish(SubmitOutcome::Busy)?;
    message_state(&node.client, &first_id, DeliveryState::Expired).await?;
    let replay: SendMessageResponse = node.client.post_json(
        "/v1/messages",
        &json!({"to": recipient, "body":"queued input", "delivery_mode":"queue_until_idle", "queue_ttl_ms":300_000}),
        Some("queued-entry-0"),
    ).await?;
    assert_eq!(first, replay);
    let added: SendMessageResponse = node
        .client
        .post_json("/v1/messages", &input, Some(rejected))
        .await?;
    assert_eq!(added.delivery_state, DeliveryState::Queued);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), mock.next_submit())
            .await
            .is_err()
    );

    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let retried = mock.next_submit().await?;
    assert_eq!(retried.request.message_id.as_ref(), Some(&second_id));
    retried.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    message_state(&node.client, &second_id, DeliveryState::Delivered).await?;
    assert_eq!(mock.submits(), 1);
    node.stop().await
}

#[tokio::test]
async fn native_approval_rejection_replays_with_current_caller_permissions() -> TestResult {
    use cooplet_node::clients::{ApprovalWait, ApprovalWaitKind};

    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-approval-target", json!({"client_kind":"mock"}))
        .await?;
    let session = mock
        .session(&recipient, 1)
        .ok_or("Missing opened session")?;
    let agent = node.agent_client(&session).await?;
    let input = json!({"to": recipient, "body":"Try a requested action"});
    let key = "native-approval-wait";
    let call = {
        let client = node.client.clone();
        let input = input.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, Value>("/v1/messages", &input, Some(key))
                .await
        })
    };
    let held = mock.next_submit().await?;
    let id = held
        .request
        .message_id
        .clone()
        .ok_or("Missing approved message correlation")?;
    held.finish(SubmitOutcome::AwaitingApproval(ApprovalWait {
        approval_id: Some("native-approval-A".into()),
        kind: ApprovalWaitKind::Permission,
        summary: Some("Action requires approval".into()),
        can_respond_remotely: true,
        action_hint: Some("\"\n".repeat(40_000)),
    }))?;
    let first = problem(
        tokio::time::timeout(WAIT, call).await??,
        ErrorCode::AwaitingApproval,
    )?;
    let replay = problem(
        node.client
            .post_json::<_, Value>("/v1/messages", &input, Some(key))
            .await,
        ErrorCode::AwaitingApproval,
    )?;
    assert_eq!(first.extensions, replay.extensions);
    assert_eq!(first.extensions["message_id"], id.as_str());
    assert_eq!(first.extensions["can_respond_remotely"], true);
    let approval_id = first.extensions["approval_id"]
        .as_str()
        .ok_or("Missing public approval ID")?;
    assert!(approval_id.starts_with("apr_"));
    assert!(serde_json::to_vec(&first)?.len() <= 65_536);
    assert!(serde_json::to_vec(&replay)?.len() <= 65_536);
    let denied = problem(
        agent
            .post_json::<_, Value>("/v1/messages", &input, Some(key))
            .await,
        ErrorCode::AwaitingApproval,
    )?;
    for key in [
        "action_hint",
        "approval_id",
        "can_respond_remotely",
        "summary",
    ] {
        assert!(!denied.extensions.contains_key(key));
    }
    message_state(&node.client, &id, DeliveryState::AwaitingApproval).await?;
    assert_eq!(mock.submits(), 0);
    node.stop().await
}

async fn receipt_write_fault(committed_answer: bool, replace_session: bool) -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-receipt-target", json!({"client_kind":"mock"}))
        .await?;
    let session = mock
        .session(&recipient, 1)
        .ok_or("Missing opened session")?;
    let mut request = json!({"to": recipient, "body":"One native input"});
    let question_id = if committed_answer {
        let agent = node.agent_client(&session).await?;
        let question: SendMessageResponse = agent
            .post_json(
                "/v1/messages",
                &json!({
                    "to": node.client.whoami().await?.actor_id,
                    "body": "",
                    "question": {"prompt":"Use release?", "mode":"text"},
                }),
                Some("question-for-fault"),
            )
            .await?;
        let id = question.message_id.ok_or("Missing question ID")?;
        request["in_reply_to"] = json!(&id);
        request["answer"] = json!({"text":"Yes"});
        Some(id)
    } else {
        None
    };
    let key = "failing-receipt";
    let pending = {
        let client = node.client.clone();
        let request = request.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, SendMessageResponse>("/v1/messages", &request, Some(key))
                .await
        })
    };
    let held = mock.next_submit().await?;
    let id = held
        .request
        .message_id
        .clone()
        .ok_or("Missing message ID")?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_delivery_receipt
                 BEFORE UPDATE OF delivery_state ON main.messages
                 WHEN OLD.delivery_state = 'accepted'
                 BEGIN SELECT RAISE(ABORT, 'test receipt writer refusal'); END;",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    let first = tokio::time::timeout(WAIT, pending).await??;
    assert_eq!(mock.submits(), 1);
    let accepted = message_state(&node.client, &id, DeliveryState::Accepted).await?;
    let operation_id = accepted.operation_id.ok_or("Missing operation ID")?;
    assert_eq!(accepted.in_reply_to, question_id);
    let verify = |result: Result<SendMessageResponse, ClientError>| -> TestResult {
        if committed_answer {
            let receipt = result?;
            assert_eq!(receipt.status(), http::StatusCode::CREATED);
            assert_eq!(receipt.operation_id, operation_id);
            assert_eq!(receipt.message_id.as_ref(), Some(&id));
            assert_eq!(receipt.in_reply_to, question_id);
            assert_eq!(receipt.delivery_state, DeliveryState::Unknown);
        } else {
            let error = problem(result, ErrorCode::DeliveryUnknown)?;
            assert_eq!(error.extensions["operation_id"], operation_id.as_str());
            assert_eq!(error.extensions["message_id"], id.as_str());
        }
        Ok(())
    };
    verify(first)?;
    verify(
        node.client
            .post_json("/v1/messages", &request, Some(key))
            .await,
    )?;

    if replace_session {
        for action in ["stop", "resume"] {
            let _: Value = node
                .client
                .post_json(
                    &format!("/v1/agents/{recipient}/{action}"),
                    &json!({}),
                    Some(&format!("replace-receipt-{action}")),
                )
                .await?;
        }
        assert!(mock.session(&recipient, 2).is_some());
    }
    // Recover the writer without restarting Node or changing the operation key.
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_delivery_receipt")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    // Recovery also runs when the sender never retries.
    message_state(&node.client, &id, DeliveryState::Unknown).await?;
    let expected_state = if replace_session {
        ParticipantState::Idle
    } else {
        ParticipantState::Unknown
    };
    node.participant_when(&recipient, |view| view.state == expected_state)
        .await?;
    let mut changed = request.clone();
    changed["body"] = json!("Different input");
    problem(
        node.client
            .post_json::<_, SendMessageResponse>("/v1/messages", &changed, Some(key))
            .await,
        ErrorCode::OperationConflict,
    )?;
    for _ in 0..2 {
        verify(
            node.client
                .post_json("/v1/messages", &request, Some(key))
                .await,
        )?;
    }
    message_state(&node.client, &id, DeliveryState::Unknown).await?;
    let updates: u64 = node.reads.read(move |conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'message.delivery_updated' AND aggregate_id = ?1",
            [id.as_str()],
            |row| row.get(0),
        )?)
    }).await?;
    assert_eq!(updates, 1);
    let losses: u64 = node.reads.read(move |conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'participant.state_changed' AND aggregate_id = ?1
             AND json_extract(payload_json, '$.state') = 'unknown'",
            [recipient.as_str()], |row| row.get(0),
        )?)
    }).await?;
    assert_eq!(losses, u64::from(!replace_session));
    assert_eq!(mock.submits(), 1);
    node.stop().await
}

#[tokio::test]
async fn orphaned_direct_receipt_becomes_unknown_when_writer_recovers() -> TestResult {
    receipt_write_fault(false, false).await
}

#[tokio::test]
async fn orphaned_answer_receipt_replays_committed_answer_when_writer_recovers() -> TestResult {
    receipt_write_fault(true, false).await
}

#[tokio::test]
async fn lost_direct_receipt_never_fences_the_replacement_generation() -> TestResult {
    receipt_write_fault(false, true).await
}

#[tokio::test]
async fn queued_dispatch_is_resolved_without_replay_after_receipt_failure() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-queue-fault-target", json!({"client_kind":"mock"}))
        .await?;
    let live = node
        .agents
        .live_session(&recipient)
        .ok_or("Missing live session")?;
    let request =
        json!({"to": recipient, "body":"One native input", "delivery_mode":"queue_until_idle"});
    let key = "failing-queue-receipt";
    let original: SendMessageResponse = node
        .client
        .post_json("/v1/messages", &request, Some(key))
        .await?;
    let id = original
        .message_id
        .clone()
        .ok_or("Missing queued message ID")?;
    let held = mock.next_submit().await?;
    assert_eq!(held.request.message_id.as_ref(), Some(&id));
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_queue_receipt
             BEFORE UPDATE OF state ON main.intake_queue
             WHEN OLD.state = 'dispatched'
             BEGIN SELECT RAISE(ABORT, 'test queue receipt refusal'); END;",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    tokio::time::timeout(WAIT, async {
        while live.state() != ParticipantState::Unknown {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    message_state(&node.client, &id, DeliveryState::Queued).await?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_queue_receipt")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let unknown = message_state(&node.client, &id, DeliveryState::Unknown).await?;
    assert_eq!(
        unknown.delivery_detail.as_deref(),
        Some("receipt_unconfirmed")
    );
    let dispatch = node
        .reads
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT state FROM intake_queue WHERE message_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await?;
    assert_eq!(dispatch, "unknown");
    let replay: SendMessageResponse = node
        .client
        .post_json("/v1/messages", &request, Some(key))
        .await?;
    assert_eq!(replay.operation_id, original.operation_id);
    assert_eq!(replay.message_id, original.message_id);
    assert_eq!(replay.delivery_state, DeliveryState::Queued);
    assert_eq!(mock.submits(), 1);
    node.stop().await
}

#[tokio::test]
async fn frame_limit_leaves_keys_free_before_busy_and_approval_denials() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-busy-frame-target", json!({"client_kind":"mock"}))
        .await?;
    let session = mock.session(&recipient, 1).ok_or("Missing session")?;
    for (key, event, state, denial) in [
        (
            "frame-before-busy",
            AdapterEventKind::StateChanged(ParticipantRuntimeState::Working),
            ParticipantState::Working,
            ErrorCode::ParticipantBusy,
        ),
        (
            "frame-before-approval",
            AdapterEventKind::ApprovalRequested(ApprovalWait {
                approval_id: Some("frame-approval".into()),
                kind: ApprovalWaitKind::Permission,
                can_respond_remotely: false,
                action_hint: None,
                summary: None,
            }),
            ParticipantState::AwaitingApproval,
            ErrorCode::AwaitingApproval,
        ),
    ] {
        session.emit(event).await?;
        node.participant_when(&recipient, |view| view.state == state)
            .await?;
        problem(
            node.client
                .post_json::<_, SendMessageResponse>(
                    "/v1/messages",
                    &json!({"to":recipient,"body":"x".repeat(1024 * 1024)}),
                    Some(key),
                )
                .await,
            ErrorCode::PayloadTooLarge,
        )?;
        problem(
            node.client
                .post_json::<_, SendMessageResponse>(
                    "/v1/messages",
                    &json!({"to":recipient,"body":"a valid frame"}),
                    Some(key),
                )
                .await,
            denial,
        )?;
    }
    assert_eq!(mock.submits(), 0);
    node.stop().await
}

#[tokio::test]
async fn frame_limit_applies_without_a_live_session_and_leaves_key_free() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-frame-target", json!({"client_kind":"mock"}))
        .await?;
    let _: Value = node
        .client
        .post_json(
            &format!("/v1/agents/{recipient}/stop"),
            &json!({}),
            Some("stop-frame-target"),
        )
        .await?;
    let key = "frame-before-admission";
    problem(
        node.client
            .post_json::<_, SendMessageResponse>(
                "/v1/messages",
                &json!({"to":recipient,"body":"x".repeat(1024 * 1024)}),
                Some(key),
            )
            .await,
        ErrorCode::PayloadTooLarge,
    )?;
    let receipt: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({"to":recipient,"body":"a valid frame"}),
            Some(key),
        )
        .await?;
    assert_eq!(receipt.delivery_state, DeliveryState::Unavailable);
    assert!(receipt.message_id.is_none());
    assert_eq!(mock.submits(), 0);
    node.stop().await
}

#[tokio::test]
async fn idle_queue_recovers_from_a_failed_claim_without_another_wakeup() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-retry-recipient", json!({"client_kind":"mock"}))
        .await?;
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_claim BEFORE UPDATE OF state ON intake_queue
             WHEN NEW.state = 'dispatched' BEGIN SELECT RAISE(ABORT, 'claim unavailable'); END",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let receipt: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({"to":id,"body":"queued","delivery_mode":"queue_until_idle"}),
            Some("retry-claim"),
        )
        .await?;
    assert_eq!(receipt.delivery_state, DeliveryState::Queued);
    assert!(
        tokio::time::timeout(Duration::from_millis(400), mock.next_submit())
            .await
            .is_err()
    );
    node.db
        .write_sqlite(|tx| {
            tx.execute_batch("DROP TRIGGER temp.fail_claim")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let submit = mock.next_submit().await?;
    assert_eq!(submit.request.message_id, receipt.message_id);
    submit.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    message_state(
        &node.client,
        receipt
            .message_id
            .as_ref()
            .ok_or("Missing queued message")?,
        DeliveryState::Delivered,
    )
    .await?;
    assert_eq!(mock.submits(), 1);
    node.stop().await
}

#[tokio::test]
async fn answer_target_matrix_and_queue_attachment_unavailable() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let id = node
        .create("create-matrix-agent", json!({"client_kind":"mock"}))
        .await?;
    let session = mock.session(&id, 1).ok_or("Missing opened session")?;
    let agent = node.agent_client(&session).await?;
    let op_id = node.client.whoami().await?.actor_id;

    // 1. Answer to a Working agent commits locally with 201 and delivery_state = busy.
    let q_working: SendMessageResponse = agent
        .post_json(
            "/v1/messages",
            &json!({"to": op_id, "body": "", "question": {"prompt": "Busy check?", "mode": "text"}}),
            Some("question-working"),
        )
        .await?;
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::Working)
        .await?;
    let a_working: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": id,
                "body": "",
                "in_reply_to": q_working.message_id,
                "answer": {"text": "Yes"}
            }),
            Some("answer-working"),
        )
        .await?;
    assert_eq!(a_working.status(), http::StatusCode::CREATED);
    assert_eq!(a_working.delivery_state, DeliveryState::Busy);

    // 2. Answer to an AwaitingApproval agent commits locally with 201 and delivery_state = awaiting_approval.
    let q_apr: SendMessageResponse = agent
        .post_json(
            "/v1/messages",
            &json!({"to": op_id, "body": "", "question": {"prompt": "Approval check?", "mode": "text"}}),
            Some("question-approval"),
        )
        .await?;
    session
        .emit(AdapterEventKind::ApprovalRequested(ApprovalWait {
            approval_id: Some("apr-matrix".into()),
            kind: ApprovalWaitKind::Permission,
            summary: Some("pending".into()),
            can_respond_remotely: true,
            action_hint: None,
        }))
        .await?;
    node.participant_when(&id, |view| view.state == ParticipantState::AwaitingApproval)
        .await?;
    let a_apr: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": id,
                "body": "",
                "in_reply_to": q_apr.message_id,
                "answer": {"text": "Later"}
            }),
            Some("answer-approval"),
        )
        .await?;
    assert_eq!(a_apr.status(), http::StatusCode::CREATED);
    assert_eq!(a_apr.delivery_state, DeliveryState::AwaitingApproval);

    // 3. Queued message whose attachment expires before drain transitions to
    //    Unavailable (attachment_unavailable) with intake_queue = cancelled and 0 submits.
    let spec = BlobUploadSpec {
        filename: "vanish.txt".into(),
        mime_type: "text/plain".into(),
        verification: UploadVerification::LocalSha256,
        expected_sha256: None,
        idempotency_key: Some("vanish-upload".into()),
        content_length: None,
    };
    let BlobUploadOutcome::VerifiedLocal(attachment) =
        node.client.upload_blob(spec, b"vanish".as_slice()).await?
    else {
        return Err("Expected verified attachment".into());
    };
    let queued: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": id,
                "body": "see file",
                "delivery_mode": "queue_until_idle",
                "attachments": [attachment]
            }),
            Some("queue-vanish"),
        )
        .await?;
    let queued_id = queued.message_id.ok_or("Missing queued message id")?;
    let blob_id = attachment.blob_id.clone();
    node.db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE blobs SET expires_at_ms = 0 WHERE blob_id = ?1",
                [blob_id.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let cancelled = message_state(&node.client, &queued_id, DeliveryState::Unavailable).await?;
    assert_eq!(
        cancelled.delivery_detail.as_deref(),
        Some("attachment_unavailable")
    );
    let (queue_state, queue_envelope): (String, String) = node
        .reads
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                [queued_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await?;
    assert_eq!(queue_state, "cancelled");
    assert!(queue_envelope.is_empty());
    assert_eq!(mock.submits(), 0);
    node.stop().await
}

#[tokio::test]
async fn direct_and_queued_messages_preserve_author_provenance_and_clear_terminal_envelopes()
-> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let alice_ws = node.workspace_dir.join("alice");
    let bob_ws = node.workspace_dir.join("bob");
    std::fs::create_dir_all(&alice_ws)?;
    std::fs::create_dir_all(&bob_ws)?;
    let sender = node
        .create(
            "create-provenance-sender",
            json!({
                "client_kind": "mock",
                "name": "alice",
                "workspace_dir": alice_ws.to_string_lossy()
            }),
        )
        .await?;
    let recipient = node
        .create(
            "create-provenance-recipient",
            json!({
                "client_kind": "mock",
                "name": "bob",
                "workspace_dir": bob_ws.to_string_lossy()
            }),
        )
        .await?;
    let sender_session = mock.session(&sender, 1).ok_or("Missing sender session")?;
    let recipient_session = mock
        .session(&recipient, 1)
        .ok_or("Missing recipient session")?;
    let agent = node.agent_client(&sender_session).await?;
    let op_id = node.client.whoami().await?.actor_id;

    // 1. Direct message from operator preserves Human provenance.
    let op_direct = {
        let client = node.client.clone();
        let to = recipient.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, SendMessageResponse>(
                    "/v1/messages",
                    &json!({"to": to, "body": "operator direct"}),
                    Some("prov-op-direct"),
                )
                .await
        })
    };
    let held = mock.next_submit().await?;
    assert!(held.request.input.is_direct_human());
    let TurnInput::Direct(ref msg) = held.request.input else {
        return Err("Expected direct TurnInput".into());
    };
    assert_eq!(msg.author.kind, AuthorKind::Human);
    assert_eq!(msg.author.actor_id, op_id);
    assert_eq!(msg.author.name, "operator");
    let rendered = held.request.input.render_canonical()?;
    assert!(rendered.starts_with("[Cooplet direct_message author_kind=human "));
    assert!(rendered.contains("<cooplet-content boundary=\""));
    recipient_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;
    recipient_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Idle)
        .await?;
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    assert_eq!(op_direct.await??.delivery_state, DeliveryState::Delivered);

    // 2. Direct message from peer agent preserves Agent provenance (!is_direct_human).
    let peer_direct = {
        let client = agent.clone();
        let to = recipient.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, SendMessageResponse>(
                    "/v1/messages",
                    &json!({"to": to, "body": "peer direct"}),
                    Some("prov-peer-direct"),
                )
                .await
        })
    };
    let held = mock.next_submit().await?;
    assert!(!held.request.input.is_direct_human());
    let TurnInput::Direct(ref msg) = held.request.input else {
        return Err("Expected direct TurnInput".into());
    };
    assert_eq!(msg.author.kind, AuthorKind::Agent);
    assert_eq!(msg.author.actor_id, sender.as_str());
    assert_eq!(msg.author.name, "alice");
    let rendered = held.request.input.render_canonical()?;
    let first_line = rendered.lines().next().ok_or("Missing header line")?;
    assert!(first_line.starts_with("[Cooplet direct_message author_kind=agent "));
    assert!(!first_line.contains("alice"));
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    assert_eq!(peer_direct.await??.delivery_state, DeliveryState::Delivered);

    // 3. Queued message from peer stores v2 envelope, preserves Agent provenance on drain,
    //    and clears payload_envelope once delivered.
    recipient_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;
    let queued_peer: SendMessageResponse = agent
        .post_json(
            "/v1/messages",
            &json!({
                "to": recipient,
                "body": "peer queued",
                "delivery_mode": "queue_until_idle"
            }),
            Some("prov-peer-queued"),
        )
        .await?;
    let queued_peer_id = queued_peer
        .message_id
        .ok_or("Missing queued peer message id")?;
    let stored_raw: String = {
        let id = queued_peer_id.clone();
        node.reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT payload_envelope FROM intake_queue WHERE message_id = ?1",
                    [id.as_str()],
                    |row| row.get(0),
                )?)
            })
            .await?
    };
    let stored_json: Value = serde_json::from_str(&stored_raw)?;
    assert_eq!(stored_json["v"], 2);
    assert_eq!(stored_json["input"]["kind"], "direct");
    assert_eq!(stored_json["input"]["author"]["kind"], "agent");

    recipient_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let held = mock.next_submit().await?;
    assert!(!held.request.input.is_direct_human());
    let TurnInput::Direct(ref msg) = held.request.input else {
        return Err("Expected direct TurnInput".into());
    };
    assert_eq!(msg.author.kind, AuthorKind::Agent);
    assert_eq!(msg.author.name, "alice");
    assert_eq!(msg.body, "peer queued");
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    message_state(&node.client, &queued_peer_id, DeliveryState::Delivered).await?;
    let (state, envelope): (String, String) = {
        let id = queued_peer_id.clone();
        node.reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                    [id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .await?
    };
    assert_eq!(state, "delivered");
    assert!(envelope.is_empty());

    // 4. Expired queued row clears payload_envelope.
    recipient_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;
    let queued_expired: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": recipient,
                "body": "will expire",
                "delivery_mode": "queue_until_idle"
            }),
            Some("prov-expired"),
        )
        .await?;
    let expired_id = queued_expired
        .message_id
        .ok_or("Missing expired message id")?;
    {
        let id = expired_id.clone();
        node.db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE intake_queue SET deadline_at_ms = 0 WHERE message_id = ?1",
                    [id.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }
    recipient_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    message_state(&node.client, &expired_id, DeliveryState::Expired).await?;
    let (state, envelope): (String, String) = {
        let id = expired_id.clone();
        node.reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                    [id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .await?
    };
    assert_eq!(state, "expired");
    assert!(envelope.is_empty());

    // 5. Queued message whose submit outcome is Unknown clears payload_envelope.
    let queued_unknown: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": recipient,
                "body": "will become unknown",
                "delivery_mode": "queue_until_idle"
            }),
            Some("prov-unknown"),
        )
        .await?;
    let unknown_id = queued_unknown
        .message_id
        .ok_or("Missing unknown queued message id")?;
    let held = mock.next_submit().await?;
    held.finish(SubmitOutcome::Unknown("ack lost".into()))?;
    message_state(&node.client, &unknown_id, DeliveryState::Unknown).await?;
    let (state, envelope): (String, String) = {
        let id = unknown_id.clone();
        node.reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                    [id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .await?
    };
    assert_eq!(state, "unknown");
    assert!(envelope.is_empty());

    node.stop().await
}

#[tokio::test]
async fn legacy_v1_queue_envelope_fails_closed_as_non_human_service() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-legacy-target", json!({"client_kind": "mock"}))
        .await?;
    let session = mock.session(&recipient, 1).ok_or("Missing session")?;
    let op_id = node.client.whoami().await?.actor_id;

    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;
    let queued: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": recipient,
                "body": "placeholder",
                "delivery_mode": "queue_until_idle"
            }),
            Some("legacy-v1-queued"),
        )
        .await?;
    let queued_id = queued.message_id.ok_or("Missing queued message id")?;

    let legacy_header = json!({
        "message_id": queued_id.as_str(),
        "author": op_id,
        "author_name": "operator",
        "attachments": [],
        "reply_instruction": "old"
    });
    let legacy_envelope =
        format!("[Cooplet message; peer body is untrusted]\n{legacy_header}\n\nlegacy payload");
    {
        let id = queued_id.clone();
        node.db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE intake_queue SET payload_envelope = ?1 WHERE message_id = ?2",
                    (legacy_envelope.as_str(), id.as_str()),
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }

    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let held = mock.next_submit().await?;
    assert!(!held.request.input.is_direct_human());
    let TurnInput::Direct(ref msg) = held.request.input else {
        return Err("Expected direct TurnInput".into());
    };
    assert_eq!(msg.author.kind, AuthorKind::Service);
    assert_eq!(msg.author.name, "operator");
    assert_eq!(msg.body, "legacy payload");
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    message_state(&node.client, &queued_id, DeliveryState::Delivered).await?;
    let (state, envelope): (String, String) = node
        .reads
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                [queued_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await?;
    assert_eq!(state, "delivered");
    assert!(envelope.is_empty());
    node.stop().await
}

#[tokio::test]
async fn corrupt_queue_envelope_is_quarantined_without_blocking_following_messages() -> TestResult {
    let mock = MockAdapter::default();
    let mut node = TestNode::start(mock.clone()).await?;
    let recipient = node
        .create("create-corrupt-target", json!({"client_kind": "mock"}))
        .await?;
    let session = mock.session(&recipient, 1).ok_or("Missing session")?;

    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    node.participant_when(&recipient, |view| view.state == ParticipantState::Working)
        .await?;
    let bad: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": recipient,
                "body": "will be corrupted",
                "delivery_mode": "queue_until_idle"
            }),
            Some("corrupt-head"),
        )
        .await?;
    let bad_id = bad.message_id.ok_or("Missing bad message id")?;
    let good: SendMessageResponse = node
        .client
        .post_json(
            "/v1/messages",
            &json!({
                "to": recipient,
                "body": "healthy follower",
                "delivery_mode": "queue_until_idle"
            }),
            Some("corrupt-follower"),
        )
        .await?;
    let good_id = good.message_id.ok_or("Missing good message id")?;

    {
        let id = bad_id.clone();
        node.db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE intake_queue SET payload_envelope = '{corrupt' WHERE message_id = ?1",
                    [id.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }

    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let held = mock.next_submit().await?;
    assert_eq!(held.request.message_id.as_ref(), Some(&good_id));
    assert!(held.request.input.is_direct_human());
    let TurnInput::Direct(ref msg) = held.request.input else {
        return Err("Expected direct TurnInput".into());
    };
    assert_eq!(msg.body, "healthy follower");
    held.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;

    let quarantined = message_state(&node.client, &bad_id, DeliveryState::Unavailable).await?;
    assert_eq!(
        quarantined.delivery_detail.as_deref(),
        Some("corrupt_envelope")
    );
    let (bad_state, bad_envelope): (String, String) = {
        let id = bad_id.clone();
        node.reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                    [id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .await?
    };
    assert_eq!(bad_state, "cancelled");
    assert!(bad_envelope.is_empty());

    message_state(&node.client, &good_id, DeliveryState::Delivered).await?;
    let (good_state, good_envelope): (String, String) = {
        let id = good_id.clone();
        node.reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT state, payload_envelope FROM intake_queue WHERE message_id = ?1",
                    [id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .await?
    };
    assert_eq!(good_state, "delivered");
    assert!(good_envelope.is_empty());
    assert_eq!(mock.submits(), 1);
    node.stop().await
}
