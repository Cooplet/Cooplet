use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{
    AnswerPayload, DeliveryMode, DeliveryState, ErrorCode, MessageId, ParticipantId, PrincipalKind,
    QuestionMode, QuestionPayload, ServiceScope, TokenId, UserId, WhoamiResponse,
};
use cooplet_storage::WriteOutcome;
use rusqlite::params;

use super::{DeliveryService, SendMessageRequest};
use crate::{
    auth::Principal,
    clients::{AdapterRegistry, AgentConfig, AgentService},
    config::{NodeConfig, OpenNode},
    operations::OperationBook,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const NOW: i64 = 100_000;

struct StateDir(PathBuf);

impl StateDir {
    fn new() -> TestResult<Self> {
        let path = std::env::temp_dir().join(format!("cooplet-msg-{}", TokenId::generate()?));
        PrivateFs::ensure_private_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for StateDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    open: OpenNode,
    service: Arc<DeliveryService>,
    clock: Arc<AtomicI64>,
    _directory: StateDir,
}

impl Fixture {
    async fn start() -> TestResult<Self> {
        let directory = StateDir::new()?;
        let open =
            OpenNode::start(NodeConfig::new(directory.0.clone(), directory.0.clone())).await?;
        let db = open.engine.db().clone();
        let reads = open.engine.reads().clone();
        let blobs = open.engine.blobs().ok_or("Missing blobs")?.clone();
        let operations = OperationBook::new(db.clone(), reads.clone());
        let config = AgentConfig {
            node_id: open.info.node_id.clone(),
            endpoint: open.operator.address()?,
            state_dir: open.state_dir.clone(),
            workspace_dir: open.workspace_dir.clone(),
        };
        let agents = Arc::new(AgentService::new(
            db.clone(),
            reads.clone(),
            open.auth.clone(),
            AdapterRegistry::default(),
            operations.clone(),
            config,
        ));
        let clock = Arc::new(AtomicI64::new(NOW));
        let sample = clock.clone();
        let service = DeliveryService::new_with_clock(
            db,
            reads,
            blobs,
            open.state_dir.join("blobs"),
            agents,
            operations,
            Arc::new(move || Ok(sample.load(Ordering::SeqCst))),
        )?;
        Ok(Self {
            open,
            service,
            clock,
            _directory: directory,
        })
    }

    fn operator(&self) -> TestResult<Principal> {
        self.principal(
            self.open.operator.participant_id.clone(),
            PrincipalKind::Operator,
            "operator",
        )
    }

    fn principal(
        &self,
        id: ParticipantId,
        kind: PrincipalKind,
        name: &str,
    ) -> TestResult<Principal> {
        let scope = if kind == PrincipalKind::Operator {
            ServiceScope::Manage
        } else {
            ServiceScope::Collaborate
        };
        Ok(Principal {
            token_id: TokenId::generate()?,
            identity: WhoamiResponse {
                actor_id: id.as_str().to_owned(),
                name: name.into(),
                principal_kind: kind,
                scopes: vec![scope],
                node_id: Some(self.open.info.node_id.clone()),
                participant_id: Some(id),
            },
        })
    }

    async fn stopped_agent(&self) -> TestResult<ParticipantId> {
        let id = ParticipantId::generate()?;
        let key = id.clone();
        self.open.engine.db().write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO participants (participant_id,kind,name,state,created_at_ms,updated_at_ms,stopped_at_ms)
                 VALUES (?1,'agent',?1,'stopped',?2,?2,?2)",
                params![key.as_str(), NOW],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(id)
    }

    async fn stop(self) -> TestResult {
        self.service.shutdown().await;
        self.open.engine.shutdown().await?;
        Ok(())
    }
}

fn message(to: impl Into<String>, body: impl Into<String>) -> SendMessageRequest {
    SendMessageRequest {
        to: to.into(),
        body: body.into(),
        in_reply_to: None,
        attachments: Vec::new(),
        question: None,
        answer: None,
        delivery_mode: DeliveryMode::DirectNow,
        queue_ttl_ms: None,
    }
}

fn question(expires_at_ms: Option<i64>) -> QuestionPayload {
    QuestionPayload {
        prompt: "Which build should I use?".into(),
        mode: QuestionMode::Text,
        options: Vec::new(),
        allow_free_text: false,
        expires_at_ms,
    }
}

#[tokio::test]
async fn replayed_hub_receipt_resolves_an_outgoing_projection_once() -> TestResult {
    use cooplet_protocol::{DirectMessageReceipt, EventKind, NodeId, OperationId};

    let fixture = Fixture::start().await?;
    let mut notices = fixture.open.engine.db().subscribe();
    let sender = fixture.open.operator.participant_id.clone();
    let recipient = format!("{}/{}", NodeId::generate()?, ParticipantId::generate()?);
    let hub_message = MessageId::generate()?;
    let operation = OperationId::generate()?;
    let apply = |delivery_state| {
        fixture.service.project_outbound(
            sender.clone(),
            recipient.clone(),
            message(&recipient, "Confirmed answer"),
            DirectMessageReceipt {
                operation_id: operation.clone(),
                message_id: hub_message.clone(),
                in_reply_to: None,
                delivery_state,
            },
        )
    };
    let local = apply(DeliveryState::Unknown)
        .await?
        .message_id
        .ok_or("missing local message")?;
    assert_eq!(notices.try_recv()?.kind, EventKind::MessageCreated);
    fixture.clock.store(NOW + 1, Ordering::SeqCst);
    let resolved = apply(DeliveryState::Delivered).await?;
    assert_eq!(resolved.message_id.as_ref(), Some(&local));
    assert_eq!(notices.try_recv()?.kind, EventKind::MessageDeliveryUpdated);
    fixture.clock.store(NOW + 2, Ordering::SeqCst);
    for state in [DeliveryState::Delivered, DeliveryState::Unknown] {
        assert_eq!(apply(state).await?.delivery_state, DeliveryState::Delivered);
    }
    assert!(matches!(
        notices.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let view = fixture.service.show(&fixture.operator()?, local).await?;
    assert_eq!(view.delivery_state, DeliveryState::Delivered);
    assert_eq!(view.delivered_at_ms, Some(NOW + 1));
    fixture.stop().await
}

#[tokio::test]
async fn question_accepts_one_answer_and_replays_it_after_expiry() -> TestResult {
    let fixture = Fixture::start().await?;
    let agent = fixture.stopped_agent().await?;
    let caller = fixture.principal(agent.clone(), PrincipalKind::Participant, "agent")?;
    let mut ask = message(fixture.open.operator.participant_id.as_str(), "");
    ask.question = Some(question(Some(NOW + 1)));
    let received = fixture
        .service
        .send(caller, "question-create".into(), ask)
        .await?;
    assert_eq!(received.delivery_state, DeliveryState::Delivered);
    let question = received.message_id.ok_or("Missing question message")?;
    let mut answer = message(agent.as_str(), "");
    answer.in_reply_to = Some(question.clone());
    answer.answer = Some(AnswerPayload {
        selected_option_ids: Vec::new(),
        text: Some("Release".into()),
    });
    let (a, b) = tokio::join!(
        fixture
            .service
            .send(fixture.operator()?, "answer-key-a".into(), answer.clone()),
        fixture
            .service
            .send(fixture.operator()?, "answer-key-b".into(), answer.clone()),
    );
    let (winning_key, winner, loser) = match (a, b) {
        (Ok(first), Err(second)) => ("answer-key-a", first, second),
        (Err(first), Ok(second)) => ("answer-key-b", second, first),
        _ => return Err("The question did not admit exactly one answer".into()),
    };
    assert_eq!(winner.delivery_state, DeliveryState::Unavailable);
    assert_eq!(winner.status(), http::StatusCode::CREATED);
    assert_eq!(loser.code(), ErrorCode::QuestionAlreadyAnswered);
    let winner_id = winner.message_id.as_ref().ok_or("Missing answer")?;
    assert_eq!(loser.0.extensions["existing_answer_id"], winner_id.as_str());
    let answer_events: u64 = fixture
        .open
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM events WHERE kind = 'question.answered'",
                [],
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(answer_events, 1);

    fixture.clock.store(NOW + 1, Ordering::SeqCst);
    let replay = fixture
        .service
        .send(fixture.operator()?, winning_key.into(), answer.clone())
        .await?;
    assert_eq!(replay, winner);
    let error = fixture
        .service
        .send(
            fixture.operator()?,
            "answer-key-fresh".into(),
            answer.clone(),
        )
        .await
        .err()
        .ok_or("Expired question accepted a new answer")?;
    assert_eq!(error.code(), ErrorCode::QuestionExpired);
    answer.body = "changed".into();
    let conflict = fixture
        .service
        .send(fixture.operator()?, winning_key.into(), answer)
        .await
        .err()
        .ok_or("Changed request reused a completed key")?;
    assert_eq!(conflict.code(), ErrorCode::OperationConflict);
    fixture.stop().await
}

#[tokio::test]
async fn hub_projection_requires_hub_authority_without_reserving_key() -> TestResult {
    let fixture = Fixture::start().await?;
    let remote = UserId::generate()?;
    let message_id = MessageId::generate()?;
    let seed_id = message_id.clone();
    let seed_remote = remote.clone();
    let local = fixture.open.operator.participant_id.clone();
    let payload = serde_json::to_string(&question(None))?;
    fixture.open.engine.db().write_sqlite(move |tx| {
        tx.execute(
            "INSERT INTO messages (message_id,sender_id,recipient_id,body,question_json,hub_message_id,
                     delivery_mode,delivery_state,created_at_ms,expires_at_ms)
             VALUES (?1,?2,?3,'',?4,?1,'direct_now','delivered',?5,?6)",
            params![seed_id.as_str(), seed_remote.as_str(), local.as_str(), payload, NOW, NOW + 1000],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    let mut input = message(remote.as_str(), "");
    input.in_reply_to = Some(message_id);
    input.answer = Some(AnswerPayload {
        selected_option_ids: Vec::new(),
        text: Some("Yes".into()),
    });
    let key = "answer-from-hub";
    let error = fixture
        .service
        .send(fixture.operator()?, key.into(), input)
        .await
        .err()
        .ok_or("Hub-authoritative answer unexpectedly accepted")?;
    assert_eq!(error.code(), ErrorCode::BridgeUnavailable);
    let counts = fixture.open.engine.reads().read(|conn| {
        Ok(conn.query_row("SELECT (SELECT COUNT(*) FROM operations), (SELECT COUNT(*) FROM messages WHERE answer_json IS NOT NULL)",
            [], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)))?)
    }).await?;
    assert_eq!(counts, (0, 0));
    let retry = fixture
        .service
        .send(
            fixture.operator()?,
            key.into(),
            message(fixture.open.operator.participant_id.as_str(), "Local"),
        )
        .await?;
    assert_eq!(retry.status(), http::StatusCode::CREATED);
    fixture.stop().await
}

#[tokio::test]
async fn large_messages_obey_event_and_page_budgets_and_visibility() -> TestResult {
    let fixture = Fixture::start().await?;
    let agent = fixture.stopped_agent().await?;
    let to = fixture.open.operator.participant_id.as_str();
    let actor = || fixture.principal(agent.clone(), PrincipalKind::Participant, "agent");
    for index in 0..5 {
        let result = fixture
            .service
            .send(
                actor()?,
                format!("large-msg-{index}"),
                message(to, "é".repeat(450 * 1024)),
            )
            .await?;
        assert_eq!(result.status(), http::StatusCode::CREATED);
    }
    let large = fixture
        .service
        .send(
            actor()?,
            "oversize-message".into(),
            message(to, "\n".repeat(512 * 1024)),
        )
        .await
        .err()
        .ok_or("Oversize event was published")?;
    assert_eq!(large.code(), ErrorCode::PayloadTooLarge);

    let first = fixture.service.list(&actor()?, None, Some(100)).await?;
    assert_eq!(first.items.len(), 4);
    assert!(serde_json::to_vec(&first)?.len() <= 4 * 1024 * 1024);
    let cursor = first.next_after.as_ref().ok_or("Missing keyset cursor")?;
    assert_eq!(
        Some(cursor),
        first.items.last().map(|item| &item.message_id)
    );
    let second = fixture
        .service
        .list(&actor()?, Some(cursor.clone()), Some(100))
        .await?;
    assert_eq!(second.items.len(), 1);
    assert!(second.next_after.is_none());
    assert_eq!(second.items[0].body.len(), 900 * 1024);
    let unknown = fixture.principal(
        ParticipantId::generate()?,
        PrincipalKind::Participant,
        "other",
    )?;
    assert!(
        fixture
            .service
            .list(&unknown, None, None)
            .await?
            .items
            .is_empty()
    );
    let error = fixture
        .service
        .show(&unknown, cursor.clone())
        .await
        .err()
        .ok_or("Private message visible to another participant")?;
    assert_eq!(error.code(), ErrorCode::NotFound);

    let counts = fixture.open.engine.reads().read(|conn| {
        Ok(conn.query_row("SELECT (SELECT COUNT(*) FROM operations), (SELECT COUNT(*) FROM events WHERE kind='message.created')",
            [], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)))?)
    }).await?;
    assert_eq!(counts, (5, 5));
    fixture.stop().await
}

#[tokio::test]
async fn attachments_limit_uniqueness_acl_and_answer_ordering_are_enforced() -> TestResult {
    use cooplet_protocol::{AttachmentRef, BlobId};
    use cooplet_storage::BlobInput;

    let fixture = Fixture::start().await?;
    let agent = fixture.stopped_agent().await?;
    let other_agent = fixture.stopped_agent().await?;
    let blobs = fixture.open.engine.blobs().ok_or("Missing blobs")?;

    let store_blob = |owner: String, name: &'static str, expires_at: i64| async move {
        let id = BlobId::generate()?;
        let (stored, ()) = blobs
            .store::<_, _, std::convert::Infallible, _>(
                BlobInput {
                    blob_id: id.clone(),
                    original_name: name.into(),
                    expected_size: Some(4),
                    expected_sha256: None,
                },
                b"data".as_slice(),
                move |tx, blob| {
                    tx.execute(
                        "INSERT INTO blobs (blob_id, sha256, size_bytes, mime_type, original_name,
                                            safe_disk_name, disk_rel_path, owner_actor_id,
                                            created_at_ms, expires_at_ms)
                         VALUES (?1, ?2, ?3, 'text/plain', ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            blob.blob_id.as_str(),
                            blob.sha256,
                            blob.size_bytes,
                            blob.original_name,
                            blob.safe_disk_name,
                            blob.disk_rel_path.to_string_lossy(),
                            owner,
                            NOW,
                            expires_at,
                        ],
                    )?;
                    Ok(WriteOutcome::new(()))
                },
            )
            .await?;
        Ok::<AttachmentRef, Box<dyn std::error::Error + Send + Sync>>(AttachmentRef {
            blob_id: stored.blob_id,
            name: stored.original_name,
            mime_type: "text/plain".into(),
            size_bytes: stored.size_bytes,
            sha256: stored.sha256,
        })
    };

    let op_id = fixture.open.operator.participant_id.as_str().to_owned();
    let own_blob = store_blob(op_id.clone(), "own.txt", NOW + 60_000).await?;
    let expired_blob = store_blob(op_id.clone(), "expired.txt", NOW).await?;
    let foreign_blob =
        store_blob(other_agent.as_str().to_owned(), "foreign.txt", NOW + 60_000).await?;

    // 1. > 64 attachments and duplicate blob_id are rejected with 400 and leave key free.
    let mut too_many = message(&op_id, "too many");
    for _ in 0..65 {
        too_many.attachments.push(AttachmentRef {
            blob_id: BlobId::generate()?,
            ..own_blob.clone()
        });
    }
    let err = fixture
        .service
        .send(fixture.operator()?, "reuse-attach-key".into(), too_many)
        .await
        .err()
        .ok_or("Expected > 64 attachments rejection")?;
    assert_eq!(err.code(), ErrorCode::InvalidRequest);

    let mut duplicate = message(&op_id, "dup");
    duplicate.attachments = vec![own_blob.clone(), own_blob.clone()];
    let err = fixture
        .service
        .send(fixture.operator()?, "reuse-attach-key".into(), duplicate)
        .await
        .err()
        .ok_or("Expected duplicate blob_id rejection")?;
    assert_eq!(err.code(), ErrorCode::InvalidRequest);

    // 2. Attach ACL: foreign live -> 403, missing/expired -> 404, metadata mismatch -> 400; all leave key free.
    let mut foreign_msg = message(&op_id, "foreign");
    foreign_msg.attachments = vec![foreign_blob.clone()];
    assert_eq!(
        fixture
            .service
            .send(fixture.operator()?, "reuse-attach-key".into(), foreign_msg)
            .await
            .err()
            .ok_or("Expected 403 for foreign blob")?
            .code(),
        ErrorCode::Forbidden
    );
    let mut expired_msg = message(&op_id, "expired");
    expired_msg.attachments = vec![expired_blob];
    assert_eq!(
        fixture
            .service
            .send(fixture.operator()?, "reuse-attach-key".into(), expired_msg)
            .await
            .err()
            .ok_or("Expected 404 for expired blob")?
            .code(),
        ErrorCode::NotFound
    );
    let mut mismatch_msg = message(&op_id, "mismatch");
    let mut bad_ref = own_blob.clone();
    bad_ref.name = "tampered.txt".into();
    mismatch_msg.attachments = vec![bad_ref];
    assert_eq!(
        fixture
            .service
            .send(fixture.operator()?, "reuse-attach-key".into(), mismatch_msg)
            .await
            .err()
            .ok_or("Expected 400 for metadata mismatch")?
            .code(),
        ErrorCode::InvalidRequest
    );

    // 3. 200 unavailable (stopped agent) must NOT mutate attached_at_ms / expires_at_ms.
    let mut unavailable = message(agent.as_str(), "to stopped");
    unavailable.attachments = vec![own_blob.clone()];
    let receipt = fixture
        .service
        .send(fixture.operator()?, "reuse-attach-key".into(), unavailable)
        .await?;
    assert_eq!(receipt.delivery_state, DeliveryState::Unavailable);
    assert!(receipt.message_id.is_none());
    let own_id = own_blob.blob_id.clone();
    let meta: (Option<i64>, i64) = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT attached_at_ms, expires_at_ms FROM blobs WHERE blob_id = ?1",
                [own_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await?;
    assert_eq!(meta, (None, NOW + 60_000));

    // 4. Real message delivery sets attached_at_ms and extends expires_at_ms to now + 14d.
    let mut delivered = message(&op_id, "real attach");
    delivered.attachments = vec![own_blob.clone()];
    let ok = fixture
        .service
        .send(fixture.operator()?, "real-attach-key".into(), delivered)
        .await?;
    assert_eq!(ok.delivery_state, DeliveryState::Delivered);
    let own_id = own_blob.blob_id.clone();
    let meta: (Option<i64>, i64) = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT attached_at_ms, expires_at_ms FROM blobs WHERE blob_id = ?1",
                [own_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await?;
    assert_eq!(meta, (Some(NOW), NOW + 14 * 24 * 60 * 60 * 1000));

    // 5. Foreign blob on an expired question returns 403 (beats answer_denial) and leaves key free.
    let agent_caller = fixture.principal(agent.clone(), PrincipalKind::Participant, "agent")?;
    let mut ask = message(&op_id, "");
    ask.question = Some(question(Some(NOW + 10)));
    let asked = fixture
        .service
        .send(agent_caller, "ask-for-order".into(), ask)
        .await?;
    let qid = asked.message_id.ok_or("Missing question id")?;
    fixture.clock.store(NOW + 20, Ordering::SeqCst);
    let mut bad_answer = message(agent.as_str(), "");
    bad_answer.in_reply_to = Some(qid);
    bad_answer.attachments = vec![foreign_blob];
    bad_answer.answer = Some(AnswerPayload {
        selected_option_ids: Vec::new(),
        text: Some("late".into()),
    });
    let denial = fixture
        .service
        .send(fixture.operator()?, "order-key".into(), bad_answer)
        .await
        .err()
        .ok_or("Expected 403 before question_expired")?;
    assert_eq!(denial.code(), ErrorCode::Forbidden);
    // Key remained free and can be used for a valid message.
    let reuse = fixture
        .service
        .send(
            fixture.operator()?,
            "order-key".into(),
            message(&op_id, "ok"),
        )
        .await?;
    assert_eq!(reuse.delivery_state, DeliveryState::Delivered);
    fixture.stop().await
}

#[tokio::test]
async fn human_queue_mode_expired_question_impersonation_and_inactive_participant_acl() -> TestResult
{
    let fixture = Fixture::start().await?;
    let agent = fixture.stopped_agent().await?;
    let op_id = fixture.open.operator.participant_id.clone();

    // 1. Human recipient + queue_until_idle delivers immediately with 0 intake_queue rows.
    let mut queued_human = message(op_id.as_str(), "hello human");
    queued_human.delivery_mode = DeliveryMode::QueueUntilIdle;
    let receipt = fixture
        .service
        .send(fixture.operator()?, "human-queue-key".into(), queued_human)
        .await?;
    assert_eq!(receipt.status(), http::StatusCode::CREATED);
    assert_eq!(receipt.delivery_state, DeliveryState::Delivered);
    let queue_rows: u64 = fixture
        .open
        .engine
        .reads()
        .read(|conn| Ok(conn.query_row("SELECT count(*) FROM intake_queue", [], |r| r.get(0))?))
        .await?;
    assert_eq!(queue_rows, 0);

    // 2. Question with expires_at_ms <= now is rejected with 400.
    let mut expired_ask = message(op_id.as_str(), "");
    expired_ask.question = Some(question(Some(NOW)));
    assert_eq!(
        fixture
            .service
            .send(
                fixture.principal(agent.clone(), PrincipalKind::Participant, "agent")?,
                "expired-ask-key".into(),
                expired_ask,
            )
            .await
            .err()
            .ok_or("Expected expired question creation error")?
            .code(),
        ErrorCode::InvalidRequest
    );

    // 3. Manage-adapter with a different actor_id cannot impersonate the recipient to answer a question.
    let mut ask = message(op_id.as_str(), "");
    ask.question = Some(question(Some(NOW + 1000)));
    let qid = fixture
        .service
        .send(
            fixture.principal(agent.clone(), PrincipalKind::Participant, "agent")?,
            "impersonation-ask".into(),
            ask,
        )
        .await?
        .message_id
        .ok_or("Missing question id")?;
    let impersonator = Principal {
        token_id: TokenId::generate()?,
        identity: WhoamiResponse {
            actor_id: "adapter-other".into(),
            name: "adapter-other".into(),
            principal_kind: PrincipalKind::Adapter,
            scopes: vec![
                ServiceScope::Read,
                ServiceScope::Collaborate,
                ServiceScope::Manage,
            ],
            node_id: Some(fixture.open.info.node_id.clone()),
            participant_id: None,
        },
    };
    let mut ans = message(agent.as_str(), "");
    ans.in_reply_to = Some(qid.clone());
    ans.answer = Some(AnswerPayload {
        selected_option_ids: Vec::new(),
        text: Some("forged".into()),
    });
    assert_eq!(
        fixture
            .service
            .send(impersonator, "impersonate-key".into(), ans.clone())
            .await
            .err()
            .ok_or("Expected anti-impersonation rejection")?
            .code(),
        ErrorCode::InvalidRequest
    );

    // 4. Post-GC replay: after the operation receipt is GC'd, retrying the same key on an
    //    already-answered question returns 409 question_already_answered.
    let answered = fixture
        .service
        .send(fixture.operator()?, "gc-answer-key".into(), ans.clone())
        .await?;
    assert_eq!(answered.status(), http::StatusCode::CREATED);
    fixture
        .open
        .engine
        .db()
        .write_sqlite(|tx| {
            tx.execute(
                "DELETE FROM operations WHERE operation_key = 'gc-answer-key'",
                [],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    assert_eq!(
        fixture
            .service
            .send(fixture.operator()?, "gc-answer-key".into(), ans)
            .await
            .err()
            .ok_or("Expected 409 after operation receipt GC")?
            .code(),
        ErrorCode::QuestionAlreadyAnswered
    );

    // 5. Participant without Collaborate scope is denied read routes (list/show).
    let inactive = Principal {
        token_id: TokenId::generate()?,
        identity: WhoamiResponse {
            actor_id: agent.as_str().to_owned(),
            name: "inactive".into(),
            principal_kind: PrincipalKind::Participant,
            scopes: Vec::new(),
            node_id: Some(fixture.open.info.node_id.clone()),
            participant_id: Some(agent),
        },
    };
    assert_eq!(
        fixture
            .service
            .list(&inactive, None, None)
            .await
            .err()
            .ok_or("Expected 403 for inactive participant list")?
            .code(),
        ErrorCode::Forbidden
    );
    assert_eq!(
        fixture
            .service
            .show(&inactive, qid)
            .await
            .err()
            .ok_or("Expected 403 for inactive participant show")?
            .code(),
        ErrorCode::Forbidden
    );
    fixture.stop().await
}

#[tokio::test]
async fn reconcile_self_pending_and_failed_receipt_distinction() -> TestResult {
    use cooplet_protocol::OperationId;

    use crate::operations::{Operation, ReceiptLookup};

    let fixture = Fixture::start().await?;
    let agent = fixture.stopped_agent().await?;
    let actor_id = fixture.open.operator.participant_id.as_str().to_owned();

    // 1. Self-owned pending direct message (lost Tx 1 commit reply after slot drop) reconciles
    //    both message (`accepted` -> `unknown`) and operation (`in_progress` -> `unknown`).
    let self_op_id = OperationId::generate()?;
    let self_msg_id = MessageId::generate()?;
    let op = Operation {
        id: self_op_id.clone(),
        actor_id: actor_id.clone(),
        key: "self-reconcile-key".into(),
        kind: "message.send",
        hash: "self-hash".into(),
        bridge_origin: None,
    };
    {
        let op = op.clone();
        let msg = self_msg_id.clone();
        let to = agent.clone();
        fixture
            .open
            .engine
            .db()
            .write_sqlite(move |tx| {
                op.reserve(tx, NOW, Some(msg.as_str()))?;
                tx.execute(
                    "INSERT INTO messages (message_id, sender_id, recipient_id, body, delivery_mode,
                                           delivery_state, operation_id, created_at_ms, expires_at_ms)
                     VALUES (?1, ?2, ?3, 'hello', 'direct_now', 'accepted', ?4, ?5, ?6)",
                    params![msg.as_str(), op.actor_id, to.as_str(), op.id.as_str(), NOW, NOW + 10_000],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }
    let err = fixture
        .service
        .reconcile_pending(&op, self_op_id.as_str(), Some(self_msg_id.as_str()), false)
        .await
        .err()
        .ok_or("Expected delivery_unknown from self reconciliation")?;
    assert_eq!(err.code(), ErrorCode::DeliveryUnknown);
    assert_eq!(err.0.extensions["operation_id"], self_op_id.as_str());
    assert_eq!(err.0.extensions["message_id"], self_msg_id.as_str());

    let msg_id_check = self_msg_id.clone();
    let op_id_check = self_op_id.clone();
    let (msg_state, op_state): (String, String) = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok((
                conn.query_row(
                    "SELECT delivery_state FROM messages WHERE message_id = ?1",
                    [msg_id_check.as_str()],
                    |r| r.get(0),
                )?,
                conn.query_row(
                    "SELECT state FROM operations WHERE operation_id = ?1",
                    [op_id_check.as_str()],
                    |r| r.get(0),
                )?,
            ))
        })
        .await?;
    assert_eq!(
        (msg_state.as_str(), op_state.as_str()),
        ("unknown", "unknown")
    );

    // 2. Self-owned pending answer reconciles into 201 succeeded with delivery_state = unknown.
    let q_id = MessageId::generate()?;
    let ans_op_id = OperationId::generate()?;
    let ans_msg_id = MessageId::generate()?;
    let ans_op = Operation {
        id: ans_op_id.clone(),
        actor_id: actor_id.clone(),
        key: "self-answer-key".into(),
        kind: "message.send",
        hash: "ans-hash".into(),
        bridge_origin: None,
    };
    {
        let ans_op = ans_op.clone();
        let q = q_id.clone();
        let ans = ans_msg_id.clone();
        let to = agent.clone();
        fixture
            .open
            .engine
            .db()
            .write_sqlite(move |tx| {
                tx.execute(
                    "INSERT INTO messages (message_id, sender_id, recipient_id, body, question_json,
                                           delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                     VALUES (?1, ?2, ?3, '', '{}', 'direct_now', 'delivered', ?4, ?5)",
                    params![q.as_str(), to.as_str(), ans_op.actor_id, NOW, NOW + 10_000],
                )?;
                ans_op.reserve(tx, NOW, Some(ans.as_str()))?;
                tx.execute(
                    "INSERT INTO messages (message_id, sender_id, recipient_id, in_reply_to, body, answer_json,
                                           delivery_mode, delivery_state, operation_id, created_at_ms, expires_at_ms)
                     VALUES (?1, ?2, ?3, ?4, '', '{\"text\":\"yes\"}', 'direct_now', 'accepted', ?5, ?6, ?7)",
                    params![
                        ans.as_str(),
                        ans_op.actor_id,
                        to.as_str(),
                        q.as_str(),
                        ans_op.id.as_str(),
                        NOW,
                        NOW + 10_000
                    ],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }
    let answer_receipt = fixture
        .service
        .reconcile_pending(&ans_op, ans_op_id.as_str(), Some(ans_msg_id.as_str()), true)
        .await?;
    assert_eq!(answer_receipt.operation_id, ans_op_id);
    assert_eq!(answer_receipt.message_id, Some(ans_msg_id));
    assert_eq!(answer_receipt.in_reply_to, Some(q_id));
    assert_eq!(answer_receipt.delivery_state, DeliveryState::Unknown);

    // 3. Stored failed operation returns Ok(ReceiptLookup::Failed(problem)), distinct from ReadPool Err.
    let failed_op = Operation {
        id: OperationId::generate()?,
        actor_id,
        key: "stored-failed-key".into(),
        kind: "message.send",
        hash: "failed-hash".into(),
        bridge_origin: None,
    };
    {
        let failed_op = failed_op.clone();
        fixture
            .open
            .engine
            .db()
            .write_sqlite(move |tx| {
                failed_op.reserve(tx, NOW, None)?;
                failed_op.fail(
                    tx,
                    NOW,
                    &crate::error::NodeError::new(ErrorCode::ParticipantBusy, "Busy"),
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }
    let looked = fixture
        .open
        .engine
        .reads()
        .read(move |conn| Ok(failed_op.lookup_state(conn)))
        .await??;
    assert!(matches!(
        looked,
        ReceiptLookup::Failed(problem) if problem.code() == ErrorCode::ParticipantBusy
    ));
    fixture.stop().await
}

#[tokio::test]
async fn hub_attachment_cache_reuses_bytes_refreshes_ttl_and_recovers_only_safe_damage()
-> TestResult {
    use std::{os::unix::fs::PermissionsExt, sync::atomic::AtomicUsize, time::Duration};

    use axum::{Router, routing::get};
    use cooplet_client::{ClientConfig, CoopletClient, Destination};
    use cooplet_platform::AdmissionGuard;
    use cooplet_protocol::{AttachmentRef, BlobId};
    use sha2::{Digest, Sha256};

    use super::relay::refresh_hub_attachments;

    let fixture = Fixture::start().await?;
    let now = crate::now_ms()?;
    fixture.clock.store(now, Ordering::SeqCst);
    let contents = b"retained attachment";
    let declared = AttachmentRef {
        blob_id: BlobId::generate()?,
        name: "readable plan.txt".into(),
        mime_type: "text/plain".into(),
        size_bytes: contents.len() as u64,
        sha256: format!("{:x}", Sha256::digest(contents)),
    };
    let gets = Arc::new(AtomicUsize::new(0));
    let handler_gets = gets.clone();
    let etag = format!("\"{}\"", declared.sha256);
    let app = Router::new().route(
        "/v1/blobs/{id}",
        get(move || {
            let etag = etag.clone();
            let gets = handler_gets.clone();
            async move {
                gets.fetch_add(1, Ordering::SeqCst);
                (
                    [("content-type", "text/plain".to_owned()), ("etag", etag)],
                    contents.as_slice(),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?).parse()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let hub = CoopletClient::new(ClientConfig::new(origin, Destination::LocalNode, None))?;
    let first = fixture
        .service
        .load_hub_attachments(
            &hub,
            std::slice::from_ref(&declared),
            &AdmissionGuard::new(Duration::from_secs(5)),
            4096,
        )
        .await?
        .pop()
        .ok_or("missing resolved attachment")?;
    let initial_id = first.local.blob_id.clone();
    let path = PathBuf::from(first.resolved().local_path.ok_or("missing cache path")?);
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("readable plan.txt")
    );
    assert_eq!(fs::read(&path)?, contents);
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    let advanced = now + 60_000;
    fixture.clock.store(advanced, Ordering::SeqCst);
    let warm = fixture
        .service
        .load_hub_attachments(
            &hub,
            std::slice::from_ref(&declared),
            &AdmissionGuard::new(Duration::from_secs(5)),
            4096,
        )
        .await?;
    let warm_id = warm
        .first()
        .ok_or("missing warm attachment")?
        .local
        .blob_id
        .clone();
    assert_eq!(warm_id, initial_id);
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    fixture
        .service
        .operations
        .finalize(move |tx| {
            refresh_hub_attachments(tx, &warm, advanced)?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let check = warm_id.clone();
    let expiry = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT expires_at_ms FROM blobs WHERE blob_id = ?1",
                [check.as_str()],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await?;
    assert_eq!(expiry, advanced + 7 * 24 * 60 * 60 * 1000);

    // Advance the fixture clock, leaving GC's actual clock behind the expiry.
    fixture.clock.store(expiry, Ordering::SeqCst);
    let reopened = fixture
        .service
        .load_hub_attachments(
            &hub,
            std::slice::from_ref(&declared),
            &AdmissionGuard::new(Duration::from_secs(5)),
            4096,
        )
        .await?
        .pop()
        .ok_or("missing refreshed attachment")?;
    assert_eq!(reopened.local.blob_id, initial_id);
    assert_eq!(gets.load(Ordering::SeqCst), 1);

    let permissions = fs::metadata(&path)?.permissions();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;
    assert!(
        fixture
            .service
            .load_hub_attachments(
                &hub,
                std::slice::from_ref(&declared),
                &AdmissionGuard::new(Duration::from_secs(5)),
                4096,
            )
            .await
            .is_err()
    );
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    let check = initial_id.clone();
    let live_after_bad_permissions = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT deleted_at_ms IS NULL FROM blobs WHERE blob_id = ?1",
                [check.as_str()],
                |row| row.get::<_, bool>(0),
            )?)
        })
        .await?;
    assert!(live_after_bad_permissions);

    fs::set_permissions(&path, permissions)?;
    fs::remove_file(&path)?;
    let replaced = fixture
        .service
        .load_hub_attachments(
            &hub,
            std::slice::from_ref(&declared),
            &AdmissionGuard::new(Duration::from_secs(5)),
            4096,
        )
        .await?
        .pop()
        .ok_or("missing replacement attachment")?;
    assert_ne!(replaced.local.blob_id, initial_id);
    assert_eq!(gets.load(Ordering::SeqCst), 2);
    let deletes = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM events WHERE kind = 'blob.deleted' AND aggregate_id = ?1",
                [initial_id.as_str()],
                |row| row.get::<_, u64>(0),
            )?)
        })
        .await?;
    assert_eq!(deletes, 1);

    let replaced_path = PathBuf::from(
        replaced
            .resolved()
            .local_path
            .ok_or("missing replacement path")?,
    );
    let sealed = fs::metadata(&replaced_path)?.permissions();
    fs::set_permissions(&replaced_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&replaced_path, vec![0_u8; contents.len()])?;
    fs::set_permissions(&replaced_path, sealed)?;
    fixture
        .clock
        .store(expiry + 7 * 24 * 60 * 60 * 1000, Ordering::SeqCst);
    let repaired = fixture
        .service
        .load_hub_attachments(
            &hub,
            std::slice::from_ref(&declared),
            &AdmissionGuard::new(Duration::from_secs(5)),
            4096,
        )
        .await?
        .pop()
        .ok_or("missing repaired attachment")?;
    assert_ne!(repaired.local.blob_id, replaced.local.blob_id);
    assert_eq!(gets.load(Ordering::SeqCst), 3);
    server.abort();
    fixture.stop().await
}

#[tokio::test]
async fn early_hub_reply_links_at_read_time_and_equal_local_id_is_not_a_party() -> TestResult {
    use std::time::Duration;

    use cooplet_client::{ClientConfig, CoopletClient, Destination};
    use cooplet_platform::AdmissionGuard;
    use cooplet_protocol::{AuthorKind, HubId, NodeId, OperationId};

    use super::InboundHubMessage;
    use crate::{clients::VerifiedAuthor, operations::DelegatedOperation};

    let fixture = Fixture::start().await?;
    let source = NodeId::generate()?;
    let assigned = NodeId::generate()?;
    let hub_id = HubId::generate()?;
    let colliding = fixture.stopped_agent().await?;
    let recipient = fixture.open.operator.participant_id.clone();
    let qualified = format!("{source}/{colliding}");
    let hub = CoopletClient::new(ClientConfig::new(
        "http://127.0.0.1:1".parse()?,
        Destination::LocalNode,
        None,
    ))?;
    let parent = MessageId::generate()?;
    let mut input = message(recipient.as_str(), "Arrived before the parent receipt");
    input.in_reply_to = Some(parent.clone());
    let context = DelegatedOperation {
        id: OperationId::generate()?,
        origin: format!("{hub_id}/{assigned}"),
        actor_id: qualified.clone(),
        admission: AdmissionGuard::new(Duration::from_secs(5)),
    };
    let incoming = fixture
        .service
        .send_delegated(
            &context,
            &hub,
            1024,
            InboundHubMessage {
                message_id: MessageId::generate()?,
                sender_id: qualified.clone(),
                author: VerifiedAuthor {
                    kind: AuthorKind::Agent,
                    actor_id: qualified.clone(),
                    name: "remote".into(),
                    source_node_id: Some(source),
                },
                input,
            },
        )
        .await?;
    let local_reply = incoming.message_id.ok_or("missing local reply")?;
    assert!(
        fixture
            .service
            .show(&fixture.operator()?, local_reply.clone())
            .await?
            .in_reply_to
            .is_none()
    );
    let local = fixture.principal(colliding, PrincipalKind::Participant, "same local ID")?;
    let denial = fixture
        .service
        .show(&local, local_reply.clone())
        .await
        .err()
        .ok_or("a colliding participant read a remote reply")?;
    assert_eq!(denial.code(), ErrorCode::NotFound);
    assert!(
        fixture
            .service
            .list(&local, None, None)
            .await?
            .items
            .is_empty()
    );

    let local_parent = MessageId::generate()?;
    let id = local_parent.clone();
    let remote = qualified.clone();
    fixture.open.engine.db().write_sqlite(move |tx| {
        tx.execute(
            "INSERT INTO messages (
               message_id, sender_id, recipient_id, body, delivery_mode, delivery_state, hub_message_id,
               created_at_ms, expires_at_ms
             ) VALUES (?1, ?2, ?3, 'original', 'direct_now', 'delivered', ?4, ?5, ?6)",
            params![id.as_str(), recipient.as_str(), remote, parent.as_str(), NOW, NOW + 60_000],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    let view = fixture
        .service
        .show(&fixture.operator()?, local_reply.clone())
        .await?;
    assert_eq!(view.sender_id, qualified);
    assert_eq!(view.in_reply_to, Some(local_parent));
    fixture.clock.store(NOW + 60_000, Ordering::SeqCst);
    assert!(
        fixture
            .service
            .show(&fixture.operator()?, local_reply)
            .await?
            .in_reply_to
            .is_none()
    );
    fixture.stop().await
}

#[tokio::test]
async fn reconcile_pending_leaves_delegated_answer_unknown() -> TestResult {
    use cooplet_protocol::OperationId;

    use crate::operations::Operation;

    let fixture = Fixture::start().await?;
    let agent = fixture.stopped_agent().await?;
    let actor_id = fixture.open.operator.participant_id.as_str().to_owned();
    let q_id = MessageId::generate()?;
    let ans_op_id = OperationId::generate()?;
    let ans_msg_id = MessageId::generate()?;
    let op = Operation {
        id: ans_op_id.clone(),
        actor_id: actor_id.clone(),
        key: "delegated-answer-key".into(),
        kind: "message.send",
        hash: "deleg-hash".into(),
        bridge_origin: Some("hub_x/node_y".into()),
    };
    {
        let op = op.clone();
        let q = q_id.clone();
        let ans = ans_msg_id.clone();
        let to = agent.clone();
        fixture
            .open
            .engine
            .db()
            .write_sqlite(move |tx| {
                tx.execute(
                    "INSERT INTO messages (message_id, sender_id, recipient_id, body, question_json,
                                           delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                     VALUES (?1, ?2, ?3, '', '{}', 'direct_now', 'delivered', ?4, ?5)",
                    params![q.as_str(), to.as_str(), op.actor_id, NOW, NOW + 10_000],
                )?;
                op.reserve(tx, NOW, Some(ans.as_str()))?;
                tx.execute(
                    "INSERT INTO messages (message_id, sender_id, recipient_id, in_reply_to, body, answer_json,
                                           delivery_mode, delivery_state, operation_id, created_at_ms, expires_at_ms)
                     VALUES (?1, ?2, ?3, ?4, '', '{\"text\":\"yes\"}', 'direct_now', 'accepted', ?5, ?6, ?7)",
                    params![
                        ans.as_str(),
                        op.actor_id,
                        to.as_str(),
                        q.as_str(),
                        op.id.as_str(),
                        NOW,
                        NOW + 10_000
                    ],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }
    let err = fixture
        .service
        .reconcile_pending(&op, ans_op_id.as_str(), Some(ans_msg_id.as_str()), true)
        .await
        .err()
        .ok_or("delegated answer reconciliation must return DeliveryUnknown")?;
    assert_eq!(err.code(), ErrorCode::DeliveryUnknown);
    let op_state: String = fixture
        .open
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT state FROM operations WHERE operation_id = ?1",
                [ans_op_id.as_str()],
                |r| r.get(0),
            )?)
        })
        .await?;
    assert_eq!(op_state, "unknown");
    fixture.stop().await
}
