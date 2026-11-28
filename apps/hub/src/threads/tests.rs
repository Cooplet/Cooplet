use std::{
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{
    ErrorCode, EventId, EventKind, MAX_THREAD_INITIAL_MEMBERS, QuestionMode, QuestionPayload,
    ServicePrincipalId, ThreadCreateRequest, ThreadListQuery, ThreadMemberJoinRequest,
    ThreadMemberLeaveRequest, ThreadMemberResumeRequest, ThreadMemberState, ThreadPinDeleteQuery,
    ThreadPinSetRequest, ThreadPostRequest, ThreadReadQuery, ThreadRole, ThreadStatus,
    ThreadWaitPolicy, ThreadWaitRequest, ThreadWaitResponse, ThreadWakePolicy, TokenId,
};
use cooplet_storage::{DEFAULT_MAX_BLOB_BYTES, StorageEngine, WriteOutcome};
use rusqlite::params;
use serde_json::json;
use tokio::sync::watch;

use crate::{
    AppState,
    auth::{self, AuthenticatedCaller},
    bridge::OfflineBridge,
    credentials::mint_service,
    storage,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const BASE_NOW: i64 = 1_800_000_000_000;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _scratch: Scratch,
    engine: StorageEngine,
    state: AppState,
    wall: Arc<AtomicI64>,
    admin: AuthenticatedCaller,
    shutdown: watch::Sender<()>,
}

impl Fixture {
    async fn new() -> TestResult<Self> {
        let root = std::env::temp_dir().join(EventId::generate()?.as_str());
        let scratch = Scratch(PrivateFs::ensure_private_dir(&root)?);
        let init = storage::init(&root, "Threads domain tests").await?;
        let token =
            PrivateFs::read_private_file(&init.token_path.ok_or("missing admin token")?, 512)?;
        let opened = storage::open(
            &root,
            DEFAULT_MAX_BLOB_BYTES,
            10 * 1024 * 1024 * 1024,
            1_048_576,
        )
        .await?;
        let (shutdown, signal) = watch::channel(());
        let mut state = AppState::new(
            &opened.engine,
            opened.meta,
            opened.master,
            opened.thread_acl_watch,
            Arc::new(OfflineBridge),
            signal,
            DEFAULT_MAX_BLOB_BYTES,
        )?;
        let wall = Arc::new(AtomicI64::new(BASE_NOW));
        let injected = wall.clone();
        state.wall_clock = Arc::new(move || injected.load(Ordering::Relaxed));
        let admin = auth::authenticate(
            &state.reads,
            &state.master,
            std::str::from_utf8(&token)?.trim(),
        )
        .await?;
        Ok(Self {
            _scratch: scratch,
            engine: opened.engine,
            state,
            wall,
            admin,
            shutdown,
        })
    }

    async fn service(&self, username: &str, scopes_json: &str) -> TestResult<AuthenticatedCaller> {
        let principal = ServicePrincipalId::generate()?;
        let token_id = TokenId::generate()?;
        let minted = mint_service(&self.state.master, &principal)?;
        let verifier = minted.verifier;
        let name = username.to_owned();
        let scopes = scopes_json.to_owned();
        let now = crate::now_ms();
        self.state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "INSERT INTO principals (principal_id, kind, username, display_name, scopes_json,
                        allowed_nodes_json, created_at_ms, updated_at_ms)
                     VALUES (?1, 'service', ?2, ?2, ?3, NULL, ?4, ?4)",
                    params![principal.as_str(), name, scopes, now],
                )?;
                tx.execute(
                    "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                        scopes_json, expires_at_ms, created_at_ms)
                     VALUES (?1, ?2, 'service_token', ?3, ?4, ?5, ?6)",
                    params![
                        token_id.as_str(),
                        principal.as_str(),
                        verifier,
                        scopes,
                        i64::MAX,
                        now
                    ],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        Ok(auth::authenticate(
            &self.state.reads,
            &self.state.master,
            minted.secret.as_str(),
        )
        .await?)
    }

    async fn member_update_count(&self, thread_id: &ThreadId) -> TestResult<u64> {
        let id = thread_id.clone();
        Ok(self
            .state
            .reads
            .read(move |db| {
                Ok(db.query_row(
                    "SELECT COUNT(*) FROM events WHERE kind = ?1 AND aggregate_id = ?2",
                    params![EventKind::ThreadMemberUpdated.as_str(), id.as_str()],
                    |row| row.get(0),
                )?)
            })
            .await?)
    }

    async fn close(self) -> TestResult {
        let Self {
            engine,
            state,
            shutdown,
            ..
        } = self;
        let _ = shutdown.send(());
        drop(state);
        engine.shutdown().await?;
        Ok(())
    }
}

fn post_req(body: &str) -> ThreadPostRequest {
    ThreadPostRequest {
        body: body.into(),
        mentions: Vec::new(),
        attachments: Vec::new(),
        question: None,
        answer: None,
        in_reply_to: None,
    }
}

fn err_code_reason<T>(result: Result<T, HubError>, code: ErrorCode, reason: &str) -> TestResult {
    let Err(error) = result else {
        return Err(format!("expected {code:?} ({reason}), got Ok").into());
    };
    assert_eq!(error.problem().code, code);
    assert_eq!(
        error
            .problem()
            .extensions
            .get("reason")
            .and_then(|v| v.as_str()),
        Some(reason)
    );
    Ok(())
}

#[tokio::test]
async fn thread_lifecycle_roles_conflicts_rejoin_and_idempotent_leave() -> TestResult {
    let fixture = Fixture::new().await?;
    let alice = fixture
        .service("alice", r#"["read","collaborate"]"#)
        .await?;
    let bob = fixture.service("bob", r#"["read","collaborate"]"#).await?;

    let board = create(
        &fixture.state,
        &alice,
        "create-lifecycle",
        ThreadCreateRequest {
            title: "Lifecycle".into(),
            topic: Some("Checks".into()),
            creator_handle: Some("alice".into()),
            members: vec![ThreadMemberJoinRequest {
                member_key: bob.actor_id().into(),
                handle: Some("bob".into()),
                role: Some(ThreadRole::Member),
                wake_policy: None,
                delivery_mode: None,
            }],
        },
    )
    .await?;
    let id = board.thread_id;

    // already_member
    err_code_reason(
        join(
            &fixture.state,
            &alice,
            id.clone(),
            "join-duplicate",
            ThreadMemberJoinRequest {
                member_key: bob.actor_id().into(),
                handle: Some("bob".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            },
        )
        .await,
        ErrorCode::OperationConflict,
        "already_member",
    )?;

    // last_owner cannot leave while bob remains
    err_code_reason(
        leave(
            &fixture.state,
            &alice,
            id.clone(),
            "alice-last-owner",
            ThreadMemberLeaveRequest { member_key: None },
        )
        .await,
        ErrorCode::OperationConflict,
        "last_owner",
    )?;

    // not_paused on active member
    err_code_reason(
        resume(
            &fixture.state,
            &alice,
            id.clone(),
            "resume-active",
            ThreadMemberResumeRequest {
                member_key: Some(bob.actor_id().into()),
                after_seq: 0,
            },
        )
        .await,
        ErrorCode::OperationConflict,
        "not_paused",
    )?;

    // Bob leaves -> idempotent replay succeeds (200), key reuse with different body -> 409 (not 404).
    let left_first = leave(
        &fixture.state,
        &bob,
        id.clone(),
        "bob-self-leave",
        ThreadMemberLeaveRequest { member_key: None },
    )
    .await?;
    assert_eq!(left_first.state, ThreadMemberState::Left);
    let left_replay = leave(
        &fixture.state,
        &bob,
        id.clone(),
        "bob-self-leave",
        ThreadMemberLeaveRequest { member_key: None },
    )
    .await?;
    assert_eq!(left_replay.state, ThreadMemberState::Left);
    assert_eq!(left_replay.member_key, bob.actor_id());

    let key_conflict = leave(
        &fixture.state,
        &bob,
        id.clone(),
        "bob-self-leave",
        ThreadMemberLeaveRequest {
            member_key: Some(alice.actor_id().into()),
        },
    )
    .await
    .err()
    .ok_or("expected 409 on leave key conflict")?;
    assert_eq!(key_conflict.problem().code, ErrorCode::OperationConflict);

    // already_left when owner tries to remove bob again
    err_code_reason(
        leave(
            &fixture.state,
            &alice,
            id.clone(),
            "remove-bob-again",
            ThreadMemberLeaveRequest {
                member_key: Some(bob.actor_id().into()),
            },
        )
        .await,
        ErrorCode::OperationConflict,
        "already_left",
    )?;

    // Rejoin validates handle: invalid -> 400, different -> 409 handle_conflict, same -> 200
    let bad_handle = join(
        &fixture.state,
        &alice,
        id.clone(),
        "rejoin-bad-handle",
        ThreadMemberJoinRequest {
            member_key: bob.actor_id().into(),
            handle: Some("all".into()),
            role: None,
            wake_policy: None,
            delivery_mode: None,
        },
    )
    .await
    .err()
    .ok_or("expected 400 on invalid rejoin handle")?;
    assert_eq!(bad_handle.problem().code, ErrorCode::InvalidRequest);

    err_code_reason(
        join(
            &fixture.state,
            &alice,
            id.clone(),
            "rejoin-wrong-handle",
            ThreadMemberJoinRequest {
                member_key: bob.actor_id().into(),
                handle: Some("robert".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            },
        )
        .await,
        ErrorCode::OperationConflict,
        "handle_conflict",
    )?;

    let rejoined = join(
        &fixture.state,
        &alice,
        id.clone(),
        "rejoin-same-handle",
        ThreadMemberJoinRequest {
            member_key: bob.actor_id().into(),
            handle: Some("BOB".into()),
            role: None,
            wake_policy: None,
            delivery_mode: None,
        },
    )
    .await?;
    assert_eq!(rejoined.state, ThreadMemberState::Active);
    assert_eq!(rejoined.handle, "bob");

    // Closing makes the thread strictly read-only, including forbidding leave (409 thread_closed).
    let closed = close(&fixture.state, &alice, id.clone(), "close-board").await?;
    assert_eq!(closed.status, ThreadStatus::Closed);
    err_code_reason(
        leave(
            &fixture.state,
            &bob,
            id.clone(),
            "leave-closed-thread",
            ThreadMemberLeaveRequest { member_key: None },
        )
        .await,
        ErrorCode::OperationConflict,
        "thread_closed",
    )?;
    err_code_reason(
        post(
            &fixture.state,
            &alice,
            id.clone(),
            "post-closed-thread",
            post_req("nope"),
        )
        .await,
        ErrorCode::OperationConflict,
        "thread_closed",
    )?;
    err_code_reason(
        wait(
            &fixture.state,
            &alice,
            id.clone(),
            ThreadWaitRequest {
                since_seq: 0,
                wake_policy: None,
            },
        )
        .await,
        ErrorCode::OperationConflict,
        "thread_closed",
    )?;

    // Auto-close when the sole remaining member leaves an open thread.
    let solo = create(
        &fixture.state,
        &alice,
        "create-solo",
        ThreadCreateRequest {
            title: "Solo".into(),
            topic: None,
            creator_handle: Some("alice".into()),
            members: Vec::new(),
        },
    )
    .await?;
    let sole_left = leave(
        &fixture.state,
        &alice,
        solo.thread_id.clone(),
        "solo-leave",
        ThreadMemberLeaveRequest { member_key: None },
    )
    .await?;
    assert_eq!(sole_left.state, ThreadMemberState::Left);
    let summary = get(&fixture.state, &fixture.admin, solo.thread_id).await?;
    assert_eq!(summary.status, ThreadStatus::Closed);
    assert!(summary.closed_at_ms.is_some());

    fixture.close().await
}

#[tokio::test]
async fn thread_scopes_wait_context_observer_muted_and_resync_extensions() -> TestResult {
    let fixture = Fixture::new().await?;
    let reader_only = fixture.service("reader", r#"["read"]"#).await?;
    let observer = fixture
        .service("observer", r#"["read","collaborate"]"#)
        .await?;
    let worker = fixture
        .service("worker", r#"["read","collaborate"]"#)
        .await?;
    let muted = fixture
        .service("muted", r#"["read","collaborate"]"#)
        .await?;

    let board = create(
        &fixture.state,
        &fixture.admin,
        "create-scopes",
        ThreadCreateRequest {
            title: "Scopes".into(),
            topic: None,
            creator_handle: Some("admin".into()),
            members: vec![
                ThreadMemberJoinRequest {
                    member_key: reader_only.actor_id().into(),
                    handle: Some("reader".into()),
                    role: Some(ThreadRole::Member),
                    wake_policy: None,
                    delivery_mode: None,
                },
                ThreadMemberJoinRequest {
                    member_key: observer.actor_id().into(),
                    handle: Some("observer".into()),
                    role: Some(ThreadRole::Observer),
                    wake_policy: None,
                    delivery_mode: None,
                },
                ThreadMemberJoinRequest {
                    member_key: worker.actor_id().into(),
                    handle: Some("worker".into()),
                    role: Some(ThreadRole::Member),
                    wake_policy: Some(ThreadWakePolicy::MentionsOnly),
                    delivery_mode: None,
                },
                ThreadMemberJoinRequest {
                    member_key: muted.actor_id().into(),
                    handle: Some("muted".into()),
                    role: Some(ThreadRole::Member),
                    wake_policy: Some(ThreadWakePolicy::Muted),
                    delivery_mode: None,
                },
            ],
        },
    )
    .await?;
    let id = board.thread_id;

    // Read-only service principal is forbidden on both wait and self-leave.
    assert_eq!(
        wait(
            &fixture.state,
            &reader_only,
            id.clone(),
            ThreadWaitRequest {
                since_seq: 0,
                wake_policy: None,
            },
        )
        .await
        .err()
        .ok_or("expected 403 on read-only wait")?
        .problem()
        .code,
        ErrorCode::Forbidden
    );
    assert_eq!(
        leave(
            &fixture.state,
            &reader_only,
            id.clone(),
            "read-only-leave",
            ThreadMemberLeaveRequest { member_key: None },
        )
        .await
        .err()
        .ok_or("expected 403 on read-only leave")?
        .problem()
        .code,
        ErrorCode::Forbidden
    );

    // Observer with collaborate scope is forbidden on post/pin/wait, allowed to leave.
    assert_eq!(
        post(
            &fixture.state,
            &observer,
            id.clone(),
            "observer-post",
            post_req("hi"),
        )
        .await
        .err()
        .ok_or("expected 403 on observer post")?
        .problem()
        .code,
        ErrorCode::Forbidden
    );
    assert_eq!(
        wait(
            &fixture.state,
            &observer,
            id.clone(),
            ThreadWaitRequest {
                since_seq: 0,
                wake_policy: None,
            },
        )
        .await
        .err()
        .ok_or("expected 403 on observer wait")?
        .problem()
        .code,
        ErrorCode::Forbidden
    );
    let obs_left = leave(
        &fixture.state,
        &observer,
        id.clone(),
        "observer-leave",
        ThreadMemberLeaveRequest { member_key: None },
    )
    .await?;
    assert_eq!(obs_left.state, ThreadMemberState::Left);

    // Post #1 mentions worker; Post #2 is unaddressed context.
    // wait(since_seq=0) under mentions_only must return BOTH posts up to head_seq=2.
    let first = post(
        &fixture.state,
        &fixture.admin,
        id.clone(),
        "scope-post-1",
        post_req("@worker look here"),
    )
    .await?;
    let second = post(
        &fixture.state,
        &fixture.admin,
        id.clone(),
        "scope-post-2",
        post_req("additional context after mention"),
    )
    .await?;
    assert_eq!((first.seq, second.seq), (1, 2));

    let before_events = fixture.member_update_count(&id).await?;
    let ready = wait(
        &fixture.state,
        &worker,
        id.clone(),
        ThreadWaitRequest {
            since_seq: 0,
            wake_policy: Some(ThreadWaitPolicy::MentionsOnly),
        },
    )
    .await?;
    match ready {
        ThreadWaitResponse::Ready {
            head_seq,
            posts,
            has_more,
            ..
        } => {
            assert_eq!(head_seq, 2);
            assert!(!has_more);
            assert_eq!(posts.iter().map(|p| p.seq).collect::<Vec<_>>(), vec![1, 2]);
        }
        other => return Err(format!("expected Ready, got {other:?}").into()),
    }
    // No-op wake_policy without advancing confirmed_seq must not emit ThreadMemberUpdated.
    assert_eq!(fixture.member_update_count(&id).await?, before_events);

    // Changing wake_policy emits ThreadMemberUpdated with confirmed_seq and delivery_generation.
    let _ = wait(
        &fixture.state,
        &worker,
        id.clone(),
        ThreadWaitRequest {
            since_seq: 2,
            wake_policy: Some(ThreadWaitPolicy::All),
        },
    )
    .await?;
    assert_eq!(fixture.member_update_count(&id).await?, before_events + 1);
    let payload: serde_json::Value = fixture
        .state
        .reads
        .read({
            let id = id.clone();
            move |db| {
                let raw: String = db.query_row(
                    "SELECT payload_json FROM events WHERE kind = ?1 AND aggregate_id = ?2 ORDER BY seq DESC LIMIT 1",
                    params![EventKind::ThreadMemberUpdated.as_str(), id.as_str()],
                    |row| row.get(0),
                )?;
                Ok(serde_json::from_str::<serde_json::Value>(&raw).unwrap_or_default())
            }
        })
        .await?;
    assert_eq!(payload["data"]["confirmed_seq"], 2);
    assert_eq!(payload["data"]["delivery_generation"], 0);

    // Muted member calling wait without policy stays Waiting, advances confirmed_seq,
    // and emits ThreadMemberUpdated; repeating with unchanged confirmed_seq emits nothing.
    let before_muted_wait = fixture.member_update_count(&id).await?;
    let muted_wait = wait(
        &fixture.state,
        &muted,
        id.clone(),
        ThreadWaitRequest {
            since_seq: 1,
            wake_policy: None,
        },
    )
    .await?;
    assert!(matches!(
        muted_wait,
        ThreadWaitResponse::Waiting { head_seq: 2 }
    ));
    assert_eq!(
        fixture.member_update_count(&id).await?,
        before_muted_wait + 1
    );
    let muted_wait_noop = wait(
        &fixture.state,
        &muted,
        id.clone(),
        ThreadWaitRequest {
            since_seq: 1,
            wake_policy: None,
        },
    )
    .await?;
    assert!(matches!(
        muted_wait_noop,
        ThreadWaitResponse::Waiting { head_seq: 2 }
    ));
    assert_eq!(
        fixture.member_update_count(&id).await?,
        before_muted_wait + 1
    );

    // 410 ResyncRequired includes thread_id, floor_seq, head_seq, reason="cursor_pruned".
    fixture
        .wall
        .store(BASE_NOW + RETENTION_MS + 1, Ordering::Relaxed);
    let resync = read(
        &fixture.state,
        &worker,
        id.clone(),
        ThreadReadQuery {
            since_seq: Some(0),
            ..ThreadReadQuery::default()
        },
    )
    .await
    .err()
    .ok_or("expected 410 ResyncRequired")?;
    assert_eq!(resync.problem().code, ErrorCode::ResyncRequired);
    assert_eq!(resync.problem().extensions["thread_id"], id.as_str());
    assert_eq!(resync.problem().extensions["floor_seq"], 2);
    assert_eq!(resync.problem().extensions["head_seq"], 2);
    assert_eq!(resync.problem().extensions["reason"], "cursor_pruned");

    fixture.close().await
}

#[tokio::test]
async fn thread_limits_pins_mentions_and_question_expiry_are_enforced() -> TestResult {
    let fixture = Fixture::new().await?;
    let alice = fixture
        .service("alice", r#"["read","collaborate"]"#)
        .await?;

    // > 64 initial members rejected with 400.
    let too_many: Vec<ThreadMemberJoinRequest> = (0..=MAX_THREAD_INITIAL_MEMBERS)
        .map(|i| ThreadMemberJoinRequest {
            member_key: format!("sp_{i}"),
            handle: None,
            role: None,
            wake_policy: None,
            delivery_mode: None,
        })
        .collect();
    assert_eq!(
        create(
            &fixture.state,
            &fixture.admin,
            "too-many-members",
            ThreadCreateRequest {
                title: "Too many".into(),
                topic: None,
                creator_handle: None,
                members: too_many,
            },
        )
        .await
        .err()
        .ok_or("expected 400 for > 64 members")?
        .problem()
        .code,
        ErrorCode::InvalidRequest
    );

    // > 8 KiB title + topic rejected with 413.
    assert_eq!(
        create(
            &fixture.state,
            &fixture.admin,
            "oversized-title",
            ThreadCreateRequest {
                title: "t".repeat(4096),
                topic: Some("x".repeat(5000)),
                creator_handle: None,
                members: Vec::new(),
            },
        )
        .await
        .err()
        .ok_or("expected 413 for > 8 KiB title+topic")?
        .problem()
        .code,
        ErrorCode::PayloadTooLarge
    );

    let board = create(
        &fixture.state,
        &fixture.admin,
        "limits-board",
        ThreadCreateRequest {
            title: "Limits".into(),
            topic: None,
            creator_handle: Some("admin".into()),
            members: vec![ThreadMemberJoinRequest {
                member_key: alice.actor_id().into(),
                handle: Some("alice".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            }],
        },
    )
    .await?;
    let id = board.thread_id;

    // > 64 KiB post rejected with 413.
    assert_eq!(
        post(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            "oversized-post",
            post_req(&"p".repeat(65 * 1024)),
        )
        .await
        .err()
        .ok_or("expected 413 for > 64 KiB post")?
        .problem()
        .code,
        ErrorCode::PayloadTooLarge
    );

    // Question with expires_at_ms <= now rejected with 400.
    let mut expired_q = post_req("Expired question");
    expired_q.question = Some(QuestionPayload {
        prompt: "Too late?".into(),
        mode: QuestionMode::Text,
        options: Vec::new(),
        allow_free_text: false,
        expires_at_ms: Some(BASE_NOW),
    });
    assert_eq!(
        post(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            "expired-q",
            expired_q
        )
        .await
        .err()
        .ok_or("expected 400 for expired question")?
        .problem()
        .code,
        ErrorCode::InvalidRequest
    );

    // Explicit unknown mention -> 400; body @unknown ignored as text; body @alice resolves.
    let mut bad_mention = post_req("hello");
    bad_mention.mentions = vec!["nobody".into()];
    assert_eq!(
        post(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            "bad-mention",
            bad_mention
        )
        .await
        .err()
        .ok_or("expected 400 for unknown explicit mention")?
        .problem()
        .code,
        ErrorCode::InvalidRequest
    );
    post(
        &fixture.state,
        &fixture.admin,
        id.clone(),
        "good-mention",
        post_req("hi @alice and @nobody email a@alice.dev"),
    )
    .await?;
    let page = read(
        &fixture.state,
        &fixture.admin,
        id.clone(),
        ThreadReadQuery::default(),
    )
    .await?;
    assert_eq!(page.posts.len(), 1);
    assert_eq!(page.posts[0].mentions, vec![alice.actor_id().to_owned()]);

    // Pins: invalid keys `.` / `..`, delete missing -> version 0, 32 max + 33rd -> 400, >64 KiB -> 413.
    for invalid_key in [".", ".."] {
        assert_eq!(
            set_pin(
                &fixture.state,
                &fixture.admin,
                id.clone(),
                invalid_key.into(),
                &format!("bad-pin-{invalid_key}"),
                ThreadPinSetRequest {
                    value_json: json!(1),
                    post_id: None,
                    expected_version: None,
                },
            )
            .await
            .err()
            .ok_or("expected 400 for invalid pin key")?
            .problem()
            .code,
            ErrorCode::InvalidRequest
        );
    }
    let missing_del = delete_pin(
        &fixture.state,
        &fixture.admin,
        id.clone(),
        "absent".into(),
        "del-missing-pin",
        ThreadPinDeleteQuery {
            expected_version: Some(0),
        },
    )
    .await?;
    assert_eq!((missing_del.deleted, missing_del.version), (true, 0));

    for i in 0..32 {
        set_pin(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            format!("pin-{i:02}"),
            &format!("set-pin-{i:02}"),
            ThreadPinSetRequest {
                value_json: json!(i),
                post_id: None,
                expected_version: Some(0),
            },
        )
        .await?;
    }
    // 33rd pin fails with 400; updating an existing pin beyond 64 KiB fails with 413.
    assert_eq!(
        set_pin(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            "pin-32".into(),
            "set-pin-32",
            ThreadPinSetRequest {
                value_json: json!(32),
                post_id: None,
                expected_version: None,
            },
        )
        .await
        .err()
        .ok_or("expected 400 for 33rd pin")?
        .problem()
        .code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        set_pin(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            "pin-00".into(),
            "oversized-pin",
            ThreadPinSetRequest {
                value_json: json!("x".repeat(65 * 1024)),
                post_id: None,
                expected_version: None,
            },
        )
        .await
        .err()
        .ok_or("expected 413 for > 64 KiB pins")?
        .problem()
        .code,
        ErrorCode::PayloadTooLarge
    );

    // 192 KiB page budget splits large posts cleanly across pages.
    for i in 0..6 {
        post(
            &fixture.state,
            &fixture.admin,
            id.clone(),
            &format!("large-post-{i}"),
            post_req(&"b".repeat(50 * 1024)),
        )
        .await?;
    }
    let p1 = read(
        &fixture.state,
        &fixture.admin,
        id.clone(),
        ThreadReadQuery {
            since_seq: Some(0),
            limit: Some(100),
            include_pins: false,
        },
    )
    .await?;
    assert!(p1.has_more);
    assert!(p1.posts.len() < 7);
    assert!(serde_json::to_vec(&p1)?.len() <= MAX_PAGE_BYTES);
    let list_page = list(&fixture.state, &fixture.admin, ThreadListQuery::default()).await?;
    assert_eq!(list_page.items.len(), 1);

    fixture.close().await
}

#[tokio::test]
async fn hub_native_member_resolve_enforces_allowed_nodes_on_create_and_join() -> TestResult {
    use cooplet_protocol::ParticipantId;

    let fixture = Fixture::new().await?;
    let node_a = NodeId::generate()?;
    let node_b = NodeId::generate()?;
    let agent_a = ParticipantId::generate()?;
    let owner = fixture.admin.actor_id().to_owned();
    let seed = (node_a.clone(), node_b.clone(), agent_a.clone(), owner);
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            for node in [&seed.0, &seed.1] {
                tx.execute(
                    "INSERT INTO nodes (node_id, display_name, owner_principal_id, credential_hmac,
                        session_epoch, status, enrolled_at_ms)
                     VALUES (?1, 'Worker', ?2, ?1, 1, 'online', 1)",
                    params![node.as_str(), seed.3],
                )?;
            }
            tx.execute(
                "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                 VALUES (?1, ?2, 'agent', 'WorkerA', 'idle', 1, 1)",
                params![seed.0.as_str(), seed.2.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let make_scoped_service = |name: &str, allowed_nodes: Vec<NodeId>| {
        let state = fixture.state.clone();
        let name = name.to_owned();
        async move {
            let principal = ServicePrincipalId::generate()?;
            let token_id = TokenId::generate()?;
            let minted = mint_service(&state.master, &principal)?;
            let verifier = minted.verifier;
            let allowed = serde_json::to_string(&allowed_nodes)?;
            let now = crate::now_ms();
            state
                .db
                .write_sqlite(move |tx| {
                    tx.execute(
                        "INSERT INTO principals (principal_id, kind, username, display_name, scopes_json,
                            allowed_nodes_json, created_at_ms, updated_at_ms)
                         VALUES (?1, 'service', ?2, ?2, '[\"read\",\"collaborate\"]', ?3, ?4, ?4)",
                        params![principal.as_str(), name, allowed, now],
                    )?;
                    tx.execute(
                        "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                            scopes_json, allowed_nodes_json, expires_at_ms, created_at_ms)
                         VALUES (?1, ?2, 'service_token', ?3, '[\"read\",\"collaborate\"]', ?4, ?5, ?6)",
                        params![
                            token_id.as_str(),
                            principal.as_str(),
                            verifier,
                            allowed,
                            i64::MAX,
                            now
                        ],
                    )?;
                    Ok(WriteOutcome::new(()))
                })
                .await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(
                auth::authenticate(&state.reads, &state.master, minted.secret.as_str()).await?,
            )
        }
    };

    let svc_only_b = make_scoped_service("svc_only_b", vec![node_b.clone()]).await?;
    let svc_has_a = make_scoped_service("svc_has_a", vec![node_a.clone()]).await?;
    let agent_key = format!("{node_a}/{agent_a}");

    // Creating a thread with svc_only_b listed FIRST before node_a agent still returns 404.
    assert_eq!(
        create(
            &fixture.state,
            &fixture.admin,
            "create-cross-scope",
            ThreadCreateRequest {
                title: "Scoped board".into(),
                topic: None,
                creator_handle: None,
                members: vec![
                    ThreadMemberJoinRequest {
                        member_key: svc_only_b.actor_id().into(),
                        handle: Some("bot_b".into()),
                        role: None,
                        wake_policy: None,
                        delivery_mode: None,
                    },
                    ThreadMemberJoinRequest {
                        member_key: agent_key.clone(),
                        handle: Some("worker_a".into()),
                        role: None,
                        wake_policy: None,
                        delivery_mode: None,
                    },
                ],
            },
        )
        .await
        .err()
        .ok_or("expected 404 when adding out-of-scope hub-native service on create")?
        .problem()
        .code,
        ErrorCode::NotFound
    );

    // Pre-validation rejects malformed keys (400) and unauthorized caller nodes (403)
    // before resolving any hub-native principal (preventing allowed_nodes oracles).
    assert_eq!(
        create(
            &fixture.state,
            &fixture.admin,
            "create-malformed-second",
            ThreadCreateRequest {
                title: "Malformed second".into(),
                topic: None,
                creator_handle: None,
                members: vec![
                    ThreadMemberJoinRequest {
                        member_key: svc_only_b.actor_id().into(),
                        handle: Some("bot_b".into()),
                        role: None,
                        wake_policy: None,
                        delivery_mode: None,
                    },
                    ThreadMemberJoinRequest {
                        member_key: "invalid/invalid".into(),
                        handle: Some("bad".into()),
                        role: None,
                        wake_policy: None,
                        delivery_mode: None,
                    },
                ],
            },
        )
        .await
        .err()
        .ok_or("expected 400 for malformed second member key")?
        .problem()
        .code,
        ErrorCode::InvalidRequest
    );
    let fake_node_b_key = format!("{node_b}/{}", ParticipantId::generate()?);
    let nonexistent_sp = ServicePrincipalId::generate()?;
    let svc_also_a = make_scoped_service("svc_also_a", vec![node_a.clone()]).await?;
    for (suffix, first_member) in [
        ("existing-allows-b", svc_only_b.actor_id().to_owned()),
        ("existing-only-a", svc_also_a.actor_id().to_owned()),
        ("nonexistent-sp", nonexistent_sp.as_str().to_owned()),
    ] {
        assert_eq!(
            create(
                &fixture.state,
                &svc_has_a,
                &format!("create-unauth-node-oracle-{suffix}"),
                ThreadCreateRequest {
                    title: "Unauth node".into(),
                    topic: None,
                    creator_handle: None,
                    members: vec![
                        ThreadMemberJoinRequest {
                            member_key: first_member,
                            handle: Some("bot_probe".into()),
                            role: None,
                            wake_policy: None,
                            delivery_mode: None,
                        },
                        ThreadMemberJoinRequest {
                            member_key: fake_node_b_key.clone(),
                            handle: Some("worker_b".into()),
                            role: None,
                            wake_policy: None,
                            delivery_mode: None,
                        },
                    ],
                },
            )
            .await
            .err()
            .ok_or("expected 403 for unauthorized node before resolving hub-native member")?
            .problem()
            .code,
            ErrorCode::Forbidden
        );
    }

    // Creating with agent_a succeeds, then joining svc_only_b with invalid handle returns 400
    // (before principal lookup), valid handle returns 404, while svc_has_a succeeds.
    let board = create(
        &fixture.state,
        &fixture.admin,
        "create-node-a-board",
        ThreadCreateRequest {
            title: "Node A board".into(),
            topic: None,
            creator_handle: None,
            members: vec![ThreadMemberJoinRequest {
                member_key: agent_key,
                handle: Some("worker_a".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            }],
        },
    )
    .await?;
    assert_eq!(
        join(
            &fixture.state,
            &fixture.admin,
            board.thread_id.clone(),
            "join-invalid-handle-before-lookup",
            ThreadMemberJoinRequest {
                member_key: svc_only_b.actor_id().into(),
                handle: Some("all".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            },
        )
        .await
        .err()
        .ok_or("expected 400 for reserved handle before principal lookup")?
        .problem()
        .code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        join(
            &fixture.state,
            &fixture.admin,
            board.thread_id.clone(),
            "join-out-of-scope-svc",
            ThreadMemberJoinRequest {
                member_key: svc_only_b.actor_id().into(),
                handle: Some("bot_b".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            },
        )
        .await
        .err()
        .ok_or("expected 404 when joining out-of-scope hub-native service")?
        .problem()
        .code,
        ErrorCode::NotFound
    );
    let joined = join(
        &fixture.state,
        &fixture.admin,
        board.thread_id,
        "join-in-scope-svc",
        ThreadMemberJoinRequest {
            member_key: svc_has_a.actor_id().into(),
            handle: Some("bot_a".into()),
            role: None,
            wake_policy: None,
            delivery_mode: None,
        },
    )
    .await?;
    assert_eq!(joined.member_key, svc_has_a.actor_id());
    fixture.close().await
}
