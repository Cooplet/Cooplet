use std::{
    error::Error,
    fs, io,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{ErrorCode, HubId, NodeId, ServiceScope, TokenId};
use cooplet_storage::{StorageEngine, WriteOutcome};
use tokio::sync::{oneshot, watch};

use crate::{
    AppState,
    auth::{AuthenticatedCaller, authenticate},
    bridge::{BridgeRegistry, CommandPermit, ReserveError},
    error::HubError,
    now_ms, storage,
};

use super::{
    CatalogQuery, CreatePrincipalRequest, IdentityKind, IssueEnrollmentRequest, IssueTokenRequest,
    NodeCatalogQuery, RedeemEnrollmentRequest, create_principal, disable_principal,
    issue_enrollment, issue_token, list_enrollment, list_nodes, list_participants, list_tokens,
    redeem_enrollment, revoke_enrollment, revoke_node, revoke_token,
};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[derive(Default)]
struct RecordedBridge(Mutex<Vec<NodeId>>);

impl BridgeRegistry for RecordedBridge {
    fn reserve(&self, _node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        Err(ReserveError::Offline)
    }

    fn disconnect_revoked(&self, node_id: &NodeId) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(node_id.clone());
    }
}

struct TempState(PathBuf);

impl Drop for TempState {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    state: AppState,
    admin: AuthenticatedCaller,
    admin_token: TokenId,
    bridge: Arc<RecordedBridge>,
    engine: StorageEngine,
    shutdown: watch::Sender<()>,
    _root: TempState,
}

impl Fixture {
    async fn start() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let root = TempState(
            std::env::temp_dir().join(format!("cooplet-admin-test-{}", HubId::generate()?)),
        );
        let init = storage::init(&root.0, "Test Hub").await?;
        let token_path = init
            .token_path
            .ok_or_else(|| io::Error::other("initial token not issued"))?;
        let master_token = String::from_utf8(PrivateFs::read_private_file(&token_path, 4096)?)?;
        let storage::HubStorage {
            engine,
            meta,
            master,
        } = storage::open(&root.0, 4 * 1024 * 1024, 20 * 1024 * 1024, 16_384).await?;
        let (shutdown, stop) = watch::channel(());
        let bridge = Arc::new(RecordedBridge::default());
        let state = AppState::new(&engine, meta, master, bridge.clone(), stop, 4 * 1024 * 1024)?;
        let admin = authenticate(&state.reads, &state.master, master_token.trim()).await?;
        let admin_token = state
            .reads
            .read(|conn| {
                let value = conn.query_row(
                    "SELECT token_id FROM auth_tokens WHERE is_admin = 1",
                    [],
                    |row| row.get::<_, String>(0),
                )?;
                Ok(value)
            })
            .await?
            .parse()?;
        Ok(Self {
            state,
            admin,
            admin_token,
            bridge,
            engine,
            shutdown,
            _root: root,
        })
    }

    async fn stop(self) -> TestResult {
        self.shutdown.send_replace(());
        self.state.background.close();
        self.state.background.drain().await;
        self.engine.shutdown().await?;
        Ok(())
    }
}

fn rejected<T>(
    result: Result<T, HubError>,
    code: ErrorCode,
) -> Result<HubError, Box<dyn Error + Send + Sync>> {
    let error = result
        .err()
        .ok_or_else(|| io::Error::other("request unexpectedly succeeded"))?;
    assert_eq!(error.problem().code, code);
    Ok(error)
}

fn required<T>(value: Option<T>) -> Result<T, io::Error> {
    value.ok_or_else(|| io::Error::other("expected response field missing"))
}

#[test]
fn a_global_node_grant_requires_an_explicit_null() {
    let mut principal = serde_json::json!({
        "kind": "service", "username": "service", "display_name": "Reader", "scopes": ["read"],
    });
    let mut token = serde_json::json!({"label": "reader", "scopes": ["read"], "expires_at_ms": 42});
    assert!(serde_json::from_value::<CreatePrincipalRequest>(principal.clone()).is_err());
    assert!(serde_json::from_value::<IssueTokenRequest>(token.clone()).is_err());
    principal["allowed_nodes"] = serde_json::Value::Null;
    token["allowed_nodes"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<CreatePrincipalRequest>(principal).is_ok());
    assert!(serde_json::from_value::<IssueTokenRequest>(token).is_ok());
}

#[tokio::test]
async fn enrollment_is_single_use_and_recovery_identifies_the_revocable_node() -> TestResult {
    let fixture = Fixture::start().await?;
    let state = &fixture.state;
    let admin = &fixture.admin;
    let request = |ttl_ms| IssueEnrollmentRequest {
        name: "build host".into(),
        ttl_ms,
    };
    let (first, second) = tokio::join!(
        issue_enrollment(state, admin, "enroll-1", request(None)),
        issue_enrollment(state, admin, "enroll-1", request(Some(900_000))),
    );
    let first = first?;
    let second = second?;
    assert_ne!(first.already_issued, second.already_issued);
    assert_eq!(first.summary.token_id, second.summary.token_id);
    assert_eq!(first.summary.node_id, second.summary.node_id);
    let token_id = first.summary.token_id;
    let node_id = first.summary.node_id;
    let enroll_secret = required(first.token.or(second.token))?;

    let (first, second) = tokio::join!(
        redeem_enrollment(
            state,
            RedeemEnrollmentRequest {
                token: enroll_secret.clone(),
                name: None
            }
        ),
        redeem_enrollment(
            state,
            RedeemEnrollmentRequest {
                token: enroll_secret.clone(),
                name: None
            }
        ),
    );
    let enrolled = match (first, second) {
        (Ok(enrolled), Err(error)) | (Err(error), Ok(enrolled)) => {
            assert_eq!(error.problem().code, ErrorCode::Unauthorized);
            enrolled
        }
        _ => return Err(io::Error::other("exactly one enrollment must succeed").into()),
    };
    assert_eq!(enrolled.node_id, node_id);
    assert_eq!(enrolled.generation, 1);
    let node_identity = authenticate(&state.reads, &state.master, &enrolled.node_token).await?;
    assert_eq!(node_identity.actor_id(), node_id.as_str());
    rejected(
        list_nodes(
            state,
            &node_identity,
            NodeCatalogQuery {
                limit: None,
                after: None,
                node_id: None,
            },
        )
        .await,
        ErrorCode::Forbidden,
    )?;

    let effects = list_enrollment(
        state,
        admin,
        CatalogQuery {
            limit: Some(1),
            after: None,
        },
    )
    .await?;
    let summary = required(effects.items.first())?;
    assert_eq!(summary.summary.node_id, node_id);
    assert!(summary.consumed_at_ms.is_some());
    assert!(effects.next_after.is_none());

    let denied = revoke_enrollment(state, admin, "revoke-enrollment-1", token_id.clone()).await?;
    assert!(denied.already_consumed);
    assert!(!denied.revoked);
    assert_eq!(denied.node_id, node_id);
    let count = state
        .reads
        .read(move |conn| {
            let count = conn.query_row(
                "SELECT count(*) FROM operations WHERE instr(result_json, ?1) != 0",
                [&enroll_secret],
                |row| row.get::<_, i64>(0),
            )?;
            Ok(count)
        })
        .await?;
    assert_eq!(count, 0);

    let unconsumed = issue_enrollment(state, admin, "enroll-2", request(None)).await?;
    let next_principal = create_principal(
        state,
        admin,
        "next-admin",
        CreatePrincipalRequest {
            kind: IdentityKind::HumanAdmin,
            username: "replacement".into(),
            display_name: "Replacement administrator".into(),
            scopes: vec![ServiceScope::Manage],
            allowed_nodes: None,
            with_participant: None,
        },
    )
    .await?;
    let next_token = issue_token(
        state,
        admin,
        "next-credential",
        &next_principal.principal_id,
        IssueTokenRequest {
            label: "durable replacement".into(),
            scopes: vec![ServiceScope::Manage],
            allowed_nodes: None,
            is_admin: true,
            expires_at_ms: None,
        },
    )
    .await?;
    let next_secret = required(next_token.token)?;
    let next_admin = authenticate(&state.reads, &state.master, &next_secret).await?;
    disable_principal(state, &next_admin, "disable-issuer", admin.actor_id()).await?;

    let enrollments = list_enrollment(
        state,
        &next_admin,
        CatalogQuery {
            limit: None,
            after: None,
        },
    )
    .await?;
    let stored = |id: &TokenId| {
        enrollments
            .items
            .iter()
            .find(|entry| &entry.summary.token_id == id)
    };
    assert!(required(stored(&token_id))?.revoked_at_ms.is_none());
    assert!(
        required(stored(&unconsumed.summary.token_id))?
            .revoked_at_ms
            .is_some()
    );
    let pending_secret = required(unconsumed.token)?;
    rejected(
        redeem_enrollment(
            state,
            RedeemEnrollmentRequest {
                token: pending_secret,
                name: None,
            },
        )
        .await,
        ErrorCode::Unauthorized,
    )?;
    let enrolled_after_disable =
        authenticate(&state.reads, &state.master, &enrolled.node_token).await?;
    assert_eq!(enrolled_after_disable.actor_id(), node_id.as_str());

    revoke_node(state, &next_admin, "revoke-node-1", node_id.clone()).await?;
    revoke_node(state, &next_admin, "revoke-node-2", node_id.clone()).await?;
    assert_eq!(
        *fixture
            .bridge
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![node_id.clone()],
    );
    rejected(
        authenticate(&state.reads, &state.master, &enrolled.node_token).await,
        ErrorCode::NodeRevoked,
    )?;
    fixture.stop().await
}

#[tokio::test]
async fn durable_admin_and_service_grants_survive_credential_rotation() -> TestResult {
    let fixture = Fixture::start().await?;
    let state = &fixture.state;
    let admin = &fixture.admin;
    let error = rejected(
        revoke_token(
            state,
            admin,
            "revoke-original",
            admin.actor_id(),
            fixture.admin_token.clone(),
        )
        .await,
        ErrorCode::OperationConflict,
    )?;
    assert_eq!(error.problem().extensions["reason"], "last_durable_admin");

    let rotation = || IssueTokenRequest {
        label: "rotation".into(),
        scopes: vec![ServiceScope::Manage],
        allowed_nodes: None,
        is_admin: true,
        expires_at_ms: None,
    };
    let replacement =
        issue_token(state, admin, "issue-durable", admin.actor_id(), rotation()).await?;
    assert!(replacement.summary.durable);
    assert_eq!(
        serde_json::to_value(&replacement.summary)?["expires_at_ms"],
        serde_json::Value::Null
    );
    let replacement_secret = required(replacement.token)?;
    let new_admin = authenticate(&state.reads, &state.master, &replacement_secret).await?;
    revoke_token(
        state,
        &new_admin,
        "revoke-original",
        new_admin.actor_id(),
        fixture.admin_token.clone(),
    )
    .await?;
    let replay = issue_token(
        state,
        &new_admin,
        "issue-durable",
        new_admin.actor_id(),
        rotation(),
    )
    .await?;
    assert!(replay.already_issued);
    assert!(replay.token.is_none());
    assert_eq!(replay.summary.token_id, replacement.summary.token_id);

    let allowed = NodeId::generate()?;
    let other = NodeId::generate()?;
    let service = create_principal(
        state,
        &new_admin,
        "create-service",
        CreatePrincipalRequest {
            kind: IdentityKind::Service,
            username: "service".into(),
            display_name: "Service reader".into(),
            scopes: vec![ServiceScope::Collaborate],
            allowed_nodes: Some(vec![allowed.clone()]),
            with_participant: None,
        },
    )
    .await?;
    assert!(service.participant_id.is_none());
    let expires_at_ms = now_ms() + 60_000;
    let request = || IssueTokenRequest {
        label: "read-only".into(),
        scopes: vec![ServiceScope::Read],
        allowed_nodes: None,
        is_admin: false,
        expires_at_ms: Some(expires_at_ms),
    };
    let service_token = issue_token(
        state,
        &new_admin,
        "issue-service",
        &service.principal_id,
        request(),
    )
    .await?;
    let service_secret = required(service_token.token)?;
    let identity = authenticate(&state.reads, &state.master, &service_secret).await?;
    assert!(identity.allows_node(&allowed));
    assert!(!identity.allows_node(&other));
    rejected(
        list_participants(
            state,
            &identity,
            NodeCatalogQuery {
                limit: None,
                after: None,
                node_id: Some(other),
            },
        )
        .await,
        ErrorCode::Forbidden,
    )?;
    rejected(
        list_enrollment(
            state,
            &identity,
            CatalogQuery {
                limit: None,
                after: None,
            },
        )
        .await,
        ErrorCode::AdminHumanRequired,
    )?;
    disable_principal(state, &new_admin, "disable-service", &service.principal_id).await?;
    let replay = issue_token(
        state,
        &new_admin,
        "issue-service",
        &service.principal_id,
        request(),
    )
    .await?;
    assert!(replay.already_issued);
    assert!(replay.token.is_none());
    assert_eq!(replay.summary.token_id, service_token.summary.token_id);
    let error = rejected(
        issue_token(
            state,
            &new_admin,
            "reissue-service",
            &service.principal_id,
            request(),
        )
        .await,
        ErrorCode::OperationConflict,
    )?;
    assert_eq!(error.problem().extensions["reason"], "principal_disabled");
    rejected(
        authenticate(&state.reads, &state.master, &service_secret).await,
        ErrorCode::Unauthorized,
    )?;
    let records = list_tokens(
        state,
        &new_admin,
        &service.principal_id,
        CatalogQuery {
            limit: None,
            after: None,
        },
    )
    .await?;
    assert!(required(records.items.first())?.revoked_at_ms.is_some());

    fixture.stop().await
}

#[tokio::test]
async fn canceling_the_enclosing_request_does_not_lose_the_revocation_notification() -> TestResult {
    let fixture = Fixture::start().await?;
    let state = &fixture.state;
    let admin = &fixture.admin;
    let token = issue_token(
        state,
        admin,
        "issue-user",
        admin.actor_id(),
        IssueTokenRequest {
            label: "bot".into(),
            scopes: vec![ServiceScope::Read],
            allowed_nodes: None,
            is_admin: false,
            expires_at_ms: Some(now_ms() + 60_000),
        },
    )
    .await?;
    let token_id = token.summary.token_id.clone();
    let secret = required(token.token)?;
    let mut revocations = state.revocations.subscribe();

    // Keep the writer occupied so cancellation occurs before the revoke commits.
    let (entered, inside) = oneshot::channel();
    let (release, blocked) = mpsc::channel();
    let db = state.db.clone();
    let blocker = tokio::spawn(async move {
        db.write_sqlite(move |_| {
            let _ = entered.send(());
            let _ = blocked.recv();
            Ok(WriteOutcome::new(()))
        })
        .await
    });
    inside.await?;
    let request_state = state.clone();
    let request_admin = admin.clone();
    let request = tokio::spawn(async move {
        revoke_token(
            &request_state,
            &request_admin,
            "revoke-user",
            request_admin.actor_id(),
            token_id,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.background.0.borrow().active == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    request.abort();
    assert!(request.await.is_err_and(|error| error.is_cancelled()));
    release.send(())?;
    blocker.await??;
    state.background.drain().await;

    tokio::time::timeout(Duration::from_secs(5), revocations.changed()).await??;
    rejected(
        authenticate(&state.reads, &state.master, &secret).await,
        ErrorCode::Unauthorized,
    )?;
    fixture.stop().await
}

#[tokio::test]
async fn invalid_enrollment_redemption_rejects_in_read_path_without_blocking_writer() -> TestResult
{
    let fixture = Fixture::start().await?;
    let state = &fixture.state;
    let admin = &fixture.admin;
    let issued = issue_enrollment(
        state,
        admin,
        "enroll-revoked-precheck",
        IssueEnrollmentRequest {
            name: "Precheck node".into(),
            ttl_ms: None,
        },
    )
    .await?;
    let revoked_secret = required(issued.token)?;
    revoke_enrollment(
        state,
        admin,
        "revoke-before-redeem",
        issued.summary.token_id,
    )
    .await?;

    let (entered, inside) = oneshot::channel();
    let (release, blocked) = mpsc::channel();
    let db = state.db.clone();
    let blocker = tokio::spawn(async move {
        db.write_sqlite(move |_| {
            let _ = entered.send(());
            let _ = blocked.recv();
            Ok(WriteOutcome::new(()))
        })
        .await
    });
    inside.await?;

    for bad_token in [
        "not-a-valid-enrollment-token".to_owned(),
        format!("cooplet_enroll_{}_{}", TokenId::generate()?, "a".repeat(64)),
        revoked_secret,
    ] {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            redeem_enrollment(
                state,
                RedeemEnrollmentRequest {
                    token: bad_token,
                    name: None,
                },
            ),
        )
        .await?;
        rejected(result, ErrorCode::Unauthorized)?;
    }

    release.send(())?;
    blocker.await??;
    fixture.stop().await
}

#[tokio::test]
async fn disabling_sole_admin_with_multiple_durable_tokens_is_rejected_and_participant_cursors_work()
-> TestResult {
    let fixture = Fixture::start().await?;
    let state = &fixture.state;
    let admin = &fixture.admin;

    issue_token(
        state,
        admin,
        "second-durable-same-admin",
        admin.actor_id(),
        IssueTokenRequest {
            label: "backup admin".into(),
            scopes: vec![ServiceScope::Manage],
            allowed_nodes: None,
            is_admin: true,
            expires_at_ms: None,
        },
    )
    .await?;
    let error = rejected(
        disable_principal(state, admin, "disable-sole-admin", admin.actor_id()).await,
        ErrorCode::OperationConflict,
    )?;
    assert_eq!(error.problem().extensions["reason"], "last_durable_admin");

    let active_node = NodeId::generate()?;
    let revoked_node = NodeId::generate()?;
    let p1 = cooplet_protocol::ParticipantId::generate()?;
    let p2 = cooplet_protocol::ParticipantId::generate()?;
    let p_revoked = cooplet_protocol::ParticipantId::generate()?;
    let owner = admin.actor_id().to_owned();
    let now = now_ms();
    let active_for_db = active_node.clone();
    let revoked_for_db = revoked_node.clone();
    let p1_db = p1.clone();
    let p2_db = p2.clone();
    let pr_db = p_revoked.clone();
    state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO nodes (node_id, display_name, owner_principal_id, credential_hmac, status, enrolled_at_ms)
                 VALUES (?1, 'Active', ?2, ?1, 'online', ?3)",
                rusqlite::params![active_for_db.as_str(), owner, now],
            )?;
            tx.execute(
                "INSERT INTO nodes (node_id, display_name, owner_principal_id, credential_hmac, status, enrolled_at_ms, revoked_at_ms)
                 VALUES (?1, 'Revoked', ?2, ?1, 'revoked', ?3, ?3)",
                rusqlite::params![revoked_for_db.as_str(), owner, now],
            )?;
            for (node, part) in [
                (active_for_db.as_str(), p1_db.as_str()),
                (active_for_db.as_str(), p2_db.as_str()),
                (revoked_for_db.as_str(), pr_db.as_str()),
            ] {
                tx.execute(
                    "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                     VALUES (?1, ?2, 'agent', 'Agent', 'idle', 1, ?3)",
                    rusqlite::params![node, part, now],
                )?;
            }
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let first = list_participants(
        state,
        admin,
        NodeCatalogQuery {
            limit: Some(1),
            after: None,
            node_id: None,
        },
    )
    .await?;
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].node_id, active_node);
    let cursor = required(first.next_after)?;
    assert!(cursor.contains('.'));
    let second = list_participants(
        state,
        admin,
        NodeCatalogQuery {
            limit: Some(10),
            after: Some(cursor),
            node_id: None,
        },
    )
    .await?;
    assert_eq!(second.items.len(), 1);
    assert!(second.next_after.is_none());
    let mut seen = vec![
        first.items[0].participant_id.clone(),
        second.items[0].participant_id.clone(),
    ];
    seen.sort();
    let mut expected = vec![p1, p2];
    expected.sort();
    assert_eq!(seen, expected);
    fixture.stop().await
}
