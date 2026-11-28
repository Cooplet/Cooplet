use std::{error::Error, fs, path::PathBuf, sync::Arc, time::Duration};

use axum::body::to_bytes;
use bytes::Bytes;
use cooplet_client::{
    IDEMPOTENCY_KEY, X_COOPLET_FILENAME, X_COOPLET_PARTICIPANT, X_COOPLET_SHA256,
};
use cooplet_platform::PrivateFs;
use cooplet_protocol::{EventId, ParticipantId, ThreadId};
use cooplet_storage::{StorageEngine, WriteOutcome};
use futures_util::stream;
use http::{HeaderMap, StatusCode};
use tokio::sync::{Barrier, oneshot, watch};

use crate::{
    auth,
    bridge::OfflineBridge,
    credentials::mint_node,
    now_ms,
    storage::{
        self,
        events::{EventAudience, StoredEvent},
    },
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const LIMIT: u64 = 8;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    engine: StorageEngine,
    state: AppState,
    admin: AuthenticatedCaller,
    shutdown: watch::Sender<()>,
    _scratch: Scratch,
}

impl Fixture {
    async fn new(quota: u64) -> TestResult<Self> {
        let root = std::env::temp_dir().join(EventId::generate()?.as_str());
        let scratch = Scratch(PrivateFs::ensure_private_dir(&root)?);
        let initial = storage::init(&root, "Blob fixture").await?;
        let token = PrivateFs::read_private_file(
            &initial.token_path.ok_or("missing initial admin token")?,
            512,
        )?;
        let ready = storage::open(&root, LIMIT, quota, 1_048_576).await?;
        let (shutdown, stopped) = watch::channel(());
        let state = AppState::new(
            &ready.engine,
            ready.meta,
            ready.master,
            ready.thread_acl_watch,
            Arc::new(OfflineBridge),
            stopped,
            LIMIT,
        )?;
        let admin = auth::authenticate(
            &state.reads,
            &state.master,
            std::str::from_utf8(&token)?.trim(),
        )
        .await?;
        Ok(Self {
            engine: ready.engine,
            state,
            admin,
            shutdown,
            _scratch: scratch,
        })
    }

    async fn node(&self) -> TestResult<(NodeId, AuthenticatedCaller)> {
        let id = NodeId::generate()?;
        let token = mint_node(&self.state.master, &id, 1)?;
        self.state
            .db
            .write_sqlite({
                let id = id.clone();
                let owner = self.admin.actor_id().to_owned();
                let verifier = token.verifier;
                move |tx| {
                    tx.execute(
                    "INSERT INTO nodes(node_id, display_name, owner_principal_id, credential_hmac,
                                       status, enrolled_at_ms)
                     VALUES (?1, 'Blob fixture Node', ?2, ?3, 'offline', ?4)",
                    params![id.as_str(), owner, verifier, now_ms()],
                )?;
                    Ok(WriteOutcome::new(()))
                }
            })
            .await?;
        let caller =
            auth::authenticate(&self.state.reads, &self.state.master, token.secret.as_str())
                .await?;
        Ok((id, caller))
    }

    async fn attach(
        &self,
        node: NodeId,
        attachment: AttachmentRef,
        now: i64,
    ) -> Result<(), HubError> {
        let admin = self.admin.clone();
        self.state
            .db
            .write::<_, HubError, _>(move |tx| {
                attach_to_node(tx, &admin, &node, None, &[attachment], now)?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        Ok(())
    }

    async fn close(self) -> TestResult {
        let _ = self.shutdown.send(());
        drop(self.state);
        self.engine.shutdown().await?;
        Ok(())
    }
}

fn upload_headers(key: Option<&'static str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/pdf"),
    );
    headers.insert(
        X_COOPLET_FILENAME,
        HeaderValue::from_static("r%C3%A9sum%C3%A9.pdf"),
    );
    if let Some(key) = key {
        headers.insert(IDEMPOTENCY_KEY, HeaderValue::from_static(key));
    }
    headers
}

#[tokio::test]
async fn full_quota_replay_and_first_attach_keep_the_node_boundary() -> TestResult {
    let fixture = Fixture::new(LIMIT).await?;
    let (_, owner) = fixture.node().await?;
    let (target_id, target) = fixture.node().await?;
    let headers = upload_headers(Some("same-upload"));
    let (status, attachment) =
        upload(&fixture.state, &owner, &headers, Body::from("12345678")).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(fixture.state.blobs.used_bytes(), LIMIT);

    let response = download(&fixture.state, &owner, attachment.blob_id.clone()).await?;
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "8");
    assert_eq!(
        response.headers()[header::ETAG].to_str()?,
        format!("\"{}\"", attachment.sha256)
    );
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        to_bytes(response.into_body(), 8).await?.as_ref(),
        b"12345678"
    );

    let forbidden = download(&fixture.state, &target, attachment.blob_id.clone()).await;
    assert!(forbidden.is_err_and(|error| error.problem().code == ErrorCode::Forbidden));

    assert_eq!(
        upload(&fixture.state, &owner, &headers, Body::from("12345678")).await?,
        (StatusCode::OK, attachment.clone()),
    );
    let mut proven_headers = headers.clone();
    proven_headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("8"));
    proven_headers.insert(X_COOPLET_SHA256, HeaderValue::from_str(&attachment.sha256)?);
    let stalled = Body::from_stream(stream::pending::<Result<Bytes, std::io::Error>>());
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            upload(&fixture.state, &owner, &proven_headers, stalled)
        )
        .await??,
        (StatusCode::OK, attachment.clone()),
    );
    let mut mismatched = headers.clone();
    mismatched.insert(X_COOPLET_FILENAME, HeaderValue::from_static("other.pdf"));
    let stalled = Body::from_stream(stream::pending::<Result<Bytes, std::io::Error>>());
    let conflict = tokio::time::timeout(
        Duration::from_secs(2),
        upload(&fixture.state, &owner, &mismatched, stalled),
    )
    .await?;
    assert!(conflict.is_err_and(|error| error.problem().code == ErrorCode::OperationConflict));
    let bad = upload(
        &fixture.state,
        &owner,
        &upload_headers(None),
        Body::from("new"),
    )
    .await;
    assert!(bad.is_err_and(|error| error.problem().code == ErrorCode::StorageFull));

    let attached_at = now_ms();
    fixture
        .attach(target_id.clone(), attachment.clone(), attached_at)
        .await?;
    fixture
        .attach(target_id.clone(), attachment.clone(), attached_at + 2)
        .await?;
    let cutoff = fixture
        .state
        .reads
        .read({
            let id = attachment.blob_id.clone();
            move |conn| Ok(lookup_blob(conn, &id)?.map(|row| row.expires_at_ms))
        })
        .await?;
    assert_eq!(cutoff, Some(attached_at + ATTACHED_TTL_MS));
    assert!(
        download(&fixture.state, &owner, attachment.blob_id.clone())
            .await
            .is_err_and(|error| error.problem().code == ErrorCode::Forbidden)
    );
    let bytes = to_bytes(
        download(&fixture.state, &target, attachment.blob_id.clone())
            .await?
            .into_body(),
        8,
    )
    .await?;
    assert_eq!(bytes.as_ref(), b"12345678");

    let expired_at = now_ms();
    fixture
        .state
        .db
        .write_sqlite({
            let blob_id = attachment.blob_id.clone();
            move |tx| {
                tx.execute(
                    "UPDATE blobs SET expires_at_ms = ?1 WHERE blob_id = ?2",
                    params![expired_at, blob_id.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            }
        })
        .await?;
    fixture.engine.gc_once(expired_at).await?;
    assert_eq!(fixture.state.blobs.used_bytes(), 0);
    let deleted = fixture
        .state
        .reads
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT payload_json FROM events WHERE aggregate_id = ?1 AND kind = 'blob.deleted'",
                [attachment.blob_id.as_str()],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await?;
    assert_eq!(
        serde_json::from_str::<StoredEvent>(&deleted)?.audience,
        EventAudience::Nodes {
            node_ids: vec![target_id]
        },
    );
    let replay = upload(&fixture.state, &owner, &headers, Body::from("12345678")).await;
    assert!(replay.is_err_and(|error| error.problem().code == ErrorCode::Forbidden));
    fixture.close().await
}

#[tokio::test]
async fn expired_unattached_blob_replays_without_being_recreated() -> TestResult {
    let fixture = Fixture::new(LIMIT).await?;
    let (_, owner) = fixture.node().await?;
    let headers = upload_headers(Some("kept-receipt"));
    let (_, original) = upload(&fixture.state, &owner, &headers, Body::from("report")).await?;
    fixture.engine.gc_once(now_ms() + 61 * 60 * 1_000).await?;
    assert_eq!(fixture.state.blobs.used_bytes(), 0);
    assert_eq!(
        upload(&fixture.state, &owner, &headers, Body::from("report")).await?,
        (StatusCode::OK, original.clone()),
    );
    assert_eq!(fixture.state.blobs.used_bytes(), 0);
    assert!(
        download(&fixture.state, &owner, original.blob_id)
            .await
            .is_err_and(|error| error.problem().code == ErrorCode::NotFound)
    );
    fixture.close().await
}

#[tokio::test]
async fn concurrent_upload_releases_duplicate_quota_and_first_attach_has_one_winner() -> TestResult
{
    let fixture = Fixture::new(2 * LIMIT).await?;
    let headers = upload_headers(Some("racing-upload"));
    let barrier = Arc::new(Barrier::new(2));
    let body = |barrier: Arc<Barrier>| {
        Body::from_stream(stream::once(async move {
            barrier.wait().await;
            Ok::<_, std::io::Error>(Bytes::from_static(b"12345678"))
        }))
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            upload(
                &fixture.state,
                &fixture.admin,
                &headers,
                body(barrier.clone())
            ),
            upload(&fixture.state, &fixture.admin, &headers, body(barrier)),
        )
    })
    .await?;
    let (first_status, attachment) = first?;
    let (second_status, replay) = second?;
    assert_ne!(first_status, second_status);
    assert_eq!(attachment, replay);
    assert_eq!(fixture.state.blobs.used_bytes(), LIMIT);

    let (left_id, _) = fixture.node().await?;
    let (right_id, _) = fixture.node().await?;
    let now = now_ms();
    let (left, right) = tokio::join!(
        fixture.attach(left_id, attachment.clone(), now),
        fixture.attach(right_id, attachment, now),
    );
    let rejected = match (left, right) {
        (Ok(()), Err(error)) | (Err(error), Ok(())) => error,
        _ => return Err("expected exactly one successful first attachment".into()),
    };
    assert_eq!(rejected.problem().code, ErrorCode::Forbidden);
    fixture.close().await
}

#[tokio::test]
async fn revoked_upload_cannot_register_at_eof() -> TestResult {
    let fixture = Fixture::new(LIMIT).await?;
    let (id, caller) = fixture.node().await?;
    let (started, wait) = oneshot::channel();
    let (finish, ready) = oneshot::channel();
    let body = Body::from_stream(stream::once(async move {
        let _ = started.send(());
        let _ = ready.await;
        Ok::<_, std::io::Error>(Bytes::from_static(b"report"))
    }));
    let revoke = async {
        wait.await?;
        fixture
            .state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE nodes SET status = 'revoked', revoked_at_ms = ?1 WHERE node_id = ?2",
                    params![now_ms(), id.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        let _ = finish.send(());
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    };
    let headers = upload_headers(None);
    let (rejected, revoked) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(upload(&fixture.state, &caller, &headers, body), revoke)
    })
    .await?;
    revoked?;
    assert!(rejected.is_err_and(|error| error.problem().code == ErrorCode::NodeRevoked));
    assert_eq!(fixture.state.blobs.used_bytes(), 0);
    let rows = fixture
        .state
        .reads
        .read(|conn| {
            Ok(conn.query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get::<_, u32>(0))?)
        })
        .await?;
    assert_eq!(rows, 0);
    fixture.close().await
}

#[tokio::test]
async fn enrolled_node_can_attach_own_blob_only_to_its_own_node() -> TestResult {
    let fixture = Fixture::new(2 * LIMIT).await?;
    let (own_id, own_node) = fixture.node().await?;
    let (other_id, _) = fixture.node().await?;
    let (_, attachment) = upload(
        &fixture.state,
        &own_node,
        &upload_headers(None),
        Body::from("nodefile"),
    )
    .await?;

    let caller = own_node.clone();
    let foreign_node = other_id.clone();
    let foreign_attachment = attachment.clone();
    let denied = fixture
        .state
        .db
        .write::<_, HubError, _>(move |tx| {
            attach_to_node(
                tx,
                &caller,
                &foreign_node,
                None,
                &[foreign_attachment],
                now_ms(),
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await;
    assert!(
        denied
            .map_err(HubError::from)
            .is_err_and(|error| error.problem().code == ErrorCode::Forbidden)
    );

    let caller = own_node.clone();
    let target_node = own_id.clone();
    let own_attachment = attachment.clone();
    fixture
        .state
        .db
        .write::<_, HubError, _>(move |tx| {
            attach_to_node(tx, &caller, &target_node, None, &[own_attachment], now_ms())?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let scope = fixture
        .state
        .reads
        .read(move |conn| Ok(lookup_blob(conn, &attachment.blob_id)?.map(|row| row.scope)))
        .await?;
    assert_eq!(
        scope,
        Some(crate::storage::events::BlobScope::Node(own_id, None))
    );
    fixture.close().await
}

#[tokio::test]
async fn attributed_upload_uses_a_pair_scope_and_replays_for_its_owner() -> TestResult {
    let fixture = Fixture::new(LIMIT).await?;
    let (a, sender_machine) = fixture.node().await?;
    let (b, target_machine) = fixture.node().await?;
    let (_, outsider_machine) = fixture.node().await?;
    let participant = ParticipantId::generate()?;
    let insert = (a.clone(), b.clone(), participant.clone());
    fixture.state.db.write_sqlite(move |tx| {
        for node in [&insert.0, &insert.1] {
            tx.execute(
                "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                 VALUES (?1, ?2, 'agent', 'sender', 'idle', 0, ?3)",
                params![node.as_str(), insert.2.as_str(), now_ms()],
            )?;
        }
        Ok(WriteOutcome::new(()))
    }).await?;
    let mut headers = upload_headers(Some("attributed-upload"));
    headers.insert(
        X_COOPLET_PARTICIPANT,
        HeaderValue::from_str(participant.as_str())?,
    );
    assert!(
        upload(
            &fixture.state,
            &fixture.admin,
            &headers,
            Body::from("nodefile")
        )
        .await
        .is_err_and(|error| error.problem().code == ErrorCode::Forbidden)
    );
    let (_, attachment) = upload(
        &fixture.state,
        &sender_machine,
        &headers,
        Body::from("nodefile"),
    )
    .await?;
    let pair = (
        a.clone(),
        b.clone(),
        participant.clone(),
        attachment.clone(),
        fixture.admin.clone(),
    );
    fixture
        .state
        .db
        .write::<_, HubError, _>(move |tx| {
            let caller = auth::attest_node_participant(tx, &pair.0, &pair.2, None)?;
            let other = auth::attest_node_participant(tx, &pair.1, &pair.2, None)?;
            let attachments = std::slice::from_ref(&pair.3);
            attach_to_node(tx, &caller, &pair.1, Some(&pair.0), attachments, now_ms())?;
            attach_to_node(tx, &pair.4, &pair.0, Some(&pair.1), attachments, now_ms())?;
            assert!(
                preflight_node_attachments(
                    tx,
                    &other,
                    &pair.0,
                    Some(&pair.1),
                    attachments,
                    now_ms()
                )
                .is_err_and(|error| error.problem().code == ErrorCode::Forbidden)
            );
            Ok(WriteOutcome::new(()))
        })
        .await?;
    for caller in [&sender_machine, &target_machine] {
        let response = download(&fixture.state, caller, attachment.blob_id.clone()).await?;
        assert_eq!(
            to_bytes(response.into_body(), 8).await?.as_ref(),
            b"nodefile"
        );
    }
    assert!(
        download(
            &fixture.state,
            &outsider_machine,
            attachment.blob_id.clone()
        )
        .await
        .is_err_and(|error| error.problem().code == ErrorCode::Forbidden)
    );
    assert_eq!(
        upload(
            &fixture.state,
            &sender_machine,
            &headers,
            Body::from("nodefile")
        )
        .await?,
        (StatusCode::OK, attachment)
    );
    assert_eq!(fixture.state.blobs.used_bytes(), LIMIT);
    fixture.close().await
}

#[tokio::test]
async fn thread_machine_grant_only_covers_live_posts_in_the_pending_suffix() -> TestResult {
    let fixture = Fixture::new(LIMIT).await?;
    let (node, _) = fixture.node().await?;
    let thread = ThreadId::generate()?;
    let recent = BlobId::generate()?;
    let expired = BlobId::generate()?;
    let truncated = BlobId::generate()?;
    let delivery = OperationId::generate()?;
    let owner = fixture.admin.actor_id().to_owned();
    let participant = ParticipantId::generate()?;
    let seed = (
        node.clone(),
        thread.clone(),
        recent.clone(),
        expired.clone(),
        truncated.clone(),
    );
    fixture.state.db.write_sqlite(move |tx| {
        let (node, thread, recent, expired, truncated) = &seed;
        tx.execute("UPDATE nodes SET status = 'online' WHERE node_id = ?1", [node.as_str()])?;
        tx.execute(
            "INSERT INTO threads(thread_id, title, created_by_actor_id, status, head_seq, created_at_ms, updated_at_ms)
             VALUES (?1, 'Grant', ?2, 'open', 22, 0, 0)",
            params![thread.as_str(), owner],
        )?;
        tx.execute(
            "INSERT INTO thread_members(thread_id, member_key, participant_id, node_id, handle,
                 role, wake_policy, pending_delivery_id, pending_from_seq, pending_to_seq, joined_at_ms)
             VALUES (?1, ?2, ?3, ?4, 'target', 'member', 'mentions_only', ?5, 1, 22, 0)",
            params![thread.as_str(), format!("{node}/{participant}"), participant.as_str(),
                node.as_str(), delivery.as_str()],
        )?;
        let mut post = tx.prepare(
            "INSERT INTO thread_posts(thread_id, seq, post_id, author_actor_id, author_kind, author_handle,
                 body, attachments_json, created_at_ms, expires_at_ms)
             VALUES (?1, ?2, ?3, ?4, 'human', 'author', '', ?5, 0, ?6)",
        )?;
        for seq in 1..=22 {
            let attachments = match seq {
                1 => format!(r#"[{{"blob_id":"{truncated}"}}]"#),
                21 => format!(r#"[{{"blob_id":"{expired}"}}]"#),
                22 => format!(r#"[{{"blob_id":"{recent}"}}]"#),
                _ => "[]".into(),
            };
            let expires = if seq <= 21 { 101 } else { 102 };
            post.execute(params![thread.as_str(), seq, format!("post-{seq}"), owner, attachments, expires])?;
        }
        assert!(thread_delivery_contains_blob(tx, thread, node, recent, 101)?);
        assert!(!thread_delivery_contains_blob(tx, thread, node, expired, 101)?);
        assert!(!thread_delivery_contains_blob(tx, thread, node, truncated, 100)?);
        tx.execute("UPDATE thread_members SET state = 'paused_unknown' WHERE thread_id = ?1", [thread.as_str()])?;
        assert!(!thread_delivery_contains_blob(tx, thread, node, recent, 101)?);
        Ok(WriteOutcome::new(()))
    }).await?;
    fixture.close().await
}

#[tokio::test]
async fn paired_dm_blob_acl_reuse_thread_oracle_and_stopped_agent_upload() -> TestResult {
    use crate::{credentials::mint_service, event_bus, threads};
    use cooplet_protocol::{ServicePrincipalId, ThreadCreateRequest, ThreadPostRequest, TokenId};

    let fixture = Fixture::new(1024).await?;
    let (a, node_a) = fixture.node().await?;
    let (b, _node_b) = fixture.node().await?;
    let x_part = ParticipantId::generate()?;
    let z_part = ParticipantId::generate()?;
    let stopped_part = ParticipantId::generate()?;
    let seed = (
        a.clone(),
        x_part.clone(),
        z_part.clone(),
        stopped_part.clone(),
    );
    fixture.state.db.write_sqlite(move |tx| {
        let now = now_ms();
        for (part, state) in [
            (&seed.1, "idle"),
            (&seed.2, "idle"),
            (&seed.3, "stopped"),
        ] {
            tx.execute(
                "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                 VALUES (?1, ?2, 'agent', 'agent', ?3, 0, ?4)",
                params![seed.0.as_str(), part.as_str(), state, now],
            )?;
        }
        Ok(WriteOutcome::new(()))
    }).await?;

    // Stopped agent cannot upload via X-Cooplet-Participant.
    let mut stopped_headers = upload_headers(None);
    stopped_headers.insert(
        X_COOPLET_PARTICIPANT,
        HeaderValue::from_str(stopped_part.as_str())?,
    );
    assert_eq!(
        upload(
            &fixture.state,
            &node_a,
            &stopped_headers,
            Body::from("nope")
        )
        .await
        .err()
        .ok_or("expected 403 for stopped participant upload")?
        .problem()
        .code,
        ErrorCode::Forbidden
    );

    // X@A uploads a blob and attaches it to paired DM (A -> B).
    let mut x_headers = upload_headers(None);
    x_headers.insert(
        X_COOPLET_PARTICIPANT,
        HeaderValue::from_str(x_part.as_str())?,
    );
    let (_, dm_blob) = upload(&fixture.state, &node_a, &x_headers, Body::from("paired")).await?;
    let args = (
        a.clone(),
        b.clone(),
        x_part.clone(),
        z_part.clone(),
        dm_blob.clone(),
    );
    fixture
        .state
        .db
        .write::<_, HubError, _>(move |tx| {
            let x_caller = auth::attest_node_participant(tx, &args.0, &args.2, None)?;
            let z_caller = auth::attest_node_participant(tx, &args.0, &args.3, None)?;
            let atts = std::slice::from_ref(&args.4);
            // Attach to (target=B, peer=A)
            attach_to_node(tx, &x_caller, &args.1, Some(&args.0), atts, now_ms())?;
            // X@A reuse -> Ok
            preflight_node_attachments(tx, &x_caller, &args.1, Some(&args.0), atts, now_ms())?;
            // Z@A reuse of X@A's blob -> 403 Forbidden
            assert_eq!(
                preflight_node_attachments(tx, &z_caller, &args.1, Some(&args.0), atts, now_ms())
                    .err()
                    .ok_or_else(|| HubError::internal("expected 403 for Z@A"))?
                    .problem()
                    .code,
                ErrorCode::Forbidden
            );
            Ok(WriteOutcome::new(()))
        })
        .await?;

    // Helper to create scoped service principals with specific allowed_nodes.
    let make_service = |nodes: Vec<NodeId>| {
        let state = fixture.state.clone();
        async move {
            let sp = ServicePrincipalId::generate()?;
            let tid = TokenId::generate()?;
            let minted = mint_service(&state.master, &sp)?;
            let verifier = minted.verifier;
            let allowed = serde_json::to_string(&nodes)?;
            let now = now_ms();
            state.db.write_sqlite(move |tx| {
                tx.execute(
                    "INSERT INTO principals (principal_id, kind, username, display_name, scopes_json,
                        allowed_nodes_json, created_at_ms, updated_at_ms)
                     VALUES (?1, 'service', ?1, 'Svc', '[\"read\",\"collaborate\"]', ?2, ?3, ?3)",
                    params![sp.as_str(), allowed, now],
                )?;
                tx.execute(
                    "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                        scopes_json, expires_at_ms, created_at_ms)
                     VALUES (?1, ?2, 'service_token', ?3, '[\"read\",\"collaborate\"]', ?4, ?5)",
                    params![tid.as_str(), sp.as_str(), verifier, i64::MAX, now],
                )?;
                Ok(WriteOutcome::new(()))
            }).await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(
                auth::authenticate(&state.reads, &state.master, minted.secret.as_str()).await?,
            )
        }
    };
    let svc_only_b = make_service(vec![b.clone()]).await?;
    let svc_a_and_b = make_service(vec![a.clone(), b.clone()]).await?;

    // Scoped service with only [B] gets 403 on paired (A, B) blob download; [A, B] succeeds.
    assert_eq!(
        download(&fixture.state, &svc_only_b, dm_blob.blob_id.clone())
            .await
            .err()
            .ok_or("expected 403 for [B]-only service")?
            .problem()
            .code,
        ErrorCode::Forbidden
    );
    let resp = download(&fixture.state, &svc_a_and_b, dm_blob.blob_id.clone()).await?;
    assert_eq!(to_bytes(resp.into_body(), 16).await?.as_ref(), b"paired");

    // Thread blob existence oracle protection: attaching a thread blob to DM returns 404,
    // while caller outside target node scope receives 403 before blob lookup.
    let board = threads::create(
        &fixture.state,
        &fixture.admin,
        "blob-oracle-board",
        ThreadCreateRequest {
            title: "Secret board".into(),
            topic: None,
            creator_handle: None,
            members: Vec::new(),
        },
    )
    .await?;
    let (_, thread_blob) = upload(
        &fixture.state,
        &fixture.admin,
        &upload_headers(None),
        Body::from("secret"),
    )
    .await?;
    threads::post(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        "attach-thread-blob",
        ThreadPostRequest {
            body: "attached".into(),
            mentions: Vec::new(),
            attachments: vec![thread_blob.clone()],
            question: None,
            answer: None,
            in_reply_to: None,
        },
    )
    .await?;
    let check_args = (
        svc_only_b.clone(),
        a.clone(),
        b.clone(),
        thread_blob.clone(),
    );
    fixture
        .state
        .db
        .write::<_, HubError, _>(move |tx| {
            let atts = std::slice::from_ref(&check_args.3);
            // Caller allowed on B probing a thread blob on DM to B -> 404 NotFound (no existence oracle).
            assert_eq!(
                preflight_node_attachments(tx, &check_args.0, &check_args.2, None, atts, now_ms())
                    .err()
                    .ok_or_else(|| HubError::internal("expected 404 for thread blob on DM"))?
                    .problem()
                    .code,
                ErrorCode::NotFound
            );
            // Caller NOT allowed on A probing any blob on DM to A -> 403 Forbidden before lookup.
            assert_eq!(
                preflight_node_attachments(tx, &check_args.0, &check_args.1, None, atts, now_ms())
                    .err()
                    .ok_or_else(|| HubError::internal("expected 403 for disallowed node"))?
                    .problem()
                    .code,
                ErrorCode::Forbidden
            );
            Ok(WriteOutcome::new(()))
        })
        .await?;

    // GC tombstoning of paired DM blob: blob.deleted hidden from [B], visible to [A, B].
    let blob_id_expire = dm_blob.blob_id.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE blobs SET expires_at_ms = 1 WHERE blob_id = ?1",
                [blob_id_expire.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    fixture.engine.gc_once(10).await?;
    let page_b = event_bus::page(
        &fixture.state,
        &svc_only_b,
        event_bus::EventsQuery {
            after: 0,
            limit: Some(100),
            kinds: Some("blob.deleted".into()),
        },
    )
    .await?;
    assert!(page_b.items.is_empty());
    let page_ab = event_bus::page(
        &fixture.state,
        &svc_a_and_b,
        event_bus::EventsQuery {
            after: 0,
            limit: Some(100),
            kinds: Some("blob.deleted".into()),
        },
    )
    .await?;
    assert_eq!(page_ab.items.len(), 1);
    assert_eq!(page_ab.items[0].aggregate_id, dm_blob.blob_id.as_str());

    // Thread blob download via production path: non-member -> 404; expired closed thread -> 404 for member, 200 for admin.
    assert_eq!(
        download(&fixture.state, &svc_only_b, thread_blob.blob_id.clone())
            .await
            .err()
            .ok_or("expected 404 for non-member thread blob download")?
            .problem()
            .code,
        ErrorCode::NotFound
    );
    threads::join(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        "join-for-blob",
        cooplet_protocol::ThreadMemberJoinRequest {
            member_key: svc_only_b.actor_id().into(),
            handle: Some("svcb".into()),
            role: Some(cooplet_protocol::ThreadRole::Observer),
            wake_policy: None,
            delivery_mode: None,
        },
    )
    .await?;
    let obs_resp = download(&fixture.state, &svc_only_b, thread_blob.blob_id.clone()).await?;
    assert_eq!(
        to_bytes(obs_resp.into_body(), 16).await?.as_ref(),
        b"secret"
    );
    // Offline node receives 404 on live thread blob download (OfflineBridge.is_online == false, no existence oracle).
    assert_eq!(
        download(&fixture.state, &node_a, thread_blob.blob_id.clone())
            .await
            .err()
            .ok_or("expected 404 for offline node thread blob download")?
            .problem()
            .code,
        ErrorCode::NotFound
    );
    // Close and expire the thread -> 404 before GC; after GC removes the closed thread, the blob
    // transitions to GlobalArchiveOnly (403 for scoped service, 200 for global admin).
    threads::close(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        "close-blob-board",
    )
    .await?;
    let tid = board.thread_id.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE threads SET expires_at_ms = 0 WHERE thread_id = ?1",
                [tid.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    assert_eq!(
        download(&fixture.state, &svc_only_b, thread_blob.blob_id.clone())
            .await
            .err()
            .ok_or("expected 404 after closed thread tombstone")?
            .problem()
            .code,
        ErrorCode::NotFound
    );
    fixture.engine.gc_once(10).await?;
    assert_eq!(
        download(&fixture.state, &svc_only_b, thread_blob.blob_id.clone())
            .await
            .err()
            .ok_or("expected 403 for scoped service after archive detach")?
            .problem()
            .code,
        ErrorCode::Forbidden
    );
    let admin_resp = download(&fixture.state, &fixture.admin, thread_blob.blob_id).await?;
    assert_eq!(
        to_bytes(admin_resp.into_body(), 16).await?.as_ref(),
        b"secret"
    );
    fixture.close().await
}
