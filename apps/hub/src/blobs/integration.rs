use std::{error::Error, fs, path::PathBuf, sync::Arc, time::Duration};

use axum::body::to_bytes;
use bytes::Bytes;
use cooplet_client::{IDEMPOTENCY_KEY, X_COOPLET_FILENAME, X_COOPLET_SHA256};
use cooplet_platform::PrivateFs;
use cooplet_protocol::EventId;
use cooplet_storage::{StorageEngine, WriteOutcome};
use futures_util::stream;
use http::{HeaderMap, StatusCode};
use tokio::sync::{Barrier, oneshot, watch};

use crate::{
    auth,
    bridge::OfflineBridge,
    credentials::mint_node,
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
                attach_to_node(tx, &admin, &node, &[attachment], now)?;
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
