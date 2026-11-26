#[path = "../../../../packages/cooplet-client/tests/support/mod.rs"]
mod support;

use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs,
    io::{self, Cursor},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, ClientConfig, ClientError, CoopletClient, Destination,
    UploadVerification,
};
use cooplet_platform::PrivateFs;
use cooplet_protocol::{AttachmentRef, BlobId, ErrorCode, ProblemDetails};
use cooplet_storage::{
    BlobInput, BlobRecord, BlobState, BlobStoreConfig, BlobTombstone, GcBatchOutcome, GcHooks,
    Migration, StorageConfig, StorageEngine, StorageError, StoredBlob, WriteError, WriteFailure,
    WriteOutcome,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use support::{HUB_ID, HUB_INFO, Server, TestResult};

const SIZE: usize = 256 * 1024;
const SHA: &str = "4b0d375a615c0382b4f958b48e43e7f356b4fcac76e20423294adc07b8d4976e";
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "relay test registry",
    sql: "CREATE TABLE objects(id TEXT PRIMARY KEY, path TEXT NOT NULL, bytes INTEGER NOT NULL)",
}];

struct Registry;

impl GcHooks for Registry {
    fn prune_batch(
        &self,
        _: &Transaction<'_>,
        _: i64,
        _: usize,
    ) -> rusqlite::Result<GcBatchOutcome> {
        Ok(GcBatchOutcome::default())
    }

    fn fetch_pending_blob_tombstones(
        &self,
        _: &Transaction<'_>,
        _: usize,
    ) -> rusqlite::Result<Vec<BlobTombstone>> {
        Ok(Vec::new())
    }

    fn ack_blob_tombstone(&self, _: &Transaction<'_>, _: &BlobTombstone) -> rusqlite::Result<bool> {
        Ok(false)
    }

    fn lookup_blobs(
        &self,
        db: &Connection,
        ids: &[BlobId],
    ) -> rusqlite::Result<BTreeMap<BlobId, BlobState>> {
        let mut query = db.prepare("SELECT path, bytes FROM objects WHERE id = ?")?;
        ids.iter()
            .map(|id| {
                let record = query
                    .query_row([id.as_str()], |row| {
                        Ok(BlobRecord {
                            blob_id: id.clone(),
                            disk_rel_path: PathBuf::from(row.get::<_, String>(0)?),
                            size_bytes: row.get(1)?,
                        })
                    })
                    .optional()?;
                Ok((
                    id.clone(),
                    record.map_or(BlobState::Absent, BlobState::Live),
                ))
            })
            .collect()
    }
}

fn register(
    tx: &Transaction<'_>,
    blob: &StoredBlob,
) -> Result<WriteOutcome<()>, WriteError<Infallible>> {
    tx.execute(
        "INSERT INTO objects VALUES (?1, ?2, ?3)",
        params![
            blob.blob_id.as_str(),
            blob.disk_rel_path.to_str(),
            blob.size_bytes
        ],
    )?;
    Ok(WriteOutcome::new(()))
}

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> TestResult<Self> {
        Ok(Self(PrivateFs::ensure_private_dir(
            &std::env::temp_dir().join(format!("relay-proof-{}", BlobId::generate()?)),
        )?))
    }

    async fn engine(&self, directory: &str) -> TestResult<StorageEngine> {
        let base = PrivateFs::ensure_private_dir(&self.0.join(directory))?;
        Ok(StorageEngine::open_managed(
            StorageConfig::new(base.join("node.sqlite3")),
            MIGRATIONS,
            Arc::new(Registry),
            Some(BlobStoreConfig::new(base.join("blobs"))),
        )
        .await?)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct ReadProgress {
    bytes: AtomicUsize,
    eof: AtomicBool,
}

struct Source {
    cursor: Cursor<Vec<u8>>,
    progress: Arc<ReadProgress>,
}

impl AsyncRead for Source {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let source = self.get_mut();
        let start = out.filled().len();
        let had_space = out.remaining() != 0;
        let result = Pin::new(&mut source.cursor).poll_read(cx, out);
        if matches!(result, Poll::Ready(Ok(()))) && had_space {
            let produced = out.filled().len() - start;
            source.progress.bytes.fetch_add(produced, Ordering::Relaxed);
            if produced == 0 {
                source.progress.eof.store(true, Ordering::Release);
            }
        }
        result
    }
}

#[derive(Clone, Copy)]
enum Case {
    Known,
    Unknown,
    BadUploadDigest,
    CorruptDownload,
    TruncatedDownload,
    VerifyLocally,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::Known => "known",
            Self::Unknown => "unknown",
            Self::BadUploadDigest => "bad-upload-digest",
            Self::CorruptDownload => "corrupt-download",
            Self::TruncatedDownload => "truncated-download",
            Self::VerifyLocally => "local",
        }
    }
}

async fn verify(case: Case) -> TestResult {
    let scratch = Scratch::new()?;
    let hub = scratch.engine("hub").await?;
    let node = scratch.engine("node").await?;
    let hub_store = hub.blobs().ok_or("missing Hub storage")?;
    let node_store = node.blobs().ok_or("missing Node storage")?;
    let server = Server::bind().await?;
    let client = CoopletClient::new(ClientConfig::new(
        server.url.clone(),
        Destination::Hub {
            expected_hub_id: HUB_ID.parse()?,
        },
        None,
    ))?;
    let id = BlobId::generate()?;
    let progress = Arc::new(ReadProgress::default());
    let known = match case {
        Case::Unknown => None,
        Case::BadUploadDigest => {
            Some("0000000000000000000000000000000000000000000000000000000000000000")
        }
        _ => Some(SHA),
    };
    let local = matches!(case, Case::VerifyLocally);
    let serve = async {
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        assert_eq!(head.count("x-cooplet-hub"), 0);
        peer.respond("200 OK", &[], HUB_INFO).await?;

        let (head, mut peer) = server.accept().await?;
        assert_eq!(head.request, "POST /v1/blobs HTTP/1.1");
        assert_eq!(head.header("x-cooplet-hub"), Some(HUB_ID));
        assert_eq!(head.header("x-cooplet-sha256"), known);
        let size: u64 = head
            .header("content-length")
            .ok_or("missing request length")?
            .parse()?;
        assert_eq!(size, SIZE as u64);
        let stored = hub_store
            .store(
                BlobInput {
                    blob_id: id.clone(),
                    original_name: "proof.bin".into(),
                    expected_size: Some(size),
                    expected_sha256: known.map(str::to_owned),
                },
                (&mut peer.0).take(size),
                register,
            )
            .await;
        if matches!(case, Case::BadUploadDigest) {
            assert!(matches!(
                stored,
                Err(WriteFailure::Storage(StorageError::DigestMismatch))
            ));
            assert!(hub_store.open_blob(&id).await?.is_none());
            assert_eq!(hub_store.used_bytes(), 0);
            return peer
                .respond(
                    "409 Conflict",
                    &[("content-type", "application/problem+json")],
                    &serde_json::to_vec(&ProblemDetails::new(
                        ErrorCode::BlobDigestMismatch,
                        "upload digest mismatch",
                    ))?,
                )
                .await;
        }

        let (stored, ()) = stored?;
        assert_eq!(stored.sha256, SHA);
        assert!(progress.eof.load(Ordering::Acquire));
        assert_eq!(progress.bytes.load(Ordering::Acquire), SIZE);
        let receipt = AttachmentRef {
            blob_id: stored.blob_id,
            name: stored.original_name,
            mime_type: "application/octet-stream".into(),
            size_bytes: size,
            sha256: stored.sha256,
        };
        peer.respond(
            "201 Created",
            &[("content-type", "application/json")],
            &serde_json::to_vec(&receipt)?,
        )
        .await?;
        let (head, mut peer) = server.accept().await?;
        assert_eq!(head.request, format!("GET /v1/blobs/{id} HTTP/1.1"));
        let (_, file) = hub_store
            .open_blob(&id)
            .await?
            .ok_or("Hub did not publish")?;
        let mut file = tokio::fs::File::from_std(file);
        peer.start_chunks(
            "application/octet-stream",
            &[("etag", &format!("\"{SHA}\""))],
        )
        .await?;
        let mut buffer = vec![0; 64 * 1024];
        let mut first = true;
        loop {
            let count = file.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            if first && matches!(case, Case::CorruptDownload) {
                buffer[0] ^= 1;
            }
            peer.chunk(&buffer[..count]).await?;
            if matches!(case, Case::TruncatedDownload) {
                return Ok(());
            }
            first = false;
        }
        peer.raw(b"0\r\n\r\n").await
    };

    let consume = async {
        let result = client
            .upload_blob(
                BlobUploadSpec {
                    filename: "proof.bin".into(),
                    mime_type: "application/octet-stream".into(),
                    verification: if local {
                        UploadVerification::LocalSha256
                    } else {
                        UploadVerification::HubReceipt
                    },
                    expected_sha256: known.map(str::to_owned),
                    idempotency_key: Some("fresh-relay-id".into()),
                    content_length: Some(SIZE as u64),
                },
                Source {
                    cursor: Cursor::new(vec![0x42; SIZE]),
                    progress: progress.clone(),
                },
            )
            .await;
        if matches!(case, Case::BadUploadDigest) {
            assert!(
                matches!(result, Err(ClientError::Problem(problem)) if problem.code == ErrorCode::BlobDigestMismatch)
            );
            return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
        }
        let receipt = match result? {
            BlobUploadOutcome::CompleteWithHubDigest(receipt) if !local => receipt,
            BlobUploadOutcome::VerifiedLocal(receipt) if local => receipt,
            _ => return Err("unexpected upload result".into()),
        };
        let download = client
            .open_blob_download(&receipt.blob_id, Some(&receipt.sha256), SIZE as u64)
            .await?;
        assert_eq!(download.expected_sha256, SHA);
        let node_blob = node_store
            .store(
                BlobInput {
                    blob_id: receipt.blob_id.clone(),
                    original_name: receipt.name,
                    expected_size: Some(receipt.size_bytes),
                    expected_sha256: Some(download.expected_sha256.clone()),
                },
                download,
                register,
            )
            .await;
        if matches!(case, Case::CorruptDownload | Case::TruncatedDownload) {
            if matches!(case, Case::CorruptDownload) {
                assert!(matches!(
                    node_blob,
                    Err(WriteFailure::Storage(StorageError::DigestMismatch))
                ));
            } else {
                assert!(matches!(
                    node_blob,
                    Err(WriteFailure::Storage(StorageError::Io(_)))
                ));
            }
            assert!(node_store.open_blob(&id).await?.is_none());
            assert_eq!(node_store.used_bytes(), 0);
            let publication = scratch.0.join("node/blobs/objects/4b").join(id.as_str());
            assert!(!publication.exists());
        } else {
            let (published, ()) = node_blob?;
            assert_eq!(published.sha256, SHA);
            assert_eq!(published.size_bytes, SIZE as u64);
            assert_eq!(
                tokio::fs::read(&published.absolute_path).await?,
                vec![0x42; SIZE]
            );
            assert_eq!(node_store.used_bytes(), SIZE as u64);
        }
        Ok(())
    };

    println!("BEGIN {}", case.name());
    let (left, right) = tokio::join!(serve, consume);
    left?;
    right?;
    hub.shutdown().await?;
    node.shutdown().await?;
    println!("PASS {}", case.name());
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult {
    for case in [
        Case::Known,
        Case::Unknown,
        Case::BadUploadDigest,
        Case::CorruptDownload,
        Case::TruncatedDownload,
        Case::VerifyLocally,
    ] {
        verify(case).await?;
    }
    Ok(())
}
