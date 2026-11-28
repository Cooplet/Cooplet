use std::{fs::File, future::Future, path::PathBuf, time::Duration};

use cooplet_client::CoopletClient;
use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{AggregateType, AttachmentRef, BlobId, ErrorCode, EventKind};
use cooplet_storage::{BlobInput, StorageError, StoredBlob, WriteError, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::{DeliveryService, queries::sql_error};
use crate::{
    clients::ResolvedAttachment,
    error::{NodeError, Result},
    operations::admission_expired,
    storage::append_event,
};

const CACHE_OWNER: &str = "system/hub-blob-cache";
const CACHE_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const CACHE_CHANGED: &str = "hub_blob_cache_changed";

#[derive(Clone)]
pub(crate) struct HubAttachment {
    pub(crate) local: AttachmentRef,
    path: PathBuf,
}

impl HubAttachment {
    pub(crate) fn resolved(&self) -> ResolvedAttachment {
        ResolvedAttachment::from_path(&self.local, self.path.clone())
    }
}

impl DeliveryService {
    pub(crate) async fn load_hub_attachments(
        &self,
        hub: &CoopletClient,
        attachments: &[AttachmentRef],
        admission: &AdmissionGuard,
        max_blob_bytes: u64,
    ) -> Result<Vec<HubAttachment>> {
        let mut resolved = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let cached = self
                .load_hub_blob(hub, attachment, admission, max_blob_bytes)
                .await;
            resolved.push(cached.map_err(|error| {
                if error.code() == ErrorCode::BridgeUnavailable {
                    return error;
                }
                // Cache writes are preparatory. Even an unconfirmed cache
                // mutation cannot represent an uncertain native submission.
                tracing::warn!(hub_blob_id = %attachment.blob_id, %error, "Hub attachment preparation failed");
                unavailable("The Hub attachment could not be prepared")
            })?);
        }
        Ok(resolved)
    }

    async fn load_hub_blob(
        &self,
        hub: &CoopletClient,
        attachment: &AttachmentRef,
        admission: &AdmissionGuard,
        max_blob_bytes: u64,
    ) -> Result<HubAttachment> {
        let _lock = tokio::time::timeout(
            remaining(admission)?,
            self.relay_locks.lock(attachment.blob_id.clone()),
        )
        .await
        .map_err(|_| admission_expired())?;
        let source = attachment.blob_id.clone();
        let current = self
            .reads
            .read(move |conn| {
                Ok(conn.query_row(
                "SELECT blob_id, original_name, mime_type, size_bytes, sha256, expires_at_ms
                 FROM blobs WHERE source_hub_blob_id = ?1 AND deleted_at_ms IS NULL",
                [source.as_str()], |row| {
                    let local = AttachmentRef {
                        blob_id: row.get::<_, String>(0)?.parse().map_err(sql_error)?,
                        name: row.get(1)?,
                        mime_type: row.get(2)?,
                        size_bytes: row.get(3)?,
                        sha256: row.get(4)?,
                    };
                    Ok((local, row.get::<_, i64>(5)?))
                },
            ).optional()?)
            })
            .await?;
        let now = (self.clock)()?;
        if let Some((local, expiry)) = current {
            if !same_metadata(attachment, &local) {
                return Err(unavailable(
                    "The Hub attachment metadata does not match the cache",
                ));
            }
            match self.blobs.open_blob(&local.blob_id).await {
                Ok(Some((record, file))) => {
                    let cached = HubAttachment {
                        local,
                        path: self.blob_root.join(record.disk_rel_path),
                    };
                    if expiry > now {
                        return Ok(cached);
                    }
                    let digest = tokio::time::timeout(
                        remaining(admission)?,
                        tokio::task::spawn_blocking(move || digest_file(file)),
                    )
                    .await
                    .map_err(|_| admission_expired())?
                    .map_err(NodeError::internal)??;
                    if digest == attachment.sha256 {
                        let id = cached.local.blob_id.clone();
                        let updated = self
                            .db
                            .write_sqlite(move |tx| {
                                Ok(WriteOutcome::new(refresh_blob(tx, &id, now)?))
                            })
                            .await?;
                        if updated {
                            return Ok(cached);
                        }
                    } else {
                        self.tombstone_hub_blob(cached.local, now).await?;
                    }
                }
                Ok(None) => {}
                Err(StorageError::BlobMissing(_)) => self.tombstone_hub_blob(local, now).await?,
                Err(error) => {
                    tracing::warn!(blob_id = %local.blob_id, %error, "Hub attachment cache could not be verified");
                    return Err(unavailable(
                        "The cached Hub attachment could not be verified",
                    ));
                }
            }
        }
        let download = tokio::time::timeout(
            remaining(admission)?,
            hub.open_blob_download(&attachment.blob_id, Some(&attachment.sha256), max_blob_bytes),
        )
        .await
        .map_err(|_| admission_expired())?
        .map_err(|error| {
            tracing::warn!(hub_blob_id = %attachment.blob_id, %error, "Hub attachment download failed");
            unavailable("The Hub attachment could not be downloaded")
        })?;
        if download
            .content_length
            .is_some_and(|length| length != attachment.size_bytes)
            || download
                .content_type
                .as_deref()
                .is_some_and(|mime| mime != attachment.mime_type)
        {
            return Err(unavailable(
                "The Hub attachment metadata does not match the download",
            ));
        }
        let input = BlobInput {
            blob_id: BlobId::generate()?,
            original_name: attachment.name.clone(),
            expected_size: Some(attachment.size_bytes),
            expected_sha256: Some(attachment.sha256.clone()),
        };
        let deadline = Box::pin(tokio::time::sleep(remaining(admission)?));
        let reader = DeadlineReader {
            reader: download,
            deadline,
            admission: admission.clone(),
        };
        let declared = attachment.clone();
        let write_admission = admission.clone();
        let clock = self.clock.clone();
        let (stored, local) = self.blobs.store(input, reader, move |tx, stored| {
            if write_admission.is_expired() {
                return Err(WriteError::Domain(admission_expired()));
            }
            let now = clock().map_err(NodeError::from)?;
            register_cache(tx, stored, &declared, now)
        }).await.map_err(|error| {
            if admission.is_expired() {
                return admission_expired();
            }
            tracing::warn!(hub_blob_id = %attachment.blob_id, %error, "Hub attachment could not be cached");
            unavailable("The Hub attachment could not be stored")
        })?;
        Ok(HubAttachment {
            local,
            path: stored.absolute_path,
        })
    }

    async fn tombstone_hub_blob(&self, attachment: AttachmentRef, now: i64) -> Result<()> {
        self.db
            .write(move |tx| -> std::result::Result<_, WriteError<NodeError>> {
                let updated = tx.execute(
                    "UPDATE blobs SET deleted_at_ms = ?1
                 WHERE blob_id = ?2 AND deleted_at_ms IS NULL",
                    params![now, attachment.blob_id.as_str()],
                )?;
                if updated == 0 {
                    return Ok(WriteOutcome::new(()));
                }
                let event = cache_event(tx, &attachment, now, EventKind::BlobDeleted)?;
                Ok(WriteOutcome::with_events((), vec![event]))
            })
            .await?;
        Ok(())
    }
}

fn remaining(admission: &AdmissionGuard) -> Result<Duration> {
    admission
        .remaining_ms()
        .map(Duration::from_millis)
        .ok_or_else(admission_expired)
}

fn reason(error: &NodeError) -> Option<&str> {
    error
        .0
        .extensions
        .get("reason")
        .and_then(serde_json::Value::as_str)
}

pub(crate) fn cache_changed(error: &NodeError) -> bool {
    reason(error) == Some(CACHE_CHANGED)
}

fn unavailable(detail: &'static str) -> NodeError {
    NodeError::new(ErrorCode::BridgeUnavailable, detail)
}

fn digest_file(mut file: File) -> std::io::Result<String> {
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

fn same_metadata(hub: &AttachmentRef, local: &AttachmentRef) -> bool {
    hub.name == local.name
        && hub.mime_type == local.mime_type
        && hub.size_bytes == local.size_bytes
        && hub.sha256 == local.sha256
}

fn refresh_blob(tx: &Transaction<'_>, id: &BlobId, now: i64) -> rusqlite::Result<bool> {
    let count = tx.execute(
        "UPDATE blobs SET expires_at_ms = MAX(expires_at_ms, ?1)
         WHERE blob_id = ?2 AND deleted_at_ms IS NULL",
        params![now.saturating_add(CACHE_TTL_MS), id.as_str()],
    )?;
    Ok(count == 1)
}

pub(crate) fn refresh_hub_attachments(
    tx: &Transaction<'_>,
    attachments: &[HubAttachment],
    now: i64,
) -> Result<()> {
    for attachment in attachments {
        if !refresh_blob(tx, &attachment.local.blob_id, now)? {
            return Err(
                unavailable("The Hub attachment cache changed before delivery")
                    .with_extension("reason", CACHE_CHANGED),
            );
        }
    }
    Ok(())
}

fn register_cache(
    tx: &Transaction<'_>,
    stored: &StoredBlob,
    declared: &AttachmentRef,
    now: i64,
) -> std::result::Result<WriteOutcome<AttachmentRef>, WriteError<NodeError>> {
    let disk_rel_path = stored
        .disk_rel_path
        .to_str()
        .ok_or_else(|| NodeError::internal("Hub cache path is not UTF-8"))?;
    tx.execute(
        "INSERT INTO blobs (blob_id, sha256, size_bytes, mime_type, original_name, safe_disk_name,
                            disk_rel_path, owner_actor_id, source_hub_blob_id, created_at_ms, expires_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![stored.blob_id.as_str(), stored.sha256, stored.size_bytes, declared.mime_type,
            stored.original_name, stored.safe_disk_name, disk_rel_path, CACHE_OWNER,
            declared.blob_id.as_str(), now, now.saturating_add(CACHE_TTL_MS)],
    )?;
    let mut local = declared.clone();
    local.blob_id = stored.blob_id.clone();
    let event = cache_event(tx, &local, now, EventKind::BlobStored)?;
    Ok(WriteOutcome::with_events(local, vec![event]))
}

fn cache_event(
    tx: &Transaction<'_>,
    attachment: &AttachmentRef,
    now: i64,
    kind: EventKind,
) -> rusqlite::Result<cooplet_storage::EventNotice> {
    append_event(
        tx,
        now,
        kind,
        AggregateType::Blob,
        attachment.blob_id.as_str(),
        (kind == EventKind::BlobStored).then_some(CACHE_OWNER),
        json!({
            "blob_id": attachment.blob_id,
            "original_name": attachment.name,
            "mime_type": attachment.mime_type,
            "size_bytes": attachment.size_bytes,
            "sha256": attachment.sha256,
        }),
    )
}

struct DeadlineReader<R> {
    reader: R,
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    admission: AdmissionGuard,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for DeadlineReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.admission.is_expired() || self.deadline.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Hub attachment admission deadline expired",
            )));
        }
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}
