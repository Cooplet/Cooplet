mod body;
mod upload;

#[cfg(test)]
mod integration;

use std::path::PathBuf;

use axum::{body::Body, response::Response};
use cooplet_protocol::{
    AttachmentRef, BlobId, ErrorCode, NodeId, OperationId, ServiceScope, ThreadId,
};
use http::{HeaderValue, header};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use rusqlite::{Connection, OptionalExtension, Transaction, params, types::Type};

use crate::{
    AppState,
    auth::{AuthenticatedCaller, reverify_caller_tx},
    error::HubError,
    event_bus::can_access_thread_scope,
    now_ms,
};

pub(crate) use upload::upload;

const ATTACHED_TTL_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
const BLOB_PUT: &str = "blob.put";

pub(crate) async fn download(
    state: &AppState,
    caller: &AuthenticatedCaller,
    blob_id: BlobId,
) -> Result<Response, HubError> {
    require_blob_scope(caller, ServiceScope::Read)?;
    let metadata = state
        .reads
        .read({
            let blob_id = blob_id.clone();
            let caller = caller.clone();
            move |conn| {
                let snapshot = conn.unchecked_transaction()?;
                Ok((|| {
                    let now_ms = now_ms();
                    reverify_caller_tx(&snapshot, &caller, now_ms)?;
                    let blob = lookup_blob(&snapshot, &blob_id)?
                        .filter(|blob| blob.is_live(now_ms))
                        .ok_or_else(blob_not_found)?;
                    blob.require_read(&caller)?;
                    Ok::<_, HubError>(blob)
                })())
            }
        })
        .await??;

    let (record, file) = state
        .blobs
        .open_blob(&blob_id)
        .await?
        .ok_or_else(blob_not_found)?;
    if record.blob_id != blob_id
        || record.size_bytes != metadata.attachment.size_bytes
        || record.disk_rel_path != metadata.disk_rel_path
    {
        return Err(HubError::internal("blob metadata changed while opening"));
    }
    let attachment = metadata.attachment;
    Response::builder()
        .header(header::CONTENT_TYPE, attachment.mime_type)
        .header(header::CONTENT_LENGTH, attachment.size_bytes)
        .header(header::ETAG, format!("\"{}\"", attachment.sha256))
        .header("x-content-type-options", "nosniff")
        .header(
            header::CONTENT_DISPOSITION,
            disposition(&metadata.safe_disk_name, &attachment.name)?,
        )
        .body(Body::from_stream(body::file_chunks(
            file,
            state.shutdown.clone(),
        )))
        .map_err(HubError::internal)
}

pub(crate) fn attach_to_node(
    tx: &Transaction<'_>,
    caller: &AuthenticatedCaller,
    node_id: &NodeId,
    attachments: &[AttachmentRef],
    now_ms: i64,
) -> Result<(), HubError> {
    if attachments.is_empty() {
        return Ok(());
    }
    caller.require_scope(ServiceScope::Collaborate)?;
    if !caller.allows_node(node_id) {
        return Err(blob_forbidden());
    }
    for attachment in attachments {
        let blob = lookup_blob(tx, &attachment.blob_id)?
            .filter(|blob| blob.is_live(now_ms))
            .ok_or_else(blob_not_found)?;
        match &blob.scope {
            BlobScope::Unattached
                if blob.owner_actor_id == caller.actor_id() || caller.is_admin() => {}
            BlobScope::Node(current) if current == node_id => blob.require_read(caller)?,
            _ => return Err(blob_forbidden()),
        }
        if blob.attachment != *attachment {
            return Err(HubError::invalid(
                "Attachment metadata does not match the stored blob.",
            ));
        }
        if matches!(blob.scope, BlobScope::Unattached) {
            tx.execute(
                "UPDATE blobs SET scope_node_id = ?1, attached_at_ms = ?2, expires_at_ms = ?3
                 WHERE blob_id = ?4 AND attached_at_ms IS NULL",
                params![
                    node_id.as_str(),
                    now_ms,
                    now_ms.saturating_add(ATTACHED_TTL_MS),
                    attachment.blob_id.as_str()
                ],
            )?;
            // Remember the first attachment in any still-retained put receipt. Once
            // the blob row disappears, a scoped caller needs current ACL evidence.
            tx.execute(
                "UPDATE operations SET result_json = json_set(result_json, '$.ever_attached', json('true'))
                 WHERE operation_id = ?1 AND kind = ?2 AND resource_id = ?3",
                params![
                    blob_operation_id(&attachment.blob_id)?.as_str(),
                    BLOB_PUT,
                    attachment.blob_id.as_str()
                ],
            )?;
        }
    }
    Ok(())
}

// Match put receipts by the primary key without scanning an actor's operations.
fn blob_operation_id(blob_id: &BlobId) -> Result<OperationId, HubError> {
    let suffix = blob_id.as_str().trim_start_matches("blob_");
    OperationId::parse(&format!("op_{suffix}")).map_err(HubError::internal)
}

#[derive(Debug, PartialEq, Eq)]
enum BlobScope {
    Unattached,
    Node(NodeId),
    Thread(ThreadId),
    GlobalArchiveOnly,
}

impl BlobScope {
    fn from_columns(attached: Option<i64>, node: Option<NodeId>, thread: Option<ThreadId>) -> Self {
        match (attached, node, thread) {
            (None, None, None) => Self::Unattached,
            (Some(_), Some(node), None) => Self::Node(node),
            (Some(_), None, Some(thread)) => Self::Thread(thread),
            _ => Self::GlobalArchiveOnly,
        }
    }

    fn allows_read(&self, caller: &AuthenticatedCaller, owner_actor_id: &str) -> bool {
        if caller.is_global_reader() {
            return true;
        }
        match self {
            Self::Unattached => caller.actor_id() == owner_actor_id,
            Self::Node(node_id) => caller.allows_node(node_id),
            Self::Thread(thread_id) => can_access_thread_scope(caller, thread_id),
            Self::GlobalArchiveOnly => false,
        }
    }
}

struct BlobRow {
    attachment: AttachmentRef,
    safe_disk_name: String,
    disk_rel_path: PathBuf,
    owner_actor_id: String,
    scope: BlobScope,
    expires_at_ms: i64,
    deleted_at_ms: Option<i64>,
}

impl BlobRow {
    fn is_live(&self, now_ms: i64) -> bool {
        self.deleted_at_ms.is_none() && self.expires_at_ms > now_ms
    }

    fn require_read(&self, caller: &AuthenticatedCaller) -> Result<(), HubError> {
        if self.scope.allows_read(caller, &self.owner_actor_id) {
            Ok(())
        } else {
            Err(blob_forbidden())
        }
    }
}

fn lookup_blob(conn: &Connection, blob_id: &BlobId) -> rusqlite::Result<Option<BlobRow>> {
    conn.query_row(
        "SELECT original_name, mime_type, size_bytes, sha256, safe_disk_name, disk_rel_path,
                owner_actor_id, attached_at_ms, scope_node_id, scope_thread_id,
                expires_at_ms, deleted_at_ms
         FROM blobs WHERE blob_id = ?1",
        [blob_id.as_str()],
        |row| {
            let node = row
                .get::<_, Option<String>>(8)?
                .map(|id| {
                    NodeId::parse(&id).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(error))
                    })
                })
                .transpose()?;
            let thread = row
                .get::<_, Option<String>>(9)?
                .map(|id| {
                    ThreadId::parse(&id).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(9, Type::Text, Box::new(error))
                    })
                })
                .transpose()?;
            Ok(BlobRow {
                attachment: AttachmentRef {
                    blob_id: blob_id.clone(),
                    name: row.get(0)?,
                    mime_type: row.get(1)?,
                    size_bytes: row.get(2)?,
                    sha256: row.get(3)?,
                },
                safe_disk_name: row.get(4)?,
                disk_rel_path: PathBuf::from(row.get::<_, String>(5)?),
                owner_actor_id: row.get(6)?,
                scope: BlobScope::from_columns(row.get(7)?, node, thread),
                expires_at_ms: row.get(10)?,
                deleted_at_ms: row.get(11)?,
            })
        },
    )
    .optional()
}

fn disposition(safe_disk_name: &str, original_name: &str) -> Result<HeaderValue, HubError> {
    let ascii: String = safe_disk_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, ' ' | '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    let encoded = utf8_percent_encode(original_name, NON_ALPHANUMERIC);
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{ascii}\"; filename*=UTF-8''{encoded}"
    ))
    .map_err(HubError::internal)
}

fn require_blob_scope(caller: &AuthenticatedCaller, scope: ServiceScope) -> Result<(), HubError> {
    if caller.node_id().is_some() {
        Ok(())
    } else {
        caller.require_scope(scope)
    }
}

fn blob_not_found() -> HubError {
    HubError::new(
        ErrorCode::NotFound,
        "The blob does not exist or has expired.",
    )
}

fn blob_forbidden() -> HubError {
    HubError::new(
        ErrorCode::Forbidden,
        "The credential cannot access this blob.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_disposition_keeps_original_utf8_and_contains_an_ascii_fallback()
    -> Result<(), HubError> {
        let value = disposition("résumé _.pdf", "résumé \"draft\".pdf")?;
        assert_eq!(
            value,
            "attachment; filename=\"r_sum_ _.pdf\"; filename*=UTF-8''r%C3%A9sum%C3%A9%20%22draft%22%2Epdf"
        );
        Ok(())
    }

    #[test]
    fn orphaned_attachment_has_no_owner_scope() {
        assert_eq!(
            BlobScope::from_columns(Some(1), None, None),
            BlobScope::GlobalArchiveOnly
        );
        assert_eq!(
            BlobScope::from_columns(None, None, None),
            BlobScope::Unattached
        );
    }

    #[test]
    fn put_receipt_id_is_derived_from_the_uploaded_blob() -> Result<(), Box<dyn std::error::Error>>
    {
        let blob_id = BlobId::from_parts(1, [0; 10])?;
        assert_eq!(
            blob_operation_id(&blob_id)?.as_str(),
            "op_00000000010000000000000000"
        );
        Ok(())
    }
}
