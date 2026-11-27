use std::{
    io,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};

use axum::{
    Json,
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, ClientError, UploadVerification, X_COOPLET_FILENAME,
    X_COOPLET_SHA256,
};
use cooplet_protocol::{
    AggregateType, AttachmentRef, BlobId, ErrorCode, EventKind, ProblemDetails, canonical_sha256,
    validate_attachment_name, validate_media_type, validate_sha256,
};
use cooplet_storage::{
    BlobInput, BlobRecord, ReadPool, StorageError, StoredBlob, WriteError, WriteFailure,
    WriteOutcome,
};
use futures_util::{
    StreamExt,
    stream::{BoxStream, unfold},
};
use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG},
};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::watch,
};
use utoipa::{
    PartialSchema, ToSchema,
    openapi::{
        RefOr, Schema,
        schema::{KnownFormat, ObjectBuilder, SchemaFormat, Type},
    },
};

use super::{
    Caller, NodeState, Path, bridge::bridge_participant, extract::one_header, idempotency_key,
};
use crate::{
    auth::Principal,
    error::{NodeError, Result},
    now_ms,
    operations::{Operation, ReceiptLookup},
    storage::append_event,
};

const OPERATION_KIND: &str = "blob.put";
const UNATTACHED_TTL_MS: i64 = 60 * 60 * 1000;
const DOWNLOAD_CHUNK_BYTES: u64 = 64 * 1024;
const UPLOAD_CHUNK_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_TOTAL_TIMEOUT: Duration = Duration::from_secs(300);

pub(crate) const BLOB_ACL_QUERY: &str = "SELECT EXISTS (
     SELECT 1 FROM messages m
     JOIN json_each(m.attachments_json) a ON json_extract(a.value, '$.blob_id') = ?2
     WHERE m.recipient_id = ?1 AND m.attachments_json != '[]'
 ) OR EXISTS (
     SELECT 1 FROM messages m
     JOIN json_each(m.attachments_json) a ON json_extract(a.value, '$.blob_id') = ?2
     WHERE m.sender_id = ?1 AND m.attachments_json != '[]'
 )";

struct BlobBytes;

impl PartialSchema for BlobBytes {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::String)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Binary)))
            .into()
    }
}

impl ToSchema for BlobBytes {}

#[utoipa::path(
    post, path = "/v1/blobs", security(("bearer" = [])),
    params(
        ("X-Cooplet-Filename" = String, Header, description = "Original filename, percent-encoded UTF-8"),
        ("Content-Length" = Option<u64>, Header, description = "Declared byte length"),
        ("X-Cooplet-Sha256" = Option<String>, Header, description = "Expected SHA-256 in lowercase hexadecimal"),
        ("Idempotency-Key" = Option<String>, Header, description = "Operation key")
    ),
    request_body(
        description = "Raw attachment bytes. Set Content-Type to the concrete media type of the file.",
        content(
            (inline(BlobBytes) = "application/octet-stream"),
            (inline(BlobBytes) = "*/*")
        )
    ),
    responses(
        (status = 201, description = "Stored attachment", body = AttachmentRef),
        (status = 200, description = "Replayed attachment receipt", body = AttachmentRef),
        (status = 400, description = "Invalid request", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 401, description = "Invalid bearer credential", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 403, description = "Collaborate permission is required", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 409, description = "Idempotency or digest conflict", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 413, description = "Blob exceeds the upload limit", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 503, description = "Write temporarily unavailable or outcome unknown", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 507, description = "Storage quota exhausted", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(crate) async fn put(
    State(state): State<NodeState>,
    Caller(principal): Caller,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<AttachmentRef>)> {
    if !principal.can_collaborate() {
        return Err(NodeError::new(
            ErrorCode::Forbidden,
            "Uploading a blob requires collaborate permission",
        ));
    }
    let upload = Upload::parse(&headers, state.max_blob_bytes)?;
    let tasks = state.tasks.clone();
    tasks
        .run(async move { put_inner(state, principal, upload, body).await })
        .await
}

#[utoipa::path(
    post, path = "/v1/bridge/blobs", security(("bearer" = [])),
    params(
        ("X-Cooplet-Filename" = String, Header, description = "Original filename, percent-encoded UTF-8"),
        ("Content-Length" = Option<u64>, Header, description = "Declared byte length"),
        ("X-Cooplet-Sha256" = Option<String>, Header, description = "Expected SHA-256 in lowercase hexadecimal"),
        ("Idempotency-Key" = Option<String>, Header, description = "Operation key")
    ),
    request_body(
        description = "Raw attachment bytes to relay to the enrolled Hub. Set Content-Type to the concrete media type.",
        content(
            (inline(BlobBytes) = "application/octet-stream"),
            (inline(BlobBytes) = "*/*")
        )
    ),
    responses(
        (status = 201, description = "Attachment stored by the Hub", body = AttachmentRef),
        (status = 200, description = "Replayed Hub attachment receipt", body = AttachmentRef),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(crate) async fn relay(
    State(state): State<NodeState>,
    Caller(principal): Caller,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<AttachmentRef>)> {
    let participant_id = bridge_participant(&principal)?;
    let connection = state.bridge.connection()?;
    let limit = state.max_blob_bytes;
    let upload = Upload::parse(&headers, limit)?;
    let tasks = state.tasks.clone();
    tasks
        .run(async move {
            let _slot = state.tasks.try_acquire_upload_slot()?;
            let spec = BlobUploadSpec {
                filename: upload.name,
                mime_type: upload.mime_type,
                verification: UploadVerification::HubReceipt,
                expected_sha256: upload.sha256,
                idempotency_key: upload.key,
                content_length: upload.size,
                participant_id: Some(participant_id),
            };
            let reader =
                BodyReader::new(upload_chunks(body, state.shutdown.subscribe())).with_limit(limit);
            let (status, outcome) = connection
                .client()
                .upload_blob_with_status(spec, reader)
                .await
                .map_err(relay_upload_error)?;
            let receipt = match outcome {
                BlobUploadOutcome::CompleteWithHubDigest(receipt)
                | BlobUploadOutcome::AcknowledgedWithoutLocalVerification(receipt)
                | BlobUploadOutcome::VerifiedLocal(receipt) => receipt,
            };
            Ok((status, Json(receipt)))
        })
        .await
}

fn relay_upload_error(error: ClientError) -> NodeError {
    match error {
        ClientError::Problem(problem) => NodeError(problem),
        ClientError::HubIdentityMismatch { .. } => {
            NodeError::new(ErrorCode::HubIdentityMismatch, "Hub identity mismatch")
        }
        ClientError::Io(cause) => body_error(&cause),
        ClientError::BlobTooLarge { max_bytes } => blob_too_large(max_bytes),
        ClientError::BlobLengthMismatch { .. } => invalid_body(),
        ClientError::BlobDigestMismatch { .. } => NodeError::new(
            ErrorCode::BlobDigestMismatch,
            "The upload digest does not match",
        ),
        _ => NodeError::new(
            ErrorCode::DeliveryUnknown,
            "The Hub did not confirm the upload outcome",
        ),
    }
}

async fn put_inner(
    state: NodeState,
    principal: Principal,
    upload: Upload,
    body: Body,
) -> Result<(StatusCode, Json<AttachmentRef>)> {
    let _guard = upload
        .key
        .as_deref()
        .map(|key| {
            state
                .operations
                .guard_unhashed(principal.actor_id(), key, OPERATION_KIND)
        })
        .transpose()?;
    let operation = _guard.as_ref().map(|guard| guard.operation.clone());
    let claims = upload.claim_fingerprint()?;

    if let Some(operation) = &operation
        && let Some(receipt) = find_receipt(&state.reads, operation).await?
    {
        let hash = if let Some(hash) = claims {
            hash
        } else {
            let _upload_slot = state.tasks.try_acquire_upload_slot()?;
            let (size_bytes, sha256) = read_fingerprint(
                upload_chunks(body, state.shutdown.subscribe()),
                &upload,
                state.max_blob_bytes,
            )
            .await?;
            upload.fingerprint(size_bytes, &sha256)?
        };
        return receipt.replay(&hash);
    }

    let _upload_slot = state.tasks.try_acquire_upload_slot()?;
    let input = BlobInput {
        blob_id: BlobId::generate()?,
        original_name: upload.name,
        expected_size: upload.size,
        expected_sha256: upload.sha256,
    };
    let actor_id = principal.actor_id().to_owned();
    let mime_type = upload.mime_type;
    let registration_operation = operation.clone();
    let reader = BodyReader::new(upload_chunks(body, state.shutdown.subscribe()));
    match state
        .blobs
        .store(input, reader, move |tx, blob| {
            register_blob(tx, blob, actor_id, mime_type, registration_operation)
        })
        .await
    {
        Ok((_, attachment)) => Ok((StatusCode::CREATED, Json(attachment))),
        Err(WriteFailure::App(RegistrationError::Replay(attachment))) => {
            Ok((StatusCode::OK, Json(attachment)))
        }
        Err(WriteFailure::App(RegistrationError::Rejected(error))) => Err(error),
        Err(WriteFailure::Storage(error)) => {
            if matches!(
                error,
                StorageError::StorageFull
                    | StorageError::WriteOutcomeUnknown
                    | StorageError::CommitFailed(_)
            ) && let (Some(operation), Some(hash)) = (&operation, claims)
            {
                match find_receipt(&state.reads, operation).await {
                    Ok(Some(receipt)) => return receipt.replay(&hash),
                    Ok(None) => {}
                    Err(read_error) if read_error.code() == ErrorCode::OperationConflict => {
                        return Err(read_error);
                    }
                    Err(read_error) => {
                        tracing::warn!(
                            operation_id = %operation.id,
                            %read_error,
                            "Could not inspect blob receipt after uncertain storage write"
                        );
                    }
                }
            }
            Err(match error {
                StorageError::Io(cause) => body_error(&cause),
                other => other.into(),
            })
        }
    }
}

#[utoipa::path(
    get, path = "/v1/blobs/{id}", security(("bearer" = [])),
    params(("id" = BlobId, Path, description = "Blob identifier")),
    responses(
        (status = 200, description = "Raw attachment bytes with a strong SHA-256 ETag",
         content(
             (inline(BlobBytes) = "application/octet-stream"),
             (inline(BlobBytes) = "*/*")
         ),
         headers(
             ("Content-Length" = u64, description = "Byte length"),
             ("ETag" = String, description = "Quoted SHA-256 digest"),
             ("Content-Disposition" = String, description = "Attachment filename")
         )),
        (status = 400, description = "Invalid identifier", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 401, description = "Invalid bearer credential", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 403, description = "Access to the blob is denied", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 404, description = "Blob is not available", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 413, description = "Request body exceeds the control limit", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(crate) async fn get(
    State(state): State<NodeState>,
    Caller(principal): Caller,
    Path(id): Path<BlobId>,
) -> Result<Response> {
    let metadata = load_download(&state.reads, &principal, id.clone()).await?;
    let opened = match state.blobs.open_blob(&id).await {
        Ok(opened) => opened,
        Err(error @ (StorageError::BlobMissing(_) | StorageError::BlobUnverified(_))) => {
            tracing::warn!(%id, %error, "Could not open registered blob");
            return Err(blob_not_found());
        }
        Err(error) => return Err(error.into()),
    };
    let Some((record, file)) = opened.filter(|(record, _)| *record == metadata.record) else {
        return Err(blob_not_found());
    };

    let content_type = HeaderValue::from_str(&metadata.mime_type).map_err(NodeError::internal)?;
    let etag =
        HeaderValue::from_str(&format!("\"{}\"", metadata.sha256)).map_err(NodeError::internal)?;
    let safe_ascii: String = metadata
        .safe_disk_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let encoded = utf8_percent_encode(&metadata.original_name, NON_ALPHANUMERIC);
    let disposition = HeaderValue::from_str(&format!(
        "attachment; filename=\"{safe_ascii}\"; filename*=UTF-8''{encoded}"
    ))
    .map_err(NodeError::internal)?;
    let body = download_stream(File::from_std(file), record.size_bytes);
    Ok((
        [
            (CONTENT_TYPE, content_type),
            (CONTENT_LENGTH, HeaderValue::from(record.size_bytes)),
            (ETAG, etag),
            (CONTENT_DISPOSITION, disposition),
        ],
        body,
    )
        .into_response())
}

fn download_stream(file: File, size: u64) -> Body {
    let chunks = unfold(Some((file, size)), |state| async move {
        let (mut file, remaining) = state?;
        if remaining == 0 {
            return None;
        }
        let mut chunk = BytesMut::with_capacity(remaining.min(DOWNLOAD_CHUNK_BYTES) as usize);
        match file.read_buf(&mut chunk).await {
            Ok(0) => Some((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), None)),
            Ok(count) if (count as u64) <= remaining => {
                Some((Ok(chunk.freeze()), Some((file, remaining - count as u64))))
            }
            Ok(_) => Some((
                Err(io::Error::other("Read more than the registered blob size")),
                None,
            )),
            Err(error) => Some((Err(error), None)),
        }
    });
    Body::from_stream(chunks)
}

struct DownloadMetadata {
    record: BlobRecord,
    mime_type: String,
    sha256: String,
    original_name: String,
    safe_disk_name: String,
}

async fn load_download(
    reads: &ReadPool,
    principal: &Principal,
    id: BlobId,
) -> Result<DownloadMetadata> {
    let reader = principal.can_read_all();
    let participant = principal.is_active_participant();
    let actor = principal.actor_id().to_owned();
    let now = now_ms()?;
    reads.read(move |conn| {
        let tx = conn.unchecked_transaction()?;
        let blob = tx.query_row(
            "SELECT disk_rel_path,size_bytes,mime_type,sha256,original_name,safe_disk_name,owner_actor_id
             FROM blobs WHERE blob_id = ?1 AND expires_at_ms > ?2 AND deleted_at_ms IS NULL",
            params![id.as_str(), now],
            |row| Ok((
                DownloadMetadata {
                    record: BlobRecord {
                        blob_id: id.clone(),
                        disk_rel_path: PathBuf::from(row.get::<_, String>(0)?),
                        size_bytes: row.get(1)?,
                    },
                    mime_type: row.get(2)?,
                    sha256: row.get(3)?,
                    original_name: row.get(4)?,
                    safe_disk_name: row.get(5)?,
                },
                row.get::<_, String>(6)?,
            )),
        ).optional()?;
        let Some((metadata, owner)) = blob else { return Ok(Err(blob_not_found())) };
        let caller_can_read = reader || (participant && (actor == owner || tx.prepare_cached(
            BLOB_ACL_QUERY,
        )?.query_row(params![actor, id.as_str()], |row| row.get::<_, bool>(0))?));
        Ok(if caller_can_read {
            Ok(metadata)
        } else {
            Err(NodeError::new(ErrorCode::Forbidden, "This credential cannot access the blob"))
        })
    }).await?
}

#[derive(Debug)]
enum RegistrationError {
    Rejected(NodeError),
    Replay(AttachmentRef),
}

fn register_blob(
    tx: &Transaction<'_>,
    blob: &StoredBlob,
    actor_id: String,
    mime_type: String,
    operation: Option<Operation>,
) -> std::result::Result<WriteOutcome<AttachmentRef>, WriteError<RegistrationError>> {
    let domain = |error| WriteError::Domain(RegistrationError::Rejected(error));
    let now = now_ms().map_err(|error| domain(error.into()))?;
    let attachment = AttachmentRef {
        blob_id: blob.blob_id.clone(),
        name: blob.original_name.clone(),
        mime_type,
        size_bytes: blob.size_bytes,
        sha256: blob.sha256.clone(),
    };
    let operation = if let Some(mut operation) = operation {
        operation.hash = fingerprint(
            &attachment.name,
            &attachment.mime_type,
            attachment.size_bytes,
            &attachment.sha256,
        )
        .map_err(domain)?;
        match operation.lookup_state(tx).map_err(domain)? {
            ReceiptLookup::Missing => {}
            ReceiptLookup::Succeeded(value) => {
                let receipt = attachment_receipt(value).map_err(domain)?;
                return Err(WriteError::Domain(RegistrationError::Replay(receipt)));
            }
            ReceiptLookup::Pending { id, .. } => {
                return Err(domain(operation_in_progress(id)));
            }
            ReceiptLookup::Failed(error) => {
                return Err(domain(error));
            }
            ReceiptLookup::Unknown { id, problem, .. } => {
                return Err(domain(problem.unwrap_or_else(|| {
                    NodeError::new(
                        ErrorCode::DeliveryUnknown,
                        "The earlier attempt may have taken effect",
                    )
                    .with_extension("operation_id", id)
                })));
            }
        }
        operation.reserve(tx, now, Some(blob.blob_id.as_str()))?;
        Some(operation)
    } else {
        None
    };

    let disk_rel_path = blob
        .disk_rel_path
        .to_str()
        .ok_or_else(|| domain(NodeError::internal("Blob path is not UTF-8")))?;
    tx.execute(
        "INSERT INTO blobs (blob_id, sha256, size_bytes, mime_type, original_name, safe_disk_name,
                            disk_rel_path, owner_actor_id, created_at_ms, expires_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            attachment.blob_id.as_str(),
            attachment.sha256,
            attachment.size_bytes,
            attachment.mime_type,
            attachment.name,
            blob.safe_disk_name,
            disk_rel_path,
            actor_id,
            now,
            now.saturating_add(UNATTACHED_TTL_MS),
        ],
    )?;
    if let Some(operation) = operation {
        operation.succeed(tx, now, &attachment)?;
    }
    let event = append_event(
        tx,
        now,
        EventKind::BlobStored,
        AggregateType::Blob,
        blob.blob_id.as_str(),
        Some(&actor_id),
        json!({
            "blob_id": attachment.blob_id,
            "original_name": attachment.name,
            "mime_type": attachment.mime_type,
            "size_bytes": attachment.size_bytes,
            "sha256": attachment.sha256,
        }),
    )?;
    Ok(WriteOutcome::with_events(attachment, vec![event]))
}

enum ReceiptOutcome {
    Succeeded(AttachmentRef),
    Pending(String),
    Rejected(NodeError),
}

struct Receipt {
    hash: String,
    outcome: ReceiptOutcome,
}

impl Receipt {
    fn replay(self, hash: &str) -> Result<(StatusCode, Json<AttachmentRef>)> {
        if self.hash != hash {
            return Err(NodeError::new(
                ErrorCode::OperationConflict,
                "The key is already bound to a different request",
            ));
        }
        match self.outcome {
            ReceiptOutcome::Succeeded(result) => Ok((StatusCode::OK, Json(result))),
            ReceiptOutcome::Pending(id) => Err(operation_in_progress(id)),
            ReceiptOutcome::Rejected(error) => Err(error),
        }
    }
}

fn operation_in_progress(id: String) -> NodeError {
    NodeError::new(
        ErrorCode::OperationInProgress,
        "The earlier attempt is still in progress",
    )
    .with_extension("operation_id", id)
}

async fn find_receipt(reads: &ReadPool, operation: &Operation) -> Result<Option<Receipt>> {
    let mut operation = operation.clone();
    reads
        .read(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let stored: Option<(String, String)> = tx
                .query_row(
                    "SELECT kind, request_sha256 FROM operations
                     WHERE actor_id = ?1 AND operation_key = ?2",
                    params![operation.actor_id, operation.key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((kind, hash)) = stored else {
                return Ok(Ok(None));
            };
            if kind != OPERATION_KIND {
                return Ok(Err(NodeError::new(
                    ErrorCode::OperationConflict,
                    "The key is already bound to a different request",
                )));
            }
            // Load the stored outcome without bypassing `request_sha256` matching.
            // Callers verify claims (or stream hash) against `Receipt.hash` before
            // returning any replayed response or error.
            operation.hash = hash.clone();
            let outcome = match operation.lookup_state(&tx) {
                Ok(ReceiptLookup::Missing) => return Ok(Ok(None)),
                Ok(ReceiptLookup::Succeeded(value)) => match attachment_receipt(value) {
                    Ok(receipt) => ReceiptOutcome::Succeeded(receipt),
                    Err(error) => return Ok(Err(error)),
                },
                Ok(ReceiptLookup::Pending { id, .. }) => ReceiptOutcome::Pending(id),
                Ok(ReceiptLookup::Failed(error)) => ReceiptOutcome::Rejected(error),
                Ok(ReceiptLookup::Unknown { id, problem, .. }) => {
                    ReceiptOutcome::Rejected(problem.unwrap_or_else(|| {
                        NodeError::new(
                            ErrorCode::DeliveryUnknown,
                            "The earlier attempt may have taken effect",
                        )
                        .with_extension("operation_id", id)
                    }))
                }
                Err(error) => return Ok(Err(error)),
            };
            Ok(Ok(Some(Receipt { hash, outcome })))
        })
        .await?
}

fn attachment_receipt(value: Value) -> Result<AttachmentRef> {
    let receipt: AttachmentRef = serde_json::from_value(value).map_err(NodeError::internal)?;
    receipt.validate().map_err(NodeError::internal)?;
    Ok(receipt)
}

struct Upload {
    name: String,
    mime_type: String,
    size: Option<u64>,
    sha256: Option<String>,
    key: Option<String>,
}

impl Upload {
    fn parse(headers: &HeaderMap, limit: u64) -> Result<Self> {
        let encoded = one_header(headers, X_COOPLET_FILENAME)?
            .ok_or_else(|| invalid_header(X_COOPLET_FILENAME.as_str()))?;
        let name = decode_filename(encoded)?;
        let mime_type = one_header(headers, CONTENT_TYPE)?
            .ok_or_else(|| invalid_header(CONTENT_TYPE.as_str()))?;
        validate_media_type(mime_type).map_err(|_| invalid_header(CONTENT_TYPE.as_str()))?;
        let size = one_header(headers, CONTENT_LENGTH)?
            .map(|length| {
                if length.is_empty() || !length.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(invalid_header(CONTENT_LENGTH.as_str()));
                }
                length
                    .parse()
                    .map_err(|_| invalid_header(CONTENT_LENGTH.as_str()))
            })
            .transpose()?;
        let sha256 = one_header(headers, X_COOPLET_SHA256)?;
        if let Some(sha256) = sha256 {
            validate_sha256(sha256).map_err(|_| invalid_header(X_COOPLET_SHA256.as_str()))?;
        }
        let key = idempotency_key(headers, false)?.map(str::to_owned);
        if size.is_some_and(|bytes| bytes > limit) {
            return Err(blob_too_large(limit));
        }
        Ok(Self {
            name,
            mime_type: mime_type.into(),
            size,
            sha256: sha256.map(str::to_owned),
            key,
        })
    }

    fn claim_fingerprint(&self) -> Result<Option<String>> {
        self.size
            .zip(self.sha256.as_deref())
            .map(|(size, sha256)| self.fingerprint(size, sha256))
            .transpose()
    }

    fn fingerprint(&self, size_bytes: u64, sha256: &str) -> Result<String> {
        fingerprint(&self.name, &self.mime_type, size_bytes, sha256)
    }
}

fn invalid_header(name: &str) -> NodeError {
    NodeError::new(
        ErrorCode::InvalidRequest,
        format!("Invalid or missing {name} header"),
    )
}

fn decode_filename(encoded: &str) -> Result<String> {
    let bytes = encoded.as_bytes();
    for (offset, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && !bytes
                .get(offset + 1..offset + 3)
                .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(invalid_header(X_COOPLET_FILENAME.as_str()));
        }
    }
    let name = percent_decode_str(encoded)
        .decode_utf8()
        .map_err(|_| invalid_header(X_COOPLET_FILENAME.as_str()))?;
    validate_attachment_name(&name).map_err(|_| invalid_header(X_COOPLET_FILENAME.as_str()))?;
    Ok(name.into_owned())
}

#[derive(Serialize)]
struct Fingerprint<'a> {
    name: &'a str,
    mime_type: &'a str,
    size_bytes: u64,
    sha256: &'a str,
}

fn fingerprint(name: &str, mime_type: &str, size_bytes: u64, sha256: &str) -> Result<String> {
    canonical_sha256(&Fingerprint {
        name,
        mime_type,
        size_bytes,
        sha256,
    })
    .map_err(NodeError::internal)
}

async fn read_fingerprint(
    mut chunks: UploadChunks,
    upload: &Upload,
    limit: u64,
) -> Result<(u64, String)> {
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|error| body_error(&error))?;
        bytes = bytes.saturating_add(chunk.len() as u64);
        if bytes > limit {
            return Err(blob_too_large(limit));
        }
        if upload.size.is_some_and(|expected| bytes > expected) {
            return Err(invalid_body());
        }
        digest.update(chunk);
    }
    if upload.size.is_some_and(|expected| bytes != expected) {
        return Err(invalid_body());
    }
    let sha256 = format!("{:x}", digest.finalize());
    if upload
        .sha256
        .as_ref()
        .is_some_and(|expected| *expected != sha256)
    {
        return Err(NodeError::new(
            ErrorCode::BlobDigestMismatch,
            "The body does not match the claimed SHA-256",
        ));
    }
    Ok((bytes, sha256))
}

type UploadChunks = BoxStream<'static, io::Result<Bytes>>;

#[derive(Debug, thiserror::Error)]
enum UploadBodyError {
    #[error("incomplete HTTP upload body")]
    Incomplete,
    #[error("upload body stalled")]
    Timeout,
    #[error("Node is shutting down")]
    Stopping,
    #[error("upload body exceeds the Node byte limit")]
    TooLarge(u64),
}

fn upload_chunks(body: Body, shutdown: watch::Receiver<bool>) -> UploadChunks {
    let total_deadline = tokio::time::Instant::now() + UPLOAD_TOTAL_TIMEOUT;
    Box::pin(unfold(
        Some((body.into_data_stream(), shutdown, total_deadline)),
        |state| async move {
            let (mut chunks, mut shutdown, total_deadline) = state?;
            let now = tokio::time::Instant::now();
            if now >= total_deadline {
                return Some((Err(io::Error::other(UploadBodyError::Timeout)), None));
            }
            let step_deadline = (now + UPLOAD_CHUNK_TIMEOUT).min(total_deadline);
            let chunk = tokio::select! {
                biased;
                _ = shutdown.wait_for(|stopping| *stopping) => {
                    return Some((Err(io::Error::other(UploadBodyError::Stopping)), None));
                }
                chunk = tokio::time::timeout_at(step_deadline, chunks.next()) => match chunk {
                    Ok(chunk) => chunk,
                    Err(_) => return Some((Err(io::Error::other(UploadBodyError::Timeout)), None)),
                },
            };
            match chunk {
                Some(Ok(bytes)) => Some((Ok(bytes), Some((chunks, shutdown, total_deadline)))),
                Some(Err(_)) => Some((Err(io::Error::other(UploadBodyError::Incomplete)), None)),
                None => None,
            }
        },
    ))
}

struct BodyReader {
    chunks: UploadChunks,
    pending: Bytes,
    remaining: Option<u64>,
    limit: u64,
}

impl BodyReader {
    fn new(chunks: UploadChunks) -> Self {
        Self {
            chunks,
            pending: Bytes::new(),
            remaining: None,
            limit: 0,
        }
    }

    fn with_limit(mut self, limit: u64) -> Self {
        self.remaining = Some(limit);
        self.limit = limit;
        self
    }
}

impl AsyncRead for BodyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        while this.pending.is_empty() {
            match ready!(this.chunks.poll_next_unpin(cx)) {
                Some(Ok(chunk)) => {
                    if let Some(remaining) = &mut this.remaining {
                        let Some(after) = remaining.checked_sub(chunk.len() as u64) else {
                            return Poll::Ready(Err(io::Error::other(UploadBodyError::TooLarge(
                                this.limit,
                            ))));
                        };
                        *remaining = after;
                    }
                    this.pending = chunk;
                }
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => return Poll::Ready(Ok(())),
            }
        }
        let count = this.pending.len().min(buffer.remaining());
        buffer.put_slice(&this.pending.split_to(count));
        Poll::Ready(Ok(()))
    }
}

fn body_error(error: &io::Error) -> NodeError {
    match error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<UploadBodyError>())
    {
        Some(UploadBodyError::Incomplete) => invalid_body(),
        Some(UploadBodyError::Timeout) => NodeError::new(
            ErrorCode::InvalidRequest,
            "Timed out reading the upload body",
        )
        .with_extension("reason", "upload_timeout"),
        Some(UploadBodyError::Stopping) => {
            NodeError::new(ErrorCode::Overloaded, "Node is shutting down")
        }
        Some(UploadBodyError::TooLarge(limit)) => blob_too_large(*limit),
        None => NodeError::internal(error),
    }
}

fn invalid_body() -> NodeError {
    NodeError::new(
        ErrorCode::InvalidRequest,
        "The upload body did not reach its declared end",
    )
}

fn blob_too_large(limit: u64) -> NodeError {
    NodeError::new(ErrorCode::BlobTooLarge, "Blob exceeds the upload limit")
        .with_extension("limit_bytes", limit)
}

fn blob_not_found() -> NodeError {
    NodeError::new(ErrorCode::NotFound, "Blob is not available")
}

#[cfg(test)]
mod tests;
