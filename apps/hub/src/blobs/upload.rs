use axum::body::Body;
use cooplet_client::{
    IDEMPOTENCY_KEY, X_COOPLET_FILENAME, X_COOPLET_PARTICIPANT, X_COOPLET_SHA256,
};
use cooplet_protocol::{
    AggregateType, AttachmentRef, BlobId, ErrorCode, EventKind, HubId, ParticipantId, ServiceScope,
    canonical_sha256, validate_attachment_name, validate_idempotency_key, validate_media_type,
    validate_sha256,
};
use cooplet_storage::{
    BlobInput, StorageError, StoredBlob, WriteError, WriteFailure, WriteOutcome,
};
use http::{HeaderMap, StatusCode, header};
use percent_encoding::percent_decode_str;
use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::{
    AppState,
    auth::{AuthenticatedCaller, attest_node_participant, reverify_caller_tx},
    error::HubError,
    storage::{
        events::{EventAudience, NewEvent, append, blob_event_data},
        operations::{self, NewOperation},
        sql::read_verified_tx_with_clock,
    },
};

use super::{
    BLOB_PUT, blob_forbidden, blob_operation_id,
    body::{CHUNK_BYTES, UploadReader, upload_read_error},
    lookup_blob, require_blob_scope,
};

const UNATTACHED_TTL_MS: i64 = 60 * 60 * 1_000;

pub(crate) async fn upload(
    state: &AppState,
    caller: &AuthenticatedCaller,
    headers: &HeaderMap,
    body: Body,
) -> Result<(StatusCode, AttachmentRef), HubError> {
    let attributed = match header_value(headers, X_COOPLET_PARTICIPANT.as_str())? {
        Some(value) => {
            let (node_id, _) = caller.node_bridge_identity().ok_or_else(blob_forbidden)?;
            let node_id = node_id.clone();
            let participant_id = ParticipantId::parse(value)?;
            Some(
                read_verified_tx_with_clock(
                    &state.reads,
                    caller,
                    state.wall_clock.clone(),
                    move |db, _, _| attest_node_participant(db, &node_id, &participant_id, None),
                )
                .await?,
            )
        }
        None => None,
    };
    let caller = attributed.as_ref().unwrap_or(caller);
    require_blob_scope(caller, ServiceScope::Collaborate)?;
    let request = UploadHeaders::parse(headers, state.max_blob_bytes)?;
    if let Some(key) = &request.key
        && let Some(receipt) = read_receipt(state, caller, key).await?
    {
        let attachment = replay(state, caller, key, &request, receipt, body).await?;
        return Ok((StatusCode::OK, attachment));
    }

    let blob_id = BlobId::generate()?;
    let input = BlobInput {
        blob_id: blob_id.clone(),
        original_name: request.name,
        expected_size: request.length,
        expected_sha256: request.sha256,
    };
    let register = {
        let caller = caller.clone();
        let hub_id = state.meta.hub_id.clone();
        let wall_clock = state.wall_clock.clone();
        move |tx: &Transaction<'_>, stored: &StoredBlob| {
            register(
                tx,
                &caller,
                &hub_id,
                request.key.as_deref(),
                &request.mime_type,
                stored,
                wall_clock(),
            )
        }
    };
    let reader = UploadReader::new(body, state.shutdown.clone());
    match state.blobs.store(input, reader, register).await {
        Ok((_, attachment)) => Ok((StatusCode::CREATED, attachment)),
        Err(WriteFailure::App(RegistrationError::Replay(attachment))) => {
            // The storage engine has unlinked the redundant object and returned
            // its reserved disk quota before delivering the replay.
            Ok((StatusCode::OK, attachment))
        }
        Err(WriteFailure::App(RegistrationError::Rejected(error))) => Err(error),
        Err(WriteFailure::Storage(StorageError::Io(error))) => Err(upload_read_error(error)),
        Err(WriteFailure::Storage(
            error @ (StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)),
        )) => {
            // The attempt's fresh blob ID can prove its own commit without
            // retrying registration or replaying the body.
            if let Some(attachment) = observe_registration(state, caller, blob_id).await? {
                Ok((StatusCode::CREATED, attachment))
            } else {
                Err(error.into())
            }
        }
        Err(WriteFailure::Storage(error)) => Err(error.into()),
    }
}

struct UploadHeaders {
    name: String,
    mime_type: String,
    length: Option<u64>,
    sha256: Option<String>,
    key: Option<String>,
}

impl UploadHeaders {
    fn parse(headers: &HeaderMap, max_size: u64) -> Result<Self, HubError> {
        let mime_type = header_value(headers, header::CONTENT_TYPE.as_str())?
            .ok_or_else(|| HubError::invalid("Content-Type is required."))?;
        validate_media_type(mime_type).map_err(|error| HubError::invalid(error.to_string()))?;
        let filename = header_value(headers, X_COOPLET_FILENAME.as_str())?
            .ok_or_else(|| HubError::invalid("X-Cooplet-Filename is required."))?;
        let name = decode_filename(filename)?;
        let sha256 = header_value(headers, X_COOPLET_SHA256.as_str())?;
        if let Some(sha256) = sha256 {
            validate_sha256(sha256).map_err(|error| HubError::invalid(error.to_string()))?;
        }
        let key = header_value(headers, IDEMPOTENCY_KEY.as_str())?;
        if let Some(key) = key {
            validate_idempotency_key(key).map_err(|error| HubError::invalid(error.to_string()))?;
        }
        let length = header_value(headers, header::CONTENT_LENGTH.as_str())?
            .map(parse_length)
            .transpose()?;
        if length.is_some_and(|size| size > max_size) {
            return Err(StorageError::BlobTooLarge {
                limit_bytes: max_size,
            }
            .into());
        }
        Ok(Self {
            name,
            mime_type: mime_type.to_owned(),
            length,
            sha256: sha256.map(str::to_owned),
            key: key.map(str::to_owned),
        })
    }
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, HubError> {
    let mut values = headers.get_all(name).into_iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(HubError::invalid(format!("Repeated {name} header.")));
    }
    first
        .map(|value| {
            value
                .to_str()
                .map_err(|_| HubError::invalid(format!("Invalid {name} header.")))
        })
        .transpose()
}

fn parse_length(value: &str) -> Result<u64, HubError> {
    if !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && let Ok(length) = value.parse()
    {
        return Ok(length);
    }
    Err(HubError::invalid("Invalid Content-Length."))
}

fn decode_filename(value: &str) -> Result<String, HubError> {
    let invalid = || HubError::invalid("X-Cooplet-Filename must be percent-encoded UTF-8.");
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%'
            && !(bytes.next().is_some_and(|digit| digit.is_ascii_hexdigit())
                && bytes.next().is_some_and(|digit| digit.is_ascii_hexdigit()))
        {
            return Err(invalid());
        }
    }
    let name = percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| invalid())?;
    validate_attachment_name(&name).map_err(|error| HubError::invalid(error.to_string()))?;
    Ok(name.into_owned())
}

#[derive(Serialize)]
struct RequestDigest<'a> {
    name: &'a str,
    mime_type: &'a str,
    size_bytes: u64,
    sha256: &'a str,
}

fn receipt_hash(
    name: &str,
    mime_type: &str,
    size_bytes: u64,
    sha256: &str,
) -> Result<String, HubError> {
    canonical_sha256(&RequestDigest {
        name,
        mime_type,
        size_bytes,
        sha256,
    })
    .map_err(HubError::internal)
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    attachment: AttachmentRef,
    ever_attached: bool,
}

struct PriorUpload {
    request_sha256: String,
    outcome: PriorOutcome,
}

enum PriorOutcome {
    Succeeded(AttachmentRef),
    Error(HubError),
}

async fn read_receipt(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
) -> Result<Option<PriorUpload>, HubError> {
    let key = key.to_owned();
    read_verified_tx_with_clock(
        &state.reads,
        caller,
        state.wall_clock.clone(),
        move |snapshot, caller, now| lookup_receipt(snapshot, caller, &key, now),
    )
    .await
}

fn lookup_receipt(
    conn: &Connection,
    caller: &AuthenticatedCaller,
    key: &str,
    now: i64,
) -> Result<Option<PriorUpload>, HubError> {
    let Some(saved) = operations::load(conn, caller.actor_id(), key)? else {
        return Ok(None);
    };
    if saved.kind != BLOB_PUT {
        return Err(receipt_conflict());
    }
    let outcome = match saved.state.as_str() {
        "succeeded" => {
            let receipt: Receipt = serde_json::from_str(
                saved
                    .result_json
                    .as_deref()
                    .ok_or_else(|| HubError::internal("missing successful upload receipt"))?,
            )?;
            match lookup_blob(conn, &receipt.attachment.blob_id)? {
                Some(blob) => blob.require_read(conn, caller, now)?,
                None if receipt.ever_attached && !caller.is_global_reader() => {
                    return Err(blob_forbidden());
                }
                // The idempotency key is already scoped to this actor.
                None => {}
            }
            PriorOutcome::Succeeded(receipt.attachment)
        }
        "accepted" | "in_progress" => {
            PriorOutcome::Error(saved.in_progress_error("This upload is in progress.")?)
        }
        "unknown" => PriorOutcome::Error(
            saved.unknown_error("The Hub could not confirm this upload.", false)?,
        ),
        "failed" => PriorOutcome::Error(saved.failed_error()?),
        _ => return Err(HubError::internal("invalid upload receipt state")),
    };
    Ok(Some(PriorUpload {
        request_sha256: saved.request_sha256,
        outcome,
    }))
}

async fn replay(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    request: &UploadHeaders,
    receipt: PriorUpload,
    body: Body,
) -> Result<AttachmentRef, HubError> {
    if let PriorOutcome::Succeeded(attachment) = &receipt.outcome
        && (request.name != attachment.name || request.mime_type != attachment.mime_type)
    {
        return Err(receipt_conflict());
    }
    if let (Some(length), Some(sha256)) = (request.length, request.sha256.as_deref()) {
        return resolve_receipt(receipt, request, length, sha256);
    }

    let mut reader = UploadReader::new(body, state.shutdown.clone());
    let (length, sha256) = hash_body(&mut reader, state.max_blob_bytes, request.length).await?;
    if request
        .sha256
        .as_ref()
        .is_some_and(|expected| expected != &sha256)
    {
        return Err(StorageError::DigestMismatch.into());
    }
    // The file may have been attached elsewhere, or this credential may
    // have been revoked, while the retry's stream was arriving.
    let fresh = read_receipt(state, caller, key)
        .await?
        .ok_or_else(receipt_conflict)?;
    resolve_receipt(fresh, request, length, &sha256)
}

fn resolve_receipt(
    receipt: PriorUpload,
    request: &UploadHeaders,
    length: u64,
    sha256: &str,
) -> Result<AttachmentRef, HubError> {
    let digest = receipt_hash(&request.name, &request.mime_type, length, sha256)?;
    if digest != receipt.request_sha256 {
        return Err(receipt_conflict());
    }
    match receipt.outcome {
        PriorOutcome::Succeeded(attachment) => Ok(attachment),
        PriorOutcome::Error(error) => Err(error),
    }
}

async fn hash_body(
    reader: &mut UploadReader,
    limit: u64,
    expected_length: Option<u64>,
) -> Result<(u64, String), HubError> {
    let mut chunk = vec![0; CHUNK_BYTES];
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    loop {
        let count = reader.read(&mut chunk).await.map_err(upload_read_error)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .filter(|size| *size <= limit)
            .ok_or(StorageError::BlobTooLarge { limit_bytes: limit })?;
        if let Some(expected) = expected_length
            && total > expected
        {
            return Err(StorageError::LengthMismatch {
                expected,
                actual: total,
            }
            .into());
        }
        hasher.update(&chunk[..count]);
    }
    if let Some(expected) = expected_length
        && total != expected
    {
        return Err(StorageError::LengthMismatch {
            expected,
            actual: total,
        }
        .into());
    }
    Ok((total, format!("{:x}", hasher.finalize())))
}

enum RegistrationError {
    Replay(AttachmentRef),
    Rejected(HubError),
}

impl From<HubError> for WriteError<RegistrationError> {
    fn from(error: HubError) -> Self {
        Self::Domain(RegistrationError::Rejected(error))
    }
}

fn register(
    tx: &Transaction<'_>,
    caller: &AuthenticatedCaller,
    hub_id: &HubId,
    key: Option<&str>,
    mime_type: &str,
    stored: &StoredBlob,
    now_ms: i64,
) -> Result<WriteOutcome<AttachmentRef>, WriteError<RegistrationError>> {
    reverify_caller_tx(tx, caller, now_ms)?;
    let attachment = AttachmentRef {
        blob_id: stored.blob_id.clone(),
        name: stored.original_name.clone(),
        mime_type: mime_type.to_owned(),
        size_bytes: stored.size_bytes,
        sha256: stored.sha256.clone(),
    };
    let digest = key
        .map(|_| {
            receipt_hash(
                &attachment.name,
                mime_type,
                attachment.size_bytes,
                &attachment.sha256,
            )
        })
        .transpose()?;
    if let (Some(key), Some(digest)) = (key, &digest)
        && let Some(prior) = lookup_receipt(tx, caller, key, now_ms)?
    {
        if prior.request_sha256 != *digest {
            return Err(receipt_conflict().into());
        }
        return match prior.outcome {
            PriorOutcome::Succeeded(attachment) => {
                Err(WriteError::Domain(RegistrationError::Replay(attachment)))
            }
            PriorOutcome::Error(error) => Err(error.into()),
        };
    }

    let path = stored
        .disk_rel_path
        .to_str()
        .ok_or_else(|| HubError::internal("blob relative path is not UTF-8"))?;
    tx.prepare_cached(
        "INSERT INTO blobs(blob_id, sha256, size_bytes, mime_type, original_name, safe_disk_name,
                           disk_rel_path, owner_actor_id, created_at_ms, expires_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?
    .execute(params![
        attachment.blob_id.as_str(),
        attachment.sha256,
        attachment.size_bytes,
        mime_type,
        attachment.name,
        stored.safe_disk_name,
        path,
        caller.actor_id(),
        now_ms,
        now_ms.saturating_add(UNATTACHED_TTL_MS)
    ])?;
    if let (Some(key), Some(digest)) = (key, digest) {
        let receipt = Receipt {
            attachment: attachment.clone(),
            ever_attached: false,
        };
        let result = serde_json::to_string(&receipt).map_err(HubError::from)?;
        let operation_id = blob_operation_id(&attachment.blob_id)?;
        operations::insert(
            tx,
            NewOperation {
                operation_id: &operation_id,
                actor_id: caller.actor_id(),
                key,
                kind: BLOB_PUT,
                request_sha256: &digest,
                target_node_id: None,
                state: "succeeded",
                resource_id: Some(attachment.blob_id.as_str()),
                result_json: Some(&result),
                now_ms,
            },
        )?;
    }
    let event = append(
        tx,
        hub_id,
        NewEvent {
            kind: EventKind::BlobStored,
            aggregate_type: AggregateType::Blob,
            aggregate_id: attachment.blob_id.as_str(),
            actor_id: Some(caller.actor_id()),
            created_at_ms: now_ms,
            data: blob_event_data(&attachment),
            audience: EventAudience::OwnerOnly {
                actor_id: caller.actor_id().to_owned(),
            },
        },
    )?;
    Ok(WriteOutcome::with_events(attachment, vec![event]))
}

async fn observe_registration(
    state: &AppState,
    caller: &AuthenticatedCaller,
    blob_id: BlobId,
) -> Result<Option<AttachmentRef>, HubError> {
    read_verified_tx_with_clock(
        &state.reads,
        caller,
        state.wall_clock.clone(),
        move |snapshot, caller, now| {
            let Some(blob) = lookup_blob(snapshot, &blob_id)? else {
                return Ok(None);
            };
            if blob.owner_actor_id != caller.actor_id() {
                return Err(blob_forbidden());
            }
            blob.require_read(snapshot, caller, now)?;
            Ok(Some(blob.attachment))
        },
    )
    .await
}

fn receipt_conflict() -> HubError {
    HubError::new(
        ErrorCode::OperationConflict,
        "This key was used with a different request.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn rejects_invalid_filename_escape_utf8_and_controls() -> Result<(), HubError> {
        for malformed in ["%2", "%zz", "%FF", "%00", "%0A", "", "%20"] {
            assert!(decode_filename(malformed).is_err());
        }
        assert_eq!(decode_filename("r%C3%A9sum%C3%A9%20%2F")?, "résumé /");
        Ok(())
    }

    #[test]
    fn upload_rejects_duplicate_headers_with_identical_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/pdf"),
        );
        headers.insert(X_COOPLET_FILENAME, HeaderValue::from_static("report.pdf"));
        for (name, sample) in [
            (header::CONTENT_TYPE, "application/pdf"),
            (X_COOPLET_FILENAME, "report.pdf"),
            (header::CONTENT_LENGTH, "2"),
            (
                X_COOPLET_SHA256,
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            ),
            (IDEMPOTENCY_KEY, "upload-key"),
        ] {
            let _ = headers.remove(&name);
            headers.append(&name, HeaderValue::from_static(sample));
            assert!(UploadHeaders::parse(&headers, 10).is_ok());
            headers.append(&name, HeaderValue::from_static(sample));
            assert!(UploadHeaders::parse(&headers, 10).is_err());
            let _ = headers.remove(&name);
            headers.insert(name, HeaderValue::from_static(sample));
        }
    }

    #[tokio::test]
    async fn streaming_replay_limits_bytes_and_requires_the_declared_length() {
        let (_send, shutdown) = tokio::sync::watch::channel(());
        let mut reader = UploadReader::new(Body::from("extra"), shutdown.clone());
        let too_large = hash_body(&mut reader, 2, None).await;
        assert!(too_large.is_err_and(|error| error.problem().code == ErrorCode::BlobTooLarge));
        let mut reader = UploadReader::new(Body::from("a"), shutdown);
        let wrong_length = hash_body(&mut reader, 9, Some(2)).await;
        assert!(wrong_length.is_err_and(|error| error.problem().code == ErrorCode::InvalidRequest));
    }

    #[test]
    fn accepts_tab_as_media_type_optional_whitespace() -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_bytes(b"text/plain;\tcharset=utf-8")?,
        );
        headers.insert(X_COOPLET_FILENAME, HeaderValue::from_static("note.txt"));
        assert_eq!(
            UploadHeaders::parse(&headers, 10)?.mime_type,
            "text/plain;\tcharset=utf-8"
        );
        Ok(())
    }
}
