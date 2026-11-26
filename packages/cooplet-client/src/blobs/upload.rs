use std::{
    io,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use cooplet_protocol::{AttachmentRef, validate_sha256};
use futures_util::stream;
use http::{
    HeaderValue, Method, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};

use super::{CHUNK_BYTES, check_digest, checked_blob_size};
use crate::{
    ClientError, CoopletClient, X_COOPLET_FILENAME, X_COOPLET_SHA256, client::timeout,
    error::read_bounded,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadVerification {
    LocalSha256,
    HubReceipt,
}

#[derive(Clone)]
pub struct BlobUploadSpec {
    pub filename: String,
    pub mime_type: String,
    pub verification: UploadVerification,
    pub expected_sha256: Option<String>,
    pub idempotency_key: Option<String>,
    pub content_length: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobUploadOutcome {
    VerifiedLocal(AttachmentRef),
    CompleteWithHubDigest(AttachmentRef),
    AcknowledgedWithoutLocalVerification(AttachmentRef),
}

impl CoopletClient {
    pub async fn upload_blob<R: AsyncRead + Unpin>(
        &self,
        spec: BlobUploadSpec,
        mut reader: R,
    ) -> Result<BlobUploadOutcome, ClientError> {
        let url = self.url("/v1/blobs", &[])?;
        let headers = UploadHeaders::validate(&spec, self.config.max_blob_bytes)?;
        if spec.verification == UploadVerification::HubReceipt && self.expected_hub().is_none() {
            return Err(ClientError::invalid(
                "Hub receipt verification requires a pinned Hub",
            ));
        }
        let mut request = self
            .protected(Method::POST, url, spec.idempotency_key.as_deref())
            .await?
            .header(X_COOPLET_FILENAME, headers.filename)
            .header(CONTENT_TYPE, headers.content_type);
        if let Some(length) = spec.content_length {
            request = request.header(CONTENT_LENGTH, length);
        }
        if let Some(sha256) = spec.expected_sha256.as_deref() {
            request = request.header(X_COOPLET_SHA256, sha256);
        }

        let (send, receive) = mpsc::channel(1);
        let progress = Arc::new(WireProgress::default());
        let request = request.body(wire_body(receive, progress.clone()));
        let (response, stats) = {
            let mut response = pin!(request.send());
            let mut source = pin!(produce(
                &mut reader,
                send,
                &spec,
                self.config.max_blob_bytes,
                self.config.request_timeout,
            ));
            tokio::select! {
                biased;
                result = &mut source => {
                    let stats = result?;
                    (timeout(self.config.request_timeout, "blob upload response", async {
                        Ok(response.await?)
                    }).await?, stats)
                }
                result = &mut response => (result?, None),
            }
        };

        let status = response.status();
        if !matches!(status, StatusCode::OK | StatusCode::CREATED) {
            return Err(self.failure(response, false).await);
        }
        let receipt: AttachmentRef = timeout(self.config.request_timeout, "blob receipt", async {
            let bytes = read_bounded(response, self.config.max_json_response_bytes).await?;
            serde_json::from_slice(&bytes)
                .map_err(|_| ClientError::protocol("invalid blob receipt"))
        })
        .await?;
        receipt
            .validate()
            .map_err(|_| ClientError::protocol("invalid blob receipt"))?;

        if let Some(length) = spec.content_length {
            check_length(length, receipt.size_bytes)?;
        }
        if let Some(expected) = spec.expected_sha256.as_deref() {
            check_digest(expected, &receipt.sha256)?;
        }
        if let Some(stats) = stats
            && progress.complete(stats.bytes, spec.content_length.is_some())
        {
            check_length(stats.bytes, receipt.size_bytes)?;
            return match stats.sha256 {
                Some(sha256) => {
                    check_digest(&sha256, &receipt.sha256)?;
                    Ok(BlobUploadOutcome::VerifiedLocal(receipt))
                }
                None => Ok(BlobUploadOutcome::CompleteWithHubDigest(receipt)),
            };
        }
        if status == StatusCode::OK
            && spec.idempotency_key.is_some()
            && spec.content_length.is_some()
            && spec.expected_sha256.is_some()
        {
            return Ok(BlobUploadOutcome::AcknowledgedWithoutLocalVerification(
                receipt,
            ));
        }
        Err(ClientError::protocol(
            "blob receipt arrived before confirmed source EOF and upload",
        ))
    }
}

struct UploadHeaders {
    filename: HeaderValue,
    content_type: HeaderValue,
}

impl UploadHeaders {
    fn validate(spec: &BlobUploadSpec, max_bytes: u64) -> Result<Self, ClientError> {
        if spec.filename.is_empty() || spec.filename.len() > 255 || spec.filename.contains('\0') {
            return Err(ClientError::invalid("invalid blob filename"));
        }
        if let Some(expected) = spec.expected_sha256.as_deref() {
            validate_sha256(expected)
                .map_err(|_| ClientError::invalid("invalid expected SHA-256"))?;
        }
        if let Some(length) = spec.content_length {
            checked_blob_size(0, length, max_bytes)?;
        }
        let invalid_mime = || ClientError::invalid("invalid blob content type");
        let mime = spec.mime_type.split(';').next().unwrap_or_default().trim();
        let (top, sub) = mime.split_once('/').ok_or_else(invalid_mime)?;
        if !mime_token(top) || !mime_token(sub) {
            return Err(invalid_mime());
        }
        let content_type = HeaderValue::from_str(&spec.mime_type).map_err(|_| invalid_mime())?;
        let filename = utf8_percent_encode(&spec.filename, NON_ALPHANUMERIC).to_string();
        let filename = HeaderValue::from_str(&filename)
            .map_err(|_| ClientError::invalid("invalid blob filename"))?;
        Ok(Self {
            filename,
            content_type,
        })
    }
}

fn mime_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$&^_.+-".contains(&b))
}

enum UploadChunk {
    Data(Bytes),
    Finish,
}

struct SourceStats {
    bytes: u64,
    sha256: Option<String>,
}

#[derive(Default)]
struct WireProgress {
    handed_to_transport: AtomicU64,
    eof: AtomicBool,
}

impl WireProgress {
    fn complete(&self, bytes: u64, declared_length: bool) -> bool {
        (declared_length || self.eof.load(Ordering::Acquire))
            && self.handed_to_transport.load(Ordering::Acquire) == bytes
    }
}

fn wire_body(receive: mpsc::Receiver<UploadChunk>, progress: Arc<WireProgress>) -> reqwest::Body {
    let wire = stream::unfold(Some((receive, progress)), |state| async move {
        let (mut receive, progress) = state?;
        match receive.recv().await {
            Some(UploadChunk::Data(data)) => {
                progress
                    .handed_to_transport
                    .fetch_add(data.len() as u64, Ordering::AcqRel);
                Some((Ok(data), Some((receive, progress))))
            }
            Some(UploadChunk::Finish) => {
                progress.eof.store(true, Ordering::Release);
                None
            }
            None => Some((
                Err(io::Error::other("upload source did not reach EOF")),
                None,
            )),
        }
    });
    reqwest::Body::wrap_stream(wire)
}

async fn produce(
    reader: &mut (impl AsyncRead + Unpin),
    send: mpsc::Sender<UploadChunk>,
    spec: &BlobUploadSpec,
    max_bytes: u64,
    progress_timeout: Duration,
) -> Result<Option<SourceStats>, ClientError> {
    let mut digest = (spec.verification == UploadVerification::LocalSha256).then(Sha256::new);
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut bytes = 0;
    let mut last = None;
    loop {
        let count = timeout(progress_timeout, "blob source progress", async {
            tokio::select! {
                biased;
                read = reader.read(&mut buffer) => Ok(Some(read?)),
                () = send.closed() => Ok(None),
            }
        })
        .await?;
        let Some(count) = count else { return Ok(None) };
        if count == 0 {
            break;
        }
        bytes = checked_blob_size(bytes, count as u64, max_bytes)?;
        if let Some(expected) = spec.content_length
            && bytes > expected
        {
            return Err(ClientError::BlobLengthMismatch {
                expected,
                actual: bytes,
            });
        }
        if let Some(digest) = &mut digest {
            digest.update(&buffer[..count]);
        }
        let chunk = UploadChunk::Data(Bytes::copy_from_slice(&buffer[..count]));
        // The transport may stop polling when Content-Length is reached.
        // Keep that last chunk until the source independently reaches EOF.
        if spec.content_length == Some(bytes) {
            last = Some(chunk);
        } else if !send_chunk(&send, chunk, progress_timeout).await? {
            return Ok(None);
        }
    }
    if let Some(expected) = spec.content_length {
        check_length(expected, bytes)?;
    }
    if let Some(last) = last
        && !send_chunk(&send, last, progress_timeout).await?
    {
        return Ok(None);
    }
    if !send_chunk(&send, UploadChunk::Finish, progress_timeout).await?
        && spec.content_length.is_none()
    {
        return Ok(None);
    }
    Ok(Some(SourceStats {
        bytes,
        sha256: digest.map(|state| format!("{:x}", state.finalize())),
    }))
}

async fn send_chunk(
    send: &mpsc::Sender<UploadChunk>,
    chunk: UploadChunk,
    duration: Duration,
) -> Result<bool, ClientError> {
    timeout(duration, "blob upload progress", async {
        Ok(send.send(chunk).await.is_ok())
    })
    .await
}

fn check_length(expected: u64, actual: u64) -> Result<(), ClientError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ClientError::BlobLengthMismatch { expected, actual })
    }
}
