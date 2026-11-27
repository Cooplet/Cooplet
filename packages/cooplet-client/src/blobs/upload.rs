use std::{
    io,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use cooplet_protocol::{
    AttachmentRef, ParticipantId, validate_attachment_name, validate_media_type, validate_sha256,
};
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

use super::{check_digest, checked_blob_size};
use crate::{
    ClientError, CoopletClient, X_COOPLET_FILENAME, X_COOPLET_PARTICIPANT, X_COOPLET_SHA256,
    client::timeout, error::read_bounded,
};

const CHUNK_BYTES: usize = 64 * 1024;
const MIN_READ_CAPACITY: usize = 8 * 1024;

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
    /// Attribute a machine's Hub upload to its locally authenticated participant.
    pub participant_id: Option<ParticipantId>,
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
        reader: R,
    ) -> Result<BlobUploadOutcome, ClientError> {
        self.upload_blob_with_status(spec, reader)
            .await
            .map(|(_, outcome)| outcome)
    }

    pub async fn upload_blob_with_status<R: AsyncRead + Unpin>(
        &self,
        spec: BlobUploadSpec,
        reader: R,
    ) -> Result<(StatusCode, BlobUploadOutcome), ClientError> {
        if self.expected_hub().is_none()
            && (spec.verification == UploadVerification::HubReceipt
                || spec.participant_id.is_some())
        {
            return Err(ClientError::invalid(
                "Hub upload options require a pinned Hub",
            ));
        }
        self.upload_on_route("/v1/blobs", spec, reader).await
    }

    /// Stream an upload through the loopback Node, preserving Hub receipt verification.
    pub async fn upload_bridge_blob<R: AsyncRead + Unpin>(
        &self,
        spec: BlobUploadSpec,
        reader: R,
    ) -> Result<BlobUploadOutcome, ClientError> {
        if self.expected_hub().is_some()
            || spec.verification != UploadVerification::HubReceipt
            || spec.participant_id.is_some()
        {
            return Err(ClientError::invalid("invalid Node relay upload options"));
        }
        self.upload_on_route("/v1/bridge/blobs", spec, reader)
            .await
            .map(|(_, outcome)| outcome)
    }

    async fn upload_on_route<R: AsyncRead + Unpin>(
        &self,
        path: &str,
        spec: BlobUploadSpec,
        mut reader: R,
    ) -> Result<(StatusCode, BlobUploadOutcome), ClientError> {
        let url = self.url(path, &[])?;
        let headers = UploadHeaders::validate(&spec, self.config.max_blob_bytes)?;
        let (request, pin_epoch) = self
            .protected(Method::POST, url, spec.idempotency_key.as_deref())
            .await?;
        let mut request = request
            .header(X_COOPLET_FILENAME, headers.filename)
            .header(CONTENT_TYPE, headers.content_type);
        if let Some(length) = spec.content_length {
            request = request.header(CONTENT_LENGTH, length);
        }
        if let Some(sha256) = spec.expected_sha256.as_deref() {
            request = request.header(X_COOPLET_SHA256, sha256);
        }
        if let Some(participant_id) = spec.participant_id.as_ref() {
            request = request.header(X_COOPLET_PARTICIPANT, participant_id.as_str());
        }

        let (response, stats) = if spec.content_length == Some(0) {
            // The request is complete as soon as its headers reach the peer.
            // There is no last body chunk to hold back while seeking EOF.
            let mut byte = [0];
            let count = timeout(self.config.request_timeout, "blob source progress", async {
                Ok(reader.read(&mut byte).await?)
            })
            .await?;
            check_length(0, count as u64)?;
            let sha256 = (spec.verification == UploadVerification::LocalSha256)
                .then(|| format!("{:x}", Sha256::digest([])));
            if let (Some(expected), Some(actual)) =
                (spec.expected_sha256.as_deref(), sha256.as_deref())
            {
                check_digest(expected, actual)?;
            }
            let response = timeout(self.config.request_timeout, "blob upload response", async {
                Ok(request.send().await?)
            })
            .await?;
            (response, Some(SourceStats { bytes: 0, sha256 }))
        } else {
            self.transmit_upload(request, &spec, &mut reader).await?
        };

        let status = response.status();
        if !matches!(status, StatusCode::OK | StatusCode::CREATED) {
            return Err(self.failure(response, false, pin_epoch).await);
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
        if let Some(stats) = stats {
            check_length(stats.bytes, receipt.size_bytes)?;
            let outcome = match stats.sha256 {
                Some(sha256) => {
                    check_digest(&sha256, &receipt.sha256)?;
                    BlobUploadOutcome::VerifiedLocal(receipt)
                }
                None => BlobUploadOutcome::CompleteWithHubDigest(receipt),
            };
            return Ok((status, outcome));
        }
        if status == StatusCode::OK
            && spec.idempotency_key.is_some()
            && spec.content_length.is_some()
            && spec.expected_sha256.is_some()
        {
            return Ok((
                status,
                BlobUploadOutcome::AcknowledgedWithoutLocalVerification(receipt),
            ));
        }
        Err(ClientError::protocol(
            "blob receipt arrived before confirmed source EOF and upload",
        ))
    }

    async fn transmit_upload(
        &self,
        request: reqwest::RequestBuilder,
        spec: &BlobUploadSpec,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<(reqwest::Response, Option<SourceStats>), ClientError> {
        let (send, receive) = mpsc::channel(4);
        let progress = Arc::new(WireProgress::default());
        let request = request.body(wire_body(receive, progress.clone()));
        let (response, stats) = {
            let mut response = pin!(request.send());
            let mut source = pin!(produce(
                reader,
                send,
                spec,
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

        let stats =
            stats.filter(|stats| progress.complete(stats.bytes, spec.content_length.is_some()));
        Ok((response, stats))
    }
}

struct UploadHeaders {
    filename: HeaderValue,
    content_type: HeaderValue,
}

impl UploadHeaders {
    fn validate(spec: &BlobUploadSpec, max_bytes: u64) -> Result<Self, ClientError> {
        validate_attachment_name(&spec.filename)
            .map_err(|_| ClientError::invalid("invalid blob filename"))?;
        if let Some(expected) = spec.expected_sha256.as_deref() {
            validate_sha256(expected)
                .map_err(|_| ClientError::invalid("invalid expected SHA-256"))?;
        }
        if let Some(length) = spec.content_length {
            checked_blob_size(0, length, max_bytes)?;
        }
        let invalid_mime = || ClientError::invalid("invalid blob content type");
        validate_media_type(&spec.mime_type).map_err(|_| invalid_mime())?;
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
    let mut buffer = BytesMut::with_capacity(CHUNK_BYTES);
    let mut bytes = 0;
    let mut last = None;
    loop {
        let min_cap = if last.is_some() { 1 } else { MIN_READ_CAPACITY };
        if buffer.capacity() < min_cap {
            buffer.reserve(if last.is_some() { 1 } else { CHUNK_BYTES });
        }
        let count = timeout(progress_timeout, "blob source progress", async {
            tokio::select! {
                biased;
                read = reader.read_buf(&mut buffer) => Ok(Some(read?)),
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
        let data = buffer.split().freeze();
        if let Some(digest) = &mut digest {
            digest.update(&data);
        }
        let chunk = UploadChunk::Data(data);
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
    let sha256 = digest.map(|state| format!("{:x}", state.finalize()));
    // Verify a locally proven SHA-256 against expected_sha256 BEFORE releasing
    // the held final chunk so an early 200 OK can never mask a digest mismatch.
    if let (Some(expected), Some(actual)) = (spec.expected_sha256.as_deref(), sha256.as_deref()) {
        check_digest(expected, actual)?;
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
    Ok(Some(SourceStats { bytes, sha256 }))
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
