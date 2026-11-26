mod upload;

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use cooplet_protocol::{BlobId, validate_sha256};
use http::{
    Method, StatusCode,
    header::{CONTENT_TYPE, ETAG},
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::{
    ClientError, CoopletClient,
    client::{ResponseChunks, timeout},
};

pub use upload::{BlobUploadOutcome, BlobUploadSpec, UploadVerification};

const CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedBlob {
    pub blob_id: BlobId,
    pub sha256: String,
    pub size_bytes: u64,
    pub content_type: Option<String>,
}

/// A bounded HTTP reader carrying an external digest. The consumer verifies
/// the content before publishing it. An error permanently fails the reader.
pub struct BlobDownload {
    pub expected_sha256: String,
    pub content_length: Option<u64>,
    pub content_type: Option<String>,
    chunks: Option<ResponseChunks>,
    pending: Bytes,
    seen: u64,
    max_bytes: u64,
    failed: bool,
}

impl CoopletClient {
    pub async fn open_blob_download(
        &self,
        blob_id: &BlobId,
        expected_sha256: Option<&str>,
        max_bytes: u64,
    ) -> Result<BlobDownload, ClientError> {
        if let Some(expected) = expected_sha256 {
            validate_sha256(expected)
                .map_err(|_| ClientError::invalid("invalid expected SHA-256"))?;
        }
        let path = crate::api_path(&["blobs", blob_id.as_str()])?;
        let url = self.url(&path, &[])?;
        let request = self.protected(Method::GET, url, None).await?;
        let response = timeout(
            self.config.request_timeout,
            "blob response headers",
            async { Ok(request.send().await?) },
        )
        .await?;
        if response.status() != StatusCode::OK {
            return Err(self.failure(response, false).await);
        }

        let mut tags = response.headers().get_all(ETAG).iter();
        let tag = tags
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(strong_sha256);
        if tags.next().is_some() {
            return Err(ClientError::protocol("ambiguous blob ETag"));
        }
        if let (Some(expected), Some(tag)) = (expected_sha256, tag) {
            check_digest(expected, tag)?;
        }
        let expected_sha256 = expected_sha256
            .or(tag)
            .ok_or_else(|| ClientError::protocol("blob download requires an external SHA-256"))?
            .to_owned();
        let content_length = response.content_length();
        if let Some(content_length) = content_length {
            checked_blob_size(0, content_length, max_bytes)?;
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .map(|content_type| {
                content_type
                    .to_str()
                    .map(str::to_owned)
                    .map_err(|_| ClientError::protocol("invalid blob content type"))
            })
            .transpose()?;
        Ok(BlobDownload {
            expected_sha256,
            content_length,
            content_type,
            chunks: Some(Box::pin(response.bytes_stream())),
            pending: Bytes::new(),
            seen: 0,
            max_bytes,
            failed: false,
        })
    }

    /// Stream to a provisional sink and verify the digest before reporting
    /// success. A caller publishing a file must discard its sink on error.
    pub async fn download_blob_to_writer<W: AsyncWrite + Unpin>(
        &self,
        blob_id: &BlobId,
        expected_sha256: Option<&str>,
        max_bytes: u64,
        writer: &mut W,
    ) -> Result<DownloadedBlob, ClientError> {
        let mut download = self
            .open_blob_download(blob_id, expected_sha256, max_bytes)
            .await?;
        let mut digest = Sha256::new();
        let mut buffer = vec![0; CHUNK_BYTES];
        loop {
            let read = download.read(&mut buffer).await.map_err(download_error)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
            writer.write_all(&buffer[..read]).await?;
        }
        writer.flush().await?;
        let digest = format!("{:x}", digest.finalize());
        check_digest(&download.expected_sha256, &digest)?;
        Ok(DownloadedBlob {
            blob_id: blob_id.clone(),
            sha256: digest,
            size_bytes: download.seen,
            content_type: download.content_type,
        })
    }
}

fn strong_sha256(tag: &str) -> Option<&str> {
    let digest = tag.strip_prefix('"')?.strip_suffix('"')?;
    validate_sha256(digest).ok()?;
    Some(digest)
}

pub(super) fn checked_blob_size(
    current: u64,
    chunk: u64,
    max_bytes: u64,
) -> Result<u64, ClientError> {
    current
        .checked_add(chunk)
        .filter(|&size| size <= max_bytes)
        .ok_or(ClientError::BlobTooLarge { max_bytes })
}

pub(super) fn check_digest(expected: &str, actual: &str) -> Result<(), ClientError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ClientError::BlobDigestMismatch {
            expected: expected.into(),
            actual: actual.into(),
        })
    }
}

fn download_error(error: io::Error) -> ClientError {
    if error
        .get_ref()
        .is_some_and(|source| source.is::<ClientError>())
    {
        match error
            .into_inner()
            .and_then(|source| source.downcast::<ClientError>().ok())
        {
            Some(error) => *error,
            None => ClientError::protocol("blob read failed"),
        }
    } else {
        error.into()
    }
}

impl BlobDownload {
    fn fail(&mut self, error: ClientError) -> Poll<io::Result<()>> {
        self.failed = true;
        self.chunks = None;
        Poll::Ready(Err(io::Error::other(error)))
    }
}

impl AsyncRead for BlobDownload {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(io::Error::other("blob reader failed earlier")));
        }
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if !this.pending.is_empty() {
                let count = buffer.remaining().min(this.pending.len());
                buffer.put_slice(&this.pending[..count]);
                this.pending.advance(count);
                return Poll::Ready(Ok(()));
            }
            let Some(chunks) = &mut this.chunks else {
                return Poll::Ready(Ok(()));
            };
            match chunks.as_mut().poll_next(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.chunks = None;
                    if let Some(expected) = this.content_length
                        && expected != this.seen
                    {
                        return this.fail(ClientError::BlobLengthMismatch {
                            expected,
                            actual: this.seen,
                        });
                    }
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(error))) => return this.fail(error.into()),
                Poll::Ready(Some(Ok(data))) => {
                    let length =
                        match checked_blob_size(this.seen, data.len() as u64, this.max_bytes) {
                            Ok(length) => length,
                            Err(error) => return this.fail(error),
                        };
                    if let Some(expected) = this.content_length
                        && length > expected
                    {
                        return this.fail(ClientError::BlobLengthMismatch {
                            expected,
                            actual: length,
                        });
                    }
                    this.seen = length;
                    this.pending = data;
                }
            }
        }
    }
}
