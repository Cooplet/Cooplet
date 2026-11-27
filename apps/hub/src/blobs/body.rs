use std::{
    fs::File,
    future::Future,
    io::{self, Read},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use axum::body::{Body, BodyDataStream};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::stream;
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::watch,
    time::{Instant, Sleep, sleep},
};

use crate::error::HubError;

pub(super) const CHUNK_BYTES: usize = 64 * 1024;
const READ_IDLE: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub(super) enum UploadReadError {
    #[error("The upload body could not be read.")]
    Body,
    #[error("The upload body made no progress before the read timeout.")]
    Idle,
    #[error("The Hub is shutting down.")]
    Shutdown,
}

pub(super) fn upload_read_error(error: io::Error) -> HubError {
    use cooplet_protocol::ErrorCode;

    match error
        .get_ref()
        .and_then(|error| error.downcast_ref::<UploadReadError>())
    {
        Some(UploadReadError::Shutdown) => HubError::new(ErrorCode::Overloaded, error.to_string()),
        Some(_) => HubError::invalid(error.to_string()),
        None => HubError::internal(error),
    }
}

pub(super) struct UploadReader {
    data: BodyDataStream,
    pending: Bytes,
    idle: Pin<Box<Sleep>>,
    stopped: Pin<Box<dyn Future<Output = ()> + Send>>,
    closed: bool,
}

impl UploadReader {
    pub(super) fn new(body: Body, mut shutdown: watch::Receiver<()>) -> Self {
        Self {
            data: body.into_data_stream(),
            pending: Bytes::new(),
            idle: Box::pin(sleep(READ_IDLE)),
            stopped: Box::pin(async move {
                let _ = shutdown.changed().await;
            }),
            closed: false,
        }
    }

    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.idle.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(io::Error::other(UploadReadError::Idle)))
        } else {
            Poll::Pending
        }
    }
}

impl AsyncRead for UploadReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.closed || this.stopped.as_mut().poll(cx).is_ready() {
            this.closed = true;
            return Poll::Ready(Err(io::Error::other(UploadReadError::Shutdown)));
        }
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // An empty data frame neither ends the request nor resets its timeout.
        for _ in 0..16 {
            if !this.pending.is_empty() {
                let count = buffer.remaining().min(this.pending.len());
                buffer.put_slice(&this.pending.split_to(count));
                this.idle.as_mut().reset(Instant::now() + READ_IDLE);
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.data).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => this.pending = chunk,
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Err(io::Error::other(UploadReadError::Body)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return this.poll_idle(cx),
            }
        }
        cx.waker().wake_by_ref();
        this.poll_idle(cx)
    }
}

pub(super) fn file_chunks(
    file: File,
    shutdown: watch::Receiver<()>,
) -> impl Stream<Item = io::Result<Bytes>> + Send {
    stream::try_unfold((file, shutdown), |(mut file, mut shutdown)| async move {
        if shutdown.has_changed().unwrap_or(true) {
            return Ok(None);
        }
        let read = tokio::task::spawn_blocking(move || -> io::Result<_> {
            let mut chunk = vec![0; CHUNK_BYTES];
            let count = loop {
                match file.read(&mut chunk) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => break result?,
                }
            };
            chunk.truncate(count);
            Ok((file, Bytes::from(chunk)))
        });
        tokio::select! {
            biased;
            _ = shutdown.changed() => Ok(None),
            result = read => {
                let (file, chunk) = result.map_err(io::Error::other)??;
                Ok((!chunk.is_empty()).then_some((chunk, (file, shutdown))))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn interrupted_upload_is_not_a_successful_eof() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_send, received) = watch::channel(());
        let body = Body::from_stream(stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(io::Error::other("peer disconnected")),
        ]));
        let mut reader = UploadReader::new(body, received);
        let mut bytes = Vec::new();
        let result = reader.read_to_end(&mut bytes).await;
        assert_eq!(bytes, b"partial");
        assert!(result.is_err_and(|error| {
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<UploadReadError>())
                .is_some_and(|error| matches!(error, UploadReadError::Body))
        }));
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_wakes_a_reader_waiting_for_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (send, received) = watch::channel(());
        let body = Body::from_stream(stream::pending::<io::Result<Bytes>>());
        let mut reader = UploadReader::new(body, received);
        let send = tokio::spawn(async move {
            tokio::task::yield_now().await;
            send.send(())
        });
        let mut buffer = [0u8; 1];
        let result = reader.read(&mut buffer).await;
        assert!(result.is_err_and(|error| {
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<UploadReadError>())
                .is_some_and(|error| matches!(error, UploadReadError::Shutdown))
        }));
        // Polling again after shutdown must stay fused instead of re-polling a finished future.
        let second = reader.read(&mut buffer).await;
        assert!(second.is_err_and(|error| {
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<UploadReadError>())
                .is_some_and(|error| matches!(error, UploadReadError::Shutdown))
        }));
        send.await??;
        Ok(())
    }

    #[tokio::test]
    async fn empty_frames_do_not_extend_the_read_deadline() {
        let (_send, received) = watch::channel(());
        let body = Body::from_stream(stream::repeat_with(|| Ok::<_, io::Error>(Bytes::new())));
        let mut reader = UploadReader::new(body, received);
        reader.idle.as_mut().reset(Instant::now());
        let mut buffer = [0u8; 1];
        let result = reader.read(&mut buffer).await;
        assert!(result.is_err_and(|error| {
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<UploadReadError>())
                .is_some_and(|error| matches!(error, UploadReadError::Idle))
        }));
    }
}
