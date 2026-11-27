use std::{
    io::{self, IoSlice},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::serve::Listener;
use futures_util::future::BoxFuture;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    time::{Instant, Sleep},
};

const CONNECTION_GRACE: Duration = Duration::from_secs(2);
const INITIAL_HEADER_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(85);
const PROGRESS_READ_THRESHOLD: usize = 4096;
const MAX_CONCURRENT_CONNECTIONS: usize = 256;

pub(crate) struct NodeListener {
    listener: TcpListener,
    shutdown: watch::Receiver<bool>,
    permits: Arc<Semaphore>,
}

impl NodeListener {
    pub(crate) fn new(listener: TcpListener, shutdown: watch::Receiver<bool>) -> Self {
        Self {
            listener,
            shutdown,
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
        }
    }
}

impl Listener for NodeListener {
    type Io = NodeConnection;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let permit = self.permits.clone().acquire_owned().await.ok();
        let (socket, address) = Listener::accept(&mut self.listener).await;
        let mut shutdown = self.shutdown.clone();
        let deadline = Box::pin(async move {
            let _ = shutdown.wait_for(|stopping| *stopping).await;
            tokio::time::sleep(CONNECTION_GRACE).await;
        });
        (NodeConnection::new(socket, deadline, permit), address)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

enum WritePhase {
    Prefix { buf: [u8; 10], len: u8 },
    InterimHeaders { crlf: u8 },
    FinalResponse,
}

impl WritePhase {
    const fn prefix() -> Self {
        Self::Prefix {
            buf: [0; 10],
            len: 0,
        }
    }
}

pub(crate) struct NodeConnection<S = TcpStream> {
    socket: S,
    deadline: Option<BoxFuture<'static, ()>>,
    timeout: Pin<Box<Sleep>>,
    header_deadline: Option<Instant>,
    crlf_state: u8,
    activity_deadline: Instant,
    awaiting_request: bool,
    write_phase: WritePhase,
    read_progress: usize,
    _permit: Option<OwnedSemaphorePermit>,
}

impl<S> NodeConnection<S> {
    fn new(
        socket: S,
        deadline: BoxFuture<'static, ()>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        let header_deadline = Instant::now() + INITIAL_HEADER_TIMEOUT;
        Self {
            socket,
            deadline: Some(deadline),
            timeout: Box::pin(tokio::time::sleep_until(header_deadline)),
            header_deadline: Some(header_deadline),
            crlf_state: 0,
            activity_deadline: header_deadline,
            awaiting_request: false,
            write_phase: WritePhase::prefix(),
            read_progress: 0,
            _permit: permit,
        }
    }

    // Enforce both the shutdown grace period and per-connection header/idle
    // timeouts. Each request's header deadline is non-extendable until
    // `\r\n\r\n` after a non-CRLF request-line byte (or an early error
    // response) so unauthenticated slowloris peers cannot prolong header reads
    // on initial or keep-alive requests; afterwards idle deadlines update
    // lazily without churning the Tokio timer wheel on every streamed chunk.
    fn check_deadlines(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if !self
            .deadline
            .as_mut()
            .is_some_and(|deadline| deadline.as_mut().poll(cx).is_pending())
        {
            self.deadline = None;
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Node connection shutdown deadline reached",
            ));
        }
        if self.timeout.as_mut().poll(cx).is_ready() {
            let effective = self.header_deadline.unwrap_or(self.activity_deadline);
            if effective > Instant::now() {
                self.timeout.as_mut().reset(effective);
                if self.timeout.as_mut().poll(cx).is_pending() {
                    return Ok(());
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Node connection idle or header timeout reached",
            ));
        }
        Ok(())
    }

    fn note_activity(&mut self) {
        self.activity_deadline = Instant::now() + IDLE_CONNECTION_TIMEOUT;
        self.read_progress = 0;
    }

    fn arm_header_deadline(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        let deadline = Instant::now() + INITIAL_HEADER_TIMEOUT;
        self.header_deadline = Some(deadline);
        self.activity_deadline = deadline;
        self.read_progress = 0;
        self.timeout.as_mut().reset(deadline);
        if self.timeout.as_mut().poll(cx).is_ready() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Node connection idle or header timeout reached",
            ));
        }
        Ok(())
    }

    fn note_written(&mut self, slices: &[IoSlice<'_>], mut written: usize) {
        self.header_deadline = None;
        self.note_activity();
        for slice in slices {
            if written == 0 {
                break;
            }
            let take = slice.len().min(written);
            written -= take;
            self.advance_write_phase(&slice[..take]);
        }
    }

    fn advance_write_phase(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            match &mut self.write_phase {
                WritePhase::FinalResponse => return,
                WritePhase::Prefix { buf, len } => {
                    let start = usize::from(*len);
                    let copy = bytes.len().min(buf.len() - start);
                    buf[start..start + copy].copy_from_slice(&bytes[..copy]);
                    *len += copy as u8;
                    bytes = &bytes[copy..];
                    if usize::from(*len) == buf.len() {
                        if buf.starts_with(b"HTTP/1.1 1") || buf.starts_with(b"HTTP/1.0 1") {
                            // The 10-byte status-line prefix is already non-CRLF (state 1).
                            self.write_phase = WritePhase::InterimHeaders { crlf: 1 };
                        } else {
                            self.write_phase = WritePhase::FinalResponse;
                            self.awaiting_request = true;
                            return;
                        }
                    }
                }
                WritePhase::InterimHeaders { crlf } => {
                    if let Some(consumed) = advance_crlf(crlf, bytes) {
                        bytes = &bytes[consumed..];
                        self.write_phase = WritePhase::prefix();
                    } else {
                        return;
                    }
                }
            }
        }
    }
}

fn advance_crlf(state: &mut u8, bytes: &[u8]) -> Option<usize> {
    for (index, &byte) in bytes.iter().enumerate() {
        *state = match (*state, byte) {
            // RFC 9112 §2.2 skips leading empty lines before the request-line;
            // stay in state 0 until at least one non-CRLF byte is observed.
            (0, b'\r' | b'\n') => 0,
            (0, _) => 1,
            (1, b'\r') => 2,
            (1, _) => 1,
            (2, b'\n') => 3,
            (2, b'\r') => 2,
            (2, _) => 1,
            (3, b'\r') => 4,
            (3, _) => 1,
            (4, b'\n') => {
                *state = 5;
                return Some(index + 1);
            }
            (4, b'\r') => 2,
            (4, _) => 1,
            _ => 5,
        };
    }
    None
}

impl<S: AsyncRead + Unpin> AsyncRead for NodeConnection<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check_deadlines(cx)?;
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buffer);
        if let Poll::Ready(Ok(())) = &result {
            let filled = buffer.filled();
            let read = filled.len().saturating_sub(before);
            if read > 0 {
                if self.header_deadline.is_some() {
                    if let Some(consumed) = advance_crlf(&mut self.crlf_state, &filled[before..]) {
                        self.header_deadline = None;
                        self.note_activity();
                        let trailing = read.saturating_sub(consumed);
                        self.read_progress = trailing % PROGRESS_READ_THRESHOLD;
                    }
                } else if self.awaiting_request {
                    self.awaiting_request = false;
                    self.write_phase = WritePhase::prefix();
                    self.crlf_state = 0;
                    if let Some(consumed) = advance_crlf(&mut self.crlf_state, &filled[before..]) {
                        self.note_activity();
                        let trailing = read.saturating_sub(consumed);
                        self.read_progress = trailing % PROGRESS_READ_THRESHOLD;
                    } else if let Err(error) = self.arm_header_deadline(cx) {
                        return Poll::Ready(Err(error));
                    }
                } else {
                    self.read_progress = self.read_progress.saturating_add(read);
                    if self.read_progress >= PROGRESS_READ_THRESHOLD {
                        self.note_activity();
                    }
                }
            }
        }
        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for NodeConnection<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check_deadlines(cx)?;
        let result = Pin::new(&mut self.socket).poll_write(cx, buffer);
        if let Poll::Ready(Ok(written)) = &result
            && *written > 0
        {
            self.note_written(&[IoSlice::new(buffer)], *written);
        }
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.check_deadlines(cx)?;
        let result = Pin::new(&mut self.socket).poll_write_vectored(cx, buffers);
        if let Poll::Ready(Ok(written)) = &result
            && *written > 0
        {
            self.note_written(buffers, *written);
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_deadlines(cx)?;
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_deadlines(cx)?;
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn test_connection() -> (
        NodeConnection<tokio::io::DuplexStream>,
        tokio::io::DuplexStream,
    ) {
        let (server, client) = tokio::io::duplex(16 * 1024);
        let conn = NodeConnection::new(server, Box::pin(std::future::pending()), None);
        (conn, client)
    }

    #[tokio::test(start_paused = true)]
    async fn slowloris_headers_cannot_extend_initial_or_keep_alive_header_timeout() -> io::Result<()>
    {
        // 1. Initial request: leading empty lines (`\r\n\r\n`) and 4 KiB header chunks
        //    before the real header terminator cannot extend the 15s deadline.
        let (mut server, mut client) = test_connection();
        let mut buf = [0u8; 4096];
        client.write_all(b"\r\n\r\n").await?;
        assert_eq!(server.read(&mut buf).await?, 4);
        client.write_all(&vec![b'X'; 4096]).await?;
        assert_eq!(server.read(&mut buf).await?, 4096);
        tokio::time::advance(Duration::from_secs(10)).await;
        client.write_all(&vec![b'Y'; 4096]).await?;
        assert_eq!(server.read(&mut buf).await?, 4096);
        tokio::time::advance(Duration::from_secs(6)).await;
        let before = Instant::now();
        client.write_all(b"x").await?;
        let err = server
            .read(&mut buf)
            .await
            .err()
            .ok_or_else(|| io::Error::other("Expected initial header timeout"))?;
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(before.elapsed(), Duration::ZERO);

        // 2. Keep-alive request #2: leading `\r\n\r\n` + incomplete headers on request #2
        //    must also expire after 15s even with 4 KiB chunks.
        let (mut server, mut client) = test_connection();
        client
            .write_all(b"GET /v1/info HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        assert!(server.read(&mut buf).await? > 0);
        server
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
            .await?;
        let mut resp = [0u8; 64];
        let _ = client.read(&mut resp).await?;

        tokio::time::advance(Duration::from_secs(55)).await;
        client
            .write_all(b"\r\n\r\nGET /v1/info HTTP/1.1\r\n")
            .await?;
        assert!(server.read(&mut buf).await? > 0);
        tokio::time::advance(Duration::from_secs(10)).await;
        client.write_all(&vec![b'Z'; 4096]).await?;
        assert_eq!(server.read(&mut buf).await?, 4096);
        tokio::time::advance(Duration::from_secs(6)).await;
        let before = Instant::now();
        client.write_all(b"x").await?;
        let err = server
            .read(&mut buf)
            .await
            .err()
            .ok_or_else(|| io::Error::other("Expected keep-alive header timeout"))?;
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(before.elapsed(), Duration::ZERO);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn keep_alive_small_request_and_small_chunk_upload_survive_idle_window() -> io::Result<()>
    {
        let (mut server, mut client) = test_connection();
        let mut buf = [0u8; 2048];
        // Complete request 1 + response 1.
        client
            .write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        let n = server.read(&mut buf).await?;
        assert!(n > 0);
        server
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await?;
        let mut resp = [0u8; 64];
        let _ = client.read(&mut resp).await?;

        // A small keep-alive request resets the deadline even when it arrives
        // just before expiry. The response then crosses the original deadline.
        tokio::time::advance(IDLE_CONNECTION_TIMEOUT - Duration::from_secs(5)).await;
        client
            .write_all(
                b"POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}",
            )
            .await?;
        assert!(server.read(&mut buf).await? > 0);
        tokio::time::advance(Duration::from_secs(25)).await;
        server
            .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n")
            .await?;
        let _ = client.read(&mut resp).await?;

        // Expect: 100-continue written in fragmented short writes must not arm header_deadline for the body.
        client
            .write_all(
                b"POST /v1/blobs HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n\r\n",
            )
            .await?;
        assert!(server.read(&mut buf).await? > 0);
        server.write_all(b"HTTP/1").await?;
        server.write_all(b".1 100 ").await?;
        server.write_all(b"Continue\r\n\r\n").await?;
        let mut drained = 0;
        while drained < b"HTTP/1.1 100 Continue\r\n\r\n".len() {
            drained += client.read(&mut resp).await?;
        }

        // An upload spanning more than one idle window remains alive while
        // its small chunks keep making progress.
        for _ in 0..(IDLE_CONNECTION_TIMEOUT.as_secs() / 10 + 2) {
            tokio::time::advance(Duration::from_secs(10)).await;
            client.write_all(&[b'a'; 1448]).await?;
            assert_eq!(server.read(&mut buf).await?, 1448);
        }
        // Genuine inactivity beyond idle_timeout closes with TimedOut.
        tokio::time::advance(IDLE_CONNECTION_TIMEOUT + Duration::from_secs(1)).await;
        let before = Instant::now();
        client.write_all(b"x").await?;
        let err = server
            .read(&mut buf)
            .await
            .err()
            .ok_or_else(|| io::Error::other("Expected idle timeout"))?;
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(before.elapsed(), Duration::ZERO);
        Ok(())
    }
}
