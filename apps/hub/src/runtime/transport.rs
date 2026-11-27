use std::{
    future::Future,
    io::{self, IoSlice},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use axum::serve::Listener;
use socket2::SockRef;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{self, Sleep},
};

pub(super) struct WriteIdleListener {
    inner: TcpListener,
    timeout: Duration,
    force_close: watch::Receiver<()>,
}

impl WriteIdleListener {
    pub(super) fn new(
        inner: TcpListener,
        timeout: Duration,
        force_close: watch::Receiver<()>,
    ) -> Self {
        Self {
            inner,
            timeout,
            force_close,
        }
    }
}

impl Listener for WriteIdleListener {
    type Io = WriteIdle<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, address) = Listener::accept(&mut self.inner).await;
        if let Err(error) = SockRef::from(&stream).set_keepalive(true) {
            tracing::warn!(%error, "Could not enable TCP keepalive");
        }
        let mut shutdown = self.force_close.clone();
        let forced = Box::pin(async move {
            let _ = shutdown.changed().await;
        });
        (WriteIdle::new(stream, self.timeout, forced), address)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

pub(super) struct WriteIdle<I> {
    inner: I,
    timeout: Duration,
    deadline: Option<Pin<Box<Sleep>>>,
    forced: Pin<Box<dyn Future<Output = ()> + Send>>,
    closed: bool,
}

impl<I> WriteIdle<I> {
    fn new(inner: I, timeout: Duration, forced: Pin<Box<dyn Future<Output = ()> + Send>>) -> Self {
        Self {
            inner,
            timeout,
            deadline: None,
            forced,
            closed: false,
        }
    }

    fn ensure_open(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.closed || self.forced.as_mut().poll(cx).is_ready() {
            self.closed = true;
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Hub HTTP listener is closing",
            ))
        } else {
            Ok(())
        }
    }

    fn check_deadline(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.deadline.as_mut().is_some_and(|deadline| {
            time::Instant::now() >= deadline.deadline() || deadline.as_mut().poll(cx).is_ready()
        }) {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP write made no progress",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_writable(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        self.ensure_open(cx)?;
        self.check_deadline(cx)
    }

    fn arm_if_stalled<T>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if result.is_pending() && self.deadline.is_none() {
            self.deadline = Some(Box::pin(time::sleep(self.timeout)));
            self.check_deadline(cx)?;
        }
        result
    }

    fn written(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        if matches!(result, Poll::Ready(Ok(bytes)) if bytes > 0) {
            self.deadline = None;
        }
        self.arm_if_stalled(cx, result)
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for WriteIdle<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.ensure_open(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for WriteIdle<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.ensure_writable(cx)?;
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.written(cx, result)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.ensure_writable(cx)?;
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.written(cx, result)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.ensure_writable(cx)?;
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        self.arm_if_stalled(cx, result)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.ensure_writable(cx)?;
        let result = Pin::new(&mut self.inner).poll_shutdown(cx);
        self.arm_if_stalled(cx, result)
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone, Copy)]
    enum Behavior {
        Stall,
        Zero,
        Write,
    }

    struct Writer {
        behavior: Behavior,
        bytes_written: usize,
    }

    impl Writer {
        fn new(behavior: Behavior) -> Self {
            Self {
                behavior,
                bytes_written: 0,
            }
        }
    }

    impl AsyncWrite for Writer {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.behavior {
                Behavior::Stall => Poll::Pending,
                Behavior::Zero => Poll::Ready(Ok(0)),
                Behavior::Write => {
                    self.bytes_written += buf.len();
                    Poll::Ready(Ok(buf.len()))
                }
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn stalled_write_wakes_on_deadline() {
        let mut stream = WriteIdle::new(
            Writer::new(Behavior::Stall),
            Duration::from_millis(5),
            Box::pin(future::pending()),
        );
        let result = time::timeout(Duration::from_secs(1), stream.write(b"x")).await;
        assert!(matches!(result, Ok(Err(error)) if error.kind() == io::ErrorKind::TimedOut));
    }

    #[tokio::test]
    async fn noop_flush_and_zero_writes_keep_deadline() -> io::Result<()> {
        let mut stream = WriteIdle::new(
            Writer::new(Behavior::Zero),
            Duration::from_secs(30),
            Box::pin(future::pending()),
        );
        let cx = &mut Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            Pin::new(&mut stream).poll_write(cx, b"x"),
            Poll::Ready(Ok(0))
        ));
        assert!(stream.deadline.is_none());

        stream.inner.behavior = Behavior::Stall;
        assert!(Pin::new(&mut stream).poll_write(cx, b"x").is_pending());
        stream.inner.behavior = Behavior::Zero;
        let deadline = stream.deadline.as_deref().map(Sleep::deadline);
        assert!(deadline.is_some());
        assert!(matches!(
            Pin::new(&mut stream).poll_write_vectored(cx, &[IoSlice::new(b"x")]),
            Poll::Ready(Ok(0))
        ));
        stream.flush().await?;
        assert_eq!(stream.deadline.as_deref().map(Sleep::deadline), deadline);

        stream.deadline = Some(Box::pin(time::sleep_until(time::Instant::now())));
        time::sleep(Duration::from_millis(2)).await;
        for result in [
            Pin::new(&mut stream).poll_write(cx, b"x"),
            Pin::new(&mut stream).poll_write_vectored(cx, &[IoSlice::new(b"x")]),
            Pin::new(&mut stream).poll_flush(cx).map_ok(|()| 0),
        ] {
            assert!(
                matches!(result, Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::TimedOut)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn positive_write_clears_the_deadline() -> io::Result<()> {
        let mut stream = WriteIdle::new(
            Writer::new(Behavior::Stall),
            Duration::from_secs(30),
            Box::pin(future::pending()),
        );
        let cx = &mut Context::from_waker(std::task::Waker::noop());
        assert!(Pin::new(&mut stream).poll_write(cx, b"x").is_pending());
        stream.inner.behavior = Behavior::Write;
        assert_eq!(stream.write_vectored(&[IoSlice::new(b"x")]).await?, 1);
        assert_eq!(stream.inner.bytes_written, 1);
        assert!(stream.deadline.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn expired_writer_cannot_resume_when_socket_becomes_writable() {
        let mut stream = WriteIdle::new(
            Writer::new(Behavior::Stall),
            Duration::from_millis(5),
            Box::pin(future::pending()),
        );
        let cx = &mut Context::from_waker(std::task::Waker::noop());
        assert!(Pin::new(&mut stream).poll_write(cx, b"x").is_pending());
        stream.inner.behavior = Behavior::Write;
        time::sleep(Duration::from_millis(10)).await;
        for result in [
            Pin::new(&mut stream).poll_write(cx, b"x"),
            Pin::new(&mut stream).poll_write_vectored(cx, &[IoSlice::new(b"x")]),
        ] {
            assert!(
                matches!(result, Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::TimedOut)
            );
        }
        assert_eq!(stream.inner.bytes_written, 0);
    }

    #[tokio::test]
    async fn listener_enables_keepalive_and_can_interrupt_idle_sockets() -> io::Result<()> {
        let (stop, stop_rx) = watch::channel(());
        let mut listener = WriteIdleListener::new(
            TcpListener::bind("127.0.0.1:0").await?,
            Duration::from_secs(30),
            stop_rx,
        );
        let _peer = TcpStream::connect(listener.local_addr()?).await?;
        let (mut accepted, _) = listener.accept().await;
        assert!(SockRef::from(&accepted.inner).keepalive()?);
        let (waiting, observed) = tokio::sync::oneshot::channel();
        let read = tokio::spawn(async move {
            let mut read = std::pin::pin!(accepted.read_u8());
            let mut waiting = Some(waiting);
            future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if let Some(waiting) = waiting.take() {
                    assert!(result.is_pending());
                    let _ = waiting.send(());
                }
                result
            })
            .await
        });
        observed.await.map_err(io::Error::other)?;
        stop.send_replace(());
        let error = time::timeout(Duration::from_secs(1), read)
            .await?
            .map_err(io::Error::other)?
            .err();
        assert!(error.is_some_and(|error| error.kind() == io::ErrorKind::ConnectionAborted));
        Ok(())
    }
}
