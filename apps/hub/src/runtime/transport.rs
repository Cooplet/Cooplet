use std::{
    future::{Future, poll_fn},
    io::{self, IoSlice},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{Router, body::Body};
use hyper::{Request, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use socket2::{SockRef, TcpKeepalive};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot, watch},
    time::{self, Sleep},
};
use tower_service::Service;

const TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
const TCP_KEEPALIVE_RETRIES: u32 = 3;

#[derive(Clone, Copy)]
pub(super) struct HttpServerConfig {
    pub(super) header_read_timeout: Duration,
    pub(super) write_idle_timeout: Duration,
    pub(super) drain_timeout: Duration,
    pub(super) force_close_timeout: Duration,
    pub(super) max_connections: usize,
}

pub(super) async fn serve_http1(
    listener: TcpListener,
    app: Router,
    config: HttpServerConfig,
    mut stop_accepting: oneshot::Receiver<()>,
) -> io::Result<()> {
    let (graceful_tx, graceful_rx) = watch::channel(());
    let (force_close, force_close_rx) = watch::channel(());
    let (active_tx, _) = watch::channel(0usize);
    let connections = Arc::new(Semaphore::new(config.max_connections.max(1)));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(config.header_read_timeout);

    loop {
        let stream = tokio::select! {
            biased;
            _ = &mut stop_accepting => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) if is_transient_accept_error(&error) => {
                    tracing::warn!(%error, "Transient TCP accept error");
                    time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => {
                    drop(listener);
                    graceful_tx.send_replace(());
                    force_close.send_replace(());
                    let _ = time::timeout(config.force_close_timeout, wait_zero(&active_tx)).await;
                    return Err(error);
                }
            }
        };

        let Ok(permit) = connections.clone().try_acquire_owned() else {
            tracing::warn!(
                max_connections = config.max_connections,
                "Rejecting TCP connection above max_connections limit"
            );
            drop(stream);
            continue;
        };
        configure_tcp(&stream);
        let mut shutdown = force_close_rx.clone();
        let forced = Box::pin(async move {
            // An upgraded connection is owned by the bridge after the HTTP
            // task exits. Only an explicit force-close invalidates its I/O.
            if shutdown.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        });
        let io = TokioIo::new(WriteIdle::new(stream, config.write_idle_timeout, forced));
        let router = app.clone();
        let service = service_fn(move |request: Request<Incoming>| {
            let mut router = router.clone();
            async move {
                poll_fn(|cx| <Router as Service<Request<Body>>>::poll_ready(&mut router, cx))
                    .await?;
                router.call(request.map(Body::new)).await
            }
        });
        let mut connection = Box::pin(builder.serve_connection(io, service).with_upgrades());
        let mut graceful = graceful_rx.clone();
        let mut force_close_task = force_close_rx.clone();
        let active = ActiveConnection::enter(&active_tx);
        tokio::spawn(async move {
            let _permit = permit;
            let _active = active;
            let result = tokio::select! {
                biased;
                result = connection.as_mut() => result,
                _ = force_close_task.changed() => Ok(()),
                _ = graceful.changed() => {
                    connection.as_mut().graceful_shutdown();
                    tokio::select! {
                        biased;
                        result = connection.as_mut() => result,
                        _ = force_close_task.changed() => Ok(()),
                    }
                }
            };
            if let Err(error) = result {
                tracing::debug!(%error, "HTTP connection ended");
            }
        });
    }

    drop(listener);
    graceful_tx.send_replace(());
    if time::timeout(config.drain_timeout, wait_zero(&active_tx))
        .await
        .is_ok()
    {
        return Ok(());
    }
    tracing::warn!("HTTP drain timed out; closing remaining connections");
    force_close.send_replace(());
    if time::timeout(config.force_close_timeout, wait_zero(&active_tx))
        .await
        .is_ok()
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "HTTP shutdown did not finish",
        ))
    }
}

struct ActiveConnection(watch::Sender<usize>);

impl ActiveConnection {
    fn enter(active: &watch::Sender<usize>) -> Self {
        active.send_modify(|count| *count += 1);
        Self(active.clone())
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.send_modify(|count| *count -= 1);
    }
}

async fn wait_zero(active: &watch::Sender<usize>) {
    let _ = active.subscribe().wait_for(|count| *count == 0).await;
}

fn configure_tcp(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let keepalive = TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_TIME)
        .with_interval(TCP_KEEPALIVE_INTERVAL);
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    let keepalive = keepalive.with_retries(TCP_KEEPALIVE_RETRIES);
    if let Err(error) = SockRef::from(stream).set_tcp_keepalive(&keepalive) {
        tracing::warn!(%error, "Could not configure TCP keepalive");
    }
}

fn is_transient_accept_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::OutOfMemory
    ) {
        return true;
    }
    // ENFILE (23) and EMFILE (24) on Linux/BSD/macOS indicate temporary descriptor exhaustion.
    #[cfg(unix)]
    if matches!(error.raw_os_error(), Some(23 | 24)) {
        return true;
    }
    false
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

    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

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
    async fn configure_tcp_enables_tuned_keepalive_and_force_close_interrupts_reads()
    -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let _peer = TcpStream::connect(listener.local_addr()?).await?;
        let (stream, _) = listener.accept().await?;
        configure_tcp(&stream);
        let sock = SockRef::from(&stream);
        assert!(sock.keepalive()?);
        assert_eq!(sock.tcp_keepalive_time()?, TCP_KEEPALIVE_TIME);
        assert_eq!(sock.tcp_keepalive_interval()?, TCP_KEEPALIVE_INTERVAL);
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
        assert_eq!(sock.tcp_keepalive_retries()?, TCP_KEEPALIVE_RETRIES);

        let (stop, mut stop_rx) = watch::channel(());
        let mut wrapped = WriteIdle::new(
            stream,
            Duration::from_secs(30),
            Box::pin(async move {
                let _ = stop_rx.changed().await;
            }),
        );
        let (waiting, observed) = oneshot::channel();
        let read = tokio::spawn(async move {
            let mut read = std::pin::pin!(wrapped.read_u8());
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

    #[tokio::test]
    async fn server_closes_slowloris_headers() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = Router::new().route("/ping", get(|| async { "ok" }));
        let (stop_tx, stop_rx) = oneshot::channel();
        let server = tokio::spawn(serve_http1(
            listener,
            app,
            HttpServerConfig {
                header_read_timeout: Duration::from_millis(120),
                write_idle_timeout: Duration::from_secs(5),
                drain_timeout: Duration::from_secs(1),
                force_close_timeout: Duration::from_millis(500),
                max_connections: 4,
            },
            stop_rx,
        ));

        // Incomplete headers must be closed promptly by header_read_timeout.
        let mut slow = TcpStream::connect(addr).await?;
        slow.write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n")
            .await?;
        let mut buf = [0u8; 32];
        let closed = time::timeout(Duration::from_secs(2), slow.read(&mut buf)).await??;
        assert_eq!(closed, 0);

        let _ = stop_tx.send(());
        server.await.map_err(io::Error::other)??;
        Ok(())
    }

    #[tokio::test]
    async fn server_caps_open_connections_and_reclaims_released_permits() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = Router::new().route("/ping", get(|| async { "ok" }));
        let (stop_tx, stop_rx) = oneshot::channel();
        let server = tokio::spawn(serve_http1(
            listener,
            app,
            HttpServerConfig {
                header_read_timeout: Duration::from_secs(5),
                write_idle_timeout: Duration::from_secs(5),
                drain_timeout: Duration::from_secs(1),
                force_close_timeout: Duration::from_millis(500),
                max_connections: 2,
            },
            stop_rx,
        ));

        // Two keep-alive sockets saturate max_connections = 2.
        let mut first = TcpStream::connect(addr).await?;
        let mut second = TcpStream::connect(addr).await?;
        let ping = b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n";
        first.write_all(ping).await?;
        second.write_all(ping).await?;
        let mut resp = [0u8; 128];
        let n1 = time::timeout(Duration::from_secs(1), first.read(&mut resp)).await??;
        assert!(
            std::str::from_utf8(&resp[..n1]).is_ok_and(|text| text.starts_with("HTTP/1.1 200"))
        );
        let n2 = time::timeout(Duration::from_secs(1), second.read(&mut resp)).await??;
        assert!(
            std::str::from_utf8(&resp[..n2]).is_ok_and(|text| text.starts_with("HTTP/1.1 200"))
        );

        // A third socket sending a valid HTTP request must be dropped immediately without 200 OK.
        let mut third = TcpStream::connect(addr).await?;
        let _ = third.write_all(ping).await;
        let mut buf = [0u8; 64];
        match time::timeout(Duration::from_secs(1), third.read(&mut buf)).await? {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                ) => {}
            other => {
                return Err(io::Error::other(format!(
                    "expected dropped excess connection, got {other:?}"
                )));
            }
        }

        // Releasing one open socket frees its permit for a fourth request.
        drop(first);
        let mut fourth = None;
        for _ in 0..20 {
            let mut candidate = TcpStream::connect(addr).await?;
            if candidate.write_all(ping).await.is_ok()
                && let Ok(Ok(n)) =
                    time::timeout(Duration::from_millis(200), candidate.read(&mut resp)).await
                && n > 0
                && std::str::from_utf8(&resp[..n])
                    .is_ok_and(|text| text.starts_with("HTTP/1.1 200"))
            {
                fourth = Some(candidate);
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            fourth.is_some(),
            "released connection permit was not reclaimed"
        );

        drop(fourth);
        drop(second);
        let _ = stop_tx.send(());
        server.await.map_err(io::Error::other)??;
        Ok(())
    }

    #[tokio::test]
    async fn force_close_aborts_stalled_get_handler_and_finishes_shutdown() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_handler = entered.clone();
        let app = Router::new().route(
            "/hang",
            get(move || {
                let entered = entered_handler.clone();
                async move {
                    entered.notify_one();
                    future::pending::<&'static str>().await
                }
            }),
        );
        let (stop_tx, stop_rx) = oneshot::channel();
        let server = tokio::spawn(serve_http1(
            listener,
            app,
            HttpServerConfig {
                header_read_timeout: Duration::from_secs(5),
                write_idle_timeout: Duration::from_secs(5),
                drain_timeout: Duration::from_millis(80),
                force_close_timeout: Duration::from_millis(500),
                max_connections: 4,
            },
            stop_rx,
        ));

        let mut client = TcpStream::connect(addr).await?;
        client
            .write_all(b"GET /hang HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        time::timeout(Duration::from_secs(1), entered.notified()).await?;

        let _ = stop_tx.send(());
        // Force-close must abort the stalled handler task cleanly instead of timing out.
        time::timeout(Duration::from_secs(2), server)
            .await?
            .map_err(io::Error::other)??;
        let mut buf = [0u8; 16];
        let closed = time::timeout(Duration::from_secs(1), client.read(&mut buf)).await??;
        assert_eq!(closed, 0);
        Ok(())
    }
}
