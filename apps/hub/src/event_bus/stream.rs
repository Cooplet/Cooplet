use std::{collections::VecDeque, convert::Infallible, sync::Arc, time::Duration};

use axum::{
    body::Body,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use cooplet_protocol::EventStreamFrame;
use cooplet_storage::DbActor;
use futures_util::stream;
use http::header;
use tokio::{
    sync::{broadcast, watch},
    time::Instant,
};

use super::{Budget, BufferedFrame, Cursor, Reader, StreamSnapshot, Watermarks};
use crate::{auth, error::HubError};

const STREAM_SCAN_LIMIT: usize = 256;
const PING_INTERVAL: Duration = Duration::from_secs(15);
const AUTH_INTERVAL: Duration = Duration::from_secs(30);
const MAX_IDLE_EXPIRY_WAIT_MS: i64 = 30_000;
const NDJSON_MEDIA_TYPE: &str = "application/x-ndjson";
const PING_FRAME: &[u8] = b"{\"type\":\"ping\"}\n";

type Watermark = (u64, u64);

#[derive(Clone)]
pub(crate) struct EventWake(watch::Receiver<Watermark>);

impl EventWake {
    pub(crate) fn new(db: &DbActor, mut shutdown: watch::Receiver<()>) -> Self {
        let mut subscription = db.subscribe();
        let (wake, receiver) = watch::channel::<Watermark>((0, 0));
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = wake.closed() => break,
                    event = subscription.recv() => match event {
                        Ok(event) => wake.send_modify(|(head, _)| *head = (*head).max(event.seq)),
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            wake.send_modify(|(_, generation)| *generation = generation.wrapping_add(1));
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                }
            }
        });
        Self(receiver)
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<Watermark> {
        self.0.clone()
    }
}

pub(super) async fn response(
    reader: Arc<Reader>,
    after: u64,
    mut wake: watch::Receiver<Watermark>,
    revocations: watch::Receiver<u64>,
    mut thread_acl: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<()>,
) -> Result<Response, HubError> {
    let budget = Budget {
        rows: STREAM_SCAN_LIMIT,
        items: STREAM_SCAN_LIMIT,
    };
    wake.borrow_and_update();
    thread_acl.borrow_and_update();
    let initial = tokio::select! {
        biased;
        _ = shutdown.changed() => return Err(HubError::new(
            cooplet_protocol::ErrorCode::Overloaded,
            "The Hub is shutting down.",
        )),
        initial = reader.snapshot_stream(Cursor::Initial(after), budget) => match initial? {
            StreamSnapshot::Batch(batch) => batch,
            StreamSnapshot::CursorLost(watermarks) => return Err(watermarks.problem()),
        },
    };
    if revocations.has_changed().unwrap_or(true) {
        auth::reverify_caller(&reader.reads, &reader.caller).await?;
    }
    let now = Instant::now();
    let confirmed_cursor = if initial.frames.is_empty() {
        initial.next_seq
    } else {
        initial.start
    };
    let state = EventStream {
        reader,
        wake,
        revocations,
        thread_acl,
        shutdown,
        budget,
        pending: initial.frames.into(),
        pending_resync: None,
        cursor: initial.next_seq,
        confirmed_cursor,
        needs_scan: initial.has_more,
        closed: false,
        ping_at: now + PING_INTERVAL,
        recheck_at: now + AUTH_INTERVAL,
    };
    let frames = stream::unfold(state, |mut stream| async move {
        let frame = stream.next_frame().await?;
        Some((Ok::<_, Infallible>(frame), stream))
    });
    Ok((
        [(header::CONTENT_TYPE, NDJSON_MEDIA_TYPE)],
        Body::from_stream(frames),
    )
        .into_response())
}

struct EventStream {
    reader: Arc<Reader>,
    wake: watch::Receiver<Watermark>,
    revocations: watch::Receiver<u64>,
    thread_acl: watch::Receiver<u64>,
    shutdown: watch::Receiver<()>,
    budget: Budget,
    pending: VecDeque<BufferedFrame>,
    pending_resync: Option<Watermarks>,
    cursor: u64,
    confirmed_cursor: u64,
    needs_scan: bool,
    closed: bool,
    ping_at: Instant,
    recheck_at: Instant,
}

impl EventStream {
    async fn next_frame(&mut self) -> Option<Bytes> {
        loop {
            if self.closed || self.shutdown.has_changed().unwrap_or(true) {
                return None;
            }

            let now = Instant::now();
            let expiry = self.expiry_wait()?;
            if self.revocations.has_changed().ok()? || self.thread_acl.has_changed().ok()? {
                self.discard_and_rescan().await?;
                // Check for a second authorization change during the snapshot.
                continue;
            }
            if now >= self.recheck_at {
                if self.pending.is_empty() && self.pending_resync.is_none() {
                    // An uncertain commit can leave a durable event without a wake.
                    // This snapshot also reverifies the caller in the same read transaction.
                    self.scan().await?;
                } else {
                    tokio::select! {
                        biased;
                        _ = self.shutdown.changed() => return None,
                        result = auth::reverify_caller(&self.reader.reads, &self.reader.caller) => result.ok()?,
                    }
                    self.needs_scan = true;
                }
                self.recheck_at = Instant::now() + AUTH_INTERVAL;
                // Check for a second revocation that could have committed during the read.
                continue;
            }

            if let Some(watermarks) = self.pending_resync.take() {
                self.closed = true;
                let frame = EventStreamFrame::ResyncRequired {
                    oldest_available_seq: watermarks.oldest(),
                    latest_seq: watermarks.head,
                    reason: "cursor_pruned".into(),
                };
                return match serde_json::to_vec(&frame) {
                    Ok(mut bytes) => {
                        bytes.push(b'\n');
                        Some(Bytes::from(bytes))
                    }
                    Err(error) => {
                        tracing::error!(%error, "Could not encode the EventBus resync frame");
                        None
                    }
                };
            }
            if now >= self.ping_at {
                self.ping_at = now + PING_INTERVAL;
                return Some(Bytes::from_static(PING_FRAME));
            }
            if let Some((_, Some(expiry), _)) = self.pending.front() {
                // GC can clear a closed board during a forward clock step. A clock
                // rollback cannot restore access to a frame fetched before that commit.
                let retained = (self.reader.clock)() < expiry.at_ms
                    && tokio::select! {
                        biased;
                        _ = self.shutdown.changed() => return None,
                        retained = self.reader.closed_thread_retained(&expiry.thread_id) => retained.ok()?,
                    };
                if self.revocations.has_changed().ok()? || self.thread_acl.has_changed().ok()? {
                    continue;
                }
                self.expiry_wait()?;
                if !retained || (self.reader.clock)() >= expiry.at_ms {
                    self.discard_and_rescan().await?;
                    continue;
                }
            }
            if let Some((seq, _, frame)) = self.pending.pop_front() {
                self.confirmed_cursor = if self.pending.is_empty() {
                    self.cursor
                } else {
                    seq
                };
                return Some(frame);
            }
            if self.needs_scan {
                self.scan().await?;
                continue;
            }

            let deadline = self.ping_at.min(self.recheck_at).min(now + expiry);
            tokio::select! {
                biased;
                _ = self.shutdown.changed() => return None,
                change = self.revocations.changed() => {
                    change.ok()?;
                    self.recheck_at = Instant::now();
                }
                change = self.thread_acl.changed() => {
                    change.ok()?;
                    // There are no buffered frames in this branch.
                    self.needs_scan = true;
                }
                change = self.wake.changed() => {
                    change.ok()?;
                    self.needs_scan = true;
                }
                () = tokio::time::sleep_until(deadline) => {},
            }
        }
    }

    async fn discard_and_rescan(&mut self) -> Option<()> {
        self.revocations.borrow_and_update();
        self.thread_acl.borrow_and_update();
        self.pending.clear();
        self.pending_resync = None;
        self.cursor = self.confirmed_cursor;
        self.scan().await?;
        self.recheck_at = Instant::now() + AUTH_INTERVAL;
        Some(())
    }

    async fn scan(&mut self) -> Option<()> {
        // Coalesce existing wakeups. A notification sent during this snapshot stays
        // pending for another pass if the snapshot could not see its commit.
        self.wake.borrow_and_update();
        let next = tokio::select! {
            biased;
            _ = self.shutdown.changed() => return None,
            next = self.reader.snapshot_stream(Cursor::Resume(self.cursor), self.budget) => next.ok()?,
        };
        match next {
            StreamSnapshot::Batch(batch) => {
                self.pending = batch.frames.into();
                self.cursor = batch.next_seq;
                if self.pending.is_empty() {
                    self.confirmed_cursor = self.cursor;
                }
                self.needs_scan = batch.has_more;
            }
            StreamSnapshot::CursorLost(watermarks) => {
                self.pending_resync = Some(watermarks);
            }
        }
        Some(())
    }

    /// Finite credentials can expire before the next idle recheck. The cap keeps
    /// the timer representable and makes wall-clock changes visible on recheck.
    fn expiry_wait(&self) -> Option<Duration> {
        let expires_at = self
            .reader
            .caller
            .expires_at_ms()
            .filter(|time| *time != i64::MAX);
        let Some(expires_at) = expires_at else {
            return Some(AUTH_INTERVAL);
        };
        let remaining = expires_at.saturating_sub((self.reader.clock)());
        if remaining <= 0 {
            return None;
        }
        let bounded = remaining.min(MAX_IDLE_EXPIRY_WAIT_MS);
        Some(Duration::from_millis(u64::try_from(bounded).ok()?))
    }
}
