use std::{
    io,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use cooplet_platform::AdmissionGuard;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
    net::unix::pipe::{Receiver, Sender},
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    time::timeout,
};

use crate::clients::{
    AdapterEvent, AdapterEventKind, SpawnedManagedProcess, StopOutcome, SubmitOutcome,
    TurnFinishStatus,
};

use super::turn::{NativeActivity, Turn};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_NOTIFICATION_BYTES: usize = 64 * 1024;

pub(super) struct Session {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<State>,
    input: AsyncMutex<Option<Sender>>,
    shutdown: watch::Receiver<bool>,
}

pub(super) struct State {
    events: Option<mpsc::Sender<AdapterEvent>>,
    shutdown: watch::Sender<bool>,
    pending: Option<Pending>,
    next_command: u64,
    event_seq: u64,
    pub(super) turn: Option<Turn>,
    pub(super) native_activity: NativeActivity,
    ready: bool,
}

struct Pending {
    id: String,
    command: &'static str,
    reply: oneshot::Sender<Reply>,
    notifications: Option<Vec<String>>,
    notification_bytes: usize,
}

#[derive(Deserialize)]
pub(super) struct Reply {
    command: String,
    pub(super) success: bool,
    #[serde(default)]
    pub(super) data: Value,
    pub(super) error: Option<String>,
    #[serde(skip)]
    pub(super) notifications: Vec<String>,
    #[serde(skip)]
    outcome: Option<SubmitOutcome>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum CommandError {
    Expired,
    Busy,
    BeforeWrite,
    AfterWrite,
}

impl Session {
    pub(super) fn new(
        process: SpawnedManagedProcess,
    ) -> io::Result<(Self, mpsc::Receiver<AdapterEvent>)> {
        let input = Sender::from_owned_fd(required_pipe(process.stdin)?.into())?;
        let output = Receiver::from_owned_fd(required_pipe(process.stdout)?.into())?;
        let errors = Receiver::from_owned_fd(required_pipe(process.stderr)?.into())?;
        Ok(Self::with_pipes(input, output, errors))
    }

    pub(super) fn with_pipes(
        input: Sender,
        output: Receiver,
        mut errors: Receiver,
    ) -> (Self, mpsc::Receiver<AdapterEvent>) {
        let (events, event_receiver) = mpsc::channel(64);
        let (signal, shutdown) = watch::channel(false);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                events: Some(events),
                shutdown: signal,
                pending: None,
                next_command: 0,
                event_seq: 0,
                turn: None,
                native_activity: NativeActivity::Idle,
                ready: false,
            }),
            input: AsyncMutex::new(Some(input)),
            shutdown: shutdown.clone(),
        });

        tokio::spawn(read_output(Arc::clone(&shared), output));
        tokio::spawn(async move {
            // Pi diagnostics may contain launch arguments. Drain without retaining them.
            let mut sink = tokio::io::sink();
            tokio::select! {
                _ = closed(shutdown) => {}
                _ = tokio::io::copy(&mut errors, &mut sink) => {}
            }
        });
        (Self { shared }, event_receiver)
    }

    pub(super) fn state(&self) -> MutexGuard<'_, State> {
        self.shared.state()
    }

    pub(super) fn mark_ready(&self) -> bool {
        let mut state = self.state();
        state.ready = state.idle();
        state.ready
    }

    pub(super) async fn close(&self) {
        self.state().close();
        self.shared.input.lock().await.take();
    }

    pub(super) async fn request(
        &self,
        kind: &'static str,
        message: Option<&str>,
        collect_notifications: bool,
    ) -> Result<Reply, CommandError> {
        let input = self.shared.input.lock().await;
        let id = self.state().new_id()?;
        self.shared
            .command(
                input.as_ref(),
                id,
                kind,
                message,
                None,
                collect_notifications,
            )
            .await
    }

    pub(super) async fn submit(
        &self,
        operation_key: String,
        message: &str,
        admission: &AdmissionGuard,
    ) -> SubmitOutcome {
        let reservation = {
            let mut state = self.state();
            if state.is_closed() {
                return SubmitOutcome::Unavailable("Pi RPC is closed".into());
            }
            if !state.idle() {
                return SubmitOutcome::Busy;
            }
            let Ok(id) = state.new_id() else {
                return SubmitOutcome::Unavailable("Pi exhausted its RPC request IDs".into());
            };
            state.turn = Some(Turn::new(id.clone(), operation_key, message));
            Reservation { session: self, id }
        };

        let Some(ms) = admission.remaining_ms() else {
            return SubmitOutcome::Expired;
        };
        let Ok(input) = timeout(Duration::from_millis(ms), self.shared.input.lock()).await else {
            return SubmitOutcome::Expired;
        };
        match self
            .shared
            .command(
                input.as_ref(),
                reservation.id.clone(),
                "prompt",
                Some(message),
                Some(admission),
                false,
            )
            .await
        {
            Ok(reply) => reply.outcome.unwrap_or_else(|| {
                SubmitOutcome::Unknown("Pi did not correlate the prompt acknowledgment".into())
            }),
            Err(CommandError::Expired) => SubmitOutcome::Expired,
            Err(CommandError::Busy) => SubmitOutcome::Busy,
            Err(CommandError::BeforeWrite) => {
                SubmitOutcome::Unavailable("Pi RPC failed before writing the prompt".into())
            }
            Err(CommandError::AfterWrite) => {
                SubmitOutcome::Unknown("Pi prompt acknowledgment was lost".into())
            }
        }
    }

    pub(super) async fn cancel(&self) -> StopOutcome {
        let Some(target) = self.state().turn.as_ref().map(|turn| turn.id.clone()) else {
            return StopOutcome::AlreadyGone;
        };
        // Hold the writer until cancellation is resolved so abort cannot race a later prompt.
        let input = self.shared.input.lock().await;
        let (id, termination) = {
            let mut state = self.state();
            let Ok(id) = state.new_id() else {
                return StopOutcome::AlreadyGone;
            };
            let Some(turn) = state
                .turn
                .as_mut()
                .filter(|turn| turn.id == target && turn.accepted)
            else {
                return StopOutcome::AlreadyGone;
            };
            let (send, receive) = oneshot::channel();
            turn.cancel = Some(send);
            (id, receive)
        };

        match self
            .shared
            .command(input.as_ref(), id, "abort", None, None, false)
            .await
        {
            Ok(reply) if reply.success => match termination.await {
                Ok(TurnFinishStatus::Cancelled) => StopOutcome::TurnCancelled,
                _ => StopOutcome::Unknown("Pi did not confirm that the turn was cancelled".into()),
            },
            _ => StopOutcome::Unknown("Pi did not acknowledge the abort command".into()),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.state().close();
    }
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn command(
        &self,
        input: Option<&Sender>,
        id: String,
        kind: &'static str,
        message: Option<&str>,
        admission: Option<&AdmissionGuard>,
        collect_notifications: bool,
    ) -> Result<Reply, CommandError> {
        let input = input.ok_or(CommandError::BeforeWrite)?;
        #[derive(serde::Serialize)]
        struct Command<'a> {
            id: &'a str,
            #[serde(rename = "type")]
            kind: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            message: Option<&'a str>,
        }
        let mut bytes = serde_json::to_vec(&Command {
            id: &id,
            kind,
            message,
        })
        .map_err(|_| CommandError::BeforeWrite)?;
        bytes.push(b'\n');
        let (reply, receive) = oneshot::channel();
        {
            let mut state = self.state();
            if state.is_closed() {
                return Err(CommandError::BeforeWrite);
            }
            state.pending = Some(Pending {
                id: id.clone(),
                command: kind,
                reply,
                notifications: collect_notifications.then(Vec::new),
                notification_bytes: 0,
            });
        }

        let mut transfer = Transfer {
            shared: self,
            id,
            written: 0,
            acknowledged: false,
        };
        let result = {
            let send_and_receive = async {
                transfer.write(input, &bytes, admission).await?;
                receive.await.map_err(|_| transfer.failure())
            };
            tokio::select! {
                biased;
                result = timeout(COMMAND_TIMEOUT, send_and_receive) => result.ok(),
                _ = closed(self.shutdown.clone()) => None,
            }
        };
        match result {
            Some(Ok(reply)) => {
                transfer.acknowledged = true;
                Ok(reply)
            }
            Some(Err(error)) => Err(error),
            _ => Err(transfer.failure()),
        }
    }
}

struct Reservation<'a> {
    session: &'a Session,
    id: String,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut state = self.session.state();
        if state
            .turn
            .as_ref()
            .is_some_and(|turn| turn.id == self.id && !turn.accepted)
        {
            state.turn = None;
        }
    }
}

struct Transfer<'a> {
    shared: &'a Shared,
    id: String,
    written: usize,
    acknowledged: bool,
}

impl Transfer<'_> {
    fn failure(&self) -> CommandError {
        if self.written == 0 {
            CommandError::BeforeWrite
        } else {
            CommandError::AfterWrite
        }
    }

    async fn write(
        &mut self,
        pipe: &Sender,
        bytes: &[u8],
        admission: Option<&AdmissionGuard>,
    ) -> Result<(), CommandError> {
        while self.written < bytes.len() {
            if self.written == 0
                && let Some(guard) = admission
            {
                let ms = guard.remaining_ms().ok_or(CommandError::Expired)?;
                if timeout(Duration::from_millis(ms), pipe.writable())
                    .await
                    .map_err(|_| CommandError::Expired)?
                    .is_err()
                {
                    return Err(self.failure());
                }
            } else {
                pipe.writable().await.map_err(|_| self.failure())?;
            }

            // Hold the state lock through the first nonblocking write so a fast reader
            // cannot observe events before the turn is marked as sent.
            let write = if self.written == 0 {
                let mut state = self.shared.state();
                if state.is_closed() {
                    return Err(CommandError::BeforeWrite);
                }
                if admission.is_some_and(AdmissionGuard::is_expired) {
                    return Err(CommandError::Expired);
                }
                if state.native_activity.is_busy()
                    && state.turn.as_ref().is_some_and(|turn| turn.id == self.id)
                {
                    return Err(CommandError::Busy);
                }
                let result = pipe.try_write(bytes);
                if matches!(result, Ok(count) if count != 0)
                    && let Some(turn) = state.turn.as_mut().filter(|turn| turn.id == self.id)
                {
                    turn.wrote = true;
                }
                result
            } else {
                pipe.try_write(&bytes[self.written..])
            };
            match write {
                Ok(0) => return Err(self.failure()),
                Ok(count) => self.written += count,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => return Err(self.failure()),
            }
        }
        Ok(())
    }
}

impl Drop for Transfer<'_> {
    fn drop(&mut self) {
        let mut state = self.shared.state();
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.id == self.id)
        {
            state.pending = None;
        }
        if self.written != 0 && !self.acknowledged {
            state.close();
        }
    }
}

impl State {
    fn new_id(&mut self) -> Result<String, CommandError> {
        if self.is_closed() {
            return Err(CommandError::BeforeWrite);
        }
        self.next_command = self
            .next_command
            .checked_add(1)
            .ok_or(CommandError::BeforeWrite)?;
        Ok(self.next_command.to_string())
    }

    pub(super) fn is_closed(&self) -> bool {
        self.events.is_none()
    }

    pub(super) fn close(&mut self) {
        self.events.take();
        self.pending.take();
        self.turn.take();
        self.shutdown.send_replace(true);
    }

    pub(super) fn emit(&mut self, kind: AdapterEventKind) {
        if self.ready
            && let Some(events) = &self.events
        {
            self.event_seq += 1;
            if events
                .try_send(AdapterEvent {
                    event_seq: self.event_seq,
                    kind,
                })
                .is_err()
            {
                self.close();
            }
        }
    }

    fn handle(&mut self, mut frame: Value) {
        let kind = frame["type"].as_str().unwrap_or_default();
        match kind {
            "response" => {
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| frame["id"] == pending.id)
                    && let Some(pending) = self.pending.take()
                {
                    if let Ok(mut reply) = serde_json::from_value::<Reply>(frame.take())
                        && reply.command == pending.command
                    {
                        reply.notifications = pending.notifications.unwrap_or_default();
                        if self.turn.as_ref().is_some_and(|turn| turn.id == pending.id) {
                            reply.outcome = Some(self.accept_turn(&pending.id, &reply));
                        }
                        let _ = pending.reply.send(reply);
                    } else {
                        self.close();
                    }
                }
            }
            "extension_ui_request" if frame["method"] == "notify" => {
                if let Some(pending) = self.pending.as_mut()
                    && let Some(notifications) = pending.notifications.as_mut()
                    && let Some(message) = frame["message"].as_str()
                {
                    pending.notification_bytes =
                        pending.notification_bytes.saturating_add(message.len());
                    if pending.notification_bytes <= MAX_NOTIFICATION_BYTES
                        && notifications.len() < 64
                    {
                        notifications.push(message.to_owned());
                    } else {
                        self.close();
                    }
                }
            }
            "extension_ui_request" => {
                // There is no remote UI to answer blocking extension dialogs.
                if matches!(
                    frame["method"].as_str(),
                    Some("select" | "confirm" | "input" | "editor" | "menu")
                ) {
                    self.close();
                }
            }
            _ => self.handle_turn_event(kind, &frame),
        }
    }
}

async fn read_output(shared: Arc<Shared>, output: Receiver) {
    let mut reader = BufReader::new(output);
    let mut buffer = Vec::new();
    let mut shutdown = shared.shutdown.clone();
    loop {
        let record = tokio::select! {
            biased;
            _ = shutdown.wait_for(|stopped| *stopped) => break,
            record = read_record(&mut reader, &mut buffer) => record,
        };
        if !matches!(record, Ok(true)) {
            break;
        }
        let Ok(frame) = serde_json::from_slice::<Value>(&buffer) else {
            break;
        };
        if !frame["type"].is_string() {
            break;
        }
        shared.state().handle(frame);
        buffer.clear();
        // Avoid retaining the high-water mark of a large echo for the whole generation.
        if buffer.capacity() > 128 * 1024 {
            buffer.shrink_to(128 * 1024);
        }
    }
    shared.state().close();
    shared.input.lock().await.take();
}

async fn read_record(
    reader: &mut (impl AsyncBufRead + Unpin),
    buffer: &mut Vec<u8>,
) -> io::Result<bool> {
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return if buffer.is_empty() {
                Ok(false)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete Pi record",
                ))
            };
        }
        let delimiter = chunk.iter().position(|byte| *byte == b'\n');
        let bytes = delimiter.map_or(chunk.len(), |pos| pos + 1);
        if bytes > MAX_RECORD_BYTES - buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized Pi record",
            ));
        }
        buffer.extend_from_slice(&chunk[..bytes]);
        reader.consume(bytes);
        if delimiter.is_some() {
            return Ok(true);
        }
    }
}

async fn closed(mut receiver: watch::Receiver<bool>) {
    let _ = receiver.wait_for(|stopped| *stopped).await;
}

fn required_pipe<T>(pipe: Option<T>) -> io::Result<T> {
    pipe.ok_or_else(|| io::Error::other("Pi managed pipe is missing"))
}

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod tests;
