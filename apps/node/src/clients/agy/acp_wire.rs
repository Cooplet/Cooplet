use std::{
    io::{self, BufRead, BufReader, Write},
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use cooplet_platform::AdmissionGuard;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) enum Frame {
    Request { method: String, id: Value },
    Notification { method: String, params: Value },
    Response { id: Value, reply: Reply },
}

pub(super) struct ReceivedFrame {
    pub frame: Frame,
    pub read_at: Instant,
}

pub(super) type Reply = Result<Value, i64>;

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Closed,
    FrameLimit,
    InvalidFrame,
    PermissionRequest,
    TextLimit,
    EventReceiverClosed,
    WriteUnconfirmed,
    PromptUnconfirmed,
    TooManyRequests,
}

impl Fault {
    pub(super) fn detail(self) -> &'static str {
        match self {
            Self::Closed => "Agy ACP transport closed",
            Self::FrameLimit => "Agy ACP frame exceeded the size limit",
            Self::InvalidFrame => "Agy sent an invalid ACP frame",
            Self::PermissionRequest => "Agy requested permission in a full-access session",
            Self::TextLimit => "Agy turn output exceeded the size limit",
            Self::EventReceiverClosed => "Agy session event receiver closed",
            Self::WriteUnconfirmed => "Agy ACP write did not finish",
            Self::PromptUnconfirmed => "Agy ACP prompt outcome is unconfirmed",
            Self::TooManyRequests => "Agy sent too many ACP requests during a write",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum WriteFailure {
    Encode,
    FrameLimit,
    Expired,
    Unwritten,
    Unknown,
}

pub(super) struct Admission {
    pub guard: AdmissionGuard,
    pub submitting: watch::Receiver<()>,
}

struct WriteFrame {
    bytes: Vec<u8>,
    admission: Option<Admission>,
    started: Arc<OnceLock<Instant>>,
    result: oneshot::Sender<Result<(), WriteFailure>>,
}

pub(super) struct PendingWrite {
    pub started: Arc<OnceLock<Instant>>,
    result: oneshot::Receiver<Result<(), WriteFailure>>,
}

pub(super) struct Wire {
    writer: mpsc::Sender<WriteFrame>,
    reader: mpsc::Receiver<Result<ReceivedFrame, Fault>>,
    _keep_faulted_stdout_open: mpsc::Sender<()>,
    closed: Arc<AtomicBool>,
    next_id: u64,
}

impl Wire {
    pub(super) fn start(
        stdin: ChildStdin,
        stdout: ChildStdout,
        stderr: Option<ChildStderr>,
    ) -> io::Result<Self> {
        let (writer, writes) = mpsc::channel(1);
        let (frames, reader) = mpsc::channel(4);
        let (keep_stdout, release_stdout) = mpsc::channel(1);
        let closed = Arc::new(AtomicBool::new(false));
        let wire = Self {
            writer,
            reader,
            _keep_faulted_stdout_open: keep_stdout,
            closed: closed.clone(),
            next_id: 1,
        };
        thread::Builder::new()
            .name("agy-acp-stdin".into())
            .spawn(move || run_writer(stdin, writes, &closed))?;
        thread::Builder::new()
            .name("agy-acp-stdout".into())
            .spawn(move || run_reader(stdout, &frames, release_stdout))?;
        if let Some(mut stderr) = stderr {
            thread::Builder::new()
                .name("agy-acp-stderr".into())
                .spawn(move || {
                    let _ = io::copy(&mut stderr, &mut io::sink());
                })?;
        }
        Ok(wire)
    }

    pub(super) fn close_signal(&self) -> Arc<AtomicBool> {
        self.closed.clone()
    }

    pub(super) fn request(&mut self, method: &str, params: Value) -> (u64, Value) {
        let id = self.next_id;
        self.next_id += 1;
        (
            id,
            json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}),
        )
    }

    pub(super) fn begin_write(
        &self,
        value: Value,
        admission: Option<Admission>,
    ) -> Result<PendingWrite, WriteFailure> {
        let mut bytes = serde_json::to_vec(&value).map_err(|_| WriteFailure::Encode)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(WriteFailure::FrameLimit);
        }
        bytes.push(b'\n');
        let (result, written) = oneshot::channel();
        let started = Arc::new(OnceLock::new());
        self.writer
            .try_send(WriteFrame {
                bytes,
                admission,
                started: started.clone(),
                result,
            })
            .map_err(|_| WriteFailure::Unwritten)?;
        Ok(PendingWrite {
            started,
            result: written,
        })
    }

    pub(super) async fn send_control(&self, value: Value) -> Result<(), WriteFailure> {
        let pending = self.begin_write(value, None)?;
        Self::finish_write(pending, &self.closed).await
    }

    /// A peer can write to stdout while reading the request on stdin. Drain
    /// inbound frames during the write so neither side waits on a full pipe.
    pub(super) async fn pump_write(
        &mut self,
        pending: PendingWrite,
        mut consume: impl FnMut(ReceivedFrame) -> Result<(), Fault>,
    ) -> (Result<(), WriteFailure>, Option<Fault>) {
        let closed = self.closed.clone();
        let written = Self::finish_write(pending, &closed);
        tokio::pin!(written);
        loop {
            tokio::select! {
                biased;
                result = &mut written => return (result, None),
                incoming = self.recv() => {
                    if let Err(fault) = incoming.and_then(&mut consume) {
                        // Fence further writes, then let the writer classify
                        // whether any bytes of this frame crossed the pipe.
                        closed.store(true, Ordering::Release);
                        return (written.await, Some(fault));
                    }
                }
            }
        }
    }

    async fn finish_write(pending: PendingWrite, closed: &AtomicBool) -> Result<(), WriteFailure> {
        match tokio::time::timeout(WRITE_TIMEOUT, pending.result).await {
            Ok(Ok(outcome)) => outcome,
            _ => {
                closed.store(true, Ordering::Release);
                Err(WriteFailure::Unknown)
            }
        }
    }

    pub(super) async fn recv(&mut self) -> Result<ReceivedFrame, Fault> {
        self.reader.recv().await.unwrap_or(Err(Fault::Closed))
    }

    pub(super) async fn reject_request(&self, id: Value) -> Result<(), Fault> {
        let response = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "Method not found"}
        });
        self.send_control(response)
            .await
            .map_err(|_| Fault::WriteUnconfirmed)
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
    }
}

fn run_writer(mut stdin: ChildStdin, mut writes: mpsc::Receiver<WriteFrame>, closed: &AtomicBool) {
    while let Some(frame) = writes.blocking_recv() {
        let outcome = write_frame(
            &mut stdin,
            &frame.bytes,
            frame.admission,
            closed,
            &frame.started,
        );
        let unusable = matches!(
            outcome,
            Err(WriteFailure::Unknown | WriteFailure::Unwritten)
        );
        if unusable {
            closed.store(true, Ordering::Release);
        }
        let _ = frame.result.send(outcome);
        if unusable {
            // Hold stdin until actor teardown, with no further writes.
            while writes.blocking_recv().is_some() {}
            break;
        }
    }
}

fn write_frame(
    writer: &mut impl Write,
    bytes: &[u8],
    admission: Option<Admission>,
    closed: &AtomicBool,
    started: &OnceLock<Instant>,
) -> Result<(), WriteFailure> {
    let abandoned = || {
        admission
            .as_ref()
            .is_some_and(|admission| admission.submitting.has_changed().is_err())
    };
    let mut remaining = bytes;
    while !remaining.is_empty() {
        if remaining.len() == bytes.len() {
            if closed.load(Ordering::Acquire) {
                return Err(WriteFailure::Unwritten);
            }
            if abandoned()
                || admission
                    .as_ref()
                    .is_some_and(|admission| admission.guard.is_expired())
            {
                return Err(WriteFailure::Expired);
            }
            started.get_or_init(Instant::now);
        } else if closed.load(Ordering::Acquire) || abandoned() {
            return Err(WriteFailure::Unknown);
        }
        // Fence again before the JSON closing byte and its LF. A blocked
        // payload write that finishes after fencing leaves incomplete JSON.
        let chunk = if remaining.len() > 2 {
            &remaining[..remaining.len() - 2]
        } else {
            remaining
        };
        match writer.write(chunk) {
            Ok(0) => break,
            Ok(count) => remaining = &remaining[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    if remaining.is_empty()
        && !closed.load(Ordering::Acquire)
        && !abandoned()
        && writer.flush().is_ok()
    {
        Ok(())
    } else if remaining.len() == bytes.len() {
        Err(WriteFailure::Unwritten)
    } else {
        Err(WriteFailure::Unknown)
    }
}

fn run_reader(
    stdout: ChildStdout,
    frames: &mpsc::Sender<Result<ReceivedFrame, Fault>>,
    mut release_stdout: mpsc::Receiver<()>,
) {
    let mut reader = BufReader::new(stdout);
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let frame = read_frame(&mut reader, &mut bytes).and_then(|()| {
            let read_at = Instant::now();
            parse_frame(&bytes).map(|frame| ReceivedFrame { frame, read_at })
        });
        let failed = frame.is_err();
        if frames.blocking_send(frame).is_err() {
            break;
        }
        if failed {
            // Keep the descriptor open until the actor is ready to close.
            // Closing it early can make the peer exit via a broken pipe.
            let _ = release_stdout.blocking_recv();
            break;
        }
    }
}

fn read_frame(reader: &mut impl BufRead, bytes: &mut Vec<u8>) -> Result<(), Fault> {
    loop {
        let buffered = reader.fill_buf().map_err(|_| Fault::Closed)?;
        if buffered.is_empty() {
            return Err(Fault::Closed);
        }
        let newline = buffered.iter().position(|byte| *byte == b'\n');
        let len = newline.unwrap_or(buffered.len());
        if len > MAX_FRAME_BYTES - bytes.len() {
            return Err(Fault::FrameLimit);
        }
        bytes.extend_from_slice(&buffered[..len]);
        reader.consume(len + usize::from(newline.is_some()));
        if newline.is_some() {
            return Ok(());
        }
    }
}

fn parse_frame(bytes: &[u8]) -> Result<Frame, Fault> {
    let mut value: Value = serde_json::from_slice(bytes).map_err(|_| Fault::InvalidFrame)?;
    let object = value.as_object_mut().ok_or(Fault::InvalidFrame)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(Fault::InvalidFrame);
    }
    let id = object.remove("id");
    let method = object.remove("method");
    let result = object.remove("result");
    let error = object.remove("error");
    let params = object.remove("params");

    match (method, id, result, error) {
        (Some(Value::String(method)), Some(id), None, None) if valid_id(&id) => {
            Ok(Frame::Request { method, id })
        }
        (Some(Value::String(method)), None, None, None) => Ok(Frame::Notification {
            method,
            params: params.unwrap_or(Value::Null),
        }),
        (None, Some(id), Some(result), None) if valid_id(&id) && params.is_none() => {
            Ok(Frame::Response {
                id,
                reply: Ok(result),
            })
        }
        (None, Some(id), None, Some(error)) if valid_id(&id) && params.is_none() => {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .ok_or(Fault::InvalidFrame)?;
            Ok(Frame::Response {
                id,
                reply: Err(code),
            })
        }
        _ => Err(Fault::InvalidFrame),
    }
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.is_i64() || id.is_u64() || id.is_null()
}

#[cfg(test)]
mod tests;
