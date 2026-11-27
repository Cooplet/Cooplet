use std::{
    io::{self, BufRead, BufReader, Write},
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::{
        Arc,
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
    result: oneshot::Sender<Result<Instant, WriteFailure>>,
}

pub(super) struct Wire {
    writer: mpsc::Sender<WriteFrame>,
    reader: mpsc::Receiver<Result<ReceivedFrame, Fault>>,
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
        let closed = Arc::new(AtomicBool::new(false));
        let wire = Self {
            writer,
            reader,
            closed: closed.clone(),
            next_id: 1,
        };
        thread::Builder::new()
            .name("agy-acp-stdin".into())
            .spawn(move || run_writer(stdin, writes, &closed))?;
        thread::Builder::new()
            .name("agy-acp-stdout".into())
            .spawn(move || run_reader(stdout, &frames))?;
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

    pub(super) async fn send(
        &self,
        value: Value,
        admission: Option<Admission>,
    ) -> Result<Instant, WriteFailure> {
        let mut bytes = serde_json::to_vec(&value).map_err(|_| WriteFailure::Encode)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(WriteFailure::FrameLimit);
        }
        bytes.push(b'\n');
        let (result, written) = oneshot::channel();
        self.writer
            .try_send(WriteFrame {
                bytes,
                admission,
                result,
            })
            .map_err(|_| WriteFailure::Unwritten)?;
        // Once queued, only the writer can prove that the first byte was prevented.
        match tokio::time::timeout(WRITE_TIMEOUT, written).await {
            Ok(Ok(outcome)) => outcome,
            _ => {
                self.closed.store(true, Ordering::Release);
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
        self.send(response, None)
            .await
            .map(|_| ())
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
        if closed.load(Ordering::Acquire) {
            break;
        }
        let outcome = write_frame(&mut stdin, &frame.bytes, frame.admission, closed);
        let unusable = matches!(
            outcome,
            Err(WriteFailure::Unknown | WriteFailure::Unwritten)
        );
        if unusable {
            drop(stdin);
            let _ = frame.result.send(outcome);
            return;
        }
        let _ = frame.result.send(outcome);
    }
}

fn write_frame(
    writer: &mut impl Write,
    bytes: &[u8],
    admission: Option<Admission>,
    closed: &AtomicBool,
) -> Result<Instant, WriteFailure> {
    let abandoned = || {
        admission
            .as_ref()
            .is_some_and(|admission| admission.submitting.has_changed().is_err())
    };
    let mut started = Instant::now();
    let mut remaining = bytes;
    while !remaining.is_empty() {
        if remaining.len() == bytes.len() {
            started = Instant::now();
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
        } else if closed.load(Ordering::Acquire) || abandoned() {
            return Err(WriteFailure::Unknown);
        }
        match writer.write(remaining) {
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
        Ok(started)
    } else if remaining.len() == bytes.len() {
        Err(WriteFailure::Unwritten)
    } else {
        Err(WriteFailure::Unknown)
    }
}

fn run_reader(stdout: ChildStdout, frames: &mpsc::Sender<Result<ReceivedFrame, Fault>>) {
    let mut reader = BufReader::new(stdout);
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let frame = read_frame(&mut reader, &mut bytes).and_then(|()| {
            let read_at = Instant::now();
            parse_frame(&bytes).map(|frame| ReceivedFrame { frame, read_at })
        });
        let failed = frame.is_err();
        if frames.blocking_send(frame).is_err() || failed {
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
