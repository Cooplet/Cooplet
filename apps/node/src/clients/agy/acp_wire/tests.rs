use std::io::Cursor;

use super::*;

#[test]
fn bounded_frame_reader_does_not_consume_an_unterminated_oversized_line() {
    let oversized = vec![b'x'; MAX_FRAME_BYTES + 10];
    let mut reader = BufReader::with_capacity(97, Cursor::new(oversized));
    let mut frame = Vec::new();
    assert!(matches!(
        read_frame(&mut reader, &mut frame),
        Err(Fault::FrameLimit)
    ));
    assert!(frame.len() <= MAX_FRAME_BYTES);
    assert!(frame.len() > MAX_FRAME_BYTES - 97);

    let mut stream = vec![b'x'; MAX_FRAME_BYTES];
    stream.extend(b"\n{}\n");
    let mut exact = Cursor::new(stream);
    frame.clear();
    assert!(read_frame(&mut exact, &mut frame).is_ok());
    assert_eq!(frame.len(), MAX_FRAME_BYTES);
    frame.clear();
    assert!(read_frame(&mut exact, &mut frame).is_ok());
    assert_eq!(frame, b"{}");
}

#[test]
fn rpc_dispatch_uses_shape_and_never_carries_error_details() {
    assert!(matches!(
        parse_frame(br#"{"jsonrpc":"2.0","id":2,"method":"fs/read_text_file","params":{}}"#),
        Ok(Frame::Request { id, .. }) if id == 2
    ));
    assert!(matches!(
        parse_frame(br#"{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"debug-email","data":"secret"}}"#),
        Ok(Frame::Response { id, reply: Err(-32602) }) if id == 2
    ));
    assert!(matches!(
        parse_frame(br#"{"jsonrpc":"2.0","id":2,"method":"session/update","result":{}}"#),
        Err(Fault::InvalidFrame)
    ));
    assert!(matches!(
        parse_frame(br#"{"jsonrpc":"1.0","id":2,"result":{}}"#),
        Err(Fault::InvalidFrame)
    ));
}

struct FailingWrite {
    allowance: usize,
    bytes: Vec<u8>,
}

impl Write for FailingWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.allowance == 0 {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let len = self.allowance.min(bytes.len());
        self.bytes.extend(&bytes[..len]);
        self.allowance -= len;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn writer_distinguishes_prewrite_rejection_from_partial_frame() {
    let closed = AtomicBool::new(false);
    for (allowance, expected) in [(0, WriteFailure::Unwritten), (3, WriteFailure::Unknown)] {
        let mut writer = FailingWrite {
            allowance,
            bytes: Vec::new(),
        };
        assert_eq!(
            write_frame(&mut writer, b"sample\n", None, &closed, &OnceLock::new()),
            Err(expected)
        );
        assert_eq!(writer.bytes, &b"sample\n"[..allowance]);
    }

    let (scope, caller) = watch::channel(());
    let mut output = Vec::new();
    assert_eq!(
        write_frame(
            &mut output,
            b"sample\n",
            Some(Admission {
                guard: AdmissionGuard::new(Duration::ZERO),
                caller: caller.clone(),
            }),
            &closed,
            &OnceLock::new(),
        ),
        Err(WriteFailure::Expired)
    );
    drop(scope);
    assert_eq!(
        write_frame(
            &mut output,
            b"sample\n",
            Some(Admission {
                guard: AdmissionGuard::new(Duration::from_secs(10)),
                caller,
            }),
            &closed,
            &OnceLock::new(),
        ),
        Err(WriteFailure::Expired)
    );
    assert!(output.is_empty());
}

#[test]
fn fenced_payload_write_never_terminates_the_frame() {
    struct FencingWriter<'a> {
        closed: &'a AtomicBool,
        output: Vec<u8>,
    }

    impl Write for FencingWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.output.extend(bytes);
            self.closed.store(true, Ordering::Release);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let closed = AtomicBool::new(false);
    let mut writer = FencingWriter {
        closed: &closed,
        output: Vec::new(),
    };
    assert_eq!(
        write_frame(
            &mut writer,
            b"{\"fixture\":true}\n",
            None,
            &closed,
            &OnceLock::new()
        ),
        Err(WriteFailure::Unknown)
    );
    assert_eq!(writer.output, b"{\"fixture\":true");
}

#[tokio::test]
async fn expired_prompt_answers_a_server_request_received_during_the_write()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::clients::{
        InboundMessage, SubmitOutcome, SubmitTurnRequest, TurnInput, VerifiedAuthor,
        agy::session::Session,
    };
    use cooplet_protocol::{AuthorKind, MessageId};
    use futures_util::FutureExt;

    let (writer, mut writes) = mpsc::channel(1);
    let (frames, reader) = mpsc::channel(1);
    let closed = Arc::new(AtomicBool::new(false));
    let wire = Wire {
        writer,
        reader,
        _keep_faulted_stdout_open: mpsc::channel(1).0,
        closed: closed.clone(),
        next_id: 1,
    };
    let (session, _events) = Session::start(wire, "session".into(), Duration::ZERO);
    let id = MessageId::generate()?;
    let input = TurnInput::direct(InboundMessage {
        message_id: id.clone(),
        author: VerifiedAuthor {
            kind: AuthorKind::Human,
            actor_id: "operator".into(),
            name: "operator".into(),
            source_node_id: None,
        },
        in_reply_to: None,
        body: "hello".into(),
        attachments: Vec::new(),
        question: None,
        answer: None,
        expect_mcp_reply: false,
    });
    let guard = AdmissionGuard::new(Duration::from_secs(10));
    let submit = session.submit(SubmitTurnRequest {
        operation_key: "prompt".into(),
        message_id: Some(id),
        input,
        output_schema: None,
        steer: false,
        admission: guard.clone(),
    });
    tokio::pin!(submit);
    assert!((&mut submit).now_or_never().is_none());
    let prompt = writes.recv().await.ok_or("missing prompt write")?;
    frames
        .send(Ok(ReceivedFrame {
            frame: Frame::Request {
                method: "fixture/request".into(),
                id: json!("server-id"),
            },
            read_at: Instant::now(),
        }))
        .await
        .map_err(|_| "reader closed")?;
    // Capacity becomes available when pump_write consumes the server request.
    drop(frames.reserve().await.map_err(|_| "reader closed")?);
    guard.expire();
    let mut output = Vec::new();
    let outcome = write_frame(
        &mut output,
        &prompt.bytes,
        prompt.admission,
        &closed,
        &prompt.started,
    );
    assert!(output.is_empty());
    prompt.result.send(outcome).map_err(|_| "writer closed")?;
    let response = tokio::time::timeout(Duration::from_secs(1), writes.recv())
        .await?
        .ok_or("server request was dropped")?;
    let frame: Value = serde_json::from_slice(&response.bytes)?;
    assert_eq!(frame["id"], "server-id");
    assert_eq!(frame["error"]["code"], -32601);
    response.result.send(Ok(())).map_err(|_| "writer closed")?;
    assert!(matches!(submit.await, SubmitOutcome::Expired));
    assert!(!session.is_closed());
    Ok(())
}
