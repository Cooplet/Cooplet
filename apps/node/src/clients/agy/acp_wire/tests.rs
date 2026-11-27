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
            write_frame(&mut writer, b"sample\n", None, &closed),
            Err(expected)
        );
        assert_eq!(writer.bytes, &b"sample\n"[..allowance]);
    }

    let (scope, submitting) = watch::channel(());
    let mut output = Vec::new();
    assert_eq!(
        write_frame(
            &mut output,
            b"sample\n",
            Some(Admission {
                guard: AdmissionGuard::new(Duration::ZERO),
                submitting: submitting.clone(),
            }),
            &closed
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
                submitting,
            }),
            &closed
        ),
        Err(WriteFailure::Expired)
    );
    assert!(output.is_empty());
}
