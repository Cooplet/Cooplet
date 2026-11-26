mod support;

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, ClientConfig, ClientError, CoopletClient, Destination,
    UploadVerification,
};
use cooplet_protocol::{BlobId, ErrorCode};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use support::*;

const LARGE_SHA: &str = "4b0d375a615c0382b4f958b48e43e7f356b4fcac76e20423294adc07b8d4976e";

fn settings(server: &Server) -> ClientConfig {
    ClientConfig::new(server.url.clone(), Destination::LocalNode, None)
}

fn spec(length: Option<u64>) -> BlobUploadSpec {
    BlobUploadSpec {
        filename: "é 100%.bin".into(),
        mime_type: "application/octet-stream".into(),
        verification: UploadVerification::LocalSha256,
        expected_sha256: None,
        idempotency_key: None,
        content_length: length,
    }
}

#[tokio::test]
async fn local_upload_verifies_chunked_bytes_and_both_empty_forms() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    let serve = async {
        for (size, digest, length) in [
            (256 * 1024, LARGE_SHA, false),
            (0, EMPTY_SHA, true),
            (0, EMPTY_SHA, false),
        ] {
            let (head, mut peer) = server.accept().await?;
            assert_eq!(head.request, "POST /v1/blobs HTTP/1.1");
            assert_eq!(
                head.header("x-cooplet-filename"),
                Some("%C3%A9%20100%25%2Ebin")
            );
            assert_eq!(
                head.header("content-type"),
                Some("application/octet-stream")
            );
            if length {
                assert_eq!(head.header("content-length"), Some("0"));
            } else {
                assert_eq!(head.header("transfer-encoding"), Some("chunked"));
            }
            let body = peer.read_body(&head).await?;
            assert_eq!(body, vec![0x42; size]);
            peer.respond(
                "201 Created",
                &[("content-type", "application/json")],
                receipt(size as u64, digest).as_bytes(),
            )
            .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        for (size, expected, length) in [
            (256 * 1024, LARGE_SHA, None),
            (0, EMPTY_SHA, Some(0)),
            (0, EMPTY_SHA, None),
        ] {
            let result = client
                .upload_blob(spec(length), io::Cursor::new(vec![0x42; size]))
                .await?;
            assert!(
                matches!(result, BlobUploadOutcome::VerifiedLocal(blob) if blob.sha256 == expected && blob.size_bytes == size as u64)
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[derive(Default)]
struct PendingReader(Arc<AtomicUsize>);

impl AsyncRead for PendingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Poll::Pending
    }
}

#[tokio::test]
async fn validates_upload_before_network_and_never_confirms_excess_or_short_input() -> TestResult {
    let server = Server::bind().await?;
    let mut config = settings(&server);
    config.max_blob_bytes = 10;
    let client = CoopletClient::new(config)?;
    let pending = PendingReader::default();
    let polls = pending.0.clone();
    assert!(matches!(
        client.upload_blob(spec(Some(11)), pending).await,
        Err(ClientError::BlobTooLarge { max_bytes: 10 })
    ));
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    for (length, payload, error) in [
        (Some(4), b"hello".as_slice(), "length"),
        (Some(6), b"hello".as_slice(), "length"),
        (None, b"12345678901".as_slice(), "limit"),
    ] {
        let outcome = client.upload_blob(spec(length), payload).await;
        if error == "length" {
            assert!(matches!(
                outcome,
                Err(ClientError::BlobLengthMismatch { .. })
            ));
        } else {
            assert!(matches!(
                outcome,
                Err(ClientError::BlobTooLarge { max_bytes: 10 })
            ));
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), server.listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn only_complete_or_explicitly_acknowledged_uploads_succeed() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    let serve = async {
        for (status, want_key, want_digest) in [
            ("200 OK", false, false),
            ("201 Created", true, true),
            ("200 OK", true, true),
        ] {
            let (head, peer) = server.accept().await?;
            assert_eq!(head.header("content-length"), Some("5"));
            assert_eq!(
                head.header("idempotency-key"),
                want_key.then_some("upload-retry-key")
            );
            assert_eq!(
                head.header("x-cooplet-sha256"),
                want_digest.then_some(HELLO_SHA)
            );
            peer.respond(status, &[], receipt(5, HELLO_SHA).as_bytes())
                .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        for (replay, success) in [(false, false), (true, false), (true, true)] {
            let mut spec = spec(Some(5));
            spec.idempotency_key = replay.then(|| "upload-retry-key".into());
            spec.expected_sha256 = replay.then(|| HELLO_SHA.into());
            let (mut held_open, source) = tokio::io::duplex(32);
            held_open.write_all(b"hello").await?;
            let result = client.upload_blob(spec, source).await;
            if success {
                assert!(
                    matches!(result, Ok(BlobUploadOutcome::AcknowledgedWithoutLocalVerification(blob)) if blob.sha256 == HELLO_SHA)
                );
            } else {
                assert!(matches!(result, Err(ClientError::Protocol(_))));
            }
            drop(held_open);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn upload_progress_can_outlive_json_budget_and_final_response_has_its_own_budget()
-> TestResult {
    let server = Server::bind().await?;
    let mut config = settings(&server);
    config.request_timeout = Duration::from_millis(90);
    let client = CoopletClient::new(config)?;
    let serve = async {
        let (head, mut peer) = server.accept().await?;
        assert_eq!(peer.read_body(&head).await?, b"hello");
        peer.respond("200 OK", &[], receipt(5, HELLO_SHA).as_bytes())
            .await?;
        let (head, mut peer) = server.accept().await?;
        assert_eq!(peer.read_body(&head).await?, b"hello");
        tokio::time::sleep(Duration::from_millis(160)).await;
        drop(peer);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        let (mut writer, reader) = tokio::io::duplex(32);
        let producer = tokio::spawn(async move {
            for byte in b"hello" {
                tokio::time::sleep(Duration::from_millis(30)).await;
                writer.write_all(&[*byte]).await?;
            }
            writer.shutdown().await
        });
        assert!(matches!(
            client.upload_blob(spec(Some(5)), reader).await?,
            BlobUploadOutcome::VerifiedLocal(_)
        ));
        producer.await??;
        assert!(matches!(
            client.upload_blob(spec(Some(5)), &b"hello"[..]).await,
            Err(ClientError::Timeout("blob upload response"))
        ));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn mismatched_receipts_and_server_storage_errors_are_distinct() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    let serve = async {
        for (size, digest) in [(6, HELLO_SHA), (5, EMPTY_SHA)] {
            let (head, mut peer) = server.accept().await?;
            assert_eq!(peer.read_body(&head).await?, b"hello");
            peer.respond("201 Created", &[], receipt(size, digest).as_bytes())
                .await?;
        }
        for (status, numeric, code) in [
            ("413 Content Too Large", 413, "blob_too_large"),
            ("507 Insufficient Storage", 507, "storage_full"),
        ] {
            let (_, peer) = server.accept().await?;
            let problem = format!(
                r#"{{"type":"about:blank","title":"server limit","status":{numeric},"detail":"limit","code":"{code}","retryable":false}}"#
            );
            peer.respond(
                status,
                &[("content-type", "application/problem+json")],
                problem.as_bytes(),
            )
            .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        assert!(matches!(
            client.upload_blob(spec(None), &b"hello"[..]).await,
            Err(ClientError::BlobLengthMismatch {
                expected: 5,
                actual: 6
            })
        ));
        assert!(matches!(
            client.upload_blob(spec(Some(5)), &b"hello"[..]).await,
            Err(ClientError::BlobDigestMismatch { .. })
        ));
        for code in [ErrorCode::BlobTooLarge, ErrorCode::StorageFull] {
            assert!(
                matches!(client.upload_blob(spec(None),PendingReader::default()).await,
                Err(ClientError::Problem(problem)) if problem.code == code)
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn download_requires_external_digest_and_rejects_partial_or_mismatched_content() -> TestResult
{
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    let id: BlobId = BLOB_ID.parse()?;
    let tag = format!("\"{HELLO_SHA}\"");
    let weak = format!("W/{tag}");
    let empty = format!("\"{EMPTY_SHA}\"");
    let serve = async {
        for (headers, bytes) in [
            (
                vec![
                    ("etag", tag.as_str()),
                    ("content-type", "application/octet-stream"),
                ],
                b"hello".as_slice(),
            ),
            (vec![], b"hello".as_slice()),
            (vec![("etag", empty.as_str())], b"".as_slice()),
            (vec![("etag", weak.as_str())], b"hello".as_slice()),
            (vec![("etag", "not-an-etag")], b"hello".as_slice()),
            (vec![], b"hello".as_slice()),
            (vec![("etag", empty.as_str())], b"hello".as_slice()),
            (vec![("etag", tag.as_str())], b"hello".as_slice()),
            (vec![("etag", tag.as_str())], b"jello".as_slice()),
        ] {
            let (head, peer) = server.accept().await?;
            assert_eq!(head.request, format!("GET /v1/blobs/{BLOB_ID} HTTP/1.1"));
            peer.respond("200 OK", &headers, bytes).await?;
        }
        let (_, mut peer) = server.accept().await?;
        peer.start_chunks("application/octet-stream", &[("etag", tag.as_str())])
            .await?;
        peer.chunk(b"hello").await?;
        peer.chunk(b"x").await?;
        peer.raw(b"0\r\n\r\n").await?;
        let (_, mut peer) = server.accept().await?;
        peer.start_chunks("application/octet-stream", &[("etag", tag.as_str())])
            .await?;
        peer.chunk(b"he").await?;
        drop(peer);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        for expected in [None, Some(HELLO_SHA)] {
            let mut sink = Vec::new();
            let downloaded = client
                .download_blob_to_writer(&id, expected, 5, &mut sink)
                .await?;
            assert_eq!(sink, b"hello");
            assert_eq!(downloaded.sha256, HELLO_SHA);
        }
        let mut zero = Vec::new();
        assert_eq!(
            client
                .download_blob_to_writer(&id, None, 0, &mut zero)
                .await?
                .size_bytes,
            0
        );
        for _ in 0..3 {
            assert!(matches!(
                client.open_blob_download(&id, None, 5).await,
                Err(ClientError::Protocol(_))
            ));
        }
        assert!(matches!(
            client.open_blob_download(&id, Some(HELLO_SHA), 5).await,
            Err(ClientError::BlobDigestMismatch { .. })
        ));
        assert!(matches!(
            client.open_blob_download(&id, None, 4).await,
            Err(ClientError::BlobTooLarge { max_bytes: 4 })
        ));
        let mut provisional = Vec::new();
        assert!(matches!(
            client
                .download_blob_to_writer(&id, None, 5, &mut provisional)
                .await,
            Err(ClientError::BlobDigestMismatch { .. })
        ));
        assert_eq!(provisional, b"jello");
        assert!(matches!(
            client
                .download_blob_to_writer(&id, None, 5, &mut Vec::new())
                .await,
            Err(ClientError::BlobTooLarge { max_bytes: 5 })
        ));
        let mut download = client.open_blob_download(&id, None, 5).await?;
        assert!(download.read_to_end(&mut Vec::new()).await.is_err());
        assert!(download.read(&mut [0; 1]).await.is_err());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn declared_empty_upload_waits_for_eof_before_sending_request() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    let (mut input, reader) = tokio::io::duplex(32);
    let uploading = tokio::spawn(async move { client.upload_blob(spec(Some(0)), reader).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), server.listener.accept())
            .await
            .is_err()
    );

    input.shutdown().await?;
    let (head, peer) = server.accept().await?;
    assert_eq!(head.request, "POST /v1/blobs HTTP/1.1");
    assert_eq!(head.header("content-length"), Some("0"));
    peer.respond("201 Created", &[], receipt(0, EMPTY_SHA).as_bytes())
        .await?;
    assert!(
        matches!(uploading.await??, BlobUploadOutcome::VerifiedLocal(blob) if blob.size_bytes == 0)
    );
    Ok(())
}

#[tokio::test]
async fn malformed_mime_parameters_fail_before_reading_source_or_network() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    for mime_type in [
        "text/",
        "text/plain; charset",
        "text/plain;charset=",
        "text/plain; charset=\"unterminated",
        "text/plain; charset =utf-8",
        "text/*",
    ] {
        let mut spec = spec(None);
        spec.mime_type = mime_type.into();
        let source = PendingReader::default();
        let polls = source.0.clone();
        assert!(matches!(
            client.upload_blob(spec, source).await,
            Err(ClientError::InvalidInput(_))
        ));
        assert_eq!(polls.load(Ordering::Relaxed), 0);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), server.listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn valid_http_media_types_keep_ows_and_quoted_parameters() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(settings(&server))?;
    let media_types = [
        "text/plain ; charset=utf-8",
        r#"application/vnd.cooplet+json; ; mode=""; note="a;b=\"z""#,
    ];
    let serve = async {
        for media in media_types {
            let (head, mut peer) = server.accept().await?;
            assert_eq!(head.header("content-type"), Some(media));
            assert_eq!(peer.read_body(&head).await?, b"hello");
            peer.respond("201 Created", &[], receipt(5, HELLO_SHA).as_bytes())
                .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        for media in media_types {
            let mut spec = spec(Some(5));
            spec.mime_type = media.into();
            assert!(matches!(
                client.upload_blob(spec, &b"hello"[..]).await?,
                BlobUploadOutcome::VerifiedLocal(_)
            ));
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}
