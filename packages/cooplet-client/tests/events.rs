mod support;

use std::time::Duration;

use cooplet_client::{ClientConfig, ClientError, CoopletClient, Destination, EventsQuery};
use cooplet_protocol::{EventKind, EventStreamFrame};

use support::*;

const RESYNC: &[u8] = br#"{"type":"https://cooplet.dev/problems/resync-required","title":"Resync required","status":410,"detail":"cursor expired","code":"resync_required","retryable":false,"extensions":{"oldest_available_seq":41,"latest_seq":99},"trace_id":"t-1"}"#;
const EVENT: &[u8] = br#"{"type":"event","event":{"seq":53,"event_id":"evt_00000000000000000000000001","source_kind":"node","source_id":"node_00000000000000000000000001","kind":"node.status","aggregate_type":"node","aggregate_id":"node_00000000000000000000000001","actor_id":null,"created_at_ms":12,"payload":{}}}"#;

#[tokio::test]
async fn list_returns_filtered_watermark_and_events_map_only_canonical_410() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(ClientConfig::new(
        server.url.clone(),
        Destination::LocalNode,
        None,
    ))?;
    let serve = async {
        let (head, peer) = server.accept().await?;
        assert_eq!(
            head.request,
            "GET /v1/events?after=12&kinds=node.status%2Cmessage.created&limit=2 HTTP/1.1"
        );
        peer.respond("200 OK", &[], br#"{"items":[],"next_seq":96,"oldest_available_seq":1,"latest_seq":99,"has_more":true}"#).await?;
        for _ in 0..2 {
            let (head, peer) = server.accept().await?;
            assert!(head.request.starts_with("GET /v1/events"));
            peer.respond(
                "410 Gone",
                &[("content-type", "application/problem+json")],
                RESYNC,
            )
            .await?;
        }
        let (_, peer) = server.accept().await?;
        peer.respond("410 Gone", &[("content-type", "application/problem+json")],
            br#"{"type":"about:blank","title":"Gone","status":410,"detail":"expired","code":"resync_required","retryable":false,"extensions":{"latest_seq":99}}"#).await
    };
    let call = async {
        assert!(matches!(
            client
                .list_events(&EventsQuery {
                    limit: Some(501),
                    ..Default::default()
                })
                .await,
            Err(ClientError::InvalidInput(_))
        ));
        let page = client
            .list_events(&EventsQuery {
                after: 12,
                limit: Some(2),
                kinds: vec![EventKind::NodeStatus, EventKind::MessageCreated],
            })
            .await?;
        assert_eq!(page.next_seq, 96);
        assert!(page.items.is_empty());
        for error in [
            client.list_events(&EventsQuery::default()).await.err(),
            client.stream_events(0, &[]).await.err(),
        ] {
            assert!(matches!(error, Some(ClientError::ResyncRequired {
                oldest_available_seq: 41, latest_seq:99, reason
            }) if reason == "cursor expired"));
        }
        assert!(matches!(
            client.list_events(&EventsQuery::default()).await,
            Err(ClientError::Protocol(_))
        ));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn invalid_events_fail_the_page_and_terminate_the_stream() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(ClientConfig::new(
        server.url.clone(),
        Destination::LocalNode,
        None,
    ))?;
    let frame: serde_json::Value = serde_json::from_slice(EVENT)?;
    let valid = &frame["event"];
    let mut wrong_source = valid.clone();
    wrong_source["source_kind"] = "hub".into();
    let mut missing_approval_id = valid.clone();
    missing_approval_id["kind"] = "participant.awaiting_approval".into();
    missing_approval_id["payload"] = serde_json::json!({
        "state": "awaiting_approval", "can_respond_remotely": true
    });

    for bad in [wrong_source, missing_approval_id] {
        let serve = async {
            let (_, peer) = server.accept().await?;
            let page = serde_json::json!({
                "items": [valid, bad], "next_seq": 54, "oldest_available_seq": 1,
                "latest_seq": 54, "has_more": false
            });
            peer.respond("200 OK", &[], &serde_json::to_vec(&page)?)
                .await?;
            let (_, peer) = server.accept().await?;
            let bad_frame = serde_json::json!({"type": "event", "event": bad});
            let mut stream = serde_json::to_vec(&bad_frame)?;
            stream.extend_from_slice(b"\n{\"type\":\"ping\"}\n");
            peer.respond(
                "200 OK",
                &[("content-type", "application/x-ndjson")],
                &stream,
            )
            .await
        };
        let call = async {
            assert!(matches!(
                client.list_events(&EventsQuery::default()).await,
                Err(ClientError::Protocol(_))
            ));
            let mut stream = client.stream_events(0, &[]).await?;
            assert!(matches!(
                stream.next_frame().await,
                Some(Err(ClientError::Protocol(_)))
            ));
            assert!(stream.next_frame().await.is_none());
            Ok::<_, ClientError>(())
        };
        let (served, result) = tokio::join!(serve, call);
        served?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn long_stream_keeps_fragmented_ping_and_event_until_terminal_resync() -> TestResult {
    let server = Server::bind().await?;
    let mut config = ClientConfig::new(server.url.clone(), Destination::LocalNode, None);
    config.request_timeout = Duration::from_millis(60);
    let client = CoopletClient::new(config)?;
    let serve = async {
        let (head, mut peer) = server.accept().await?;
        assert_eq!(
            head.request,
            "GET /v1/events/stream?after=11&kinds=node.status HTTP/1.1"
        );
        assert_eq!(head.header("accept"), Some("application/x-ndjson"));
        peer.start_chunks("application/x-ndjson; charset=utf-8", &[])
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        for byte in b"\r\n{\"type\":\"ping\"}\r\n" {
            peer.chunk(&[*byte]).await?;
        }
        let mut combined = EVENT.to_vec();
        combined.extend_from_slice(b"\n{\"type\":\"resync_required\",\"oldest_available_seq\":41,\"latest_seq\":99,\"reason\":\"cursor expired\"}\n{\"type\":\"ping\"}\n");
        peer.chunk(&combined).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        let mut stream = client.stream_events(11, &[EventKind::NodeStatus]).await?;
        assert!(matches!(
            stream.next_frame().await,
            Some(Ok(EventStreamFrame::Ping {}))
        ));
        assert!(
            matches!(stream.next_frame().await, Some(Ok(EventStreamFrame::Event { event })) if event.seq == 53)
        );
        assert!(
            matches!(stream.next_frame().await, Some(Err(ClientError::ResyncRequired {
            oldest_available_seq:41, latest_seq:99, reason,
        })) if reason == "cursor expired")
        );
        assert!(stream.next_frame().await.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn invalid_stream_shapes_fail_once_and_do_not_resynchronize_silently() -> TestResult {
    let server = Server::bind().await?;
    let mut config = ClientConfig::new(server.url.clone(), Destination::LocalNode, None);
    config.max_ndjson_line_bytes = 30;
    let client = CoopletClient::new(config)?;
    let serve = async {
        for (content_type, body) in [
            ("application/json", b"{}".as_slice()),
            (
                "application/x-ndjson",
                b"{\"type\":\"ping\",\"extra\":1}\n".as_slice(),
            ),
            ("application/x-ndjson", b"{\"type\":\"ping\"}".as_slice()),
            (
                "application/x-ndjson",
                b"0123456789012345678901234567890\n".as_slice(),
            ),
        ] {
            let (_, peer) = server.accept().await?;
            peer.respond("200 OK", &[("content-type", content_type)], body)
                .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        assert!(matches!(
            client.stream_events(0, &[]).await,
            Err(ClientError::Protocol(_))
        ));
        for _ in 0..3 {
            let mut stream = client.stream_events(0, &[]).await?;
            assert!(matches!(
                stream.next_frame().await,
                Some(Err(ClientError::Protocol(_)))
            ));
            assert!(stream.next_frame().await.is_none());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn both_resync_forms_reject_zero_floor_but_admit_an_empty_journal() -> TestResult {
    let server = Server::bind().await?;
    let client = CoopletClient::new(ClientConfig::new(
        server.url.clone(),
        Destination::LocalNode,
        None,
    ))?;
    let serve = async {
        let (_, peer) = server.accept().await?;
        peer.respond("410 Gone", &[("content-type", "application/problem+json")],
            br#"{"type":"about:blank","title":"Gone","status":410,"detail":"expired","code":"resync_required","retryable":false,"extensions":{"oldest_available_seq":0,"latest_seq":0}}"#).await?;
        let (_, peer) = server.accept().await?;
        peer.respond("200 OK", &[("content-type", "application/x-ndjson")],
            b"{\"type\":\"resync_required\",\"oldest_available_seq\":0,\"latest_seq\":0,\"reason\":\"expired\"}\n").await?;
        let (_, peer) = server.accept().await?;
        peer.respond("200 OK", &[("content-type", "application/x-ndjson")],
            b"{\"type\":\"resync_required\",\"oldest_available_seq\":1,\"latest_seq\":0,\"reason\":\"expired\"}\n").await
    };
    let call = async {
        assert!(matches!(
            client.list_events(&EventsQuery::default()).await,
            Err(ClientError::Protocol(_))
        ));
        let mut invalid = client.stream_events(0, &[]).await?;
        assert!(matches!(
            invalid.next_frame().await,
            Some(Err(ClientError::Protocol(_)))
        ));
        let mut empty = client.stream_events(0, &[]).await?;
        assert!(matches!(
            empty.next_frame().await,
            Some(Err(ClientError::ResyncRequired {
                oldest_available_seq: 1,
                latest_seq: 0,
                ..
            }))
        ));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}
