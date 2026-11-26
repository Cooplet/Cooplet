mod support;

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use cooplet_client::{
    BearerToken, ClientConfig, ClientError, CoopletClient, Destination, api_path,
};
use cooplet_protocol::{ErrorCode, HealthResponse, InfoResponse};
use serde::Serialize;
use serde_json::Value;
use url::Url;

use support::*;

fn pinned(url: Url, token: Option<BearerToken>) -> TestResult<CoopletClient> {
    Ok(CoopletClient::new(ClientConfig::new(
        url,
        Destination::Hub {
            expected_hub_id: HUB_ID.parse()?,
        },
        token,
    ))?)
}

struct SecretBody<'a>(&'a AtomicUsize);

impl Serialize for SecretBody<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.fetch_add(1, Ordering::Relaxed);
        serializer.serialize_str("one-shot-token")
    }
}

#[tokio::test]
async fn origin_and_path_validation_precede_serialization_and_network() -> TestResult {
    for (base, destination) in [
        (
            "http://example.org",
            Destination::Hub {
                expected_hub_id: HUB_ID.parse()?,
            },
        ),
        ("https://example.org", Destination::LocalNode),
        ("http://localhost:8080", Destination::LocalNode),
        ("http://user:secret@127.0.0.1", Destination::LocalNode),
        ("http://127.0.0.1/subpath", Destination::LocalNode),
        ("http://127.0.0.1/?query=secret", Destination::LocalNode),
        ("http://127.0.0.1/#fragment", Destination::LocalNode),
    ] {
        assert!(
            CoopletClient::new(ClientConfig::new(Url::parse(base)?, destination, None)).is_err()
        );
    }
    let server = Server::bind().await?;
    let client = pinned(server.url.clone(), Some(BearerToken::new("test-secret")?))?;
    let serialized = AtomicUsize::new(0);
    for route in [
        "http://127.0.0.1/v1/health",
        "//127.0.0.1/v1/health",
        "/v1/../health",
        "/v1/%2e%2e/health",
        "/v1/n%2fhealth",
        "/v1/n%5chealth",
        "/v1/%252e",
        "/v1//health",
        "/v1/health?query=1",
        "/v1/health#fragment",
        "/v1/health%not",
        "/v1/health\\path",
        "/v2/health",
    ] {
        let result = client
            .post_json::<_, Value>(route, &SecretBody(&serialized), None)
            .await;
        assert!(
            matches!(result, Err(ClientError::InvalidInput(_))),
            "{route}"
        );
    }
    assert!(matches!(
        client
            .post_json::<_, Value>("/v1/enroll", &SecretBody(&serialized), Some("short"))
            .await,
        Err(ClientError::InvalidInput(_))
    ));
    assert_eq!(serialized.load(Ordering::Relaxed), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), server.listener.accept())
            .await
            .is_err()
    );
    assert_eq!(api_path(&["a", "é +"])?, "/v1/a/%C3%A9%20%2B");
    assert!(api_path(&["a", "/b"]).is_err());
    Ok(())
}

#[tokio::test]
async fn bad_hub_info_never_serializes_or_sends_secrets() -> TestResult {
    let other_hub =
        format!(r#"{{"hub_id":"{OTHER_HUB_ID}","display_name":"Wrong Hub","protocol_version":1}}"#);
    let fixtures = [
        (Some(BearerToken::new("test-secret")?), other_hub.as_bytes(), true),
        (None, NODE_INFO, true),
        (None, br#"{"hub_id":"hub_00000000000000000000000001","node_id":"node_00000000000000000000000001","display_name":"Bad","protocol_version":1}"#.as_slice(), false),
        (None, br#"{"display_name":"Bad","protocol_version":1}"#.as_slice(), false),
        (None, br#"{"hub_id":"hub_00000000000000000000000001","display_name":"Bad","protocol_version":1,"extra":1}"#.as_slice(), false),
    ];
    for (token, body, mismatch) in fixtures {
        let server = Server::bind().await?;
        let client = pinned(server.url.clone(), token)?;
        let serialized = AtomicUsize::new(0);
        let serve = async {
            let (head, peer) = server.accept().await?;
            assert_eq!(head.request, "GET /v1/info HTTP/1.1");
            assert_eq!(head.count("authorization"), 0);
            assert_eq!(head.count("x-cooplet-hub"), 0);
            peer.respond("200 OK", &[("Content-Type", "application/json")], body)
                .await?;
            assert!(
                tokio::time::timeout(Duration::from_millis(20), server.listener.accept())
                    .await
                    .is_err()
            );
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let body = SecretBody(&serialized);
        let (result, server_result) = tokio::join!(
            client.post_json::<_, Value>("/v1/enroll", &body, None),
            serve
        );
        server_result?;
        assert_eq!(serialized.load(Ordering::Relaxed), 0);
        if mismatch {
            assert!(matches!(
                result,
                Err(ClientError::HubIdentityMismatch { .. })
            ));
        } else {
            assert!(matches!(result, Err(ClientError::Protocol(_))));
        }
    }
    Ok(())
}

#[tokio::test]
async fn overlapping_calls_pin_once_and_local_node_never_gets_hub_header() -> TestResult {
    let server = Server::bind().await?;
    let client = pinned(server.url.clone(), Some(BearerToken::new("test-secret")?))?;
    let serve = async {
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        peer.respond("200 OK", &[], HUB_INFO).await?;
        for _ in 0..2 {
            let (head, peer) = server.accept().await?;
            assert_eq!(head.request, "GET /v1/health HTTP/1.1");
            assert_eq!(head.count("x-cooplet-hub"), 1);
            assert_eq!(head.header("x-cooplet-hub"), Some(HUB_ID));
            assert_eq!(head.header("authorization"), Some("Bearer test-secret"));
            peer.respond("200 OK", &[], br#"{"status":"ok"}"#).await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b, served) = tokio::join!(client.health(), client.health(), serve);
    served?;
    assert_eq!(a?.status, "ok");
    assert_eq!(b?.status, "ok");

    let local = Server::bind().await?;
    let client = CoopletClient::new(ClientConfig::new(
        local.url.clone(),
        Destination::LocalNode,
        Some(BearerToken::new("local-token")?),
    ))?;
    let serve = async {
        let (head, peer) = local.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        assert_eq!(head.count("authorization"), 0);
        peer.respond("200 OK", &[], NODE_INFO).await?;
        let (head, mut peer) = local.accept().await?;
        assert_eq!(head.request, "PUT /v1/a/%C3%A9 HTTP/1.1");
        assert_eq!(head.count("x-cooplet-hub"), 0);
        assert_eq!(head.header("authorization"), Some("Bearer local-token"));
        assert_eq!(head.header("idempotency-key"), Some("request-key"));
        assert_eq!(peer.read_body(&head).await?, br#""body""#);
        peer.respond("200 OK", &[], br#"{"status":"ok"}"#).await
    };
    let call = async {
        assert!(matches!(client.info().await?, InfoResponse::Node(_)));
        let response: HealthResponse = client
            .put_json(&api_path(&["a", "é"])?, "body", Some("request-key"))
            .await?;
        assert_eq!(response.status, "ok");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn redirect_is_terminal_and_failed_preflight_is_not_cached() -> TestResult {
    let server = Server::bind().await?;
    let target = Server::bind().await?;
    let mut config = ClientConfig::new(
        server.url.clone(),
        Destination::Hub {
            expected_hub_id: HUB_ID.parse()?,
        },
        Some(BearerToken::new("secret")?),
    );
    config.request_timeout = Duration::from_millis(80);
    let client = CoopletClient::new(config)?;
    let serve = async {
        let (head, first_peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        drop(first_peer);
        peer.respond("200 OK", &[], HUB_INFO).await?;
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "POST /v1/enroll HTTP/1.1");
        peer.respond("302 Found", &[("location", target.url.as_str())], b"secret")
            .await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), target.listener.accept())
                .await
                .is_err()
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        assert!(matches!(
            client.verify_hub_identity().await,
            Err(ClientError::Timeout(_))
        ));
        let result = client
            .post_json::<_, Value>("/v1/enroll", "one-shot-secret", None)
            .await;
        let error = result.err().ok_or("redirect was accepted")?;
        assert!(matches!(
            error,
            ClientError::UnexpectedStatus { status: 302, .. }
        ));
        assert!(!format!("{error:?}").contains("secret"));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn problem_details_are_structured_and_their_display_is_safe() -> TestResult {
    type Fixture = (
        &'static str,
        &'static [(&'static str, &'static str)],
        &'static [u8],
    );
    let server = Server::bind().await?;
    let client = CoopletClient::new(ClientConfig::new(
        server.url.clone(),
        Destination::LocalNode,
        None,
    ))?;
    let serve = async {
        let fixtures: &[Fixture] = &[
            ("409 Conflict", &[("Content-Type", "application/problem+json; charset=utf-8")],
             br#"{"type":"https://cooplet.dev/problems/pin-conflict","title":"Pin conflict","status":409,"detail":"test-secret","code":"pin_conflict","retryable":true,"extensions":{"pin_version":3},"trace_id":"ignored"}"#),
            ("410 Gone", &[],
             br#"{"type":"https://cooplet.dev/problems/resync-required","title":"Resync required","status":410,"detail":"expired","code":"resync_required","retryable":false,"extensions":{"floor_seq":42}}"#),
            ("502 Bad Gateway", &[], b"test-secret from proxy"),
            ("502 Bad Gateway", &[("Content-Type", "application/problem+json")], b"not JSON"),
        ];
        for (status, headers, body) in fixtures {
            let (_, peer) = server.accept().await?;
            peer.respond(status, headers, body).await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        let first = client
            .get_json::<Value>("/v1/pins", &[])
            .await
            .err()
            .ok_or("missing problem")?;
        assert!(!format!("{first:?}").contains("test-secret"));
        assert!(matches!(first, ClientError::Problem(problem)
            if problem.code == ErrorCode::PinConflict && problem.retryable && problem.extensions["pin_version"] == 3));
        assert!(
            matches!(client.get_json::<Value>("/v1/thread_read",&[]).await,
            Err(ClientError::Problem(problem)) if problem.extensions["floor_seq"] == 42)
        );
        assert!(matches!(client.get_json::<Value>("/v1/health",&[]).await,
            Err(ClientError::UnexpectedStatus { status:502, body_preview }) if body_preview.is_empty()));
        assert!(matches!(
            client.get_json::<Value>("/v1/health", &[]).await,
            Err(ClientError::Protocol(_))
        ));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}
