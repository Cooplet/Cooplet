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
        (None, br#"{"hub_id":"hub_00000000000000000000000001","display_name":"Hub","protocol_version":2}"#.as_slice(), false),
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
async fn enrollment_sends_one_pinned_anonymous_post_without_following_redirects() -> TestResult {
    let server = Server::bind().await?;
    let redirected = Server::bind().await?;
    let hub_id = HUB_ID.parse()?;
    let serialized = AtomicUsize::new(0);
    let body = SecretBody(&serialized);
    let request =
        CoopletClient::enroll::<_, Value>(&server.url, &hub_id, &[], Duration::from_secs(1), &body);
    let serve = async {
        let (head, mut peer) = server.accept().await?;
        assert_eq!(head.request, "POST /v1/enrollment/token HTTP/1.1");
        assert_eq!(head.count("authorization"), 0);
        assert_eq!(head.count("x-cooplet-hub"), 1);
        assert_eq!(head.header("x-cooplet-hub"), Some(HUB_ID));
        assert_eq!(peer.read_body(&head).await?, br#""one-shot-token""#);
        peer.respond(
            "307 Temporary Redirect",
            &[("location", redirected.url.as_str())],
            &[],
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (response, served) = tokio::join!(request, serve);
    served?;
    assert!(matches!(
        response,
        Err(ClientError::UnexpectedStatus { status: 307 })
    ));
    assert_eq!(serialized.load(Ordering::Relaxed), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), redirected.listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn separate_transport_checks_the_pin_even_when_http_has_cached_it() -> TestResult {
    let server = Server::bind().await?;
    let client = pinned(server.url.clone(), Some(BearerToken::new("test-secret")?))?;
    let serve = async {
        for body in [HUB_INFO, NODE_INFO] {
            let (head, peer) = server.accept().await?;
            assert_eq!(head.request, "GET /v1/info HTTP/1.1");
            assert_eq!(head.count("authorization"), 0);
            assert_eq!(head.count("x-cooplet-hub"), 0);
            peer.respond("200 OK", &[], body).await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let check = async {
        client.verify_hub_identity().await?;
        client
            .verify_hub_identity_with_timeout(Duration::from_secs(2))
            .await
    };
    let (checked, served) =
        tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(check, serve) }).await?;
    served?;
    assert!(matches!(
        checked,
        Err(ClientError::HubIdentityMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn cancelling_hub_waiters_releases_only_abandoned_probes() -> TestResult {
    let server = Server::bind().await?;
    let client = pinned(server.url.clone(), Some(BearerToken::new("test-secret")?))?;
    let mut abandoned = Box::pin(client.verify_hub_identity());
    let (head, held_open) = tokio::select! {
        biased;
        _ = &mut abandoned => return Err("preflight completed before the reply".into()),
        accepted = server.accept() => accepted?,
    };
    assert_eq!(head.request, "GET /v1/info HTTP/1.1");
    drop(abandoned);

    let mut leader = Box::pin(client.verify_hub_identity());
    let mut waiter = Box::pin(client.verify_hub_identity());
    let (head, peer) = tokio::select! {
        biased;
        _ = &mut leader => return Err("preflight completed before the reply".into()),
        _ = &mut waiter => return Err("preflight completed before the reply".into()),
        accepted = server.accept() => accepted?,
    };
    assert_eq!(head.request, "GET /v1/info HTTP/1.1");
    assert_eq!(head.count("authorization"), 0);
    drop(leader);
    peer.respond("200 OK", &[], NODE_INFO).await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), waiter).await?,
        Err(ClientError::HubIdentityMismatch { actual: None, .. })
    ));
    drop(held_open);
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
        let (first, second, third) = tokio::join!(
            client.verify_hub_identity(),
            client.verify_hub_identity(),
            client.verify_hub_identity()
        );
        for failed in [first, second, third] {
            assert!(matches!(failed, Err(ClientError::Timeout(_))));
        }
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
        assert!(matches!(
            client.get_json::<Value>("/v1/health", &[]).await,
            Err(ClientError::UnexpectedStatus { status: 502 })
        ));
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

#[tokio::test]
async fn post_pin_hub_mismatch_invalidates_pin_before_next_protected_call() -> TestResult {
    let server = Server::bind().await?;
    let client = pinned(server.url.clone(), Some(BearerToken::new("top-secret")?))?;
    let mismatch_problem = format!(
        r#"{{"type":"https://cooplet.dev/problems/hub-identity-mismatch","title":"Hub mismatch","status":409,"detail":"restarted","code":"hub_identity_mismatch","retryable":false,"extensions":{{"actual_hub_id":"{OTHER_HUB_ID}"}}}}"#
    );
    let other_hub_info =
        format!(r#"{{"hub_id":"{OTHER_HUB_ID}","display_name":"New Hub","protocol_version":1}}"#);
    let serve = async {
        // Initial pin succeeds.
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        peer.respond("200 OK", &[], HUB_INFO).await?;

        // Next call hits restarted hub -> 409 hub_identity_mismatch must reset pin!
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/health HTTP/1.1");
        assert_eq!(head.header("x-cooplet-hub"), Some(HUB_ID));
        peer.respond(
            "409 Conflict",
            &[("content-type", "application/problem+json")],
            mismatch_problem.as_bytes(),
        )
        .await?;

        // Subsequent protected POST must re-check /v1/info with NO Authorization and NEVER receive the POST!
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        assert_eq!(head.count("authorization"), 0);
        assert_eq!(head.count("x-cooplet-hub"), 0);
        peer.respond("200 OK", &[], other_hub_info.as_bytes())
            .await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.listener.accept())
                .await
                .is_err()
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let serialized = AtomicUsize::new(0);
    let call = async {
        let err = client.health().await.err().ok_or("expected 409 mismatch")?;
        assert!(matches!(
            err,
            ClientError::HubIdentityMismatch {
                actual: Some(ref actual),
                ..
            } if actual == OTHER_HUB_ID
        ));
        let post_err = client
            .post_json::<_, Value>("/v1/enroll", &SecretBody(&serialized), None)
            .await
            .err()
            .ok_or("expected post-reset mismatch")?;
        assert!(matches!(
            post_err,
            ClientError::HubIdentityMismatch {
                actual: Some(ref actual),
                ..
            } if actual == OTHER_HUB_ID
        ));
        assert_eq!(serialized.load(Ordering::Relaxed), 0);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}

#[tokio::test]
async fn patch_delete_discover_whoami_token_and_sanitized_errors() -> TestResult {
    // BearerToken validation and redaction.
    assert!(BearerToken::new("").is_err());
    assert!(BearerToken::new("has space").is_err());
    assert!(BearerToken::new("bad\nline").is_err());
    let token = BearerToken::new("my-secret-token")?;
    assert_eq!(format!("{token:?}"), "BearerToken([REDACTED])");

    // Invalid or empty root certificate PEM (and malformed DER inside PEM) is rejected at construction.
    let server = Server::bind().await?;
    for bad in [
        b"not a pem cert".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".as_slice(),
    ] {
        let mut bad_pem = ClientConfig::new(server.url.clone(), Destination::LocalNode, None);
        bad_pem.root_certificates_pem.push(bad.to_vec());
        assert!(matches!(
            CoopletClient::new(bad_pem),
            Err(ClientError::InvalidInput(_))
        ));
    }

    let client = CoopletClient::new(ClientConfig::new(
        server.url.clone(),
        Destination::LocalNode,
        Some(token),
    ))?;
    let serve = async {
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        assert_eq!(head.count("authorization"), 0);
        peer.respond("200 OK", &[], HUB_INFO).await?;

        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/whoami HTTP/1.1");
        assert_eq!(head.header("authorization"), Some("Bearer my-secret-token"));
        peer.respond(
            "200 OK",
            &[],
            br#"{"actor_id":"sp_00000000000000000000000001","principal_kind":"service","node_id":null,"participant_id":null,"name":"bot","scopes":["read"]}"#,
        )
        .await?;

        let (head, mut peer) = server.accept().await?;
        assert_eq!(head.request, "PATCH /v1/threads/t1 HTTP/1.1");
        assert_eq!(peer.read_body(&head).await?, br#"{"title":"hi"}"#);
        peer.respond("200 OK", &[], br#"{"status":"ok"}"#).await?;

        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "DELETE /v1/threads/t1 HTTP/1.1");
        peer.respond("200 OK", &[], br#"{"deleted":true}"#).await?;

        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "DELETE /v1/pins/p1 HTTP/1.1");
        peer.respond("204 No Content", &[], b"").await?;

        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "DELETE /v1/pins/p2 HTTP/1.1");
        peer.respond("200 OK", &[], b"unexpected").await
    };
    let call = async {
        assert!(matches!(
            CoopletClient::discover_info(&server.url).await?,
            InfoResponse::Hub(_)
        ));
        let who = client.whoami().await?;
        assert_eq!(who.name, "bot");
        let patched: HealthResponse = client
            .patch_json("/v1/threads/t1", &serde_json::json!({"title": "hi"}), None)
            .await?;
        assert_eq!(patched.status, "ok");
        let deleted: Value = client.delete_json("/v1/threads/t1", None).await?;
        assert_eq!(deleted["deleted"], true);
        client.delete_no_content("/v1/pins/p1", None).await?;
        assert!(matches!(
            client.delete_no_content("/v1/pins/p2", None).await,
            Err(ClientError::Protocol(_))
        ));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b?;

    // Closed server produces a Transport error whose Display includes the cause without secrets.
    let closed_url = server.url.clone();
    drop(server);
    let err = client
        .health()
        .await
        .err()
        .ok_or("expected transport error")?;
    assert!(matches!(err, ClientError::Transport(_)));
    let rendered = format!("{err}");
    assert!(rendered.starts_with("HTTP transport failed: "));
    assert!(rendered.len() > "HTTP transport failed: ".len());
    assert!(!rendered.contains("my-secret-token"));
    assert!(!rendered.contains(closed_url.as_str()));

    // discover_info_with_roots validates custom CA PEM before network I/O.
    assert!(matches!(
        CoopletClient::discover_info_with_roots(&closed_url, &[b"not a pem".to_vec()]).await,
        Err(ClientError::InvalidInput(_))
    ));
    Ok(())
}

#[tokio::test]
async fn stale_epoch_hub_mismatch_does_not_invalidate_newer_pin_verification() -> TestResult {
    let server = Server::bind().await?;
    let client = pinned(server.url.clone(), Some(BearerToken::new("top-secret")?))?;
    let mismatch_problem = format!(
        r#"{{"type":"https://cooplet.dev/problems/hub-identity-mismatch","title":"Hub mismatch","status":409,"detail":"restarted","code":"hub_identity_mismatch","retryable":false,"extensions":{{"actual_hub_id":"{OTHER_HUB_ID}"}}}}"#
    );
    let (stale_accepted_tx, stale_accepted_rx) = tokio::sync::oneshot::channel();
    let (reverified_tx, reverified_rx) = tokio::sync::oneshot::channel();
    let serve = async {
        // Epoch 1 pin verification.
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        peer.respond("200 OK", &[], HUB_INFO).await?;

        // Req 1 enters under epoch 1 and stays open until epoch 2 is verified.
        let (head, stale_peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/health HTTP/1.1");
        let _ = stale_accepted_tx.send(());

        // Req 2 also uses epoch 1 and receives 409, invalidating epoch 1.
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/health HTTP/1.1");
        peer.respond(
            "409 Conflict",
            &[("content-type", "application/problem+json")],
            mismatch_problem.as_bytes(),
        )
        .await?;

        // Req 3 re-verifies under epoch 2 and succeeds.
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/info HTTP/1.1");
        peer.respond("200 OK", &[], HUB_INFO).await?;
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/health HTTP/1.1");
        peer.respond("200 OK", &[], br#"{"status":"ok"}"#).await?;

        // Now deliver the delayed 409 to Req 1 (stale epoch 1).
        let _ = reverified_rx.await;
        stale_peer
            .respond(
                "409 Conflict",
                &[("content-type", "application/problem+json")],
                mismatch_problem.as_bytes(),
            )
            .await?;

        // Req 4 must keep epoch 2 and go straight to /v1/health without another /v1/info.
        let (head, peer) = server.accept().await?;
        assert_eq!(head.request, "GET /v1/health HTTP/1.1");
        peer.respond("200 OK", &[], br#"{"status":"ok"}"#).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let call = async {
        client.verify_hub_identity().await?;
        let stale_client = client.clone();
        let req1 = tokio::spawn(async move { stale_client.health().await });
        let _ = stale_accepted_rx.await;

        assert!(matches!(
            client.health().await,
            Err(ClientError::HubIdentityMismatch { .. })
        ));
        assert_eq!(client.health().await?.status, "ok");
        let _ = reverified_tx.send(());
        assert!(matches!(
            req1.await?,
            Err(ClientError::HubIdentityMismatch { .. })
        ));
        assert_eq!(client.health().await?.status, "ok");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (a, b) = tokio::join!(call, serve);
    a?;
    b
}
