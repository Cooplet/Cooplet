#[path = "http/bridge.rs"]
mod bridge;
mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, CoopletClient, EventsQuery, UploadVerification,
};
use cooplet_protocol::{
    AttachmentRef, BlobId, ErrorCode, EventStreamFrame, HubId, ParticipantId, PrincipalKind,
};
use serde_json::{Value, json};
use support::{Hub, TestResult, expect_problem, hub_command, issue_enrollment, redeem, string};
use tokio::time::timeout;

#[tokio::test]
async fn bootstrap_enrollment_and_admin_boundary() -> TestResult {
    let mut hub = Hub::start().await?;
    let admin = hub.admin()?;
    let empty = hub.client(None)?;
    let who = admin.whoami().await?;
    assert_eq!(who.principal_kind, PrincipalKind::HumanAdmin);

    let cli_openapi = hub_command().arg("openapi").output()?;
    assert!(cli_openapi.status.success());
    let http_openapi = hub.raw("GET", "/v1/openapi.json", &[], &[]).await?;
    assert_eq!(http_openapi.status, 200);
    assert_eq!(cli_openapi.stdout, http_openapi.body);

    let wrong_id = HubId::generate()?;
    let mismatch_raw = hub
        .raw(
            "GET",
            "/v1/health",
            &[
                ("X-Cooplet-Hub", wrong_id.as_str()),
                ("Authorization", "Bearer invalid"),
            ],
            &[],
        )
        .await?;
    let mismatch = mismatch_raw.problem(409, ErrorCode::HubIdentityMismatch)?;
    assert_eq!(mismatch.extensions["actual_hub_id"], hub.id.as_str());

    let auth_header = format!("Bearer {}", hub.secret);
    let duplicate = hub
        .raw(
            "GET",
            "/v1/health",
            &[
                ("X-Cooplet-Hub", hub.id.as_str()),
                ("Authorization", &auth_header),
                ("Authorization", &auth_header),
            ],
            &[],
        )
        .await?;
    duplicate.problem(401, ErrorCode::Unauthorized)?;
    assert_eq!(
        duplicate
            .headers
            .get(http::header::WWW_AUTHENTICATE)
            .ok_or("missing Bearer challenge")?,
        "Bearer realm=\"cooplet-hub\"",
    );
    let standard_bearer = format!("bearer  {}", hub.secret);
    let health = hub
        .raw(
            "GET",
            "/v1/health",
            &[
                ("X-Cooplet-Hub", hub.id.as_str()),
                ("Authorization", &standard_bearer),
            ],
            &[],
        )
        .await?;
    assert_eq!(health.status, 200);

    let original = hub.issue_enrollment_cli("Test Node", "first-enrollment")?;
    let cli_replay = hub_command()
        .args(["enrollment-token", "create", "--hub", hub.origin.as_str()])
        .args(["--hub-id", hub.id.as_str(), "--token-file"])
        .arg(hub.admin_token_path())
        .args([
            "--name",
            "Test Node",
            "--idempotency-key",
            "first-enrollment",
        ])
        .output()?;
    assert!(cli_replay.status.success(), "CLI enrollment replay failed");
    let cli_replay_json: Value = serde_json::from_slice(&cli_replay.stdout)?;
    assert_eq!(cli_replay_json["already_issued"], true);
    assert!(cli_replay_json.get("token").is_none());
    assert_eq!(cli_replay_json["token_id"], original.token_id);

    let replay: Value = admin
        .post_json(
            "/v1/enrollment-tokens",
            &json!({"name":"Test Node","ttl_ms":900_000}),
            Some("first-enrollment"),
        )
        .await?;
    assert_eq!(replay["node_id"], original.node_id.as_str());
    assert_eq!(replay["token_id"], original.token_id);
    assert_eq!(replay["already_issued"], true);
    assert!(replay.get("token").is_none());

    // Public and error responses always carry X-Cooplet-Hub and Cache-Control: no-store.
    for response in [&http_openapi, &health, &mismatch_raw, &duplicate] {
        assert_eq!(
            response
                .headers
                .get("x-cooplet-hub")
                .and_then(|v| v.to_str().ok()),
            Some(hub.id.as_str())
        );
        assert_eq!(
            response
                .headers
                .get(http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
    }

    // Duplicate Authorization on public /v1/info and any Authorization on /v1/enrollment/token
    // return 401 without WWW-Authenticate.
    let info_dup_auth = hub
        .raw(
            "GET",
            "/v1/info",
            &[
                ("Authorization", &auth_header),
                ("Authorization", &auth_header),
            ],
            &[],
        )
        .await?;
    info_dup_auth.problem(401, ErrorCode::Unauthorized)?;
    assert!(
        info_dup_auth
            .headers
            .get(http::header::WWW_AUTHENTICATE)
            .is_none()
    );
    let enroll_with_auth = hub
        .raw(
            "POST",
            "/v1/enrollment/token",
            &[
                ("X-Cooplet-Hub", hub.id.as_str()),
                ("Authorization", &auth_header),
                ("Content-Type", "application/json"),
                ("Content-Length", "2"),
            ],
            b"{}",
        )
        .await?;
    enroll_with_auth.problem(401, ErrorCode::Unauthorized)?;
    assert!(
        enroll_with_auth
            .headers
            .get(http::header::WWW_AUTHENTICATE)
            .is_none()
    );

    // 404 and 405 fallbacks return matching ProblemDetails via CoopletClient.
    expect_problem(
        admin.get_json::<Value>("/v1/does-not-exist", &[]).await,
        ErrorCode::NotFound,
    )?;
    let method_err = admin
        .post_json::<_, Value>("/v1/info", &json!({}), Some("wrong-method"))
        .await;
    match method_err {
        Err(cooplet_client::ClientError::Problem(problem)) => {
            assert_eq!(problem.status, 405);
            assert_eq!(problem.code, ErrorCode::InvalidRequest);
        }
        other => return Err(format!("expected 405 problem, got {other:?}").into()),
    }

    // GET /v1/events/stream rejects unknown query parameters including limit.
    let stream_limit = hub
        .raw(
            "GET",
            "/v1/events/stream?limit=1",
            &[
                ("X-Cooplet-Hub", hub.id.as_str()),
                ("Authorization", &auth_header),
            ],
            &[],
        )
        .await?;
    stream_limit.problem(400, ErrorCode::InvalidRequest)?;

    let (first, second) = tokio::join!(redeem(&empty, &original), redeem(&empty, &original));
    let (enrolled, failure) = match (first, second) {
        (Ok(node), failure @ Err(_)) | (failure @ Err(_), Ok(node)) => (node, failure),
        _ => return Err("exactly one enrollment redemption must succeed".into()),
    };
    expect_problem(failure, ErrorCode::Unauthorized)?;
    assert_eq!(enrolled.node_id, original.node_id);
    assert_eq!(enrolled.generation, 1);
    let node = hub.client(Some(&enrolled.node_token))?;
    assert_eq!(
        node.whoami().await?.node_id.as_ref(),
        Some(&original.node_id)
    );
    expect_problem(
        node.get_json::<Value>("/v1/nodes", &[]).await,
        ErrorCode::Forbidden,
    )?;
    expect_problem(
        node.get_json::<Value>("/v1/events", &[]).await,
        ErrorCode::Forbidden,
    )?;
    expect_problem(
        node.post_json::<_, Value>(
            "/v1/messages",
            &json!({
                "target_node_id": original.node_id,
                "target_participant_id": ParticipantId::generate()?,
                "body": "hello"
            }),
            Some("node-forbidden-message"),
        )
        .await,
        ErrorCode::Forbidden,
    )?;
    expect_problem(
        node.get_json::<Value>("/v1/principals", &[]).await,
        ErrorCode::AdminHumanRequired,
    )?;

    // Bodyless POST accepts genuinely empty 0-byte body without Content-Type and replays with {}.
    let empty_revoke = hub
        .raw(
            "POST",
            &format!("/v1/enrollment-tokens/{}/revoke", original.token_id),
            &[
                ("X-Cooplet-Hub", hub.id.as_str()),
                ("Authorization", &auth_header),
                ("Idempotency-Key", "consumed-enrollment"),
            ],
            &[],
        )
        .await?;
    assert_eq!(empty_revoke.status, 200);
    let empty_revoke_json: Value = serde_json::from_slice(&empty_revoke.body)?;
    assert_eq!(empty_revoke_json["already_consumed"], true);
    assert_eq!(empty_revoke_json["revoked"], false);

    let consumed: Value = admin
        .post_json(
            &format!("/v1/enrollment-tokens/{}/revoke", original.token_id),
            &json!({}),
            Some("consumed-enrollment"),
        )
        .await?;
    assert_eq!(consumed["already_consumed"], true);
    assert_eq!(consumed["revoked"], false);
    let enrollment_list: Value = admin.get_json("/v1/enrollment-tokens", &[]).await?;
    assert_eq!(
        enrollment_list["items"][0]["node_id"],
        original.node_id.as_str()
    );
    assert!(enrollment_list["items"][0].get("verifier_hmac").is_none());
    assert!(enrollment_list["items"][0].get("token").is_none());

    let token_path = format!("/v1/principals/{}/tokens", who.actor_id);
    let token_list: Value = admin.get_json(&token_path, &[]).await?;
    assert!(token_list["items"][0]["expires_at_ms"].is_null());
    let initial_token_id = string(&token_list["items"][0], "token_id")?;
    let last_admin_path = format!("{token_path}/{initial_token_id}/revoke");
    let Err(cooplet_client::ClientError::Problem(last_admin)) = admin
        .post_json::<_, Value>(&last_admin_path, &json!({}), Some("last-admin-revoke"))
        .await
    else {
        return Err("expected protection for the last durable admin".into());
    };
    assert_eq!(last_admin.code, ErrorCode::OperationConflict);
    assert_eq!(last_admin.extensions["reason"], "last_durable_admin");

    let member_token: Value = admin.post_json(
        &token_path,
        &json!({"label":"personal adapter","scopes":["manage"],"allowed_nodes":null,"expires_at_ms":later()?}),
        Some("member-bearer-create"),
    ).await?;
    let member = hub.client(Some(&string(&member_token, "token")?))?;
    assert_eq!(
        member.whoami().await?.principal_kind,
        PrincipalKind::HumanMember
    );
    for path in ["/v1/principals", "/v1/enrollment-tokens", &token_path] {
        expect_problem(
            member.get_json::<Value>(path, &[]).await,
            ErrorCode::AdminHumanRequired,
        )?;
    }
    expect_problem(
        member
            .post_json::<_, Value>(
                &format!("/v1/nodes/{}/revoke", original.node_id),
                &json!({}),
                Some("member-node-revoke"),
            )
            .await,
        ErrorCode::AdminHumanRequired,
    )?;

    let participant = ParticipantId::generate()?;
    let node_id = &original.node_id;
    let run_id = cooplet_protocol::RunId::generate()?;
    let json_paths = [
        "/v1/principals".into(),
        token_path.clone(),
        format!("{token_path}/{}/revoke", string(&member_token, "token_id")?),
        format!("/v1/principals/{}/disable", who.actor_id),
        "/v1/enrollment-tokens".into(),
        format!("/v1/enrollment-tokens/{}/revoke", original.token_id),
        format!("/v1/nodes/{node_id}/revoke"),
        "/v1/messages".into(),
        "/v1/agents".into(),
        format!("/v1/agents/{participant}/stop"),
        format!("/v1/participants/{participant}/approval"),
        "/v1/workflows/runs".into(),
        format!("/v1/workflows/runs/{run_id}/cancel"),
    ];
    let oversized = vec![b' '; 65 * 1024];
    let length = oversized.len().to_string();
    for path in json_paths {
        let response = hub
            .raw(
                "POST",
                &path,
                &[
                    ("X-Cooplet-Hub", hub.id.as_str()),
                    ("Authorization", &auth_header),
                    ("Content-Type", "application/json"),
                    ("Content-Length", &length),
                    ("Idempotency-Key", "oversized-json"),
                ],
                &oversized,
            )
            .await?;
        response.problem(413, ErrorCode::PayloadTooLarge)?;
    }
    let oversized_enroll = vec![b' '; 16 * 1024 + 1];
    let enroll_len = oversized_enroll.len().to_string();
    hub.raw(
        "POST",
        "/v1/enrollment/token",
        &[
            ("X-Cooplet-Hub", hub.id.as_str()),
            ("Content-Type", "application/json"),
            ("Content-Length", &enroll_len),
        ],
        &oversized_enroll,
    )
    .await?
    .problem(413, ErrorCode::PayloadTooLarge)?;

    let chunked_enroll = encode_chunked(&oversized_enroll);
    hub.raw(
        "POST",
        "/v1/enrollment/token",
        &[
            ("X-Cooplet-Hub", hub.id.as_str()),
            ("Content-Type", "application/json"),
            ("Transfer-Encoding", "chunked"),
        ],
        &chunked_enroll,
    )
    .await?
    .problem(413, ErrorCode::PayloadTooLarge)?;

    let chunked_json = encode_chunked(&oversized);
    for path in [
        "/v1/principals".to_owned(),
        format!("/v1/nodes/{node_id}/revoke"),
    ] {
        hub.raw(
            "POST",
            &path,
            &[
                ("X-Cooplet-Hub", hub.id.as_str()),
                ("Authorization", &auth_header),
                ("Content-Type", "application/json"),
                ("Transfer-Encoding", "chunked"),
                ("Idempotency-Key", "oversized-chunked"),
            ],
            &chunked_json,
        )
        .await?
        .problem(413, ErrorCode::PayloadTooLarge)?;
    }

    // Open-socket GET requests with body headers must be rejected immediately without waiting for bytes.
    hub.raw_open("GET", "/v1/info", &[("Content-Length", "1")])
        .await?
        .problem(413, ErrorCode::PayloadTooLarge)?;
    hub.raw_open("GET", "/v1/info", &[("Transfer-Encoding", "chunked")])
        .await?
        .problem(400, ErrorCode::InvalidRequest)?;

    hub.stop_gracefully().await
}

#[tokio::test]
async fn scoped_service_access_and_stream_lifecycle() -> TestResult {
    let mut hub = Hub::start().await?;
    let admin = hub.admin()?;
    let empty = hub.client(None)?;
    let allowed = redeem(
        &empty,
        &issue_enrollment(&admin, "Allowed", "allowed-enrollment").await?,
    )
    .await?;
    let hidden = redeem(
        &empty,
        &issue_enrollment(&admin, "Hidden", "hidden-enrollment").await?,
    )
    .await?;
    let principal: Value = admin
        .post_json(
            "/v1/principals",
            &json!({
                "kind":"service",
                "username":"orchestrator",
                "display_name":"Orchestrateur d’équipe",
                "scopes":["manage"],
                "allowed_nodes":[allowed.node_id],
                "with_participant":true,
            }),
            Some("service-principal"),
        )
        .await?;
    let principal_id = string(&principal, "principal_id")?;
    let token_path = format!("/v1/principals/{principal_id}/tokens");
    let token = issue_service_token(&admin, &token_path, "service-token-first").await?;
    let other_token = issue_service_token(&admin, &token_path, "service-token-second").await?;
    let service = hub.client(Some(&string(&token, "token")?))?;
    let other_service = hub.client(Some(&string(&other_token, "token")?))?;
    let who = service.whoami().await?;
    assert_eq!(who.principal_kind, PrincipalKind::Service);
    assert!(who.participant_id.is_some());
    assert_eq!(who.name, "Orchestrateur d’équipe");

    // Bidi override and zero-width characters are rejected on principal display names.
    for bad_name in [
        "Spoof\u{202E}Admin",
        "Spoof\u{200B}Admin",
        "Line\u{2028}Break",
    ] {
        expect_problem(
            admin
                .post_json::<_, Value>(
                    "/v1/principals",
                    &json!({
                        "kind": "human_member",
                        "username": "badname_user",
                        "display_name": bad_name,
                        "scopes": ["read"],
                    }),
                    Some("bad-display-name"),
                )
                .await,
            ErrorCode::InvalidRequest,
        )?;
    }

    let nodes: Value = service.get_json("/v1/nodes", &[]).await?;
    assert_eq!(
        nodes["items"].as_array().ok_or("node list missing")?.len(),
        1
    );
    assert_eq!(nodes["items"][0]["node_id"], allowed.node_id.as_str());

    // Admin unscoped catalogs: GET /v1/principals pagination, GET /v1/nodes, GET /v1/participants.
    let principals_p1: Value = admin
        .get_json("/v1/principals", &[("limit", "1".into())])
        .await?;
    assert_eq!(
        principals_p1["items"]
            .as_array()
            .ok_or("principals missing")?
            .len(),
        1
    );
    let next_principal = string(&principals_p1, "next_after")?;
    let principals_p2: Value = admin
        .get_json("/v1/principals", &[("after", next_principal)])
        .await?;
    assert_eq!(
        principals_p2["items"]
            .as_array()
            .ok_or("principals page 2 missing")?
            .len(),
        1
    );
    let admin_nodes: Value = admin.get_json("/v1/nodes", &[]).await?;
    assert_eq!(
        admin_nodes["items"]
            .as_array()
            .ok_or("admin nodes missing")?
            .len(),
        2
    );
    let admin_participants: Value = admin.get_json("/v1/participants", &[]).await?;
    assert!(admin_participants["items"].is_array());
    for path in ["/v1/nodes", "/v1/participants"] {
        expect_problem(
            service
                .get_json::<Value>(path, &[("node_id", hidden.node_id.to_string())])
                .await,
            ErrorCode::Forbidden,
        )?;
    }
    for path in ["/v1/principals", "/v1/enrollment-tokens", &token_path] {
        expect_problem(
            service.get_json::<Value>(path, &[]).await,
            ErrorCode::AdminHumanRequired,
        )?;
    }
    expect_problem(
        service
            .post_json::<_, Value>(
                &format!("/v1/nodes/{}/revoke", hidden.node_id),
                &json!({}),
                Some("service-node-revoke"),
            )
            .await,
        ErrorCode::AdminHumanRequired,
    )?;

    let target = ParticipantId::generate()?;
    for (path, body) in [
        (
            "/v1/messages",
            json!({"target_node_id":hidden.node_id,"target_participant_id":target,"body":"hello"}),
        ),
        (
            "/v1/agents",
            json!({"target_node_id":hidden.node_id,"client_kind":"codex"}),
        ),
        (
            "/v1/workflows/runs",
            json!({"target_node_id":hidden.node_id,"workflow_name":"audit","workspace_dir":"."}),
        ),
    ] {
        expect_problem(
            service
                .post_json::<_, Value>(path, &body, Some("outside-service-scope"))
                .await,
            ErrorCode::Forbidden,
        )?;
    }

    let node = hub.client(Some(&hidden.node_token))?;
    let content = b"private node upload";
    let blob = upload_text(&node, "node-private-blob", content).await?;
    // HTTP/1.1 early replay via CoopletClient with expected_sha256 + content_length returns
    // 200 OK before the stalled body finishes streaming without poisoning the connection pool.
    let replay_outcome = timeout(
        Duration::from_secs(3),
        node.upload_blob(
            BlobUploadSpec {
                filename: "résumé.txt".into(),
                mime_type: "text/plain".into(),
                verification: UploadVerification::LocalSha256,
                expected_sha256: Some(blob.sha256.clone()),
                idempotency_key: Some("node-private-blob".into()),
                content_length: Some(content.len() as u64),
                participant_id: None,
            },
            PendingReader,
        ),
    )
    .await??;
    assert!(matches!(
        replay_outcome,
        BlobUploadOutcome::AcknowledgedWithoutLocalVerification(ref replayed) if replayed == &blob
    ));
    let mut bytes = Vec::new();
    node.download_blob_to_writer(&blob.blob_id, Some(&blob.sha256), 1024, &mut bytes)
        .await?;
    assert_eq!(bytes, content);
    expect_problem(
        service
            .open_blob_download(&blob.blob_id, Some(&blob.sha256), 1024)
            .await,
        ErrorCode::Forbidden,
    )?;

    let revocation: Value = admin
        .post_json(
            &format!("/v1/nodes/{}/revoke", hidden.node_id),
            &json!({}),
            Some("revoke-hidden-node"),
        )
        .await?;
    assert_eq!(revocation["revoked"], true);
    expect_problem(node.whoami().await, ErrorCode::NodeRevoked)?;
    let archived = admin.list_events(&EventsQuery::default()).await?;
    assert!(
        archived
            .items
            .iter()
            .any(|event| event.aggregate_id == blob.blob_id.as_str())
    );
    let filtered = service.list_events(&EventsQuery::default()).await?;
    assert!(filtered.items.is_empty());
    assert_eq!(filtered.next_seq, archived.latest_seq);
    assert!(matches!(
        service.stream_events(u64::MAX, &[]).await,
        Err(cooplet_client::ClientError::ResyncRequired { latest_seq, .. }) if latest_seq == archived.latest_seq
    ));

    let mut stream = service.stream_events(filtered.next_seq, &[]).await?;
    let mut second_stream = other_service.stream_events(filtered.next_seq, &[]).await?;
    upload_text(&admin, "admin-private-blob", b"hidden live event").await?;
    let visible = upload_text(&service, "service-live-blob", b"visible live event").await?;
    assert_next_blob(&mut stream, &visible.blob_id).await?;
    assert_next_blob(&mut second_stream, &visible.blob_id).await?;
    let revoked: Value = admin
        .post_json(
            &format!("{token_path}/{}/revoke", string(&token, "token_id")?),
            &json!({}),
            Some("revoke-live-stream"),
        )
        .await?;
    assert_eq!(revoked["revoked"], true);
    await_eof(&mut stream).await?;
    expect_problem(service.whoami().await, ErrorCode::Unauthorized)?;
    other_service.health().await?;

    hub.stop_gracefully().await?;
    await_eof(&mut second_stream).await
}

async fn upload_text(client: &CoopletClient, key: &str, bytes: &[u8]) -> TestResult<AttachmentRef> {
    let outcome = client
        .upload_blob(
            BlobUploadSpec {
                filename: "résumé.txt".into(),
                mime_type: "text/plain".into(),
                verification: UploadVerification::LocalSha256,
                expected_sha256: None,
                idempotency_key: Some(key.into()),
                content_length: Some(bytes.len() as u64),
                participant_id: None,
            },
            bytes,
        )
        .await?;
    match outcome {
        BlobUploadOutcome::VerifiedLocal(blob) => Ok(blob),
        _ => Err("blob upload was not verified".into()),
    }
}

async fn assert_next_blob(
    stream: &mut cooplet_client::EventStream,
    expected: &BlobId,
) -> TestResult {
    let frame = timeout(Duration::from_secs(3), stream.next_frame())
        .await?
        .ok_or("stream ended")??;
    let EventStreamFrame::Event { event } = frame else {
        return Err("expected a live blob event".into());
    };
    assert_eq!(event.kind, cooplet_protocol::EventKind::BlobStored);
    assert_eq!(event.aggregate_id, expected.as_str());
    assert!(event.payload.get("audience").is_none());
    Ok(())
}

async fn await_eof(stream: &mut cooplet_client::EventStream) -> TestResult {
    timeout(Duration::from_secs(2), async {
        while let Some(frame) = stream.next_frame().await {
            assert!(
                matches!(frame?, EventStreamFrame::Ping {}),
                "unexpected data before stream closed"
            );
        }
        Ok(())
    })
    .await?
}

async fn issue_service_token(admin: &CoopletClient, path: &str, key: &str) -> TestResult<Value> {
    Ok(admin
        .post_json(
            path,
            &json!({
                "label":"orchestration",
                "scopes":["manage"],
                "allowed_nodes":null,
                "expires_at_ms":later()?,
            }),
            Some(key),
        )
        .await?)
}

struct PendingReader;

impl tokio::io::AsyncRead for PendingReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

fn encode_chunked(body: &[u8]) -> Vec<u8> {
    let mut encoded = format!("{:x}\r\n", body.len()).into_bytes();
    encoded.extend_from_slice(body);
    encoded.extend_from_slice(b"\r\n0\r\n\r\n");
    encoded
}

fn later() -> TestResult<i64> {
    Ok(i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? + 60_000)
}
