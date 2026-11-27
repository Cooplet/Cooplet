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
    let mismatch = hub
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
    let mismatch = mismatch.problem(409, ErrorCode::HubIdentityMismatch)?;
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

    let original = hub.issue_enrollment_cli("Test Node", "first-enrollment")?;
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
    hub.raw(
        "POST",
        "/v1/enrollment/token",
        &[
            ("X-Cooplet-Hub", hub.id.as_str()),
            ("Content-Type", "application/json"),
            ("Content-Length", &length),
        ],
        &oversized,
    )
    .await?
    .problem(413, ErrorCode::PayloadTooLarge)?;
    hub.raw("GET", "/v1/info", &[("Transfer-Encoding", "chunked")], &[])
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

    let nodes: Value = service.get_json("/v1/nodes", &[]).await?;
    assert_eq!(
        nodes["items"].as_array().ok_or("node list missing")?.len(),
        1
    );
    assert_eq!(nodes["items"][0]["node_id"], allowed.node_id.as_str());
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

fn later() -> TestResult<i64> {
    Ok(i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? + 60_000)
}
