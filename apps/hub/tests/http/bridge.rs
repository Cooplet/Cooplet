use std::time::Duration;

use cooplet_protocol::{
    ApprovalWaitKind, BRIDGE_ERROR_HEADER, BRIDGE_SUBPROTOCOL, BridgeFrame, BridgePayload,
    ErrorCode, HelloAckPayload, HubId, ParticipantId, ParticipantKind, ParticipantState,
    ParticipantWire, RosterReadyPayload, RosterSyncPayload, TemplateSummary,
};
use futures_util::{SinkExt, StreamExt};
use http::{HeaderValue, Request, StatusCode, header};
use serde_json::{Value, json};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error, Message, client::IntoClientRequest},
};

use super::support::{Enrolled, Hub, TestResult, issue_enrollment, redeem};

const PATH: &str = "/v1/bridge/connect";
const DEADLINE: Duration = Duration::from_secs(5);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn enroll(hub: &Hub, label: &str, key: &str) -> TestResult<Enrolled> {
    let enrollment = issue_enrollment(&hub.admin()?, label, key).await?;
    Ok(redeem(&hub.client(None)?, &enrollment).await?)
}

fn request(
    hub: &Hub,
    hub_pin: &HubId,
    token: &str,
    protocol: Option<&'static str>,
) -> TestResult<Request<()>> {
    let mut url = hub.origin.join(PATH)?;
    url.set_scheme("ws")
        .map_err(|()| "could not set WS scheme")?;
    let mut request = url.as_str().into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("X-Cooplet-Hub", hub_pin.as_str().parse()?);
    headers.insert(header::AUTHORIZATION, format!("Bearer {token}").parse()?);
    if let Some(protocol) = protocol {
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(protocol),
        );
    }
    Ok(request)
}

async fn open(hub: &Hub, enrolled: &Enrolled) -> TestResult<(Socket, u64)> {
    let request = request(hub, &hub.id, &enrolled.node_token, Some(BRIDGE_SUBPROTOCOL))?;
    let (mut socket, response) = timeout(DEADLINE, connect_async(request)).await??;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response.headers()[header::SEC_WEBSOCKET_PROTOCOL],
        BRIDGE_SUBPROTOCOL
    );
    let frame = frame(&mut socket).await?;
    let hello: HelloAckPayload = frame.decode_payload()?;
    assert_eq!(hello.hub_id, hub.id);
    assert_eq!(hello.node_id, enrolled.node_id);
    assert_eq!(hello.max_inflight_commands, 32);
    assert_eq!(hello.max_inflight_threads, 16);
    Ok((socket, frame.epoch))
}

async fn rejected(
    request: Request<()>,
    status: StatusCode,
    bridge_code: Option<ErrorCode>,
) -> TestResult {
    let response = match timeout(DEADLINE, connect_async(request)).await? {
        Err(Error::Http(response)) => response,
        _ => return Err("expected WebSocket handshake HTTP rejection".into()),
    };
    assert_eq!(response.status(), status);
    if let Some(code) = bridge_code {
        assert_eq!(response.headers()[BRIDGE_ERROR_HEADER], code.as_str());
    }
    if status == StatusCode::UNAUTHORIZED {
        assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
    }
    Ok(())
}

async fn frame(socket: &mut Socket) -> TestResult<BridgeFrame> {
    let message = timeout(DEADLINE, socket.next())
        .await?
        .ok_or("bridge closed")??;
    match message {
        Message::Text(text) => Ok(BridgeFrame::from_json(text.as_bytes())?),
        _ => Err("expected a text bridge frame".into()),
    }
}

async fn close_code(socket: &mut Socket, code: u16, reason: &str) -> TestResult {
    let message = timeout(DEADLINE, socket.next())
        .await?
        .ok_or("bridge closed without a frame")??;
    let Message::Close(Some(close)) = message else {
        return Err("expected a bridge close frame".into());
    };
    assert_eq!(u16::from(close.code), code);
    assert_eq!(close.reason.as_str(), reason);
    Ok(())
}

async fn send(socket: &mut Socket, epoch: u64, payload: &impl BridgePayload) -> TestResult {
    let text = String::from_utf8(BridgeFrame::encode_json(epoch, payload)?)?;
    socket.send(Message::Text(text.into())).await?;
    Ok(())
}

fn roster() -> RosterSyncPayload {
    RosterSyncPayload {
        chunk_index: 0,
        is_final: true,
        participants: Vec::new(),
        templates: Vec::new(),
        workflows: Vec::new(),
        unresolved_thread_fences: Vec::new(),
        resolved_op_receipts: Vec::new(),
    }
}

async fn ready(socket: &mut Socket, epoch: u64, roster: &RosterSyncPayload) -> TestResult {
    send(socket, epoch, roster).await?;
    let frame = frame(socket).await?;
    assert_eq!(frame.epoch, epoch);
    let _: RosterReadyPayload = frame.decode_payload()?;
    Ok(())
}

async fn summary(hub: &Hub, node: &Enrolled) -> TestResult<Value> {
    let catalog: Value = hub
        .admin()?
        .get_json("/v1/nodes", &[("node_id", node.node_id.to_string())])
        .await?;
    let item = catalog["items"]
        .as_array()
        .and_then(|items| items.first())
        .ok_or("node summary is missing")?;
    Ok(item.clone())
}

#[tokio::test]
async fn bridge_handshake_fences_epochs_and_rejects_revoked_nodes() -> TestResult {
    let hub = Hub::start().await?;
    let enrolled = enroll(&hub, "Worker", "bridge-handshake").await?;
    for protocol in [None, Some("invalid")] {
        rejected(
            request(&hub, &hub.id, &enrolled.node_token, protocol)?,
            StatusCode::BAD_REQUEST,
            None,
        )
        .await?;
    }
    rejected(
        request(
            &hub,
            &HubId::generate()?,
            &enrolled.node_token,
            Some(BRIDGE_SUBPROTOCOL),
        )?,
        StatusCode::CONFLICT,
        Some(ErrorCode::HubIdentityMismatch),
    )
    .await?;
    rejected(
        request(&hub, &hub.id, &hub.secret, Some(BRIDGE_SUBPROTOCOL))?,
        StatusCode::UNAUTHORIZED,
        None,
    )
    .await?;
    assert_eq!(summary(&hub, &enrolled).await?["session_epoch"], 0);

    let (mut old, first) = open(&hub, &enrolled).await?;
    ready(&mut old, first, &roster()).await?;
    assert_eq!(summary(&hub, &enrolled).await?["status"], "online");
    let (mut replacement, second) = open(&hub, &enrolled).await?;
    assert_eq!(second, first + 1);
    close_code(&mut old, 4001, "superseded_by_new_epoch").await?;
    assert_eq!(summary(&hub, &enrolled).await?["status"], "offline");
    ready(&mut replacement, second, &roster()).await?;
    send(&mut replacement, first, &roster()).await?;
    close_code(&mut replacement, 4002, "epoch_mismatch").await?;

    let (mut current, third) = open(&hub, &enrolled).await?;
    assert_eq!(third, second + 1);
    ready(&mut current, third, &roster()).await?;
    let revoked: Value = hub
        .admin()?
        .post_json(
            &format!("/v1/nodes/{}/revoke", enrolled.node_id),
            &json!({}),
            Some("bridge-revoke-node"),
        )
        .await?;
    assert_eq!(revoked["revoked"], true);
    close_code(&mut current, 4003, "node_revoked").await?;
    rejected(
        request(
            &hub,
            &hub.id,
            &enrolled.node_token,
            Some(BRIDGE_SUBPROTOCOL),
        )?,
        StatusCode::UNAUTHORIZED,
        Some(ErrorCode::NodeRevoked),
    )
    .await?;
    let persisted = summary(&hub, &enrolled).await?;
    assert_eq!(persisted["session_epoch"], third);
    assert_eq!(persisted["status"], "revoked");
    Ok(())
}

#[tokio::test]
async fn bridge_snapshot_is_atomic_and_same_node_replacement_reuses_capacity() -> TestResult {
    let hub =
        Hub::start_with_args(&["--max-active-bridges", "1", "--handshake-permits", "1"]).await?;
    let first = enroll(&hub, "First", "bridge-capacity-first").await?;
    let second = enroll(&hub, "Second", "bridge-capacity-second").await?;

    let (mut live, epoch) = open(&hub, &first).await?;
    rejected(
        request(&hub, &hub.id, &second.node_token, Some(BRIDGE_SUBPROTOCOL))?,
        StatusCode::SERVICE_UNAVAILABLE,
        None,
    )
    .await?;
    assert_eq!(summary(&hub, &second).await?["session_epoch"], 0);

    let member = ParticipantWire {
        participant_id: ParticipantId::generate()?,
        kind: ParticipantKind::Agent,
        name: "coordinator".into(),
        client_kind: Some("mock".into()),
        workspace_dir: None,
        state: ParticipantState::AwaitingApproval,
        capabilities: json!({}),
        wait_kind: Some(ApprovalWaitKind::Permission),
        approval_id: Some("approval-1".into()),
        action_hint: None,
        summary: Some("Proceed?".into()),
        can_respond_remotely: Some(true),
    };
    let mut initial = roster();
    initial.participants.push(member.clone());
    initial.templates.push(TemplateSummary {
        template_id: "review".into(),
        title: "Review".into(),
        client_kind: "mock".into(),
        description: None,
    });
    ready(&mut live, epoch, &initial).await?;
    assert_eq!(
        summary(&hub, &first).await?["templates"][0]["template_id"],
        "review"
    );

    let (mut next, next_epoch) = open(&hub, &first).await?;
    assert_eq!(next_epoch, epoch + 1);
    close_code(&mut live, 4001, "superseded_by_new_epoch").await?;
    let mut partial = roster();
    partial.is_final = false;
    partial.participants.push(ParticipantWire {
        participant_id: ParticipantId::generate()?,
        ..member.clone()
    });
    send(&mut next, next_epoch, &partial).await?;
    let catalog: Value = hub.admin()?.get_json("/v1/participants", &[]).await?;
    assert_eq!(catalog["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        catalog["items"][0]["participant_id"],
        member.participant_id.as_str()
    );
    assert_eq!(catalog["items"][0]["approval_id"], "approval-1");
    assert_eq!(catalog["items"][0]["can_respond_remotely"], true);

    partial.chunk_index = 1;
    partial.is_final = true;
    send(&mut next, next_epoch, &partial).await?;
    close_code(&mut next, 1002, "invalid_bridge_frame").await?;
    let node = summary(&hub, &first).await?;
    assert_eq!(node["templates"][0]["template_id"], "review");
    assert_eq!(node["status"], "offline");
    Ok(())
}

#[tokio::test]
async fn bridge_closes_binary_and_oversized_frames() -> TestResult {
    let hub = Hub::start().await?;
    let enrolled = enroll(&hub, "Worker", "bridge-frame-limits").await?;
    let (mut binary, _) = open(&hub, &enrolled).await?;
    binary
        .send(Message::Binary(bytes::Bytes::from_static(b"binary")))
        .await?;
    close_code(&mut binary, 1003, "expected_bridge_text").await?;

    let (mut oversized, _) = open(&hub, &enrolled).await?;
    let text = "x".repeat(cooplet_protocol::MAX_WS_FRAME_BYTES + 1);
    oversized.send(Message::Text(text.into())).await?;
    close_code(&mut oversized, 1009, "bridge_frame_too_large").await?;
    Ok(())
}
