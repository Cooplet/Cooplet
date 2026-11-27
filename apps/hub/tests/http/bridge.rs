use std::time::Duration;

use cooplet_protocol::{
    ApprovalWaitKind, BRIDGE_ERROR_HEADER, BRIDGE_SUBPROTOCOL, BridgeFrame, BridgePayload,
    ErrorCode, HelloAckPayload, HubId, MemberView, ParticipantId, ParticipantKind,
    ParticipantState, ParticipantWire, RosterReadyPayload, RosterSyncPayload, TemplateSummary,
    ThreadAckPayload, ThreadDeliverPayload, ThreadDeliveryOutcome, ThreadMemberState,
    ThreadMembersResponse, ThreadSummary,
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

fn agent_roster(participant_id: ParticipantId) -> RosterSyncPayload {
    let mut initial = roster();
    initial.participants.push(ParticipantWire {
        participant_id,
        kind: ParticipantKind::Agent,
        name: "coordinator".into(),
        client_kind: Some("mock".into()),
        workspace_dir: None,
        state: ParticipantState::Idle,
        capabilities: json!({}),
        wait_kind: None,
        approval_id: None,
        action_hint: None,
        summary: None,
        can_respond_remotely: None,
    });
    initial
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

fn thread_ack(delivery: ThreadDeliverPayload) -> ThreadAckPayload {
    ThreadAckPayload {
        delivery_generation: delivery.delivery_generation,
        delivery_id: delivery.delivery_id,
        thread_id: delivery.thread_id,
        member_id: delivery.member_id,
        acked_seq: delivery.to_seq,
        outcome: ThreadDeliveryOutcome::Delivered,
        detail: None,
    }
}

#[tokio::test]
async fn resume_skips_previously_sent_posts_and_ignores_the_old_ack() -> TestResult {
    let hub = Hub::start().await?;
    let enrolled = enroll(&hub, "Worker", "bridge-resume-epoch").await?;
    let (mut socket, epoch) = open(&hub, &enrolled).await?;
    let participant = ParticipantId::generate()?;
    let initial = agent_roster(participant.clone());
    ready(&mut socket, epoch, &initial).await?;
    let admin = hub.admin()?;
    let member = format!("{}/{participant}", enrolled.node_id);
    let join = json!({"member_key":member,"handle":"coordinator"});
    let board: ThreadSummary = admin
        .post_json(
            "/v1/threads",
            &json!({
                "title":"Visible on the wire", "members":[join],
            }),
            Some("old-range-thread"),
        )
        .await?;
    let posts = format!("/v1/threads/{}/posts", board.thread_id);
    let members = format!("/v1/threads/{}/members", board.thread_id);
    let _: Value = admin
        .post_json(
            &posts,
            &json!({"body":"@coordinator old range"}),
            Some("old-range-post"),
        )
        .await?;
    let old: ThreadDeliverPayload = frame(&mut socket).await?.decode_payload()?;
    assert_eq!(
        (old.delivery_generation, old.from_seq, old.to_seq),
        (0, 1, 1)
    );

    let left: MemberView = admin
        .post_json(
            &format!("{members}/leave"),
            &json!({"member_key":member}),
            Some("old-range-leave"),
        )
        .await?;
    assert_eq!(
        left.pending
            .ok_or("leave lost the unresolved range")?
            .delivery_id,
        old.delivery_id
    );
    let joined: Value = admin
        .post_json(&format!("{members}/join"), &join, Some("old-range-join"))
        .await?;
    assert_eq!(joined["state"], "paused_unknown");
    let resumed: MemberView = admin
        .post_json(
            &format!("{members}/resume"),
            &json!({"member_key":member,"after_seq":old.to_seq}),
            Some("old-range-skip"),
        )
        .await?;
    assert_eq!(resumed.delivery_generation, 1);
    assert_eq!(resumed.confirmed_seq, 1);
    assert!(resumed.pending.is_none());

    let _: Value = admin
        .post_json(
            &posts,
            &json!({"body":"@coordinator new range"}),
            Some("new-range-post"),
        )
        .await?;
    let new: ThreadDeliverPayload = frame(&mut socket).await?.decode_payload()?;
    assert_eq!(
        (new.delivery_generation, new.from_seq, new.to_seq),
        (1, 2, 2)
    );
    send(&mut socket, epoch, &thread_ack(old)).await?;
    send(&mut socket, epoch, &thread_ack(new)).await?;
    timeout(DEADLINE, async {
        loop {
            let page: ThreadMembersResponse = admin.get_json(&members, &[]).await?;
            let view = page
                .items
                .iter()
                .find(|view| view.member_key == member)
                .ok_or("member disappeared after resume")?;
            if view.confirmed_seq == 2 {
                assert_eq!(view.delivery_generation, 1);
                assert!(view.pending.is_none());
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
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

#[tokio::test]
async fn an_unready_replacement_or_revoke_pauses_the_old_unacknowledged_range() -> TestResult {
    let hub = Hub::start().await?;
    let admin = hub.admin()?;
    for revoke in [false, true] {
        let mode = if revoke { "revoked" } else { "replaced" };
        let enrolled = enroll(&hub, "Worker", &format!("enroll-{mode}")).await?;
        let (mut old, epoch) = open(&hub, &enrolled).await?;
        let participant = ParticipantId::generate()?;
        ready(&mut old, epoch, &agent_roster(participant.clone())).await?;
        let member = format!("{}/{participant}", enrolled.node_id);
        let board: ThreadSummary = admin
            .post_json(
                "/v1/threads",
                &json!({
                    "title":"Recover an interrupted range",
                    "members":[{"member_key":member,"handle":"coordinator"}],
                }),
                Some(&format!("thread-{mode}")),
            )
            .await?;
        let prefix = format!("/v1/threads/{}", board.thread_id);
        let _: Value = admin
            .post_json(
                &format!("{prefix}/posts"),
                &json!({"body":"@coordinator interrupted range"}),
                Some(&format!("post-{mode}")),
            )
            .await?;
        let delivery: ThreadDeliverPayload = frame(&mut old).await?.decode_payload()?;

        if revoke {
            let revoked: Value = admin
                .post_json(
                    &format!("/v1/nodes/{}/revoke", enrolled.node_id),
                    &json!({}),
                    Some("revoke-live-range"),
                )
                .await?;
            assert_eq!(revoked["revoked"], true);
            close_code(&mut old, 4003, "node_revoked").await?;
            assert_eq!(summary(&hub, &enrolled).await?["status"], "revoked");
        } else {
            let (mut replacement, next_epoch) = open(&hub, &enrolled).await?;
            assert_eq!(next_epoch, epoch + 1);
            close_code(&mut old, 4001, "superseded_by_new_epoch").await?;
            replacement.send(Message::Close(None)).await?;
            let closed = timeout(DEADLINE, replacement.next())
                .await?
                .ok_or("replacement did not echo close")??;
            assert!(matches!(closed, Message::Close(_)));
            drop(replacement);
            let item = summary(&hub, &enrolled).await?;
            assert_eq!(item["status"], "offline");
            assert_eq!(item["session_epoch"], next_epoch);
        }

        let members: ThreadMembersResponse =
            admin.get_json(&format!("{prefix}/members"), &[]).await?;
        let current = members
            .items
            .iter()
            .find(|view| view.member_key == member)
            .ok_or("thread member disappeared after its session ended")?;
        assert_eq!(current.state, ThreadMemberState::PausedUnknown);
        assert_eq!(current.confirmed_seq, 0);
        let pending = current
            .pending
            .as_ref()
            .ok_or("interrupted range was discarded")?;
        assert_eq!(pending.delivery_id, delivery.delivery_id);
        assert_eq!(
            (pending.from_seq, pending.to_seq),
            (delivery.from_seq, delivery.to_seq)
        );
        if !revoke {
            let resumed: MemberView = admin
                .post_json(
                    &format!("{prefix}/members/resume"),
                    &json!({"member_key":member,"after_seq":delivery.to_seq}),
                    Some("resume-while-replacement-offline"),
                )
                .await?;
            assert_eq!(resumed.state, ThreadMemberState::Active);
            assert_eq!(
                resumed.delivery_generation,
                delivery.delivery_generation + 1
            );
            assert_eq!(resumed.confirmed_seq, delivery.to_seq);
            assert!(resumed.pending.is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_reports_a_failed_disconnect_commit() -> TestResult {
    let mut hub = Hub::start().await?;
    let enrolled = enroll(&hub, "Disconnect", "disconnect-commit").await?;
    let (mut socket, epoch) = open(&hub, &enrolled).await?;
    ready(&mut socket, epoch, &roster()).await?;
    let db = rusqlite::Connection::open(hub.database_path())?;
    db.execute_batch(
        "CREATE TRIGGER fail_disconnect BEFORE INSERT ON events WHEN NEW.kind='node.disconnected'
         BEGIN SELECT RAISE(ABORT,'simulated disconnect failure'); END;",
    )?;

    let status = hub.stop().await?;
    assert!(!status.success());
    let recorded: String = db.query_row(
        "SELECT status FROM nodes WHERE node_id=?1",
        [enrolled.node_id.as_str()],
        |row| row.get(0),
    )?;
    assert_eq!(recorded, "online");
    Ok(())
}
