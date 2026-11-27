#[path = "common/bridge_crash.rs"]
mod bridge_crash;
#[path = "common/bridge_hub.rs"]
mod bridge_hub;
#[path = "common/mock_adapter.rs"]
mod mock_adapter;
#[path = "common/test_node.rs"]
mod test_node;

use std::{
    ffi::{OsStr, OsString},
    fs,
    io::{self, Write},
    os::unix::{ffi::OsStringExt, fs::PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use cooplet_client::{ClientError, CoopletClient, EventsQuery};
use cooplet_node::clients::{AdapterEventKind, ParticipantRuntimeState, SubmitOutcome, TurnInput};
use cooplet_protocol::{
    ApprovalWait, ApprovalWaitKind, AttachmentRef, AuthorKind, ErrorCode, EventKind, MessageId,
    NodeId, ParticipantId,
};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};

use bridge_hub::{BridgeHub, Process, write_private};
use mock_adapter::{MockAdapter, MockSession, MockSubmit};
use test_node::{TestNode, TestResult};

const WAIT: Duration = Duration::from_secs(12);
const NEW_POSTS: u64 = 4;
const FILE_BYTES: &[u8] = b"A reproducible result shared across two nodes.\n";
const FILE_NAME: &str = "Coordinator notes.txt";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrolled_nodes_share_a_thread_and_reply_over_the_same_bridge() -> TestResult {
    let mut hub = BridgeHub::start().await?;
    let a_mock = MockAdapter::default();
    let b_mock = MockAdapter::default();
    let mut a = TestNode::start(a_mock.clone()).await?;
    let b_state = OsString::from_vec(b"state-\xff".to_vec());
    let mut b = TestNode::start_with_state_name(b_mock.clone(), &b_state).await?;

    let a_agent = a
        .create(
            "create-observer",
            json!({"client_kind":"mock","name":"captain"}),
        )
        .await?;
    let a_session = session(&a_mock, &a_agent)?;
    let a_agent_client = a.agent_client(&a_session).await?;
    let a_identity_before = a_agent_client.whoami().await?;
    let a_operator_before = a.client.whoami().await?;

    let a_enrollment = hub.issue("alpha").await?;
    let a_token = hub.root.0.join("alpha.enrollment");
    write_private(&a_token, a_enrollment.token.as_bytes())?;
    let mut cli_enroll = node_cli(&a.state_dir);
    cli_enroll
        .args([
            "node",
            "enroll",
            "--hub",
            &hub.origin,
            "--hub-id",
            hub.id.as_str(),
            "--token-file",
        ])
        .arg(a_token);
    let binding = cli_json(cli_enroll).await?;
    assert_eq!(binding["node_id"], a_enrollment.node_id.as_str());
    assert_eq!(binding["local_node_id"], json!(a_operator_before.node_id));
    let replay: Value = a
        .client
        .post_json("/v1/bridge/enroll", &hub.binding(&a_enrollment), None)
        .await?;
    assert_eq!(replay["node_id"], binding["node_id"]);
    assert_eq!(
        fs::metadata(a.state_dir.join("creds/hub.json"))?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(a.client.whoami().await?, a_operator_before);
    assert_eq!(a_agent_client.whoami().await?, a_identity_before);

    let b_enrollment = hub.issue("beta").await?;
    let b_binding: Value = b
        .client
        .post_json("/v1/bridge/enroll", &hub.binding(&b_enrollment), None)
        .await?;
    assert_eq!(b_binding["node_id"], b_enrollment.node_id.as_str());
    online(&hub.admin, &a_enrollment.node_id).await?;
    online(&hub.admin, &b_enrollment.node_id).await?;

    // Declared after the initial roster. The Hub must project this participant
    // and its working state before it can be selected for delivery.
    let coordinator = b
        .create(
            "create-coordinator",
            json!({"client_kind":"mock","name":"coordinator"}),
        )
        .await?;
    let b_session = session(&b_mock, &coordinator)?;
    b_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    let b_member = format!("{}/{coordinator}", b_enrollment.node_id);
    let a_member = format!("{}/{a_agent}", a_enrollment.node_id);
    participant_state(&hub.admin, &b_enrollment.node_id, &coordinator, "working").await?;

    let human = hub.admin.whoami().await?;
    let service: Value = hub
        .admin
        .post_json(
            "/v1/principals",
            &json!({
                "kind":"service", "username":"reporter", "display_name":"Reporter",
                "scopes":["read","collaborate"], "allowed_nodes":null, "with_participant":true,
            }),
            Some("create-reporter"),
        )
        .await?;
    let service_actor = field(&service, "principal_id")?;
    let service_token: Value = hub
        .admin
        .post_json(
            &format!("/v1/principals/{service_actor}/tokens"),
            &json!({
                "label":"e2e", "scopes":["read","collaborate"], "allowed_nodes":null,
                "expires_at_ms":cooplet_node::now_ms()? + 600_000,
            }),
            Some("mint-reporter"),
        )
        .await?;
    let reporter = hub.client(field(&service_token, "token")?)?;

    let mut create = node_cli(&a.state_dir);
    create.args([
        "thread",
        "create",
        "--title",
        "Cross-machine review",
        "--operation-key",
        "thread-create",
    ]);
    for member in [
        json!({"member_key":a_member,"handle":"captain"}),
        json!({"member_key":b_member,"handle":"coordinator","wake_policy":"mentions_only"}),
        json!({"member_key":human.actor_id,"handle":"reviewer"}),
        json!({"member_key":service_actor,"handle":"reporter"}),
    ] {
        create.arg("--member").arg(member.to_string());
    }
    let board = cli_json(create).await?;
    let thread = field(&board, "thread_id")?.to_owned();
    let posts = format!("/v1/threads/{thread}/posts");

    let source = a.workspace_dir.join(FILE_NAME);
    fs::write(&source, FILE_BYTES)?;
    let (failed, upload) = mcp_call(
        a_session.credential.clone(),
        "blob_put",
        json!({"path":FILE_NAME,"target":"hub","operation_key":"agent-attachment"}),
    )
    .await?;
    assert!(!failed, "MCP blob relay failed: {upload}");
    let attachment: AttachmentRef = serde_json::from_value(upload)?;
    assert_eq!(attachment.name, FILE_NAME);
    assert_eq!(attachment.size_bytes, FILE_BYTES.len() as u64);
    let operator_post = json!({"body":"Coordination note", "mentions":["coordinator"]});
    let human_post = json!({"body":"@coordinator review the decision"});
    let service_post = json!({"body":"@coordinator the checks passed"});
    let agent_post = mcp_call(
        a_session.credential.clone(),
        "thread_post",
        json!({
            "thread_id":thread, "body":"@coordinator I agree", "operation_key":"agent-post",
            "attachments":[attachment],
        }),
    );
    let (operator_saved, human_saved, service_saved, agent_saved) = tokio::try_join!(
        post(&a.client, &posts, &operator_post, "operator-post"),
        post(&hub.admin, &posts, &human_post, "human-post"),
        post(&reporter, &posts, &service_post, "service-post"),
        agent_post,
    )?;
    assert!(
        !agent_saved.0,
        "thread_post failed through MCP: {}",
        agent_saved.1
    );
    let mut seqs = [operator_saved, human_saved, service_saved, agent_saved.1]
        .map(|post| post["seq"].as_u64());
    seqs.sort_unstable();
    assert_eq!(seqs, [Some(1), Some(2), Some(3), Some(4)]);
    assert!(
        member_when(&hub.admin, &thread, &b_member, |row| row["state"]
            == "active")
        .await?["pending"]
            .is_null()
    );
    assert!(
        timeout(Duration::from_millis(120), b_mock.next_submit())
            .await
            .is_err()
    );

    b_session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    let turn = required_submit(&b_mock, "thread fan-in to B").await?;
    assert_eq!(turn.participant_id, coordinator);
    assert!(turn.request.message_id.is_none());
    assert!(!turn.request.input.is_direct_human());
    let TurnInput::Thread(batch) = &turn.request.input else {
        return Err("coordinator did not receive a thread turn".into());
    };
    assert_eq!(batch.thread_id.as_str(), thread);
    assert_eq!((batch.from_seq, batch.to_seq), (1, NEW_POSTS));
    assert_eq!(batch.posts.len(), NEW_POSTS as usize);
    assert!(
        batch
            .posts
            .iter()
            .all(|post| post.mentions.contains(&b_member))
    );
    for kind in [AuthorKind::Human, AuthorKind::Agent, AuthorKind::Service] {
        assert!(batch.posts.iter().any(|post| post.author.kind == kind));
    }
    let path = batch
        .posts
        .iter()
        .flat_map(|post| &post.attachments)
        .next()
        .ok_or("missing relayed attachment")?;
    assert!(path.local_path.is_none());
    let local = PathBuf::from(OsString::from_vec(
        path.local_path_bytes
            .clone()
            .ok_or("non-UTF-8 path was lost")?,
    ));
    assert_eq!(local.file_name(), Some(OsStr::new(FILE_NAME)));
    assert_eq!(fs::read(local)?, FILE_BYTES);
    assert!(
        turn.request
            .input
            .render_canonical()?
            .contains(&format!("reply_to={thread}"))
    );
    let pending = member_when(&hub.admin, &thread, &b_member, |row| {
        row["pending"].is_object()
    })
    .await?;
    assert_eq!(
        pending["pending"]["delivery_id"],
        turn.request.operation_key
    );
    turn.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    member_when(&hub.admin, &thread, &b_member, |row| {
        row["confirmed_seq"] == NEW_POSTS
    })
    .await?;

    let (failed, reply) = mcp_call(b_session.credential.clone(), "thread_post", json!({
        "thread_id":thread, "body":"@coordinator reviewed", "operation_key":"coordinator-self-reply",
    })).await?;
    assert!(!failed, "MCP thread reply failed: {reply}");
    assert_eq!(reply["seq"], NEW_POSTS + 1);
    assert!(
        timeout(Duration::from_millis(120), b_mock.next_submit())
            .await
            .is_err()
    );

    let (outgoing, early_reply) = exercise_direct_reply(
        &hub, &a_session, &a_mock, &a_member, &b_session, &b_mock, &b_member,
    )
    .await?;
    let linked: Value = a_agent_client
        .get_json(&format!("/v1/messages/{early_reply}"), &[])
        .await?;
    assert_eq!(linked["in_reply_to"], outgoing.as_str());
    exercise_hub_native_reply(&hub, &reporter, &b_session, &b_mock, &b_enrollment.node_id).await?;
    exercise_board_api(&hub, &a, &a_session, &thread, &posts).await?;
    failed_open_does_not_interrupt_an_inflight_command(
        &hub,
        &b_session,
        &b_mock,
        &b_enrollment.node_id,
    )
    .await?;
    exercise_approval_redaction(&hub, &reporter, &b_session, &b_enrollment.node_id).await?;

    // Signal the Hub while both WebSockets are registered, before stopping either Node.
    hub.stop().await?;
    a.stop().await?;
    b.stop().await?;
    Ok(())
}

async fn post(client: &CoopletClient, path: &str, body: &Value, key: &str) -> TestResult<Value> {
    Ok(client.post_json(path, body, Some(key)).await?)
}

async fn exercise_direct_reply(
    hub: &BridgeHub,
    a: &MockSession,
    a_mock: &MockAdapter,
    a_member: &str,
    b: &MockSession,
    b_mock: &MockAdapter,
    b_member: &str,
) -> TestResult<(MessageId, MessageId)> {
    let b_node = b_member
        .split_once('/')
        .ok_or("expected a qualified member")?
        .0
        .parse()?;
    finish_live_turn(&hub.admin, &b_node, b).await?;
    let mut message = tokio::spawn(mcp_call(
        a.credential.clone(),
        "send_message",
        json!({
            "to":b_member, "body":"Could you send the report?", "operation_key":"direct-to-b",
        }),
    ));
    let first_turn = tokio::select! {
        turn = required_submit(b_mock, "direct message to B") => turn?,
        response = &mut message => return Err(io::Error::other(format!("Direct request to B returned before adapter intake: {:?}", response??)).into()),
    };
    let TurnInput::Direct(input) = &first_turn.request.input else {
        return Err("B did not receive a direct message".into());
    };
    let b_projection = input.message_id.clone();
    assert_eq!(input.author.kind, AuthorKind::Agent);
    assert_eq!(input.author.actor_id, a_member);
    assert_eq!(first_turn.request.message_id.as_ref(), Some(&b_projection));
    assert!(
        first_turn
            .request
            .input
            .render_canonical()?
            .contains(&format!("reply_to={a_member}"))
    );
    // A reply can arrive before A receives the original Hub receipt.
    let mut reply = tokio::spawn(mcp_call(
        b.credential.clone(),
        "send_message",
        json!({
            "to":a_member, "body":"The report is ready", "in_reply_to":b_projection,
            "operation_key":"direct-back-to-a",
        }),
    ));
    let turn = tokio::select! {
        turn = required_submit(a_mock, "early direct reply to A") => turn?,
        response = &mut reply => return Err(io::Error::other(format!("Early reply to A returned before adapter intake: {:?}", response??)).into()),
    };
    let TurnInput::Direct(input) = &turn.request.input else {
        return Err("A did not receive a direct reply".into());
    };
    assert_eq!(input.author.actor_id, b_member);
    assert_eq!(input.body, "The report is ready");
    assert!(input.in_reply_to.is_some());
    let reply_id = input.message_id.clone();
    turn.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    let (failed, report) = reply.await??;
    assert!(!failed, "MCP cross-machine reply failed: {report}");
    assert!(!message.is_finished());
    first_turn.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    let (failed, report) = message.await??;
    assert!(!failed, "MCP cross-machine message failed: {report}");
    let outgoing_id = field(&report, "message_id")?.parse()?;
    Ok((outgoing_id, reply_id))
}

async fn exercise_hub_native_reply(
    hub: &BridgeHub,
    reporter: &CoopletClient,
    destination: &MockSession,
    adapter: &MockAdapter,
    node: &NodeId,
) -> TestResult {
    for (sender, kind) in [
        (&hub.admin, AuthorKind::Human),
        (reporter, AuthorKind::Service),
    ] {
        let participant = &destination.target.participant_id;
        finish_live_turn(&hub.admin, node, destination).await?;
        let who = sender.whoami().await?;
        let sender_id = who.participant_id.ok_or("sender has no Hub participant")?;
        let address = format!("{}/{sender_id}", hub.id);
        let request = json!({"target_node_id":node,"target_participant_id":participant,"body":"Please confirm the result"});
        let key = format!("hub-{}-origin", kind.as_str());
        let sender = sender.clone();
        let send = tokio::spawn(async move { post(&sender, "/v1/messages", &request, &key).await });
        let turn =
            required_submit(adapter, &format!("Hub-native {kind:?} direct message to B")).await?;
        let TurnInput::Direct(input) = &turn.request.input else {
            return Err("Hub sender did not produce a direct turn".into());
        };
        let parent = input.message_id.clone();
        assert_eq!(input.author.actor_id, address);
        assert_eq!(input.author.kind, kind);
        assert!(input.author.source_node_id.is_none());
        assert_eq!(
            turn.request.input.is_direct_human(),
            kind == AuthorKind::Human
        );
        turn.finish(SubmitOutcome::Accepted {
            native_turn_id: None,
        })?;
        assert_eq!(send.await??["delivery_state"], "delivered");

        let (failed, reply) = mcp_call(destination.credential.clone(), "send_message", json!({
            "to":address,"in_reply_to":parent,"body":"I confirm", "operation_key":format!("reply-to-{}",kind.as_str()),
        })).await?;
        assert!(
            !failed,
            "reply to the Hub-native participant failed: {reply}"
        );
        assert_eq!(reply["delivery_state"], "delivered");
    }
    Ok(())
}

async fn failed_open_does_not_interrupt_an_inflight_command(
    hub: &BridgeHub,
    destination: &MockSession,
    adapter: &MockAdapter,
    node: &NodeId,
) -> TestResult {
    finish_live_turn(&hub.admin, node, destination).await?;
    let epoch = online_epoch(&hub.admin, node).await?;
    let recipient = &destination.target.participant_id;
    let body = json!({"target_node_id":node,"target_participant_id":recipient,"body":"Keep this command in flight"});
    let admin = hub.admin.clone();
    let mut existing = tokio::spawn(async move {
        post(
            &admin,
            "/v1/messages",
            &body,
            "in-flight-during-failed-open",
        )
        .await
    });
    let turn = tokio::select! {
        turn = required_submit(adapter, "command concurrent with failed open") => turn?,
        response = &mut existing => return Err(io::Error::other(format!("Command returned before adapter intake: {:?}", response??)).into()),
    };
    adapter.fail_next_open();
    let rejected = hub
        .admin
        .post_json::<_, Value>(
            "/v1/agents",
            &json!({"target_node_id":node,"client_kind":"mock","name":"failed-after-roster"}),
            Some("failed-bridged-open"),
        )
        .await;
    problem(rejected, ErrorCode::BridgeUnavailable, 502)?;
    wait_for(async || {
        let catalog: Value = hub
            .admin
            .get_json("/v1/participants", &[("node_id", node.to_string())])
            .await?;
        Ok(items(&catalog)?
            .iter()
            .any(|row| row["name"] == "failed-after-roster" && row["state"] == "stopped")
            .then_some(()))
    })
    .await?;
    assert_eq!(online_epoch(&hub.admin, node).await?, epoch);
    assert!(!existing.is_finished());
    turn.finish(SubmitOutcome::Accepted {
        native_turn_id: None,
    })?;
    assert_eq!(existing.await??["delivery_state"], "delivered");
    Ok(())
}

async fn exercise_approval_redaction(
    hub: &BridgeHub,
    reporter: &CoopletClient,
    session: &MockSession,
    node: &NodeId,
) -> TestResult {
    let participant = &session.target.participant_id;
    let mut cursor = hub
        .admin
        .list_events(&EventsQuery::default())
        .await?
        .latest_seq;
    let mut previous = None;
    for approval in ["initial approval", "replacement approval"] {
        session
            .emit(AdapterEventKind::ApprovalRequested(ApprovalWait {
                approval_id: Some(approval.into()),
                kind: ApprovalWaitKind::Permission,
                summary: Some(approval.into()),
                can_respond_remotely: true,
                action_hint: Some("Review the requested permission".into()),
            }))
            .await?;
        let admin_row = wait_for(async || {
            let catalog: Value = hub
                .admin
                .get_json("/v1/participants", &[("node_id", node.to_string())])
                .await?;
            Ok(items(&catalog)?
                .iter()
                .find(|row| {
                    row["participant_id"] == participant.as_str() && row["summary"] == approval
                })
                .cloned())
        })
        .await?;
        let id = field(&admin_row, "approval_id")?;
        assert_ne!(previous.as_deref(), Some(id));
        previous = Some(id.to_owned());
        assert_eq!(admin_row["can_respond_remotely"], true);

        let catalog: Value = reporter
            .get_json("/v1/participants", &[("node_id", node.to_string())])
            .await?;
        let redacted = items(&catalog)?
            .iter()
            .find(|row| row["participant_id"] == participant.as_str())
            .ok_or("catalog omitted the awaiting participant")?;
        assert_redacted_approval(redacted, approval);

        let query = EventsQuery {
            after: cursor,
            kinds: vec![EventKind::ParticipantAwaitingApproval],
            ..EventsQuery::default()
        };
        let events = reporter.list_events(&query).await?;
        let event = events
            .items
            .iter()
            .find(|event| {
                event.payload["participant_id"] == participant.as_str()
                    && event.payload["summary"] == approval
            })
            .ok_or("missing live redacted approval event")?;
        assert_redacted_approval(&event.payload, approval);
        let admin_events = hub.admin.list_events(&query).await?;
        assert!(
            admin_events
                .items
                .iter()
                .any(|event| event.payload["approval_id"] == id)
        );
        cursor = events.next_seq;
    }
    let rejected = reporter.post_json::<_, Value>(
        "/v1/messages", &json!({"target_node_id":node,"target_participant_id":participant,"body":"Waiting for input"}),
        Some("reporter-blocked-by-approval"),
    ).await;
    let error = problem(rejected, ErrorCode::AwaitingApproval, 409)?;
    assert_redacted_approval(&json!(error.extensions), "replacement approval");
    Ok(())
}

fn assert_redacted_approval(payload: &Value, summary: &str) {
    assert!(payload.get("approval_id").is_none());
    assert_eq!(payload["can_respond_remotely"], false);
    assert_eq!(payload["wait_kind"], "permission");
    assert_eq!(payload["summary"], summary);
}

async fn exercise_board_api(
    hub: &BridgeHub,
    a: &TestNode,
    agent: &MockSession,
    thread: &str,
    posts: &str,
) -> TestResult {
    let base = format!("/v1/threads/{thread}");
    let listing: Value = a.client.get_json("/v1/threads", &[]).await?;
    assert!(
        items(&listing)?
            .iter()
            .any(|item| item["thread_id"] == thread)
    );
    let pin_path = format!("{base}/pins/review");
    let pin: Value = a
        .client
        .put_json(
            &pin_path,
            &json!({"value_json":{"status":"open"},"expected_version":0}),
            Some("pin-create"),
        )
        .await?;
    let version = pin["version"].as_u64().ok_or("missing pin version")?;
    let error = a
        .client
        .put_json::<_, Value>(
            &pin_path,
            &json!({"value_json":"stale","expected_version":0}),
            Some("pin-conflict"),
        )
        .await;
    assert_eq!(
        problem(error, ErrorCode::PinConflict, 409)?.extensions["current_version"],
        version
    );
    let stale_delete = a
        .client
        .delete_json_with_query::<Value>(
            &pin_path,
            &[("expected_version", "0".into())],
            Some("stale-delete"),
        )
        .await;
    assert_eq!(
        problem(stale_delete, ErrorCode::PinConflict, 409)?.extensions["current_version"],
        version
    );
    let deleted: Value = a
        .client
        .delete_json_with_query(
            &pin_path,
            &[("expected_version", version.to_string())],
            Some("delete-pin"),
        )
        .await?;
    assert_eq!(deleted["deleted"], true);
    assert!(
        deleted["version"]
            .as_u64()
            .is_some_and(|next| next > version)
    );

    let wait_path = format!("{base}/wait");
    let ready = timeout(
        Duration::from_secs(2),
        a.client.post_json::<_, Value>(
            &wait_path,
            &json!({"since_seq":0,"wake_policy":"all"}),
            None,
        ),
    )
    .await??;
    assert_eq!(ready["status"], "ready");
    let waiting = timeout(
        Duration::from_secs(2),
        a.client
            .post_json::<_, Value>(&wait_path, &json!({"since_seq":ready["head_seq"]}), None),
    )
    .await??;
    assert_eq!(waiting["status"], "waiting");
    assert_eq!(waiting["head_seq"], ready["head_seq"]);

    let question = post(
        &a.client,
        posts,
        &json!({
            "body":"Is the change ready?", "question":{"prompt":"What should we do?","mode":"text"},
        }),
        "question",
    )
    .await?;
    let answer =
        json!({"body":"Ship it", "in_reply_to":question["post_id"], "answer":{"text":"Ship it"}});
    post(&hub.admin, posts, &answer, "accepted-answer").await?;
    let error = hub
        .admin
        .post_json::<_, Value>(posts, &answer, Some("another-answer"))
        .await;
    problem(error, ErrorCode::QuestionAlreadyAnswered, 409)?;

    // Age a committed fixture post without waiting for the 90-day retention period.
    let database = hub.root.0.join("hub.sqlite3");
    {
        let connection = rusqlite::Connection::open(database)?;
        connection.busy_timeout(Duration::from_secs(2))?;
        assert_eq!(
            connection.execute(
                "UPDATE thread_posts SET expires_at_ms = 1 WHERE thread_id = ?1 AND seq = 1",
                [thread]
            )?,
            1
        );
    }
    let (failed, read) = mcp_call(
        agent.credential.clone(),
        "thread_read",
        json!({"thread_id":thread,"since_seq":0}),
    )
    .await?;
    assert!(
        failed,
        "MCP unexpectedly read behind the retained floor: {read}"
    );
    assert_eq!(read["code"], "resync_required");
    assert_eq!(read["extensions"]["floor_seq"], 1);
    assert!(
        read["extensions"]["head_seq"]
            .as_u64()
            .is_some_and(|head| head >= NEW_POSTS + 3)
    );

    let mut command = node_cli(&a.state_dir);
    command.args(["thread", "close", thread, "--operation-key", "thread-close"]);
    let closed = cli_json(command).await?;
    assert_eq!(closed["status"], "closed");
    Ok(())
}

fn node_cli(state_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cooplet"));
    command.arg("--state-dir").arg(state_dir);
    command
}

async fn cli_json(mut command: Command) -> TestResult<Value> {
    let output = tokio::task::spawn_blocking(move || command.output()).await??;
    assert!(
        output.status.success(),
        "Node CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

async fn mcp_call(credential: PathBuf, tool: &str, arguments: Value) -> TestResult<(bool, Value)> {
    let call = json!({"jsonrpc":"2.0", "id":2, "method":"tools/call", "params":{"name":tool,"arguments":arguments}});
    tokio::task::spawn_blocking(move || {
        let mut child = Process(Command::new(env!("CARGO_BIN_EXE_cooplet"))
            .args(["node", "mcp", "--participant-cred"])
            .arg(credential)
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn()?);
        let mut stdin = child.0.stdin.take().ok_or("MCP stdin was not piped")?;
        for request in [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"bridge-e2e","version":"1"},
            }}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            call,
        ] {
            writeln!(stdin, "{request}")?;
        }
        drop(stdin);
        use std::io::Read;
        let mut stdout = String::new();
        child.0.stdout.take().ok_or("MCP stdout was not piped")?.read_to_string(&mut stdout)?;
        let status = child.0.wait()?;
        assert!(status.success(), "MCP subprocess exited with {status}");
        let response: Value = serde_json::from_str(stdout.lines().nth(1).ok_or("missing tool response")?)?;
        let result = &response["result"];
        let failed = result["isError"].as_bool().ok_or_else(|| io::Error::other(format!("unexpected MCP response: {response}")))?;
        let data = serde_json::from_str(field(&result["content"][0], "text")?)?;
        Ok((failed, data))
    }).await?
}

fn session(adapter: &MockAdapter, id: &ParticipantId) -> TestResult<MockSession> {
    adapter
        .session(id, 1)
        .ok_or_else(|| "mock session was not registered".into())
}

async fn required_submit(mock: &MockAdapter, scenario: &str) -> TestResult<MockSubmit> {
    mock.next_submit().await.map_err(|error| {
        io::Error::other(format!(
            "{scenario} did not reach the native adapter: {error}"
        ))
        .into()
    })
}

fn field<'a>(value: &'a Value, key: &str) -> TestResult<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| io::Error::other(format!("missing string field {key}")).into())
}

fn items(page: &Value) -> TestResult<&[Value]> {
    page["items"]
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| io::Error::other(format!("missing items in {page}")).into())
}

fn problem(
    result: Result<Value, ClientError>,
    code: ErrorCode,
    status: u16,
) -> TestResult<cooplet_protocol::ProblemDetails> {
    let Err(ClientError::Problem(problem)) = result else {
        return Err(
            io::Error::other(format!("expected a problem response, got {result:?}")).into(),
        );
    };
    assert_eq!(problem.code, code);
    assert_eq!(problem.status, status);
    Ok(*problem)
}

async fn wait_for<T>(mut check: impl AsyncFnMut() -> TestResult<Option<T>>) -> TestResult<T> {
    timeout(WAIT, async {
        loop {
            if let Some(value) = check().await? {
                return Ok(value);
            }
            sleep(Duration::from_millis(15)).await;
        }
    })
    .await?
}

async fn online(client: &CoopletClient, id: &NodeId) -> TestResult {
    online_epoch(client, id).await?;
    Ok(())
}

async fn online_epoch(client: &CoopletClient, id: &NodeId) -> TestResult<u64> {
    wait_for(async || {
        let nodes: Value = client.get_json("/v1/nodes", &[]).await?;
        Ok(items(&nodes)?
            .iter()
            .find(|node| node["node_id"] == id.as_str() && node["status"] == "online")
            .and_then(|node| node["session_epoch"].as_u64()))
    })
    .await
}

async fn participant_state(
    client: &CoopletClient,
    node: &NodeId,
    participant: &ParticipantId,
    state: &str,
) -> TestResult {
    wait_for(async || {
        let participants: Value = client
            .get_json("/v1/participants", &[("node_id", node.to_string())])
            .await?;
        Ok(items(&participants)?
            .iter()
            .any(|view| view["participant_id"] == participant.as_str() && view["state"] == state)
            .then_some(()))
    })
    .await
}

// The mock hands back an uncorrelated acceptance. Its native Working to Idle
// transition releases the shared submission slot for the next turn.
async fn finish_live_turn(hub: &CoopletClient, node: &NodeId, session: &MockSession) -> TestResult {
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ))
        .await?;
    participant_state(hub, node, &session.target.participant_id, "working").await?;
    session
        .emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Idle,
        ))
        .await?;
    participant_state(hub, node, &session.target.participant_id, "idle").await
}

async fn member_when(
    client: &CoopletClient,
    thread: &str,
    member: &str,
    predicate: impl Fn(&Value) -> bool,
) -> TestResult<Value> {
    let url = format!("/v1/threads/{thread}/members");
    wait_for(async || {
        let page: Value = client.get_json(&url, &[]).await?;
        Ok(items(&page)?
            .iter()
            .find(|row| row["member_key"] == member && predicate(row))
            .cloned())
    })
    .await
}
