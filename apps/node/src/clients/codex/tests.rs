use std::{error::Error, io, time::Duration};

use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{BlobId, MessageId, ParticipantId, ThreadId};
use futures_util::{SinkExt, StreamExt};
use tokio::{net::UnixStream, sync::mpsc, time::timeout};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

use super::*;
use crate::clients::{
    AdapterEvent, AdapterEventKind, ApprovalAction, ApprovalWaitKind, InboundMessage,
    InboundThreadBatch, InboundThreadPost, ResolvedAttachment, TurnFinishStatus, VerifiedAuthor,
};

type Test<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const NATIVE_THREAD: &str = "native-thread";
const LIMIT: Duration = Duration::from_secs(3);

struct Peer(WebSocketStream<UnixStream>);

impl Peer {
    async fn send(&mut self, value: Value) -> Test {
        Ok(self.0.send(Message::Text(value.to_string().into())).await?)
    }

    async fn recv(&mut self) -> Test<Value> {
        let frame = timeout(LIMIT, self.0.next())
            .await?
            .ok_or_else(|| io::Error::other("missing RPC frame"))??;
        Ok(serde_json::from_str(frame.to_text()?)?)
    }

    async fn ack(&mut self, rpc: &Value, value: Value) -> Test {
        self.send(json!({"id": rpc["id"], "result": value})).await
    }

    async fn event(&mut self, method: &str, mut params: Value) -> Test {
        params["threadId"] = json!(NATIVE_THREAD);
        self.send(json!({"method": method, "params": params})).await
    }

    async fn state(&mut self, status: Value) -> Test {
        self.event("thread/status/changed", json!({"status": status}))
            .await
    }

    async fn begin(&mut self, turn: &str) -> Test {
        self.event("turn/started", json!({"turn": {"id": turn}}))
            .await
    }

    async fn finish(&mut self, turn: &str, status: &str, items: Value) -> Test {
        self.event(
            "turn/completed",
            json!({"turn": {"id": turn, "status": status, "items": items}}),
        )
        .await
    }

    async fn callback(&mut self, id: Value, method: &str, mut params: Value) -> Test {
        params["threadId"] = json!(NATIVE_THREAD);
        params["turnId"] = json!("t");
        self.send(json!({"method": method, "id": id, "params": params}))
            .await
    }
}

async fn live(
    revision: Option<u64>,
    schema: bool,
) -> Test<(Peer, SessionHandle, mpsc::Receiver<AdapterEvent>)> {
    let (client, server) = UnixStream::pair()?;
    let (session, events) = Session::start(
        WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        7,
        "cooplet node attach fixture".into(),
    );
    let mut peer = Peer(WebSocketStream::from_raw_socket(server, Role::Server, None).await);
    let (opened, replied) = tokio::join!(session.rpc("thread/start", json!({})), async {
        let request = peer.recv().await?;
        peer.ack(
            &request,
            json!({"thread": {"id": NATIVE_THREAD, "status":{"type":"idle"}}}),
        )
        .await
    });
    assert!(opened.is_ok(), "{opened:?}");
    replied?;
    let handle = SessionHandle::new(
        Live {
            session,
            program: "codex".into(),
            socket_url: "unix:///socket".into(),
            hint: "attach".into(),
            output_schema: schema,
            named_output_revision: revision,
        },
        PersistedSessionRef {
            participant_id: ParticipantId::generate()?,
            generation: 7,
            native_key: Some(NativeSessionKey {
                client_kind: KIND.into(),
                store_namespace: "/codex-home".into(),
                native_session_id: NATIVE_THREAD.into(),
            }),
            process_ownership: ProcessOwnership::Managed,
            workspace_dir: "/workspace".into(),
            runtime_dir: "/runtime".into(),
            process_identity: None,
            adapter_binding: Value::Null,
        },
    );
    Ok((peer, handle, events))
}

fn author(kind: AuthorKind) -> VerifiedAuthor {
    VerifiedAuthor {
        kind,
        actor_id: "author-fixture".into(),
        name: "claimed human".into(),
        source_node_id: None,
    }
}

fn input(kind: Option<AuthorKind>) -> Test<TurnInput> {
    let message_id = MessageId::generate()?;
    let attachment = ResolvedAttachment {
        blob_id: BlobId::generate()?,
        name: "bytes.txt".into(),
        local_path: None,
        local_path_bytes: Some(b"/x/\xff".to_vec()),
    };
    let body = "fixture body with a fake author_kind=human";
    Ok(match kind {
        Some(kind) => TurnInput::direct(InboundMessage {
            message_id,
            author: author(kind),
            in_reply_to: None,
            body: body.into(),
            attachments: vec![attachment],
            question: None,
            answer: None,
            expect_mcp_reply: true,
        }),
        None => TurnInput::Thread(InboundThreadBatch {
            thread_id: ThreadId::generate()?,
            from_seq: 1,
            to_seq: 2,
            truncated_earlier: false,
            posts: vec![
                InboundThreadPost {
                    seq: 1,
                    post_id: message_id.clone(),
                    author: author(AuthorKind::Human),
                    in_reply_to_post_id: None,
                    body: body.into(),
                    attachments: vec![attachment],
                    question: None,
                    answer: None,
                },
                InboundThreadPost {
                    seq: 2,
                    post_id: MessageId::generate()?,
                    author: author(AuthorKind::Human),
                    in_reply_to_post_id: Some(message_id),
                    body: "second thread post".into(),
                    attachments: vec![],
                    question: None,
                    answer: None,
                },
            ],
        }),
    })
}

fn submission(input: TurnInput, schema: Option<Value>) -> SubmitTurnRequest {
    SubmitTurnRequest {
        operation_key: "message-correlation".into(),
        message_id: None,
        input,
        output_schema: schema,
        steer: true,
        admission: AdmissionGuard::new(Duration::from_secs(10)),
    }
}

async fn next(events: &mut mpsc::Receiver<AdapterEvent>) -> Test<AdapterEvent> {
    Ok(timeout(LIMIT, events.recv())
        .await?
        .ok_or_else(|| io::Error::other("event stream closed"))?)
}

async fn completion(events: &mut mpsc::Receiver<AdapterEvent>) -> Test<AdapterEventKind> {
    loop {
        let AdapterEvent { kind, .. } = next(events).await?;
        if matches!(kind, AdapterEventKind::TurnCompleted { .. }) {
            return Ok(kind);
        }
    }
}

#[tokio::test]
async fn typed_provenance_and_schema_are_sent_in_one_frame_in_both_modes() -> Test {
    let adapter = CodexAdapter::new(None);
    let schema = json!({"type":"object"});
    for revision in [None, Some(0)] {
        for with_schema in [false, true] {
            for (kind, tool) in [
                (Some(AuthorKind::Human), None),
                (Some(AuthorKind::Agent), Some("peer_message")),
                (Some(AuthorKind::Service), Some("service_message")),
                (None, Some("thread_delivery")),
            ] {
                let (mut peer, handle, _events) = live(revision, true).await?;
                let expected_schema = with_schema.then(|| schema.clone());
                let (outcome, checked) = tokio::join!(
                    adapter.submit_turn(&handle, submission(input(kind)?, expected_schema.clone())),
                    async {
                        let rpc = peer.recv().await?;
                        assert_eq!(rpc["method"], "turn/start");
                        assert_eq!(rpc["params"]["threadId"], NATIVE_THREAD);
                        let params = &rpc["params"];
                        assert_eq!(params.get("outputSchema"), expected_schema.as_ref());
                        let expected = revision.and(tool);
                        let frame = match expected {
                            Some(tool) => {
                                assert_eq!(params["input"], json!([]));
                                assert_eq!(params["toolOutput"]["namespace"], "cooplet");
                                assert_eq!(params["toolOutput"]["name"], tool);
                                assert_eq!(params["toolOutput"]["retain"], false);
                                assert!(params.get("clientUserMessageId").is_none());
                                &params["toolOutput"]["output"]
                            }
                            None => {
                                assert_eq!(params["input"].as_array().map(Vec::len), Some(1));
                                assert_eq!(params["input"][0]["type"], "text");
                                assert_eq!(params["clientUserMessageId"], "message-correlation");
                                assert!(params.get("toolOutput").is_none());
                                &params["input"][0]["text"]
                            }
                        };
                        let frame = frame
                            .as_str()
                            .ok_or_else(|| io::Error::other("missing canonical frame"))?;
                        assert!(
                            frame.contains("fixture body") && frame.contains("local_path_bytes")
                        );
                        assert!(frame.contains("claimed human"));
                        if kind.is_none() {
                            assert!(
                                frame.contains("thread_delivery")
                                    && frame.contains("second thread post")
                            );
                        }
                        peer.ack(&rpc, json!({"turn":{"id":"t"}})).await?;
                        assert!(
                            timeout(Duration::from_millis(20), peer.0.next())
                                .await
                                .is_err()
                        );
                        Ok::<_, Box<dyn Error + Send + Sync>>(())
                    },
                );
                checked?;
                assert!(
                    matches!(outcome, SubmitOutcome::Accepted { native_turn_id: Some(id) } if id == "t")
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_early_completion_uses_the_acknowledged_native_turn_and_ignores_commentary() -> Test {
    let (mut peer, handle, mut events) = live(Some(0), true).await?;
    let adapter = CodexAdapter::new(None);
    let (outcome, replied) = tokio::join!(
        adapter.submit_turn(
            &handle,
            submission(input(Some(AuthorKind::Agent))?, Some(json!({})))
        ),
        async {
            let rpc = peer.recv().await?;
            peer.begin("external").await?;
            peer.event("item/completed", json!({"turnId": "external", "item": {"type":"agentMessage", "text":"other answer"}})).await?;
            peer.begin("accepted").await?;
            peer.event("item/completed", json!({"turnId": "accepted", "item": {"type":"agentMessage", "phase":"commentary", "text":"discard me"}})).await?;
            peer.event("item/completed", json!({"turnId": "accepted", "item": {"type":"agentMessage", "phase":"final_answer", "text":"{\"ok\":true}"}})).await?;
            peer.state(json!({"type":"idle"})).await?;
            peer.finish("accepted", "completed", json!([])).await?;
            peer.finish("accepted", "completed", json!([])).await?;
            peer.finish("external", "interrupted", json!([])).await?;
            peer.ack(&rpc, json!({"turn":{"id":"accepted"}})).await
        },
    );
    replied?;
    assert!(
        matches!(outcome, SubmitOutcome::Accepted { native_turn_id: Some(id) } if id == "accepted")
    );
    assert!(
        matches!(completion(&mut events).await?, AdapterEventKind::TurnCompleted {
        native_turn_id: Some(id), status: TurnFinishStatus::Completed, structured: Some(value), text, operation_key: None,
    } if id == "accepted" && text == "{\"ok\":true}" && value == json!({"ok":true}))
    );
    assert!(
        matches!(completion(&mut events).await?, AdapterEventKind::TurnCompleted {
        native_turn_id: Some(id), status: TurnFinishStatus::Cancelled, structured: None, text, ..
    } if id == "external" && text == "other answer")
    );
    assert!(
        timeout(Duration::from_millis(25), events.recv())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn accounts_invalidate_route_and_native_rejections_are_classified_after_one_write() -> Test {
    let adapter = CodexAdapter::new(None);
    for (code, message, verdict) in [
        (
            -32603,
            "failed to submit turn input: ActiveTurnOutputSchemaMismatch",
            "busy",
        ),
        (-32603, "unexpected internal error", "unknown"),
        (-32600, "thread not found: native-thread", "unavailable"),
        (-32001, "Server overloaded; retry later.", "unavailable"),
    ] {
        let (mut peer, handle, _events) = live(Some(0), true).await?;
        peer.send(json!({"method":"account/updated","params":{}}))
            .await?;
        let session = &handle
            .downcast_ref::<Live>()
            .ok_or_else(|| io::Error::other("missing live fixture"))?
            .session;
        timeout(LIMIT, async {
            while session.snapshot().route_revision == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        let (outcome, replied) = tokio::join!(
            adapter.submit_turn(&handle, submission(input(Some(AuthorKind::Service))?, None)),
            async {
                let rpc = peer.recv().await?;
                assert!(rpc["params"].get("toolOutput").is_none());
                peer.send(json!({"id":rpc["id"],"error":{"code":code,"message":message}}))
                    .await?;
                assert!(
                    timeout(Duration::from_millis(25), peer.0.next())
                        .await
                        .is_err()
                );
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            }
        );
        replied?;
        assert!(matches!(
            (verdict, outcome),
            ("busy", SubmitOutcome::Busy)
                | ("unavailable", SubmitOutcome::Unavailable(_))
                | ("unknown", SubmitOutcome::Unknown(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn admission_interrupt_and_provider_failure_follow_native_state() -> Test {
    let (mut peer, handle, mut events) = live(None, true).await?;
    let adapter = CodexAdapter::new(None);
    peer.begin("t").await?;
    assert!(matches!(
        next(&mut events).await?.kind,
        AdapterEventKind::StateChanged(State::Working)
    ));
    assert!(matches!(
        adapter
            .submit_turn(&handle, submission(input(Some(AuthorKind::Human))?, None))
            .await,
        SubmitOutcome::Busy
    ));
    let (stop, reply) = tokio::join!(
        adapter.cancel_or_stop(&handle, StopMode::CancelTurn),
        async {
            let rpc = peer.recv().await?;
            assert_eq!(rpc["method"], "turn/interrupt");
            assert_eq!(rpc["params"]["turnId"], "t");
            peer.ack(&rpc, json!({})).await?;
            peer.finish("t", "interrupted", json!([])).await
        },
    );
    reply?;
    assert!(matches!(stop?, StopOutcome::TurnCancelled));
    assert!(matches!(
        completion(&mut events).await?,
        AdapterEventKind::TurnCompleted {
            status: TurnFinishStatus::Cancelled,
            ..
        }
    ));
    assert!(matches!(
        next(&mut events).await?.kind,
        AdapterEventKind::StateChanged(State::Idle)
    ));

    let mut expired = submission(input(Some(AuthorKind::Agent))?, None);
    expired.admission = AdmissionGuard::new(Duration::ZERO);
    assert!(matches!(
        adapter.submit_turn(&handle, expired).await,
        SubmitOutcome::Expired
    ));
    assert!(
        timeout(Duration::from_millis(25), peer.0.next())
            .await
            .is_err()
    );

    for (id, status) in [("provider-failure", "failed"), ("bad-json", "completed")] {
        let (outcome, replied) = tokio::join!(
            adapter.submit_turn(
                &handle,
                submission(input(Some(AuthorKind::Human))?, Some(json!({})))
            ),
            async {
                let rpc = peer.recv().await?;
                peer.ack(&rpc, json!({"turn":{"id":id}})).await?;
                peer.finish(
                    id,
                    status,
                    json!([{"type":"agentMessage","text":"not JSON"}]),
                )
                .await
            }
        );
        replied?;
        assert!(
            matches!(outcome, SubmitOutcome::Accepted { native_turn_id: Some(actual) } if actual == id)
        );
        assert!(matches!(
            completion(&mut events).await?,
            AdapterEventKind::TurnCompleted {
                status: TurnFinishStatus::Failed,
                structured: None,
                ..
            }
        ));
        assert!(matches!(
            next(&mut events).await?.kind,
            AdapterEventKind::StateChanged(State::Idle)
        ));
    }
    Ok(())
}

async fn approval(events: &mut mpsc::Receiver<AdapterEvent>) -> Test<String> {
    loop {
        if let AdapterEventKind::ApprovalRequested(wait) = next(events).await?.kind {
            return Ok(wait
                .approval_id
                .ok_or_else(|| io::Error::other("missing approval id"))?);
        }
    }
}

#[tokio::test]
async fn written_turn_without_ack_is_unknown_and_transport_closes() -> Test {
    let (mut peer, handle, mut events) = live(None, true).await?;
    let adapter = CodexAdapter::new(None);
    tokio::time::pause();
    let (outcome, received) = tokio::join!(
        adapter.submit_turn(&handle, submission(input(Some(AuthorKind::Service))?, None)),
        async {
            let rpc = peer.recv().await?;
            assert_eq!(rpc["method"], "turn/start");
            tokio::time::advance(Duration::from_secs(13)).await;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        },
    );
    received?;
    assert!(matches!(outcome, SubmitOutcome::Unknown(_)));
    assert!(timeout(LIMIT, events.recv()).await?.is_none());
    assert!(matches!(
        adapter
            .submit_turn(&handle, submission(input(Some(AuthorKind::Service))?, None))
            .await,
        SubmitOutcome::Unavailable(_)
    ));
    Ok(())
}

#[tokio::test]
async fn separate_native_approval_ids_and_unknown_application_do_not_allow_a_replay() -> Test {
    let (mut peer, handle, mut events) = live(None, true).await?;
    let adapter = CodexAdapter::new(None);
    peer.begin("t").await?;
    peer.state(json!({"type":"active","activeFlags":["waitingOnUserInput"]}))
        .await?;
    assert!(matches!(
        next(&mut events).await?.kind,
        AdapterEventKind::StateChanged(State::Working)
    ));
    assert!(
        matches!(next(&mut events).await?.kind, AdapterEventKind::StateChanged(State::AwaitingApproval(wait))
        if wait.kind == ApprovalWaitKind::Question && !wait.can_respond_remotely)
    );
    assert!(matches!(
        adapter
            .respond_approval(
                &handle,
                ApprovalDecision {
                    approval_id: "status-only".into(),
                    action: ApprovalAction::Approve,
                }
            )
            .await,
        ApprovalRespondOutcome::Unsupported(_)
    ));
    peer.callback(json!(1), "item/commandExecution/requestApproval", json!({}))
        .await?;
    peer.callback(
        json!("1"),
        "item/permissions/requestApproval",
        json!({"permissions":{"network":{"enabled":true}}}),
    )
    .await?;
    let first = approval(&mut events).await?;
    assert!(
        matches!(adapter.submit_turn(&handle, submission(input(Some(AuthorKind::Human))?, None)).await,
        SubmitOutcome::AwaitingApproval(wait) if wait.approval_id.as_deref() == Some(first.as_str()))
    );
    // The TUI resolves the numeric request before the operator writes.
    peer.event("serverRequest/resolved", json!({"requestId": 1}))
        .await?;
    let second = approval(&mut events).await?;
    assert_ne!(first, second);
    assert!(matches!(
        adapter
            .respond_approval(
                &handle,
                ApprovalDecision {
                    approval_id: first,
                    action: ApprovalAction::Approve,
                }
            )
            .await,
        ApprovalRespondOutcome::Stale(_)
    ));

    let decision = ApprovalDecision {
        approval_id: second,
        action: ApprovalAction::Approve,
    };
    let (response, checked) = tokio::join!(
        adapter.respond_approval(&handle, decision.clone()),
        async {
            let reply = peer.recv().await?;
            assert_eq!(
                reply,
                json!({"id":"1","result":{"scope":"turn","permissions":{"network":{"enabled":true}}}})
            );
            peer.event("serverRequest/resolved", json!({"requestId": "1"}))
                .await?;
            peer.state(json!({"type":"active","activeFlags":[]})).await
        }
    );
    checked?;
    assert!(matches!(response, ApprovalRespondOutcome::Unknown(_)));
    assert!(matches!(
        adapter.respond_approval(&handle, decision).await,
        ApprovalRespondOutcome::Stale(_)
    ));
    assert!(
        timeout(Duration::from_millis(25), peer.0.next())
            .await
            .is_err()
    );
    Ok(())
}

#[path = "../../../tests/common/codex_backend.rs"]
mod codex_backend;

#[tokio::test]
#[ignore = "requires an installed Codex CLI on the host"]
async fn native_structured_smoke() -> Test {
    use cooplet_platform::{PrivateFs, ProcessHandle, TerminateOutcome};
    use cooplet_protocol::RequestId;

    struct TestTree(PathBuf);

    impl Drop for TestTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    let binary: PathBuf = env::var_os("COOPLET_NATIVE_CODEX_BIN")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("set COOPLET_NATIVE_CODEX_BIN to the native CLI path"))?;
    let root = TestTree(PrivateFs::ensure_private_dir(
        &env::temp_dir().join(format!("cx-{}", RequestId::generate()?)),
    )?);
    let home = PrivateFs::ensure_private_dir(&root.0.join("home"))?;
    let workspace = PrivateFs::ensure_private_dir(&root.0.join("work"))?;
    let mut backend = codex_backend::Backend::start(&home).await?;
    let features = wire_features(&binary, &root.0.join("schema"), &home).await;
    assert!(
        features.output_schema,
        "the native schema generator must report outputSchema"
    );
    let socket = root.0.join("c.sock");
    let socket_url = format!("unix://{}", path_text(&socket)?);
    let mut launch = Command::new(&binary);
    launch
        .args(["app-server", "--listen", &socket_url])
        .current_dir(&workspace)
        .env("CODEX_HOME", &home)
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env("HTTP_PROXY", "http://127.0.0.1:1")
        .env("HTTPS_PROXY", "http://127.0.0.1:1")
        .env("ALL_PROXY", "http://127.0.0.1:1")
        .env("NO_PROXY", "localhost,127.0.0.1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut process = ProcessHandle::spawn_isolated_group(&mut launch)?;
    let socket = transport::connect(&socket, process.identity()).await?;
    let (session, mut events) = Session::start(socket, 1, "attach".into());
    let checked = |result: std::result::Result<Value, Failure>| {
        result.map_err(|error| io::Error::other(format!("native smoke RPC: {error:?}")))
    };
    checked(session.rpc("initialize", initialize_params()).await)?;
    checked(session.request(Operation::Initialized).await)?;
    let opened = checked(session.rpc("thread/start", json!({"cwd":workspace})).await)?;
    let thread = native_id(&opened["thread"]["id"])?;
    let handle = SessionHandle::new(
        Live {
            session,
            program: binary,
            socket_url,
            hint: "attach".into(),
            output_schema: features.output_schema,
            named_output_revision: None,
        },
        PersistedSessionRef {
            participant_id: ParticipantId::generate()?,
            generation: 1,
            native_key: Some(NativeSessionKey {
                client_kind: KIND.into(),
                store_namespace: path_text(&home)?.into(),
                native_session_id: thread.into(),
            }),
            process_ownership: ProcessOwnership::Managed,
            workspace_dir: workspace,
            runtime_dir: root.0.clone(),
            process_identity: Some(process.identity().clone()),
            adapter_binding: Value::Null,
        },
    );
    let adapter = CodexAdapter::new(None);
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    let outcome = adapter
        .submit_turn(
            &handle,
            submission(input(Some(AuthorKind::Human))?, Some(schema.clone())),
        )
        .await;
    let SubmitOutcome::Accepted {
        native_turn_id: Some(accepted),
    } = outcome
    else {
        return Err(io::Error::other(format!("expected native intake, got {outcome:?}")).into());
    };
    assert_eq!(backend.request().await?["text"]["format"]["schema"], schema);
    assert!(
        matches!(completion(&mut events).await?, AdapterEventKind::TurnCompleted {
        native_turn_id: Some(id), status: TurnFinishStatus::Completed, structured: Some(value), ..
    } if id == accepted && value == json!({"answer":42}))
    );
    assert!(matches!(
        adapter
            .cancel_or_stop(&handle, StopMode::StopSession)
            .await?,
        StopOutcome::Stopped
    ));
    assert_ne!(
        process
            .terminate_and_reap(Duration::from_millis(200))?
            .outcome,
        TerminateOutcome::CleanupUnverified
    );
    Ok(())
}

#[tokio::test]
async fn interrupt_refusal_for_a_replaced_turn_reports_the_old_turn_gone() -> Test {
    let (mut peer, handle, mut events) = live(None, true).await?;
    let adapter = CodexAdapter::new(None);
    peer.begin("old").await?;
    assert!(matches!(
        next(&mut events).await?.kind,
        AdapterEventKind::StateChanged(State::Working)
    ));
    let (outcome, checked) = tokio::join!(
        adapter.cancel_or_stop(&handle, StopMode::CancelTurn),
        async {
            let request = peer.recv().await?;
            assert_eq!(request["method"], "turn/interrupt");
            assert_eq!(request["params"]["turnId"], "old");
            peer.finish("old", "completed", json!([])).await?;
            peer.begin("new").await?;
            peer.send(json!({"id":request["id"],"error":{"code":-32600,
                "message":"expected active turn id old but found new"}}))
                .await?;
            assert!(
                timeout(Duration::from_millis(25), peer.0.next())
                    .await
                    .is_err()
            );
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }
    );
    checked?;
    assert!(matches!(outcome?, StopOutcome::AlreadyGone));
    Ok(())
}

#[tokio::test]
async fn initial_workspace_routing_update_rechecks_evidence_once() -> Test {
    let (mut peer, handle, _events) = live(None, true).await?;
    let session = &handle
        .downcast_ref::<Live>()
        .ok_or_else(|| io::Error::other("missing live fixture"))?
        .session;
    let opened = json!({"modelProvider":"openai","thread":{"modelProvider":"openai"}});
    let (revision, fixture) = tokio::join!(
        provider::named_output_revision(session, &opened, "/workspace", true),
        async {
            for iteration in 0..2 {
                for _ in 0..4 {
                    let request = peer.recv().await?;
                    let response = match request["method"].as_str() {
                        Some("account/read") => {
                            assert_eq!(request["params"]["refreshToken"], false);
                            if iteration == 0 {
                                peer.send(json!({"method":"account/updated","params":{"reason":"workspaceRoutingReady"}})).await?;
                            }
                            json!({"requiresOpenaiAuth":true,"account":{"type":"chatgpt"},
                                "workspaceRouting":{"backendOrigin":"https://chatgpt.com","accountRoutingOverride":"NO_CONSTRAINT"}})
                        }
                        Some("config/read") => json!({"config":{}}),
                        Some("configRequirements/read") => json!({"requirements":null}),
                        Some("modelProvider/list") => {
                            json!({"data":[{"id":"openai","baseUrl":null}]})
                        }
                        _ => return Err(io::Error::other("unexpected provider probe").into()),
                    };
                    peer.ack(&request, response).await?;
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        },
    );
    fixture?;
    assert_eq!(revision, Some(1));
    Ok(())
}

#[tokio::test]
async fn cancel_after_transport_loss_reports_uncertainty_instead_of_missing_support() -> Test {
    let (mut peer, handle, mut events) = live(None, true).await?;
    let adapter = CodexAdapter::new(None);
    peer.begin("active").await?;
    assert!(matches!(
        next(&mut events).await?.kind,
        AdapterEventKind::StateChanged(State::Working)
    ));
    drop(peer);
    assert!(timeout(LIMIT, events.recv()).await?.is_none());
    let cancelled = adapter
        .cancel_or_stop(&handle, StopMode::CancelTurn)
        .await?;
    assert!(matches!(cancelled, StopOutcome::Unknown(_)));
    Ok(())
}
