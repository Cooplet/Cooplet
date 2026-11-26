use cooplet_protocol::*;
use serde_json::{Value, json};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const OP: &str = "op_00000000000000000000000000";
const PART: &str = "part_00000000000000000000000000";
const THREAD: &str = "thr_00000000000000000000000000";

fn strict_round_trip<P: BridgePayload + PartialEq + std::fmt::Debug>(value: Value) -> TestResult {
    let payload: P = serde_json::from_value(value)?;
    let mut frame = BridgeFrame::encode(3, &payload)?;
    assert_eq!(
        BridgeFrame::from_json(&frame.to_json()?)?.decode_payload::<P>()?,
        payload
    );
    let mut with_unknown = serde_json::to_value(&frame)?;
    with_unknown["extra"] = json!(1);
    assert!(serde_json::from_value::<BridgeFrame>(with_unknown).is_err());
    frame.payload["extra"] = json!(1);
    assert!(frame.decode_payload::<P>().is_err());
    frame.v = 2;
    assert!(matches!(
        frame.decode_payload::<P>(),
        Err(ProtocolError::BridgeVersion(2))
    ));
    frame.v = 1;
    frame.kind = if P::KIND == FrameKind::HelloAck {
        FrameKind::NodeEvent
    } else {
        FrameKind::HelloAck
    };
    assert!(matches!(
        frame.decode_payload::<P>(),
        Err(ProtocolError::BridgeKind { .. })
    ));
    Ok(())
}

fn empty_roster() -> Value {
    json!({"chunk_index":0,"is_final":true,"participants":[],"templates":[],"workflows":[],
        "unresolved_thread_fences":[],"resolved_op_receipts":[]})
}

#[test]
fn all_seven_bridge_payloads_are_strict() -> TestResult {
    strict_round_trip::<HelloAckPayload>(json!({
        "hub_id":"hub_00000000000000000000000000","node_id":"node_00000000000000000000000000",
        "max_inflight_commands":8,"max_inflight_threads":16,"pending_op_ids":[]
    }))?;
    strict_round_trip::<CommandReqPayload>(json!({
        "op_id":OP,"actor_id":"operator:alfa","admission_budget_ms":100,"delivery_mode":"direct_now",
        "command":{"type":"stop_agent","participant_id":PART}
    }))?;
    strict_round_trip::<CommandResPayload>(json!({"op_id":OP,"outcome":"unknown"}))?;
    strict_round_trip::<ThreadDeliverPayload>(json!({
        "delivery_id":OP,"thread_id":THREAD,"member_id":PART,"from_seq":6,"to_seq":10,
        "posts":[],"delivery_mode":"queue_until_idle","admission_budget_ms":100
    }))?;
    strict_round_trip::<ThreadAckPayload>(json!({
        "delivery_id":OP,"thread_id":THREAD,"member_id":PART,"acked_seq":10,"outcome":"delivered"
    }))?;
    strict_round_trip::<RosterSyncPayload>(empty_roster())?;
    strict_round_trip::<NodeEventPayload>(json!({
        "type":"participant_state_changed","participant_id":PART,"state":"awaiting_approval",
        "approval_id":"a1","action_hint":"review","summary":"Read file","wait_kind":"permission",
        "can_respond_remotely":true
    }))?;
    let unknown = json!({
        "op_id":OP,"actor_id":"operator:alfa","admission_budget_ms":100,"delivery_mode":"direct_now",
        "command":{"type":"stop_agent","participant_id":PART,"extra":1}
    });
    assert!(serde_json::from_value::<CommandReqPayload>(unknown).is_err());
    Ok(())
}

#[test]
fn roster_and_hello_require_complete_accounting() -> TestResult {
    let minimal = empty_roster();
    for field in [
        "chunk_index",
        "is_final",
        "participants",
        "templates",
        "workflows",
        "unresolved_thread_fences",
        "resolved_op_receipts",
    ] {
        let mut broken = minimal.clone();
        broken.as_object_mut().ok_or("roster object")?.remove(field);
        assert!(
            serde_json::from_value::<RosterSyncPayload>(broken).is_err(),
            "{field}"
        );
    }
    let mut hello = json!({
        "hub_id":"hub_00000000000000000000000000","node_id":"node_00000000000000000000000000",
        "max_inflight_commands":8,"max_inflight_threads":16
    });
    assert!(serde_json::from_value::<HelloAckPayload>(hello.clone()).is_err());
    hello["pending_op_ids"] = json!([]);
    let mut hello: HelloAckPayload = serde_json::from_value(hello)?;
    let frame = BridgeFrame::encode(1, &hello)?;
    assert_eq!(frame.payload["pending_op_ids"], json!([]));
    hello.pending_op_ids = (0..MAX_PENDING_OP_IDS)
        .map(|n| OperationId::from_parts(n as u64, [0; 10]))
        .collect::<Result<_, _>>()?;
    BridgeFrame::encode(1, &hello)?;
    hello
        .pending_op_ids
        .push(OperationId::from_parts(MAX_PENDING_OP_IDS as u64, [0; 10])?);
    assert!(BridgeFrame::encode(1, &hello).is_err());
    hello.pending_op_ids = vec![hello.pending_op_ids[0].clone(); 2];
    assert!(BridgeFrame::encode(1, &hello).is_err());
    Ok(())
}

#[test]
fn roster_chunks_measure_real_bytes_with_the_envelope() -> TestResult {
    let mut roster: RosterSyncPayload = serde_json::from_value(empty_roster())?;
    let bytes_without_text = {
        roster.workflows.push(WorkflowSummary {
            workflow_name: "wf".into(),
            title: None,
            description: Some(String::new()),
        });
        BridgeFrame::encode(16, &roster)?.to_json()?.len()
    };
    roster.workflows[0].description = Some("a".repeat(MAX_WS_FRAME_BYTES - bytes_without_text));
    let at_limit = BridgeFrame::encode(16, &roster)?.to_json()?;
    assert_eq!(at_limit.len(), 262_144);
    BridgeFrame::from_json(&at_limit)?;
    let mut over_limit = at_limit;
    over_limit.push(b' ');
    assert!(matches!(
        BridgeFrame::from_json(&over_limit),
        Err(ProtocolError::BridgeFrameTooLarge { .. })
    ));
    roster.workflows[0]
        .description
        .as_mut()
        .ok_or("description")?
        .push('é');
    assert!(matches!(
        BridgeFrame::encode(16, &roster),
        Err(ProtocolError::BridgeFrameTooLarge { .. })
    ));

    roster.workflows.clear();
    let receipt = CommandResPayload {
        op_id: OP.parse()?,
        outcome: CommandOutcome::Succeeded,
        resource_id: None,
        result: Some(json!("a".repeat(56 * 1024))),
        error: None,
    };
    roster.resolved_op_receipts = vec![receipt; 5];
    assert!(BridgeFrame::encode(16, &roster).is_err());
    let fifth = roster.resolved_op_receipts.pop().ok_or("fifth receipt")?;
    roster.is_final = false;
    let first = BridgeFrame::encode(16, &roster)?.to_json()?;
    roster.resolved_op_receipts = vec![fifth];
    roster.chunk_index = 1;
    roster.is_final = true;
    let second = BridgeFrame::encode(16, &roster)?.to_json()?;
    assert!(first.len() + second.len() < MAX_ROSTER_BYTES);
    assert_eq!(MAX_ROSTER_BYTES / MAX_WS_FRAME_BYTES, 32);
    roster.chunk_index = MAX_ROSTER_FRAMES - 1;
    BridgeFrame::encode(16, &roster)?;
    roster.is_final = false;
    assert!(BridgeFrame::encode(16, &roster).is_err());
    roster.chunk_index = MAX_ROSTER_FRAMES;
    roster.is_final = true;
    assert!(BridgeFrame::encode(16, &roster).is_err());
    Ok(())
}

#[test]
fn event_stream_forms_and_consumer_approval_contract() -> TestResult {
    assert_eq!(EventKind::ALL.len(), 23);
    for kind in EventKind::ALL {
        assert_eq!(kind.as_str().parse::<EventKind>()?, *kind);
        assert_eq!(serde_json::to_value(kind)?, kind.as_str());
    }
    for valid in [
        r#"{"type":"ping"}"#,
        r#"{"type":"resync_required","oldest_available_seq":2,"latest_seq":5,"reason":"expired"}"#,
    ] {
        let frame: EventStreamFrame = serde_json::from_str(valid)?;
        assert_eq!(
            serde_json::to_value(frame)?,
            serde_json::from_str::<Value>(valid)?
        );
        let mut invalid: Value = serde_json::from_str(valid)?;
        invalid["extra"] = json!(1);
        assert!(serde_json::from_value::<EventStreamFrame>(invalid).is_err());
    }
    let mut envelope = json!({
        "seq":1,"event_id":"evt_00000000000000000000000000","source_kind":"node",
        "source_id":"node_00000000000000000000000000","kind":"participant.awaiting_approval",
        "aggregate_type":"participant","aggregate_id":PART,"actor_id":null,"created_at_ms":0,
        "payload":{"node_id":"node_00000000000000000000000000","participant_id":PART,"name":"agent",
            "state":"awaiting_approval","intake_state":"awaiting_approval","approval_id":"a1",
            "wait_kind":"permission","can_respond_remotely":true}
    });
    for kind in ["participant.awaiting_approval", "participant.state_changed"] {
        envelope["kind"] = json!(kind);
        for remote in [true, false] {
            envelope["payload"]["can_respond_remotely"] = json!(remote);
            let frame = json!({"type":"event","event":envelope});
            let decoded: EventStreamFrame = serde_json::from_value(frame.clone())?;
            let EventStreamFrame::Event { event } = &decoded else {
                return Err("expected event".into());
            };
            event.validate()?;
            assert_eq!(serde_json::to_value(decoded)?, frame);
        }
        for remote in [json!(null), json!("false")] {
            envelope["payload"]["can_respond_remotely"] = remote;
            assert!(
                serde_json::from_value::<EventEnvelope>(envelope.clone())?
                    .validate()
                    .is_err()
            );
        }
        envelope["payload"]
            .as_object_mut()
            .ok_or("payload object")?
            .remove("can_respond_remotely");
        assert!(
            serde_json::from_value::<EventEnvelope>(envelope.clone())?
                .validate()
                .is_err()
        );
        envelope["payload"]["can_respond_remotely"] = json!(true);
        let mut invalid = envelope.clone();
        invalid["payload"]["approval_id"] = json!(" ");
        assert!(
            serde_json::from_value::<EventEnvelope>(invalid)?
                .validate()
                .is_err()
        );
    }

    envelope["kind"] = json!("question.answered");
    envelope["aggregate_type"] = json!("message");
    envelope["payload"] = json!({
        "question_message_id":"msg_00000000000000000000000000",
        "answer_message_id":"msg_00000000000000000000000001",
        "answered_by":"participant","answer":{"selected_option_ids":["a"]}
    });
    serde_json::from_value::<EventEnvelope>(envelope.clone())?.validate()?;
    envelope["payload"]
        .as_object_mut()
        .ok_or("payload object")?
        .remove("question_message_id");
    assert!(
        serde_json::from_value::<EventEnvelope>(envelope.clone())?
            .validate()
            .is_err()
    );
    envelope["payload"] = Value::Null;
    assert!(
        serde_json::from_value::<EventEnvelope>(envelope)?
            .validate()
            .is_err()
    );
    Ok(())
}

#[test]
fn roster_and_node_event_reject_ambiguous_approval() -> TestResult {
    let mut payload = json!({
        "type":"participant_state_changed","participant_id":PART,"state":"awaiting_approval",
        "approval_id":"a1","can_respond_remotely":false
    });
    let event: NodeEventPayload = serde_json::from_value(payload.clone())?;
    BridgeFrame::encode(9, &event)?;
    payload["can_respond_remotely"] = json!(true);
    payload["approval_id"] = json!(" ");
    assert!(
        BridgeFrame::encode(
            9,
            &serde_json::from_value::<NodeEventPayload>(payload.clone())?
        )
        .is_err()
    );
    payload["approval"] = json!({"approval_id":"a1"});
    assert!(serde_json::from_value::<NodeEventPayload>(payload).is_err());

    let mut value = empty_roster();
    value["participants"] = json!([{"participant_id":PART,"kind":"agent","name":"agent",
        "state":"awaiting_approval","approval_id":"a1","can_respond_remotely":false}]);
    let frame = BridgeFrame::encode(
        9,
        &serde_json::from_value::<RosterSyncPayload>(value.clone())?,
    )?;
    frame.decode_payload::<RosterSyncPayload>()?;
    value["participants"][0]["can_respond_remotely"] = Value::Null;
    assert!(BridgeFrame::encode(9, &serde_json::from_value::<RosterSyncPayload>(value)?).is_err());
    Ok(())
}

#[test]
fn reconnect_wire_preserves_receipt_and_injection_fence_after_lost_ack() -> TestResult {
    // A response and a thread acknowledgment have been lost. The next
    // epoch carries a targeted operation query and durable thread evidence.
    let hello = BridgeFrame::from_json(
        br#"{
        "v":1,"epoch":12,"kind":"hello_ack","payload":{
            "hub_id":"hub_00000000000000000000000000","node_id":"node_00000000000000000000000000",
            "max_inflight_commands":8,"max_inflight_threads":8,
            "pending_op_ids":["op_00000000000000000000000000"]
        }
    }"#,
    )?;
    let roster = BridgeFrame::from_json(
        br#"{
        "v":1,"epoch":12,"kind":"roster_sync","payload":{
            "chunk_index":0,"is_final":true,"participants":[],"templates":[],"workflows":[],
            "unresolved_thread_fences":[{
                "thread_id":"thr_00000000000000000000000000",
                "participant_id":"part_00000000000000000000000000","highest_dispatched_seq":10
            }],
            "resolved_op_receipts":[{
                "op_id":"op_00000000000000000000000000","outcome":"succeeded",
                "resource_id":"run_00000000000000000000000001","result":{"stored":true}
            }]
        }
    }"#,
    )?;
    assert_eq!(hello.epoch, roster.epoch);
    let hello = hello.decode_payload::<HelloAckPayload>()?;
    let roster = roster.decode_payload::<RosterSyncPayload>()?;
    let result = roster
        .resolved_op_receipts
        .first()
        .ok_or("missing receipt")?;
    assert_eq!(
        hello.pending_op_ids.as_slice(),
        std::slice::from_ref(&result.op_id)
    );
    assert_eq!(result.outcome, CommandOutcome::Succeeded);
    assert_eq!(result.result, Some(json!({"stored":true})));
    let fence = roster
        .unresolved_thread_fences
        .first()
        .ok_or("missing unresolved fence")?;
    assert_eq!(fence.thread_id.as_str(), THREAD);
    assert_eq!(fence.participant_id.as_str(), PART);
    assert_eq!(fence.highest_dispatched_seq, 10);
    Ok(())
}

#[test]
fn bridge_requires_named_fields_at_each_record_boundary() -> TestResult {
    let positional_envelope = br#"[1,3,"roster_sync",[0,true,[],[],[],[],[]]]"#;
    assert!(BridgeFrame::from_json(positional_envelope).is_err());

    let positional_roster =
        br#"{"v":1,"epoch":3,"kind":"roster_sync","payload":[0,true,[],[],[],[],[]]}"#;
    let frame = BridgeFrame::from_json(positional_roster)?;
    assert!(frame.decode_payload::<RosterSyncPayload>().is_err());

    let positional_fence = br#"{"v":1,"epoch":3,"kind":"roster_sync","payload":{
        "chunk_index":0,"is_final":true,"participants":[],"templates":[],"workflows":[],
        "resolved_op_receipts":[],"unresolved_thread_fences":[
            ["thr_00000000000000000000000000","part_00000000000000000000000000",10]
        ]}}"#;
    let frame = BridgeFrame::from_json(positional_fence)?;
    assert!(frame.decode_payload::<RosterSyncPayload>().is_err());
    Ok(())
}

#[test]
fn tagged_enum_schemas_close_every_variant_and_keep_the_discriminator() -> TestResult {
    use utoipa::PartialSchema;

    for (schema, expected_variants) in [
        (BridgeCommand::schema(), 7),
        (NodeEventPayload::schema(), 7),
        (EventStreamFrame::schema(), 3),
    ] {
        let schema = serde_json::to_value(schema)?;
        let variants = schema["oneOf"].as_array().ok_or("expected oneOf")?;
        assert_eq!(variants.len(), expected_variants);
        for variant in variants {
            assert_eq!(variant["type"], "object");
            assert_eq!(variant["additionalProperties"], false);
            assert!(variant["properties"]["type"]["enum"].is_array());
            assert!(
                variant["required"]
                    .as_array()
                    .is_some_and(|required| required.iter().any(|name| name == "type"))
            );
        }
    }
    Ok(())
}
