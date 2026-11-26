use cooplet_protocol::*;
use serde_json::{Value, json};
use utoipa::PartialSchema;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn id_schemas_and_parsers_share_the_128_bit_boundary() -> TestResult {
    macro_rules! check {
        ($id:ident, $prefix:literal) => {{
            let first = $id::from_parts(0, [0; 10])?;
            let last = $id::from_parts((1 << 48) - 1, [255; 10])?;
            assert_eq!(first.to_string(), concat!($prefix, "00000000000000000000000000"));
            assert_eq!(last.to_string(), concat!($prefix, "7zzzzzzzzzzzzzzzzzzzzzzzzz"));
            assert!(first < $id::from_parts(1, [0; 10])?);
            assert!($id::from_parts(1 << 48, [0; 10]).is_err());
            assert_eq!($id::parse(last.as_ref())?, last);
            assert_eq!(serde_json::from_value::<$id>(json!(last))?, last);
            let generated = $id::generate()?;
            assert_eq!(generated.to_string().parse::<$id>()?, generated);
            assert_eq!(
                serde_json::to_value($id::schema())?,
                json!({"type":"string","pattern":concat!("^", $prefix, "[0-7][0-9a-hjkmnpqrstvwxyz]{25}$")})
            );
            for bad in ["80000000000000000000000000", "0000000000000000000000000",
                "000000000000000000000000000", "0000000000000000000000000A",
                "0000000000000000000000000i", "0000000000000000000000000l",
                "0000000000000000000000000o", "0000000000000000000000000u",
                "0000000000000000000000000\n", "0000000000000000000000000é"] {
                let bad = format!("{}{}", $prefix, bad);
                assert!(bad.parse::<$id>().is_err(), "{bad}");
                assert!(serde_json::from_value::<$id>(json!(bad)).is_err());
            }
        }};
    }
    check!(NodeId, "node_");
    check!(ParticipantId, "part_");
    check!(MessageId, "msg_");
    check!(ThreadId, "thr_");
    check!(BlobId, "blob_");
    check!(RunId, "run_");
    check!(EventId, "evt_");
    check!(OperationId, "op_");
    check!(UserId, "usr_");
    check!(ServicePrincipalId, "sp_");
    check!(TokenId, "tok_");
    check!(HubId, "hub_");
    check!(RequestId, "req_");
    for (prefix, kind) in [("node", SourceKind::Node), ("hub", SourceKind::Hub)] {
        let last = format!("{prefix}_7zzzzzzzzzzzzzzzzzzzzzzzzz");
        let overflow = format!("{prefix}_80000000000000000000000000");
        assert_eq!(SourceId::parse(&last)?.kind(), kind);
        assert_eq!(last.parse::<SourceId>()?, SourceId::parse(&last)?);
        assert_eq!(
            serde_json::from_value::<SourceId>(json!(last))?.to_string(),
            last
        );
        assert!(SourceId::parse(&overflow).is_err());
        assert!(overflow.parse::<SourceId>().is_err());
        assert!(serde_json::from_value::<SourceId>(json!(overflow)).is_err());
    }
    assert!(SourceId::parse("part_00000000000000000000000000").is_err());
    assert_eq!(
        serde_json::to_value(SourceId::schema())?["pattern"],
        "^(node|hub)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$"
    );
    assert!(NodeId::parse("NODE_00000000000000000000000000").is_err());
    assert!(NodeId::parse("part_00000000000000000000000000").is_err());
    Ok(())
}

#[test]
fn canonical_json_is_recursive_and_sdk_vectors_match() -> TestResult {
    let a: Value = serde_json::from_str(r#"{"b":1,"a":{"d":2,"c":[{"y":4,"x":3},5]}}"#)?;
    let b: Value = serde_json::from_str(r#"{"a":{"c":[{"x":3,"y":4},5],"d":2},"b":1}"#)?;
    assert_eq!(
        canonical_json_bytes(&a)?,
        br#"{"a":{"c":[{"x":3,"y":4},5],"d":2},"b":1}"#
    );
    assert_eq!(canonical_sha256(&a)?, canonical_sha256(&b)?);
    assert_ne!(
        canonical_sha256(&a)?,
        canonical_sha256(&json!({"a":{"c":[5,{"x":3,"y":4}],"d":2},"b":1}))?
    );
    let run = RunId::from_parts(0, [0; 10])?;
    for (ordinal, expected) in [
        (
            0,
            "6afc5313f9f2804cbdfcc1d4f6f4f817a6fd7bd9040fb88d4526c984d780eb27",
        ),
        (
            1,
            "ec82582f573ff982291b45a582455c34d8c5c7645eb9aac30cceb4f6060b983c",
        ),
    ] {
        let input = ("cooplet.workflow.step.v1", &run, "a", ordinal);
        assert_eq!(canonical_sha256(&input)?, expected);
    }
    for len in [97, 98, 128] {
        let step = "a".repeat(len);
        validate_step_key(&step)?;
        let key = canonical_sha256(&("cooplet.workflow.step.v1", &run, &step, 0))?;
        assert_eq!(key.len(), 64);
        validate_idempotency_key(&key)?;
        assert_ne!(
            key,
            canonical_sha256(&("cooplet.workflow.step.v1", &run, &step, 1))?
        );
    }
    assert!(validate_step_key("spaces fail").is_err());
    assert!(validate_idempotency_key("short").is_err());
    assert!(validate_idempotency_key("not-ASCII-é").is_err());
    Ok(())
}

#[test]
fn problem_codes_and_extension_policy() -> TestResult {
    let expected = [
        ("invalid_request", 400, false),
        ("unauthorized", 401, false),
        ("node_revoked", 401, false),
        ("forbidden", 403, false),
        ("admin_human_required", 403, false),
        ("not_found", 404, false),
        ("operation_conflict", 409, false),
        ("operation_in_progress", 409, true),
        ("participant_busy", 409, true),
        ("awaiting_approval", 409, true),
        ("question_already_answered", 409, false),
        ("question_expired", 409, false),
        ("pin_conflict", 409, true),
        ("blob_digest_mismatch", 409, false),
        ("hub_identity_mismatch", 409, false),
        ("resync_required", 410, false),
        ("blob_too_large", 413, false),
        ("payload_too_large", 413, false),
        ("rate_limited", 429, true),
        ("bridge_unavailable", 502, true),
        ("delivery_unknown", 503, false),
        ("overloaded", 503, true),
        ("storage_full", 507, false),
        ("internal_error", 500, true),
    ];
    assert_eq!(ErrorCode::ALL.len(), expected.len());
    for (code, (wire, status, retryable)) in ErrorCode::ALL.iter().zip(expected) {
        let problem = ProblemDetails::new(*code, "details");
        let value = serde_json::to_value(&problem)?;
        assert_eq!(value["code"], wire);
        assert_eq!(
            value["type"],
            format!("https://cooplet.dev/problems/{}", wire.replace('_', "-"))
        );
        assert_eq!(value["status"], status);
        assert_eq!(value["retryable"], retryable);
        assert!(value.get("extensions").is_none());
        assert_eq!(serde_json::from_value::<ProblemDetails>(value)?, problem);
    }
    let problem = ProblemDetails::new(ErrorCode::ResyncRequired, "expired")
        .with_extension("oldest_available_seq", 10)
        .with_extension("latest_seq", 20)
        .with_extension("other", json!({"opaque":true}));
    let mut value = serde_json::to_value(&problem)?;
    value["trace_id"] = json!("unknown");
    assert_eq!(
        serde_json::from_value::<ProblemDetails>(value.clone())?,
        problem
    );
    value["status"] = json!("410");
    assert!(serde_json::from_value::<ProblemDetails>(value).is_err());
    Ok(())
}

#[test]
fn literal_info_shapes_and_intake_projection() -> TestResult {
    let hub =
        r#"{"hub_id":"hub_00000000000000000000000000","display_name":"Hub","protocol_version":1}"#;
    let node = r#"{"node_id":"node_00000000000000000000000000","display_name":"Node","protocol_version":1}"#;
    assert!(matches!(
        serde_json::from_str::<InfoResponse>(hub)?,
        InfoResponse::Hub(_)
    ));
    assert!(matches!(
        serde_json::from_str::<InfoResponse>(node)?,
        InfoResponse::Node(_)
    ));
    let schema = serde_json::to_value(InfoResponse::schema())?;
    let branches = schema["oneOf"].as_array().ok_or("missing oneOf")?;
    assert_eq!(branches.len(), 2);
    for branch in branches {
        assert_eq!(branch["additionalProperties"], false);
    }
    let mut value: Value = serde_json::from_str(hub)?;
    value["node_id"] = json!("node_00000000000000000000000000");
    assert!(serde_json::from_value::<InfoResponse>(value.clone()).is_err());
    value["extra"] = json!(true);
    let object = value.as_object_mut().ok_or("missing object")?;
    object.remove("node_id");
    assert!(serde_json::from_value::<InfoResponse>(value).is_err());
    assert!(
        serde_json::from_value::<InfoResponse>(json!({"display_name":"Node","protocol_version":1}))
            .is_err()
    );
    for (state, expected) in [
        (ParticipantState::Starting, IntakeState::Unavailable),
        (ParticipantState::Idle, IntakeState::Ready),
        (ParticipantState::Working, IntakeState::Busy),
        (
            ParticipantState::AwaitingApproval,
            IntakeState::AwaitingApproval,
        ),
        (ParticipantState::Stopping, IntakeState::Unavailable),
        (ParticipantState::Stopped, IntakeState::Unavailable),
        (ParticipantState::Unavailable, IntakeState::Unavailable),
        (ParticipantState::Unknown, IntakeState::Unknown),
    ] {
        assert_eq!(state.intake_state(), expected);
    }
    Ok(())
}

#[test]
fn questions_validate_shape_ids_and_utf8_bounds() -> TestResult {
    let mut question: QuestionPayload = serde_json::from_str(
        r#"{"prompt":"Choose","mode":"single","options":[{"id":"a","label":"A"},{"id":"b","label":"B"}]}"#,
    )?;
    let mut answer = AnswerPayload {
        selected_option_ids: vec!["a".into()],
        text: None,
    };
    answer.validate_against(&question)?;
    answer.text = Some("extra".into());
    assert!(answer.validate_against(&question).is_err());
    answer.selected_option_ids.clear();
    assert!(answer.validate_against(&question).is_err());
    question.allow_free_text = true;
    answer.validate_against(&question)?;
    question.mode = QuestionMode::Multi;
    assert!(answer.validate_against(&question).is_err());
    answer.selected_option_ids = vec!["a".into(), "b".into()];
    answer.validate_against(&question)?;
    question.allow_free_text = false;
    assert!(answer.validate_against(&question).is_err());
    answer.text = None;
    answer.selected_option_ids = vec!["a".into(), "a".into()];
    assert!(answer.validate_against(&question).is_err());
    answer.selected_option_ids = vec!["unknown".into()];
    assert!(answer.validate_against(&question).is_err());

    let options = question.options.clone();
    for count in [0, 1, 17] {
        question.options = vec![options[0].clone(); count];
        assert!(question.validate().is_err());
    }
    question.options = vec![options[0].clone(); 2];
    assert!(question.validate().is_err());
    question.options = options;
    question.mode = QuestionMode::Text;
    assert!(question.validate().is_err());
    question.options.clear();
    answer.selected_option_ids.clear();
    answer.text = Some("é".repeat(4000));
    answer.validate_against(&question)?;
    for text in ["é".repeat(4001), "  ".into()] {
        answer.text = Some(text);
        assert!(answer.validate_against(&question).is_err());
    }
    question.prompt = "é".repeat(1001);
    assert!(question.validate().is_err());
    Ok(())
}

#[test]
fn http_and_event_records_require_objects_and_preserve_json_data() -> TestResult {
    let info = r#"["hub_00000000000000000000000000","Hub",1]"#;
    assert!(serde_json::from_str::<InfoResponse>(info).is_err());
    assert!(serde_json::from_str::<HealthResponse>(r#"["ok"]"#).is_err());
    assert!(serde_json::from_str::<EventStreamFrame>(r#"["ping"]"#).is_err());
    let question = r#"{"prompt":"Choose","mode":"single","options":[["a","One",null]]}"#;
    assert!(serde_json::from_str::<QuestionPayload>(question).is_err());

    let mut event: EventEnvelope = serde_json::from_str(
        r#"{
        "seq":1,"event_id":"evt_00000000000000000000000000","source_kind":"node",
        "source_id":"node_00000000000000000000000000","kind":"participant.awaiting_approval",
        "aggregate_type":"participant","aggregate_id":"p","created_at_ms":0,
        "payload":["awaiting_approval","a1",true]
    }"#,
    )?;
    assert!(event.validate().is_err());
    event.kind = EventKind::QuestionAnswered;
    event.payload = json!(["msg_00000000000000000000000000","msg_00000000000000000000000001",
        null,"p",{"text":"yes"}]);
    assert!(event.validate().is_err());

    // An open JSON value still admits arrays, including nested objects and arrays.
    let receipt: CommandResPayload = serde_json::from_str(
        r#"{"op_id":"op_00000000000000000000000000","outcome":"succeeded","result":[[1],{"values":[2]}]}"#,
    )?;
    assert_eq!(receipt.result, Some(json!([[1], {"values":[2]}])));
    Ok(())
}

#[test]
fn wire_enum_labels_accept_only_strings_in_every_json_representation() -> TestResult {
    use serde::{Deserialize, de::DeserializeOwned};

    fn check<T>(value: T, label: &str) -> TestResult
    where
        T: DeserializeOwned + serde::Serialize + PartialSchema + std::fmt::Debug + Eq,
    {
        assert_eq!(serde_json::to_value(&value)?, json!(label));
        let schema = serde_json::to_value(T::schema())?;
        assert_eq!(schema["type"], "string");
        assert!(
            schema["enum"]
                .as_array()
                .is_some_and(|variants| variants.iter().any(|v| v == label)),
            "{schema} lacks {label}"
        );
        for input in [json!(label), json!({label:null}), json!(0), json!([label])] {
            let bytes = serde_json::to_vec(&input)?;
            for result in [
                serde_json::from_slice::<T>(&bytes),
                serde_json::from_value::<T>(input.clone()),
                T::deserialize(&input),
            ] {
                if input.is_string() {
                    assert_eq!(result?, value);
                } else {
                    assert!(result.is_err(), "{input} accepted as {value:?}");
                }
            }
        }
        Ok(())
    }

    check(FrameKind::RosterSync, "roster_sync")?;
    check(SourceKind::Node, "node")?;
    check(AggregateType::WorkflowRun, "workflow_run")?;
    check(ParticipantKind::Human, "human")?;
    check(ParticipantState::Idle, "idle")?;
    check(IntakeState::AwaitingApproval, "awaiting_approval")?;
    check(DeliveryMode::QueueUntilIdle, "queue_until_idle")?;
    check(DeliveryState::Delivered, "delivered")?;
    check(ApprovalWaitKind::Permission, "permission")?;
    check(ApprovalAction::Approve, "approve")?;
    check(ServiceScope::Manage, "manage")?;
    check(PrincipalKind::HumanAdmin, "human_admin")?;
    check(QuestionMode::Multi, "multi")?;
    check(CommandOutcome::Busy, "busy")?;
    check(ThreadDeliveryOutcome::Delivered, "delivered")?;
    check(EventKind::WorkflowLog, "workflow.log")?;
    check(ErrorCode::DeliveryUnknown, "delivery_unknown")?;
    check(WorkflowRunState::Interrupted, "interrupted")?;
    check(WorkflowStepStatus::Completed, "completed")?;
    check(WorkflowLogLevel::Warn, "warn")?;
    let mut seen_states = std::collections::HashSet::new();
    seen_states.insert(ParticipantState::Idle);
    assert!(seen_states.contains(&ParticipantState::Idle));
    let mut seen_events = std::collections::HashSet::new();
    seen_events.insert(EventKind::WorkflowLog);
    assert!(seen_events.contains(&EventKind::WorkflowLog));
    assert_eq!(ErrorCode::DeliveryUnknown.as_str(), "delivery_unknown");
    assert_eq!(ErrorCode::DeliveryUnknown.to_string(), "delivery_unknown");
    assert_eq!(
        "delivery_unknown".parse::<ErrorCode>()?,
        ErrorCode::DeliveryUnknown
    );
    assert_eq!(
        serde_json::from_str::<EventKind>(r#""workflow\u002elog""#)?,
        EventKind::WorkflowLog
    );

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Frame {
        Event(EventStreamFrame),
    }
    let Frame::Event(EventStreamFrame::Ping {}) =
        serde_json::from_str::<Frame>(r#"{"type":"p\u0069ng"}"#)?
    else {
        return Err("expected ping".into());
    };
    for bad_tag in [json!(1), json!({"ping":null}), json!(["ping"])] {
        let frame = json!({"type":bad_tag});
        assert!(
            serde_json::from_value::<Frame>(frame.clone()).is_err(),
            "{frame}"
        );
        assert!(Frame::deserialize(&frame).is_err());
        assert!(serde_json::from_slice::<Frame>(&serde_json::to_vec(&frame)?).is_err());
    }
    Ok(())
}

#[test]
fn attachment_approval_question_and_canonical_edge_cases() -> TestResult {
    use std::collections::BTreeMap;

    // Borrow<str> lookup and SourceId conversions
    let node: NodeId = "node_00000000000000000000000000".parse()?;
    let hub: HubId = "hub_00000000000000000000000000".parse()?;
    let mut nodes = BTreeMap::new();
    nodes.insert(node.clone(), 1u8);
    assert_eq!(nodes.get(node.as_str()), Some(&1));
    let source: SourceId = node.into();
    assert_eq!(source.as_ref(), "node_00000000000000000000000000");
    let hub_source: SourceId = hub.into();
    assert!(hub_source < source);

    // AttachmentRef & RFC 9110 media type validation
    let valid_att = AttachmentRef {
        blob_id: "blob_00000000000000000000000000".parse()?,
        name: "spec.md".into(),
        mime_type: "text/markdown; charset=utf-8".into(),
        size_bytes: 12,
        sha256: "a".repeat(64),
    };
    valid_att.validate()?;
    for bad_mime in [
        "",
        "   ",
        "not-a-mime",
        "text/plain\r\nX-Injected: 1",
        "text/plain\0",
        "*/*",
        "text/*",
        &format!("text/{}", "a".repeat(255)),
    ] {
        let mut att = valid_att.clone();
        att.mime_type = bad_mime.into();
        assert!(att.validate().is_err(), "accepted bad mime {bad_mime:?}");
    }
    validate_attachment_name("spec.md")?;
    for bad_name in ["", "   ", "a\0b", "a\nb", "a\x1bb", &"a".repeat(256)] {
        assert!(
            validate_attachment_name(bad_name).is_err(),
            "accepted bad name {bad_name:?}"
        );
        let mut att = valid_att.clone();
        att.name = bad_name.into();
        assert!(att.validate().is_err(), "accepted bad name {bad_name:?}");
    }
    for bad_sha in ["", &"A".repeat(64), &"a".repeat(63), &"g".repeat(64)] {
        assert!(validate_sha256(bad_sha).is_err());
    }

    // ApprovalWait & ApprovalDecision validation
    let wait = ApprovalWait {
        approval_id: Some("a1".into()),
        kind: ApprovalWaitKind::Permission,
        summary: None,
        can_respond_remotely: true,
        action_hint: None,
    };
    wait.validate()?;
    let mut bad_wait = wait.clone();
    bad_wait.approval_id = Some("  ".into());
    assert!(bad_wait.validate().is_err());
    bad_wait.can_respond_remotely = false;
    assert!(bad_wait.validate().is_err());
    bad_wait.approval_id = None;
    bad_wait.validate()?;
    assert!(
        ApprovalDecision {
            approval_id: " ".into(),
            action: ApprovalAction::Approve,
        }
        .validate()
        .is_err()
    );

    // QuestionPayload.expires_at_ms & key boundary checks
    let mut q = QuestionPayload {
        prompt: "Proceed?".into(),
        mode: QuestionMode::Text,
        options: Vec::new(),
        allow_free_text: true,
        expires_at_ms: Some(1),
    };
    q.validate()?;
    for bad_expiry in [0, -1, i64::MIN] {
        q.expires_at_ms = Some(bad_expiry);
        assert!(q.validate().is_err());
    }
    assert!(validate_idempotency_key(&"a".repeat(7)).is_err());
    assert!(validate_idempotency_key(&"a".repeat(8)).is_ok());
    assert!(validate_idempotency_key(&"a".repeat(128)).is_ok());
    assert!(validate_idempotency_key(&"a".repeat(129)).is_err());
    assert!(validate_idempotency_key("has space!").is_err());
    assert!(validate_step_key("").is_err());
    assert!(validate_step_key("a._:-9").is_ok());
    assert!(validate_step_key(&"a".repeat(128)).is_ok());
    assert!(validate_step_key(&"a".repeat(129)).is_err());

    // Canonical JSON rejects non-finite floats instead of collapsing them to null
    assert!(canonical_json_bytes(&f64::NAN).is_err());
    assert!(canonical_json_bytes(&f64::INFINITY).is_err());
    assert!(canonical_json_bytes(&f32::NEG_INFINITY).is_err());
    assert!(canonical_sha256(&vec![1.0_f64, f64::NAN]).is_err());
    assert_eq!(canonical_json_bytes(&1.5_f64)?, b"1.5");

    // ProblemDetails OpenAPI schema allows RFC 9457 top-level extensions
    let problem_schema = serde_json::to_value(ProblemDetails::schema())?;
    assert_eq!(problem_schema["type"], "object");
    assert!(problem_schema.get("additionalProperties").is_none());
    assert!(problem_schema["properties"].get("type").is_some());
    assert!(problem_schema["properties"].get("extensions").is_some());
    Ok(())
}
