use cooplet_protocol::{
    ApprovalWaitKind, CommandOutcome, CommandResPayload, DeliveryMode, DeliveryState, ErrorCode,
    MessageId, ParticipantId, ProblemDetails, RunId,
};
use serde_json::Value;

use crate::{bridge::SubmitError, error::HubError, storage::sql::json_byte_len};

use super::{CommandReceipt, Kind, ReceiptContext, Reply, Success};

pub(super) const UNKNOWN_DETAIL: &str =
    "The Hub could not confirm whether the command was delivered.";
const MAX_DETAIL_BYTES: usize = 4096;
const MAX_RESOURCE_ID_BYTES: usize = 128;
pub(super) const MAX_RESULT_BYTES: usize = 64 * 1024;

pub(super) enum TerminalState {
    Succeeded,
    Failed,
    Unknown,
}

impl TerminalState {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

pub(super) struct Terminal {
    pub(super) state: TerminalState,
    pub(super) success: Option<Success>,
    pub(super) problem: Option<ProblemDetails>,
    pub(super) delivery: Option<DeliveryState>,
    pub(super) delivery_detail: Option<String>,
    pub(super) resource_id: Option<String>,
    pub(super) release_key: bool,
}

impl Terminal {
    pub(super) fn response(&self) -> Result<Success, HubError> {
        if let Some(success) = &self.success {
            return Ok(success.clone());
        }
        if let Some(problem) = &self.problem {
            return Err(HubError::from_problem(problem.clone()));
        }
        Err(HubError::internal("terminal operation has no outcome"))
    }
}

pub(super) fn classify(
    request: &ReceiptContext<'_>,
    response: Result<CommandResPayload, SubmitError>,
) -> Result<Terminal, HubError> {
    let response = match response {
        Ok(response) => response,
        Err(SubmitError::Unavailable) => {
            let mut terminal = failure(
                request,
                DeliveryState::Unavailable,
                ProblemDetails::new(
                    ErrorCode::BridgeUnavailable,
                    "The node could not deliver the command.",
                ),
            )?;
            terminal.release_key = !request.is_answer();
            return Ok(terminal);
        }
        Err(SubmitError::Unknown) => return unknown_terminal(request),
    };
    if &response.op_id != request.id {
        tracing::warn!(
            operation_id = %request.id,
            received_operation_id = %response.op_id,
            "Bridge returned a response for a different command"
        );
        return unknown_terminal(request);
    }
    let addresses_message = request.message_id.is_some();
    match response.outcome {
        CommandOutcome::Delivered if addresses_message => {
            message_success(request, DeliveryState::Delivered)
        }
        CommandOutcome::Queued
            if addresses_message && request.delivery_mode == Some(DeliveryMode::QueueUntilIdle) =>
        {
            message_success(request, DeliveryState::Queued)
        }
        CommandOutcome::Succeeded if !addresses_message => {
            if let Some(resource_id) = response.resource_id.as_deref()
                && !valid_resource_id(request.kind, resource_id)
            {
                tracing::warn!(
                    operation_id = %request.id,
                    kind = request.kind.as_str(),
                    "Bridge returned an invalid resource_id for a succeeded command"
                );
                return unknown_terminal(request);
            }
            let result = match response.result {
                Some(value) if json_byte_len(&value)? <= MAX_RESULT_BYTES => Some(value),
                Some(_) => {
                    tracing::warn!(
                        operation_id = %request.id,
                        kind = request.kind.as_str(),
                        "Dropping oversized node command result exceeding 64 KiB budget"
                    );
                    None
                }
                None => None,
            };
            Ok(Terminal {
                state: TerminalState::Succeeded,
                success: Some(Success {
                    status: request.kind.default_status(),
                    body: Reply::Command(CommandReceipt {
                        operation_id: request.id.clone(),
                        resource_id: response.resource_id.clone(),
                        result,
                    }),
                }),
                problem: None,
                delivery: None,
                delivery_detail: None,
                resource_id: response.resource_id,
                release_key: false,
            })
        }
        CommandOutcome::Unknown
        | CommandOutcome::Delivered
        | CommandOutcome::Queued
        | CommandOutcome::Succeeded => unknown_terminal(request),
        CommandOutcome::Busy => {
            let mut problem =
                ProblemDetails::new(ErrorCode::ParticipantBusy, "The participant is busy.");
            if let Some(remote) = response.error.as_ref() {
                copy_extensions(&mut problem, remote);
            }
            failure(request, DeliveryState::Busy, problem)
        }
        CommandOutcome::AwaitingApproval => {
            let mut problem = ProblemDetails::new(
                ErrorCode::AwaitingApproval,
                "The participant is waiting for an approval decision.",
            );
            if let Some(remote) = response.error.as_ref() {
                copy_extensions(&mut problem, remote);
            }
            failure(request, DeliveryState::AwaitingApproval, problem)
        }
        CommandOutcome::Unavailable => failure(
            request,
            DeliveryState::Unavailable,
            ProblemDetails::new(
                ErrorCode::BridgeUnavailable,
                "The node could not deliver the command.",
            ),
        ),
        CommandOutcome::Rejected => {
            if response
                .error
                .as_ref()
                .is_some_and(|error| error.code == ErrorCode::DeliveryUnknown)
            {
                return unknown_terminal(request);
            }
            let problem = rejected(response.error.as_ref());
            let delivery = if problem.code == ErrorCode::AwaitingApproval {
                DeliveryState::AwaitingApproval
            } else {
                DeliveryState::Unavailable
            };
            failure(request, delivery, problem)
        }
        CommandOutcome::Expired => failure(
            request,
            DeliveryState::Expired,
            ProblemDetails::new(
                ErrorCode::Overloaded,
                "The node could not admit the command before its deadline.",
            )
            .with_extension("reason", "admission_expired"),
        ),
    }
}

fn valid_resource_id(kind: Kind, value: &str) -> bool {
    if value.trim().is_empty()
        || value.len() > MAX_RESOURCE_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return false;
    }
    match kind {
        Kind::CreateAgent => ParticipantId::parse(value).is_ok(),
        Kind::RunWorkflow => RunId::parse(value).is_ok(),
        _ => true,
    }
}

pub(super) fn unknown(request: &ReceiptContext<'_>) -> HubError {
    HubError::from_problem(correlate(
        request,
        ProblemDetails::new(ErrorCode::DeliveryUnknown, UNKNOWN_DETAIL),
    ))
}

pub(super) fn unknown_terminal(request: &ReceiptContext<'_>) -> Result<Terminal, HubError> {
    let problem = unknown(request);
    Ok(Terminal {
        state: TerminalState::Unknown,
        success: request
            .is_answer()
            .then(|| request.message_reply(DeliveryState::Unknown))
            .transpose()?,
        problem: Some(problem.problem().clone()),
        delivery: request.message_id.map(|_| DeliveryState::Unknown),
        delivery_detail: Some(UNKNOWN_DETAIL.into()),
        resource_id: None,
        release_key: false,
    })
}

fn message_success(
    request: &ReceiptContext<'_>,
    delivery: DeliveryState,
) -> Result<Terminal, HubError> {
    Ok(Terminal {
        state: TerminalState::Succeeded,
        success: Some(request.message_reply(delivery)?),
        problem: None,
        delivery: Some(delivery),
        delivery_detail: None,
        resource_id: None,
        release_key: false,
    })
}

fn failure(
    request: &ReceiptContext<'_>,
    delivery: DeliveryState,
    problem: ProblemDetails,
) -> Result<Terminal, HubError> {
    let problem = correlate(request, problem);
    let delivery_detail = request.message_id.map(|_| clean_text(&problem.detail));
    let (state, success, problem) = if request.is_answer() {
        (
            TerminalState::Succeeded,
            Some(request.message_reply(delivery)?),
            None,
        )
    } else {
        (TerminalState::Failed, None, Some(problem))
    };
    Ok(Terminal {
        state,
        success,
        problem,
        delivery: request.message_id.map(|_| delivery),
        delivery_detail,
        resource_id: None,
        release_key: false,
    })
}

fn correlate(request: &ReceiptContext<'_>, mut problem: ProblemDetails) -> ProblemDetails {
    problem
        .extensions
        .insert("operation_id".into(), Value::from(request.id.as_str()));
    if let Some(id) = request.message_id {
        problem
            .extensions
            .insert("message_id".into(), Value::from(id.as_str()));
    }
    problem
}

fn rejected(remote: Option<&ProblemDetails>) -> ProblemDetails {
    let rejection =
        ProblemDetails::new(ErrorCode::InvalidRequest, "The node rejected the command.");
    let Some(remote) = remote else {
        return rejection;
    };
    if !matches!(
        remote.code,
        ErrorCode::InvalidRequest
            | ErrorCode::NotFound
            | ErrorCode::ParticipantBusy
            | ErrorCode::AwaitingApproval
            | ErrorCode::QuestionAlreadyAnswered
            | ErrorCode::QuestionExpired
            | ErrorCode::RateLimited
    ) {
        return rejection;
    }
    let detail = if remote.code == ErrorCode::AwaitingApproval {
        "The participant is waiting for an approval decision.".to_owned()
    } else {
        clean_text(&remote.detail)
    };
    let mut problem = ProblemDetails::new(
        remote.code,
        if detail.trim().is_empty() {
            rejection.detail
        } else {
            detail
        },
    );
    copy_extensions(&mut problem, remote);
    problem
}

fn copy_extensions(local: &mut ProblemDetails, remote: &ProblemDetails) {
    let value = |name| remote.extensions.get(name).and_then(Value::as_str);
    if local.code == ErrorCode::AwaitingApproval {
        if let Some(approval_id) = value("approval_id")
            && !approval_id.trim().is_empty()
            && approval_id.len() <= MAX_DETAIL_BYTES
            && !approval_id.chars().any(char::is_control)
        {
            local
                .extensions
                .insert("approval_id".into(), Value::from(approval_id));
        }
        if let Some(action_hint) = value("action_hint") {
            let action_hint = if action_hint.len() > MAX_DETAIL_BYTES {
                "Check the participant for a pending approval.".into()
            } else {
                clean_text(action_hint)
            };
            if !action_hint.trim().is_empty() {
                local
                    .extensions
                    .insert("action_hint".into(), Value::from(action_hint));
            }
        }
        if let Some(summary) = value("summary") {
            local
                .extensions
                .insert("summary".into(), Value::from(clean_text(summary)));
        }
        if let Some(wait_kind) =
            value("wait_kind").and_then(|kind| kind.parse::<ApprovalWaitKind>().ok())
        {
            local
                .extensions
                .insert("wait_kind".into(), Value::from(wait_kind.as_str()));
        }
        if let Some(can_respond) = remote
            .extensions
            .get("can_respond_remotely")
            .and_then(Value::as_bool)
        {
            let can_respond = can_respond && local.extensions.contains_key("approval_id");
            local
                .extensions
                .insert("can_respond_remotely".into(), Value::from(can_respond));
        }
    }
    if local.code == ErrorCode::QuestionAlreadyAnswered
        && let Some(id) = value("existing_answer_id").filter(|id| MessageId::parse(id).is_ok())
    {
        local
            .extensions
            .insert("existing_answer_id".into(), Value::from(id));
    }
    if let Some(reason) = value("reason").filter(|r| cooplet_protocol::is_valid_error_reason(r)) {
        local
            .extensions
            .insert("reason".into(), Value::from(reason));
    }
}

pub(super) fn redact_error(error: HubError, can_manage: bool) -> HubError {
    if can_manage || error.problem().code != ErrorCode::AwaitingApproval {
        return error;
    }
    let mut problem = error.into_problem();
    let fields = std::mem::take(&mut problem.extensions);
    let mut object = Value::Object(fields.into_iter().collect());
    crate::bridge::redact_approval_value(&mut object);
    if let Value::Object(fields) = object {
        problem.extensions = fields.into_iter().collect();
    }
    HubError::from_problem(problem)
}

fn clean_text(text: &str) -> String {
    let mut clean = String::with_capacity(text.len().min(MAX_DETAIL_BYTES));
    for ch in text.chars().filter(|ch| !ch.is_control()) {
        if clean.len() + ch.len_utf8() > MAX_DETAIL_BYTES {
            break;
        }
        clean.push(ch);
    }
    clean
}
