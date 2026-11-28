use cooplet_protocol::{DeliveryMode, DeliveryState, MessageId, NodeId};
use http::StatusCode;
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};

use crate::{
    error::{HubError, bounded_problem_json},
    storage::operations::{self, FinishOperation, NewOperation, StoredOperation},
};

use super::{
    ANSWER_KIND, MessageReceipt, Reply, Request, Success, Target, check_node_access,
    outcome::{self, Terminal},
};

#[derive(Deserialize, Serialize)]
pub(super) struct RecordedResult {
    pub(super) required_node_ids: Vec<NodeId>,
    pub(super) in_reply_to: Option<MessageId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) delivery_mode: Option<DeliveryMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Reply>,
}

impl RecordedResult {
    fn new(request: &Request, required_node_ids: &[NodeId], result: Option<&Success>) -> Self {
        let mut record = Self {
            required_node_ids: required_node_ids.to_vec(),
            in_reply_to: request.in_reply_to().cloned(),
            delivery_mode: request.delivery_mode(),
            http_status: None,
            body: None,
        };
        record.set_success(result);
        record
    }

    pub(super) fn set_success(&mut self, result: Option<&Success>) {
        self.http_status = result.map(|reply| reply.status.as_u16());
        self.body = result.map(|reply| reply.body.clone());
    }

    fn into_success(self) -> Result<Success, HubError> {
        let status = self
            .http_status
            .and_then(|code| StatusCode::from_u16(code).ok())
            .filter(StatusCode::is_success)
            .ok_or_else(|| HubError::internal("successful operation has no valid status"))?;
        let body = self
            .body
            .ok_or_else(|| HubError::internal("successful operation has no result"))?;
        Ok(Success { status, body })
    }
}

pub(super) fn replay(db: &Connection, request: &Request) -> Result<Option<Success>, HubError> {
    let Some(saved) = operations::load(db, request.caller.actor_id(), &request.key)? else {
        return Ok(None);
    };
    if let Some(node_id) = &saved.target_node_id {
        let node_id = NodeId::parse(node_id).map_err(HubError::internal)?;
        check_node_access(&request.caller, std::slice::from_ref(&node_id))?;
    }
    // A direct message or answer can involve two nodes or have no WS target.
    if matches!(saved.kind.as_str(), "message.send" | ANSWER_KIND) {
        check_node_access(&request.caller, &recorded_result(&saved)?.required_node_ids)?;
    }
    saved.check_match(request.kind.as_str(), &request.fingerprint)?;
    reply(&saved).map(Some)
}

fn message_id(saved: &StoredOperation) -> Result<MessageId, HubError> {
    let id = saved
        .resource_id
        .as_deref()
        .ok_or_else(|| HubError::internal("operation has no message ID"))?;
    MessageId::parse(id).map_err(HubError::internal)
}

fn recorded_result(saved: &StoredOperation) -> Result<RecordedResult, HubError> {
    let data = saved
        .result_json
        .as_deref()
        .ok_or_else(|| HubError::internal("operation has no recorded metadata"))?;
    Ok(serde_json::from_str(data)?)
}

fn reply(saved: &StoredOperation) -> Result<Success, HubError> {
    match saved.state.as_str() {
        "succeeded" => recorded_result(saved)?.into_success(),
        "failed" => Err(saved.failed_error()?),
        "accepted" | "in_progress" => {
            Err(saved.in_progress_error("The operation is still in progress.")?)
        }
        "unknown" if saved.kind == ANSWER_KIND => Ok(Success {
            status: StatusCode::CREATED,
            body: Reply::Message(MessageReceipt {
                operation_id: saved.id()?,
                message_id: message_id(saved)?,
                in_reply_to: recorded_result(saved)?.in_reply_to,
                delivery_state: DeliveryState::Unknown,
            }),
        }),
        "unknown" => {
            let has_message = saved.kind == "message.send";
            Err(saved.unknown_error(outcome::UNKNOWN_DETAIL, has_message)?)
        }
        _ => Err(HubError::internal("unrecognized persisted operation state")),
    }
}

pub(super) enum Observed {
    Missing,
    Accepted(Vec<NodeId>),
    Terminal(Result<Success, HubError>),
}

pub(super) fn observe_acceptance(db: &Connection, request: &Request) -> Result<Observed, HubError> {
    let Some(saved) = operations::load(db, request.caller.actor_id(), &request.key)? else {
        return Ok(Observed::Missing);
    };
    if saved.operation_id != request.id.as_str() {
        return Ok(Observed::Missing);
    }
    match saved.state.as_str() {
        "accepted" | "in_progress" => Ok(Observed::Accepted(
            recorded_result(&saved)?.required_node_ids,
        )),
        _ => Ok(Observed::Terminal(reply(&saved))),
    }
}

pub(super) fn insert(
    tx: &Transaction<'_>,
    request: &Request,
    target: &Target,
    local: Option<&Success>,
    now: i64,
) -> Result<(), HubError> {
    let result =
        serde_json::to_string(&RecordedResult::new(request, &target.required_nodes, local))?;
    operations::insert(
        tx,
        NewOperation {
            operation_id: &request.id,
            actor_id: request.caller.actor_id(),
            key: &request.key,
            kind: request.kind.as_str(),
            request_sha256: &request.fingerprint,
            target_node_id: request.action.target_node().map(NodeId::as_str),
            state: if local.is_some() {
                "succeeded"
            } else {
                "accepted"
            },
            resource_id: request.message_id().map(MessageId::as_str),
            result_json: Some(&result),
            now_ms: now,
        },
    )?;
    Ok(())
}

pub(super) fn finish(
    tx: &Transaction<'_>,
    request: &Request,
    required_nodes: &[NodeId],
    terminal: &Terminal,
    now: i64,
) -> Result<bool, HubError> {
    let result = serde_json::to_string(&RecordedResult::new(
        request,
        required_nodes,
        terminal.success.as_ref(),
    ))?;
    let problem = encode_problem(terminal)?;
    let resource_id = request
        .message_id()
        .map(MessageId::as_str)
        .or(terminal.resource_id.as_deref());
    Ok(operations::finish(
        tx,
        FinishOperation {
            operation_id: &request.id,
            state: terminal.state.as_str(),
            resource_id,
            result_json: Some(&result),
            error_json: problem.as_deref(),
            now_ms: now,
        },
    )?)
}

pub(super) fn encode_problem(terminal: &Terminal) -> Result<Option<String>, HubError> {
    terminal
        .problem
        .as_ref()
        .map(bounded_problem_json)
        .transpose()?
        .map(String::from_utf8)
        .transpose()
        .map_err(HubError::internal)
}

pub(super) fn release_unsubmitted(
    tx: &Transaction<'_>,
    request: &Request,
) -> Result<bool, HubError> {
    Ok(tx.prepare_cached(
        "DELETE FROM operations WHERE operation_id = ?1 AND state IN ('accepted', 'in_progress')",
    )?.execute([request.id.as_str()])? == 1)
}

/// Compact in-place transition to `unknown` when SQLITE_FULL prevents writing a
/// large terminal payload, avoiding an operation stuck forever in `accepted`.
pub(super) fn finish_compact_unknown(
    tx: &Transaction<'_>,
    request: &Request,
    required_nodes: &[NodeId],
    now: i64,
) -> Result<bool, HubError> {
    let result = serde_json::to_string(&RecordedResult::new(request, required_nodes, None))?;
    Ok(operations::finish(
        tx,
        FinishOperation {
            operation_id: &request.id,
            state: "unknown",
            resource_id: request.message_id().map(MessageId::as_str),
            result_json: Some(&result),
            error_json: None,
            now_ms: now,
        },
    )?)
}
