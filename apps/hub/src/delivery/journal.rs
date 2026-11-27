use cooplet_protocol::{DeliveryState, ErrorCode, MessageId, NodeId, OperationId, ProblemDetails};
use http::StatusCode;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use crate::error::{HubError, bounded_problem_json};

use super::{
    ANSWER_KIND, MessageReceipt, Reply, Request, Success, Target, check_node_access,
    outcome::{self, Terminal},
};

const OPERATION_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

struct StoredOperation {
    operation_id: String,
    kind: String,
    fingerprint: String,
    target_node_id: Option<String>,
    state: String,
    resource_id: Option<String>,
    result_json: Option<String>,
    error_json: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct RecordedResult {
    required_node_ids: Vec<NodeId>,
    in_reply_to: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Reply>,
}

impl RecordedResult {
    fn new(request: &Request, required_node_ids: &[NodeId], result: Option<&Success>) -> Self {
        Self {
            required_node_ids: required_node_ids.to_vec(),
            in_reply_to: request.in_reply_to().cloned(),
            http_status: result.map(|reply| reply.status.as_u16()),
            body: result.map(|reply| reply.body.clone()),
        }
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

fn load(db: &Connection, request: &Request) -> rusqlite::Result<Option<StoredOperation>> {
    db.query_row(
        "SELECT operation_id, kind, request_sha256, target_node_id, state, resource_id, result_json, error_json
         FROM operations WHERE actor_id = ?1 AND operation_key = ?2",
        params![request.caller.actor_id(), request.key],
        |row| Ok(StoredOperation {
            operation_id: row.get(0)?,
            kind: row.get(1)?,
            fingerprint: row.get(2)?,
            target_node_id: row.get(3)?,
            state: row.get(4)?,
            resource_id: row.get(5)?,
            result_json: row.get(6)?,
            error_json: row.get(7)?,
        }),
    ).optional()
}

pub(super) fn replay(db: &Connection, request: &Request) -> Result<Option<Success>, HubError> {
    let Some(saved) = load(db, request)? else {
        return Ok(None);
    };
    if let Some(node_id) = &saved.target_node_id {
        let node_id = NodeId::parse(node_id).map_err(HubError::internal)?;
        check_node_access(&request.caller, std::slice::from_ref(&node_id))?;
    }
    // A locally accepted answer can cover two nodes, or have no WS target.
    if saved.kind == ANSWER_KIND {
        check_node_access(&request.caller, &saved.result()?.required_node_ids)?;
    }
    if saved.kind != request.kind.as_str() || saved.fingerprint != request.fingerprint {
        return Err(HubError::new(
            ErrorCode::OperationConflict,
            "This key already identifies a different operation.",
        ));
    }
    saved.reply().map(Some)
}

impl StoredOperation {
    fn id(&self) -> Result<OperationId, HubError> {
        OperationId::parse(&self.operation_id).map_err(HubError::internal)
    }

    fn message_id(&self) -> Result<MessageId, HubError> {
        let id = self
            .resource_id
            .as_deref()
            .ok_or_else(|| HubError::internal("operation has no message ID"))?;
        MessageId::parse(id).map_err(HubError::internal)
    }

    fn result(&self) -> Result<RecordedResult, HubError> {
        let data = self
            .result_json
            .as_deref()
            .ok_or_else(|| HubError::internal("operation has no recorded metadata"))?;
        Ok(serde_json::from_str(data)?)
    }

    fn reply(&self) -> Result<Success, HubError> {
        match self.state.as_str() {
            "succeeded" => self.result()?.into_success(),
            "failed" => {
                let data = self
                    .error_json
                    .as_deref()
                    .ok_or_else(|| HubError::internal("failed operation has no recorded error"))?;
                Err(HubError::from_problem(serde_json::from_str::<
                    ProblemDetails,
                >(data)?))
            }
            "accepted" | "in_progress" => Err(HubError::new(
                ErrorCode::OperationInProgress,
                "The operation is still in progress.",
            )
            .with_extension("operation_id", self.id()?.as_str())),
            "unknown" if self.kind == ANSWER_KIND => Ok(Success {
                status: StatusCode::CREATED,
                body: Reply::Message(MessageReceipt {
                    operation_id: self.id()?,
                    message_id: self.message_id()?,
                    in_reply_to: self.result()?.in_reply_to,
                    delivery_state: DeliveryState::Unknown,
                }),
            }),
            "unknown" => {
                let mut error = HubError::new(ErrorCode::DeliveryUnknown, outcome::UNKNOWN_DETAIL)
                    .with_extension("operation_id", self.id()?.as_str());
                if let Some(id) = &self.resource_id {
                    error = error.with_extension("message_id", id.as_str());
                }
                Err(error)
            }
            _ => Err(HubError::internal("unrecognized persisted operation state")),
        }
    }
}

pub(super) enum Observed {
    Missing,
    Accepted(Vec<NodeId>),
    Terminal(Result<Success, HubError>),
}

pub(super) fn observe_acceptance(db: &Connection, request: &Request) -> Result<Observed, HubError> {
    let Some(saved) = load(db, request)? else {
        return Ok(Observed::Missing);
    };
    if saved.operation_id != request.id.as_str() {
        return Ok(Observed::Missing);
    }
    match saved.state.as_str() {
        "accepted" | "in_progress" => Ok(Observed::Accepted(saved.result()?.required_node_ids)),
        _ => Ok(Observed::Terminal(saved.reply())),
    }
}

pub(super) fn insert(
    tx: &Transaction<'_>,
    request: &Request,
    target: &Target,
    local: Option<&Success>,
    now: i64,
) -> Result<(), HubError> {
    let result = RecordedResult::new(request, &target.required_nodes, local);
    tx.execute(
        "INSERT INTO operations (
            operation_id, actor_id, operation_key, kind, request_sha256, target_node_id, state,
            resource_id, result_json, created_at_ms, updated_at_ms, expires_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11)",
        params![
            request.id.as_str(),
            request.caller.actor_id(),
            request.key,
            request.kind.as_str(),
            request.fingerprint,
            request.action.target_node().map(NodeId::as_str),
            if local.is_some() {
                "succeeded"
            } else {
                "accepted"
            },
            request.message_id().map(MessageId::as_str),
            serde_json::to_string(&result)?,
            now,
            now.saturating_add(OPERATION_TTL_MS),
        ],
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
    let result = RecordedResult::new(request, required_nodes, terminal.success.as_ref());
    let problem = terminal
        .problem
        .as_ref()
        .map(bounded_problem_json)
        .transpose()?
        .map(String::from_utf8)
        .transpose()
        .map_err(HubError::internal)?;
    let resource_id = request
        .message_id()
        .map(MessageId::as_str)
        .or(terminal.resource_id.as_deref());
    let changed = tx.execute(
        "UPDATE operations SET state = ?2, resource_id = ?3, result_json = ?4, error_json = ?5, updated_at_ms = ?6
         WHERE operation_id = ?1 AND state = 'accepted'",
        params![
            request.id.as_str(),
            terminal.state.as_str(),
            resource_id,
            serde_json::to_string(&result)?,
            problem,
            now,
        ],
    )?;
    Ok(changed == 1)
}
