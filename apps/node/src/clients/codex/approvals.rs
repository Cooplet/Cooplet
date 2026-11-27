use std::collections::VecDeque;

use serde_json::{Value, json};

use crate::clients::{ApprovalAction, ApprovalWait, ApprovalWaitKind};

const MAX_APPROVALS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub(super) enum NativeRequestId {
    Integer(i64),
    String(String),
}

#[derive(Default)]
pub(super) struct Approvals {
    next: u64,
    pending: VecDeque<Callback>,
}

pub(super) struct Callback {
    pub(super) request_id: NativeRequestId,
    pub(super) turn_id: String,
    key: String,
    kind: ApprovalWaitKind,
    summary: &'static str,
    reply: Option<Reply>,
    sent: bool,
}

enum Reply {
    Decision,
    Permissions(Value),
}

impl Approvals {
    pub(super) fn push(
        &mut self,
        generation: u64,
        request_id: NativeRequestId,
        method: &str,
        params: &Value,
        turn_id: String,
    ) -> Result<(), &'static str> {
        if self
            .pending
            .iter()
            .any(|entry| entry.request_id == request_id)
            || self.pending.len() >= MAX_APPROVALS
        {
            return Err("approval_request_limit_or_duplicate");
        }
        let (kind, summary, reply) = match method {
            "item/commandExecution/requestApproval" => (
                ApprovalWaitKind::Permission,
                "Codex is waiting for command approval",
                one_time_decisions(params).then_some(Reply::Decision),
            ),
            "item/fileChange/requestApproval" => (
                ApprovalWaitKind::Permission,
                "Codex is waiting for file change approval",
                one_time_decisions(params).then_some(Reply::Decision),
            ),
            "item/permissions/requestApproval" => (
                ApprovalWaitKind::Permission,
                "Codex is waiting for additional permissions",
                requested_permissions(&params["permissions"]).map(Reply::Permissions),
            ),
            "mcpServer/elicitation/request"
                if params["_meta"]["codex_approval_kind"] == "mcp_tool_call" =>
            {
                (
                    ApprovalWaitKind::Permission,
                    "Codex is waiting for MCP tool approval",
                    None,
                )
            }
            "item/tool/requestUserInput" | "mcpServer/elicitation/request" => (
                ApprovalWaitKind::Question,
                "Codex is waiting for an answer",
                None,
            ),
            _ => (
                ApprovalWaitKind::Unspecified,
                "Codex is waiting for input",
                None,
            ),
        };
        self.next += 1;
        self.pending.push_back(Callback {
            request_id,
            turn_id,
            key: format!("codex-{generation}-{}", self.next),
            kind,
            summary,
            reply,
            sent: false,
        });
        Ok(())
    }

    pub(super) fn resolve(&mut self, id: &NativeRequestId) {
        self.pending.retain(|entry| &entry.request_id != id);
    }

    pub(super) fn clear_turn(&mut self, turn_id: &str) {
        self.pending.retain(|entry| entry.turn_id != turn_id);
    }

    pub(super) fn clear(&mut self) {
        self.pending.clear();
    }

    pub(super) fn current(&self) -> Option<&Callback> {
        self.pending.front()
    }

    pub(super) fn consume_current(&mut self) {
        if let Some(callback) = self.pending.front_mut() {
            callback.sent = true;
        }
    }
}

#[derive(Debug)]
pub(super) enum ReplyError {
    Stale,
    Unsupported,
}

impl Callback {
    pub(super) fn wait(&self, hint: &str) -> ApprovalWait {
        let can_respond_remotely = self.reply.is_some() && !self.sent;
        ApprovalWait {
            approval_id: can_respond_remotely.then(|| self.key.clone()),
            kind: self.kind,
            summary: Some(self.summary.into()),
            can_respond_remotely,
            action_hint: Some(hint.into()),
        }
    }

    pub(super) fn response(&self, id: &str, action: ApprovalAction) -> Result<Value, ReplyError> {
        if self.key != id || self.sent {
            return Err(ReplyError::Stale);
        }
        let result = match &self.reply {
            Some(Reply::Decision) => json!({
                "decision": match action {
                    ApprovalAction::Approve => "accept",
                    ApprovalAction::Deny => "decline",
                },
            }),
            Some(Reply::Permissions(permissions)) => json!({
                "permissions": match action {
                    ApprovalAction::Approve => permissions.clone(),
                    ApprovalAction::Deny => json!({}),
                },
                "scope": "turn",
            }),
            None => return Err(ReplyError::Unsupported),
        };
        Ok(json!({"id": self.request_id, "result": result}))
    }
}

fn one_time_decisions(params: &Value) -> bool {
    match &params["availableDecisions"] {
        Value::Null => true,
        Value::Array(decisions) => {
            decisions.iter().any(|v| v == "accept") && decisions.iter().any(|v| v == "decline")
        }
        _ => false,
    }
}

fn requested_permissions(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    for (key, value) in object {
        if value.is_null() {
            continue;
        }
        let fields = value.as_object()?;
        let supported = match key.as_str() {
            "network" => fields
                .iter()
                .all(|(k, v)| k == "enabled" && (v.is_null() || v.is_boolean())),
            "fileSystem" => fields.iter().all(|(k, v)| {
                v.is_null()
                    || match k.as_str() {
                        "read" | "write" => v
                            .as_array()
                            .is_some_and(|paths| paths.iter().all(Value::is_string)),
                        "entries" => v.is_array(),
                        "globScanMaxDepth" => v.as_u64().is_some_and(|n| n > 0),
                        _ => false,
                    }
            }),
            _ => false,
        };
        if !supported {
            return None;
        }
    }
    // The grant is precisely the native request, using turn scope only.
    Some(value.clone())
}
