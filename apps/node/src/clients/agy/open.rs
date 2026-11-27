use std::time::Duration;

use cooplet_protocol::ErrorCode;
use serde_json::{Value, json};

use super::{
    acp_wire::{Fault, Frame, Wire, WriteFailure},
    failure, invalid,
};
use crate::{
    clients::EffectiveAgentOptions,
    error::{NodeError, Result},
};

const SESSION_RPC_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) struct AgentInfo {
    pub version: Option<String>,
    resume: bool,
}

impl AgentInfo {
    pub(super) fn reopen_method(&self) -> &'static str {
        if self.resume {
            "session/resume"
        } else {
            "session/load"
        }
    }
}

pub(super) async fn initialize(wire: &mut Wire) -> Result<AgentInfo> {
    let result = rpc(
        wire,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {"name": "cooplet", "version": env!("CARGO_PKG_VERSION")}
        }),
        Stage::Initialize,
    )
    .await?;
    let caps = &result["agentCapabilities"];
    let resume = caps["sessionCapabilities"]["resume"].is_object();
    if result["protocolVersion"].as_u64() != Some(1)
        || caps
            .pointer("/_meta/antigravity/systemPrompt/append")
            .and_then(Value::as_bool)
            != Some(true)
        || !(resume || caps["loadSession"] == true)
    {
        return Err(invalid(
            "Agy ACP lacks the required v1 session capabilities",
        ));
    }
    // The vendor supplies this string. Expose only a version-shaped value.
    let version = result["agentInfo"]["version"]
        .as_str()
        .filter(|version| {
            version.len() <= 64
                && version.starts_with(|character: char| character.is_ascii_digit())
                && version.contains('.')
                && version
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".+-".contains(&byte))
        })
        .map(str::to_owned);
    Ok(AgentInfo { version, resume })
}

pub(super) async fn open(
    wire: &mut Wire,
    method: &str,
    params: Value,
    options: &EffectiveAgentOptions,
) -> Result<String> {
    let expected_id = params["sessionId"].as_str().map(str::to_owned);
    let result = rpc(wire, method, params, Stage::Open).await?;
    if !result.is_object() {
        return Err(failure(
            ErrorCode::DeliveryUnknown,
            "Agy returned an invalid session result",
        ));
    }
    let returned_id = result.get("sessionId");
    let session_id = match (expected_id, returned_id) {
        (Some(id), None) => id,
        (Some(id), Some(returned)) if returned.as_str() == Some(&id) => id,
        (None, Some(Value::String(id))) if !id.trim().is_empty() => id.clone(),
        _ => {
            return Err(failure(
                ErrorCode::DeliveryUnknown,
                "Agy returned an unexpected session ID",
            ));
        }
    };
    let mut config = config_options(&result)?;
    for (id, desired) in [
        ("model", &options.model),
        ("thought_level", &options.effort),
    ] {
        if let Some(desired) = desired {
            config = set_option(wire, &session_id, config, id, desired).await?;
        }
    }
    Ok(session_id)
}

fn config_options(result: &Value) -> Result<Option<Vec<Value>>> {
    match result.get("configOptions") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(options)) => Ok(Some(options.clone())),
        _ => Err(invalid("Agy returned invalid configuration options")),
    }
}

fn selector<'a>(options: &'a [Value], id: &str) -> Option<&'a Value> {
    options
        .iter()
        .find(|option| option["id"] == id)
        .or_else(|| {
            (id == "thought_level")
                .then(|| {
                    options
                        .iter()
                        .find(|option| option["category"] == "thought_level")
                })
                .flatten()
        })
}

fn advertises(option: &Value, desired: &str) -> bool {
    option["options"].as_array().is_some_and(|options| {
        options.iter().any(|value| {
            value["value"] == desired
                || value["options"]
                    .as_array()
                    .is_some_and(|group| group.iter().any(|member| member["value"] == desired))
        })
    })
}

async fn set_option(
    wire: &mut Wire,
    session_id: &str,
    config: Option<Vec<Value>>,
    id: &str,
    desired: &str,
) -> Result<Option<Vec<Value>>> {
    let unavailable = || invalid("Agy did not advertise or accept the requested model or effort");
    let config_id = match &config {
        Some(options) => selector(options, id)
            .filter(|option| advertises(option, desired))
            .and_then(|option| option["id"].as_str())
            .ok_or_else(unavailable)?,
        None => id,
    };
    let result = rpc(
        wire,
        "session/set_config_option",
        json!({
            "sessionId": session_id, "configId": config_id, "value": desired
        }),
        Stage::Configure,
    )
    .await?;
    let updated = config_options(&result)?.ok_or_else(unavailable)?;
    let applied = updated
        .iter()
        .any(|option| option["id"] == config_id && option["currentValue"] == desired);
    if !applied {
        return Err(unavailable());
    }
    Ok(Some(updated))
}

#[derive(Clone, Copy)]
enum Stage {
    Initialize,
    Open,
    Configure,
}

impl Stage {
    fn lost(self, detail: &'static str) -> NodeError {
        let code = match self {
            Self::Initialize => ErrorCode::Overloaded,
            Self::Open | Self::Configure => ErrorCode::DeliveryUnknown,
        };
        failure(code, detail)
    }

    fn rejected(self, rpc_code: i64) -> NodeError {
        tracing::warn!(rpc_code, "Agy rejected an ACP request during session setup");
        let code = match (self, rpc_code) {
            (Self::Open, -32000 | -32602 | -32002) => ErrorCode::InvalidRequest,
            (Self::Configure, _) => ErrorCode::InvalidRequest,
            (Self::Initialize | Self::Open, _) => ErrorCode::Overloaded,
        };
        failure(code, "Agy rejected an ACP request during session setup")
    }
}

async fn rpc(wire: &mut Wire, method: &str, params: Value, stage: Stage) -> Result<Value> {
    let (id, request) = wire.request(method, params);
    wire.send(request, None)
        .await
        .map_err(|error| match error {
            WriteFailure::Encode => {
                failure(ErrorCode::InternalError, "Could not encode Agy ACP request")
            }
            WriteFailure::FrameLimit => invalid("Agy ACP setup request exceeded the size limit"),
            WriteFailure::Expired | WriteFailure::Unwritten => {
                let code = if matches!(stage, Stage::Configure) {
                    ErrorCode::DeliveryUnknown
                } else {
                    ErrorCode::Overloaded
                };
                failure(code, "Agy ACP setup request did not reach the child")
            }
            WriteFailure::Unknown => stage.lost(Fault::WriteUnconfirmed.detail()),
        })?;
    tokio::time::timeout(SESSION_RPC_TIMEOUT, async {
        loop {
            match wire
                .recv()
                .await
                .map_err(|fault| stage.lost(fault.detail()))?
                .frame
            {
                Frame::Response {
                    id: reply_id,
                    reply,
                } if reply_id.as_u64() == Some(id) => {
                    return reply.map_err(|code| stage.rejected(code));
                }
                Frame::Request { method, .. } if method == "session/request_permission" => {
                    return Err(stage.lost(Fault::PermissionRequest.detail()));
                }
                Frame::Request { id, .. } => wire
                    .reject_request(id)
                    .await
                    .map_err(|fault| stage.lost(fault.detail()))?,
                // In particular, session/load may replay the conversation here.
                Frame::Notification { .. } | Frame::Response { .. } => {}
            }
        }
    })
    .await
    .map_err(|_| stage.lost("Agy ACP setup response timed out"))?
}
