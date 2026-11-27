use cooplet_protocol::OperationId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{AgentOverrides, ApprovalWait, ApprovalWaitKind};

/// Persist core state separately from the adapter's opaque transport metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdapterBinding {
    pub core: CoreBinding,
    pub adapter: Value,
}

impl Default for AdapterBinding {
    fn default() -> Self {
        Self {
            core: CoreBinding::default(),
            adapter: Value::Object(Map::new()),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoreBinding {
    /// Latest state-bearing event committed for this generation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_event_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overrides: Option<AgentOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<PendingApprovalRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_response: Option<ApprovalResponseRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_block: Option<LifecycleBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingApprovalRecord {
    pub public_id: String,
    pub native_id: Option<String>,
    pub kind: ApprovalWaitKind,
    pub summary: Option<String>,
    pub action_hint: Option<String>,
    pub can_respond_remotely: bool,
}

impl PendingApprovalRecord {
    pub fn from_wait(public_id: String, wait: ApprovalWait, supports_remote: bool) -> Self {
        let native_id = wait.approval_id.filter(|id| !id.trim().is_empty());
        let can_respond_remotely =
            supports_remote && wait.can_respond_remotely && native_id.is_some();
        Self {
            public_id,
            native_id,
            kind: wait.kind,
            summary: bounded_approval_text(wait.summary),
            action_hint: bounded_approval_text(wait.action_hint),
            can_respond_remotely,
        }
    }

    pub fn as_wait(&self) -> ApprovalWait {
        ApprovalWait {
            approval_id: Some(self.public_id.clone()),
            kind: self.kind,
            summary: self.summary.clone(),
            can_respond_remotely: self.can_respond_remotely,
            action_hint: self.action_hint.clone(),
        }
    }
}

fn bounded_approval_text(text: Option<String>) -> Option<String> {
    text.map(|mut text| {
        text.truncate(text.floor_char_boundary(512));
        text
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalResponseRecord {
    pub public_id: String,
    pub operation_id: OperationId,
    pub state: ApprovalResponseState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalResponseState {
    InFlight,
    Responded,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleBlock {
    pub kind: LifecycleBlockKind,
    pub boot_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleBlockKind {
    SpawnUncertain,
    CleanupUnverified,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn core_and_opaque_metadata_round_trip_independently() -> serde_json::Result<()> {
        let mut binding: AdapterBinding = serde_json::from_str("{}")?;
        assert_eq!(binding.adapter, json!({}));
        binding.adapter = json!({
            "overrides": "adapter-owned string",
            "pending_approval": {"native": [1, 2]},
        });
        binding.core.overrides = Some(AgentOverrides {
            model: Some("operator-model".into()),
            ..AgentOverrides::default()
        });
        let encoded = serde_json::to_value(binding)?;
        assert_eq!(encoded["adapter"]["overrides"], "adapter-owned string");
        assert_eq!(encoded["core"]["overrides"]["model"], "operator-model");
        assert!(
            !encoded["core"]
                .as_object()
                .is_some_and(|object| object.contains_key("pending_approval"))
        );
        Ok(())
    }

    #[test]
    fn approval_projection_redacts_native_id_and_bounds_metadata() {
        let native_id = "native".repeat(1000);
        let wait = ApprovalWait {
            approval_id: Some(native_id.clone()),
            kind: ApprovalWaitKind::Permission,
            summary: Some("字".repeat(600)),
            action_hint: Some("\"\n".repeat(1000)),
            can_respond_remotely: true,
        };
        let pending = PendingApprovalRecord::from_wait("apr_public".into(), wait.clone(), true);
        assert_eq!(pending.native_id.as_deref(), Some(native_id.as_str()));
        let public = pending.as_wait();
        assert_eq!(public.approval_id.as_deref(), Some("apr_public"));
        assert_eq!(public.summary.as_ref().map(String::len), Some(510));
        assert_eq!(public.action_hint.as_ref().map(String::len), Some(512));
        assert!(public.can_respond_remotely);
        assert!(
            !PendingApprovalRecord::from_wait("apr_public".into(), wait, false)
                .can_respond_remotely
        );
    }
}
