use std::path::{Path, PathBuf};

use cooplet_platform::ProcessIdentity;
use cooplet_protocol::{
    ApprovalWaitKind, ErrorCode, IntakeState, ParticipantId, ParticipantKind, ParticipantState,
};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    auth::Principal,
    error::{NodeError, Result},
};

use super::{AgentService, bad_data, invalid, not_found};
use crate::clients::{
    AdapterBinding, AdapterCapabilities, NativeSessionKey, PersistedSessionRef, ProcessOwnership,
};

const SELECT: &str = "SELECT participant_id, kind, name, client_kind, store_namespace,
    native_session_id, workspace_dir, template_id, process_ownership, state, capabilities_json,
    adapter_binding_json, runtime_pid, runtime_pgid, runtime_start_token, generation,
    ephemeral_for_run_id FROM participants";

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ParticipantView {
    pub participant_id: ParticipantId,
    pub kind: ParticipantKind,
    pub name: String,
    pub state: ParticipantState,
    pub intake_state: IntakeState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_kind: Option<String>,
    pub capabilities: AdapterCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_kind: Option<ApprovalWaitKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub can_respond_remotely: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ParticipantPage {
    pub items: Vec<ParticipantView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<ParticipantId>,
}

#[derive(Clone)]
pub(super) struct ParticipantRow {
    pub(super) id: ParticipantId,
    pub(super) kind: ParticipantKind,
    pub(super) name: String,
    pub(super) client_kind: Option<String>,
    pub(super) native_key: Option<NativeSessionKey>,
    pub(super) workspace_dir: Option<PathBuf>,
    pub(super) template_id: Option<String>,
    pub(super) ownership: ProcessOwnership,
    pub(super) state: ParticipantState,
    pub(super) capabilities: AdapterCapabilities,
    pub(super) binding: AdapterBinding,
    pub(super) identity: Option<ProcessIdentity>,
    pub(super) generation: u64,
    pub(super) ephemeral_for_run_id: Option<String>,
}

impl ParticipantRow {
    fn decode(row: &Row<'_>) -> rusqlite::Result<Self> {
        let client_kind: Option<String> = row.get(3)?;
        let store_namespace: Option<String> = row.get(4)?;
        let native_session_id: Option<String> = row.get(5)?;
        let native_key = match (&client_kind, store_namespace, native_session_id) {
            (Some(client_kind), Some(store_namespace), Some(native_session_id)) => {
                Some(NativeSessionKey {
                    client_kind: client_kind.clone(),
                    store_namespace,
                    native_session_id,
                })
            }
            (_, None, None) => None,
            _ => {
                return Err(bad_data(std::io::Error::other(
                    "incomplete native session identity",
                )));
            }
        };
        let identity = match (row.get(12)?, row.get(13)?, row.get(14)?) {
            (Some(pid), Some(pgid), Some(start_token)) => Some(ProcessIdentity {
                pid,
                pgid,
                start_token,
            }),
            (None, None, None) => None,
            _ => {
                return Err(bad_data(std::io::Error::other(
                    "incomplete managed process identity",
                )));
            }
        };
        let capabilities: String = row.get(10)?;
        let binding: String = row.get(11)?;
        Ok(Self {
            id: parse(row, 0)?,
            kind: parse(row, 1)?,
            name: row.get(2)?,
            client_kind,
            native_key,
            workspace_dir: row.get::<_, Option<String>>(6)?.map(PathBuf::from),
            template_id: row.get(7)?,
            ownership: parse(row, 8)?,
            state: parse(row, 9)?,
            capabilities: serde_json::from_str(&capabilities).map_err(bad_data)?,
            binding: serde_json::from_str(&binding).map_err(bad_data)?,
            identity,
            generation: row.get(15)?,
            ephemeral_for_run_id: row.get(16)?,
        })
    }

    fn view(self, requester: Option<&ParticipantId>, manage: bool) -> ParticipantView {
        let workspace_dir = if manage || requester == Some(&self.id) {
            self.workspace_dir
                .map(|path| path.to_string_lossy().into_owned())
        } else {
            None
        };
        let approval = if manage && self.state == ParticipantState::AwaitingApproval {
            self.binding
                .core
                .pending_approval
                .map(|pending| pending.as_wait())
        } else {
            None
        };
        ParticipantView {
            participant_id: self.id,
            kind: self.kind,
            name: self.name,
            state: self.state,
            intake_state: self.state.intake_state(),
            client_kind: self.client_kind,
            capabilities: self.capabilities,
            workspace_dir,
            wait_kind: approval.as_ref().map(|wait| wait.kind),
            approval_id: approval.as_ref().and_then(|wait| wait.approval_id.clone()),
            action_hint: approval.as_ref().and_then(|wait| wait.action_hint.clone()),
            summary: approval.as_ref().and_then(|wait| wait.summary.clone()),
            can_respond_remotely: approval.map(|wait| wait.can_respond_remotely),
        }
    }

    pub(super) fn persisted(&self, state_dir: &Path) -> Result<PersistedSessionRef> {
        let workspace_dir = self.workspace_dir.clone().ok_or_else(|| {
            invalid(
                "missing_workspace",
                "This participant has no agent workspace",
            )
        })?;
        Ok(PersistedSessionRef {
            participant_id: self.id.clone(),
            generation: self.generation,
            native_key: self.native_key.clone(),
            process_ownership: self.ownership,
            workspace_dir,
            runtime_dir: runtime_dir(state_dir, &self.id, self.generation),
            process_identity: self.identity.clone(),
            adapter_binding: self.binding.adapter.clone(),
        })
    }
}

pub(super) fn runtime_dir(
    state_dir: &Path,
    participant_id: &ParticipantId,
    generation: u64,
) -> PathBuf {
    state_dir
        .join("runtime")
        .join(format!("{participant_id}.g{generation}"))
}

fn parse<T>(row: &Row<'_>, col: usize) -> rusqlite::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    row.get::<_, String>(col)?.parse().map_err(bad_data)
}

pub(super) fn load_participant(
    conn: &Connection,
    id: &ParticipantId,
) -> rusqlite::Result<Option<ParticipantRow>> {
    conn.prepare_cached(&format!("{SELECT} WHERE participant_id = ?1"))?
        .query_row([id.as_str()], ParticipantRow::decode)
        .optional()
}

impl AgentService {
    pub(super) async fn participant_row(&self, id: &ParticipantId) -> Result<ParticipantRow> {
        let id = id.clone();
        self.0
            .reads
            .read(move |conn| Ok(load_participant(conn, &id)?))
            .await?
            .ok_or_else(not_found)
    }

    pub async fn participant(
        &self,
        principal: &Principal,
        id: ParticipantId,
    ) -> Result<ParticipantView> {
        catalog_access(principal)?;
        Ok(self.participant_row(&id).await?.view(
            principal.identity.participant_id.as_ref(),
            principal.can_manage(),
        ))
    }

    pub async fn participants(
        &self,
        principal: &Principal,
        after: Option<ParticipantId>,
        limit: usize,
    ) -> Result<ParticipantPage> {
        catalog_access(principal)?;
        if !(1..=500).contains(&limit) {
            return Err(invalid(
                "invalid_page_limit",
                "The participant page limit must be between 1 and 500",
            ));
        }
        let requester = principal.identity.participant_id.clone();
        let manage = principal.can_manage();
        let page = self
            .0
            .reads
            .read(move |conn| {
                let mut stmt = conn.prepare_cached(&format!(
                    "{SELECT} WHERE participant_id > ?1 ORDER BY participant_id ASC LIMIT ?2",
                ))?;
                let mut rows = stmt.query(params![
                    after.as_ref().map_or("", ParticipantId::as_str),
                    limit + 1
                ])?;
                let mut items = Vec::new();
                let mut bytes = br#"{"items":[]}"#.len();
                let mut has_more = false;
                let mut next = rows.next()?.map(ParticipantRow::decode).transpose()?;
                while let Some(row) = next {
                    let view = row.view(requester.as_ref(), manage);
                    let item_len = crate::storage::json_encoded_len(&view).map_err(bad_data)?;
                    let upcoming = if items.len() + 1 < limit {
                        rows.next()?.map(ParticipantRow::decode).transpose()?
                    } else if rows.next()?.is_some() {
                        has_more = true;
                        None
                    } else {
                        None
                    };
                    let next_bytes = bytes + usize::from(!items.is_empty()) + item_len;
                    let cursor_bytes = if upcoming.is_some() || has_more {
                        br#","next_after":"""#.len() + view.participant_id.as_str().len()
                    } else {
                        0
                    };
                    if next_bytes + cursor_bytes > 4 * 1024 * 1024 {
                        has_more = true;
                        break;
                    }
                    bytes = next_bytes;
                    items.push(view);
                    next = upcoming;
                }
                let next_after = if has_more {
                    Some(
                        items
                            .last()
                            .ok_or_else(|| {
                                bad_data(std::io::Error::other(
                                    "One participant exceeds the page budget",
                                ))
                            })?
                            .participant_id
                            .clone(),
                    )
                } else {
                    None
                };
                Ok(ParticipantPage { items, next_after })
            })
            .await?;
        Ok(page)
    }
}

fn catalog_access(principal: &Principal) -> Result<()> {
    if principal.can_read_all() || principal.is_active_participant() {
        Ok(())
    } else {
        Err(NodeError::new(
            ErrorCode::Forbidden,
            "This credential cannot read the participant catalog",
        ))
    }
}
