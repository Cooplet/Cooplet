use std::{collections::HashSet, io};

use cooplet_protocol::{
    AggregateType, ApprovalWaitKind, ErrorCode, EventKind, HubId, NodeEventPayload, NodeId,
    ParticipantId, ParticipantState, ParticipantWire, TemplateSummary, WorkflowSummary,
};
use cooplet_storage::{EventNotice, WriteError, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};

use crate::{
    AppState,
    error::HubError,
    storage::events::{
        EventAudience, NewEvent, advance_head, append, node_disconnected_data, record,
    },
};

use super::registry::{CloseReason, SessionKey};

pub(super) const MAX_PARTICIPANTS: usize = 4096;
const MAX_PROJECTION_BATCH: usize = 64;

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RosterCatalog {
    pub(crate) templates: Vec<TemplateSummary>,
    pub(crate) workflows: Vec<WorkflowSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovalView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wait_kind: Option<ApprovalWaitKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) can_respond_remotely: Option<bool>,
}

impl ApprovalView {
    fn from_participant(participant: &ParticipantWire) -> Self {
        Self {
            wait_kind: participant.wait_kind,
            approval_id: participant.approval_id.clone(),
            action_hint: participant.action_hint.clone(),
            summary: participant.summary.clone(),
            can_respond_remotely: participant.can_respond_remotely,
        }
    }

    fn encode(&self, state: ParticipantState) -> Result<Option<String>, serde_json::Error> {
        (state == ParticipantState::AwaitingApproval)
            .then(|| serde_json::to_string(self))
            .transpose()
    }

    pub(crate) fn redact(&mut self) {
        redact_fields(&mut self.approval_id, &mut self.can_respond_remotely);
    }
}

fn redact_fields<T>(approval_id: &mut Option<T>, can_respond_remotely: &mut Option<bool>) {
    *approval_id = None;
    *can_respond_remotely = Some(false);
}

/// Shared redaction for the EventBus and problem-details extensions. The
/// caller selects an approval surface and checks the destination-node scope.
pub(crate) fn redact_approval_value(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.remove("approval_id");
        if object
            .get("state")
            .is_none_or(|state| state == ParticipantState::AwaitingApproval.as_str())
        {
            object.insert("can_respond_remotely".to_owned(), Value::Bool(false));
        }
    }
}

pub(super) fn upsert_participant(
    tx: &Transaction<'_>,
    key: &SessionKey,
    participant: &ParticipantWire,
    now: i64,
) -> Result<(), HubError> {
    tx.prepare_cached(
        "INSERT INTO node_participants
           (node_id, participant_id, kind, name, client_kind, workspace_dir, state,
            capabilities_json, approval_json, synced_epoch, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT (node_id, participant_id) DO UPDATE SET
            kind = excluded.kind, name = excluded.name, client_kind = excluded.client_kind,
            workspace_dir = excluded.workspace_dir, state = excluded.state,
            capabilities_json = excluded.capabilities_json, approval_json = excluded.approval_json,
            synced_epoch = excluded.synced_epoch, updated_at_ms = excluded.updated_at_ms",
    )?
    .execute(params![
        key.node_id.as_str(),
        participant.participant_id.as_str(),
        participant.kind.as_str(),
        participant.name,
        participant.client_kind,
        participant.workspace_dir,
        participant.state.as_str(),
        serde_json::to_string(&participant.capabilities)?,
        ApprovalView::from_participant(participant).encode(participant.state)?,
        key.epoch,
        now,
    ])?;
    Ok(())
}

pub(super) fn require_session(tx: &Transaction<'_>, key: &SessionKey) -> Result<(), HubError> {
    let current = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = ?1 AND generation = ?2
                       AND session_epoch = ?3 AND revoked_at_ms IS NULL AND status != 'revoked')",
        params![key.node_id.as_str(), key.generation, key.epoch],
        |row| row.get::<_, bool>(0),
    )?;
    if !current {
        return Err(HubError::new(
            ErrorCode::BridgeUnavailable,
            "The bridge session is no longer current.",
        ));
    }
    Ok(())
}

pub(super) async fn mark_offline(state: &AppState, key: SessionKey) -> Result<(), HubError> {
    let hub_id = state.meta.hub_id.clone();
    let wall_clock = state.wall_clock.clone();
    Ok(state
        .db
        .write_wait(move |tx| {
            let now = wall_clock();
            let changed = tx.execute(
                "UPDATE nodes SET status = 'offline' WHERE node_id = ?1 AND generation = ?2
                     AND session_epoch = ?3 AND revoked_at_ms IS NULL AND status = 'online'",
                params![key.node_id.as_str(), key.generation, key.epoch],
            )?;
            let events = if changed == 1 {
                vec![node_status_event(tx, &hub_id, &key, now, false)?]
            } else {
                Vec::new()
            };
            Ok::<_, WriteError<HubError>>(WriteOutcome::with_events((), events))
        })
        .await?)
}

pub(super) fn node_status_event(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    key: &SessionKey,
    now: i64,
    online: bool,
) -> rusqlite::Result<EventNotice> {
    let (kind, status) = if online {
        (EventKind::NodeConnected, "online")
    } else {
        (EventKind::NodeDisconnected, "offline")
    };
    append(
        tx,
        hub_id,
        NewEvent {
            kind,
            aggregate_type: AggregateType::Node,
            aggregate_id: key.node_id.as_str(),
            actor_id: None,
            created_at_ms: now,
            data: node_disconnected_data(&key.node_id, status, key.epoch),
            audience: node_audience(&key.node_id),
        },
    )
}

fn node_audience(node: &NodeId) -> EventAudience {
    EventAudience::Nodes {
        node_ids: vec![node.clone()],
    }
}

pub(super) async fn worker(
    state: AppState,
    key: SessionKey,
    mut incoming: mpsc::Receiver<(u64, NodeEventPayload)>,
    applied: watch::Sender<u64>,
    mut close: watch::Receiver<Option<CloseReason>>,
) -> Result<(), HubError> {
    loop {
        if close.borrow().is_some() {
            return Ok(());
        }
        let next = tokio::select! {
            biased;
            _ = close.changed() => return Ok(()),
            next = incoming.recv() => next,
        };
        let Some(first) = next else {
            return Ok(());
        };
        let mut events = Vec::with_capacity(MAX_PROJECTION_BATCH);
        let mut last_seq = first.0;
        events.push(first.1);
        while events.len() < MAX_PROJECTION_BATCH {
            match incoming.try_recv() {
                Ok((seq, event)) => {
                    last_seq = seq;
                    events.push(event);
                }
                Err(_) => break,
            }
        }
        let key = key.clone();
        let hub_id = state.meta.hub_id.clone();
        let cancelled = close.clone();
        let wake_node = key.node_id.clone();
        let wall_clock = state.wall_clock.clone();
        let write = state.db.write_wait(move |tx| {
            if cancelled.borrow().is_some() {
                return Err(
                    HubError::new(ErrorCode::BridgeUnavailable, "The bridge closed.").into(),
                );
            }
            require_session(tx, &key)?;
            let now = wall_clock();
            let mut idle_participants = HashSet::new();
            let mut notices = Vec::new();
            for event in events {
                let (idle, applied_notices) = apply_event(tx, &hub_id, &key, now, event)?;
                if let Some(participant) = idle {
                    idle_participants.insert(participant);
                }
                notices.extend(applied_notices);
            }
            if let Some(last) = notices.last() {
                advance_head(tx, last.seq)?;
            }
            for participant in &idle_participants {
                crate::threads::participant_idle(tx, &key.node_id, participant)?;
            }
            if cancelled.borrow().is_some() {
                return Err(
                    HubError::new(ErrorCode::BridgeUnavailable, "The bridge closed.").into(),
                );
            }
            Ok::<_, WriteError<HubError>>(WriteOutcome::with_events(
                idle_participants.into_iter().collect::<Vec<_>>(),
                notices,
            ))
        });
        let idle_participants = tokio::select! {
            biased;
            _ = close.changed() => return Ok(()),
            result = write => result?,
        };
        applied.send_replace(last_seq);
        if !idle_participants.is_empty() {
            crate::threads::on_participants_idle(&state, &wake_node, &idle_participants);
        }
    }
}

struct PreviousParticipant {
    name: String,
    state: ParticipantState,
    approval_json: Option<String>,
}

fn previous_participant(
    tx: &Transaction<'_>,
    key: &SessionKey,
    participant_id: &ParticipantId,
) -> Result<Option<PreviousParticipant>, HubError> {
    Ok(tx
        .prepare_cached(
            "SELECT name, state, approval_json FROM node_participants
               WHERE node_id = ?1 AND participant_id = ?2 AND synced_epoch = ?3",
        )?
        .query_row(
            params![key.node_id.as_str(), participant_id.as_str(), key.epoch],
            |row| {
                Ok(PreviousParticipant {
                    name: row.get(0)?,
                    state: crate::storage::sql::parse_col(row, 1)?,
                    approval_json: row.get(2)?,
                })
            },
        )
        .optional()?)
}

fn apply_event(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    key: &SessionKey,
    now: i64,
    event: NodeEventPayload,
) -> Result<(Option<ParticipantId>, Vec<EventNotice>), HubError> {
    let (kinds, aggregate_type, aggregate_id, data, idle) = match event {
        NodeEventPayload::ParticipantDeclared { participant } => {
            let previous = previous_participant(tx, key, &participant.participant_id)?;
            if previous.is_none() {
                let count: usize = tx.query_row(
                    "SELECT COUNT(*) FROM node_participants WHERE node_id = ?1",
                    [key.node_id.as_str()],
                    |row| row.get(0),
                )?;
                if count >= MAX_PARTICIPANTS {
                    return Err(HubError::invalid(
                        "The node participant limit was exceeded.",
                    ));
                }
            }
            upsert_participant(tx, key, &participant, now)?;
            let approval = ApprovalView::from_participant(&participant);
            let change_kinds = if let Some(previous) = previous {
                changed_state(
                    &previous,
                    participant.state,
                    &approval.encode(participant.state)?,
                )
                .into_iter()
                .collect::<Vec<_>>()
            } else if participant.state == ParticipantState::AwaitingApproval {
                vec![
                    EventKind::ParticipantCreated,
                    EventKind::ParticipantAwaitingApproval,
                ]
            } else {
                vec![EventKind::ParticipantCreated]
            };
            if change_kinds.is_empty() {
                let idle = (participant.state == ParticipantState::Idle)
                    .then_some(participant.participant_id);
                return Ok((idle, Vec::new()));
            }
            let mut data = serde_json::to_value(&participant)?;
            data["intake_state"] = json!(participant.state.intake_state());
            (
                change_kinds,
                AggregateType::Participant,
                participant.participant_id.to_string(),
                data,
                (participant.state == ParticipantState::Idle).then_some(participant.participant_id),
            )
        }
        NodeEventPayload::ParticipantStateChanged {
            participant_id,
            state,
            wait_kind,
            approval_id,
            action_hint,
            summary,
            can_respond_remotely,
        } => {
            let previous = previous_participant(tx, key, &participant_id)?.ok_or_else(|| {
                HubError::invalid("The bridge event references an unknown participant.")
            })?;
            let approval = ApprovalView {
                wait_kind,
                approval_id,
                action_hint,
                summary,
                can_respond_remotely,
            };
            let encoded = approval.encode(state)?;
            let Some(kind) = changed_state(&previous, state, &encoded) else {
                return Ok((
                    (state == ParticipantState::Idle).then_some(participant_id),
                    Vec::new(),
                ));
            };
            tx.execute(
                "UPDATE node_participants SET state = ?1, approval_json = ?2, updated_at_ms = ?3
                   WHERE node_id = ?4 AND participant_id = ?5 AND synced_epoch = ?6",
                params![
                    state.as_str(),
                    encoded,
                    now,
                    key.node_id.as_str(),
                    participant_id.as_str(),
                    key.epoch
                ],
            )?;
            let mut data = serde_json::to_value(approval)?;
            data["participant_id"] = json!(participant_id);
            data["name"] = json!(previous.name);
            data["state"] = json!(state);
            data["intake_state"] = json!(state.intake_state());
            (
                vec![kind],
                AggregateType::Participant,
                participant_id.to_string(),
                data,
                (state == ParticipantState::Idle).then_some(participant_id),
            )
        }
        event => {
            let (kind, run_id) = match &event {
                NodeEventPayload::WorkflowRunStarted { run_id, .. } => {
                    (EventKind::WorkflowRunStarted, run_id)
                }
                NodeEventPayload::WorkflowRunFinished { run_id, .. } => {
                    (EventKind::WorkflowRunFinished, run_id)
                }
                NodeEventPayload::WorkflowStepStarted { run_id, .. } => {
                    (EventKind::WorkflowStepStarted, run_id)
                }
                NodeEventPayload::WorkflowStepCompleted { run_id, .. } => {
                    (EventKind::WorkflowStepCompleted, run_id)
                }
                NodeEventPayload::WorkflowLog { run_id, .. } => (EventKind::WorkflowLog, run_id),
                _ => return Err(HubError::invalid("Unexpected bridge event.")),
            };
            let mut data = serde_json::to_value(&event)?;
            data.as_object_mut()
                .ok_or_else(|| {
                    HubError::internal(io::Error::other("Expected workflow event object"))
                })?
                .remove("type");
            (
                vec![kind],
                AggregateType::WorkflowRun,
                run_id.to_string(),
                data,
                None,
            )
        }
    };
    let mut data = data;
    data["node_id"] = json!(key.node_id);
    let mut notices = Vec::with_capacity(kinds.len());
    for kind in kinds {
        notices.push(record(
            tx,
            hub_id,
            NewEvent {
                kind,
                aggregate_type,
                aggregate_id: &aggregate_id,
                actor_id: None,
                created_at_ms: now,
                data: data.clone(),
                audience: node_audience(&key.node_id),
            },
        )?);
    }
    Ok((idle, notices))
}

fn changed_state(
    previous: &PreviousParticipant,
    state: ParticipantState,
    approval: &Option<String>,
) -> Option<EventKind> {
    if previous.state == state && previous.approval_json == *approval {
        None
    } else {
        Some(match state {
            ParticipantState::AwaitingApproval => EventKind::ParticipantAwaitingApproval,
            ParticipantState::Stopped => EventKind::ParticipantStopped,
            _ => EventKind::ParticipantStateChanged,
        })
    }
}
