use std::sync::Arc;

use cooplet_protocol::{
    ApprovalWaitKind, EventEnvelope, EventKind, NodeEventPayload, NodeId, ParticipantId,
    ParticipantState, ParticipantWire,
};
use cooplet_storage::{EventNotice, ReadPool, StorageError};
use rusqlite::{Connection, ToSql};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};

use crate::{error::Result, http::events::events_query_sql, storage::read_event};

use super::roster::{bad_data, lookup_participant};

const PAGE_ROWS: usize = 32;
const FORWARDED: &[EventKind] = &[
    EventKind::ParticipantCreated,
    EventKind::ParticipantStateChanged,
    EventKind::ParticipantAwaitingApproval,
    EventKind::ParticipantStopped,
    EventKind::WorkflowRunStarted,
    EventKind::WorkflowStepStarted,
    EventKind::WorkflowStepCompleted,
    EventKind::WorkflowRunFinished,
    EventKind::WorkflowLog,
];

pub(super) struct Batch {
    pub(super) through: u64,
    pub(super) initial_head: u64,
    pub(super) events: Vec<NodeEventPayload>,
}

pub(super) async fn pump(
    reads: ReadPool,
    local_id: NodeId,
    mut notices: broadcast::Receiver<EventNotice>,
    mut cursor: u64,
    outbox: mpsc::Sender<Batch>,
) -> Result<()> {
    let kinds: Vec<_> = FORWARDED.iter().map(|kind| kind.as_str()).collect();
    let sql = Arc::new(events_query_sql(kinds.len()));
    let mut initial_head = None;
    let mut last_approval = None;
    let mut first = true;
    loop {
        let page = reads
            .read({
                let sql = sql.clone();
                let local_id = local_id.clone();
                let kinds = kinds.clone();
                move |conn| {
                    read_page(
                        conn,
                        &local_id,
                        &sql,
                        &kinds,
                        cursor,
                        initial_head,
                        last_approval,
                    )
                }
            })
            .await?;
        last_approval = page.last_approval;
        let head = *initial_head.get_or_insert(page.head);
        let allowed = page.through >= head;
        if (first || page.through > cursor)
            && outbox
                .send(Batch {
                    through: page.through,
                    initial_head: head,
                    events: page.events,
                })
                .await
                .is_err()
        {
            return Ok(());
        }
        first = false;
        cursor = page.through;
        if !allowed || page.head > cursor {
            continue;
        }

        // Catch-up can leave buffered notices below the cursor. Skip
        // them without starting another read of the same window.
        loop {
            tokio::select! {
                () = outbox.closed() => return Ok(()),
                notice = notices.recv() => match notice {
                    Ok(notice) if notice.seq <= cursor => {},
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => break,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
    }
}

struct Page {
    through: u64,
    head: u64,
    events: Vec<NodeEventPayload>,
    last_approval: Option<LastApproval>,
}

#[derive(PartialEq, Eq)]
struct LastApproval {
    seq: u64,
    actor: ParticipantId,
    fields: ApprovalFields,
}

fn read_page(
    conn: &Connection,
    local_id: &NodeId,
    sql: &str,
    kinds: &[&str],
    cursor: u64,
    initial_head: Option<u64>,
    mut last_approval: Option<LastApproval>,
) -> std::result::Result<Page, StorageError> {
    let tx = conn.unchecked_transaction()?;
    let (floor, head): (u64, u64) = tx.query_row(
        "SELECT events_pruned_through_seq, events_head_seq FROM node_meta WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if cursor < floor || cursor > head {
        return Err(StorageError::InvalidConfig(
            "The bridge event cursor is outside the retained window",
        ));
    }
    let cut = initial_head
        .filter(|initial| cursor < *initial)
        .unwrap_or(head);
    let mut statement = tx.prepare_cached(sql)?;
    let mut bindings: Vec<&dyn ToSql> = vec![&cursor, &cut];
    bindings.extend(kinds.iter().map(|kind| kind as &dyn ToSql));
    bindings.push(&PAGE_ROWS);
    let mut rows = statement.query(bindings.as_slice())?;
    let mut events = Vec::new();
    let mut scanned = 0;
    let mut latest = cursor;
    while let Some(row) = rows.next()? {
        let event = read_event(row, local_id)?;
        latest = event.seq;
        scanned += 1;
        if let Some(event) = project(&tx, event, &mut last_approval)? {
            events.push(event);
        }
    }
    Ok(Page {
        through: if scanned < PAGE_ROWS { cut } else { latest },
        head,
        events,
        last_approval,
    })
}

fn project(
    conn: &Connection,
    event: EventEnvelope,
    last_approval: &mut Option<LastApproval>,
) -> rusqlite::Result<Option<NodeEventPayload>> {
    match event.kind {
        EventKind::ParticipantCreated
        | EventKind::ParticipantStateChanged
        | EventKind::ParticipantAwaitingApproval
        | EventKind::ParticipantStopped => {
            let state = ParticipantChange::deserialize(&event.payload).map_err(bad_data)?;
            let previous = last_approval.take();
            let duplicate = event.kind == EventKind::ParticipantAwaitingApproval
                && previous.as_ref().is_some_and(|previous| {
                    previous.seq.checked_add(1) == Some(event.seq)
                        && previous.actor == state.participant_id
                        && previous.fields == state.approval
                });
            if duplicate {
                return Ok(None);
            }
            if state.state == ParticipantState::AwaitingApproval
                && matches!(
                    event.kind,
                    EventKind::ParticipantCreated | EventKind::ParticipantStateChanged
                )
            {
                *last_approval = Some(LastApproval {
                    seq: event.seq,
                    actor: state.participant_id.clone(),
                    fields: state.approval.clone(),
                });
            }
            if event.kind == EventKind::ParticipantCreated
                || state.reason.as_deref() == Some("resumed")
            {
                let mut participant = lookup_participant(conn, &state.participant_id)?
                    .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                state.apply_to(&mut participant);
                return Ok(Some(NodeEventPayload::ParticipantDeclared { participant }));
            }
            Ok(Some(NodeEventPayload::ParticipantStateChanged {
                participant_id: state.participant_id,
                state: state.state,
                approval_id: state.approval.approval_id,
                action_hint: state.approval.action_hint,
                summary: state.approval.summary,
                wait_kind: state.approval.wait_kind,
                can_respond_remotely: state.approval.can_respond_remotely,
            }))
        }
        other => {
            *last_approval = None;
            let (kind, fields): (&str, &[&str]) = match other {
                EventKind::WorkflowRunStarted => {
                    ("workflow_run_started", &["run_id", "workflow_name"])
                }
                EventKind::WorkflowRunFinished => (
                    "workflow_run_finished",
                    &["run_id", "workflow_name", "state", "result", "error"],
                ),
                EventKind::WorkflowStepStarted => {
                    ("workflow_step_started", &["run_id", "step_key"])
                }
                EventKind::WorkflowStepCompleted => {
                    ("workflow_step_completed", &["run_id", "step_key", "status"])
                }
                EventKind::WorkflowLog => ("workflow_log", &["run_id", "level", "message"]),
                _ => return Ok(None),
            };
            let mut projected = serde_json::Map::new();
            projected.insert("type".into(), kind.into());
            for &field in fields {
                if let Some(value) = event.payload.get(field) {
                    projected.insert(field.into(), value.clone());
                }
            }
            serde_json::from_value(serde_json::Value::Object(projected))
                .map(Some)
                .map_err(bad_data)
        }
    }
}

#[derive(Deserialize)]
struct ParticipantChange {
    participant_id: ParticipantId,
    state: ParticipantState,
    name: Option<String>,
    reason: Option<String>,
    #[serde(flatten)]
    approval: ApprovalFields,
}

impl ParticipantChange {
    fn apply_to(self, declared: &mut ParticipantWire) {
        if let Some(name) = self.name {
            declared.name =
                super::roster::participant_name(&declared.participant_id, declared.kind, name);
        }
        declared.state = self.state;
        declared.wait_kind = self.approval.wait_kind;
        declared.approval_id = self.approval.approval_id;
        declared.action_hint = self.approval.action_hint;
        declared.summary = self.approval.summary;
        declared.can_respond_remotely = self.approval.can_respond_remotely;
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ApprovalFields {
    wait_kind: Option<ApprovalWaitKind>,
    approval_id: Option<String>,
    action_hint: Option<String>,
    summary: Option<String>,
    can_respond_remotely: Option<bool>,
}
