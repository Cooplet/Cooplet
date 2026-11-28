use std::{collections::HashSet, io, sync::Arc};

use cooplet_protocol::{
    ApprovalWaitKind, EventEnvelope, EventKind, NodeEventPayload, NodeId, ParticipantId,
    ParticipantState, ParticipantWire,
};
use cooplet_storage::{EventNotice, ReadPool, StorageError};
use rusqlite::{Connection, ToSql};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc, watch};

use crate::{error::Result, http::events::events_query_sql, storage::read_event};

use super::roster::{MAX_PARTICIPANTS, bad_data, lookup_participant};

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
    mut uncertain_commit: watch::Receiver<()>,
    mut cursor: u64,
    participants: HashSet<ParticipantId>,
    outbox: mpsc::Sender<Batch>,
) -> Result<()> {
    let kinds: Vec<_> = FORWARDED.iter().map(|kind| kind.as_str()).collect();
    let sql = Arc::new(events_query_sql(kinds.len()));
    let mut initial_head = None;
    let mut projection = Projection {
        participants,
        last_approval: None,
    };
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
                        projection,
                    )
                }
            })
            .await?;
        projection = page.projection;
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
                change = uncertain_commit.changed() => {
                    if change.is_err() {
                        return Ok(());
                    }
                    break;
                }
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
    projection: Projection,
}

struct Projection {
    participants: HashSet<ParticipantId>,
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
    mut projection: Projection,
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
        if let Some(event) = project(&tx, event, &mut projection)? {
            events.push(event);
        }
    }
    Ok(Page {
        through: if scanned < PAGE_ROWS { cut } else { latest },
        head,
        events,
        projection,
    })
}

fn project(
    conn: &Connection,
    event: EventEnvelope,
    projection: &mut Projection,
) -> rusqlite::Result<Option<NodeEventPayload>> {
    match event.kind {
        EventKind::ParticipantCreated
        | EventKind::ParticipantStateChanged
        | EventKind::ParticipantAwaitingApproval
        | EventKind::ParticipantStopped => {
            let state = ParticipantChange::deserialize(&event.payload).map_err(bad_data)?;
            let previous = projection.last_approval.take();
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
                projection.last_approval = Some(LastApproval {
                    seq: event.seq,
                    actor: state.participant_id.clone(),
                    fields: state.approval.clone(),
                });
            }
            let known = projection.participants.contains(&state.participant_id);
            // A new participant can have a durable starting row whose first
            // public transition is a failed open. Declare it before sending
            // state-only updates, even if the first state is already terminal.
            if !known
                || event.kind == EventKind::ParticipantCreated
                || state.reason.as_deref() == Some("resumed")
            {
                if !known && projection.participants.len() == MAX_PARTICIPANTS {
                    return Err(bad_data(io::Error::other(
                        "The bridge participant limit was reached",
                    )));
                }
                let mut participant = lookup_participant(conn, &state.participant_id)?
                    .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                state.apply_to(&mut participant);
                projection
                    .participants
                    .insert(participant.participant_id.clone());
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
            projection.last_approval = None;
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

#[cfg(test)]
mod tests {
    use cooplet_protocol::{AggregateType, EventId, SourceKind};
    use rusqlite::params;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn uncertain_commit_wakes_the_pump_for_the_last_durable_event()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::config::{NodeConfig, OpenNode};
        use cooplet_protocol::RunId;
        use cooplet_storage::WriteOutcome;
        use std::{path::PathBuf, time::Duration};

        struct Scratch(PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let dir = Scratch(cooplet_platform::PrivateFs::ensure_private_dir(
            &std::env::temp_dir().join(format!("cooplet-pump-{}", EventId::generate()?)),
        )?);
        let open = OpenNode::start(NodeConfig::new(dir.0.join("node"), dir.0.clone())).await?;
        let db = open.engine.db();
        let cursor = open
            .engine
            .reads()
            .read(|conn| Ok(super::super::roster::head(conn)?))
            .await?;
        let (outbox, mut batches) = mpsc::channel(1);
        let mut ordinary = db.subscribe();
        let pump = tokio::spawn(pump(
            open.engine.reads().clone(),
            open.info.node_id,
            db.subscribe(),
            db.subscribe_uncertain_commits(),
            cursor,
            HashSet::new(),
            outbox,
        ));
        let wait = Duration::from_secs(5);
        let initial = tokio::time::timeout(wait, batches.recv())
            .await?
            .ok_or("missing initial cut")?;
        assert_eq!(initial.through, cursor);
        let run = RunId::generate()?;
        let event_run = run.clone();
        let now = crate::now_ms()?;
        let write = db
            .write_sqlite(move |tx| {
                let notice = crate::storage::append_event(
                    tx,
                    now,
                    EventKind::WorkflowRunStarted,
                    AggregateType::WorkflowRun,
                    event_run.as_str(),
                    None,
                    json!({ "run_id": event_run, "workflow_name": "report" }),
                )?;
                // The row is durable, but DbActor's subsequent COMMIT reports an error.
                tx.execute_batch("COMMIT")?;
                Ok(WriteOutcome::with_events((), vec![notice]))
            })
            .await;
        assert!(matches!(write, Err(StorageError::CommitFailed(_))));
        let batch = tokio::time::timeout(wait, batches.recv())
            .await?
            .ok_or("missing durable event")?;
        assert_eq!(batch.through, cursor + 1);
        assert!(matches!(batch.events.as_slice(),
            [NodeEventPayload::WorkflowRunStarted { run_id, .. }] if run_id == &run));
        assert!(matches!(
            ordinary.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        drop(batches);
        tokio::time::timeout(wait, pump).await???;
        open.engine.shutdown().await?;
        Ok(())
    }

    #[test]
    fn first_failed_open_is_declared_without_losing_event_history()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(crate::storage::MIGRATIONS[0].sql)?;
        let local_id = NodeId::generate()?;
        let new_participant = ParticipantId::generate()?;
        let known_participant = ParticipantId::generate()?;
        for id in [&new_participant, &known_participant] {
            connection.execute(
                "INSERT INTO participants (participant_id, kind, name, client_kind, state,
                    created_at_ms, updated_at_ms, capabilities_json)
                 VALUES (?1, 'agent', ?2, 'test', 'idle', 1, 3, '{\"supports_resume\":true}')",
                params![id.as_str(), format!("latest-{id}")],
            )?;
        }
        let mut projection = Projection {
            participants: HashSet::from([known_participant.clone()]),
            last_approval: None,
        };
        let event = |seq, actor: &ParticipantId, kind, state, name, reason| {
            Ok::<_, cooplet_protocol::IdError>(EventEnvelope {
                seq,
                event_id: EventId::generate()?,
                source_kind: SourceKind::Node,
                source_id: local_id.clone().into(),
                kind,
                aggregate_type: AggregateType::Participant,
                aggregate_id: actor.to_string(),
                actor_id: None,
                created_at_ms: seq as i64,
                payload: json!({
                    "participant_id": actor,
                    "state": state,
                    "name": name,
                    "reason": reason,
                }),
            })
        };
        let stopped = event(
            1,
            &new_participant,
            EventKind::ParticipantStopped,
            ParticipantState::Stopped,
            "first-name",
            "native_open_failed",
        )?;
        let Some(NodeEventPayload::ParticipantDeclared { participant }) =
            project(&connection, stopped, &mut projection)?
        else {
            return Err(
                io::Error::other("the first failed open did not declare the participant").into(),
            );
        };
        assert_eq!(participant.participant_id, new_participant);
        assert_eq!(participant.state, ParticipantState::Stopped);
        assert_eq!(participant.name, "first-name");
        assert_eq!(participant.client_kind.as_deref(), Some("test"));
        assert_eq!(participant.capabilities["supports_resume"], true);
        assert_eq!(projection.participants.len(), 2);

        let stopped_known = event(
            2,
            &known_participant,
            EventKind::ParticipantStopped,
            ParticipantState::Stopped,
            "known-name",
            "native_open_failed",
        )?;
        assert!(matches!(
            project(&connection, stopped_known, &mut projection)?,
            Some(NodeEventPayload::ParticipantStateChanged {
                participant_id, state: ParticipantState::Stopped, ..
            }) if participant_id == known_participant
        ));

        let renamed = event(
            3,
            &new_participant,
            EventKind::ParticipantStateChanged,
            ParticipantState::Idle,
            "new-name",
            "resumed",
        )?;
        assert!(matches!(
            project(&connection, renamed, &mut projection)?,
            Some(NodeEventPayload::ParticipantDeclared { participant })
                if participant.participant_id == new_participant
                    && participant.state == ParticipantState::Idle
                    && participant.name == "new-name"
        ));
        assert_eq!(projection.participants.len(), 2);
        Ok(())
    }
}
