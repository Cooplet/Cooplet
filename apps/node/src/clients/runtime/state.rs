use cooplet_protocol::{AggregateType, EventKind, ParticipantId, ParticipantState, RequestId};
use cooplet_storage::EventNotice;
use rusqlite::{Transaction, params};
use serde_json::{Value, json};

use crate::{
    clients::{AdapterBinding, ApprovalWait, PendingApprovalRecord, SubmitOutcome},
    storage::append_event,
};

use super::{
    bad_data,
    participants::{ParticipantRow, load_participant},
};

pub fn apply_submit_outcome(
    tx: &Transaction<'_>,
    now: i64,
    id: &ParticipantId,
    generation: u64,
    submitted_after_seq: u64,
    outcome: &SubmitOutcome,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<Option<PendingApprovalRecord>> {
    match outcome {
        SubmitOutcome::AwaitingApproval(wait) => enter_awaiting_approval(
            tx,
            now,
            id,
            generation,
            wait.clone(),
            ApprovalSource::Submit(submitted_after_seq),
            events,
        ),
        SubmitOutcome::Unknown(_) => {
            set_runtime_state(
                tx,
                now,
                id,
                generation,
                RuntimeStateChange {
                    state: ParticipantState::Unknown,
                    reason: Some("submit_outcome_unknown"),
                    event_seq: None,
                },
                events,
            )?;
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy)]
pub(super) enum ApprovalSource<'a> {
    Initial { created_by: Option<&'a str> },
    Event(u64),
    Submit(u64),
}

pub(super) fn enter_awaiting_approval(
    tx: &Transaction<'_>,
    now: i64,
    id: &ParticipantId,
    generation: u64,
    wait: ApprovalWait,
    source: ApprovalSource<'_>,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<Option<PendingApprovalRecord>> {
    let Some(mut participant) = live_row(tx, id, generation)? else {
        return Ok(None);
    };
    let previous_state = participant.state;
    let native_id = wait
        .approval_id
        .as_deref()
        .filter(|id| !id.trim().is_empty());
    let current = participant
        .binding
        .core
        .pending_approval
        .as_ref()
        .filter(|prior| prior.native_id.as_deref() == native_id && prior.kind == wait.kind);
    let last_event_seq = participant.binding.core.runtime_event_seq.unwrap_or(0);
    let event_seq = match source {
        ApprovalSource::Submit(cutoff) if cutoff < last_event_seq => return Ok(current.cloned()),
        ApprovalSource::Event(seq) if seq <= last_event_seq => return Ok(current.cloned()),
        ApprovalSource::Event(seq) => Some(seq),
        _ => None,
    };
    if let Some(seq) = event_seq {
        participant.binding.core.runtime_event_seq = Some(seq);
    }
    let public_id = if let Some(previous) = current {
        previous.public_id.clone()
    } else {
        RequestId::generate()
            .map_err(bad_data)?
            .as_str()
            .replacen("req_", "apr_", 1)
    };
    let pending = PendingApprovalRecord::from_wait(
        public_id,
        wait,
        participant.capabilities.supports_remote_approval,
    );
    if let Some(previous) = current
        && (previous.can_respond_remotely || !pending.can_respond_remotely)
    {
        let previous = previous.clone();
        if event_seq.is_some()
            && !write_runtime(
                tx,
                id,
                generation,
                now,
                previous_state,
                &participant.binding,
            )?
        {
            return Ok(None);
        }
        return Ok(Some(previous));
    }
    participant.binding.core.pending_approval = Some(pending.clone());
    participant.binding.core.state_reason = None;
    if !write_runtime(
        tx,
        id,
        generation,
        now,
        ParticipantState::AwaitingApproval,
        &participant.binding,
    )? {
        return Ok(None);
    }
    let payload = approval_payload(id, &pending);
    if let ApprovalSource::Initial {
        created_by: Some(created_by),
    } = source
    {
        participant_event(
            tx,
            now,
            EventKind::ParticipantCreated,
            id,
            Some(created_by),
            payload.clone(),
            events,
        )?;
    } else if previous_state != ParticipantState::AwaitingApproval {
        let mut state_payload = payload.clone();
        if matches!(source, ApprovalSource::Initial { created_by: None }) {
            state_payload["reason"] = Value::from("resumed");
        }
        participant_event(
            tx,
            now,
            EventKind::ParticipantStateChanged,
            id,
            None,
            state_payload,
            events,
        )?;
    }
    participant_event(
        tx,
        now,
        EventKind::ParticipantAwaitingApproval,
        id,
        None,
        payload,
        events,
    )?;
    Ok(Some(pending))
}

pub(super) fn approval_payload(id: &ParticipantId, pending: &PendingApprovalRecord) -> Value {
    let wait = pending.as_wait();
    json!({
        "participant_id": id, "state": ParticipantState::AwaitingApproval,
        "approval_id": wait.approval_id, "wait_kind": wait.kind, "summary": wait.summary,
        "can_respond_remotely": wait.can_respond_remotely, "action_hint": wait.action_hint,
    })
}

pub(super) struct RuntimeStateChange<'a> {
    pub(super) state: ParticipantState,
    pub(super) reason: Option<&'a str>,
    pub(super) event_seq: Option<u64>,
}

pub(super) fn set_runtime_state(
    tx: &Transaction<'_>,
    now: i64,
    id: &ParticipantId,
    generation: u64,
    change: RuntimeStateChange<'_>,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<bool> {
    let Some(mut participant) = live_row(tx, id, generation)? else {
        return Ok(false);
    };
    if let Some(event_seq) = change.event_seq {
        if event_seq <= participant.binding.core.runtime_event_seq.unwrap_or(0) {
            return Ok(true);
        }
        participant.binding.core.runtime_event_seq = Some(event_seq);
    }
    if change.state == participant.state {
        return if change.event_seq.is_some() {
            write_runtime(tx, id, generation, now, change.state, &participant.binding)
        } else {
            Ok(true)
        };
    }
    participant.binding.core.pending_approval = None;
    participant.binding.core.state_reason = change.reason.map(str::to_owned);
    if !write_runtime(tx, id, generation, now, change.state, &participant.binding)? {
        return Ok(false);
    }
    let mut payload = json!({"participant_id": id, "state": change.state});
    if let Some(reason) = change.reason {
        payload["reason"] = json!(reason);
    }
    participant_event(
        tx,
        now,
        EventKind::ParticipantStateChanged,
        id,
        None,
        payload,
        events,
    )?;
    Ok(true)
}

pub(super) fn participant_event(
    tx: &Transaction<'_>,
    now: i64,
    kind: EventKind,
    id: &ParticipantId,
    actor: Option<&str>,
    mut payload: Value,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    let (node_id, name, state): (String, String, String) = tx.query_row(
        "SELECT n.node_id, p.name, p.state FROM participants p CROSS JOIN node_meta n
         WHERE p.participant_id = ?1 AND n.id = 1",
        [id.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let state: ParticipantState = state.parse().map_err(bad_data)?;
    payload["node_id"] = json!(node_id);
    payload["participant_id"] = json!(id);
    payload["name"] = json!(name);
    payload["state"] = json!(state);
    payload["intake_state"] = json!(state.intake_state());
    events.push(append_event(
        tx,
        now,
        kind,
        AggregateType::Participant,
        id.as_str(),
        actor,
        payload,
    )?);
    Ok(())
}

fn live_row(
    tx: &Transaction<'_>,
    id: &ParticipantId,
    generation: u64,
) -> rusqlite::Result<Option<ParticipantRow>> {
    Ok(load_participant(tx, id)?.filter(|p| {
        p.generation == generation
            && !matches!(
                p.state,
                ParticipantState::Stopping | ParticipantState::Stopped | ParticipantState::Unknown
            )
    }))
}

fn write_runtime(
    tx: &Transaction<'_>,
    id: &ParticipantId,
    generation: u64,
    now: i64,
    state: ParticipantState,
    binding: &AdapterBinding,
) -> rusqlite::Result<bool> {
    let binding = serde_json::to_string(binding).map_err(bad_data)?;
    let count = tx.execute(
        "UPDATE participants SET state = ?3, adapter_binding_json = ?4, updated_at_ms = ?5
         WHERE participant_id = ?1 AND generation = ?2 AND state NOT IN ('stopping','stopped','unknown')",
        params![id.as_str(), generation, state.as_str(), binding, now],
    )?;
    Ok(count == 1)
}

#[cfg(test)]
mod tests {
    use cooplet_protocol::{ApprovalWaitKind, NodeId};
    use rusqlite::Connection;

    use super::*;
    use crate::storage::MIGRATIONS;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    struct Session {
        conn: Connection,
        id: ParticipantId,
    }

    impl Session {
        fn open() -> TestResult<Self> {
            let conn = Connection::open_in_memory()?;
            conn.execute_batch(MIGRATIONS[0].sql)?;
            let node_id = NodeId::generate()?;
            let id = ParticipantId::generate()?;
            conn.execute(
                "INSERT INTO node_meta (id,node_id,display_name,created_at_ms,updated_at_ms)
                 VALUES (1,?1,'test',1,1)",
                [node_id.as_str()],
            )?;
            conn.execute(
                "INSERT INTO participants (participant_id,kind,name,state,capabilities_json,created_at_ms,updated_at_ms)
                 VALUES (?1,'agent','test','idle','{\"supports_remote_approval\":true}',1,1)",
                [id.as_str()],
            )?;
            Ok(Self { conn, id })
        }

        fn event_wait(&mut self, seq: u64, native_id: &str) -> TestResult<String> {
            let tx = self.conn.transaction()?;
            let approval = enter_awaiting_approval(
                &tx,
                1,
                &self.id,
                1,
                wait(native_id),
                ApprovalSource::Event(seq),
                &mut Vec::new(),
            )?
            .ok_or("The event should resolve to an approval")?;
            tx.commit()?;
            Ok(approval.public_id)
        }

        fn event_state(&mut self, seq: u64, state: ParticipantState) -> TestResult {
            let tx = self.conn.transaction()?;
            assert!(set_runtime_state(
                &tx,
                1,
                &self.id,
                1,
                RuntimeStateChange {
                    state,
                    reason: None,
                    event_seq: Some(seq)
                },
                &mut Vec::new(),
            )?);
            tx.commit()?;
            Ok(())
        }

        fn ack(
            &mut self,
            cutoff: u64,
            native_id: &str,
        ) -> TestResult<Option<PendingApprovalRecord>> {
            let tx = self.conn.transaction()?;
            let mut events = Vec::new();
            let approval = apply_submit_outcome(
                &tx,
                1,
                &self.id,
                1,
                cutoff,
                &SubmitOutcome::AwaitingApproval(wait(native_id)),
                &mut events,
            )?;
            if approval.is_none() {
                assert!(
                    events.is_empty(),
                    "A superseded ACK must not publish an approval"
                );
            }
            tx.commit()?;
            Ok(approval)
        }

        fn read(&self) -> TestResult<ParticipantRow> {
            load_participant(&self.conn, &self.id)?.ok_or_else(|| "Missing participant".into())
        }
    }

    fn wait(native_id: &str) -> ApprovalWait {
        ApprovalWait {
            approval_id: Some(native_id.into()),
            kind: ApprovalWaitKind::Permission,
            summary: None,
            can_respond_remotely: true,
            action_hint: None,
        }
    }

    #[test]
    fn late_ack_cannot_recreate_a_closed_wait_or_replace_its_successor() -> TestResult {
        let mut session = Session::open()?;
        let a = session.event_wait(1, "native-a")?;
        assert_eq!(
            session.ack(0, "native-a")?.map(|record| record.public_id),
            Some(a)
        );
        session.event_state(2, ParticipantState::Idle)?;
        assert!(session.ack(0, "native-a")?.is_none());
        assert_eq!(session.read()?.state, ParticipantState::Idle);

        session.event_state(3, ParticipantState::Working)?;
        assert!(session.ack(0, "native-a")?.is_none());
        assert_eq!(session.read()?.state, ParticipantState::Working);

        let b = session.event_wait(4, "native-b")?;
        assert!(session.ack(0, "native-a")?.is_none());
        let row = session.read()?;
        assert_eq!(row.state, ParticipantState::AwaitingApproval);
        assert_eq!(
            row.binding
                .core
                .pending_approval
                .map(|record| record.public_id),
            Some(b)
        );
        Ok(())
    }

    #[test]
    fn newer_events_win_in_both_transaction_orders_even_without_a_state_change() -> TestResult {
        let mut session = Session::open()?;
        session.event_state(1, ParticipantState::Idle)?;
        assert!(session.ack(0, "native-a")?.is_none());

        assert!(session.ack(1, "native-a")?.is_some());
        session.event_state(2, ParticipantState::Idle)?;
        let row = session.read()?;
        assert_eq!(row.state, ParticipantState::Idle);
        assert!(row.binding.core.pending_approval.is_none());
        assert_eq!(row.binding.core.runtime_event_seq, Some(2));
        Ok(())
    }
}
