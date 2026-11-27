use std::time::{Duration, Instant};

use cooplet_platform::{ProcessHandle, ProcessIdentity, TerminateOutcome, current_linux_boot_id};
use cooplet_protocol::{AggregateType, EventKind, ParticipantState};
use cooplet_storage::{DbActor, EventNotice, StorageEngine, StorageError, WriteOutcome};
use rusqlite::{Connection, Row, Transaction, params};
use serde_json::json;
use tokio::sync::watch;

use super::{
    DAY_MS,
    events::{append_event, delivery_event, participant_event, read_node_id},
    gc::expire_queued,
};

const CLEANUP_BUDGET: Duration = Duration::from_secs(5);
const TERMINATE_GRACE: Duration = Duration::from_millis(250);
const INTERRUPTED_ON_STARTUP: &str = "interrupted_on_startup";
const RECEIPT_UNCONFIRMED: &str = "receipt_unconfirmed";

/// Complete an idempotent repair after a live session has fenced its input.
/// Startup recovery takes over if the daemon shuts down before SQLite recovers.
pub(crate) async fn retry_recovery_write(
    db: DbActor,
    mut stopping: watch::Receiver<bool>,
    repair: impl Fn(&Transaction<'_>, i64) -> rusqlite::Result<Vec<EventNotice>> + Send + Sync + 'static,
) {
    let repair = std::sync::Arc::new(repair);
    let initial_delay = Duration::from_millis(20);
    let mut delay = initial_delay;
    while !*stopping.borrow() {
        let repair = repair.clone();
        let attempt = db
            .write_sqlite(move |tx| {
                let now = crate::now_ms().map_err(super::bad_data)?;
                Ok(WriteOutcome::with_events((), repair(tx, now)?))
            })
            .await;
        match attempt {
            Ok(()) | Err(StorageError::Closed | StorageError::WriterPanicked) => return,
            Err(error) if delay == initial_delay => {
                tracing::warn!(%error, "Retrying session recovery after a failed write");
            }
            Err(_) => {}
        }
        tokio::select! {
            _ = stopping.wait_for(|stop| *stop) => return,
            () = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

/// Reconcile durable state before accepting commands or enabling retention.
pub async fn recover(engine: &StorageEngine, now_ms: i64) -> Result<(), StorageError> {
    recover_with_boot_id(engine, now_ms, current_linux_boot_id()).await
}

pub(super) async fn recover_with_boot_id(
    engine: &StorageEngine,
    now_ms: i64,
    boot_id: Option<String>,
) -> Result<(), StorageError> {
    let recovery_boot_id = boot_id.clone();
    engine
        .db()
        .write_sqlite(move |tx| recover_transactions(tx, now_ms, recovery_boot_id.as_deref()))
        .await?;

    let candidates = engine
        .reads()
        .read(|conn| Ok(cleanup_candidates(conn)?))
        .await?;
    if candidates.is_empty() {
        return Ok(());
    }
    let proofs =
        tokio::task::spawn_blocking(move || reconcile_processes(candidates, boot_id.as_deref()))
            .await?;
    engine
        .db()
        .write_sqlite(move |tx| finalize_cleanup(tx, now_ms, proofs))
        .await
}

fn recover_transactions(
    tx: &Transaction<'_>,
    now: i64,
    boot_id: Option<&str>,
) -> rusqlite::Result<WriteOutcome<()>> {
    let mut events = recover_delivery(tx, now)?;
    recover_participants(tx, now, boot_id, &mut events)?;
    tx.execute(
        "UPDATE participant_credentials SET revoked_at_ms = ?1
         WHERE revoked_at_ms IS NULL AND (
             workflow_run_id IN (SELECT run_id FROM workflow_runs WHERE state = 'running')
             OR participant_id IN (
                 SELECT participant_id FROM participants
                 WHERE ephemeral_for_run_id IN (SELECT run_id FROM workflow_runs WHERE state = 'running')
             )
         )",
        [now],
    )?;
    let cleanup_error = json!({
        "code": "cleanup_unverified",
        "detail": INTERRUPTED_ON_STARTUP,
    });
    tx.execute(
        "UPDATE workflow_runs SET state = 'interrupted', error_json = ?2,
             updated_at_ms = ?1, finished_at_ms = NULL WHERE state = 'running'",
        params![now, cleanup_error.to_string()],
    )?;
    Ok(WriteOutcome::with_events((), events))
}

fn recover_delivery(tx: &Transaction<'_>, now: i64) -> rusqlite::Result<Vec<EventNotice>> {
    let mut messages = tx
        .prepare(
            "UPDATE messages SET delivery_state = 'unknown', delivery_detail = ?1
             WHERE (delivery_state = 'accepted' AND operation_id IN (
                 SELECT operation_id FROM operations WHERE state IN ('accepted', 'in_progress')
             )) OR message_id IN (SELECT message_id FROM intake_queue WHERE state = 'dispatched')
             RETURNING message_id",
        )?
        .query_map([INTERRUPTED_ON_STARTUP], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    messages.sort_unstable();
    let mut events = messages
        .iter()
        .map(|id| delivery_event(tx, now, id, "unknown", INTERRUPTED_ON_STARTUP))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    finish_unconfirmed_answers(tx, now, None)?;
    tx.execute(
        "UPDATE operations SET state = 'unknown', updated_at_ms = ?1
         WHERE state IN ('accepted', 'in_progress')",
        [now],
    )?;
    tx.execute(
        "UPDATE intake_queue SET state = 'unknown', resolved_at_ms = ?1, payload_envelope = '' WHERE state = 'dispatched'",
        [now],
    )?;
    events.extend(expire_queued(tx, now, -1)?);
    tx.execute(
        "UPDATE thread_inject_fences SET state = 'unknown', updated_at_ms = ?1 WHERE state = 'injecting'",
        [now],
    )?;
    Ok(events)
}

/// Reconcile a direct message whose guarded executor has already returned.
/// No future can submit for this operation after the caller acquires its key.
pub(crate) fn reconcile_pending_message(
    tx: &Transaction<'_>,
    now: i64,
    operation_id: &str,
) -> rusqlite::Result<Vec<EventNotice>> {
    let mut statement = tx.prepare(
        "UPDATE messages SET delivery_state = 'unknown', delivery_detail = ?1
         WHERE operation_id = ?2 AND delivery_state = 'accepted'
         RETURNING message_id",
    )?;
    let messages = statement
        .query_map(params![RECEIPT_UNCONFIRMED, operation_id], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let events = messages
        .iter()
        .map(|id| delivery_event(tx, now, id, "unknown", RECEIPT_UNCONFIRMED))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    finish_unconfirmed_answers(tx, now, Some(operation_id))?;
    tx.execute(
        "UPDATE operations SET state = 'unknown', updated_at_ms = ?1
         WHERE operation_id = ?2 AND kind = 'message.send' AND state IN ('accepted', 'in_progress')",
        params![now, operation_id],
    )?;
    Ok(events)
}

fn finish_unconfirmed_answers(
    tx: &Transaction<'_>,
    now: i64,
    operation_id: Option<&str>,
) -> rusqlite::Result<()> {
    const UPDATE: &str =
        "UPDATE operations AS o SET state = 'succeeded', updated_at_ms = ?1, error_json = NULL,
             result_json = json_object(
                 'operation_id', o.operation_id,
                 'message_id', m.message_id,
                 'delivery_state', m.delivery_state,
                 'in_reply_to', m.in_reply_to
             )
         FROM messages AS m
         WHERE o.state IN ('accepted', 'in_progress') AND o.kind = 'message.send'
           AND m.operation_id = o.operation_id AND m.answer_json IS NOT NULL
           AND m.delivery_state = 'unknown'";
    match operation_id {
        Some(id) => tx.execute(
            &format!("{UPDATE} AND o.operation_id = ?2"),
            params![now, id],
        )?,
        None => tx.execute(UPDATE, [now])?,
    };
    Ok(())
}

fn recover_participants(
    tx: &Transaction<'_>,
    now: i64,
    boot_id: Option<&str>,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    tx.execute(
        "UPDATE participants SET adapter_binding_json = json_remove(adapter_binding_json, '$.core.pending_approval')
         WHERE kind = 'agent' AND json_type(adapter_binding_json, '$.core.pending_approval') IS NOT NULL",
        [],
    )?;
    tx.execute(
        "UPDATE participants SET adapter_binding_json = json_set(adapter_binding_json, '$.core.approval_response.state', 'unknown')
         WHERE kind = 'agent' AND json_extract(adapter_binding_json, '$.core.approval_response.state') = 'in_flight'",
        [],
    )?;
    tx.execute(
        "UPDATE participant_credentials SET revoked_at_ms = ?1
         WHERE revoked_at_ms IS NULL AND participant_id IN (
             SELECT participant_id FROM participants WHERE kind = 'agent' AND state != 'stopped'
         )",
        [now],
    )?;
    let mut lost_sessions = tx
        .prepare(
            "UPDATE participants SET state = 'unknown', updated_at_ms = ?1
             WHERE kind = 'agent' AND state NOT IN ('stopped', 'unknown')
             RETURNING participant_id, name",
        )?
        .query_map([now], participant_name)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    lost_sessions.sort_unstable();
    for (id, name) in lost_sessions {
        events.push(participant_event(
            tx,
            now,
            &id,
            &name,
            ParticipantState::Unknown,
            INTERRUPTED_ON_STARTUP,
        )?);
    }
    if let Some(boot_id) = boot_id {
        complete_rebooted_participants(tx, now, boot_id, events)?;
    }
    Ok(())
}

fn complete_rebooted_participants(
    tx: &Transaction<'_>,
    now: i64,
    boot_id: &str,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    let mut rebooted = tx
        .prepare(
            "UPDATE participants SET state = 'stopped', updated_at_ms = ?1, stopped_at_ms = ?1,
                 adapter_binding_json = json_remove(adapter_binding_json, '$.core.lifecycle_block'),
                 runtime_pid = NULL, runtime_pgid = NULL, runtime_start_token = NULL
             WHERE kind = 'agent' AND state = 'unknown' AND process_ownership = 'managed'
               AND json_extract(adapter_binding_json, '$.core.lifecycle_block.kind')
                   IN ('spawn_uncertain', 'cleanup_unverified')
               AND json_type(adapter_binding_json, '$.core.lifecycle_block.boot_id') = 'text'
               AND json_extract(adapter_binding_json, '$.core.lifecycle_block.boot_id') != ?2
             RETURNING participant_id, name",
        )?
        .query_map(params![now, boot_id], participant_name)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rebooted.sort_unstable();
    for (id, name) in rebooted {
        revoke_participant_credentials(tx, now, &id)?;
        events.push(participant_event(
            tx,
            now,
            &id,
            &name,
            ParticipantState::Stopped,
            "host_rebooted",
        )?);
    }
    Ok(())
}

fn participant_name(row: &Row<'_>) -> rusqlite::Result<(String, String)> {
    Ok((row.get(0)?, row.get(1)?))
}

pub(crate) fn revoke_participant_credentials(
    tx: &Transaction<'_>,
    now: i64,
    id: &str,
) -> rusqlite::Result<()> {
    tx.execute(
        "UPDATE participant_credentials SET revoked_at_ms = ?1
         WHERE participant_id = ?2 AND revoked_at_ms IS NULL",
        params![now, id],
    )?;
    Ok(())
}

struct Run {
    id: String,
    name: String,
    identity: StoredIdentity,
    participants: Vec<ManagedParticipant>,
}

struct ManagedParticipant {
    id: String,
    name: String,
    generation: i64,
    identity: StoredIdentity,
    lifecycle_boot_id: Option<String>,
}

// Store the observed values for the final SQL CAS, including half-written or
// invalid fingerprints that cannot authorize a signal.
struct StoredIdentity {
    pid: Option<i64>,
    pgid: Option<i64>,
    start_token: Option<String>,
}

impl StoredIdentity {
    fn read(row: &Row<'_>, offset: usize) -> rusqlite::Result<Self> {
        Ok(Self {
            pid: row.get(offset)?,
            pgid: row.get(offset + 1)?,
            start_token: row.get(offset + 2)?,
        })
    }

    fn process_identity(&self) -> Option<ProcessIdentity> {
        Some(ProcessIdentity {
            pid: u32::try_from(self.pid?).ok()?,
            pgid: u32::try_from(self.pgid?).ok()?,
            start_token: self.start_token.clone()?,
        })
    }
}

fn cleanup_candidates(conn: &Connection) -> rusqlite::Result<Vec<Run>> {
    let mut runs = conn
        .prepare(
            "SELECT run_id, workflow_name, runner_pid, runner_pgid, runner_start_token
             FROM workflow_runs WHERE state = 'interrupted' AND finished_at_ms IS NULL
             ORDER BY run_id",
        )?
        .query_map([], |row| {
            Ok(Run {
                id: row.get(0)?,
                name: row.get(1)?,
                identity: StoredIdentity::read(row, 2)?,
                participants: Vec::new(),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut participants = conn.prepare(
        "SELECT participant_id, name, generation, runtime_pid, runtime_pgid, runtime_start_token,
             CASE WHEN json_extract(adapter_binding_json, '$.core.lifecycle_block.kind')
                      IN ('spawn_uncertain', 'cleanup_unverified')
                     AND json_type(adapter_binding_json, '$.core.lifecycle_block.boot_id') = 'text'
                 THEN json_extract(adapter_binding_json, '$.core.lifecycle_block.boot_id')
             END
         FROM participants
         WHERE ephemeral_for_run_id = ?1 AND process_ownership = 'managed' AND state != 'stopped'
         ORDER BY participant_id",
    )?;
    for run in &mut runs {
        run.participants = participants
            .query_map([&run.id], |row| {
                Ok(ManagedParticipant {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    generation: row.get(2)?,
                    identity: StoredIdentity::read(row, 3)?,
                    lifecycle_boot_id: row.get(6)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(runs)
}

struct CleanupProof {
    run: Run,
    runner_gone: bool,
    stopped_participants: Vec<(ManagedParticipant, &'static str)>,
}

fn reconcile_processes(candidates: Vec<Run>, boot_id: Option<&str>) -> Vec<CleanupProof> {
    let deadline = Instant::now() + CLEANUP_BUDGET;
    let mut proofs = Vec::new();
    for mut run in candidates {
        if Instant::now() >= deadline {
            break;
        }
        let runner_gone = terminate(&run.identity, deadline);
        let stopped_participants = std::mem::take(&mut run.participants)
            .into_iter()
            .filter_map(|participant| {
                let reason = verified_cleanup(&participant, boot_id, deadline)?;
                Some((participant, reason))
            })
            .collect::<Vec<_>>();
        if runner_gone || !stopped_participants.is_empty() {
            proofs.push(CleanupProof {
                run,
                runner_gone,
                stopped_participants,
            });
        }
    }
    proofs
}

fn verified_cleanup(
    participant: &ManagedParticipant,
    boot_id: Option<&str>,
    deadline: Instant,
) -> Option<&'static str> {
    if boot_id
        .zip(participant.lifecycle_boot_id.as_deref())
        .is_some_and(|(current, previous)| current != previous)
    {
        return Some("host_rebooted");
    }
    terminate(&participant.identity, deadline).then_some("workflow_recovery")
}

fn terminate(identity: &StoredIdentity, deadline: Instant) -> bool {
    let Some(identity) = identity.process_identity() else {
        return false;
    };
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return false;
    };
    match ProcessHandle::terminate_detached(&identity, remaining.min(TERMINATE_GRACE)) {
        Ok(
            TerminateOutcome::AlreadyGone
            | TerminateOutcome::TerminatedGracefully
            | TerminateOutcome::KilledAfterGrace,
        ) => true,
        Ok(TerminateOutcome::CleanupUnverified) => false,
        Err(error) => {
            tracing::warn!(%error, pid = identity.pid, "workflow process cleanup is unverified");
            false
        }
    }
}

fn finalize_cleanup(
    tx: &Transaction<'_>,
    now: i64,
    proofs: Vec<CleanupProof>,
) -> rusqlite::Result<WriteOutcome<()>> {
    let mut events = Vec::new();
    for proof in proofs {
        for (participant, reason) in proof.stopped_participants {
            if stop_managed(tx, now, &proof.run.id, &participant)? {
                revoke_participant_credentials(tx, now, &participant.id)?;
                events.push(participant_event(
                    tx,
                    now,
                    &participant.id,
                    &participant.name,
                    ParticipantState::Stopped,
                    reason,
                )?);
            }
        }
        if proof.runner_gone && finish_interrupted_run(tx, now, &proof.run)? {
            let node_id = read_node_id(tx)?;
            events.push(append_event(
                tx,
                now,
                EventKind::WorkflowRunFinished,
                AggregateType::WorkflowRun,
                &proof.run.id,
                None,
                json!({
                    "node_id": node_id,
                    "run_id": proof.run.id,
                    "workflow_name": proof.run.name,
                    "state": "interrupted",
                }),
            )?);
        }
    }
    Ok(WriteOutcome::with_events((), events))
}

fn stop_managed(
    tx: &Transaction<'_>,
    now: i64,
    run_id: &str,
    participant: &ManagedParticipant,
) -> rusqlite::Result<bool> {
    let rows = tx.execute(
        "UPDATE participants SET state = 'stopped', updated_at_ms = ?1, stopped_at_ms = ?1,
             adapter_binding_json = json_remove(adapter_binding_json, '$.core.lifecycle_block'),
             runtime_pid = NULL, runtime_pgid = NULL, runtime_start_token = NULL
         WHERE participant_id = ?2 AND ephemeral_for_run_id = ?3 AND generation = ?4
           AND process_ownership = 'managed' AND state != 'stopped'
           AND runtime_pid IS ?5 AND runtime_pgid IS ?6 AND runtime_start_token IS ?7",
        params![
            now,
            participant.id,
            run_id,
            participant.generation,
            participant.identity.pid,
            participant.identity.pgid,
            participant.identity.start_token,
        ],
    )?;
    Ok(rows == 1)
}

fn finish_interrupted_run(tx: &Transaction<'_>, now: i64, run: &Run) -> rusqlite::Result<bool> {
    let rows = tx.execute(
        "UPDATE workflow_runs SET finished_at_ms = ?1, updated_at_ms = ?1,
             expires_at_ms = ?2, error_json = NULL
         WHERE run_id = ?3 AND state = 'interrupted' AND finished_at_ms IS NULL
           AND runner_pid IS ?4 AND runner_pgid IS ?5 AND runner_start_token IS ?6
           AND NOT EXISTS (
               SELECT 1 FROM participants WHERE ephemeral_for_run_id = ?3
                   AND process_ownership = 'managed' AND state != 'stopped'
           )",
        params![
            now,
            now.saturating_add(30 * DAY_MS),
            run.id,
            run.identity.pid,
            run.identity.pgid,
            run.identity.start_token,
        ],
    )?;
    Ok(rows == 1)
}
