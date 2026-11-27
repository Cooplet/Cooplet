use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{
    ErrorCode, EventKind, OperationId, ParticipantId, ParticipantKind, ParticipantState,
};
use cooplet_storage::{EventNotice, WriteOutcome};
use futures_util::FutureExt;
use rusqlite::{Transaction, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio::sync::watch;
use utoipa::ToSchema;

use crate::{
    clients::{LifecycleBlock, LifecycleBlockKind, ProcessOwnership, StopMode, StopOutcome},
    error::Result,
    now_ms,
    operations::Operation,
    storage::revoke_participant_credentials,
};

use super::{
    AgentService, LiveSession, SessionKey,
    approvals::abandon_inflight_approval,
    bad_data, busy, invalid,
    lifecycle::{Reservation, WriteResult, decode, remove_runtime_dir, unlink_credential},
    lock, not_found,
    participants::{ParticipantRow, load_participant},
    state::participant_event,
    unknown,
};

const STOP_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

async fn await_interrupt(
    mut disconnected: watch::Receiver<()>,
    admission: &AdmissionGuard,
    call: impl Future<Output = Result<StopOutcome>>,
) -> Option<StopOutcome> {
    let started = AtomicBool::new(false);
    let response = async {
        started.store(true, Ordering::Relaxed);
        tokio::time::timeout(STOP_REQUEST_TIMEOUT, call)
            .await
            .ok()?
            .ok()
    };
    tokio::pin!(response);
    tokio::select! {
        biased;
        _ = disconnected.changed() => {
            admission.expire();
            if !started.load(Ordering::Relaxed) {
                return Some(StopOutcome::AlreadyGone);
            }
            response.as_mut().now_or_never().flatten()
        }
        result = response.as_mut() => result,
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StopAgentRequest {
    #[serde(default)]
    pub cancel_turn_only: bool,
    #[serde(default)]
    pub acknowledge_manual_cleanup: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StopDisposition {
    Stopped,
    TurnCancelled,
    AlreadyStopped,
    CleanupUnverified,
    AcknowledgedManualCleanup,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct StopAgentResponse {
    pub operation_id: OperationId,
    pub participant_id: ParticipantId,
    pub state: ParticipantState,
    pub stop_outcome: StopDisposition,
}

enum StopWork {
    Finish(StopAgentResponse, Option<SessionKey>),
    Cancel(ParticipantState, Arc<LiveSession>),
    Disconnect(Box<ParticipantRow>),
}

impl AgentService {
    pub async fn stop(
        &self,
        actor: &str,
        key: &str,
        id: ParticipantId,
        request: StopAgentRequest,
    ) -> Result<StopAgentResponse> {
        if request.cancel_turn_only && request.acknowledge_manual_cleanup {
            return Err(invalid(
                "conflicting_stop_options",
                "Cancellation and manual cleanup acknowledgment cannot be combined",
            ));
        }
        let guard = self
            .0
            .operations
            .guard(actor, key, "agent.stop", &(&id, request))?;
        let op = &guard.operation;
        if let Some(value) = self.0.operations.lookup(op).await? {
            return decode(value);
        }
        let _mutation = self.mutation(&id)?;
        let now = now_ms()?;
        let live = self.live_session(&id);
        let write_op = op.clone();
        let reserve = self
            .0
            .db
            .write(move |tx| -> WriteResult<_> {
                if let Some(value) = write_op.lookup(tx)? {
                    return Ok(WriteOutcome::new(Reservation::Replay(value)));
                }
                let mut row = load_participant(tx, &id)?.ok_or_else(not_found)?;
                if row.kind != ParticipantKind::Agent {
                    return Err(
                        invalid("not_an_agent", "Only agents have a session to stop").into(),
                    );
                }
                if request.acknowledge_manual_cleanup
                    && (row.state != ParticipantState::Unknown
                        || row.binding.core.lifecycle_block.is_none())
                {
                    return Err(invalid(
                        "manual_cleanup_not_required",
                        "This agent has no unverified cleanup to acknowledge",
                    )
                    .into());
                }
                if request.cancel_turn_only
                    && matches!(
                        row.state,
                        ParticipantState::Starting | ParticipantState::Stopping
                    )
                {
                    return Err(busy("lifecycle_in_progress", true).into());
                }
                let mut events = Vec::new();
                let work = if request.acknowledge_manual_cleanup {
                    revoke_participant_credentials(tx, now, row.id.as_str())?;
                    finish_stop(
                        tx,
                        &mut row,
                        StopUpdate {
                            now,
                            clean: true,
                            boot_id: None,
                            reason: "manual_cleanup_acknowledged",
                            actor: Some(&write_op.actor_id),
                        },
                        &mut events,
                    )?;
                    StopWork::Finish(
                        stop_response(&write_op, &row, StopDisposition::AcknowledgedManualCleanup),
                        Some((row.id.clone(), row.generation)),
                    )
                } else if request.cancel_turn_only {
                    let live = live
                        .filter(|live| {
                            live.generation() == row.generation
                                && live.state() != ParticipantState::Unknown
                                && !matches!(
                                    row.state,
                                    ParticipantState::Unknown | ParticipantState::Stopped
                                )
                        })
                        .ok_or_else(|| busy("no_live_session", true))?;
                    StopWork::Cancel(row.state, live)
                } else if row.state == ParticipantState::Stopped {
                    StopWork::Finish(
                        stop_response(&write_op, &row, StopDisposition::AlreadyStopped),
                        None,
                    )
                } else {
                    // The mutation lease excludes a live open or stop. An
                    // interrupted transition can continue through group cleanup.
                    if row.state != ParticipantState::Stopping {
                        transition_stopping(
                            tx,
                            &mut row,
                            now,
                            Some(&write_op.actor_id),
                            &mut events,
                        )?;
                    }
                    StopWork::Disconnect(Box::new(row))
                };
                write_op.reserve(tx, now, Some(id.as_str()))?;
                if let StopWork::Finish(response, _) = &work {
                    write_op.succeed(tx, now, response)?;
                }
                Ok(WriteOutcome::with_events(Reservation::New(work), events))
            })
            .await?;
        match reserve {
            Reservation::Replay(value) => decode(value),
            Reservation::New(work) => {
                guard.mark_committed();
                match work {
                    StopWork::Finish(response, credential) => {
                        if let Some(key) = credential {
                            self.disconnect(&key.0, key.1);
                            self.reap_after_manual_cleanup(&key);
                            unlink_credential(self.0.config.state_dir.clone(), key.clone()).await;
                            remove_runtime_dir(self.0.config.state_dir.clone(), key).await;
                        }
                        Ok(response)
                    }
                    StopWork::Cancel(state, live) => self.cancel_turn(op, state, &live).await,
                    StopWork::Disconnect(row) => {
                        let clean = self.stop_transport(&row).await;
                        if clean {
                            remove_runtime_dir(
                                self.0.config.state_dir.clone(),
                                (row.id.clone(), row.generation),
                            )
                            .await;
                        }
                        let response = self.persist_stop(&row, clean, op).await;
                        self.confirm_operation(op, response, "stop_result_unconfirmed")
                            .await
                    }
                }
            }
        }
    }

    async fn cancel_turn(
        &self,
        operation: &Operation,
        state: ParticipantState,
        live: &LiveSession,
    ) -> Result<StopAgentResponse> {
        let disconnected = live.event_stop();
        let outcome = if live.state() == ParticipantState::Unknown {
            Some(StopOutcome::AlreadyGone)
        } else {
            let admission = AdmissionGuard::new(STOP_REQUEST_TIMEOUT);
            await_interrupt(disconnected, &admission, async {
                live.adapter
                    .cancel_or_stop(
                        &live.handle,
                        StopMode::CancelTurn {
                            admission: admission.clone(),
                        },
                    )
                    .await
            })
            .await
        };
        let id = live.handle.persisted().participant_id.clone();
        let state = self
            .participant_row(&id)
            .await
            .map_or(state, |row| row.state);
        let response = StopAgentResponse {
            operation_id: operation.id.clone(),
            participant_id: id,
            state,
            stop_outcome: StopDisposition::TurnCancelled,
        };
        let error = match outcome {
            Some(StopOutcome::TurnCancelled) => None,
            Some(StopOutcome::Unsupported(_)) => Some(invalid(
                "cancel_unsupported",
                "This adapter cannot cancel a turn",
            )),
            Some(StopOutcome::Unavailable(_)) => Some(busy("cancel_unavailable", true)),
            Some(StopOutcome::AlreadyGone) => Some(busy("no_live_session", true)),
            _ => Some(unknown("cancel_outcome_unconfirmed")),
        };
        let now = now_ms()?;
        let op = operation.clone();
        let saved = response.clone();
        let error = error.map(Arc::new);
        let saved_error = error.clone();
        let write = self
            .0
            .operations
            .finalize(move |tx| -> WriteResult<_> {
                match saved_error.as_deref() {
                    None => op.succeed(tx, now, &saved)?,
                    Some(error) if error.code() == ErrorCode::DeliveryUnknown => {
                        op.unknown(tx, now, Some(error))?
                    }
                    Some(error) => op.fail(tx, now, error)?,
                }
                Ok(WriteOutcome::new(()))
            })
            .await;
        if let Err(error) = write {
            return self
                .confirm_operation(operation, Err(error), "cancel_result_unconfirmed")
                .await;
        }
        match error {
            Some(error) => {
                Err(Arc::try_unwrap(error)
                    .unwrap_or_else(|_| unknown("cancel_outcome_unconfirmed")))
            }
            None => Ok(response),
        }
    }

    async fn stop_transport(&self, row: &ParticipantRow) -> bool {
        let key = (row.id.clone(), row.generation);
        let live = self
            .live_session(&row.id)
            .filter(|live| live.generation() == row.generation);
        self.disconnect(&row.id, row.generation);
        let clean = if row.ownership == ProcessOwnership::Attached {
            true
        } else {
            if let Some(live) = live {
                let _ = tokio::time::timeout(
                    STOP_REQUEST_TIMEOUT,
                    live.adapter
                        .cancel_or_stop(&live.handle, StopMode::StopSession),
                )
                .await;
            }
            self.clean_owned_process(&key, row.identity.clone()).await
        };
        unlink_credential(self.0.config.state_dir.clone(), key).await;
        clean
    }

    async fn persist_stop(
        &self,
        row: &ParticipantRow,
        clean: bool,
        operation: &Operation,
    ) -> Result<StopAgentResponse> {
        let now = now_ms()?;
        let id = row.id.clone();
        let generation = row.generation;
        let boot_id = row
            .binding
            .core
            .lifecycle_block
            .as_ref()
            .and_then(|old| old.boot_id.clone())
            .or_else(|| self.0.boot.current());
        let operation = operation.clone();
        self.0
            .operations
            .finalize(move |tx| -> WriteResult<_> {
                let mut row = load_participant(tx, &id)?.ok_or_else(not_found)?;
                if row.generation != generation || row.state != ParticipantState::Stopping {
                    return Err(unknown("stop_generation_changed").into());
                }
                let mut events = Vec::new();
                let reason = if clean {
                    "operator_stop"
                } else {
                    "cleanup_unverified"
                };
                let actor = Some(operation.actor_id.as_str());
                finish_stop(
                    tx,
                    &mut row,
                    StopUpdate {
                        now,
                        clean,
                        boot_id: boot_id.clone(),
                        reason,
                        actor,
                    },
                    &mut events,
                )?;
                let disposition = if clean {
                    StopDisposition::Stopped
                } else {
                    StopDisposition::CleanupUnverified
                };
                let response = stop_response(&operation, &row, disposition);
                operation.succeed(tx, now, &response)?;
                Ok(WriteOutcome::with_events(response, events))
            })
            .await
    }

    pub(super) async fn confirm_operation<T: DeserializeOwned>(
        &self,
        operation: &Operation,
        result: Result<T>,
        reason: &'static str,
    ) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(_) => match self.0.operations.lookup(operation).await {
                Ok(Some(value)) => decode(value),
                Err(error)
                    if matches!(
                        error.code(),
                        ErrorCode::DeliveryUnknown
                            | ErrorCode::InvalidRequest
                            | ErrorCode::ParticipantBusy
                    ) =>
                {
                    Err(error)
                }
                _ => Err(unknown(reason).with_extension("operation_id", operation.id.as_str())),
            },
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut by_participant: BTreeMap<ParticipantId, Vec<u64>> = BTreeMap::new();
        {
            let sessions = lock(&self.0.sessions);
            for (id, live) in &sessions.live {
                let gens = by_participant.entry(id.clone()).or_default();
                let generation = live.generation();
                if !gens.contains(&generation) {
                    gens.push(generation);
                }
            }
            for (id, generation) in sessions.processes.keys() {
                let gens = by_participant.entry(id.clone()).or_default();
                if !gens.contains(generation) {
                    gens.push(*generation);
                }
            }
        }
        let results =
            futures_util::future::join_all(by_participant.into_iter().map(|(id, gens)| async move {
                let mut first_error = None;
                for generation in gens {
                    let key = (id.clone(), generation);
                    if let Err(error) = self.shutdown_session(&key).await {
                        tracing::warn!(participant_id = %key.0, %error, "Could not complete adapter session shutdown");
                        self.disconnect(&key.0, key.1);
                        // Only a handle retained by the spawner authorizes this fallback.
                        let cleaned = if self.has_owned_process(&key) {
                            self.clean_owned_process(&key, None).await
                        } else {
                            false
                        };
                        unlink_credential(self.0.config.state_dir.clone(), key.clone()).await;
                        if cleaned {
                            remove_runtime_dir(self.0.config.state_dir.clone(), key.clone()).await;
                        }
                        // A state/event write may fail even while credential revocation is writable.
                        let revoke_key = key.clone();
                        let revoked = self
                            .0
                            .operations
                            .finalize(move |tx| -> WriteResult<()> {
                                tx.execute(
                                    "UPDATE participant_credentials SET revoked_at_ms = ?3
                                     WHERE participant_id = ?1 AND revoked_at_ms IS NULL AND EXISTS (
                                         SELECT 1 FROM participants WHERE participant_id = ?1 AND generation = ?2)",
                                    params![
                                        revoke_key.0.as_str(),
                                        revoke_key.1,
                                        now_ms().map_err(bad_data)?
                                    ],
                                )?;
                                Ok(WriteOutcome::new(()))
                            })
                            .await;
                        if let Err(error) = revoked {
                            tracing::warn!(%error, "Could not persist shutdown credential revocation");
                        }
                        first_error.get_or_insert(error);
                    }
                }
                first_error.map_or(Ok(()), Err)
            }))
            .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn shutdown_session(&self, key: &SessionKey) -> Result<()> {
        let _mutation = self.mutation(&key.0)?;
        let (id, generation) = key.clone();
        let now = now_ms()?;
        let row = self
            .0
            .operations
            .finalize(move |tx| -> WriteResult<_> {
                let Some(mut row) = load_participant(tx, &id)? else {
                    return Ok(WriteOutcome::new(None));
                };
                if row.generation != generation || row.state == ParticipantState::Stopped {
                    return Ok(WriteOutcome::new(None));
                }
                let mut events = Vec::new();
                if row.state != ParticipantState::Stopping {
                    transition_stopping(tx, &mut row, now, None, &mut events)?;
                }
                Ok(WriteOutcome::with_events(Some(row), events))
            })
            .await?;
        let Some(row) = row else { return Ok(()) };
        let clean = self.stop_transport(&row).await;
        if clean {
            remove_runtime_dir(self.0.config.state_dir.clone(), key.clone()).await;
        }
        let now = now_ms()?;
        let boot_id = row
            .binding
            .core
            .lifecycle_block
            .as_ref()
            .and_then(|old| old.boot_id.clone())
            .or_else(|| self.0.boot.current());
        self.0
            .operations
            .finalize(move |tx| -> WriteResult<_> {
                let mut current = load_participant(tx, &row.id)?.ok_or_else(not_found)?;
                if row.generation != current.generation
                    || current.state != ParticipantState::Stopping
                {
                    return Err(unknown("shutdown_generation_changed").into());
                }
                let mut events = Vec::new();
                finish_stop(
                    tx,
                    &mut current,
                    StopUpdate {
                        now,
                        clean,
                        boot_id: boot_id.clone(),
                        reason: if clean {
                            "node_shutdown"
                        } else {
                            "cleanup_unverified"
                        },
                        actor: None,
                    },
                    &mut events,
                )?;
                Ok(WriteOutcome::with_events((), events))
            })
            .await
    }
}

fn stop_response(
    operation: &Operation,
    row: &ParticipantRow,
    disposition: StopDisposition,
) -> StopAgentResponse {
    StopAgentResponse {
        operation_id: operation.id.clone(),
        participant_id: row.id.clone(),
        state: row.state,
        stop_outcome: disposition,
    }
}

fn transition_stopping(
    tx: &Transaction<'_>,
    row: &mut ParticipantRow,
    now: i64,
    actor: Option<&str>,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    row.state = ParticipantState::Stopping;
    row.binding.core.pending_approval = None;
    abandon_inflight_approval(tx, row, now)?;
    let binding = serde_json::to_string(&row.binding).map_err(bad_data)?;
    tx.execute(
        "UPDATE participants SET state = 'stopping', adapter_binding_json = ?3, updated_at_ms = ?4
         WHERE participant_id = ?1 AND generation = ?2",
        params![row.id.as_str(), row.generation, binding, now],
    )?;
    revoke_participant_credentials(tx, now, row.id.as_str())?;
    participant_event(
        tx,
        now,
        EventKind::ParticipantStateChanged,
        &row.id,
        actor,
        json!({}),
        events,
    )
}

pub(super) struct StopUpdate<'a> {
    pub(super) now: i64,
    pub(super) clean: bool,
    pub(super) boot_id: Option<String>,
    pub(super) reason: &'a str,
    pub(super) actor: Option<&'a str>,
}

pub(super) fn finish_stop(
    tx: &Transaction<'_>,
    row: &mut ParticipantRow,
    update: StopUpdate<'_>,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    row.binding.core.pending_approval = None;
    abandon_inflight_approval(tx, row, update.now)?;
    row.binding.core.state_reason = Some(update.reason.to_owned());
    let kind = if update.clean {
        row.state = ParticipantState::Stopped;
        row.identity = None;
        row.binding.core.lifecycle_block = None;
        EventKind::ParticipantStopped
    } else {
        row.state = ParticipantState::Unknown;
        row.binding.core.lifecycle_block = Some(LifecycleBlock {
            kind: LifecycleBlockKind::CleanupUnverified,
            boot_id: update.boot_id,
        });
        EventKind::ParticipantStateChanged
    };
    let binding = serde_json::to_string(&row.binding).map_err(bad_data)?;
    tx.execute(
        "UPDATE participants SET state = ?3, adapter_binding_json = ?4, updated_at_ms = ?5,
         stopped_at_ms = ?6, runtime_pid = ?7, runtime_pgid = ?8, runtime_start_token = ?9,
         process_ownership = ?10
         WHERE participant_id = ?1 AND generation = ?2",
        params![
            row.id.as_str(),
            row.generation,
            row.state.as_str(),
            binding,
            update.now,
            update.clean.then_some(update.now),
            row.identity.as_ref().map(|id| id.pid),
            row.identity.as_ref().map(|id| id.pgid),
            row.identity.as_ref().map(|id| id.start_token.as_str()),
            row.ownership.as_str(),
        ],
    )?;
    participant_event(
        tx,
        update.now,
        kind,
        &row.id,
        update.actor,
        json!({"reason": update.reason}),
        events,
    )
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll, Waker};

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn ready_interrupt_ack_wins_over_disconnect_and_closes_admission()
    -> std::result::Result<(), &'static str> {
        let (shutdown, disconnected) = watch::channel(());
        let (ack, result) = oneshot::channel();
        let admission = AdmissionGuard::new(STOP_REQUEST_TIMEOUT);
        let call = await_interrupt(disconnected, &admission, async {
            result.await.map_err(crate::error::NodeError::internal)
        });
        tokio::pin!(call);
        assert!(matches!(
            call.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        ack.send(StopOutcome::TurnCancelled)
            .map_err(|_| "closed interrupt receiver")?;
        shutdown.send_replace(());
        assert!(matches!(call.await, Some(StopOutcome::TurnCancelled)));
        assert!(admission.is_expired());
        Ok(())
    }

    #[tokio::test]
    async fn interrupt_aborted_before_first_poll_does_not_call_adapter() {
        let (shutdown, disconnected) = watch::channel(());
        let admission = AdmissionGuard::new(STOP_REQUEST_TIMEOUT);
        let called = AtomicBool::new(false);
        shutdown.send_replace(());
        let outcome = await_interrupt(disconnected, &admission, async {
            called.store(true, Ordering::Relaxed);
            Ok(StopOutcome::TurnCancelled)
        })
        .await;
        assert!(matches!(outcome, Some(StopOutcome::AlreadyGone)));
        assert!(admission.is_expired());
        assert!(!called.load(Ordering::Relaxed));
    }
}
