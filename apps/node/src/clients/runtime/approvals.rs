use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{ErrorCode, OperationId, ParticipantId, ParticipantState, canonical_sha256};
use cooplet_storage::{WriteError, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    AgentService, LiveSession, bad_data, busy, invalid,
    lifecycle::decode,
    not_found,
    participants::{ParticipantRow, load_participant},
    unknown,
};
use crate::{
    clients::{
        ApprovalAction, ApprovalDecision, ApprovalRespondOutcome, ApprovalResponseRecord,
        ApprovalResponseState,
    },
    error::{NodeError, Result},
    now_ms,
    operations::{
        DelegatedOperation, Operation, OperationGuard, abandon_pending, admission_expired,
    },
};

const APPROVAL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ApprovalResponse {
    pub operation_id: OperationId,
    pub participant_id: ParticipantId,
    pub approval_id: String,
    pub action: ApprovalAction,
}

enum ApprovalReservation {
    Replay(serde_json::Value),
    Rejected(NodeError),
    Call {
        live: Arc<LiveSession>,
        native_id: String,
    },
}

impl AgentService {
    pub async fn respond_approval(
        &self,
        actor: &str,
        key: &str,
        id: ParticipantId,
        decision: ApprovalDecision,
    ) -> Result<ApprovalResponse> {
        validate_decision(&decision)?;
        let guard = self
            .0
            .operations
            .guard(actor, key, "approval.respond", &(&id, &decision))?;
        self.respond_approval_guarded(guard, id, decision, None)
            .await
    }

    pub(crate) async fn respond_approval_delegated(
        &self,
        delegated: &DelegatedOperation,
        id: ParticipantId,
        decision: ApprovalDecision,
    ) -> Result<ApprovalResponse> {
        validate_decision(&decision)?;
        let guard =
            self.0
                .operations
                .guard_delegated(delegated, "approval.respond", &(&id, &decision))?;
        self.respond_approval_guarded(guard, id, decision, Some(&delegated.admission))
            .await
    }

    async fn respond_approval_guarded(
        &self,
        guard: OperationGuard,
        id: ParticipantId,
        decision: ApprovalDecision,
        admission: Option<&AdmissionGuard>,
    ) -> Result<ApprovalResponse> {
        let op = &guard.operation;
        let participant_id = id.clone();
        if let Some(value) = self
            .0
            .operations
            .lookup_on_abandon(op, move |tx, abandoned_id, now| {
                tx.execute(
                    "UPDATE participants
                     SET adapter_binding_json = json_set(
                             adapter_binding_json, '$.core.approval_response.state', 'unknown'
                         ),
                         updated_at_ms = ?2
                     WHERE participant_id = ?1
                       AND json_extract(adapter_binding_json, '$.core.approval_response.operation_id') = ?3
                       AND json_extract(adapter_binding_json, '$.core.approval_response.state') = 'in_flight'",
                    params![participant_id.as_str(), now, abandoned_id],
                )?;
                Ok(())
            })
            .await
            .map_err(|error| {
                if error.code() == ErrorCode::DeliveryUnknown {
                    error.with_extension("approval_id", decision.approval_id.as_str())
                } else {
                    error
                }
            })?
        {
            return decode(value);
        }
        let _mutation = self.mutation(&id)?;
        let reserved = self
            .reserve_approval(op, &id, &decision.approval_id, admission.cloned())
            .await?;
        let (live, native_id) = match reserved {
            ApprovalReservation::Call { live, native_id } => (live, native_id),
            ApprovalReservation::Replay(value) => return decode(value),
            ApprovalReservation::Rejected(error) => return Err(error),
        };
        guard.mark_committed();
        let action = decision.action;
        let outcome = send_approval(
            &live,
            ApprovalDecision {
                approval_id: native_id,
                action,
            },
            admission,
        )
        .await;
        let response = ApprovalResponse {
            operation_id: op.id.clone(),
            participant_id: id,
            approval_id: decision.approval_id,
            action,
        };
        let (state, error) = match outcome {
            None => (None, Some(admission_expired())),
            Some(ApprovalRespondOutcome::Accepted) => {
                (Some(ApprovalResponseState::Responded), None)
            }
            Some(ApprovalRespondOutcome::Unsupported(_)) => (
                None,
                Some(invalid(
                    "approval_unsupported",
                    "The adapter cannot respond to this approval",
                )),
            ),
            Some(ApprovalRespondOutcome::Stale(_)) => (
                None,
                Some(invalid(
                    "approval_stale",
                    "The adapter no longer awaits this approval",
                )),
            ),
            Some(ApprovalRespondOutcome::Unavailable(_)) => {
                (None, Some(busy("approval_unavailable", true)))
            }
            Some(ApprovalRespondOutcome::Unknown(_)) => (
                Some(ApprovalResponseState::Unknown),
                Some(unknown("approval_outcome_unconfirmed")),
            ),
        };
        let error = error.map(|error| {
            error
                .with_extension("approval_id", response.approval_id.clone())
                .with_extension("operation_id", response.operation_id.as_str())
        });
        self.finish_approval(op, live.generation(), response, state, error)
            .await
    }

    async fn reserve_approval(
        &self,
        op: &Operation,
        id: &ParticipantId,
        approval_id: &str,
        admission: Option<AdmissionGuard>,
    ) -> Result<ApprovalReservation> {
        let live = self.live_session(id);
        let id = id.clone();
        let approval_id = approval_id.to_owned();
        let now = now_ms()?;
        let op = op.clone();
        self.0
            .db
            .write(move |tx| -> std::result::Result<_, WriteError<NodeError>> {
                if let Some(value) = op.lookup(tx)? {
                    return Ok(WriteOutcome::new(ApprovalReservation::Replay(value)));
                }
                let mut row = load_participant(tx, &id)?.ok_or_else(not_found)?;
                // The participant lease excludes any live approval response.
                if let Some(previous_id) = row
                    .binding
                    .core
                    .approval_response
                    .as_ref()
                    .filter(|prior| prior.state == ApprovalResponseState::InFlight)
                    .map(|prior| prior.operation_id.clone())
                {
                    let recovered = abandon_inflight_approval(tx, &mut row, now)
                        .and_then(|_| update_binding(tx, &row, now));
                    if let Err(error) = recovered {
                        if let Err(unconfirmed) = check_history(&row, &approval_id) {
                            tracing::warn!(operation_id = %previous_id, %error, "Could not finalize an unconfirmed approval");
                            return Err(unconfirmed.into());
                        }
                        return Err(error.into());
                    }
                }
                if let Err(error) = check_history(&row, &approval_id) {
                    return Ok(WriteOutcome::new(ApprovalReservation::Rejected(error)));
                }
                let pending = row
                    .binding
                    .core
                    .pending_approval
                    .as_ref()
                    .filter(|pending| {
                        row.state == ParticipantState::AwaitingApproval
                            && pending.public_id == approval_id
                    });
                let Some(pending) = pending else {
                    let rejection = stale_approval(tx, &row.id, &approval_id)?;
                    return Ok(WriteOutcome::new(ApprovalReservation::Rejected(rejection)));
                };
                if !row.capabilities.supports_remote_approval || !pending.can_respond_remotely {
                    return Err(invalid(
                        "approval_unsupported",
                        "This approval needs a local response",
                    )
                    .into());
                }
                let native_id = pending.native_id.clone().ok_or_else(|| {
                    invalid(
                        "approval_unsupported",
                        "This approval needs a local response",
                    )
                })?;
                let live = live
                    .filter(|live| {
                        live.generation() == row.generation
                            && live.state() != ParticipantState::Unknown
                    })
                    .ok_or_else(|| busy("no_live_session", true))?;
                if admission.as_ref().is_some_and(AdmissionGuard::is_expired) {
                    return Err(admission_expired().into());
                }
                row.binding.core.approval_response = Some(ApprovalResponseRecord {
                    operation_id: op.id.clone(),
                    public_id: approval_id,
                    state: ApprovalResponseState::InFlight,
                });
                update_binding(tx, &row, now)?;
                op.reserve(tx, now, Some(id.as_str()))?;
                Ok(WriteOutcome::new(ApprovalReservation::Call {
                    live,
                    native_id,
                }))
            })
            .await
            .map_err(Into::into)
    }

    async fn finish_approval(
        &self,
        operation: &Operation,
        generation: u64,
        response: ApprovalResponse,
        state: Option<ApprovalResponseState>,
        error: Option<NodeError>,
    ) -> Result<ApprovalResponse> {
        let now = now_ms()?;
        let op = operation.clone();
        let saved = response.clone();
        let error = error.map(Arc::new);
        let saved_error = error.clone();
        let write = self
            .0
            .operations
            .finalize(move |tx| -> std::result::Result<_, WriteError<NodeError>> {
                if let Some(mut row) = load_participant(tx, &saved.participant_id)?
                    && row.generation == generation
                    && let Some(mut prior) = row.binding.core.approval_response.take()
                    && prior.operation_id == op.id
                    && prior.public_id == saved.approval_id
                {
                    row.binding.core.approval_response = state.map(|state| {
                        prior.state = state;
                        prior
                    });
                    update_binding(tx, &row, now)?;
                }
                match (state, saved_error.as_deref()) {
                    (Some(ApprovalResponseState::Unknown), error) => op.unknown(tx, now, error)?,
                    (_, Some(error)) => op.fail(tx, now, error)?,
                    (_, None) => op.succeed(tx, now, &saved)?,
                }
                Ok(WriteOutcome::new(()))
            })
            .await;
        if let Err(write_error) = write {
            // A lost SQLite reply can still have left a committed receipt.
            return self
                .confirm_operation(operation, Err(write_error), "approval_result_unconfirmed")
                .await
                .map_err(|error| {
                    if error.code() == ErrorCode::DeliveryUnknown
                        && !error.0.extensions.contains_key("approval_id")
                    {
                        error.with_extension("approval_id", response.approval_id)
                    } else {
                        error
                    }
                });
        }
        match error {
            None => Ok(response),
            Some(error) => {
                Err(Arc::try_unwrap(error)
                    .unwrap_or_else(|_| unknown("approval_outcome_unconfirmed")))
            }
        }
    }
}

fn validate_decision(decision: &ApprovalDecision) -> Result<()> {
    if decision.approval_id.len() > 32 || decision.validate().is_err() {
        return Err(invalid(
            "invalid_approval_id",
            "The public approval ID is invalid",
        ));
    }
    Ok(())
}

pub(super) fn abandon_inflight_approval(
    tx: &Transaction<'_>,
    row: &mut ParticipantRow,
    now: i64,
) -> rusqlite::Result<bool> {
    let Some(prior) = row.binding.core.approval_response.as_mut() else {
        return Ok(false);
    };
    if prior.state != ApprovalResponseState::InFlight {
        return Ok(false);
    }
    prior.state = ApprovalResponseState::Unknown;
    abandon_pending(tx, prior.operation_id.as_str(), now)?;
    Ok(true)
}

async fn send_approval(
    live: &LiveSession,
    decision: ApprovalDecision,
    admission: Option<&AdmissionGuard>,
) -> Option<ApprovalRespondOutcome> {
    let mut disconnected = live.event_stop();
    if live.state() == ParticipantState::Unknown {
        return Some(ApprovalRespondOutcome::Unavailable(
            "session closed before approval response".into(),
        ));
    }
    let started = AtomicBool::new(false);
    let acknowledged = async {
        if admission.is_some_and(AdmissionGuard::is_expired) {
            return None;
        }
        started.store(true, Ordering::Relaxed);
        Some(
            tokio::time::timeout(
                APPROVAL_TIMEOUT,
                live.adapter.respond_approval(&live.handle, decision),
            )
            .await
            .unwrap_or_else(|_| {
                ApprovalRespondOutcome::Unknown("approval acknowledgment timed out".into())
            }),
        )
    };
    tokio::select! {
        biased;
        _ = disconnected.changed() => {
            if started.load(Ordering::Relaxed) {
                Some(ApprovalRespondOutcome::Unknown("session closed during approval response".into()))
            } else {
                Some(ApprovalRespondOutcome::Unavailable("session closed before approval response".into()))
            }
        }
        result = acknowledged => result,
    }
}

// The binding tracks the most recent native response. Older uncertain outcomes
// live in the original receipts, including receipts recovered after a crash.
fn stale_approval(
    tx: &Transaction<'_>,
    id: &ParticipantId,
    approval_id: &str,
) -> Result<NodeError> {
    let fingerprint = |action| {
        canonical_sha256(&(
            id,
            ApprovalDecision {
                approval_id: approval_id.to_owned(),
                action,
            },
        ))
        .map_err(NodeError::internal)
    };
    let previous: Option<String> = tx.query_row(
        "SELECT operation_id FROM operations WHERE kind = 'approval.respond' AND state = 'unknown'
         AND request_sha256 IN (?1, ?2) LIMIT 1",
        params![fingerprint(ApprovalAction::Approve)?, fingerprint(ApprovalAction::Deny)?],
        |row| row.get(0),
    ).optional()?;
    Ok(match previous {
        Some(operation_id) => unknown("approval_outcome_unconfirmed")
            .with_extension("operation_id", operation_id)
            .with_extension("approval_id", approval_id),
        None => invalid(
            "approval_stale",
            "The participant no longer awaits this approval",
        ),
    })
}

fn check_history(row: &ParticipantRow, approval_id: &str) -> Result<()> {
    let Some(prior) = &row.binding.core.approval_response else {
        return Ok(());
    };
    match prior.state {
        ApprovalResponseState::InFlight => Err(busy("approval_in_flight", true)),
        ApprovalResponseState::Unknown if prior.public_id == approval_id => {
            Err(unknown("approval_outcome_unconfirmed")
                .with_extension("operation_id", prior.operation_id.as_str())
                .with_extension("approval_id", approval_id))
        }
        ApprovalResponseState::Responded if prior.public_id == approval_id => Err(invalid(
            "approval_already_responded",
            "This approval was already answered",
        )),
        _ => Ok(()),
    }
}

fn update_binding(tx: &Transaction<'_>, row: &ParticipantRow, now: i64) -> rusqlite::Result<()> {
    let binding = serde_json::to_string(&row.binding).map_err(bad_data)?;
    tx.execute(
        "UPDATE participants SET adapter_binding_json = ?3, updated_at_ms = ?4
         WHERE participant_id = ?1 AND generation = ?2",
        params![row.id.as_str(), row.generation, binding, now],
    )?;
    Ok(())
}
