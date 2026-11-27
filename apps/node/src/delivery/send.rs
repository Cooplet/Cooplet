use std::{path::Path, sync::Arc, time::Duration};

use cooplet_client::CoopletClient;
use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{
    AttachmentRef, DeliveryMode, DeliveryState, ErrorCode, MessageId, ParticipantId,
    ParticipantKind, ParticipantState,
};
use cooplet_storage::{EventNotice, StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::json;

use super::{
    Clock, DeliveryService, MessageView, SendMessageRequest, SendMessageResponse, SubmitFence,
    envelope::{VerifiedAttachments, encode_queue_envelope},
    model::invalid,
    queries::{
        HubProjection, Recipient, check_message_frame_fields, committed_pending_answer, recipient,
        sql_error, store_message, update_delivery,
    },
    questions::{answer_denial, answered_event, validate_reply},
    queue::{ATTACHMENT_UNAVAILABLE, enqueue, expire_for_recipient, queue_has_room},
    relay::{HubAttachment, cache_changed, refresh_hub_attachments},
};
use crate::{
    auth::Principal,
    clients::{
        InboundMessage, PendingApprovalRecord, SlotDenial, SubmitOutcome, SubmitSlot,
        SubmitTurnRequest, TurnInput, VerifiedAuthor, apply_submit_outcome,
    },
    error::{NodeError, Result},
    operations::{DelegatedOperation, Operation, OperationGuard, ReceiptLookup, admission_expired},
    storage::{reconcile_pending_message, retry_recovery_write},
};

const DIRECT_ADMISSION: Duration = Duration::from_secs(5);

pub(crate) struct InboundHubMessage {
    pub message_id: MessageId,
    pub sender_id: String,
    pub author: VerifiedAuthor,
    pub input: SendMessageRequest,
}

impl DeliveryService {
    pub async fn send(
        self: &Arc<Self>,
        caller: Principal,
        key: String,
        input: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let service = self.clone();
        tokio::spawn(async move { service.send_in_place(caller, key, input).await })
            .await
            .map_err(NodeError::internal)?
    }

    /// Run `send` directly on the caller's already-detached task (for example
    /// inside `http::Tasks::run`) without adding a second `tokio::spawn`.
    pub(crate) async fn send_in_place(
        self: &Arc<Self>,
        caller: Principal,
        key: String,
        mut input: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        if !caller.can_collaborate() {
            return Err(NodeError::new(
                ErrorCode::Forbidden,
                "Collaborate scope is required",
            ));
        }
        self.ensure_accepting()?;
        input.normalize()?;
        let _admission = self.activity.read().await;
        self.ensure_accepting()?;
        let can_manage = caller.can_manage();
        self.send_inner(caller, &key, input)
            .await
            .map_err(|error| redact_approval(error, can_manage))
    }

    async fn send_inner(
        self: &Arc<Self>,
        caller: Principal,
        key: &str,
        input: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let guard = self
            .operations
            .guard(caller.actor_id(), key, "message.send", &input)?;
        if let Some(receipt) = self
            .existing_receipt(&guard.operation, input.answer.is_some())
            .await?
        {
            return Ok(receipt);
        }
        let attempt = Attempt {
            message_id: MessageId::generate()?,
            operation: guard.operation.clone(),
            can_read_all: caller.can_read_all(),
            sender_id: caller.actor_id().to_owned(),
            author: VerifiedAuthor {
                kind: caller.author_kind(),
                actor_id: caller.actor_id().to_owned(),
                name: caller.identity.name,
                source_node_id: None,
            },
            hub: None,
            input,
        };
        self.send_new(&guard, attempt, None).await
    }

    pub(crate) async fn send_delegated(
        self: &Arc<Self>,
        delegated: &DelegatedOperation,
        hub: &CoopletClient,
        max_blob_bytes: u64,
        mut message: InboundHubMessage,
    ) -> Result<SendMessageResponse> {
        self.ensure_accepting()?;
        let _activity = self.activity.read().await;
        self.ensure_accepting()?;
        message.input.normalize()?;
        let guard = self.operations.guard_delegated(
            delegated,
            "message.send",
            &(&message.message_id, &message.input),
        )?;
        if let Some(receipt) = self
            .existing_receipt(&guard.operation, message.input.answer.is_some())
            .await?
        {
            return Ok(receipt);
        }
        let message_id = MessageId::generate()?;
        let build_attempt = |attachments| Attempt {
            message_id: message_id.clone(),
            operation: guard.operation.clone(),
            can_read_all: false,
            sender_id: message.sender_id.clone(),
            author: message.author.clone(),
            hub: Some(InboundProjection {
                message_id: message.message_id.clone(),
                attachments,
            }),
            input: message.input.clone(),
        };
        let materialize = || {
            self.load_hub_attachments(
                hub,
                &message.input.attachments,
                &delegated.admission,
                max_blob_bytes,
            )
        };
        let attempt = build_attempt(materialize().await?);
        match self
            .send_new(&guard, attempt, Some(&delegated.admission))
            .await
        {
            Err(error) if cache_changed(&error) => {
                let attempt = build_attempt(materialize().await?);
                self.send_new(&guard, attempt, Some(&delegated.admission))
                    .await
            }
            result => result,
        }
    }

    async fn existing_receipt(
        &self,
        operation: &Operation,
        is_answer: bool,
    ) -> Result<Option<SendMessageResponse>> {
        match self.operations.lookup_state(operation).await? {
            ReceiptLookup::Missing => Ok(None),
            ReceiptLookup::Succeeded(receipt) => SendMessageResponse::decode(receipt).map(Some),
            ReceiptLookup::Failed(stored) => Err(stored),
            ReceiptLookup::Unknown {
                id,
                problem,
                resource_id,
            } => Err(unknown_receipt(id, problem, resource_id)),
            ReceiptLookup::Pending { id, resource_id } => self
                .reconcile_pending(operation, &id, resource_id.as_deref(), is_answer)
                .await
                .map(Some),
        }
    }

    async fn send_new(
        self: &Arc<Self>,
        guard: &OperationGuard,
        attempt: Attempt,
        external_admission: Option<&AdmissionGuard>,
    ) -> Result<SendMessageResponse> {
        let operation = guard.operation.clone();
        // Validate reply authority before looking at the local recipient or taking its slot.
        let (attempt, target) = self
            .reads
            .read(move |conn| {
                let result = if attempt.hub.is_none() {
                    validate_reply(
                        conn,
                        &attempt.input,
                        &attempt.sender_id,
                        attempt.can_read_all,
                    )
                    .and_then(|_| recipient(conn, &attempt.input.to))
                } else {
                    recipient(conn, &attempt.input.to)
                };
                Ok((attempt, result))
            })
            .await?;
        let target = target?;
        let id = target.id.clone();
        let live = self.agents.live_session(&id).filter(|session| {
            matches!(
                session.state(),
                ParticipantState::Idle
                    | ParticipantState::Working
                    | ParticipantState::AwaitingApproval,
            )
        });
        let live_generation = live.as_ref().map(|session| session.generation());
        let mut slot = None;
        let mut direct_denial = SlotDenial::Unavailable;
        let admission = external_admission
            .cloned()
            .unwrap_or_else(|| AdmissionGuard::new(DIRECT_ADMISSION));
        if attempt.input.delivery_mode == DeliveryMode::DirectNow
            && target.kind == ParticipantKind::Agent
            && let Some(session) = &live
        {
            match session.try_submit(attempt.message_id.as_str()) {
                Ok(reserved) => slot = Some(reserved),
                Err(denied) => direct_denial = denied,
            }
        }
        let live = AttemptAdmission {
            live_generation,
            preflight: if slot.is_some() {
                DirectGate::Ready
            } else {
                DirectGate::Denied(direct_denial)
            },
        };

        let is_answer = attempt.input.answer.is_some();
        let blob_root = self.blob_root.clone();
        let clock = self.clock.clone();
        let write_admission = external_admission.cloned();
        let prepared = self
            .db
            .write(move |tx| {
                attempt.prepare(tx, &blob_root, &clock, live, write_admission.as_ref())
            })
            .await;
        match prepared {
            Ok(Prepared::Direct(work)) => {
                guard.mark_committed();
                let slot = slot
                    .ok_or_else(|| NodeError::internal("Direct message lacks an admission slot"))?;
                self.submit_direct(id, operation, *work, slot, admission)
                    .await
            }
            prepared => {
                let released_slot = slot.is_some();
                drop(slot);
                match prepared {
                    Ok(Prepared::Done(result)) => {
                        if released_slot
                            || result.as_ref().is_ok_and(|receipt| {
                                receipt.delivery_state == DeliveryState::Queued
                            })
                        {
                            self.wake(id);
                        }
                        result
                    }
                    Err(error) => {
                        if released_slot {
                            self.wake(id.clone());
                        }
                        if matches!(
                            error,
                            WriteFailure::Storage(
                                StorageError::CommitFailed(_) | StorageError::WriteOutcomeUnknown
                            )
                        ) {
                            match self.operations.lookup_state(&operation).await {
                                Ok(ReceiptLookup::Succeeded(value)) => {
                                    let receipt = SendMessageResponse::decode(value)?;
                                    if receipt.delivery_state == DeliveryState::Queued {
                                        self.wake(id);
                                    }
                                    return Ok(receipt);
                                }
                                Ok(ReceiptLookup::Failed(stored)) => return Err(stored),
                                Ok(ReceiptLookup::Pending {
                                    id: pending_id,
                                    resource_id,
                                }) => {
                                    // Tx 1 committed a direct send before the reply was lost;
                                    // the slot was already dropped, so reconcile it honestly.
                                    return self
                                        .reconcile_pending(
                                            &operation,
                                            &pending_id,
                                            resource_id.as_deref(),
                                            is_answer,
                                        )
                                        .await;
                                }
                                Ok(ReceiptLookup::Unknown {
                                    id: op_id,
                                    problem,
                                    resource_id,
                                }) => return Err(unknown_receipt(op_id, problem, resource_id)),
                                Err(read_error)
                                    if read_error.code() == ErrorCode::OperationConflict =>
                                {
                                    return Err(read_error);
                                }
                                Err(read_error) => {
                                    tracing::warn!(
                                        operation_id = %operation.id,
                                        %read_error,
                                        "Could not inspect delivery receipt after uncertain commit"
                                    );
                                    if released_slot {
                                        let retry_id = operation.id.clone();
                                        tokio::spawn(retry_recovery_write(
                                            self.db.clone(),
                                            self.closing.subscribe(),
                                            move |tx, now| {
                                                reconcile_pending_message(
                                                    tx,
                                                    now,
                                                    retry_id.as_str(),
                                                )
                                            },
                                        ));
                                    } else {
                                        self.wake(id);
                                    }
                                }
                                Ok(ReceiptLookup::Missing) => {}
                            }
                        }
                        Err(error.into())
                    }
                    Ok(Prepared::Direct(_)) => {
                        Err(NodeError::internal("Unexpected direct message phase"))
                    }
                }
            }
        }
    }

    pub(super) async fn reconcile_pending(
        &self,
        operation: &Operation,
        pending_id: &str,
        resource_id: Option<&str>,
        is_answer: bool,
    ) -> Result<SendMessageResponse> {
        let clock = self.clock.clone();
        let op = operation.clone();
        let finished = self
            .operations
            .finalize(move |tx| {
                replay_in_writer(tx, &op, clock.as_ref(), true)?.ok_or_else(|| {
                    NodeError::internal("Predecessor receipt vanished during reconciliation").into()
                })
            })
            .await;
        match finished {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(operation_id = %pending_id, %error, "Could not reconcile the delivery receipt");
                match self.operations.lookup_state(operation).await {
                    Ok(ReceiptLookup::Succeeded(value)) => {
                        return SendMessageResponse::decode(value);
                    }
                    Ok(ReceiptLookup::Failed(stored)) => return Err(stored),
                    Ok(ReceiptLookup::Unknown {
                        id,
                        problem,
                        resource_id,
                    }) => return Err(unknown_receipt(id, problem, resource_id)),
                    Err(read_error) if read_error.code() == ErrorCode::OperationConflict => {
                        return Err(read_error);
                    }
                    Err(read_error) => {
                        tracing::warn!(
                            operation_id = %pending_id,
                            %read_error,
                            "Could not inspect delivery receipt after failed reconciliation"
                        );
                    }
                    Ok(ReceiptLookup::Missing | ReceiptLookup::Pending { .. }) => {}
                }
                let retry_id = pending_id.to_owned();
                tokio::spawn(retry_recovery_write(
                    self.db.clone(),
                    self.closing.subscribe(),
                    move |tx, now| reconcile_pending_message(tx, now, &retry_id),
                ));
                let stored_id = pending_id.to_owned();
                if is_answer
                    && let Ok(Some(answer)) = self
                        .reads
                        .read(move |conn| Ok(committed_pending_answer(conn, &stored_id)?))
                        .await
                {
                    return Ok(answer);
                }
                Err(unconfirmed_problem(pending_id, resource_id))
            }
        }
    }

    async fn submit_direct(
        self: &Arc<Self>,
        participant: ParticipantId,
        operation: Operation,
        work: DirectWork,
        mut slot: SubmitSlot,
        admission: AdmissionGuard,
    ) -> Result<SendMessageResponse> {
        let DirectWork {
            mut receipt,
            message_id,
            input,
            attachments,
            answer,
            prepared_at,
        } = work;
        let (outcome, detail) = if self.ensure_accepting().is_err() {
            (
                SubmitOutcome::Unavailable("Node is shutting down".into()),
                Some("node_stopping"),
            )
        } else if admission.is_expired() {
            (SubmitOutcome::Expired, Some("admission_expired"))
        } else {
            let attachment_check = match (self.clock)() {
                Ok(now) => self.attachments_available(&attachments, now).await,
                Err(error) => Err(error.into()),
            };
            match attachment_check {
                Ok(true) if self.ensure_accepting().is_err() => (
                    SubmitOutcome::Unavailable("Node is shutting down".into()),
                    Some("node_stopping"),
                ),
                Ok(true) if admission.is_expired() => {
                    (SubmitOutcome::Expired, Some("admission_expired"))
                }
                Ok(true) => {
                    let outcome = slot
                        .submit(SubmitTurnRequest {
                            operation_key: message_id.to_string(),
                            message_id: Some(message_id.clone()),
                            input,
                            output_schema: None,
                            steer: false,
                            admission,
                        })
                        .await;
                    let detail = outcome_detail(&outcome);
                    (outcome, detail)
                }
                Ok(false) => (
                    SubmitOutcome::Unavailable("Attachment is unavailable".into()),
                    Some(ATTACHMENT_UNAVAILABLE),
                ),
                Err(error) => {
                    tracing::warn!(
                        operation_id = %operation.id,
                        message_id = %message_id,
                        %error,
                        "Could not verify attachment availability before direct submit"
                    );
                    (
                        SubmitOutcome::Unavailable("Attachment availability check failed".into()),
                        Some("attachment_check_failed"),
                    )
                }
            }
        };
        let fence = SubmitFence {
            generation: slot.generation(),
            after_seq: slot.submitted_after_seq(),
        };
        receipt.delivery_state = submit_state(&outcome);
        let now = (self.clock)().unwrap_or(prepared_at);
        let finished = self
            .finish_direct(
                &participant,
                fence,
                &operation,
                &receipt,
                &outcome,
                detail,
                answer,
                now,
            )
            .await;
        match finished {
            Ok(result) => {
                slot.finish(&outcome, result.approval_current).await;
                if matches!(
                    outcome,
                    SubmitOutcome::Expired | SubmitOutcome::Unavailable(_)
                ) {
                    self.wake(participant);
                }
                result.response
            }
            Err(error) => {
                tracing::warn!(operation_id = %operation.id, %error, "Could not finalize direct message delivery");
                match self.operations.lookup(&operation).await {
                    Ok(Some(value)) => {
                        slot.finish(&outcome, false).await;
                        SendMessageResponse::decode(value)
                    }
                    Err(error)
                        if matches!(
                            error.code(),
                            ErrorCode::AwaitingApproval
                                | ErrorCode::ParticipantBusy
                                | ErrorCode::DeliveryUnknown,
                        ) =>
                    {
                        slot.finish(&outcome, false).await;
                        Err(error)
                    }
                    _ => {
                        let failure = SubmitOutcome::Unknown(
                            "The delivery receipt could not be confirmed".into(),
                        );
                        let mut uncertain = receipt;
                        uncertain.delivery_state = DeliveryState::Unknown;
                        let fallback = self
                            .finish_direct(
                                &participant,
                                fence,
                                &operation,
                                &uncertain,
                                &failure,
                                Some("receipt_unconfirmed"),
                                answer,
                                now,
                            )
                            .await;
                        slot.finish(&failure, false).await;
                        if let Ok(result) = fallback {
                            return result.response;
                        }
                        // A replay may finalize the receipt first. The original
                        // generation still needs its durable intake fence.
                        let id = operation.id.clone();
                        tokio::spawn(retry_recovery_write(
                            self.db.clone(),
                            self.closing.subscribe(),
                            move |tx, now| {
                                let mut events = reconcile_pending_message(tx, now, id.as_str())?;
                                apply_submit_outcome(
                                    tx,
                                    now,
                                    &participant,
                                    fence.generation,
                                    fence.after_seq,
                                    &failure,
                                    &mut events,
                                )?;
                                Ok(events)
                            },
                        ));
                        if answer {
                            Ok(uncertain)
                        } else {
                            Err(delivery_unknown(&operation, &message_id))
                        }
                    }
                }
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "The operation and session fence travel with the final result"
    )]
    async fn finish_direct(
        &self,
        participant: &ParticipantId,
        fence: SubmitFence,
        operation: &Operation,
        receipt: &SendMessageResponse,
        outcome: &SubmitOutcome,
        detail: Option<&'static str>,
        answer: bool,
        now: i64,
    ) -> Result<FinishedDirect> {
        let participant = participant.clone();
        let operation = operation.clone();
        let receipt = receipt.clone();
        let outcome = outcome.clone();
        let id = receipt
            .message_id
            .clone()
            .ok_or_else(|| NodeError::internal("Direct result lacks a message ID"))?;
        self.operations
            .finalize(move |tx| {
                let mut events = Vec::new();
                let approval = apply_submit_outcome(
                    tx,
                    now,
                    &participant,
                    fence.generation,
                    fence.after_seq,
                    &outcome,
                    &mut events,
                )?;
                let approval_current = approval.is_some();
                events.push(update_delivery(
                    tx,
                    now,
                    id.as_str(),
                    receipt.delivery_state,
                    detail,
                )?);
                if !answer {
                    let failure = match receipt.delivery_state {
                        DeliveryState::Busy => Some(participant_busy()),
                        DeliveryState::AwaitingApproval => {
                            Some(awaiting_approval(approval.as_ref()))
                        }
                        DeliveryState::Unknown => Some(delivery_unknown(&operation, &id)),
                        _ => None,
                    };
                    if let Some(failure) = failure {
                        let failure = failure
                            .with_extension("operation_id", json!(&operation.id))
                            .with_extension("message_id", json!(&id));
                        if receipt.delivery_state == DeliveryState::Unknown {
                            operation.unknown(tx, now, Some(&failure))?;
                        } else {
                            operation.fail(tx, now, &failure)?;
                        }
                        return Ok(WriteOutcome::with_events(
                            FinishedDirect {
                                response: Err(failure),
                                approval_current,
                            },
                            events,
                        ));
                    }
                }
                operation.succeed(tx, now, &receipt)?;
                Ok(WriteOutcome::with_events(
                    FinishedDirect {
                        response: Ok(receipt.clone()),
                        approval_current,
                    },
                    events,
                ))
            })
            .await
    }
}

struct FinishedDirect {
    response: Result<SendMessageResponse>,
    approval_current: bool,
}

enum Prepared {
    Done(Result<SendMessageResponse>),
    Direct(Box<DirectWork>),
}

struct DirectWork {
    receipt: SendMessageResponse,
    message_id: MessageId,
    input: TurnInput,
    attachments: Vec<AttachmentRef>,
    answer: bool,
    prepared_at: i64,
}

struct Attempt {
    message_id: MessageId,
    operation: Operation,
    sender_id: String,
    author: VerifiedAuthor,
    can_read_all: bool,
    input: SendMessageRequest,
    hub: Option<InboundProjection>,
}

struct InboundProjection {
    message_id: MessageId,
    attachments: Vec<HubAttachment>,
}

struct AttemptAdmission {
    live_generation: Option<u64>,
    preflight: DirectGate,
}

enum DirectGate {
    Ready,
    Denied(SlotDenial),
}

impl Attempt {
    fn prepare(
        self,
        tx: &Transaction<'_>,
        blob_root: &Path,
        clock: &Arc<Clock>,
        live: AttemptAdmission,
        admission: Option<&AdmissionGuard>,
    ) -> std::result::Result<WriteOutcome<Prepared>, WriteError<NodeError>> {
        if let Some(previous) = replay_in_writer(tx, &self.operation, clock.as_ref(), false)? {
            return Ok(WriteOutcome::with_events(
                Prepared::Done(previous.value),
                previous.events,
            ));
        }
        let now = clock().map_err(NodeError::from)?;
        let input = &self.input;
        let question = if self.hub.is_none() {
            validate_reply(tx, input, &self.sender_id, self.can_read_all)?
        } else {
            None
        };
        let recipient = recipient(tx, &input.to)?;
        if input
            .question
            .as_ref()
            .and_then(|question| question.expires_at_ms)
            .is_some_and(|expiry| expiry <= now)
        {
            return Err(invalid("The question expiry must be in the future").into());
        }
        let local_attachments = self
            .hub
            .is_none()
            .then(|| VerifiedAttachments::load(tx, &input.attachments, &self.sender_id, now))
            .transpose()?;
        if let Some(hub) = &self.hub {
            refresh_hub_attachments(tx, &hub.attachments, now)?;
        }
        if let (Some(question), Some(question_id)) = (&question, &input.in_reply_to)
            && let Some(error) = answer_denial(tx, question_id, question, now)?
        {
            self.check_frame(tx, now, DeliveryState::Unavailable)?;
            return self.remember_denial(tx, now, error).map_err(Into::into);
        }

        let mut events = Vec::new();
        if admission.is_some_and(AdmissionGuard::is_expired) {
            return Err(admission_expired().into());
        }
        let mut state = self.initial_state(tx, &recipient, &live, now, &mut events)?;
        if state == DeliveryState::Queued && !queue_has_room(tx, &recipient.id)? {
            if input.answer.is_some() {
                state = DeliveryState::Unavailable;
            } else {
                self.check_frame(tx, now, state)?;
                return Ok(WriteOutcome::with_events(
                    Prepared::Done(Err(NodeError::new(
                        ErrorCode::RateLimited,
                        "The participant intake queue is full",
                    )
                    .with_extension("reason", "queue_full"))),
                    events,
                ));
            }
        }
        if input.answer.is_none() {
            if matches!(
                state,
                DeliveryState::Busy | DeliveryState::AwaitingApproval | DeliveryState::Unavailable
            ) {
                self.check_frame(tx, now, state)?;
            }
            match state {
                DeliveryState::Busy => {
                    return self
                        .remember_denial(tx, now, participant_busy())
                        .map_err(Into::into);
                }
                DeliveryState::AwaitingApproval => {
                    return self
                        .remember_denial(tx, now, awaiting_approval(recipient.approval.as_ref()))
                        .map_err(Into::into);
                }
                DeliveryState::Unavailable => {
                    let receipt = self.receipt(None, state);
                    self.operation.reserve(tx, now, None)?;
                    self.operation.succeed(tx, now, &receipt)?;
                    return Ok(WriteOutcome::with_events(
                        Prepared::Done(Ok(receipt)),
                        events,
                    ));
                }
                _ => {}
            }
        }
        let local_parent = match &self.hub {
            Some(_) => hub_parent(tx, input.in_reply_to.as_ref(), &input.to, now)?,
            None => input.in_reply_to.clone(),
        };
        let mut receipt = self.receipt(Some(self.message_id.clone()), state);
        receipt.in_reply_to.clone_from(&local_parent);
        let ttl = if state == DeliveryState::Queued {
            Some(
                self.input
                    .queue_ttl_ms
                    .and_then(|ttl| i64::try_from(ttl).ok())
                    .ok_or_else(|| NodeError::internal("Queued message lacks a normalized TTL"))?,
            )
        } else {
            None
        };
        let operation = self.operation.clone();
        let hub_parent = self.input.in_reply_to.clone();
        let attachments = match &self.hub {
            Some(hub) => hub
                .attachments
                .iter()
                .map(|attachment| attachment.local.clone())
                .collect(),
            None => self.input.attachments,
        };
        let view = MessageView {
            message_id: self.message_id,
            sender_id: self.sender_id,
            recipient_id: self.input.to,
            in_reply_to: local_parent,
            body: self.input.body,
            attachments,
            question: self.input.question,
            answer: self.input.answer,
            delivery_mode: self.input.delivery_mode,
            delivery_state: state,
            delivery_detail: None,
            operation_id: Some(operation.id.clone()),
            created_at_ms: now,
            delivered_at_ms: (state == DeliveryState::Delivered).then_some(now),
        };
        operation.reserve(tx, now, Some(view.message_id.as_str()))?;
        let attachments_json = serde_json::to_string(&view.attachments).map_err(sql_error)?;
        let projection = self.hub.as_ref().map(|hub| HubProjection {
            message: &hub.message_id,
            parent: hub_parent.as_ref(),
        });
        events.push(store_message(tx, &view, &attachments_json, projection)?);
        if local_attachments.is_some() {
            VerifiedAttachments::attach(tx, &view.attachments, now)?;
        }
        if let (Some(id), Some(answer)) = (&view.in_reply_to, &view.answer) {
            events.push(answered_event(
                tx,
                now,
                id,
                &view.message_id,
                &view.sender_id,
                answer,
            )?);
        }
        let turn = || -> Result<TurnInput> {
            match (&self.hub, local_attachments) {
                (Some(hub), _) => Ok(TurnInput::direct(InboundMessage {
                    message_id: view.message_id.clone(),
                    author: self.author,
                    in_reply_to: view.in_reply_to.clone().or(hub_parent),
                    body: view.body.clone(),
                    attachments: hub
                        .attachments
                        .iter()
                        .map(HubAttachment::resolved)
                        .collect(),
                    question: view.question.clone(),
                    answer: view.answer.clone(),
                    expect_mcp_reply: true,
                })),
                (None, Some(attachments)) => Ok(attachments.turn_input(
                    &view,
                    self.author.kind,
                    &self.author.name,
                    blob_root,
                )),
                _ => Err(NodeError::internal("Message attachment source is missing")),
            }
        };
        if state == DeliveryState::Accepted {
            let input = turn()?;
            let work = DirectWork {
                receipt,
                message_id: view.message_id,
                input,
                attachments: view.attachments,
                answer: view.answer.is_some(),
                prepared_at: now,
            };
            return Ok(WriteOutcome::with_events(
                Prepared::Direct(Box::new(work)),
                events,
            ));
        }
        if let Some(ttl) = ttl {
            let input = turn()?;
            let envelope = encode_queue_envelope(&input)?;
            if admission.is_some_and(AdmissionGuard::is_expired) {
                return Err(admission_expired().into());
            }
            enqueue(
                tx,
                &recipient.id,
                &view.message_id,
                &envelope,
                &attachments_json,
                now,
                ttl,
            )?;
        }
        operation.succeed(tx, now, &receipt)?;
        Ok(WriteOutcome::with_events(
            Prepared::Done(Ok(receipt)),
            events,
        ))
    }

    fn initial_state(
        &self,
        tx: &Transaction<'_>,
        target: &Recipient,
        live: &AttemptAdmission,
        now: i64,
        events: &mut Vec<EventNotice>,
    ) -> rusqlite::Result<DeliveryState> {
        if target.kind == ParticipantKind::Human {
            return Ok(DeliveryState::Delivered);
        }
        if target.kind != ParticipantKind::Agent
            || live.live_generation != Some(target.generation)
            || !matches!(
                target.state,
                ParticipantState::Idle
                    | ParticipantState::Working
                    | ParticipantState::AwaitingApproval
            )
        {
            return Ok(DeliveryState::Unavailable);
        }
        if self.input.delivery_mode == DeliveryMode::QueueUntilIdle {
            expire_for_recipient(tx, &target.id, now, events)?;
            return Ok(DeliveryState::Queued);
        }
        Ok(match (&live.preflight, target.state) {
            (_, ParticipantState::AwaitingApproval)
            | (DirectGate::Denied(SlotDenial::AwaitingApproval), _) => {
                DeliveryState::AwaitingApproval
            }
            (_, ParticipantState::Working) | (DirectGate::Denied(SlotDenial::Busy), _) => {
                DeliveryState::Busy
            }
            (DirectGate::Ready, _) => DeliveryState::Accepted,
            (DirectGate::Denied(SlotDenial::Unavailable), _) => DeliveryState::Unavailable,
        })
    }

    fn remember_denial(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        error: NodeError,
    ) -> rusqlite::Result<WriteOutcome<Prepared>> {
        self.operation.reserve(tx, now, None)?;
        self.operation.fail(tx, now, &error)?;
        Ok(WriteOutcome::new(Prepared::Done(Err(error))))
    }

    fn receipt(
        &self,
        message_id: Option<MessageId>,
        delivery_state: DeliveryState,
    ) -> SendMessageResponse {
        SendMessageResponse {
            operation_id: self.operation.id.clone(),
            message_id,
            delivery_state,
            in_reply_to: self.input.in_reply_to.clone(),
        }
    }

    fn check_frame(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        delivery_state: DeliveryState,
    ) -> Result<()> {
        check_message_frame_fields(
            tx,
            now,
            &self.message_id,
            &self.sender_id,
            &self.input,
            delivery_state,
        )
    }
}

fn hub_parent(
    tx: &Transaction<'_>,
    hub_id: Option<&MessageId>,
    recipient: &str,
    now: i64,
) -> Result<Option<MessageId>> {
    let Some(hub_id) = hub_id else {
        return Ok(None);
    };
    tx.query_row(
        "SELECT message_id FROM messages WHERE hub_message_id = ?1 AND expires_at_ms > ?2
         AND (sender_id = ?3 OR recipient_id = ?3)",
        params![hub_id.as_str(), now, recipient],
        |row| row.get::<_, String>(0)?.parse().map_err(sql_error),
    )
    .optional()
    .map_err(Into::into)
}

pub(super) fn submit_state(outcome: &SubmitOutcome) -> DeliveryState {
    match outcome {
        SubmitOutcome::Accepted { .. } => DeliveryState::Delivered,
        SubmitOutcome::Busy => DeliveryState::Busy,
        SubmitOutcome::AwaitingApproval(_) => DeliveryState::AwaitingApproval,
        SubmitOutcome::Expired => DeliveryState::Expired,
        SubmitOutcome::Unavailable(_) => DeliveryState::Unavailable,
        SubmitOutcome::Unknown(_) => DeliveryState::Unknown,
    }
}

pub(super) fn outcome_detail(outcome: &SubmitOutcome) -> Option<&'static str> {
    match outcome {
        SubmitOutcome::Accepted { .. } => None,
        SubmitOutcome::Busy => Some("participant_busy"),
        SubmitOutcome::AwaitingApproval(_) => Some("awaiting_approval"),
        SubmitOutcome::Expired => Some("admission_expired"),
        SubmitOutcome::Unavailable(_) => Some("adapter_unavailable"),
        SubmitOutcome::Unknown(_) => Some("adapter_outcome_unknown"),
    }
}

fn participant_busy() -> NodeError {
    NodeError::new(
        ErrorCode::ParticipantBusy,
        "The participant is already working",
    )
}

fn awaiting_approval(approval: Option<&PendingApprovalRecord>) -> NodeError {
    let mut error = NodeError::new(
        ErrorCode::AwaitingApproval,
        "The participant is waiting for approval",
    );
    if let Some(approval) = approval {
        error = error
            .with_extension("approval_id", approval.public_id.clone())
            .with_extension("wait_kind", approval.kind.as_str())
            .with_extension("can_respond_remotely", approval.can_respond_remotely);
        if let Some(summary) = &approval.summary {
            error = error.with_extension("summary", summary.clone());
        }
        if let Some(hint) = &approval.action_hint {
            error = error.with_extension("action_hint", hint.clone());
        }
    }
    error
}

fn delivery_unknown(operation: &Operation, id: &MessageId) -> NodeError {
    unconfirmed_problem(operation.id.as_str(), Some(id.as_str()))
}

fn unconfirmed_problem(operation_id: &str, message_id: Option<&str>) -> NodeError {
    let problem = NodeError::new(
        ErrorCode::DeliveryUnknown,
        "Delivery may already have taken effect",
    )
    .with_extension("operation_id", operation_id);
    match message_id {
        Some(id) => problem.with_extension("message_id", id),
        None => problem,
    }
}

fn unknown_receipt(
    id: String,
    problem: Option<NodeError>,
    resource_id: Option<String>,
) -> NodeError {
    let mut problem = problem.unwrap_or_else(|| unconfirmed_problem(&id, None));
    if let Some(message_id) = resource_id
        && !problem.0.extensions.contains_key("message_id")
    {
        problem = problem.with_extension("message_id", message_id);
    }
    problem
}

type ReplayOutcome = WriteOutcome<Result<SendMessageResponse>>;

fn replay_in_writer(
    tx: &Transaction<'_>,
    operation: &Operation,
    clock: &Clock,
    reconcile_self: bool,
) -> std::result::Result<Option<ReplayOutcome>, WriteError<NodeError>> {
    match operation.lookup_state(tx) {
        Ok(ReceiptLookup::Missing) => Ok(None),
        Ok(ReceiptLookup::Succeeded(value)) => {
            Ok(Some(WriteOutcome::new(SendMessageResponse::decode(value))))
        }
        Ok(ReceiptLookup::Failed(stored)) => Ok(Some(WriteOutcome::new(Err(stored)))),
        Ok(ReceiptLookup::Unknown {
            id,
            problem,
            resource_id,
        }) => Ok(Some(WriteOutcome::new(Err(unknown_receipt(
            id,
            problem,
            resource_id,
        ))))),
        Ok(ReceiptLookup::Pending { id, .. }) if reconcile_self || id != operation.id.as_str() => {
            // This request holds the key for its entire detached lifetime.
            // Either it belongs to a previous dead attempt or the caller dropped
            // its own slot after an uncertain Tx 1 commit (`reconcile_self`).
            let now = clock().map_err(NodeError::from)?;
            let events = reconcile_pending_message(tx, now, &id)?;
            let result = match operation.lookup_state(tx) {
                Ok(ReceiptLookup::Succeeded(value)) => SendMessageResponse::decode(value),
                Ok(ReceiptLookup::Failed(stored)) => Err(stored),
                Ok(ReceiptLookup::Unknown {
                    id,
                    problem,
                    resource_id,
                }) => Err(unknown_receipt(id, problem, resource_id)),
                Ok(ReceiptLookup::Missing | ReceiptLookup::Pending { .. }) => {
                    Err(NodeError::internal("Missing reconciled receipt"))
                }
                Err(error) => Err(error),
            };
            Ok(Some(WriteOutcome::with_events(result, events)))
        }
        Ok(ReceiptLookup::Pending { id, .. }) => Err(NodeError::new(
            ErrorCode::OperationInProgress,
            "The earlier attempt is still in progress",
        )
        .with_extension("operation_id", id)
        .into()),
        Err(error) => Ok(Some(WriteOutcome::new(Err(error)))),
    }
}

fn redact_approval(mut error: NodeError, can_manage: bool) -> NodeError {
    if !can_manage && error.code() == ErrorCode::AwaitingApproval {
        for field in [
            "approval_id",
            "can_respond_remotely",
            "summary",
            "action_hint",
        ] {
            error.0.extensions.remove(field);
        }
    }
    error
}
