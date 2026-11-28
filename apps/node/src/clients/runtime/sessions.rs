use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use cooplet_protocol::{ParticipantId, ParticipantState};
use cooplet_storage::{StorageError, WriteOutcome};
use futures_util::FutureExt;
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::now_ms;
use crate::workflows::{TurnCompletion, on_turn_completed};

use super::{
    QueueWakeup, Service, lock,
    state::{ApprovalSource, RuntimeStateChange, enter_awaiting_approval, set_runtime_state},
};
use crate::clients::{
    AdapterCapabilities, AdapterEvent, AdapterEventKind, ClientAdapter, ParticipantRuntimeState,
    SessionHandle, SubmitOutcome, SubmitTurnRequest, TurnFinishStatus,
};

const MAX_EARLY_EVENTS: usize = 256;
const SUBMIT_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotDenial {
    Busy,
    AwaitingApproval,
    Unavailable,
}

pub struct LiveSession {
    id: ParticipantId,
    generation: u64,
    pub(super) adapter: Arc<dyn ClientAdapter>,
    pub(super) handle: SessionHandle,
    capabilities: AdapterCapabilities,
    state: Mutex<SessionState>,
    wakeup: Option<QueueWakeup>,
    disconnected: watch::Sender<()>,
}

struct SessionState {
    current: ParticipantState,
    last_event_seq: u64,
    latest_runtime_event_seq: u64,
    next_slot: u64,
    waiting_for_idle: bool,
    phase: Phase,
}

impl SessionState {
    fn apply_approval_ack(&mut self, submitted_after_seq: u64, approval_current: bool) {
        if approval_current && self.latest_runtime_event_seq <= submitted_after_seq {
            self.current = ParticipantState::AwaitingApproval;
        }
    }
}

enum Phase {
    Open,
    Submitting {
        slot_id: u64,
        operation_key: String,
        submitted: bool,
        early_events: Vec<SlotEvent>,
        early_completions: Vec<DeferredTurnCompletion>,
    },
    Running(Run),
    Fenced(Option<FencedSubmission>),
}

struct FencedSubmission {
    slot_id: u64,
    operation_key: String,
    early_completions: Vec<DeferredTurnCompletion>,
}

struct DeferredTurnCompletion {
    native_turn_id: Option<String>,
    operation_key: Option<String>,
    status: TurnFinishStatus,
    text: String,
    structured: Option<Value>,
}

impl DeferredTurnCompletion {
    fn as_turn_completion<'a>(
        &'a self,
        participant_id: &'a ParticipantId,
        generation: u64,
    ) -> TurnCompletion<'a> {
        TurnCompletion {
            participant_id,
            generation,
            native_turn_id: self.native_turn_id.as_deref(),
            operation_key: self.operation_key.as_deref(),
            status: self.status,
            text: &self.text,
            structured: self.structured.as_ref(),
        }
    }
}

struct FinishPlan {
    deferred: Vec<DeferredTurnCompletion>,
    disconnect: bool,
}

struct Run {
    operation_key: String,
    native_turn_id: Option<String>,
    has_correlation: bool,
    saw_working: bool,
    completed: bool,
    idle: bool,
}

enum SlotEvent {
    Working,
    Idle,
    Completed {
        turn_id: Option<String>,
        operation_key: Option<String>,
    },
    Other,
}

impl Run {
    fn new(operation_key: String, native_turn_id: Option<String>) -> Self {
        Self {
            operation_key,
            has_correlation: native_turn_id.is_some(),
            native_turn_id,
            saw_working: false,
            completed: false,
            idle: false,
        }
    }

    fn correlate_completion(&self, completion: &mut DeferredTurnCompletion) -> bool {
        let matches = self.matches_completion(
            completion.native_turn_id.as_deref(),
            completion.operation_key.as_deref(),
        );
        if matches && completion.operation_key.is_none() {
            completion.operation_key = Some(self.operation_key.clone());
        }
        matches
    }

    fn matches_completion(&self, turn_id: Option<&str>, operation_key: Option<&str>) -> bool {
        match (self.native_turn_id.as_deref(), turn_id, operation_key) {
            (Some(expected), Some(actual), key) => {
                expected == actual && key.is_none_or(|key| key == self.operation_key)
            }
            (_, _, Some(key)) => key == self.operation_key,
            (None, None, None) => true,
            _ => false,
        }
    }

    fn apply(&mut self, event: SlotEvent) -> bool {
        match event {
            SlotEvent::Working => {
                self.saw_working = true;
                self.idle = false;
            }
            SlotEvent::Idle => self.idle = true,
            SlotEvent::Completed {
                turn_id,
                operation_key,
            } => {
                // Only `operation_key` introduces correlation when the adapter did
                // not return a `native_turn_id` on submit; an unannounced `turn_id`
                // cannot be matched against `None` and must not lock out Idle release.
                self.has_correlation |= operation_key.is_some();
                self.completed |=
                    self.matches_completion(turn_id.as_deref(), operation_key.as_deref());
            }
            SlotEvent::Other => self.idle = false,
        }
        self.idle && (self.completed || (!self.has_correlation && self.saw_working))
    }
}

struct RecordOutcome {
    early_limit_exceeded: bool,
    immediate_completion: Option<DeferredTurnCompletion>,
}

impl LiveSession {
    pub(super) fn new(
        adapter: Arc<dyn ClientAdapter>,
        handle: SessionHandle,
        capabilities: AdapterCapabilities,
        current: ParticipantState,
        wakeup: Option<QueueWakeup>,
    ) -> Self {
        let persisted = handle.persisted();
        Self {
            id: persisted.participant_id.clone(),
            generation: persisted.generation,
            adapter,
            handle,
            capabilities,
            wakeup,
            state: Mutex::new(SessionState {
                current,
                last_event_seq: 0,
                latest_runtime_event_seq: 0,
                next_slot: 1,
                waiting_for_idle: false,
                phase: Phase::Open,
            }),
            disconnected: watch::channel(()).0,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn capabilities(&self) -> &AdapterCapabilities {
        &self.capabilities
    }
    pub fn state(&self) -> ParticipantState {
        lock(&self.state).current
    }

    pub fn try_submit(self: &Arc<Self>, operation_key: &str) -> Result<SubmitSlot, SlotDenial> {
        self.reserve_submit(operation_key, SlotDenial::Busy)
    }

    pub(crate) fn try_thread_submit(
        self: &Arc<Self>,
        operation_key: &str,
    ) -> Result<SubmitSlot, SlotDenial> {
        // Hub waits for a new Idle event after Busy. An Idle event may already
        // have committed while the runtime applies it or settles its last ACK.
        self.reserve_submit(operation_key, SlotDenial::Unavailable)
    }

    fn reserve_submit(
        self: &Arc<Self>,
        operation_key: &str,
        idle_conflict: SlotDenial,
    ) -> Result<SubmitSlot, SlotDenial> {
        let mut state = lock(&self.state);
        if idle_conflict == SlotDenial::Unavailable
            && state.latest_runtime_event_seq > state.last_event_seq
        {
            return Err(SlotDenial::Unavailable);
        }
        match state.current {
            ParticipantState::Working => return Err(SlotDenial::Busy),
            ParticipantState::AwaitingApproval => return Err(SlotDenial::AwaitingApproval),
            ParticipantState::Idle => (),
            _ => return Err(SlotDenial::Unavailable),
        }
        if state.waiting_for_idle || !matches!(state.phase, Phase::Open) {
            return Err(idle_conflict);
        }
        let slot_id = state.next_slot;
        state.next_slot = state.next_slot.wrapping_add(1);
        state.phase = Phase::Submitting {
            slot_id,
            operation_key: operation_key.into(),
            submitted: false,
            early_events: Vec::new(),
            early_completions: Vec::new(),
        };
        Ok(SubmitSlot {
            live: self.clone(),
            slot_id,
            submitted_after_seq: state.last_event_seq,
            native_started: false,
            finished: false,
            wake_on_drop: true,
        })
    }

    fn observe(&self, event_seq: u64, changes_runtime: bool) -> bool {
        let mut state = lock(&self.state);
        if matches!(state.phase, Phase::Fenced(_)) || state.last_event_seq >= event_seq {
            return false;
        }
        if changes_runtime {
            // Announce the sequence before sending its write to SQLite. A
            // completed ACK cannot outrun an event already being committed.
            state.latest_runtime_event_seq = event_seq;
        }
        true
    }

    fn record(
        &self,
        event_seq: u64,
        current: Option<ParticipantState>,
        event: SlotEvent,
        mut completion: Option<DeferredTurnCompletion>,
    ) -> RecordOutcome {
        let mut state = lock(&self.state);
        if matches!(state.phase, Phase::Fenced(_)) || state.last_event_seq >= event_seq {
            return RecordOutcome {
                early_limit_exceeded: false,
                immediate_completion: None,
            };
        }
        state.last_event_seq = event_seq;
        if let Some(current) = current {
            state.current = current;
            if current == ParticipantState::Idle {
                state.waiting_for_idle = false;
            }
        }
        match &mut state.phase {
            Phase::Submitting {
                submitted: true,
                early_events,
                ..
            } if early_events.len() >= MAX_EARLY_EVENTS => {
                state.current = ParticipantState::Unknown;
                state.phase = Phase::Fenced(None);
                RecordOutcome {
                    early_limit_exceeded: true,
                    immediate_completion: None,
                }
            }
            Phase::Submitting {
                submitted: true,
                early_events,
                early_completions,
                ..
            } => {
                early_events.push(event);
                if let Some(completion) = completion {
                    early_completions.push(completion);
                }
                RecordOutcome {
                    early_limit_exceeded: false,
                    immediate_completion: None,
                }
            }
            Phase::Running(run) => {
                if let (
                    Some(completion),
                    SlotEvent::Completed {
                        turn_id,
                        operation_key,
                    },
                ) = (&mut completion, &event)
                    && completion.operation_key.is_none()
                    && run.matches_completion(turn_id.as_deref(), operation_key.as_deref())
                {
                    completion.operation_key = Some(run.operation_key.clone());
                }
                if run.apply(event) {
                    state.phase = Phase::Open;
                }
                RecordOutcome {
                    early_limit_exceeded: false,
                    immediate_completion: completion,
                }
            }
            _ => RecordOutcome {
                early_limit_exceeded: false,
                immediate_completion: completion,
            },
        }
    }

    pub(super) fn disconnect(&self) {
        self.fence();
        self.disconnected.send_replace(());
    }

    #[cfg(target_os = "linux")]
    pub(super) fn is_fenced(&self) -> bool {
        matches!(lock(&self.state).phase, Phase::Fenced(_))
    }

    fn fence(&self) {
        let mut state = lock(&self.state);
        state.current = ParticipantState::Unknown;
        if matches!(state.phase, Phase::Fenced(_)) {
            return;
        }
        if let Phase::Submitting {
            slot_id,
            operation_key,
            early_completions,
            submitted: true,
            ..
        } = std::mem::replace(&mut state.phase, Phase::Fenced(None))
            && !early_completions.is_empty()
        {
            // A ready ACK may still finish this exact slot after stream loss.
            state.phase = Phase::Fenced(Some(FencedSubmission {
                slot_id,
                operation_key,
                early_completions,
            }));
        }
    }

    pub(super) fn wake_if_idle(&self) {
        let ready = {
            let state = lock(&self.state);
            state.current == ParticipantState::Idle
                && !state.waiting_for_idle
                && matches!(state.phase, Phase::Open)
        };
        if ready && let Some(wakeup) = &self.wakeup {
            wakeup(self.id.clone());
        }
    }
}

pub struct SubmitSlot {
    live: Arc<LiveSession>,
    slot_id: u64,
    submitted_after_seq: u64,
    native_started: bool,
    finished: bool,
    wake_on_drop: bool,
}

impl SubmitSlot {
    pub fn generation(&self) -> u64 {
        self.live.generation
    }

    pub fn submitted_after_seq(&self) -> u64 {
        self.submitted_after_seq
    }

    /// Yield a pre-submit reservation when the queue worker cannot proceed.
    pub fn release_without_wakeup(mut self) {
        self.wake_on_drop = false;
    }

    pub async fn submit(&mut self, request: SubmitTurnRequest) -> SubmitOutcome {
        if self.native_started || self.finished {
            return SubmitOutcome::Unknown("submit slot is already used".into());
        }
        let mut disconnected = self.live.event_stop();
        let still_live = {
            let mut state = lock(&self.live.state);
            if let Phase::Submitting {
                slot_id, submitted, ..
            } = &mut state.phase
                && *slot_id == self.slot_id
            {
                *submitted = true;
                self.submitted_after_seq = state.last_event_seq;
                true
            } else {
                false
            }
        };
        if !still_live {
            return SubmitOutcome::Unavailable("session closed before submit".into());
        }
        self.native_started = true;
        let admission = request.admission.clone();
        let started = AtomicBool::new(false);
        let acknowledged = async {
            if admission.is_expired() {
                return Ok(SubmitOutcome::Expired);
            }
            started.store(true, Ordering::Relaxed);
            tokio::time::timeout(
                SUBMIT_ACK_TIMEOUT,
                self.live.adapter.submit_turn(&self.live.handle, request),
            )
            .await
        };
        tokio::pin!(acknowledged);
        tokio::select! {
            biased;
            _ = disconnected.changed() => {
                // Fence a possible first write before checking for an ACK that
                // the adapter already received as its event stream closed.
                admission.expire();
                if started.load(Ordering::Relaxed) {
                    match acknowledged.as_mut().now_or_never() {
                        Some(Ok(outcome)) => outcome,
                        _ => SubmitOutcome::Unknown("session closed during submit".into()),
                    }
                } else {
                    SubmitOutcome::Unavailable("session closed before submit".into())
                }
            }
            result = acknowledged.as_mut() => match result {
                Ok(outcome) => outcome,
                Err(_) => SubmitOutcome::Unknown("submit acknowledgment timed out".into()),
            },
        }
    }

    fn apply_finish(&mut self, outcome: &SubmitOutcome, approval_current: bool) -> FinishPlan {
        self.finished = true;
        let mut state = lock(&self.live.state);
        if let Phase::Fenced(fenced) = &mut state.phase
            && fenced
                .as_ref()
                .is_some_and(|submission| submission.slot_id == self.slot_id)
        {
            let deferred = match (fenced.take(), outcome) {
                (Some(submission), SubmitOutcome::Accepted { native_turn_id }) => {
                    let run = Run::new(submission.operation_key, native_turn_id.clone());
                    submission
                        .early_completions
                        .into_iter()
                        .filter_map(|mut completion| {
                            run.correlate_completion(&mut completion)
                                .then_some(completion)
                        })
                        .collect()
                }
                _ => Vec::new(),
            };
            return FinishPlan {
                deferred,
                disconnect: false,
            };
        }
        let Phase::Submitting { slot_id, .. } = &state.phase else {
            return FinishPlan {
                deferred: Vec::new(),
                disconnect: false,
            };
        };
        if *slot_id != self.slot_id {
            return FinishPlan {
                deferred: Vec::new(),
                disconnect: false,
            };
        }
        let Phase::Submitting {
            operation_key,
            early_events,
            mut early_completions,
            ..
        } = std::mem::replace(&mut state.phase, Phase::Open)
        else {
            return FinishPlan {
                deferred: Vec::new(),
                disconnect: false,
            };
        };
        match outcome {
            SubmitOutcome::Accepted { native_turn_id } => {
                let mut run = Run::new(operation_key, native_turn_id.clone());
                for completion in &mut early_completions {
                    run.correlate_completion(completion);
                }
                let mut opens = false;
                for event in early_events {
                    opens = run.apply(event);
                }
                if !opens || state.current != ParticipantState::Idle {
                    state.phase = Phase::Running(run);
                }
                FinishPlan {
                    deferred: if state.current != ParticipantState::Unknown {
                        early_completions
                    } else {
                        Vec::new()
                    },
                    disconnect: false,
                }
            }
            SubmitOutcome::Unknown(_) => {
                state.current = ParticipantState::Unknown;
                state.phase = Phase::Fenced(None);
                FinishPlan {
                    deferred: Vec::new(),
                    disconnect: true,
                }
            }
            SubmitOutcome::AwaitingApproval(_) => {
                state.apply_approval_ack(self.submitted_after_seq, approval_current);
                FinishPlan {
                    deferred: if state.current != ParticipantState::Unknown {
                        early_completions
                    } else {
                        Vec::new()
                    },
                    disconnect: false,
                }
            }
            SubmitOutcome::Busy => {
                // A fresh Idle can arrive while the negative ACK is pending.
                let early_idle = early_events
                    .iter()
                    .any(|event| matches!(event, SlotEvent::Idle));
                state.waiting_for_idle = !(early_idle && state.current == ParticipantState::Idle);
                FinishPlan {
                    deferred: if state.current != ParticipantState::Unknown {
                        early_completions
                    } else {
                        Vec::new()
                    },
                    disconnect: false,
                }
            }
            _ => FinishPlan {
                deferred: if state.current != ParticipantState::Unknown {
                    early_completions
                } else {
                    Vec::new()
                },
                disconnect: false,
            },
        }
    }

    pub async fn finish(mut self, outcome: &SubmitOutcome, approval_current: bool) {
        let plan = self.apply_finish(outcome, approval_current);
        if plan.disconnect {
            self.live.disconnected.send_replace(());
            return;
        }
        for completion in plan.deferred {
            on_turn_completed(completion.as_turn_completion(&self.live.id, self.live.generation))
                .await;
        }
        self.live.wake_if_idle();
    }
}

impl Drop for SubmitSlot {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut fenced = false;
        {
            let mut state = lock(&self.live.state);
            if !matches!(state.phase, Phase::Submitting { slot_id, .. } if slot_id == self.slot_id)
            {
                return;
            }
            if self.native_started {
                state.current = ParticipantState::Unknown;
                state.phase = Phase::Fenced(None);
                fenced = true;
            } else {
                state.phase = Phase::Open;
            }
        }
        if fenced {
            self.live.disconnected.send_replace(());
        } else if self.wake_on_drop {
            self.live.wake_if_idle();
        }
    }
}

pub(super) async fn run_events(
    service: Weak<Service>,
    live: Arc<LiveSession>,
    mut events: mpsc::Receiver<AdapterEvent>,
    mut disconnected: watch::Receiver<()>,
) {
    loop {
        let event = tokio::select! {
            biased;
            _ = disconnected.changed() => return,
            event = events.recv() => event,
        };
        let Some(service) = service.upgrade() else {
            return;
        };
        let Some(event) = event else {
            mark_unknown(&service, &live, "event_stream_closed").await;
            return;
        };
        let (current, slot_event, reason) = match &event.kind {
            AdapterEventKind::ApprovalRequested(_)
            | AdapterEventKind::StateChanged(ParticipantRuntimeState::AwaitingApproval(_)) => (
                Some(ParticipantState::AwaitingApproval),
                SlotEvent::Other,
                None,
            ),
            AdapterEventKind::StateChanged(runtime) => match runtime.state() {
                ParticipantState::Idle => (Some(ParticipantState::Idle), SlotEvent::Idle, None),
                ParticipantState::Working => {
                    (Some(ParticipantState::Working), SlotEvent::Working, None)
                }
                ParticipantState::Unavailable => {
                    (Some(ParticipantState::Unavailable), SlotEvent::Other, None)
                }
                ParticipantState::Stopped | ParticipantState::Unknown => {
                    (None, SlotEvent::Other, Some("unconfirmed_session_end"))
                }
                _ => (None, SlotEvent::Other, None),
            },
            AdapterEventKind::TurnCompleted {
                native_turn_id,
                operation_key,
                ..
            } => (
                None,
                SlotEvent::Completed {
                    turn_id: native_turn_id.clone(),
                    operation_key: operation_key.clone(),
                },
                None,
            ),
            AdapterEventKind::ProcessExited { .. } => {
                (None, SlotEvent::Other, Some("process_exited"))
            }
        };
        if !live.observe(event.event_seq, current.is_some() || reason.is_some()) {
            continue;
        }
        if let Some(reason) = reason {
            mark_unknown(&service, &live, reason).await;
            return;
        }
        if let Some(state) = current {
            let approval = match &event.kind {
                AdapterEventKind::ApprovalRequested(wait)
                | AdapterEventKind::StateChanged(ParticipantRuntimeState::AwaitingApproval(wait)) => {
                    Some(wait.clone())
                }
                _ => None,
            };
            let result = persist_runtime(&service, &live, state, approval, event.event_seq).await;
            if let Err(error) = result {
                tracing::warn!(%error, "Could not record adapter state");
                mark_unknown(&service, &live, "state_commit_failed").await;
                return;
            }
            if !result.unwrap_or(false) {
                live.disconnect();
                evict_live(&service, &live.id, live.generation);
                return;
            }
        }
        let completion = match event.kind {
            AdapterEventKind::TurnCompleted {
                native_turn_id,
                operation_key,
                status,
                text,
                structured,
            } => Some(DeferredTurnCompletion {
                native_turn_id,
                operation_key,
                status,
                text,
                structured,
            }),
            _ => None,
        };
        let recorded = live.record(event.event_seq, current, slot_event, completion);
        if recorded.early_limit_exceeded {
            mark_unknown(&service, &live, "early_event_limit").await;
            return;
        }
        if let Some(completion) = recorded.immediate_completion
            && live.state() != ParticipantState::Unknown
        {
            on_turn_completed(completion.as_turn_completion(&live.id, live.generation)).await;
        }
        live.wake_if_idle();
    }
}

impl LiveSession {
    pub(super) fn event_stop(&self) -> watch::Receiver<()> {
        self.disconnected.subscribe()
    }
}

fn evict_live(service: &Service, id: &ParticipantId, generation: u64) {
    let mut sessions = lock(&service.sessions);
    if sessions
        .live
        .get(id)
        .is_some_and(|live| live.generation() == generation)
    {
        sessions.live.remove(id);
    }
    #[cfg(target_os = "linux")]
    service.exit_monitor.unwatch(&(id.clone(), generation));
}

pub(super) async fn mark_unknown(service: &Service, live: &LiveSession, reason: &'static str) {
    live.disconnect();
    evict_live(service, &live.id, live.generation);
    let id = live.id.clone();
    let generation = live.generation;
    crate::storage::retry_recovery_write(
        service.db.clone(),
        service.stopping.subscribe(),
        move |tx, now| {
            let mut events = Vec::new();
            set_runtime_state(
                tx,
                now,
                &id,
                generation,
                RuntimeStateChange {
                    state: ParticipantState::Unknown,
                    reason: Some(reason),
                    event_seq: None,
                },
                &mut events,
            )?;
            Ok(events)
        },
    )
    .await;
}

async fn persist_runtime(
    service: &Service,
    live: &LiveSession,
    state: ParticipantState,
    approval: Option<crate::clients::ApprovalWait>,
    event_seq: u64,
) -> Result<bool, StorageError> {
    let now = now_ms()?;
    let generation = live.generation;
    for delay in [0, 10, 25, 50, 100] {
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        let id = live.id.clone();
        let approval = approval.clone();
        let result = service
            .db
            .write_sqlite(move |tx| {
                let mut events = Vec::new();
                let applied = if let Some(wait) = approval {
                    enter_awaiting_approval(
                        tx,
                        now,
                        &id,
                        generation,
                        wait,
                        ApprovalSource::Event(event_seq),
                        &mut events,
                    )?
                    .is_some()
                } else {
                    set_runtime_state(
                        tx,
                        now,
                        &id,
                        generation,
                        RuntimeStateChange {
                            state,
                            reason: None,
                            event_seq: Some(event_seq),
                        },
                        &mut events,
                    )?
                };
                Ok(WriteOutcome::with_events(applied, events))
            })
            .await;
        if !matches!(result, Err(StorageError::Overloaded)) {
            return result;
        }
    }
    Err(StorageError::Overloaded)
}

#[cfg(test)]
mod tests {
    use super::{Phase, Run, SessionState, SlotEvent};
    use cooplet_protocol::ParticipantState;

    fn run(native_turn_id: Option<&str>) -> Run {
        Run {
            operation_key: "delivery-key".into(),
            native_turn_id: native_turn_id.map(str::to_owned),
            has_correlation: native_turn_id.is_some(),
            saw_working: false,
            completed: false,
            idle: false,
        }
    }

    #[test]
    fn correlated_turn_requires_its_completion_and_fresh_idle() {
        let mut turn = run(Some("native-turn"));
        assert!(!turn.apply(SlotEvent::Working));
        assert!(!turn.apply(SlotEvent::Idle));
        assert!(!turn.apply(SlotEvent::Completed {
            turn_id: Some("unrelated-turn".into()),
            operation_key: None,
        }));
        for (native, key) in [
            ("native-turn", "another-key"),
            ("another-turn", "delivery-key"),
        ] {
            assert!(!turn.apply(SlotEvent::Completed {
                turn_id: Some(native.into()),
                operation_key: Some(key.into()),
            }));
        }
        assert!(turn.apply(SlotEvent::Completed {
            turn_id: Some("native-turn".into()),
            operation_key: None,
        }));
    }

    #[test]
    fn completed_does_not_release_a_non_idle_session() {
        let mut turn = run(None);
        assert!(!turn.apply(SlotEvent::Completed {
            turn_id: None,
            operation_key: Some("delivery-key".into()),
        }));
        assert!(!turn.apply(SlotEvent::Other));
        assert!(turn.apply(SlotEvent::Idle));
    }

    #[test]
    fn uncorrelated_adapter_can_confirm_completion_by_working_then_idle() {
        let mut turn = run(None);
        assert!(!turn.apply(SlotEvent::Idle));
        assert!(!turn.apply(SlotEvent::Working));
        assert!(turn.apply(SlotEvent::Idle));
    }

    #[test]
    fn an_announced_event_fences_an_ack_before_its_write_reply() {
        let mut state = SessionState {
            current: ParticipantState::Idle,
            last_event_seq: 1,
            latest_runtime_event_seq: 2,
            next_slot: 1,
            waiting_for_idle: false,
            phase: Phase::Open,
        };
        state.apply_approval_ack(1, true);
        assert_eq!(state.current, ParticipantState::Idle);

        state.apply_approval_ack(2, false);
        assert_eq!(state.current, ParticipantState::Idle);

        state.apply_approval_ack(2, true);
        assert_eq!(state.current, ParticipantState::AwaitingApproval);
    }

    #[test]
    fn uncorrelated_turn_ignores_foreign_native_turn_id_without_deadlocking() {
        let mut turn = run(None);
        assert!(!turn.apply(SlotEvent::Completed {
            turn_id: Some("unrelated-turn".into()),
            operation_key: None,
        }));
        assert!(!turn.has_correlation);
        assert!(!turn.apply(SlotEvent::Working));
        assert!(turn.apply(SlotEvent::Idle));
    }

    #[test]
    fn run_matches_completion_by_native_turn_id_when_operation_key_is_omitted() {
        let mut turn = run(Some("native-turn-1"));
        assert!(turn.matches_completion(Some("native-turn-1"), None));
        assert!(!turn.matches_completion(Some("other-turn"), None));
        assert!(!turn.matches_completion(Some("native-turn-1"), Some("wrong-key")));
        assert!(turn.matches_completion(None, Some("delivery-key")));
        assert!(!turn.apply(SlotEvent::Completed {
            turn_id: Some("native-turn-1".into()),
            operation_key: None,
        }));
        assert!(turn.apply(SlotEvent::Idle));
    }

    struct StubAdapter;

    impl crate::clients::ClientAdapter for StubAdapter {
        fn kind(&self) -> &'static str {
            "stub"
        }

        fn discover<'a>(
            &'a self,
            _cfg: &'a crate::clients::AdapterConfig,
        ) -> futures_util::future::BoxFuture<
            'a,
            crate::error::Result<crate::clients::DiscoveryReport>,
        > {
            Box::pin(async { Err(crate::error::NodeError::internal("unused")) })
        }

        fn capabilities(&self) -> crate::clients::AdapterCapabilities {
            crate::clients::AdapterCapabilities::default()
        }

        fn create_or_resume(
            &self,
            _request: crate::clients::SessionOpenRequest,
        ) -> futures_util::future::BoxFuture<'_, crate::error::Result<crate::clients::OpenedSession>>
        {
            Box::pin(async { Err(crate::error::NodeError::internal("unused")) })
        }

        fn inspect_state<'a>(
            &'a self,
            _target: &'a crate::clients::PersistedSessionRef,
            _live: Option<&'a crate::clients::SessionHandle>,
        ) -> futures_util::future::BoxFuture<
            'a,
            crate::error::Result<crate::clients::SessionInspection>,
        > {
            Box::pin(async { Err(crate::error::NodeError::internal("unused")) })
        }

        fn submit_turn<'a>(
            &'a self,
            handle: &'a crate::clients::SessionHandle,
            _request: crate::clients::SubmitTurnRequest,
        ) -> futures_util::future::BoxFuture<'a, crate::clients::SubmitOutcome> {
            Box::pin(async move {
                let receiver = handle
                    .downcast_ref::<StubAck>()
                    .and_then(|ack| super::lock(ack).take());
                if let Some(receiver) = receiver {
                    return receiver
                        .await
                        .unwrap_or_else(|_| super::SubmitOutcome::Unknown("stub closed".into()));
                }
                super::SubmitOutcome::Unknown("stub".into())
            })
        }

        fn respond_approval<'a>(
            &'a self,
            _handle: &'a crate::clients::SessionHandle,
            _decision: crate::clients::ApprovalDecision,
        ) -> futures_util::future::BoxFuture<'a, crate::clients::ApprovalRespondOutcome> {
            Box::pin(async { crate::clients::ApprovalRespondOutcome::Unsupported("unused".into()) })
        }

        fn cancel_or_stop<'a>(
            &'a self,
            _handle: &'a crate::clients::SessionHandle,
            _mode: crate::clients::StopMode,
        ) -> futures_util::future::BoxFuture<'a, crate::error::Result<crate::clients::StopOutcome>>
        {
            Box::pin(async { Err(crate::error::NodeError::internal("unused")) })
        }

        fn tui_attach_plan<'a>(
            &'a self,
            _target: &'a crate::clients::PersistedSessionRef,
            _live: Option<&'a crate::clients::SessionHandle>,
        ) -> futures_util::future::BoxFuture<'a, crate::error::Result<crate::clients::TuiAttachPlan>>
        {
            Box::pin(async { Err(crate::error::NodeError::internal("unused")) })
        }
    }

    type SessionTestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
    type StubAck = std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<super::SubmitOutcome>>>;

    fn test_turn_input() -> SessionTestResult<crate::clients::TurnInput> {
        Ok(crate::clients::TurnInput::direct(
            crate::clients::InboundMessage {
                message_id: cooplet_protocol::MessageId::from_parts(1, [0; 10])?,
                author: crate::clients::VerifiedAuthor {
                    kind: cooplet_protocol::AuthorKind::Human,
                    actor_id: "part_00000000000000000000000001".into(),
                    name: "operator".into(),
                    source_node_id: None,
                },
                in_reply_to: None,
                body: "hi".into(),
                attachments: Vec::new(),
                question: None,
                answer: None,
                expect_mcp_reply: true,
            },
        ))
    }

    fn live_session() -> SessionTestResult<std::sync::Arc<super::LiveSession>> {
        live_session_with(())
    }

    fn live_session_with<T: Send + Sync + 'static>(
        transport: T,
    ) -> SessionTestResult<std::sync::Arc<super::LiveSession>> {
        let participant_id = cooplet_protocol::ParticipantId::from_parts(1, [0; 10])?;
        let persisted = crate::clients::PersistedSessionRef {
            participant_id,
            generation: 1,
            native_key: None,
            process_ownership: crate::clients::ProcessOwnership::Attached,
            workspace_dir: std::path::PathBuf::from("."),
            runtime_dir: std::path::PathBuf::from("."),
            process_identity: None,
            adapter_binding: serde_json::json!({}),
        };
        let handle = crate::clients::SessionHandle::new(transport, persisted);
        Ok(std::sync::Arc::new(super::LiveSession::new(
            std::sync::Arc::new(StubAdapter),
            handle,
            crate::clients::AdapterCapabilities::default(),
            ParticipantState::Idle,
            None,
        )))
    }

    #[test]
    fn thread_can_retry_if_the_durable_idle_event_outruns_the_live_slot() -> SessionTestResult {
        let live = live_session()?;
        live.record(1, Some(ParticipantState::Working), SlotEvent::Working, None);
        assert!(matches!(
            live.try_thread_submit("working"),
            Err(super::SlotDenial::Busy)
        ));

        assert!(live.observe(2, true));
        assert!(matches!(
            live.try_thread_submit("idle-committing"),
            Err(super::SlotDenial::Unavailable)
        ));
        assert!(matches!(
            live.try_submit("local"),
            Err(super::SlotDenial::Busy)
        ));

        live.record(2, Some(ParticipantState::Idle), SlotEvent::Idle, None);
        let slot = live
            .try_thread_submit("idle")
            .map_err(|error| format!("{error:?}"))?;
        assert!(matches!(
            live.try_thread_submit("already-projected-idle"),
            Err(super::SlotDenial::Unavailable)
        ));
        drop(slot);
        assert!(live.try_thread_submit("retry").is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn expired_admission_stops_before_adapter_but_late_ack_survives() -> SessionTestResult {
        use cooplet_platform::{AdmissionGuard, MonoClock, MonoInstant};
        use std::{
            future::Future,
            sync::{
                Arc,
                atomic::{AtomicU64, Ordering},
            },
            task::{Context, Poll, Waker},
            time::Duration,
        };

        struct Clock(AtomicU64);
        impl MonoClock for Clock {
            fn now(&self) -> MonoInstant {
                MonoInstant::from_millis(self.0.load(Ordering::Relaxed))
            }
        }
        let clock = Arc::new(Clock(AtomicU64::new(0)));
        let (ack, receiver) = tokio::sync::oneshot::channel();
        let live = live_session_with(StubAck::new(Some(receiver)))?;
        let make = |operation_key: &str, admission: AdmissionGuard| -> SessionTestResult<_> {
            Ok(super::SubmitTurnRequest {
                operation_key: operation_key.into(),
                message_id: None,
                input: test_turn_input()?,
                output_schema: None,
                steer: false,
                admission,
            })
        };
        let expired = AdmissionGuard::with_clock(Duration::from_millis(5), clock.clone());
        clock.0.store(5, Ordering::Relaxed);
        let mut slot = live
            .try_submit("expired")
            .map_err(|error| format!("{error:?}"))?;
        let outcome = slot.submit(make("expired", expired)?).await;
        assert!(matches!(outcome, super::SubmitOutcome::Expired));
        slot.finish(&outcome, false).await;

        let admitting = AdmissionGuard::with_clock(Duration::from_millis(5), clock.clone());
        let mut slot = live
            .try_submit("admitted")
            .map_err(|error| format!("{error:?}"))?;
        let submit = slot.submit(make("admitted", admitting.clone())?);
        tokio::pin!(submit);
        assert!(matches!(
            submit
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        clock.0.store(11, Ordering::Relaxed);
        assert!(admitting.is_expired());
        ack.send(super::SubmitOutcome::Accepted {
            native_turn_id: Some("accepted".into()),
        })
        .map_err(|_| "adapter was not called within admission")?;
        assert!(matches!(
            submit.await,
            super::SubmitOutcome::Accepted { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn ready_ack_and_its_early_terminal_survive_event_stream_loss() -> SessionTestResult {
        use super::{DeferredTurnCompletion, SubmitOutcome, SubmitTurnRequest, TurnFinishStatus};
        use cooplet_platform::AdmissionGuard;
        use serde_json::json;
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
            time::Duration,
        };
        let (ack, receiver) = tokio::sync::oneshot::channel();
        let live = live_session_with(StubAck::new(Some(receiver)))?;
        let mut slot = live
            .try_submit("simultaneous-ack")
            .map_err(|error| format!("{error:?}"))?;
        let admission = AdmissionGuard::new(Duration::from_secs(10));
        let outcome = {
            let submit = slot.submit(SubmitTurnRequest {
                operation_key: "simultaneous-ack".into(),
                message_id: None,
                input: test_turn_input()?,
                output_schema: None,
                steer: false,
                admission: admission.clone(),
            });
            tokio::pin!(submit);
            assert!(matches!(
                submit
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            for (seq, id) in [(1, "unrelated"), (2, "acknowledged")] {
                let recorded = live.record(
                    seq,
                    None,
                    SlotEvent::Completed {
                        turn_id: Some(id.into()),
                        operation_key: None,
                    },
                    Some(DeferredTurnCompletion {
                        native_turn_id: Some(id.into()),
                        operation_key: None,
                        status: TurnFinishStatus::Completed,
                        text: id.into(),
                        structured: Some(json!({"id": id})),
                    }),
                );
                assert!(recorded.immediate_completion.is_none());
            }
            ack.send(SubmitOutcome::Accepted {
                native_turn_id: Some("acknowledged".into()),
            })
            .map_err(|_| "stub acknowledgement receiver closed")?;
            live.disconnect();
            live.disconnect();
            submit.await
        };
        assert!(
            matches!(&outcome, SubmitOutcome::Accepted {native_turn_id: Some(id)} if id == "acknowledged")
        );
        assert!(admission.is_expired());
        let plan = slot.apply_finish(&outcome, false);
        assert!(!plan.disconnect);
        assert_eq!(plan.deferred.len(), 1);
        let terminal = &plan.deferred[0];
        assert_eq!(terminal.operation_key.as_deref(), Some("simultaneous-ack"));
        assert_eq!(terminal.structured, Some(json!({"id":"acknowledged"})));
        assert_eq!(live.state(), ParticipantState::Unknown);
        assert!(live.try_submit("later").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn ready_no_intake_survives_event_stream_loss() -> SessionTestResult {
        use std::time::Duration;

        let (ack, receiver) = tokio::sync::oneshot::channel();
        let live = live_session_with(StubAck::new(Some(receiver)))?;
        let mut slot = live
            .try_submit("rejected")
            .map_err(|err| format!("{err:?}"))?;
        let submit = slot.submit(super::SubmitTurnRequest {
            operation_key: "rejected".into(),
            message_id: None,
            input: test_turn_input()?,
            output_schema: None,
            steer: false,
            admission: cooplet_platform::AdmissionGuard::new(Duration::from_secs(10)),
        });
        tokio::pin!(submit);
        assert!(
            submit
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        ack.send(super::SubmitOutcome::Unavailable(
            "native server is draining".into(),
        ))
        .map_err(|_| "stub acknowledgement receiver closed")?;
        live.disconnect();
        assert!(matches!(submit.await, super::SubmitOutcome::Unavailable(_)));
        assert_eq!(live.state(), ParticipantState::Unknown);
        Ok(())
    }

    #[test]
    fn early_turn_completed_during_submit_resolves_operation_key_on_finish() -> SessionTestResult {
        let live = live_session()?;
        let mut slot = live
            .try_submit("op-early")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;

        // Before submit_turn starts, an unrelated completion is not deferred.
        let pre_submit = live.record(
            1,
            None,
            SlotEvent::Completed {
                turn_id: Some("old-turn".into()),
                operation_key: None,
            },
            Some(super::DeferredTurnCompletion {
                native_turn_id: Some("old-turn".into()),
                operation_key: None,
                status: crate::clients::TurnFinishStatus::Completed,
                text: "previous".into(),
                structured: None,
            }),
        );
        assert!(pre_submit.immediate_completion.is_some());

        // Mark submit as started (as SubmitSlot::submit does before calling the adapter).
        {
            let mut state = super::lock(&live.state);
            if let Phase::Submitting { submitted, .. } = &mut state.phase {
                *submitted = true;
            }
        }
        let early_match = live.record(
            2,
            None,
            SlotEvent::Completed {
                turn_id: Some("turn-42".into()),
                operation_key: None,
            },
            Some(super::DeferredTurnCompletion {
                native_turn_id: Some("turn-42".into()),
                operation_key: None,
                status: crate::clients::TurnFinishStatus::Completed,
                text: "done".into(),
                structured: None,
            }),
        );
        assert!(early_match.immediate_completion.is_none());
        let early_foreign = live.record(
            3,
            None,
            SlotEvent::Completed {
                turn_id: Some("other-turn".into()),
                operation_key: None,
            },
            Some(super::DeferredTurnCompletion {
                native_turn_id: Some("other-turn".into()),
                operation_key: None,
                status: crate::clients::TurnFinishStatus::Completed,
                text: "other".into(),
                structured: None,
            }),
        );
        assert!(early_foreign.immediate_completion.is_none());

        let plan = slot.apply_finish(
            &crate::clients::SubmitOutcome::Accepted {
                native_turn_id: Some("turn-42".into()),
            },
            false,
        );
        assert!(!plan.disconnect);
        assert_eq!(plan.deferred.len(), 2);
        assert_eq!(plan.deferred[0].operation_key.as_deref(), Some("op-early"));
        assert_eq!(plan.deferred[1].operation_key, None);
        Ok(())
    }

    #[tokio::test]
    async fn unknown_finish_and_dropped_started_slot_signal_disconnect() -> SessionTestResult {
        let live = live_session()?;
        let stop = live.event_stop();
        let slot = live
            .try_submit("op-unknown")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;
        slot.finish(
            &crate::clients::SubmitOutcome::Unknown("uncertain".into()),
            false,
        )
        .await;
        assert_eq!(live.state(), ParticipantState::Unknown);
        assert!(stop.has_changed()?);

        let live_drop = live_session()?;
        let stop_drop = live_drop.event_stop();
        let mut slot_drop = live_drop
            .try_submit("op-drop")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;
        let _ = slot_drop
            .submit(crate::clients::SubmitTurnRequest {
                operation_key: "op-drop".into(),
                message_id: None,
                input: test_turn_input()?,
                output_schema: None,
                steer: false,
                admission: cooplet_platform::AdmissionGuard::new(std::time::Duration::from_secs(5)),
            })
            .await;
        drop(slot_drop);
        assert_eq!(live_drop.state(), ParticipantState::Unknown);
        assert!(stop_drop.has_changed()?);
        Ok(())
    }

    #[test]
    fn pre_submit_events_do_not_pollute_early_events_for_run_or_busy() -> SessionTestResult {
        // 1. Pre-submit Working/Idle/anonymous Completed must not open an uncorrelated Run early
        //    or trigger early_event_limit.
        let live = live_session()?;
        let mut slot = live
            .try_submit("op-uncorrelated")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;
        for seq in 1..=300u64 {
            let outcome = live.record(seq, None, SlotEvent::Other, None);
            assert!(!outcome.early_limit_exceeded);
        }
        assert!(
            live.record(
                301,
                Some(ParticipantState::Working),
                SlotEvent::Working,
                None
            )
            .immediate_completion
            .is_none()
        );
        assert!(
            live.record(
                302,
                None,
                SlotEvent::Completed {
                    turn_id: None,
                    operation_key: None,
                },
                Some(super::DeferredTurnCompletion {
                    native_turn_id: None,
                    operation_key: None,
                    status: crate::clients::TurnFinishStatus::Completed,
                    text: "old".into(),
                    structured: None,
                }),
            )
            .immediate_completion
            .is_some()
        );
        live.record(303, Some(ParticipantState::Idle), SlotEvent::Idle, None);
        let plan = slot.apply_finish(
            &crate::clients::SubmitOutcome::Accepted {
                native_turn_id: None,
            },
            false,
        );
        assert!(!plan.disconnect);
        assert!(plan.deferred.is_empty());
        assert!(matches!(super::lock(&live.state).phase, Phase::Running(_)));

        // 2. Pre-submit Idle must not clear waiting_for_idle on a subsequent Busy ACK.
        let live_busy = live_session()?;
        let mut slot_busy = live_busy
            .try_submit("op-busy")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;
        live_busy.record(1, Some(ParticipantState::Idle), SlotEvent::Idle, None);
        let plan_busy = slot_busy.apply_finish(&crate::clients::SubmitOutcome::Busy, false);
        assert!(!plan_busy.disconnect);
        assert!(super::lock(&live_busy.state).waiting_for_idle);
        Ok(())
    }

    #[tokio::test]
    async fn submit_refreshes_submitted_after_seq_across_pre_submit_events() -> SessionTestResult {
        let live = live_session()?;
        let mut slot = live
            .try_submit("op-approval")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;
        assert_eq!(slot.submitted_after_seq(), 0);

        // A pre-submit runtime Idle event commits before SubmitSlot::submit runs.
        assert!(live.observe(1, true));
        live.record(1, Some(ParticipantState::Idle), SlotEvent::Idle, None);
        let _ = slot
            .submit(crate::clients::SubmitTurnRequest {
                operation_key: "op-approval".into(),
                message_id: None,
                input: test_turn_input()?,
                output_schema: None,
                steer: false,
                admission: cooplet_platform::AdmissionGuard::new(std::time::Duration::from_secs(5)),
            })
            .await;
        assert_eq!(slot.submitted_after_seq(), 1);

        let wait = crate::clients::ApprovalWait {
            approval_id: Some("apr-1".into()),
            kind: crate::clients::ApprovalWaitKind::Permission,
            summary: None,
            can_respond_remotely: true,
            action_hint: None,
        };
        let plan = slot.apply_finish(&crate::clients::SubmitOutcome::AwaitingApproval(wait), true);
        assert!(!plan.disconnect);
        assert_eq!(live.state(), ParticipantState::AwaitingApproval);

        // An announced (in-flight, not yet recorded) seq still fences the ACK.
        let live_inflight = live_session()?;
        let mut slot_inflight = live_inflight
            .try_submit("op-inflight")
            .map_err(|denied| format!("slot denied: {denied:?}"))?;
        assert!(live_inflight.observe(1, true));
        live_inflight.record(1, Some(ParticipantState::Idle), SlotEvent::Idle, None);
        assert!(live_inflight.observe(2, true));
        let _ = slot_inflight
            .submit(crate::clients::SubmitTurnRequest {
                operation_key: "op-inflight".into(),
                message_id: None,
                input: test_turn_input()?,
                output_schema: None,
                steer: false,
                admission: cooplet_platform::AdmissionGuard::new(std::time::Duration::from_secs(5)),
            })
            .await;
        assert_eq!(slot_inflight.submitted_after_seq(), 1);
        let wait_inflight = crate::clients::ApprovalWait {
            approval_id: Some("apr-2".into()),
            kind: crate::clients::ApprovalWaitKind::Permission,
            summary: None,
            can_respond_remotely: true,
            action_hint: None,
        };
        let _ = slot_inflight.apply_finish(
            &crate::clients::SubmitOutcome::AwaitingApproval(wait_inflight),
            true,
        );
        assert_eq!(live_inflight.state(), ParticipantState::Idle);
        Ok(())
    }
}
