use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
};

use serde_json::Value;
use tokio::sync::{mpsc, watch};

use super::approvals::{Approvals, NativeRequestId};
use crate::clients::{
    AdapterEvent, AdapterEventKind, ApprovalWait, ApprovalWaitKind,
    ParticipantRuntimeState as State, TurnFinishStatus,
};

const MAX_TURNS: usize = 16;
const RECENT_TURNS: usize = 128;
const MAX_TEXT_BYTES: usize = 1024 * 1024;
const MAX_DEFERRED: usize = 64;

pub(super) type ProtocolResult<T> = Result<T, &'static str>;

#[derive(Clone, Debug)]
pub(super) struct Snapshot {
    pub(super) state: State,
    pub(super) active_turn: Option<String>,
    pub(super) rollout_path: Option<PathBuf>,
    pub(super) route_revision: u64,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            state: State::Starting,
            active_turn: None,
            rollout_path: None,
            route_revision: 0,
        }
    }
}

pub(super) struct Observations {
    pub(super) thread_id: Option<String>,
    pub(super) approvals: Approvals,
    pub(super) snapshot: Snapshot,
    native_state: State,
    accepts_direct_input: bool,
    turns: BTreeMap<String, Turn>,
    completed: VecDeque<String>,
    submitted: Option<Submitted>,
    generation: u64,
    hint: String,
    events: mpsc::Sender<AdapterEvent>,
    snapshots: watch::Sender<Snapshot>,
    sequence: u64,
    deferred: Vec<Deferred>,
}

#[derive(Default)]
struct Turn {
    answer: Answer,
    schema: bool,
    foreign_input: bool,
}

#[derive(Default)]
struct Answer {
    final_text: Option<String>,
    unclassified: Option<String>,
}

struct Submitted {
    operation_key: String,
    schema: bool,
    colliding: bool,
}

enum Deferred {
    Event(AdapterEventKind),
    Completion(Completion),
}

struct Completion {
    turn_id: String,
    turn: Turn,
    status: TurnFinishStatus,
}

impl Observations {
    pub(super) fn new(
        generation: u64,
        hint: String,
        events: mpsc::Sender<AdapterEvent>,
        snapshots: watch::Sender<Snapshot>,
    ) -> Self {
        Self {
            generation,
            hint,
            events,
            snapshots,
            snapshot: Snapshot::default(),
            native_state: State::Starting,
            accepts_direct_input: true,
            thread_id: None,
            approvals: Approvals::default(),
            turns: BTreeMap::new(),
            completed: VecDeque::new(),
            submitted: None,
            sequence: 0,
            deferred: Vec::new(),
        }
    }

    pub(super) fn bind(&mut self, result: &Value) -> ProtocolResult<()> {
        let thread = &result["thread"];
        let id = native_id(&thread["id"])?;
        self.thread_id = Some(id.to_owned());
        self.snapshot.rollout_path = rollout_path(thread);
        self.accepts_direct_input = thread["canAcceptDirectInput"] != false;
        self.native_state = thread_state(thread, &self.hint);
        self.snapshot.state = self.native_state.clone();
        self.sync_snapshot();
        Ok(())
    }

    pub(super) fn refresh(&mut self, result: &Value) -> ProtocolResult<()> {
        let thread = &result["thread"];
        if thread["id"].as_str() != self.thread_id.as_deref() {
            return Err("thread_read_identity_mismatch");
        }
        self.snapshot.rollout_path = rollout_path(thread);
        self.sync_snapshot();
        // Inspection is a point-in-time result. The event stream still drives admission.
        Ok(())
    }

    pub(super) fn notify(&mut self, method: &str, params: &Value) -> ProtocolResult<()> {
        if matches!(
            method,
            "account/updated"
                | "account/login/completed"
                | "config/changed"
                | "modelProvider/updated"
        ) {
            self.snapshot.route_revision += 1;
            self.sync_snapshot();
            return Ok(());
        }
        let Some(thread_id) = self.thread_id.as_deref() else {
            return Ok(());
        };
        if params["threadId"].as_str() != Some(thread_id) {
            return Ok(());
        }
        match method {
            "thread/status/changed" => {
                self.native_state = native_state(&params["status"], &self.hint);
                if matches!(self.native_state, State::Unavailable { .. }) {
                    self.approvals.clear();
                }
                self.publish_state()
            }
            "turn/started" => self.started(native_id(&params["turn"]["id"])?),
            "turn/completed" => self.completed(&params["turn"]),
            "item/completed" => self.item(native_id(&params["turnId"])?, &params["item"]),
            "serverRequest/resolved" => {
                let id = serde_json::from_value(params["requestId"].clone())
                    .map_err(|_| "invalid_server_request_id")?;
                self.approvals.resolve(&id);
                self.publish_state()
            }
            _ => Ok(()),
        }
    }

    pub(super) fn request(
        &mut self,
        id: NativeRequestId,
        method: &str,
        params: &Value,
    ) -> ProtocolResult<()> {
        if self.thread_id.is_none() || params["threadId"].as_str() != self.thread_id.as_deref() {
            return Ok(());
        }
        // A thread-scoped request without a turn can still be observed via its status flag.
        let Ok(turn_id) = native_id(&params["turnId"]) else {
            return Ok(());
        };
        if self.is_completed(turn_id) {
            return Ok(());
        }
        self.approvals
            .push(self.generation, id, method, params, turn_id.to_owned())?;
        if self.snapshot.active_turn.is_none() {
            self.snapshot.active_turn = Some(turn_id.to_owned());
        }
        if self.native_state == State::Idle {
            self.native_state = State::Working;
        }
        self.sync_snapshot();
        self.publish_state()
    }

    pub(super) fn submission_pending(&self) -> bool {
        self.submitted.is_some()
    }

    pub(super) fn begin_submit(&mut self, operation_key: String, schema: bool) {
        self.submitted = Some(Submitted {
            operation_key,
            schema,
            colliding: self.snapshot.active_turn.is_some(),
        });
    }

    pub(super) fn acknowledge(&mut self, turn_id: Option<&str>) -> ProtocolResult<()> {
        let Some(submitted) = self.submitted.take() else {
            return Err("unexpected_turn_response");
        };
        if let Some(id) = turn_id {
            let active = self.snapshot.active_turn.as_deref();
            let colliding = submitted.colliding || active.is_some_and(|active| active != id);
            let mut foreign_input = false;
            if !self.is_completed(id) {
                let turn = self.turn(id)?;
                turn.schema = submitted.schema;
                foreign_input = turn.foreign_input;
                if self.snapshot.active_turn.is_none() {
                    self.started(id)?;
                }
            }
            for deferred in &mut self.deferred {
                if let Deferred::Completion(completion) = deferred
                    && completion.turn_id == id
                {
                    completion.turn.schema = submitted.schema;
                    foreign_input |= completion.turn.foreign_input;
                }
            }
            if colliding || foreign_input {
                tracing::warn!(
                    generation = self.generation,
                    "Codex acknowledged a turn with concurrent input"
                );
            }
        }
        let deferred = std::mem::take(&mut self.deferred);
        for entry in deferred {
            match entry {
                Deferred::Event(event) => self.emit(event)?,
                Deferred::Completion(completion) => self.emit_completion(completion)?,
            }
        }
        Ok(())
    }

    pub(super) fn publish_state(&mut self) -> ProtocolResult<()> {
        let state = self.with_approval(self.native_state.clone());
        if state == self.snapshot.state {
            return Ok(());
        }
        self.snapshot.state = state.clone();
        self.sync_snapshot();
        let event = match state {
            State::AwaitingApproval(ref wait) if wait.can_respond_remotely => {
                AdapterEventKind::ApprovalRequested(wait.clone())
            }
            _ => AdapterEventKind::StateChanged(state),
        };
        if self.deferred.is_empty() {
            self.emit(event)
        } else {
            self.defer(Deferred::Event(event))
        }
    }

    pub(super) fn sync_snapshot(&self) {
        self.snapshots.send_replace(self.snapshot.clone());
    }

    pub(super) fn closed(&mut self, reason: &'static str) {
        self.snapshot.state = State::Unknown {
            reason: reason.into(),
        };
        self.sync_snapshot();
    }

    fn with_approval(&self, state: State) -> State {
        if !self.accepts_direct_input {
            return direct_input_unavailable();
        }
        if matches!(state, State::Unavailable { .. } | State::Idle) {
            return state;
        }
        self.approvals.current().map_or(state, |current| {
            State::AwaitingApproval(current.wait(&self.hint))
        })
    }

    fn started(&mut self, turn_id: &str) -> ProtocolResult<()> {
        if self.is_completed(turn_id) {
            return Ok(());
        }
        self.turn(turn_id)?;
        self.snapshot.active_turn = Some(turn_id.to_owned());
        self.sync_snapshot();
        if matches!(self.native_state, State::Idle | State::Starting) {
            self.native_state = State::Working;
        }
        self.publish_state()
    }

    fn item(&mut self, turn_id: &str, item: &Value) -> ProtocolResult<()> {
        if self.is_completed(turn_id) {
            return Ok(());
        }
        let foreign_input = self.submitted.as_ref().is_some_and(|submitted| {
            item["type"] == "userMessage"
                && item["clientId"]
                    .as_str()
                    .is_some_and(|id| id != submitted.operation_key)
        });
        if foreign_input {
            self.turn(turn_id)?.foreign_input = true;
        }
        if item["type"] == "agentMessage" {
            self.turn(turn_id)?.answer.record(item)?;
        }
        Ok(())
    }

    fn completed(&mut self, turn: &Value) -> ProtocolResult<()> {
        let id = native_id(&turn["id"])?;
        if self.is_completed(id) {
            return Ok(());
        }
        let mut track = self.turns.remove(id).unwrap_or_default();
        if let Some(items) = turn["items"].as_array() {
            for item in items {
                track.answer.record(item)?;
            }
        }
        let status = match turn["status"].as_str() {
            Some("completed") => TurnFinishStatus::Completed,
            Some("failed") => TurnFinishStatus::Failed,
            Some("interrupted") => TurnFinishStatus::Cancelled,
            _ => return Err("invalid_turn_completion"),
        };
        self.completed.push_back(id.to_owned());
        if self.completed.len() > RECENT_TURNS {
            self.completed.pop_front();
        }
        let completion = Completion {
            turn_id: id.to_owned(),
            turn: track,
            status,
        };
        if self.submitted.is_some() {
            self.defer(Deferred::Completion(completion))?;
        } else {
            self.emit_completion(completion)?;
        }
        self.approvals.clear_turn(id);
        if self.snapshot.active_turn.as_deref() == Some(id) {
            self.snapshot.active_turn = None;
            if !matches!(self.native_state, State::Unavailable { .. }) {
                self.native_state = State::Idle;
            }
        }
        self.sync_snapshot();
        self.publish_state()
    }

    fn turn(&mut self, id: &str) -> ProtocolResult<&mut Turn> {
        if self.turns.len() >= MAX_TURNS && !self.turns.contains_key(id) {
            return Err("native_turn_limit");
        }
        Ok(self.turns.entry(id.to_owned()).or_default())
    }

    fn is_completed(&self, id: &str) -> bool {
        self.completed.iter().any(|entry| entry == id)
    }

    fn defer(&mut self, event: Deferred) -> ProtocolResult<()> {
        if self.deferred.len() >= MAX_DEFERRED {
            return Err("early_turn_event_limit");
        }
        self.deferred.push(event);
        Ok(())
    }

    fn emit_completion(&mut self, completion: Completion) -> ProtocolResult<()> {
        let text = completion
            .turn
            .answer
            .final_text
            .or(completion.turn.answer.unclassified)
            .unwrap_or_default();
        let mut status = completion.status;
        let structured = if status == TurnFinishStatus::Completed && completion.turn.schema {
            match serde_json::from_str(&text) {
                Ok(value) => Some(value),
                Err(_) => {
                    tracing::warn!(
                        generation = self.generation,
                        "Codex returned invalid structured output"
                    );
                    status = TurnFinishStatus::Failed;
                    None
                }
            }
        } else {
            None
        };
        self.emit(AdapterEventKind::TurnCompleted {
            native_turn_id: Some(completion.turn_id),
            operation_key: None,
            status,
            text,
            structured,
        })
    }

    fn emit(&mut self, kind: AdapterEventKind) -> ProtocolResult<()> {
        self.sequence += 1;
        self.events
            .try_send(AdapterEvent {
                event_seq: self.sequence,
                kind,
            })
            .map_err(|_| "codex_event_stream_overflow_or_closed")
    }
}

impl Answer {
    fn record(&mut self, item: &Value) -> ProtocolResult<()> {
        if item["type"] != "agentMessage" {
            return Ok(());
        }
        let slot = match item["phase"].as_str() {
            Some("final_answer") => &mut self.final_text,
            None if item["phase"].is_null() => &mut self.unclassified,
            _ => return Ok(()),
        };
        let text = item["text"]
            .as_str()
            .ok_or("invalid_native_agent_message")?;
        if text.len() > MAX_TEXT_BYTES {
            return Err("native_answer_limit");
        }
        *slot = Some(text.to_owned());
        Ok(())
    }
}

pub(super) fn native_state(status: &Value, hint: &str) -> State {
    let flags = status["activeFlags"].as_array();
    let has = |flag: &str| flags.is_some_and(|flags| flags.iter().any(|value| value == flag));
    let wait = |kind| {
        State::AwaitingApproval(ApprovalWait {
            approval_id: None,
            kind,
            summary: None,
            can_respond_remotely: false,
            action_hint: Some(hint.to_owned()),
        })
    };
    match status["type"].as_str() {
        Some("active") if has("waitingOnApproval") => wait(ApprovalWaitKind::Permission),
        Some("active") if has("waitingOnUserInput") => wait(ApprovalWaitKind::Question),
        _ if status["canAcceptDirectInput"] == false => State::Unavailable {
            reason: "Codex is not accepting direct input".into(),
        },
        Some("active") => State::Working,
        Some("idle") => State::Idle,
        _ => State::Unavailable {
            reason: "Codex thread is unavailable".into(),
        },
    }
}

pub(super) fn thread_state(thread: &Value, hint: &str) -> State {
    if thread["canAcceptDirectInput"] == false {
        direct_input_unavailable()
    } else {
        native_state(&thread["status"], hint)
    }
}

fn direct_input_unavailable() -> State {
    State::Unavailable {
        reason: "Codex thread is not accepting direct input".into(),
    }
}

pub(super) fn native_id(value: &Value) -> ProtocolResult<&str> {
    value
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .ok_or("invalid_codex_identifier")
}

fn rollout_path(thread: &Value) -> Option<PathBuf> {
    thread["path"]
        .as_str()
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}
