use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::oneshot;

use crate::clients::{AdapterEventKind, ParticipantRuntimeState, SubmitOutcome, TurnFinishStatus};

use super::rpc::{Reply, State};

pub(super) const NATIVE_BUSY_ERROR: &str = "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.";
pub(super) const COMPACTION_BUSY_ERROR: &str = "Cannot submit a prompt while compaction is in progress. Wait for compaction to finish and retry.";

#[derive(Default)]
pub(super) struct NativeActivity {
    agent: AgentActivity,
    manual_compaction: bool,
}

#[derive(Default)]
enum AgentActivity {
    #[default]
    Idle,
    BusyBeforeStart,
    Running {
        ended: bool,
        retry: bool,
    },
    CheckingContinuation,
}

impl NativeActivity {
    pub(super) fn is_busy(&self) -> bool {
        self.manual_compaction || !matches!(self.agent, AgentActivity::Idle)
    }

    fn observe(&mut self, kind: &str, frame: &Value) -> bool {
        match kind {
            // A fresh prompt always projects its input before an assistant starts.
            // A low-level continuation can instead start directly with an assistant.
            "agent_start" => {
                self.agent = match self.agent {
                    AgentActivity::Running {
                        ended: true,
                        retry: false,
                    } => AgentActivity::CheckingContinuation,
                    AgentActivity::Running { retry: false, .. }
                    | AgentActivity::CheckingContinuation => return false,
                    _ => AgentActivity::Running {
                        ended: false,
                        retry: false,
                    },
                };
            }
            "message_start" if matches!(self.agent, AgentActivity::CheckingContinuation) => {
                if frame["message"]["role"] != "assistant" {
                    return false;
                }
                self.agent = AgentActivity::Running {
                    ended: false,
                    retry: false,
                };
            }
            "agent_end" | "agent_settled"
                if matches!(self.agent, AgentActivity::CheckingContinuation) =>
            {
                return false;
            }
            "agent_end" => {
                if let AgentActivity::Running { ended, .. } = &mut self.agent {
                    *ended = true;
                }
            }
            "agent_settled" => self.agent = AgentActivity::Idle,
            "compaction_start" if frame["reason"] == "manual" => {
                self.manual_compaction = true;
            }
            "compaction_end" if frame["reason"] == "manual" => {
                self.manual_compaction = false;
            }
            "auto_retry_start" | "auto_retry_end" | "compaction_end" => {
                if let AgentActivity::Running { retry, .. } = &mut self.agent {
                    *retry = kind == "auto_retry_start"
                        || (kind == "compaction_end" && frame["willRetry"] == true);
                }
            }
            _ => {}
        }
        true
    }

    fn acknowledge_busy(&mut self, error: &str, turn: &Turn) -> bool {
        match error {
            NATIVE_BUSY_ERROR => {
                if !turn.saw_native_settle && matches!(self.agent, AgentActivity::Idle) {
                    self.agent = AgentActivity::BusyBeforeStart;
                }
                true
            }
            // Pi also uses this error for branch navigation. Only manual
            // compaction has an observable settlement to release our intake.
            COMPACTION_BUSY_ERROR => self.manual_compaction || turn.saw_manual_compaction_end,
            _ => false,
        }
    }
}

pub(super) struct Turn {
    pub(super) id: String,
    pub(super) wrote: bool,
    pub(super) accepted: bool,
    pub(super) awaiting_state_check: bool,
    pub(super) cancel: Option<oneshot::Sender<TurnFinishStatus>>,
    operation_key: String,
    input: [u8; 32],
    run: NativeRun,
    saw_managed_user: bool,
    saw_native_settle: bool,
    saw_manual_compaction_end: bool,
    assistant: Option<TurnResult>,
    finished: Option<TurnResult>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeRun {
    Waiting,
    Unclaimed { conflicting: bool },
    Managed,
}

pub(super) struct TurnResult {
    status: TurnFinishStatus,
    text: String,
}

impl Turn {
    pub(super) fn new(id: String, operation_key: String, message: &str) -> Self {
        Self {
            id,
            wrote: false,
            accepted: false,
            awaiting_state_check: false,
            cancel: None,
            operation_key,
            input: Sha256::digest(message.as_bytes()).into(),
            run: NativeRun::Waiting,
            saw_managed_user: false,
            saw_native_settle: false,
            saw_manual_compaction_end: false,
            assistant: None,
            finished: None,
        }
    }

    /// Returns false when the native stream no longer identifies the managed run.
    fn observe(&mut self, kind: &str, frame: &Value) -> bool {
        if !self.wrote {
            return true;
        }
        if self.finished.is_some() {
            // Several native runs crossed one ACK. An idle snapshot cannot
            // establish which of their results belongs to this operation.
            return kind != "agent_start";
        }
        match kind {
            "agent_start" => return self.begin(),
            "message_start" if frame["message"]["role"] == "user" => {
                return self.observe_user(&frame["message"]["content"]);
            }
            "message_end"
                if self.run == NativeRun::Managed && frame["message"]["role"] == "assistant" =>
            {
                self.assistant = Some(final_assistant(&frame["message"]));
            }
            "compaction_end" if frame["reason"] == "manual" => {
                self.saw_manual_compaction_end = true;
            }
            "agent_settled" => {
                self.saw_native_settle = true;
                match self.run {
                    NativeRun::Managed => self.finish(frame),
                    NativeRun::Waiting if self.accepted => self.finish(frame),
                    NativeRun::Unclaimed { .. } if self.accepted => return false,
                    _ => self.run = NativeRun::Waiting,
                }
            }
            _ => {}
        }
        true
    }

    fn begin(&mut self) -> bool {
        if self.run == NativeRun::Waiting && self.accepted {
            self.awaiting_state_check = false;
        }
        self.run = match self.run {
            NativeRun::Waiting if self.accepted => NativeRun::Managed,
            NativeRun::Waiting => NativeRun::Unclaimed { conflicting: false },
            NativeRun::Unclaimed { .. } if !self.accepted => {
                NativeRun::Unclaimed { conflicting: true }
            }
            NativeRun::Managed => NativeRun::Managed,
            _ => return false,
        };
        self.assistant = None;
        true
    }

    fn observe_user(&mut self, content: &Value) -> bool {
        // A started ACK followed by an unambiguous new run identifies that
        // run even if a trusted Pi input hook transformed the user text.
        if self.run == NativeRun::Managed && !self.saw_managed_user {
            self.saw_managed_user = true;
            return true;
        }
        let matches = user_input_digest(content) == Some(self.input);
        match self.run {
            NativeRun::Managed => matches,
            NativeRun::Unclaimed { conflicting: true } if matches => false,
            _ => {
                self.run = if matches {
                    self.saw_managed_user = true;
                    NativeRun::Managed
                } else {
                    NativeRun::Unclaimed { conflicting: true }
                };
                true
            }
        }
    }

    fn finish(&mut self, frame: &Value) {
        self.finished = Some(match frame.get("aborted") {
            Some(Value::Bool(true)) => TurnResult {
                status: TurnFinishStatus::Cancelled,
                text: String::new(),
            },
            // Older Pi reports cancellation only via the current assistant stop reason.
            None | Some(Value::Bool(false)) => self.assistant.take().unwrap_or_else(failed),
            _ => failed(),
        });
    }
}

impl State {
    pub(super) fn handle_turn_event(&mut self, kind: &str, frame: &Value) {
        let was_busy = self.native_activity.is_busy();
        if !self.native_activity.observe(kind, frame) {
            self.close_ambiguous_stream();
            return;
        }
        if let Some(turn) = self.turn.as_mut().filter(|turn| turn.wrote) {
            if !turn.observe(kind, frame) {
                self.close_ambiguous_stream();
            }
        } else if was_busy != self.native_activity.is_busy() {
            self.emit_native_state();
        }
    }

    pub(super) fn accept_turn(&mut self, id: &str, reply: &Reply) -> SubmitOutcome {
        let Some(turn) = self.turn.as_mut().filter(|turn| turn.id == id) else {
            return SubmitOutcome::Unknown("Pi disconnected during intake".into());
        };
        if !reply.success
            && turn.run != NativeRun::Managed
            && reply
                .error
                .as_deref()
                .is_some_and(|error| self.native_activity.acknowledge_busy(error, turn))
        {
            // Releasing the Node slot on Busy requires an Idle event even if it
            // already arrived on the native stream while the ACK was pending.
            self.turn = None;
            self.emit_native_state();
            return SubmitOutcome::Busy;
        }
        if !reply.success || reply.data["disposition"] != "started" {
            self.close();
            return SubmitOutcome::Unknown("Pi did not confirm that the turn started".into());
        }
        // A native input hook may run the exact same text in a separate prompt.
        // Even an exact echo does not prove that a pre-ACK result belongs to us.
        turn.awaiting_state_check = turn.run == NativeRun::Managed;
        turn.accepted = true;
        self.emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ));
        SubmitOutcome::Accepted {
            native_turn_id: None,
        }
    }

    pub(super) fn reconcile_early(
        &mut self,
        id: &str,
        result: TurnResult,
        streaming: Option<bool>,
    ) {
        let Some(turn) = self
            .turn
            .as_mut()
            .filter(|turn| turn.id == id && turn.awaiting_state_check)
        else {
            // New native activity already superseded this early result.
            return;
        };
        turn.awaiting_state_check = false;
        match streaming {
            Some(false)
                if turn.run == NativeRun::Waiting
                    && matches!(self.native_activity.agent, AgentActivity::Idle) =>
            {
                self.emit_finished(result);
            }
            Some(_) => {} // Keep collecting the managed run after its ACK.
            None => self.close(),
        }
    }

    pub(super) fn finish_or_check(&mut self) -> Option<(String, TurnResult)> {
        let turn = self.turn.as_mut().filter(|turn| turn.accepted)?;
        let result = turn.finished.take()?;
        if turn.awaiting_state_check {
            turn.run = NativeRun::Waiting;
            turn.saw_managed_user = false;
            turn.assistant = None;
            return Some((turn.id.clone(), result));
        }
        self.emit_finished(result);
        None
    }

    fn emit_finished(&mut self, finished: TurnResult) {
        if let Some(turn) = self.turn.take() {
            self.emit(AdapterEventKind::TurnCompleted {
                operation_key: Some(turn.operation_key),
                native_turn_id: None,
                status: finished.status,
                text: finished.text,
                structured: None,
            });
            self.emit_native_state();
            if let Some(cancel) = turn.cancel {
                let _ = cancel.send(finished.status);
            }
        }
    }

    fn emit_native_state(&mut self) {
        let state = if self.native_activity.is_busy() {
            ParticipantRuntimeState::Working
        } else {
            ParticipantRuntimeState::Idle
        };
        self.emit(AdapterEventKind::StateChanged(state));
    }

    fn close_ambiguous_stream(&mut self) {
        tracing::warn!("Could not correlate overlapping native Pi runs");
        self.close();
    }

    pub(super) fn runtime_state(&self) -> ParticipantRuntimeState {
        if self.is_closed() {
            ParticipantRuntimeState::Unknown {
                reason: "pi_rpc_closed".into(),
            }
        } else if self.native_activity.is_busy()
            || self.turn.as_ref().is_some_and(|turn| turn.wrote)
        {
            ParticipantRuntimeState::Working
        } else {
            ParticipantRuntimeState::Idle
        }
    }

    pub(super) fn idle(&self) -> bool {
        !self.is_closed() && !self.native_activity.is_busy() && self.turn.is_none()
    }
}

fn user_input_digest(content: &Value) -> Option<[u8; 32]> {
    let mut digest = Sha256::new();
    if let Some(text) = content.as_str() {
        digest.update(text.as_bytes());
    } else {
        for part in content.as_array()? {
            if part["type"] != "text" {
                return None;
            }
            digest.update(part["text"].as_str()?.as_bytes());
        }
    }
    Some(digest.finalize().into())
}

fn failed() -> TurnResult {
    TurnResult {
        status: TurnFinishStatus::Failed,
        text: String::new(),
    }
}

fn final_assistant(message: &Value) -> TurnResult {
    let status = match message["stopReason"].as_str() {
        Some("stop" | "length") => TurnFinishStatus::Completed,
        Some("aborted") => TurnFinishStatus::Cancelled,
        _ => TurnFinishStatus::Failed,
    };
    let text = if status == TurnFinishStatus::Completed {
        message["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|block| block["type"] == "text")
            .filter_map(|block| block["text"].as_str())
            .collect()
    } else {
        String::new()
    };
    TurnResult { status, text }
}
