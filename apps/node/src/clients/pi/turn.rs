use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::oneshot;

use crate::clients::{AdapterEventKind, ParticipantRuntimeState, SubmitOutcome, TurnFinishStatus};

use super::rpc::{Reply, State};

pub(super) const NATIVE_BUSY_ERROR: &str = "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.";

pub(super) struct Turn {
    pub(super) id: String,
    pub(super) wrote: bool,
    pub(super) accepted: bool,
    pub(super) cancel: Option<oneshot::Sender<TurnFinishStatus>>,
    operation_key: String,
    input: [u8; 32],
    saw_input: bool,
    saw_native_settle: bool,
    assistant: Option<TurnResult>,
    finished: Option<TurnResult>,
}

struct TurnResult {
    status: TurnFinishStatus,
    text: String,
}

impl Turn {
    pub(super) fn new(id: String, operation_key: String, message: &str) -> Self {
        Self {
            id,
            wrote: false,
            accepted: false,
            cancel: None,
            operation_key,
            input: Sha256::digest(message.as_bytes()).into(),
            saw_input: false,
            saw_native_settle: false,
            assistant: None,
            finished: None,
        }
    }

    fn observe(&mut self, kind: &str, frame: &Value) {
        if !self.wrote || self.finished.is_some() {
            return;
        }
        match kind {
            // AgentSession can retry or continue without emitting the input a second time.
            "agent_start" => self.assistant = None,
            "message_start" if !self.saw_input && frame["message"]["role"] == "user" => {
                self.saw_input =
                    user_input_digest(&frame["message"]["content"]) == Some(self.input);
            }
            "message_end" if self.saw_input && frame["message"]["role"] == "assistant" => {
                self.assistant = Some(final_assistant(&frame["message"]));
            }
            "agent_settled" => {
                self.saw_native_settle = true;
                // Pi extensions can start and finish their own run during prompt preflight.
                // Only a run that echoed our input can complete the operation before its ACK.
                if self.saw_input || self.accepted {
                    let aborted = frame.get("aborted");
                    self.finished = Some(match aborted {
                        Some(Value::Bool(true)) => TurnResult {
                            status: TurnFinishStatus::Cancelled,
                            text: String::new(),
                        },
                        // Older Pi reports cancellation only via the current assistant stop reason.
                        None | Some(Value::Bool(false)) => {
                            self.assistant.take().unwrap_or_else(failed)
                        }
                        _ => failed(),
                    });
                }
            }
            _ => {}
        }
    }
}

impl State {
    pub(super) fn handle_turn_event(&mut self, kind: &str, frame: &Value) {
        let was_busy = self.native_busy;
        match kind {
            "agent_start" => self.native_busy = true,
            "agent_settled" => self.native_busy = false,
            _ => {}
        }
        if let Some(turn) = self.turn.as_mut().filter(|turn| turn.wrote) {
            turn.observe(kind, frame);
            self.emit_finished();
        } else if was_busy != self.native_busy {
            self.emit_native_state();
        }
    }

    pub(super) fn accept_turn(&mut self, id: &str, reply: &Reply) -> SubmitOutcome {
        let Some(turn) = self.turn.as_mut().filter(|turn| turn.id == id) else {
            return SubmitOutcome::Unknown("Pi disconnected during intake".into());
        };
        if !reply.success && !turn.saw_input && reply.error.as_deref() == Some(NATIVE_BUSY_ERROR) {
            // Releasing the Node slot on Busy requires an Idle event even if it
            // already arrived on the native stream while the ACK was pending.
            self.native_busy = self.native_busy || !turn.saw_native_settle;
            self.turn = None;
            self.emit_native_state();
            return SubmitOutcome::Busy;
        }
        if !reply.success || reply.data["disposition"] != "started" {
            self.close();
            return SubmitOutcome::Unknown("Pi did not confirm that the turn started".into());
        }
        turn.accepted = true;
        self.emit(AdapterEventKind::StateChanged(
            ParticipantRuntimeState::Working,
        ));
        self.emit_finished();
        SubmitOutcome::Accepted {
            native_turn_id: None,
        }
    }

    fn emit_finished(&mut self) {
        if let Some(turn) = self.turn.as_mut()
            && turn.accepted
            && let Some(finished) = turn.finished.take()
            && let Some(turn) = self.turn.take()
        {
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
        let state = if self.native_busy {
            ParticipantRuntimeState::Working
        } else {
            ParticipantRuntimeState::Idle
        };
        self.emit(AdapterEventKind::StateChanged(state));
    }

    pub(super) fn runtime_state(&self) -> ParticipantRuntimeState {
        if self.is_closed() {
            ParticipantRuntimeState::Unknown {
                reason: "pi_rpc_closed".into(),
            }
        } else if self.native_busy || self.turn.as_ref().is_some_and(|turn| turn.wrote) {
            ParticipantRuntimeState::Working
        } else {
            ParticipantRuntimeState::Idle
        }
    }

    pub(super) fn idle(&self) -> bool {
        !self.is_closed() && !self.native_busy && self.turn.is_none()
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
