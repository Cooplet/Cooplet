use serde_json::Value;
use tokio::sync::oneshot;

use crate::clients::{AdapterEventKind, ParticipantRuntimeState, SubmitOutcome, TurnFinishStatus};

use super::rpc::{Reply, State};

pub(super) struct Turn {
    pub(super) id: String,
    pub(super) wrote: bool,
    pub(super) accepted: bool,
    pub(super) cancel: Option<oneshot::Sender<TurnFinishStatus>>,
    operation_key: String,
    saw_start: bool,
    assistant: Option<TurnResult>,
    finished: Option<TurnResult>,
}

struct TurnResult {
    status: TurnFinishStatus,
    text: String,
}

impl Turn {
    pub(super) fn new(id: String, operation_key: String) -> Self {
        Self {
            id,
            wrote: false,
            accepted: false,
            cancel: None,
            operation_key,
            saw_start: false,
            assistant: None,
            finished: None,
        }
    }

    pub(super) fn observe(&mut self, kind: &str, frame: &Value) {
        if !self.wrote || self.finished.is_some() {
            return;
        }
        match kind {
            "agent_start" => {
                self.saw_start = true;
                self.assistant = None;
            }
            "message_end" if self.saw_start => {
                let message = &frame["message"];
                if message["role"] == "assistant" {
                    self.assistant = Some(final_assistant(message));
                }
            }
            "agent_settled" if self.accepted || self.saw_start || frame["aborted"] == true => {
                self.finished = Some(match (frame["aborted"].as_bool(), self.assistant.take()) {
                    (Some(true), _) => TurnResult {
                        status: TurnFinishStatus::Cancelled,
                        text: String::new(),
                    },
                    (Some(false), Some(assistant)) => assistant,
                    _ => TurnResult {
                        status: TurnFinishStatus::Failed,
                        text: String::new(),
                    },
                });
            }
            _ => {}
        }
    }
}

impl State {
    pub(super) fn handle_turn_event(&mut self, kind: &str, frame: &Value) {
        if let Some(turn) = self.turn.as_mut() {
            turn.observe(kind, frame);
            self.emit_finished();
        }
    }

    pub(super) fn accept_turn(&mut self, id: &str, reply: &Reply) -> SubmitOutcome {
        let Some(turn) = self.turn.as_mut().filter(|turn| turn.id == id) else {
            return SubmitOutcome::Unknown("Pi disconnected during intake".into());
        };
        if !reply.success
            && !turn.saw_start
            && reply.error.as_deref()
                == Some(
                    "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.",
                )
        {
            self.turn = None;
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
            self.emit(AdapterEventKind::StateChanged(
                ParticipantRuntimeState::Idle,
            ));
            if let Some(cancel) = turn.cancel {
                let _ = cancel.send(finished.status);
            }
        }
    }

    pub(super) fn runtime_state(&self) -> ParticipantRuntimeState {
        if self.is_closed() {
            ParticipantRuntimeState::Unknown {
                reason: "pi_rpc_closed".into(),
            }
        } else if self.turn.as_ref().is_some_and(|turn| turn.wrote) {
            ParticipantRuntimeState::Working
        } else {
            ParticipantRuntimeState::Idle
        }
    }

    pub(super) fn idle(&self) -> bool {
        !self.is_closed() && self.turn.is_none()
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
