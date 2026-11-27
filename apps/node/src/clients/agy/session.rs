use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, sleep_until, timeout},
};

use super::{
    acp_wire::{Admission, Fault, Frame, ReceivedFrame, Reply, Wire, WriteFailure},
    inbound::InboundDuringWrite,
};
use crate::clients::{
    AdapterEvent, AdapterEventKind, ParticipantRuntimeState, StopOutcome, SubmitOutcome,
    SubmitTurnRequest, TurnFinishStatus,
};

const MAX_TURN_TEXT_BYTES: usize = 2 * 1024 * 1024;
const CANCEL_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) struct Session {
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<Observation>,
    submitters: watch::Sender<()>,
    stopping: watch::Sender<bool>,
    close_writer: Arc<AtomicBool>,
}

struct Observation {
    runtime: ParticipantRuntimeState,
    prompt_id: Option<u64>,
}

impl Session {
    pub(super) fn start(
        wire: Wire,
        session_id: String,
        fail_fast_window: Duration,
    ) -> (Self, mpsc::Receiver<AdapterEvent>) {
        let (commands, mut requests) = mpsc::channel(8);
        let (events, event_stream) = mpsc::channel(16);
        let (state, observation) = watch::channel(Observation {
            runtime: ParticipantRuntimeState::Idle,
            prompt_id: None,
        });
        let (submitters, _) = watch::channel(());
        let active_submits = submitters.clone();
        let (stopping, mut stop) = watch::channel(false);
        let close_writer = wire.close_signal();
        let mut actor = Actor {
            session_id,
            events,
            state,
            event_seq: 0,
            turn: None,
            warned_unexpected_notification: false,
            fail_fast_window,
        };
        tokio::spawn(async move {
            let mut wire = wire;
            let stopping = stop.wait_for(|stopped| *stopped);
            tokio::pin!(stopping);
            let mut fault = tokio::select! {
                biased;
                _ = &mut stopping => None,
                fault = actor.run(&mut wire, &mut requests) => Some(fault),
            };
            requests.close();
            while let Ok(request) = requests.try_recv() {
                request.transport_closed();
            }
            drop(requests);
            // A fault must keep its pipes open until submit exits. An
            // explicit stop can take over as long as we still own the wire.
            if fault.is_some() {
                tokio::select! {
                    biased;
                    _ = &mut stopping => fault = None,
                    () = active_submits.closed() => {}
                }
            }
            drop(wire);
            // Even an explicit stop releases the event stream after submit.
            active_submits.closed().await;
            actor.finish(fault).await;
        });
        (
            Self {
                commands,
                state: observation,
                submitters,
                stopping,
                close_writer,
            },
            event_stream,
        )
    }

    pub(super) fn state(&self) -> ParticipantRuntimeState {
        let state = self.state.borrow().runtime.clone();
        if self.is_closed() && !matches!(state, ParticipantRuntimeState::Unknown { .. }) {
            ParticipantRuntimeState::Unknown {
                reason: "Agy ACP transport is closed".into(),
            }
        } else {
            state
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        *self.stopping.borrow() || self.commands.is_closed()
    }

    pub(super) async fn submit(&self, request: SubmitTurnRequest) -> SubmitOutcome {
        let closed = closed_submit;
        if self.is_closed() {
            return closed();
        }
        let place = match self.commands.try_reserve() {
            Ok(place) => place,
            Err(mpsc::error::TrySendError::Closed(())) => return closed(),
            Err(mpsc::error::TrySendError::Full(())) => {
                let Some(ms) = request.admission.remaining_ms() else {
                    return SubmitOutcome::Expired;
                };
                match timeout(Duration::from_millis(ms), self.commands.reserve()).await {
                    Ok(Ok(place)) => place,
                    Ok(Err(_)) => return closed(),
                    Err(_) => return SubmitOutcome::Expired,
                }
            }
        };
        let (reply, outcome) = oneshot::channel();
        // Keep transport teardown behind every submitted call until its caller
        // observes the outcome or drops the future.
        let _active_submit = self.submitters.subscribe();
        // The writer separately checks whether this particular caller has left.
        let (_submit_scope, exit) = watch::channel(());
        place.send(Command::Submit {
            request: Box::new(request),
            exit,
            reply,
        });
        outcome.await.unwrap_or_else(|_| {
            SubmitOutcome::Unknown("Agy ACP handoff outcome is unconfirmed".into())
        })
    }

    pub(super) async fn cancel(&self) -> StopOutcome {
        if self.is_closed() {
            return StopOutcome::AlreadyGone;
        }
        let Some(prompt_id) = self.state.borrow().prompt_id else {
            return StopOutcome::TurnCancelled;
        };
        let (reply, result) = oneshot::channel();
        match timeout(CANCEL_TIMEOUT, async {
            if self
                .commands
                .send(Command::Cancel { prompt_id, reply })
                .await
                .is_err()
            {
                return StopOutcome::AlreadyGone;
            }
            result.await.unwrap_or_else(|_| {
                StopOutcome::Unknown("Agy ACP transport closed during cancellation".into())
            })
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => StopOutcome::Unknown("Agy turn cancellation timed out".into()),
        }
    }

    pub(super) fn stop(&self) -> StopOutcome {
        self.stopping.send_replace(true);
        self.close_writer.store(true, Ordering::Release);
        StopOutcome::Stopped
    }
}

enum Command {
    Submit {
        request: Box<SubmitTurnRequest>,
        exit: watch::Receiver<()>,
        reply: oneshot::Sender<SubmitOutcome>,
    },
    Cancel {
        prompt_id: u64,
        reply: oneshot::Sender<StopOutcome>,
    },
}

impl Command {
    fn transport_closed(self) {
        match self {
            Self::Submit { reply, .. } => {
                let _ = reply.send(closed_submit());
            }
            Self::Cancel { reply, .. } => {
                let _ = reply.send(StopOutcome::AlreadyGone);
            }
        }
    }
}

fn closed_submit() -> SubmitOutcome {
    SubmitOutcome::Unavailable("Agy ACP transport is closed".into())
}

struct Actor {
    session_id: String,
    events: mpsc::Sender<AdapterEvent>,
    state: watch::Sender<Observation>,
    event_seq: u64,
    turn: Option<Turn>,
    warned_unexpected_notification: bool,
    fail_fast_window: Duration,
}

struct Turn {
    id: u64,
    first_write: Arc<OnceLock<std::time::Instant>>,
    operation_key: String,
    text: String,
    cancellation: Option<oneshot::Sender<StopOutcome>>,
}

impl Actor {
    async fn run(
        &mut self,
        wire: &mut Wire,
        commands: &mut mpsc::Receiver<Command>,
    ) -> Result<(), Fault> {
        loop {
            tokio::select! {
                biased;
                _ = self.events.closed() => return Err(Fault::EventReceiverClosed),
                frame = wire.recv() => {
                    if let Some(terminal) = self.receive(wire, frame?).await? {
                        self.complete(terminal).await?;
                    }
                }
                command = commands.recv() => match command {
                    Some(Command::Submit { request, exit, reply }) => self.submit(wire, *request, exit, reply).await?,
                    Some(Command::Cancel { prompt_id, reply }) => self.cancel(wire, prompt_id, reply).await?,
                    None => return Ok(()),
                },
            }
        }
    }

    async fn submit(
        &mut self,
        wire: &mut Wire,
        request: SubmitTurnRequest,
        exit: watch::Receiver<()>,
        reply: oneshot::Sender<SubmitOutcome>,
    ) -> Result<(), Fault> {
        let rejection = if request.output_schema.is_some() {
            Some(SubmitOutcome::Unavailable(
                "Agy does not support structured output".into(),
            ))
        } else if self.turn.is_some() {
            Some(SubmitOutcome::Busy)
        } else if request.steer {
            Some(SubmitOutcome::Unavailable(
                "Agy does not support steering".into(),
            ))
        } else {
            None
        };
        if let Some(outcome) = rejection {
            let _ = reply.send(outcome);
            return Ok(());
        }
        let Ok(text) = request.input.render_canonical() else {
            let outcome = if request.admission.is_expired() {
                SubmitOutcome::Expired
            } else {
                SubmitOutcome::Unavailable("Could not render Agy turn input".into())
            };
            let _ = reply.send(outcome);
            return Ok(());
        };
        let mut reply = Some(reply);
        let result = self.handoff(wire, request, text, exit, &mut reply).await;
        if let Err(fault) = result
            && let Some(reply) = reply
        {
            let _ = reply.send(SubmitOutcome::Unknown(fault.detail().into()));
        }
        result
    }

    async fn handoff(
        &mut self,
        wire: &mut Wire,
        request: SubmitTurnRequest,
        text: String,
        exit: watch::Receiver<()>,
        reply: &mut Option<oneshot::Sender<SubmitOutcome>>,
    ) -> Result<(), Fault> {
        let (id, frame) = wire.request(
            "session/prompt",
            json!({
                "sessionId": self.session_id,
                "prompt": [{"type": "text", "text": text}]
            }),
        );
        // A concurrent cancel must keep the identity of the prompt it observed,
        // including while the writer and the fail-fast window are still active.
        self.state.send_modify(|state| state.prompt_id = Some(id));
        let mut inbox = InboundDuringWrite::new(id);
        let (written, read_fault) = match wire.begin_write(
            frame,
            Some(Admission {
                guard: request.admission,
                submitting: exit,
            }),
        ) {
            Ok(pending) => {
                self.turn = Some(Turn {
                    id,
                    first_write: pending.started.clone(),
                    operation_key: request.operation_key,
                    text: String::new(),
                    cancellation: None,
                });
                wire.pump_write(pending, |frame| {
                    inbox.consume(frame, |method, params, read_at| {
                        self.notification(method, params, read_at)
                    })
                })
                .await
            }
            Err(failure) => (Err(failure), None),
        };
        if let Err(failure) = written {
            self.turn = None;
            self.state.send_modify(|state| state.prompt_id = None);
            let outcome = match failure {
                WriteFailure::Expired => SubmitOutcome::Expired,
                WriteFailure::Encode | WriteFailure::FrameLimit => {
                    SubmitOutcome::Unavailable("Could not frame Agy turn input".into())
                }
                WriteFailure::Unwritten => {
                    SubmitOutcome::Unavailable("Agy ACP pipe closed before the first write".into())
                }
                WriteFailure::Unknown => {
                    return Err(read_fault.unwrap_or(Fault::WriteUnconfirmed));
                }
            };
            send_outcome(reply, outcome);
            return match read_fault {
                Some(fault) => Err(fault),
                None if failure == WriteFailure::Unwritten => Err(Fault::Closed),
                None => Ok(()),
            };
        }
        if let Some(fault) = read_fault {
            return Err(fault);
        }

        let deadline = Instant::now() + self.fail_fast_window;
        let mut terminal = inbox.finish(wire).await?;
        while terminal.is_none() {
            // A reply already read at the deadline takes priority. Check the
            // deadline again after every nonterminal frame to bound this window.
            terminal = tokio::select! {
                biased;
                frame = wire.recv() => self.receive(wire, frame?).await?,
                _ = sleep_until(deadline) => break,
            };
            if Instant::now() >= deadline {
                break;
            }
        }
        if let Some(Err(code)) = terminal {
            tracing::warn!(rpc_code = code, "Agy rejected a prompt during handoff");
            if matches!(code, -32602 | -32000) {
                self.turn = None;
                self.state.send_modify(|state| state.prompt_id = None);
                send_outcome(
                    reply,
                    SubmitOutcome::Unavailable("Agy rejected the prompt".into()),
                );
                return Ok(());
            }
            return Err(Fault::PromptUnconfirmed);
        }

        self.change_state(ParticipantRuntimeState::Working).await?;
        if let Some(terminal) = terminal {
            self.complete(terminal).await?;
        }
        send_outcome(
            reply,
            SubmitOutcome::Accepted {
                native_turn_id: None,
            },
        );
        Ok(())
    }

    async fn receive(
        &mut self,
        wire: &Wire,
        received: ReceivedFrame,
    ) -> Result<Option<Reply>, Fault> {
        match received.frame {
            Frame::Request { method, .. } if method == "session/request_permission" => {
                return Err(Fault::PermissionRequest);
            }
            Frame::Request { id, .. } => wire.reject_request(id).await?,
            Frame::Response { id, reply }
                if self
                    .turn
                    .as_ref()
                    .is_some_and(|turn| id.as_u64() == Some(turn.id)) =>
            {
                return Ok(Some(reply));
            }
            Frame::Notification { method, params } => {
                self.notification(&method, &params, received.read_at)?;
            }
            Frame::Response { .. } => {}
        }
        Ok(None)
    }

    fn notification(
        &mut self,
        method: &str,
        params: &Value,
        read_at: std::time::Instant,
    ) -> Result<(), Fault> {
        if method != "session/update" || params["sessionId"] != self.session_id {
            if !self.warned_unexpected_notification {
                self.warned_unexpected_notification = true;
                tracing::warn!("Ignoring an unknown or misrouted Agy ACP notification");
            }
            return Ok(());
        }
        let Some(turn) = &mut self.turn else {
            return Ok(());
        };
        let update = &params["update"];
        let content = &update["content"];
        // A line already read while idle can still be queued behind the write.
        if turn
            .first_write
            .get()
            .is_some_and(|started| read_at >= *started)
            && update["sessionUpdate"] == "agent_message_chunk"
            && content["type"] == "text"
            && let Some(text) = content["text"].as_str()
        {
            if text.len() > MAX_TURN_TEXT_BYTES - turn.text.len() {
                return Err(Fault::TextLimit);
            }
            turn.text.push_str(text);
        }
        Ok(())
    }

    async fn complete(&mut self, reply: Reply) -> Result<(), Fault> {
        let Some(mut turn) = self.turn.take() else {
            return Ok(());
        };
        self.state.send_modify(|state| state.prompt_id = None);
        let status = match reply {
            Ok(result) => match result["stopReason"].as_str() {
                Some("end_turn") => TurnFinishStatus::Completed,
                Some("cancelled") => TurnFinishStatus::Cancelled,
                _ => {
                    tracing::warn!("Agy prompt ended without a successful stop reason");
                    TurnFinishStatus::Failed
                }
            },
            Err(code) => {
                tracing::warn!(rpc_code = code, "Agy prompt failed after handoff");
                turn.text.clear();
                TurnFinishStatus::Failed
            }
        };
        self.emit(AdapterEventKind::TurnCompleted {
            native_turn_id: None,
            operation_key: Some(turn.operation_key),
            status,
            text: turn.text,
            structured: None,
        })
        .await?;
        self.change_state(ParticipantRuntimeState::Idle).await?;
        if let Some(reply) = turn.cancellation {
            let outcome = if status == TurnFinishStatus::Cancelled {
                StopOutcome::TurnCancelled
            } else {
                StopOutcome::Unknown("Agy turn finished before cancellation".into())
            };
            let _ = reply.send(outcome);
        }
        Ok(())
    }

    async fn cancel(
        &mut self,
        wire: &Wire,
        prompt_id: u64,
        reply: oneshot::Sender<StopOutcome>,
    ) -> Result<(), Fault> {
        if reply.is_closed() {
            return Ok(());
        }
        let Some(turn) = self.turn.as_mut().filter(|turn| turn.id == prompt_id) else {
            let _ = reply.send(StopOutcome::Unknown(
                "Agy turn finished before cancellation".into(),
            ));
            return Ok(());
        };
        if turn
            .cancellation
            .as_ref()
            .is_some_and(|pending| !pending.is_closed())
        {
            let _ = reply.send(StopOutcome::Unknown(
                "Agy turn cancellation is already pending".into(),
            ));
            return Ok(());
        }
        turn.cancellation = Some(reply);
        wire.send_control(json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": self.session_id}
        }))
        .await
        .map_err(|_| Fault::WriteUnconfirmed)
    }

    async fn change_state(&mut self, state: ParticipantRuntimeState) -> Result<(), Fault> {
        self.state
            .send_modify(|observation| observation.runtime = state.clone());
        self.emit(AdapterEventKind::StateChanged(state)).await
    }

    async fn emit(&mut self, kind: AdapterEventKind) -> Result<(), Fault> {
        self.event_seq += 1;
        self.events
            .send(AdapterEvent {
                event_seq: self.event_seq,
                kind,
            })
            .await
            .map_err(|_| Fault::EventReceiverClosed)
    }

    async fn finish(&mut self, fault: Option<Result<(), Fault>>) {
        if let Some(Err(fault)) = fault
            && !matches!(fault, Fault::EventReceiverClosed)
        {
            tracing::warn!(reason = fault.detail(), "Agy ACP session lost");
            let _ = self
                .change_state(ParticipantRuntimeState::Unknown {
                    reason: fault.detail().into(),
                })
                .await;
        } else {
            self.state
                .send_modify(|state| state.runtime = ParticipantRuntimeState::Stopped);
        }
    }
}

fn send_outcome(reply: &mut Option<oneshot::Sender<SubmitOutcome>>, outcome: SubmitOutcome) {
    if let Some(reply) = reply.take() {
        let _ = reply.send(outcome);
    }
}
