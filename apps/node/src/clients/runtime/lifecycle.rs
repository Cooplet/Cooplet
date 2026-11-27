use std::{path::PathBuf, sync::Arc, time::Duration};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{
    ErrorCode, EventKind, NodeId, OperationId, ParticipantId, ParticipantKind, ParticipantState,
    PrincipalKind, RunId,
};
use cooplet_storage::{WriteError, WriteOutcome};
use rusqlite::params;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use utoipa::ToSchema;

use crate::{
    auth::{Credential, platform_io},
    clients::{
        AdapterBinding, AdapterConfig, AgentOverrides, ClientAdapter, CoreBinding, DiscoveryReport,
        EffectiveAgentOptions, LifecycleBlock, LifecycleBlockKind, OpenedSession,
        ParticipantRuntimeState, ProcessOwnership, SessionOpenMode, SessionOpenRequest,
        TuiAttachPlan, compose_session_instructions, load_template, load_template_options,
    },
    config::CREDS_DIR,
    error::{NodeError, Result},
    now_ms,
    operations::{Operation, OperationGuard},
    storage::revoke_participant_credentials,
};

use super::{
    AgentService, SessionKey,
    approvals::abandon_inflight_approval,
    bad_data, busy, invalid, lock, not_found,
    participants::{ParticipantRow, load_participant, runtime_dir},
    processes::OpeningSpawner,
    sessions::{LiveSession, run_events},
    state::{ApprovalSource, enter_awaiting_approval, participant_event},
    stop::{StopUpdate, finish_stop},
    unknown,
};

const OPEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const ADAPTER_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const ACTIVE_NAME_EXISTS_QUERY: &str = "SELECT EXISTS(SELECT 1 FROM participants WHERE name = ?1 AND state NOT IN ('stopped') AND participant_id != ?2)";

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateAgentRequest {
    pub template_id: Option<String>,
    pub client_kind: Option<String>,
    pub name: Option<String>,
    pub workspace_dir: Option<String>,
    pub overrides: Option<AgentOverrides>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct AgentResponse {
    pub operation_id: OperationId,
    pub participant_id: ParticipantId,
    pub name: String,
    pub client_kind: String,
    pub node_id: NodeId,
}

struct OpenPlan {
    id: ParticipantId,
    name: String,
    workspace: PathBuf,
    template_id: Option<String>,
    overrides: Option<AgentOverrides>,
    client_kind: String,
    adapter: Arc<dyn ClientAdapter>,
    effective_options: EffectiveAgentOptions,
    instructions: Option<String>,
    previous: Option<ParticipantRow>,
    ephemeral_for_run_id: Option<RunId>,
}

pub(super) type WriteResult<T> = std::result::Result<WriteOutcome<T>, WriteError<NodeError>>;

pub(super) enum Reservation<T> {
    Replay(Value),
    New(T),
}

impl AgentService {
    pub async fn discover_all(&self) -> Result<Vec<DiscoveryReport>> {
        self.cancel_on_shutdown(self.0.registry.discover_all(&self.adapter_config()))
            .await
    }

    fn adapter_config(&self) -> AdapterConfig {
        AdapterConfig {
            state_dir: self.0.config.state_dir.clone(),
            runtime_dir: self.0.config.state_dir.join("runtime"),
        }
    }

    pub async fn create(
        &self,
        actor: &str,
        key: &str,
        mut request: CreateAgentRequest,
        ephemeral_for_run_id: Option<RunId>,
    ) -> Result<AgentResponse> {
        request.overrides = request
            .overrides
            .filter(|overrides| overrides != &AgentOverrides::default());
        let guard = self.0.operations.guard(
            actor,
            key,
            "agent.create",
            &(request.clone(), &ephemeral_for_run_id),
        )?;
        if let Some(value) = self.0.operations.lookup(&guard.operation).await? {
            return decode(value);
        }
        let id = ParticipantId::generate()?;
        let _mutation = self.mutation(&id)?;
        let name = request.name.unwrap_or_else(|| id.to_string());
        if cooplet_protocol::validate_actor_handle(&name).is_err() {
            return Err(invalid(
                "invalid_name",
                "An agent name must be a nonempty handle of at most 128 bytes",
            ));
        }
        if name.eq_ignore_ascii_case("operator") {
            return Err(invalid(
                "reserved_name",
                "The participant name 'operator' is reserved for the local human operator",
            ));
        }
        let default_workspace = self.0.config.workspace_dir.clone();
        let requested_workspace = request.workspace_dir;
        let workspace = tokio::task::spawn_blocking(move || {
            resolve_workspace(requested_workspace, &default_workspace)
        })
        .await
        .map_err(NodeError::internal)??;
        let plan = self
            .plan(OpenPlanInput {
                id,
                name,
                workspace,
                template_id: request.template_id,
                client_kind: request.client_kind,
                overrides: request.overrides,
                previous: None,
                ephemeral_for_run_id,
            })
            .await?;
        self.open(&guard, plan).await
    }

    pub async fn resume(&self, actor: &str, key: &str, id: ParticipantId) -> Result<AgentResponse> {
        let guard = self.0.operations.guard(actor, key, "agent.resume", &id)?;
        if let Some(value) = self.0.operations.lookup(&guard.operation).await? {
            return decode(value);
        }
        let _mutation = self.mutation(&id)?;
        let previous = self.participant_row(&id).await?;
        ensure_resumable(&previous)?;
        let persisted = previous.persisted(&self.0.config.state_dir)?;
        let workspace =
            tokio::task::spawn_blocking(move || resolve_workspace(None, &persisted.workspace_dir))
                .await
                .map_err(NodeError::internal)??;
        let plan = self
            .plan(OpenPlanInput {
                id,
                name: previous.name.clone(),
                workspace,
                template_id: previous.template_id.clone(),
                client_kind: previous.client_kind.clone(),
                overrides: previous.binding.core.overrides.clone(),
                previous: Some(previous),
                ephemeral_for_run_id: None,
            })
            .await?;
        self.open(&guard, plan).await
    }

    async fn plan(&self, input: OpenPlanInput) -> Result<OpenPlan> {
        let has_frozen_instructions = input
            .previous
            .as_ref()
            .and_then(|previous| previous.binding.core.frozen_instructions.as_ref())
            .is_some();
        let template = match input.template_id.clone() {
            Some(id) => {
                let workspace = input.workspace.clone();
                Some(
                    tokio::task::spawn_blocking(move || {
                        if has_frozen_instructions {
                            load_template_options(&workspace, &id)
                        } else {
                            load_template(&workspace, &id)
                        }
                    })
                    .await
                    .map_err(NodeError::internal)??,
                )
            }
            None => None,
        };
        let from_template = template.as_ref().map(|template| template.client.as_str());
        if let (Some(client), Some(template_client)) = (input.client_kind.as_deref(), from_template)
            && client != template_client
        {
            return Err(invalid(
                "template_client_mismatch",
                "The requested client differs from the template client",
            ));
        }
        let client_kind = input
            .client_kind
            .as_deref()
            .or(from_template)
            .ok_or_else(|| invalid("client_required", "Set a template_id or a client_kind"))?
            .to_owned();
        let adapter = self.0.registry.get(&client_kind)?;
        let config = self.adapter_config();
        let report = self
            .cancel_on_shutdown(self.0.registry.discover(&client_kind, &config))
            .await?;
        if !report.available {
            return Err(invalid(
                "adapter_unavailable",
                "The selected adapter cannot open this session",
            ));
        }
        let capabilities = report.capabilities;
        let supported = if input.previous.is_some() {
            capabilities.supports_resume
        } else {
            capabilities.supports_create
        };
        if !supported {
            return Err(invalid(
                "unsupported_operation",
                "The selected adapter does not support this operation",
            ));
        }
        let overrides = input.overrides.clone().unwrap_or_default();
        let effective_options = template.as_ref().map_or_else(
            || overrides.clone(),
            |template| template.options.merged(&overrides),
        );
        let instructions = input
            .previous
            .as_ref()
            .and_then(|previous| previous.binding.core.frozen_instructions.clone())
            .or_else(|| {
                Some(compose_session_instructions(
                    template
                        .as_ref()
                        .and_then(|template| template.instructions.as_deref()),
                ))
            });
        Ok(OpenPlan {
            id: input.id,
            name: input.name,
            workspace: input.workspace,
            template_id: input.template_id,
            overrides: input.overrides,
            client_kind,
            adapter,
            effective_options,
            instructions,
            previous: input.previous,
            ephemeral_for_run_id: input.ephemeral_for_run_id,
        })
    }

    async fn open(&self, guard: &OperationGuard, plan: OpenPlan) -> Result<AgentResponse> {
        let op = &guard.operation;
        let generation = match &plan.previous {
            Some(previous) => previous
                .generation
                .checked_add(1)
                .ok_or_else(|| NodeError::internal("Participant generation exhausted"))?,
            None => 1,
        };
        let key = (plan.id.clone(), generation);
        let credential = Credential::new(
            self.0.config.node_id.clone(),
            plan.id.clone(),
            self.0.config.endpoint,
        )?;
        let boot_id = self.0.boot.current();
        let reservation = self
            .reserve_open(op, &plan, generation, credential.clone(), boot_id.clone())
            .await?;
        if let Reservation::Replay(value) = reservation {
            return decode(value);
        }
        guard.mark_committed();
        if let Some(previous) = &plan.previous {
            let previous_key = (plan.id.clone(), previous.generation);
            unlink_credential(self.0.config.state_dir.clone(), previous_key.clone()).await;
            remove_runtime_dir(self.0.config.state_dir.clone(), previous_key).await;
        }
        let previous_ownership = plan.previous.as_ref().map(|previous| previous.ownership);
        let result = self.start_open(&plan, &key, &credential).await;
        let (opened, mode) = match result {
            Ok(opened) => opened,
            Err(error) => {
                return self
                    .rollback_open(op, &key, boot_id, previous_ownership, error)
                    .await;
            }
        };
        let committed = self.commit_open(op, &plan, &key, &opened).await;
        let response = match committed {
            Ok(response) => response,
            Err(error) => match self.0.operations.lookup(op).await {
                Ok(Some(value)) => decode(value)?,
                _ => {
                    return self
                        .rollback_open(op, &key, boot_id, previous_ownership, error)
                        .await;
                }
            },
        };

        let ownership = opened.process_ownership;
        let persisted = crate::clients::PersistedSessionRef {
            participant_id: plan.id.clone(),
            generation,
            native_key: Some(opened.native_key),
            process_ownership: ownership,
            workspace_dir: plan.workspace,
            runtime_dir: runtime_dir(&self.0.config.state_dir, &plan.id, generation),
            process_identity: self.owned_process_identity(&key),
            adapter_binding: opened.adapter_binding,
        };
        let live = Arc::new(LiveSession::new(
            plan.adapter,
            opened.handle.with_persisted(persisted),
            opened.capabilities,
            mode,
            self.0.queue_wakeup.get().cloned(),
        ));
        let event_stop = live.event_stop();
        let quiescing = {
            let mut sessions = lock(&self.0.sessions);
            let quiescing = *self.0.stopping.borrow();
            if quiescing {
                live.disconnect();
            }
            sessions.live.insert(plan.id, live.clone());
            quiescing
        };
        if !quiescing {
            tokio::spawn(run_events(
                Arc::downgrade(&self.0),
                live.clone(),
                opened.events,
                event_stop,
            ));
            #[cfg(target_os = "linux")]
            if ownership == ProcessOwnership::Managed
                && let Some(pidfd) = self.try_open_owned_pidfd(&key)
            {
                self.0
                    .exit_monitor
                    .watch(Arc::downgrade(&self.0), key, live.clone(), pidfd);
            }
            live.wake_if_idle();
        }
        Ok(response)
    }

    async fn reserve_open(
        &self,
        operation: &Operation,
        plan: &OpenPlan,
        generation: u64,
        credential: Credential,
        boot_id: Option<String>,
    ) -> Result<Reservation<()>> {
        let now = now_ms()?;
        let id = plan.id.clone();
        let name = plan.name.clone();
        let client_kind = plan.client_kind.clone();
        let workspace = plan
            .workspace
            .to_str()
            .ok_or_else(|| {
                invalid(
                    "invalid_workspace",
                    "The workspace directory must be a UTF-8 path",
                )
            })?
            .to_owned();
        let template_id = plan.template_id.clone();
        let overrides = plan.overrides.clone();
        let frozen_instructions = plan.instructions.clone();
        let resume = plan.previous.as_ref().map(|previous| previous.generation);
        let run = plan.ephemeral_for_run_id.clone();
        let auth = self.0.auth.clone();
        let op = operation.clone();
        self.0.db.write(move |tx| -> WriteResult<_> {
            if let Some(value) = op.lookup(tx)? {
                return Ok(WriteOutcome::new(Reservation::Replay(value)));
            }
            let conflicts: bool = tx.query_row(
                ACTIVE_NAME_EXISTS_QUERY,
                params![name, id.as_str()],
                |row| row.get(0),
            )?;
            if conflicts {
                return Err(busy("name_in_use", false).into());
            }
            let core = CoreBinding {
                overrides,
                frozen_instructions,
                lifecycle_block: Some(LifecycleBlock { kind: LifecycleBlockKind::SpawnUncertain, boot_id }),
                ..CoreBinding::default()
            };
            if let Some(previous_generation) = resume {
                let mut previous = load_participant(tx, &id)?.ok_or_else(not_found)?;
                ensure_resumable(&previous)?;
                if previous.generation != previous_generation {
                    return Err(busy("generation_changed", true).into());
                }
                abandon_inflight_approval(tx, &mut previous, now)?;
                previous.binding.core = core;
                let binding = serde_json::to_string(&previous.binding).map_err(bad_data)?;
                tx.execute(
                    "UPDATE participants SET generation = ?2, process_ownership = 'managed', state = 'starting',
                     stopped_at_ms = NULL, adapter_binding_json = ?3, updated_at_ms = ?4,
                     runtime_pid = NULL, runtime_pgid = NULL, runtime_start_token = NULL WHERE participant_id = ?1",
                    params![id.as_str(), generation, binding, now],
                )?;
                revoke_participant_credentials(tx, now, id.as_str())?;
            } else {
                let binding = serde_json::to_string(&AdapterBinding { core, ..AdapterBinding::default() }).map_err(bad_data)?;
                tx.execute(
                    "INSERT INTO participants
                     (participant_id,kind,name,client_kind,workspace_dir,template_id,ephemeral_for_run_id,
                      state,adapter_binding_json,generation,created_at_ms,updated_at_ms)
                     VALUES(?1,'agent',?2,?3,?4,?5,?6,'starting',?7,?8,?9,?9)",
                    params![id.as_str(),name,client_kind,workspace,template_id,run.as_ref().map(RunId::as_str),binding,generation,now],
                )?;
            }
            auth.insert_credential(tx, &credential, PrincipalKind::Participant, now)?;
            op.reserve(tx, now, Some(id.as_str()))?;
            Ok(WriteOutcome::new(Reservation::New(())))
        }).await.map_err(Into::into)
    }

    async fn start_open(
        &self,
        plan: &OpenPlan,
        key: &SessionKey,
        credential: &Credential,
    ) -> Result<(OpenedSession, ParticipantState)> {
        let state_dir = self.0.config.state_dir.clone();
        let prep_key = key.clone();
        let prep_cred = credential.clone();
        let (runtime, cred_file_path) = tokio::task::spawn_blocking(move || {
            let runtime_root = state_dir.join("runtime");
            let runtime = runtime_dir(&state_dir, &prep_key.0, prep_key.1);
            PrivateFs::ensure_private_dir(&runtime_root).map_err(platform_io)?;
            PrivateFs::ensure_private_dir(&runtime).map_err(platform_io)?;
            let creds = state_dir.join(CREDS_DIR);
            let filename = credential_filename(&prep_key);
            prep_cred.publish(&creds, &filename)?;
            Ok::<_, std::io::Error>((runtime, creds.join(filename)))
        })
        .await
        .map_err(NodeError::internal)??;
        let mode = match &plan.previous {
            Some(previous) => SessionOpenMode::Resume {
                native_key: previous.native_key.clone().ok_or_else(|| {
                    invalid("missing_native_session", "No native session was saved")
                })?,
                previous_binding: previous.binding.adapter.clone(),
            },
            None => SessionOpenMode::Create,
        };
        let spawner = Arc::new(OpeningSpawner::new(self.clone(), key.clone()));
        let request = SessionOpenRequest {
            participant_id: key.0.clone(),
            generation: key.1,
            mode,
            workspace_dir: plan.workspace.clone(),
            runtime_dir: runtime,
            cred_file_path,
            template_id: plan.template_id.clone(),
            effective_options: plan.effective_options.clone(),
            instructions: plan.instructions.clone(),
            spawner: spawner.clone(),
        };
        let result = self
            .cancel_on_shutdown(async {
                tokio::time::timeout(OPEN_REQUEST_TIMEOUT, plan.adapter.create_or_resume(request))
                    .await
                    .map_err(|_| unknown("native_open_timed_out"))?
                    // Adapter-controlled diagnostics can contain credentials.
                    .map_err(|error| {
                        NodeError::new(
                            error.code(),
                            "The adapter could not open its native session",
                        )
                        .with_extension("reason", "native_open_failed")
                    })
            })
            .await;
        // A detached spawn may still be publishing its process identity.
        // Rollback needs ownership settled before it checks for the child.
        spawner.seal().await;
        let opened = result?;
        opened.native_key.validate(&plan.client_kind)?;
        if let Some(previous) = &plan.previous
            && previous.native_key.as_ref() != Some(&opened.native_key)
        {
            return Err(invalid(
                "native_session_mismatch",
                "The resumed session differs from the saved conversation",
            ));
        }
        let owned = self.owned_process_identity(key);
        let valid_process = match opened.process_ownership {
            ProcessOwnership::Managed => {
                owned.is_some() && self.participant_row(&key.0).await?.identity == owned
            }
            ProcessOwnership::Attached => !self.has_owned_process(key),
        };
        if !valid_process {
            return Err(invalid(
                "process_ownership_mismatch",
                "The adapter's declared ownership differs from the process it opened",
            ));
        }
        let state = opened.initial_state.state();
        if !matches!(
            state,
            ParticipantState::Idle
                | ParticipantState::Working
                | ParticipantState::AwaitingApproval
                | ParticipantState::Unavailable
        ) {
            return Err(invalid(
                "invalid_initial_state",
                "The adapter did not open a live session",
            ));
        }
        Ok((opened, state))
    }

    async fn commit_open(
        &self,
        operation: &Operation,
        plan: &OpenPlan,
        key: &SessionKey,
        opened: &OpenedSession,
    ) -> Result<AgentResponse> {
        let now = now_ms()?;
        let op = operation.clone();
        let key = key.clone();
        let native = opened.native_key.clone();
        let state = opened.initial_state.clone();
        let ownership = opened.process_ownership;
        let binding = opened.adapter_binding.clone();
        let capabilities =
            serde_json::to_string(&opened.capabilities).map_err(NodeError::internal)?;
        let creating = plan.previous.is_none();
        let response = AgentResponse {
            operation_id: op.id.clone(),
            participant_id: plan.id.clone(),
            name: plan.name.clone(),
            client_kind: plan.client_kind.clone(),
            node_id: self.0.config.node_id.clone(),
        };
        self.0.operations.finalize(move |tx| -> WriteResult<_> {
            let conflicts: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM participants
                 WHERE client_kind = ?1 AND store_namespace = ?2 AND native_session_id = ?3 AND participant_id != ?4)",
                params![native.client_kind,native.store_namespace,native.native_session_id,key.0.as_str()], |row| row.get(0),
            )?;
            if conflicts {
                return Err(busy("native_session_in_use", false).into());
            }
            let mut current = load_participant(tx, &key.0)?.ok_or_else(not_found)?;
            if current.generation != key.1 || current.state != ParticipantState::Starting {
                return Err(unknown("open_generation_changed").into());
            }
            current.binding.core.lifecycle_block = None;
            current.binding.adapter.clone_from(&binding);
            let persisted_binding = serde_json::to_string(&current.binding).map_err(bad_data)?;
            let approval = if let ParticipantRuntimeState::AwaitingApproval(wait) = &state {
                Some(wait.clone())
            } else {
                None
            };
            // Publish the approval transition through the same mapper used for later events.
            let next = if approval.is_some() { ParticipantState::Starting } else { state.state() };
            tx.execute(
                "UPDATE participants SET store_namespace = ?3, native_session_id = ?4, process_ownership = ?5,
                 state = ?6, capabilities_json = ?7, adapter_binding_json = ?8, updated_at_ms = ?9
                 WHERE participant_id = ?1 AND generation = ?2 AND state = 'starting'",
                params![key.0.as_str(),key.1,native.store_namespace,native.native_session_id,ownership.as_str(),
                    next.as_str(),capabilities,persisted_binding,now],
            )?;
            let mut events = Vec::new();
            if let Some(wait) = approval {
                enter_awaiting_approval(
                    tx, now, &key.0, key.1, wait,
                    ApprovalSource::Initial { created_by: creating.then_some(op.actor_id.as_str()) },
                    &mut events,
                )?.ok_or_else(|| unknown("open_generation_changed"))?;
            } else {
                participant_event(tx, now,
                    if creating { EventKind::ParticipantCreated } else { EventKind::ParticipantStateChanged },
                    &key.0, Some(&op.actor_id), json!({}), &mut events,
                )?;
            }
            op.succeed(tx, now, &response)?;
            Ok(WriteOutcome::with_events(response.clone(), events))
        }).await
    }

    async fn rollback_open<T>(
        &self,
        operation: &Operation,
        key: &SessionKey,
        boot_id: Option<String>,
        previous_ownership: Option<ProcessOwnership>,
        error: NodeError,
    ) -> Result<T> {
        let clean = if self.has_owned_process(key) {
            self.clean_owned_process(key, None).await
        } else {
            true
        };
        unlink_credential(self.0.config.state_dir.clone(), key.clone()).await;
        if clean {
            remove_runtime_dir(self.0.config.state_dir.clone(), key.clone()).await;
        }
        let now = now_ms()?;
        let op = operation.clone();
        let key = key.clone();
        let identity = self.owned_process_identity(&key);
        let returned = if clean {
            error
        } else {
            unknown("open_cleanup_unverified")
        };
        let code = returned.code();
        let error = Arc::new(returned);
        let saved = error.clone();
        let write = self
            .0
            .operations
            .finalize(move |tx| -> WriteResult<_> {
                let mut row = load_participant(tx, &key.0)?.ok_or_else(not_found)?;
                if row.generation != key.1 || row.state != ParticipantState::Starting {
                    return Err(unknown("open_generation_changed").into());
                }
                if !clean && row.identity.is_none() {
                    row.identity.clone_from(&identity);
                }
                if clean && let Some(ownership) = previous_ownership {
                    row.ownership = ownership;
                }
                revoke_participant_credentials(tx, now, key.0.as_str())?;
                let mut events = Vec::new();
                finish_stop(
                    tx,
                    &mut row,
                    StopUpdate {
                        now,
                        clean,
                        boot_id: boot_id.clone(),
                        reason: "native_open_failed",
                        actor: Some(&op.actor_id),
                    },
                    &mut events,
                )?;
                if !clean || code == ErrorCode::DeliveryUnknown {
                    op.unknown(tx, now, Some(&saved))?;
                } else {
                    op.fail(tx, now, &saved)?;
                }
                Ok(WriteOutcome::with_events((), events))
            })
            .await;
        if write.is_err() {
            return Err(unknown("open_result_unconfirmed")
                .with_extension("operation_id", operation.id.as_str()));
        }
        Err(Arc::try_unwrap(error).unwrap_or_else(|_| unknown("open_result_unconfirmed")))
    }

    pub async fn attach_plan(&self, id: ParticipantId) -> Result<TuiAttachPlan> {
        let row = self.participant_row(&id).await?;
        let target = row.persisted(&self.0.config.state_dir)?;
        let client_kind = row
            .client_kind
            .as_deref()
            .ok_or_else(|| invalid("not_an_agent", "The participant has no client adapter"))?;
        let adapter = self.0.registry.get(client_kind)?;
        let live = self.live_session(&id).filter(|live| {
            live.generation() == row.generation && live.state() != ParticipantState::Unknown
        });
        self.cancel_on_shutdown(async {
            tokio::time::timeout(
                ADAPTER_READ_TIMEOUT,
                adapter.tui_attach_plan(&target, live.as_ref().map(|session| &session.handle)),
            )
            .await
            .map_err(|_| adapter_read_timeout())?
            .map_err(|error| NodeError::new(error.code(), "Could not prepare an attach plan"))
        })
        .await
    }

    pub async fn inspect_state(
        &self,
        id: ParticipantId,
    ) -> Result<crate::clients::SessionInspection> {
        let row = self.participant_row(&id).await?;
        let target = row.persisted(&self.0.config.state_dir)?;
        let client_kind = row
            .client_kind
            .as_deref()
            .ok_or_else(|| invalid("not_an_agent", "The participant has no client adapter"))?;
        let adapter = self.0.registry.get(client_kind)?;
        let live = self.live_session(&id).filter(|live| {
            live.generation() == row.generation && live.state() != ParticipantState::Unknown
        });
        self.cancel_on_shutdown(async {
            tokio::time::timeout(
                ADAPTER_READ_TIMEOUT,
                adapter.inspect_state(&target, live.as_ref().map(|session| &session.handle)),
            )
            .await
            .map_err(|_| adapter_read_timeout())?
            .map_err(|error| {
                tracing::warn!(participant_id = %id, %error, "Adapter state inspection failed");
                NodeError::new(error.code(), "Could not inspect the session state")
            })
        })
        .await
    }
}

fn adapter_read_timeout() -> NodeError {
    NodeError::new(
        ErrorCode::Overloaded,
        "The adapter did not complete the diagnostic request",
    )
    .with_extension("reason", "adapter_read_timeout")
}

struct OpenPlanInput {
    id: ParticipantId,
    name: String,
    workspace: PathBuf,
    template_id: Option<String>,
    client_kind: Option<String>,
    overrides: Option<AgentOverrides>,
    previous: Option<ParticipantRow>,
    ephemeral_for_run_id: Option<RunId>,
}

fn resolve_workspace(input: Option<String>, default: &std::path::Path) -> Result<PathBuf> {
    let path = input
        .map(|path| default.join(path))
        .unwrap_or_else(|| default.to_owned());
    let path = std::fs::canonicalize(path)
        .map_err(|_| invalid("invalid_workspace", "The agent workspace cannot be opened"))?;
    if !path.is_dir() {
        return Err(invalid(
            "invalid_workspace",
            "The agent workspace must be a directory",
        ));
    }
    Ok(path)
}

fn ensure_resumable(row: &ParticipantRow) -> Result<()> {
    if row.kind != ParticipantKind::Agent {
        return Err(invalid(
            "not_an_agent",
            "Only agents have a resumable conversation",
        ));
    }
    if row.ephemeral_for_run_id.is_some() {
        return Err(invalid(
            "ephemeral_participant",
            "Ephemeral workflow participants cannot be resumed",
        ));
    }
    if row.state != ParticipantState::Stopped
        || row.binding.core.lifecycle_block.is_some()
        || row.identity.is_some()
    {
        return Err(busy("session_not_stopped", false));
    }
    if row.native_key.is_none() {
        return Err(invalid(
            "missing_native_session",
            "No saved native session is available",
        ));
    }
    Ok(())
}

fn credential_filename(key: &SessionKey) -> String {
    format!("{}.g{}.json", key.0, key.1)
}

pub(super) async fn unlink_credential(state_dir: PathBuf, key: SessionKey) {
    let result = tokio::task::spawn_blocking(move || {
        let dir = PrivateFs::open_dir_nofollow(&state_dir.join(CREDS_DIR))?;
        let filename = credential_filename(&key);
        if PrivateFs::unlink_file_at(&dir, &filename)? {
            dir.sync_all()?;
        }
        Ok::<_, cooplet_platform::PlatformError>(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "Could not remove a revoked agent credential"),
        Err(error) => tracing::warn!(%error, "Credential cleanup task failed"),
    }
}

pub(super) async fn remove_runtime_dir(state_dir: PathBuf, key: SessionKey) {
    let result = tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::MetadataExt as _;

        let runtime_root = state_dir.join("runtime");
        let root_fd = match PrivateFs::open_dir_nofollow(&runtime_root) {
            Ok(fd) => fd,
            Err(cooplet_platform::PlatformError::Io(err))
                if err.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(std::io::Error::other(error)),
        };
        let root_meta = root_fd.metadata()?;
        let path_meta = std::fs::symlink_metadata(&runtime_root)?;
        if !path_meta.is_dir()
            || (root_meta.dev(), root_meta.ino()) != (path_meta.dev(), path_meta.ino())
        {
            return Ok(());
        }
        let dir = runtime_dir(&state_dir, &key.0, key.1);
        match std::fs::symlink_metadata(&dir) {
            Ok(meta) if !meta.file_type().is_symlink() && meta.is_dir() => {
                std::fs::remove_dir_all(&dir)
            }
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "Could not remove an agent runtime directory"),
        Err(error) => tracing::warn!(%error, "Agent runtime cleanup task failed"),
    }
}

pub(super) fn decode<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(NodeError::internal)
}

#[cfg(test)]
mod tests;
