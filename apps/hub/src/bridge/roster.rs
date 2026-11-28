use std::collections::HashSet;

use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{
    CommandResPayload, ErrorCode, MAX_PENDING_OP_IDS, MAX_ROSTER_BYTES, MAX_ROSTER_CATALOG_BYTES,
    MAX_ROSTER_FENCES, MAX_ROSTER_TEMPLATES, MAX_ROSTER_WORKFLOWS, OperationId, ParticipantWire,
    RosterSyncPayload, ThreadFenceReport,
};
use cooplet_storage::{WriteError, WriteOutcome};
use rusqlite::params;
use tokio::sync::watch;

use crate::{AppState, delivery, error::HubError, threads};

use super::{
    projection::{
        MAX_PARTICIPANTS, RosterCatalog, node_status_event, require_session, upsert_participant,
    },
    registry::{CloseReason, SessionKey},
};

#[derive(Default)]
pub(super) struct Roster {
    index: u32,
    bytes: usize,
    participants: Vec<ParticipantWire>,
    catalog: RosterCatalog,
    fences: Vec<ThreadFenceReport>,
    receipts: Vec<CommandResPayload>,
}

impl Roster {
    pub(super) fn append(
        &mut self,
        chunk: RosterSyncPayload,
        frame_bytes: usize,
    ) -> Result<bool, HubError> {
        if chunk.chunk_index != self.index {
            return Err(HubError::invalid("Bridge roster chunk order is invalid."));
        }
        self.bytes += frame_bytes;
        if self.bytes > MAX_ROSTER_BYTES
            || self.participants.len() + chunk.participants.len() > MAX_PARTICIPANTS
            || self.catalog.templates.len() + chunk.templates.len() > MAX_ROSTER_TEMPLATES
            || self.catalog.workflows.len() + chunk.workflows.len() > MAX_ROSTER_WORKFLOWS
            || self.fences.len() + chunk.unresolved_thread_fences.len() > MAX_ROSTER_FENCES
            || self.receipts.len() + chunk.resolved_op_receipts.len() > MAX_PENDING_OP_IDS
        {
            return Err(oversized());
        }
        self.index += 1;
        self.participants.extend(chunk.participants);
        self.catalog.templates.extend(chunk.templates);
        self.catalog.workflows.extend(chunk.workflows);
        self.fences.extend(chunk.unresolved_thread_fences);
        self.receipts.extend(chunk.resolved_op_receipts);
        Ok(chunk.is_final)
    }

    pub(super) async fn commit(
        self,
        state: &AppState,
        key: &SessionKey,
        pending_ops: Vec<OperationId>,
        deadline: AdmissionGuard,
        cancelled: watch::Receiver<Option<CloseReason>>,
    ) -> Result<(), HubError> {
        check_active(&deadline, &cancelled)?;
        self.require_unique()?;
        let catalog = serde_json::to_string(&self.catalog)?;
        if catalog.len() > MAX_ROSTER_CATALOG_BYTES {
            return Err(oversized());
        }

        let key = key.clone();
        let hub_id = state.meta.hub_id.clone();
        let wall_clock = state.wall_clock.clone();
        Ok(state
            .db
            .write(move |tx| {
                let now = wall_clock();
                check_active(&deadline, &cancelled)?;
                require_session(tx, &key)?;
                tx.execute(
                    "DELETE FROM node_participants WHERE node_id = ?1",
                    [key.node_id.as_str()],
                )?;
                for participant in &self.participants {
                    upsert_participant(tx, &key, participant, now)?;
                }
                tx.execute(
                "UPDATE nodes SET status = 'online', last_seen_at_ms = ?1, roster_catalog_json = ?2
                  WHERE node_id = ?3 AND session_epoch = ?4",
                params![now, catalog, key.node_id.as_str(), key.epoch],
            )?;
                let mut events = Vec::new();
                threads::reconcile_fences(
                    tx,
                    &hub_id,
                    &key.node_id,
                    &self.fences,
                    now,
                    &mut events,
                )?;
                delivery::reconcile_receipts(
                    tx,
                    &hub_id,
                    &key.node_id,
                    &self.receipts,
                    &pending_ops,
                    now,
                    &mut events,
                )?;
                events.push(node_status_event(tx, &hub_id, &key, now, true)?);
                check_active(&deadline, &cancelled)?;
                Ok::<_, WriteError<HubError>>(WriteOutcome::with_events((), events))
            })
            .await?)
    }

    fn require_unique(&self) -> Result<(), HubError> {
        if distinct(self.participants.iter().map(|item| &item.participant_id))
            && distinct(self.catalog.templates.iter().map(|item| &item.template_id))
            && distinct(
                self.catalog
                    .workflows
                    .iter()
                    .map(|item| &item.workflow_name),
            )
            && distinct(
                self.fences
                    .iter()
                    .map(|item| (&item.thread_id, &item.participant_id)),
            )
            && distinct(self.receipts.iter().map(|item| &item.op_id))
        {
            Ok(())
        } else {
            Err(HubError::invalid(
                "A bridge roster contains duplicate entries.",
            ))
        }
    }
}

fn distinct<T: Eq + std::hash::Hash>(mut items: impl Iterator<Item = T>) -> bool {
    let mut seen = HashSet::with_capacity(items.size_hint().0);
    items.all(|item| seen.insert(item))
}

fn check_active(
    deadline: &AdmissionGuard,
    cancelled: &watch::Receiver<Option<CloseReason>>,
) -> Result<(), HubError> {
    if deadline.is_expired() || cancelled.borrow().is_some() {
        Err(HubError::new(
            ErrorCode::BridgeUnavailable,
            "The bridge roster synchronization expired.",
        ))
    } else {
        Ok(())
    }
}

fn oversized() -> HubError {
    HubError::new(
        ErrorCode::PayloadTooLarge,
        "The bridge roster exceeds its limits.",
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        task::Poll,
        time::Duration,
    };

    use cooplet_platform::{MonoClock, MonoInstant};
    use cooplet_protocol::{HubId, NodeId, ParticipantId, ParticipantKind, ParticipantState};
    use serde_json::json;
    use tokio::sync::{oneshot, watch};

    use crate::{bridge::OfflineBridge, storage};

    use super::*;

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct Clock(AtomicU64);

    impl MonoClock for Clock {
        fn now(&self) -> MonoInstant {
            MonoInstant::from_millis(self.0.load(Ordering::Relaxed))
        }
    }

    #[tokio::test]
    async fn expired_roster_queued_behind_a_writer_publishes_nothing()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let scratch = Scratch(
            std::env::temp_dir().join(format!("cooplet-bridge-test-{}", HubId::generate()?)),
        );
        storage::init(&scratch.0, "Test Hub").await?;
        let storage::HubStorage {
            engine,
            meta,
            master,
            thread_acl_watch,
        } = storage::open(&scratch.0, 1024, 16_384, 16_384).await?;
        let (_shutdown, stop) = watch::channel(());
        let state = AppState::new(
            &engine,
            meta,
            master,
            thread_acl_watch,
            Arc::new(OfflineBridge),
            stop,
            1024,
        )?;
        let node_id = NodeId::generate()?;
        let id = node_id.clone();
        state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "INSERT INTO nodes
                   (node_id, display_name, owner_principal_id, credential_hmac, session_epoch,
                    status, enrolled_at_ms)
                 VALUES (?1, 'Worker', (SELECT principal_id FROM principals LIMIT 1),
                        'test-verifier', 5, 'offline', 0)",
                    [id.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;

        let (entered_tx, entered) = oneshot::channel();
        let (unblock, blocked) = mpsc::channel();
        let db = state.db.clone();
        let blocker = tokio::spawn(async move {
            db.write_sqlite(move |_| {
                let _ = entered_tx.send(());
                blocked
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                Ok(WriteOutcome::new(()))
            })
            .await
        });
        entered.await?;

        let key = SessionKey {
            node_id: node_id.clone(),
            generation: 1,
            epoch: 5,
        };
        let clock = Arc::new(Clock::default());
        let budget = AdmissionGuard::with_clock(Duration::from_secs(30), clock.clone());
        let (_close_tx, close) = watch::channel(None);
        let roster = Roster {
            participants: vec![ParticipantWire {
                participant_id: ParticipantId::generate()?,
                kind: ParticipantKind::Agent,
                name: "coordinator".into(),
                client_kind: None,
                workspace_dir: None,
                state: ParticipantState::Idle,
                capabilities: json!({}),
                wait_kind: None,
                approval_id: None,
                action_hint: None,
                summary: None,
                can_respond_remotely: None,
            }],
            ..Roster::default()
        };
        let commit = roster.commit(&state, &key, Vec::new(), budget, close);
        tokio::pin!(commit);
        std::future::poll_fn(|cx| {
            assert!(commit.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        clock.0.store(30_000, Ordering::Relaxed);
        unblock.send(())?;
        blocker.await??;

        let error = commit
            .await
            .err()
            .ok_or_else(|| io::Error::other("roster unexpectedly committed"))?;
        assert_eq!(error.problem().code, ErrorCode::BridgeUnavailable);
        let (status, participants, events) = state
            .reads
            .read(move |conn| {
                let status: String = conn.query_row(
                    "SELECT status FROM nodes WHERE node_id = ?1",
                    [node_id.as_str()],
                    |row| row.get(0),
                )?;
                let participants: usize =
                    conn.query_row("SELECT COUNT(*) FROM node_participants", [], |row| {
                        row.get(0)
                    })?;
                let events: usize = conn.query_row(
                    "SELECT COUNT(*) FROM events WHERE kind = 'node.connected'",
                    [],
                    |row| row.get(0),
                )?;
                Ok((status, participants, events))
            })
            .await?;
        assert_eq!((status.as_str(), participants, events), ("offline", 0, 0));
        engine.shutdown().await?;
        Ok(())
    }
}
