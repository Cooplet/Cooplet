use std::{convert::Infallible, fs, path::PathBuf, process::Command, time::Duration};

use cooplet_platform::{PrivateFs, ProcessHandle, ProcessProbe, TerminateOutcome};
use cooplet_protocol::{AggregateType, BlobId, EventKind, NodeId};
use cooplet_storage::{BlobInput, GcHooks, StorageConfig, WriteOutcome};
use rusqlite::params;
use serde_json::{Value, json};

use crate::config::{NodeConfig, OpenNode};

use super::{
    DAY_MS, NodeGcHooks, append_event, event_frame_too_large, gc::indexed_gc_statements,
    recovery::recover_with_boot_id,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let dir = std::env::temp_dir().join(NodeId::generate()?.as_str());
        Ok(Self(PrivateFs::ensure_private_dir(&dir)?))
    }

    async fn start(&self) -> Result<OpenNode, cooplet_storage::StorageError> {
        OpenNode::start(NodeConfig::new(self.0.join("state"), self.0.clone())).await
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn gate_sqlite_hygiene() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    let blocked = scratch.start().await;
    assert!(matches!(
        blocked,
        Err(cooplet_storage::StorageError::Platform(
            cooplet_platform::PlatformError::LockBusy
        ))
    ));

    let schema = node
        .engine
        .db()
        .write_sqlite(|tx| {
            let tables = tx
                .prepare(
                    "SELECT name FROM sqlite_schema
             WHERE type = 'table' AND substr(name, 1, 7) != 'sqlite_'
             ORDER BY name",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let triggers: i64 = tx.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'trigger'",
                [],
                |row| row.get(0),
            )?;
            let foreign_keys: i64 = tx.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
            let auto_vacuum: i64 = tx.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))?;
            let journal_mode: String = tx.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
            let max_page_count: u32 =
                tx.query_row("PRAGMA max_page_count", [], |row| row.get(0))?;
            Ok(WriteOutcome::new((
                tables,
                triggers,
                foreign_keys,
                auto_vacuum,
                journal_mode,
                max_page_count,
            )))
        })
        .await?;

    let (tables, triggers, foreign_keys, auto_vacuum, journal_mode, max_page_count) = schema;
    assert_eq!(
        tables,
        [
            "blobs",
            "events",
            "intake_queue",
            "messages",
            "node_meta",
            "operations",
            "participant_credentials",
            "participants",
            "thread_inject_fences",
            "workflow_runs",
            "workflow_steps"
        ]
    );
    assert_eq!((triggers, foreign_keys, auto_vacuum), (0, 1, 2));
    assert_eq!(journal_mode, "wal");
    assert_eq!(
        max_page_count,
        StorageConfig::new(node.state_dir.join("node.sqlite3")).max_page_count
    );
    let query_only: i64 = node
        .engine
        .reads()
        .read(|conn| Ok(conn.query_row("PRAGMA query_only", [], |row| row.get(0))?))
        .await?;
    assert_eq!(query_only, 1);

    let info = node.info.clone();
    let address = node.listener.local_addr()?;
    drop(node.listener);
    node.engine.shutdown().await?;
    let reopened = scratch.start().await?;
    assert_eq!(reopened.info, info);
    assert_eq!(reopened.listener.local_addr()?, address);
    reopened.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn gc_selects_use_indexes() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    node.engine
        .reads()
        .read(|conn| {
            for select in indexed_gc_statements() {
                let plan = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {select}"))?
                    .query_map(params![0, 200], |row| row.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(
                    plan.iter().any(|line| line.contains("SEARCH")),
                    "{select}: {plan:?}"
                );
                assert!(
                    plan.iter().all(|line| !line.contains("SCAN")),
                    "{select}: {plan:?}"
                );
            }
            Ok(())
        })
        .await?;
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn restart_keeps_uncertain_delivery_and_answer_receipts_distinct() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    node.engine.db().write_sqlite(|tx| {
        tx.execute_batch(
            "INSERT INTO participants(participant_id, kind, name, state, adapter_binding_json, created_at_ms, updated_at_ms, stopped_at_ms)
             VALUES ('active','agent','active','awaiting_approval',
                     '{\"core\":{\"pending_approval\":{},\"approval_response\":{\"state\":\"in_flight\",\"public_id\":\"apr_active\"}},\"adapter\":{\"kept\":true}}',0,0,NULL),
                    ('stopped','agent','stopped','stopped',
                     '{\"core\":{\"pending_approval\":{},\"approval_response\":{\"state\":\"in_flight\"}}}',0,0,3),
                    ('unknown','agent','unknown','unknown',
                     '{\"core\":{\"pending_approval\":{},\"approval_response\":{\"state\":\"in_flight\"}}}',0,0,NULL),
                    ('reboot','agent','reboot','unknown',
                     '{\"core\":{\"lifecycle_block\":{\"kind\":\"spawn_uncertain\",\"boot_id\":\"previous\"}}}',0,0,NULL),
                    ('no_boot','agent','no_boot','unknown',
                     '{\"core\":{\"lifecycle_block\":{\"kind\":\"cleanup_unverified\",\"boot_id\":null}}}',0,0,NULL);
             INSERT INTO operations(operation_id, actor_id, operation_key, kind, request_sha256, state, resource_id,
                                    created_at_ms, updated_at_ms, expires_at_ms)
             VALUES ('direct_op','active','direct','message.send','digest','in_progress','direct',0,0,10000),
                    ('answer_op','active','answer','message.send','digest','in_progress','answer',0,0,10000),
                    ('queued_op','active','queued','message.send','digest','succeeded','queued',0,0,10000);
             INSERT INTO messages(message_id, sender_id, recipient_id, body, delivery_mode, delivery_state,
                                  created_at_ms, expires_at_ms, operation_id)
             VALUES ('direct','active','unknown','','direct_now','accepted',0,10000,'direct_op'),
                    ('question','unknown','active','','direct_now','delivered',0,10000,NULL),
                    ('dispatched','active','unknown','','queue_until_idle','queued',0,10000,'queued_op'),
                    ('queued','active','unknown','','queue_until_idle','queued',0,10000,NULL),
                    ('expired','active','unknown','','queue_until_idle','queued',0,10000,NULL);
             INSERT INTO messages(message_id, sender_id, recipient_id, body, delivery_mode, delivery_state,
                                  created_at_ms, expires_at_ms, operation_id, answer_json, in_reply_to)
             VALUES ('answer','active','unknown','','direct_now','accepted',0,10000,'answer_op','{}','question');
             INSERT INTO intake_queue(participant_id, message_id, payload_envelope, state, enqueued_at_ms, deadline_at_ms)
             VALUES ('unknown','dispatched','{}','dispatched',0,1000),
                    ('unknown','queued','{}','queued',0,1000),
                    ('unknown','expired','{}','queued',0,10);
             INSERT INTO thread_inject_fences(thread_id, participant_id, highest_dispatched_seq, state, updated_at_ms, expires_at_ms)
                VALUES ('thread','unknown',7,'injecting',0,1000);",
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;

    recover_with_boot_id(&node.engine, 10, None).await?;
    recover_with_boot_id(&node.engine, 11, Some("current".into())).await?;
    let proof = node
        .engine
        .reads()
        .read(|conn| {
            let receipts = conn
                .prepare(
                    "SELECT operation_id, state, result_json FROM operations ORDER BY operation_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let states = conn
                .prepare("SELECT message_id, delivery_state FROM messages ORDER BY message_id")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let participants = conn.prepare(
            "SELECT participant_id, state, stopped_at_ms, adapter_binding_json FROM participants
             WHERE kind = 'agent' ORDER BY participant_id",
        )?.query_map([], |row| Ok((
            row.get::<_, String>(0)?, row.get::<_, String>(1)?,
            row.get::<_, Option<i64>>(2)?, row.get::<_, String>(3)?,
        )))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let events = conn
                .prepare("SELECT kind, aggregate_id FROM events ORDER BY seq")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let fence = conn.query_row(
                "SELECT state, highest_dispatched_seq FROM thread_inject_fences",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )?;
            let queue_envelopes = conn
                .prepare("SELECT message_id, state, payload_envelope FROM intake_queue ORDER BY message_id")?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((receipts, states, participants, events, fence, queue_envelopes))
        })
        .await?;
    let (receipts, states, participants, events, fence, queue_envelopes) = proof;
    assert_eq!(
        queue_envelopes
            .iter()
            .map(|(id, state, env)| (id.as_str(), state.as_str(), env.as_str()))
            .collect::<Vec<_>>(),
        [
            ("dispatched", "unknown", ""),
            ("expired", "expired", ""),
            ("queued", "queued", "{}"),
        ]
    );
    assert_eq!(
        receipts
            .iter()
            .map(|(id, state, _)| (id.as_str(), state.as_str()))
            .collect::<Vec<_>>(),
        [
            ("answer_op", "succeeded"),
            ("direct_op", "unknown"),
            ("queued_op", "succeeded")
        ]
    );
    let result_json: Value =
        serde_json::from_str(receipts[0].2.as_deref().ok_or("missing answer receipt")?)?;
    assert_eq!(
        result_json,
        json!({
            "operation_id": "answer_op", "message_id": "answer",
            "delivery_state": "unknown", "in_reply_to": "question",
        })
    );
    assert_eq!(
        states
            .iter()
            .map(|(id, state)| (id.as_str(), state.as_str()))
            .collect::<Vec<_>>(),
        [
            ("answer", "unknown"),
            ("direct", "unknown"),
            ("dispatched", "unknown"),
            ("expired", "expired"),
            ("question", "delivered"),
            ("queued", "queued")
        ]
    );
    assert_eq!((fence.0.as_str(), fence.1), ("unknown", 7));
    for (id, state, stopped_at, binding) in &participants {
        let binding: Value = serde_json::from_str(binding)?;
        match id.as_str() {
            "active" | "unknown" | "stopped" => {
                assert_eq!(binding["core"]["approval_response"]["state"], "unknown");
                assert!(binding["core"].get("pending_approval").is_none());
                if id == "active" {
                    assert_eq!(binding["adapter"]["kept"], true);
                } else if id == "stopped" {
                    assert_eq!((state.as_str(), *stopped_at), ("stopped", Some(3)));
                }
            }
            "reboot" => {
                assert_eq!((state.as_str(), *stopped_at), ("stopped", Some(11)));
                assert!(binding["core"].get("lifecycle_block").is_none());
            }
            "no_boot" => {
                assert_eq!(state, "unknown");
                assert!(binding["core"].get("lifecycle_block").is_some());
            }
            id => return Err(format!("unexpected participant {id}").into()),
        }
    }
    assert_eq!(
        events
            .iter()
            .map(|(kind, id)| (kind.as_str(), id.as_str()))
            .collect::<Vec<_>>(),
        [
            ("message.delivery_updated", "answer"),
            ("message.delivery_updated", "direct"),
            ("message.delivery_updated", "dispatched"),
            ("message.delivery_updated", "expired"),
            ("participant.state_changed", "active"),
            ("participant.stopped", "reboot")
        ]
    );
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn blocked_workflow_survives_gc_until_cleanup_is_verified() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    let mut runner = ProcessHandle::spawn_isolated_group(Command::new("sleep").arg("60"))?;
    let mut attached = ProcessHandle::spawn_isolated_group(Command::new("sleep").arg("60"))?;
    let runner_identity = runner.identity().clone();
    let attached_identity = attached.identity().clone();
    assert_ne!(
        runner
            .terminate_and_reap(Duration::from_millis(50))?
            .outcome,
        TerminateOutcome::CleanupUnverified
    );
    node.engine.db().write_sqlite(move |tx| {
        tx.execute(
            "INSERT INTO workflow_runs(run_id, workflow_name, entry_path, workspace_dir, initiator_actor_id, state,
                                       runner_pid, runner_pgid, runner_start_token, started_at_ms, updated_at_ms, expires_at_ms)
             VALUES ('run','example','workflow.ts','.','operator','running',?1,?2,?3,0,0,1000)",
            params![runner_identity.pid, runner_identity.pgid, runner_identity.start_token],
        )?;
        tx.execute(
            "INSERT INTO participants(participant_id, kind, name, state, ephemeral_for_run_id,
                                     process_ownership, runtime_pid, runtime_pgid, runtime_start_token,
                                     created_at_ms, updated_at_ms)
             VALUES ('attached','agent','attached','idle','run','attached',?1,?2,?3,0,0)",
            params![attached_identity.pid, attached_identity.pgid, attached_identity.start_token],
        )?;
        tx.execute_batch(
            "INSERT INTO participants(participant_id, kind, name, state, ephemeral_for_run_id,
                                     process_ownership, adapter_binding_json, created_at_ms, updated_at_ms)
             VALUES ('managed','agent','managed','starting','run','managed',
                     '{\"core\":{\"lifecycle_block\":{\"kind\":\"spawn_uncertain\",\"boot_id\":\"old\"}}}',0,0);
             INSERT INTO workflow_steps(run_id, step_key, status, result_json, created_at_ms)
                VALUES ('run','step','completed','{}',0);
             INSERT INTO operations(operation_id, actor_id, operation_key, kind, request_sha256, state,
                                    created_at_ms, updated_at_ms, expires_at_ms)
             VALUES ('complete','run','complete','message.send','digest','succeeded',0,0,1000),
                    ('unknown','run','unknown','message.send','digest','unknown',0,0,1000);
             INSERT INTO participant_credentials(token_id, participant_id, principal_kind, actor_id, scopes_json,
                                                 verifier_hmac, workflow_run_id, created_at_ms)
             VALUES ('run_token',NULL,'workflow_run','run','[]','digest1','run',0),
                    ('managed_token','managed','participant','managed','[]','digest2',NULL,0),
                    ('attached_token','attached','participant','attached','[]','digest3',NULL,0);",
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    recover_with_boot_id(&node.engine, 1, Some("old".into())).await?;
    let revoked: i64 = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT count(*) FROM participant_credentials WHERE revoked_at_ms = 1",
                [],
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(revoked, 3);
    let later = 40 * DAY_MS;
    node.engine.gc_once(later).await?;
    let blocked: (String, Option<i64>, Option<String>, u64, u64) = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT state, finished_at_ms, error_json,
                 (SELECT count(*) FROM operations WHERE actor_id = 'run'),
                 (SELECT count(*) FROM workflow_steps WHERE run_id = 'run')
             FROM workflow_runs WHERE run_id = 'run'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?)
        })
        .await?;
    assert_eq!(
        (blocked.0.as_str(), blocked.1, blocked.3, blocked.4),
        ("interrupted", None, 2, 1)
    );
    let error: Value = serde_json::from_str(blocked.2.as_deref().ok_or("missing cleanup error")?)?;
    assert_eq!(error["code"], "cleanup_unverified");

    recover_with_boot_id(&node.engine, later, Some("new".into())).await?;
    assert_eq!(attached.identity().probe()?, ProcessProbe::Running);
    attached.terminate_and_reap(Duration::from_millis(50))?;

    let recovered: (Option<i64>, i64, Option<String>, String, Option<i64>) = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT r.finished_at_ms, r.expires_at_ms, r.error_json, p.state, p.runtime_pid
             FROM workflow_runs AS r JOIN participants AS p ON p.ephemeral_for_run_id = r.run_id
             WHERE r.run_id = 'run' AND p.participant_id = 'managed'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?)
        })
        .await?;
    assert_eq!(
        recovered,
        (
            Some(later),
            later + 30 * DAY_MS,
            None,
            "stopped".into(),
            None
        )
    );
    node.engine.gc_once(later + 30 * DAY_MS - 1).await?;
    let remaining_runs: u64 = node
        .engine
        .reads()
        .read(
            |conn| Ok(conn.query_row("SELECT count(*) FROM workflow_runs", [], |row| row.get(0))?),
        )
        .await?;
    assert_eq!(remaining_runs, 1);
    node.engine.gc_once(later + 30 * DAY_MS).await?;
    let remaining: (u64, u64, u64) = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT (SELECT count(*) FROM workflow_runs),
                    (SELECT count(*) FROM workflow_steps),
                    (SELECT count(*) FROM operations WHERE actor_id = 'run')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?)
        })
        .await?;
    assert_eq!(remaining, (0, 0, 0));
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rollback_does_not_publish_and_prune_stops_at_first_live_event() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    let db = node.engine.db();
    let mut events = db.subscribe();
    let overflow = db
        .write_sqlite(|tx| {
            let event = append_event(
                tx,
                100,
                EventKind::MessageCreated,
                AggregateType::Message,
                "oversized",
                None,
                json!({ "body": "x".repeat(super::MAX_EVENT_FRAME_BYTES) }),
            )?;
            Ok(WriteOutcome::with_events((), vec![event]))
        })
        .await;
    assert!(
        matches!(overflow, Err(cooplet_storage::StorageError::Sqlite(error)) if event_frame_too_large(&error))
    );
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    db.write_sqlite(|tx| {
        let events = [100, 102, 99]
            .into_iter()
            .map(|time| {
                append_event(
                    tx,
                    time,
                    EventKind::NodeStatus,
                    AggregateType::Node,
                    "status",
                    None,
                    json!({}),
                )
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(WriteOutcome::with_events((), events))
    })
    .await?;
    for seq in 1..=3 {
        assert_eq!(events.try_recv()?.seq, seq);
    }
    node.engine.gc_once(7 * DAY_MS + 101).await?;
    let window = node
        .engine
        .reads()
        .read(|conn| {
            let floor_head: (u64, u64) = conn.query_row(
                "SELECT events_pruned_through_seq, events_head_seq FROM node_meta",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let times = conn
                .prepare("SELECT created_at_ms FROM events ORDER BY seq")?
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((floor_head, times))
        })
        .await?;
    assert_eq!(window, ((1, 3), vec![102, 99]));
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn gc_expires_eligible_batches_and_preserves_active_references() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    let deadline = 40 * DAY_MS;
    node.engine.db().write_sqlite(move |tx| {
        tx.execute(
            "WITH RECURSIVE batch(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM batch WHERE n < 230)
             INSERT INTO participant_credentials
                 (token_id, principal_kind, actor_id, scopes_json, verifier_hmac, expires_at_ms, created_at_ms)
             SELECT 'expired_' || n, 'adapter', 'test', '[]', 'verifier_' || n, ?1, 0 FROM batch",
            [deadline - DAY_MS],
        )?;
        tx.execute(
            "WITH RECURSIVE batch(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM batch WHERE n < 230)
             INSERT INTO operations (operation_id, actor_id, operation_key, kind, request_sha256,
                                     state, created_at_ms, updated_at_ms, expires_at_ms)
             SELECT 'terminal_' || n, 'test', n, 'message.send', 'digest', 'succeeded', 0, 0, ?1 FROM batch",
            [deadline],
        )?;
        tx.execute(
            "INSERT INTO operations(operation_id, actor_id, operation_key, kind, request_sha256, state,
                                   created_at_ms, updated_at_ms, expires_at_ms)
             VALUES ('live_operation','test','working','message.send','digest','in_progress',0,0,?1)",
            [deadline],
        )?;
        tx.execute(
            "INSERT INTO participants(participant_id, kind, name, state, created_at_ms, updated_at_ms, stopped_at_ms)
             VALUES ('kept_fence','agent','kept_fence','stopped',0,0,?1),
                    ('kept_queue','agent','kept_queue','stopped',0,0,?1),
                    ('expired_participant','agent','expired_participant','stopped',0,0,?1)",
            [deadline - 30 * DAY_MS],
        )?;
        tx.execute(
            "INSERT INTO thread_inject_fences(thread_id, participant_id, highest_dispatched_seq, state,
                                              updated_at_ms, expires_at_ms)
             VALUES ('unknown','kept_fence',1,'unknown',0,?1),
                    ('confirmed','kept_fence',2,'confirmed',0,?1)",
            [deadline],
        )?;
        tx.execute(
            "INSERT INTO messages(message_id, sender_id, recipient_id, body, delivery_mode,
                                   delivery_state, created_at_ms, expires_at_ms)
             VALUES ('queued_message','operator','kept_queue','','queue_until_idle','queued',0,?1),
                    ('old_message','operator','kept_queue','','direct_now','delivered',0,?1),
                    ('accepted_message','operator','kept_queue','','direct_now','accepted',0,?1)",
            [deadline],
        )?;
        tx.execute(
            "INSERT INTO intake_queue(participant_id, message_id, payload_envelope, state,
                                       enqueued_at_ms, deadline_at_ms, resolved_at_ms)
             VALUES ('kept_queue','queued_message','{}','dispatched',0,?1,NULL),
                    ('kept_queue','old_message','{}','delivered',0,?1,?2)",
            params![deadline, deadline - 7 * DAY_MS],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;

    assert_eq!(node.engine.gc_once(deadline - 1).await?.prune_batches, 1);
    let pruned = node.engine.gc_once(deadline).await?;
    assert!(pruned.prune_batches >= 4);
    assert!(!pruned.has_more);
    let counts: Vec<u64> = node
        .engine
        .reads()
        .read(|conn| {
            let tables = [
                "participant_credentials",
                "operations",
                "participants",
                "thread_inject_fences",
                "messages",
                "intake_queue",
            ];
            tables
                .into_iter()
                .map(|table| {
                    Ok(
                        conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                            row.get(0)
                        })?,
                    )
                })
                .collect()
        })
        .await?;
    // The operator and their credential account for one remaining row each.
    assert_eq!(counts, [1, 1, 3, 1, 2, 1]);
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn event_row_cap_counts_rows_across_sequence_gaps() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    node.engine.db().write_sqlite(|tx| {
        tx.execute_batch(
            "WITH RECURSIVE batch(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM batch WHERE n < 200001)
             INSERT INTO events (seq, event_id, kind, aggregate_type, aggregate_id, payload_json, created_at_ms)
             SELECT 2*n, printf('evt_%026d', n), 'node.status', 'node', 'status', '{}', 0 FROM batch;
             UPDATE node_meta SET events_head_seq = 400002 WHERE id = 1;",
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    assert!(!node.engine.gc_once(0).await?.has_more);
    let window: (u64, u64, u64, u64) = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT events_pruned_through_seq, events_head_seq, (SELECT count(*) FROM events),
                    (SELECT min(seq) FROM events) FROM node_meta",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?)
        })
        .await?;
    assert_eq!(window, (2, 400_002, 200_000, 4));
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn expired_blob_is_tombstoned_before_the_file_is_unlinked() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    let blobs = node.engine.blobs().ok_or("expected managed blob storage")?;
    let id = BlobId::generate()?;
    let expires_at = 60 * 60 * 1000;
    let (stored, ()) = blobs.store::<_, _, Infallible, _>(
        BlobInput {
            blob_id: id.clone(),
            original_name: "hello.txt".into(),
            expected_size: Some(5),
            expected_sha256: None,
        },
        b"hello".as_slice(),
        move |tx, blob| {
            tx.execute(
                "INSERT INTO blobs (blob_id, sha256, size_bytes, mime_type, original_name, safe_disk_name,
                                    disk_rel_path, owner_actor_id, created_at_ms, expires_at_ms)
                 VALUES (?1, ?2, ?3, 'text/plain', ?4, ?5, ?6, 'operator', 0, ?7)",
                params![
                    blob.blob_id.as_str(), blob.sha256, blob.size_bytes, blob.original_name,
                    blob.safe_disk_name, blob.disk_rel_path.to_string_lossy(), expires_at,
                ],
            )?;
            Ok(WriteOutcome::new(()))
        },
    ).await?;
    assert_eq!(stored.safe_disk_name, "hello.txt");
    assert_eq!(fs::read(&stored.absolute_path)?, b"hello");
    node.engine.gc_once(expires_at - 1).await?;
    assert!(blobs.open_blob(&id).await?.is_some());

    let db = node.engine.db();
    let mut broadcast = db.subscribe();
    db.write_sqlite(move |tx| {
        let hooks = NodeGcHooks::default();
        hooks.mark_ready();
        let batch = hooks.prune_batch(tx, expires_at, 200)?;
        Ok(WriteOutcome::with_events((), batch.events))
    })
    .await?;
    assert_eq!(broadcast.try_recv()?.kind, EventKind::BlobDeleted);
    assert!(stored.absolute_path.exists());
    assert!(blobs.open_blob(&id).await?.is_none());

    let collected = node.engine.gc_once(expires_at).await?;
    assert_eq!(collected.tombstones_completed, 1);
    assert!(!stored.absolute_path.exists());
    let row_exists: bool = node
        .engine
        .reads()
        .read(move |conn| {
            Ok(conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM blobs WHERE blob_id = ?1)",
                [id.as_str()],
                |row| row.get(0),
            )?)
        })
        .await?;
    assert!(!row_exists);
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn startup_revokes_all_non_stopped_agent_credentials_and_sweeps_disk_files() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    let state_dir = node.state_dir.clone();
    let creds_dir = state_dir.join("creds");
    let runtime_dir = state_dir.join("runtime");
    fs::create_dir_all(&runtime_dir)?;
    PrivateFs::write_private_file_atomic(&creds_dir, "unrelated.json", b"{}")?;
    let mut ids = Vec::new();
    for state in ["idle", "working", "unknown", "stopped"] {
        let id = cooplet_protocol::ParticipantId::generate()?.to_string();
        ids.push((id.clone(), state));
        let cred_name = format!("{id}.g1.json");
        PrivateFs::write_private_file_atomic(&creds_dir, &cred_name, b"{}")?;
        for gen_dir in [format!("{id}.g0"), format!("{id}.g1")] {
            let gen_runtime = runtime_dir.join(gen_dir);
            fs::create_dir_all(&gen_runtime)?;
            fs::write(gen_runtime.join("socket"), b"stale")?;
        }
        let state = state.to_owned();
        node.engine
            .db()
            .write_sqlite(move |tx| {
                let stopped_at = (state == "stopped").then_some(5i64);
                tx.execute(
                    "INSERT INTO participants(participant_id, kind, name, state, generation, created_at_ms, updated_at_ms, stopped_at_ms)
                     VALUES (?1, 'agent', ?1, ?2, 1, 1, 1, ?3)",
                    params![id, state, stopped_at],
                )?;
                let revoked_at = (state == "stopped").then_some(5i64);
                tx.execute(
                    "INSERT INTO participant_credentials(token_id, participant_id, principal_kind, actor_id, scopes_json, verifier_hmac, created_at_ms, revoked_at_ms)
                     VALUES (?1 || '_tok', ?1, 'participant', ?1, '[\"collaborate\"]', ?1 || '_hmac', 1, ?2)",
                    params![id, revoked_at],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
    }
    drop(node.listener);
    node.engine.shutdown().await?;

    let restarted = scratch.start().await?;
    let active_agent_creds: u64 = restarted
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT count(*) FROM participant_credentials
                 WHERE principal_kind = 'participant' AND revoked_at_ms IS NULL",
                [],
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(active_agent_creds, 0);
    assert!(creds_dir.join("operator.json").is_file());
    assert!(creds_dir.join("unrelated.json").is_file());
    for (id, initial_state) in &ids {
        assert!(
            !creds_dir.join(format!("{id}.g1.json")).exists(),
            "revoked credential file for {id} ({initial_state}) should be swept on startup"
        );
        assert!(
            !runtime_dir.join(format!("{id}.g0")).exists(),
            "stale previous-generation runtime dir for {id} should be swept on startup"
        );
        if *initial_state == "stopped" {
            assert!(
                !runtime_dir.join(format!("{id}.g1")).exists(),
                "stopped runtime dir for {id} should be swept on startup"
            );
        } else {
            assert!(
                runtime_dir.join(format!("{id}.g1")).is_dir(),
                "current runtime dir for non-stopped {id} ({initial_state}) must stay until stop"
            );
        }
    }
    restarted.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn reboot_unblocks_ephemeral_managed_participant_even_with_runtime_pid() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    node.engine
        .db()
        .write_sqlite(|tx| {
            tx.execute_batch(
                "INSERT INTO workflow_runs(run_id, workflow_name, entry_path, workspace_dir, initiator_actor_id, state,
                                           runner_pid, runner_pgid, runner_start_token, started_at_ms, updated_at_ms, expires_at_ms)
                 VALUES ('run_ephem','demo','w.ts','.','operator','interrupted',999999,999999,'linux:00000000-0000-0000-0000-000000000000:1',0,0,1000);
                 INSERT INTO participants(participant_id, kind, name, state, ephemeral_for_run_id,
                                          process_ownership, runtime_pid, runtime_pgid, runtime_start_token,
                                          adapter_binding_json, created_at_ms, updated_at_ms)
                 VALUES ('ephem_pid','agent','ephem_pid','unknown','run_ephem','managed',
                         888888,888888,'linux:00000000-0000-0000-0000-000000000000:2',
                         '{\"core\":{\"lifecycle_block\":{\"kind\":\"cleanup_unverified\",\"boot_id\":\"prev-boot\"}}}',0,0);",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    recover_with_boot_id(&node.engine, 25, Some("next-boot".into())).await?;
    let row: (String, Option<i64>, Option<i64>, String) = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT state, stopped_at_ms, runtime_pid, adapter_binding_json
                 FROM participants WHERE participant_id = 'ephem_pid'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?)
        })
        .await?;
    assert_eq!(row.0, "stopped");
    assert_eq!(row.1, Some(25));
    assert_eq!(row.2, None);
    let binding: Value = serde_json::from_str(&row.3)?;
    assert!(binding["core"].get("lifecycle_block").is_none());
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn offline_state_check_rejects_uninitialized_user_version_zero() -> TestResult {
    let scratch = Scratch::new()?;
    let state_dir = PrivateFs::ensure_private_dir(&scratch.0.join("uninit"))?;
    let db_path = state_dir.join("node.sqlite3");
    let lock_path = state_dir.join("node.lock");
    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA page_size = 4096;
         PRAGMA auto_vacuum = INCREMENTAL;
         PRAGMA journal_mode = WAL;
         CREATE TABLE placeholder(id INTEGER PRIMARY KEY);
         PRAGMA wal_checkpoint(TRUNCATE);",
    )?;
    drop(conn);
    for suffix in ["-wal", "-shm"] {
        let _ = fs::remove_file(state_dir.join(format!("node.sqlite3{suffix}")));
    }
    fs::write(&lock_path, b"")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o600))?;
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))?;
    }
    let err = crate::config::check_offline_state(&state_dir).await.err();
    assert!(
        matches!(
            err,
            Some(cooplet_storage::StorageError::IncompatibleDatabase(ref msg))
                if msg.contains("has not been initialized")
        ),
        "unexpected offline check result: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn events_kind_queries_and_blob_acl_use_indexes_without_table_scans() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;
    node.engine
        .reads()
        .read(|conn| {
            let single_sql = format!(
                "EXPLAIN QUERY PLAN {}",
                crate::http::events::events_query_sql(1)
            );
            let single_plan: Vec<String> = conn
                .prepare(&single_sql)?
                .query_map(params![0i64, 100i64, "blob.stored", 101i64], |row| {
                    row.get(3)
                })?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                single_plan
                    .iter()
                    .any(|line| line.contains("idx_events_kind_seq")),
                "single-kind plan: {single_plan:?}"
            );
            assert!(
                single_plan
                    .iter()
                    .all(|line| !line.contains("SCAN") && !line.contains("TEMP B-TREE")),
                "single-kind plan: {single_plan:?}"
            );

            let multi_sql = format!(
                "EXPLAIN QUERY PLAN {}",
                crate::http::events::events_query_sql(2)
            );
            let multi_plan: Vec<String> = conn
                .prepare(&multi_sql)?
                .query_map(
                    params![0i64, 100i64, "blob.stored", "message.created", 101i64],
                    |row| row.get(3),
                )?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                multi_plan
                    .iter()
                    .any(|line| line.contains("idx_events_kind_seq")),
                "multi-kind plan: {multi_plan:?}"
            );
            assert!(
                multi_plan
                    .iter()
                    .all(|line| !line.contains("SCAN") && !line.contains("TEMP B-TREE")),
                "multi-kind plan: {multi_plan:?}"
            );

            let acl_sql = format!("EXPLAIN QUERY PLAN {}", crate::http::blobs::BLOB_ACL_QUERY);
            let acl_plan: Vec<String> = conn
                .prepare(&acl_sql)?
                .query_map(params!["part_actor", "blob_id"], |row| row.get(3))?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                acl_plan
                    .iter()
                    .any(|line| line.contains("idx_messages_recipient_attachments")),
                "blob ACL plan: {acl_plan:?}"
            );
            assert!(
                acl_plan
                    .iter()
                    .any(|line| line.contains("idx_messages_sender_attachments")),
                "blob ACL plan: {acl_plan:?}"
            );
            assert!(
                acl_plan.iter().all(|line| !line.contains("SCAN m")),
                "blob ACL plan: {acl_plan:?}"
            );

            let scoped_sql = format!(
                "EXPLAIN QUERY PLAN {}",
                crate::delivery::queries::scoped_messages_sql()
            );
            let scoped_plan: Vec<String> = conn
                .prepare(&scoped_sql)?
                .query_map(params!["", 101i64, "part_actor", 0i64, false], |row| {
                    row.get(3)
                })?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                scoped_plan
                    .iter()
                    .any(|line| line.contains("idx_messages_sender_id")),
                "scoped messages plan: {scoped_plan:?}"
            );
            assert!(
                scoped_plan
                    .iter()
                    .any(|line| line.contains("idx_messages_recipient_id")),
                "scoped messages plan: {scoped_plan:?}"
            );
            assert!(
                scoped_plan
                    .iter()
                    .all(|line| !line.contains("SCAN") && !line.contains("TEMP B-TREE")),
                "scoped messages plan: {scoped_plan:?}"
            );

            let active_name_sql = format!(
                "EXPLAIN QUERY PLAN {}",
                crate::clients::ACTIVE_NAME_EXISTS_QUERY
            );
            let active_name_plan: Vec<String> = conn
                .prepare(&active_name_sql)?
                .query_map(params!["worker", "part_self"], |row| row.get(3))?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                active_name_plan
                    .iter()
                    .any(|line| line.contains("idx_participants_active_name")),
                "active name plan: {active_name_plan:?}"
            );
            assert!(
                active_name_plan.iter().all(
                    |line| !line.contains("SCAN participants") && !line.contains("TEMP B-TREE")
                ),
                "active name plan: {active_name_plan:?}"
            );
            Ok(())
        })
        .await?;
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn recover_leaves_delegated_answers_unknown_and_uses_unresolved_fence_index() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.start().await?;

    node.engine
        .reads()
        .read(|conn| {
            let plan: Vec<String> = conn
                .prepare(
                    "EXPLAIN QUERY PLAN
                     UPDATE thread_inject_fences SET state = 'unknown', updated_at_ms = ?1
                     WHERE state = 'injecting' AND state IN ('injecting', 'unknown')",
                )?
                .query_map([100i64], |row| row.get(3))?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                plan.iter()
                    .any(|line| line.contains("idx_thread_fences_unresolved")),
                "unresolved fence recovery plan: {plan:?}"
            );
            Ok(())
        })
        .await?;

    node.engine
        .db()
        .write_sqlite(|tx| {
            tx.execute_batch(
                "INSERT INTO participants(participant_id, kind, name, state, created_at_ms, updated_at_ms)
                 VALUES ('agent_rec','agent','agent_rec','idle',0,0);
                 INSERT INTO messages(message_id, sender_id, recipient_id, body, question_json,
                                      delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                 VALUES ('q_local','agent_rec','operator','','{}','direct_now','delivered',0,10000),
                        ('q_deleg','agent_rec','operator','','{}','direct_now','delivered',0,10000);
                 INSERT INTO operations(operation_id, actor_id, operation_key, kind, request_sha256,
                                        state, bridge_origin, created_at_ms, updated_at_ms, expires_at_ms)
                 VALUES ('op_local','operator','k_local','message.send','h1','in_progress',NULL,0,0,10000),
                        ('op_deleg','operator','k_deleg','message.send','h2','in_progress','hub_1/node_1',0,0,10000);
                 INSERT INTO messages(message_id, sender_id, recipient_id, in_reply_to, body, answer_json,
                                      delivery_mode, delivery_state, operation_id, created_at_ms, expires_at_ms)
                 VALUES ('a_local','operator','agent_rec','q_local','','{\"text\":\"ok\"}','direct_now','accepted','op_local',0,10000),
                        ('a_deleg','operator','agent_rec','q_deleg','','{\"text\":\"ok\"}','direct_now','accepted','op_deleg',0,10000);
                 INSERT INTO thread_inject_fences(thread_id, participant_id, highest_dispatched_seq,
                                                  state, updated_at_ms, expires_at_ms)
                 VALUES ('thr_1','agent_rec',1,'injecting',0,10000),
                        ('thr_2','agent_rec',2,'unknown',5,10000);",
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    recover_with_boot_id(&node.engine, 50, None).await?;
    let (local_state, deleg_state, f1, f2): (String, String, (String, i64), (String, i64)) = node
        .engine
        .reads()
        .read(|conn| {
            Ok((
                conn.query_row(
                    "SELECT state FROM operations WHERE operation_id = 'op_local'",
                    [],
                    |r| r.get(0),
                )?,
                conn.query_row(
                    "SELECT state FROM operations WHERE operation_id = 'op_deleg'",
                    [],
                    |r| r.get(0),
                )?,
                conn.query_row(
                    "SELECT state, updated_at_ms FROM thread_inject_fences WHERE thread_id = 'thr_1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?,
                conn.query_row(
                    "SELECT state, updated_at_ms FROM thread_inject_fences WHERE thread_id = 'thr_2'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?,
            ))
        })
        .await?;
    assert_eq!(local_state, "succeeded");
    assert_eq!(deleg_state, "unknown");
    assert_eq!(f1, ("unknown".into(), 50));
    assert_eq!(f2, ("unknown".into(), 5));
    node.engine.shutdown().await?;
    Ok(())
}
