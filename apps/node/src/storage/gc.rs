use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use cooplet_protocol::{AggregateType, BlobId, EventKind};
use cooplet_storage::{BlobRecord, BlobState, BlobTombstone, EventNotice, GcBatchOutcome, GcHooks};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde_json::json;

use super::{
    DAY_MS, bad_data,
    events::{append_event, delivery_event},
};

const EVENT_MAX_ROWS: u64 = 200_000;
pub(crate) const QUEUE_EXPIRED_DETAIL: &str = "queue_deadline_expired";

#[derive(Default)]
pub struct NodeGcHooks {
    ready: AtomicBool,
    next_category: AtomicUsize,
}

impl NodeGcHooks {
    /// Begin retention after node identity and crash recovery are committed.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }
}

impl GcHooks for NodeGcHooks {
    fn prune_batch(
        &self,
        tx: &Transaction<'_>,
        now_ms: i64,
        limit: usize,
    ) -> rusqlite::Result<GcBatchOutcome> {
        if limit == 0 || !self.ready.load(Ordering::Acquire) {
            return Ok(GcBatchOutcome::default());
        }
        let limit = i64::try_from(limit).map_err(bad_data)?;
        let start = self.next_category.fetch_add(1, Ordering::Relaxed) % CATEGORIES.len();
        let categories = CATEGORIES.iter().cycle().skip(start).take(CATEGORIES.len());
        let mut events = Vec::new();
        for category in categories {
            if let Some(work) = category.try_prune(tx, now_ms, limit)? {
                events = work.events;
                if work.has_more_self {
                    return Ok(GcBatchOutcome {
                        events,
                        has_more: true,
                    });
                }
                // Commit a single category at a time. Recheck all other predicates:
                // one prune may release a hold that protected another table.
                for remaining in CATEGORIES {
                    if matches!(category, Category::Events) && matches!(remaining, Category::Events)
                    {
                        continue;
                    }
                    if remaining.has_work(tx, now_ms)? {
                        return Ok(GcBatchOutcome {
                            events,
                            has_more: true,
                        });
                    }
                }
                break;
            }
        }
        Ok(GcBatchOutcome {
            events,
            has_more: false,
        })
    }

    fn fetch_pending_blob_tombstones(
        &self,
        connection: &Connection,
        after: Option<&BlobTombstone>,
        limit: usize,
    ) -> rusqlite::Result<Vec<BlobTombstone>> {
        let limit = i64::try_from(limit).map_err(bad_data)?;
        let decode = |row: &Row<'_>| {
            Ok(BlobTombstone {
                record: read_blob_record(row)?,
                deleted_at_ms: row.get(3)?,
            })
        };
        match after {
            Some(after) => connection
                .prepare_cached(
                    "SELECT blob_id, disk_rel_path, size_bytes, deleted_at_ms FROM blobs
                     WHERE deleted_at_ms IS NOT NULL AND (deleted_at_ms, blob_id) > (?1, ?2)
                     ORDER BY deleted_at_ms, blob_id LIMIT ?3",
                )?
                .query_map(
                    params![after.deleted_at_ms, after.record.blob_id.as_str(), limit],
                    decode,
                )?
                .collect(),
            None => connection
                .prepare_cached(
                    "SELECT blob_id, disk_rel_path, size_bytes, deleted_at_ms FROM blobs
                     WHERE deleted_at_ms IS NOT NULL ORDER BY deleted_at_ms, blob_id LIMIT ?1",
                )?
                .query_map([limit], decode)?
                .collect(),
        }
    }

    fn ack_blob_tombstone(
        &self,
        tx: &Transaction<'_>,
        observed: &BlobTombstone,
    ) -> rusqlite::Result<bool> {
        let path = observed.record.disk_rel_path.to_str().ok_or_else(|| {
            bad_data(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid blob record path",
            ))
        })?;
        let changed = tx.execute(
            "DELETE FROM blobs
             WHERE blob_id = ?1 AND disk_rel_path = ?2 AND size_bytes = ?3 AND deleted_at_ms = ?4",
            params![
                observed.record.blob_id.as_str(),
                path,
                observed.record.size_bytes,
                observed.deleted_at_ms
            ],
        )?;
        Ok(changed == 1)
    }

    fn lookup_blobs(
        &self,
        connection: &Connection,
        blob_ids: &[BlobId],
    ) -> rusqlite::Result<BTreeMap<BlobId, BlobState>> {
        let mut statement = connection.prepare_cached(
            "SELECT blob_id, disk_rel_path, size_bytes, deleted_at_ms FROM blobs WHERE blob_id = ?1",
        )?;
        blob_ids
            .iter()
            .map(|id| {
                let state = statement
                    .query_row([id.as_str()], |row| {
                        let record = read_blob_record(row)?;
                        let timestamp: Option<i64> = row.get(3)?;
                        Ok(match timestamp {
                            Some(deleted_at_ms) => BlobState::Tombstone(BlobTombstone {
                                record,
                                deleted_at_ms,
                            }),
                            None => BlobState::Live(record),
                        })
                    })
                    .optional()?
                    .unwrap_or(BlobState::Absent);
                Ok((id.clone(), state))
            })
            .collect()
    }
}

fn read_blob_record(row: &Row<'_>) -> rusqlite::Result<BlobRecord> {
    Ok(BlobRecord {
        blob_id: row.get::<_, String>(0)?.parse().map_err(bad_data)?,
        disk_rel_path: PathBuf::from(row.get::<_, String>(1)?),
        size_bytes: row.get(2)?,
    })
}

const QUEUE_DEADLINES: &str = "SELECT queue_id FROM intake_queue
    WHERE state = 'queued' AND deadline_at_ms <= ?1 ORDER BY deadline_at_ms LIMIT ?2";
const EXPIRED_BLOBS: &str = "SELECT rowid FROM blobs
    WHERE deleted_at_ms IS NULL AND expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2";
const STOPPED_PARTICIPANTS: &str = "SELECT p.rowid FROM participants AS p
    WHERE p.kind = 'agent' AND p.state = 'stopped' AND p.stopped_at_ms IS NOT NULL
      AND p.stopped_at_ms <= ?1
      AND NOT EXISTS (
          SELECT 1 FROM thread_inject_fences AS f
          WHERE f.participant_id = p.participant_id AND f.state IN ('injecting', 'unknown')
      )
      AND NOT EXISTS (
          SELECT 1 FROM intake_queue AS q
          WHERE q.participant_id = p.participant_id AND q.state IN ('queued', 'dispatched')
      )
      AND NOT EXISTS (
          SELECT 1 FROM workflow_runs AS r
          WHERE r.run_id = p.ephemeral_for_run_id AND r.state IN ('running', 'interrupted')
            AND r.finished_at_ms IS NULL
      )
    ORDER BY p.stopped_at_ms LIMIT ?2";
const EXPIRED_CREDENTIALS: &str = "SELECT rowid FROM participant_credentials
    WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2";
const REVOKED_CREDENTIALS: &str = "SELECT rowid FROM participant_credentials
    WHERE revoked_at_ms IS NOT NULL AND revoked_at_ms <= ?1 ORDER BY revoked_at_ms LIMIT ?2";
const EXPIRED_OPERATIONS: &str = "SELECT o.rowid FROM operations AS o
    WHERE o.expires_at_ms <= ?1 AND o.state NOT IN ('accepted', 'in_progress')
      AND NOT EXISTS (
          SELECT 1 FROM workflow_runs AS r
          WHERE r.run_id = o.actor_id AND r.state IN ('running', 'interrupted')
      )
    ORDER BY o.expires_at_ms LIMIT ?2";
const EXPIRED_MESSAGES: &str = "SELECT m.rowid FROM messages AS m
    WHERE m.expires_at_ms <= ?1 AND m.delivery_state != 'accepted'
      AND NOT EXISTS (
          SELECT 1 FROM intake_queue AS q
          WHERE q.message_id = m.message_id AND q.state IN ('queued', 'dispatched')
      )
    ORDER BY m.expires_at_ms LIMIT ?2";
const RESOLVED_QUEUE: &str = "SELECT queue_id FROM intake_queue
    WHERE resolved_at_ms IS NOT NULL AND resolved_at_ms <= ?1
      AND state IN ('delivered', 'expired', 'cancelled', 'unknown')
    ORDER BY resolved_at_ms LIMIT ?2";
const CONFIRMED_FENCES: &str = "SELECT rowid FROM thread_inject_fences
    WHERE state = 'confirmed' AND expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2";
const FINISHED_RUNS: &str = "SELECT rowid FROM workflow_runs
    WHERE finished_at_ms IS NOT NULL AND expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2";

#[derive(Clone, Copy)]
enum Category {
    QueueDeadlines,
    BlobTombstones,
    Delete {
        table: &'static str,
        select: &'static str,
        retention_ms: i64,
    },
    Events,
}

const CATEGORIES: &[Category] = &[
    Category::QueueDeadlines,
    Category::BlobTombstones,
    deletion("participants", STOPPED_PARTICIPANTS, 30 * DAY_MS),
    deletion("participant_credentials", EXPIRED_CREDENTIALS, DAY_MS),
    deletion("participant_credentials", REVOKED_CREDENTIALS, DAY_MS),
    deletion("operations", EXPIRED_OPERATIONS, 0),
    deletion("messages", EXPIRED_MESSAGES, 0),
    deletion("intake_queue", RESOLVED_QUEUE, 7 * DAY_MS),
    deletion("thread_inject_fences", CONFIRMED_FENCES, 0),
    deletion("workflow_runs", FINISHED_RUNS, 0),
    Category::Events,
];

const fn deletion(table: &'static str, select: &'static str, retention_ms: i64) -> Category {
    Category::Delete {
        table,
        select,
        retention_ms,
    }
}

struct PruneResult {
    events: Vec<EventNotice>,
    has_more_self: bool,
}

impl Category {
    fn has_work(self, tx: &Transaction<'_>, now: i64) -> rusqlite::Result<bool> {
        match self {
            Self::QueueDeadlines => any_row(tx, QUEUE_DEADLINES, now),
            Self::BlobTombstones => any_row(tx, EXPIRED_BLOBS, now),
            Self::Delete {
                select,
                retention_ms,
                ..
            } => any_row(tx, select, now.saturating_sub(retention_ms)),
            Self::Events => Ok(prunable_event_prefix(tx, now, 1)?.last.is_some()),
        }
    }

    fn try_prune(
        self,
        tx: &Transaction<'_>,
        now: i64,
        limit: i64,
    ) -> rusqlite::Result<Option<PruneResult>> {
        match self {
            Self::QueueDeadlines => {
                let events = expire_queued(tx, now, limit)?;
                if events.is_empty() {
                    Ok(None)
                } else {
                    let has_more_self = i64::try_from(events.len()).ok() == Some(limit)
                        && self.has_work(tx, now)?;
                    Ok(Some(PruneResult {
                        events,
                        has_more_self,
                    }))
                }
            }
            Self::BlobTombstones => {
                let events = tombstone_blobs(tx, now, limit)?;
                if events.is_empty() {
                    Ok(None)
                } else {
                    let has_more_self = i64::try_from(events.len()).ok() == Some(limit)
                        && self.has_work(tx, now)?;
                    Ok(Some(PruneResult {
                        events,
                        has_more_self,
                    }))
                }
            }
            Self::Delete {
                table,
                select,
                retention_ms,
            } => {
                let sql = format!("DELETE FROM {table} WHERE rowid IN ({select})");
                let deleted = tx
                    .prepare_cached(&sql)?
                    .execute(params![now.saturating_sub(retention_ms), limit])?;
                if deleted == 0 {
                    Ok(None)
                } else {
                    let has_more_self =
                        i64::try_from(deleted).ok() == Some(limit) && self.has_work(tx, now)?;
                    Ok(Some(PruneResult {
                        events: Vec::new(),
                        has_more_self,
                    }))
                }
            }
            Self::Events => prune_events(tx, now, limit),
        }
    }
}

fn any_row(tx: &Transaction<'_>, sql: &str, cutoff: i64) -> rusqlite::Result<bool> {
    let mut statement = tx.prepare_cached(sql)?;
    statement.exists(params![cutoff, 1])
}

pub(super) fn expire_queued(
    tx: &Transaction<'_>,
    now: i64,
    limit: i64,
) -> rusqlite::Result<Vec<EventNotice>> {
    let sql = format!(
        "UPDATE intake_queue SET state = 'expired', resolved_at_ms = ?1, payload_envelope = ''
         WHERE queue_id IN ({QUEUE_DEADLINES}) RETURNING queue_id, message_id"
    );
    let mut expired = tx
        .prepare_cached(&sql)?
        .query_map(params![now, limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    expired.sort_unstable();
    expired
        .into_iter()
        .map(|(_, id)| {
            tx.execute(
                "UPDATE messages SET delivery_state = 'expired', delivery_detail = ?1 WHERE message_id = ?2",
                params![QUEUE_EXPIRED_DETAIL, id],
            )?;
            delivery_event(tx, now, &id, "expired", QUEUE_EXPIRED_DETAIL)
        })
        .collect()
}

fn tombstone_blobs(
    tx: &Transaction<'_>,
    now: i64,
    limit: i64,
) -> rusqlite::Result<Vec<EventNotice>> {
    let sql = format!(
        "UPDATE blobs SET deleted_at_ms = ?1 WHERE rowid IN ({EXPIRED_BLOBS})
         RETURNING blob_id, original_name, mime_type, size_bytes, sha256"
    );
    let mut blobs = tx
        .prepare_cached(&sql)?
        .query_map(params![now, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    blobs.sort_unstable_by(|first, second| first.0.cmp(&second.0));
    blobs
        .into_iter()
        .map(|(id, name, mime_type, size, sha256)| {
            append_event(
                tx,
                now,
                EventKind::BlobDeleted,
                AggregateType::Blob,
                &id,
                None,
                json!({
                    "blob_id": id,
                    "original_name": name,
                    "mime_type": mime_type,
                    "size_bytes": size,
                    "sha256": sha256,
                }),
            )
        })
        .collect()
}

const EVENT_PREFIX: &str = "SELECT seq, created_at_ms FROM events
    WHERE seq > ?1 ORDER BY seq LIMIT ?2";

struct EventPrefixPlan {
    last: Option<u64>,
    has_more: bool,
}

fn prunable_event_prefix(
    tx: &Transaction<'_>,
    now: i64,
    limit: i64,
) -> rusqlite::Result<EventPrefixPlan> {
    let (floor, head): (u64, u64) = tx.query_row(
        "SELECT events_pruned_through_seq, events_head_seq FROM node_meta WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // The watermark is advanced in the same transaction as prefix deletion.
    // Count live rows only when the sequence span could exceed the cap. SQLite
    // can count b-tree pages without walking the event payloads or sequence gaps.
    let mut excess = if head.saturating_sub(floor) > EVENT_MAX_ROWS {
        tx.query_row("SELECT count(*) FROM events", [], |row| {
            row.get::<_, u64>(0)
        })?
        .saturating_sub(EVENT_MAX_ROWS)
    } else {
        0
    };
    let cutoff = now.saturating_sub(7 * DAY_MS);
    let mut statement = tx.prepare_cached(EVENT_PREFIX)?;
    let fetch = limit.saturating_add(1);
    let mut rows = statement.query(params![floor, fetch])?;
    let mut last = None;
    let mut taken = 0i64;
    let mut has_more = false;
    while let Some(row) = rows.next()? {
        let seq: u64 = row.get(0)?;
        let created_at_ms: i64 = row.get(1)?;
        if created_at_ms > cutoff && excess == 0 {
            break;
        }
        if taken == limit {
            has_more = true;
            break;
        }
        excess = excess.saturating_sub(1);
        last = Some(seq);
        taken += 1;
    }
    Ok(EventPrefixPlan { last, has_more })
}

fn prune_events(
    tx: &Transaction<'_>,
    now: i64,
    limit: i64,
) -> rusqlite::Result<Option<PruneResult>> {
    let plan = prunable_event_prefix(tx, now, limit)?;
    let Some(last) = plan.last else {
        return Ok(None);
    };
    tx.execute("DELETE FROM events WHERE seq <= ?1", [last])?;
    tx.execute(
        "UPDATE node_meta SET events_pruned_through_seq = MAX(events_pruned_through_seq, ?1)
         WHERE id = 1",
        [last],
    )?;
    Ok(Some(PruneResult {
        events: Vec::new(),
        has_more_self: plan.has_more,
    }))
}

#[cfg(test)]
pub(super) fn indexed_gc_statements() -> Vec<String> {
    let mut statements = [QUEUE_DEADLINES, EXPIRED_BLOBS, EVENT_PREFIX]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for category in CATEGORIES {
        if let Category::Delete { table, select, .. } = category {
            statements.push(format!("DELETE FROM {table} WHERE rowid IN ({select})"));
        }
    }
    statements
}
