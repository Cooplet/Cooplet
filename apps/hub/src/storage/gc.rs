use std::{
    collections::BTreeMap,
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

use cooplet_protocol::{AggregateType, BlobId, EventEnvelope, EventKind, NodeId, ThreadId};
use cooplet_storage::{BlobRecord, BlobState, BlobTombstone, GcBatchOutcome, GcHooks};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde_json::json;

use super::{
    events::{NewEvent, advance_head, blob_audience, encode_error, record},
    hub_id, id_at, optional_id_at,
};

const DAY_MS: i64 = 86_400_000;
const EVENT_RETENTION: i64 = 14 * DAY_MS;
const EVENT_CAP: u64 = 500_000;
const PHASES: usize = 5;

pub(super) const PENDING_BLOB_TOMBSTONES: &str =
    "SELECT blob_id, disk_rel_path, size_bytes, deleted_at_ms FROM blobs
     WHERE deleted_at_ms IS NOT NULL AND (deleted_at_ms, blob_id) > (?1, ?2)
     ORDER BY deleted_at_ms, blob_id LIMIT ?3";
pub(super) const DELETE_EXPIRED_TOKENS: &str = "DELETE FROM auth_tokens WHERE token_id IN (
         SELECT token_id FROM auth_tokens
         WHERE expires_at_ms <= ?1 OR revoked_at_ms <= ?1 OR consumed_at_ms <= ?1
         LIMIT ?2
     )";

#[derive(Default)]
pub(super) struct HubGcHooks {
    next_phase: AtomicUsize,
}

impl GcHooks for HubGcHooks {
    fn prune_batch(
        &self,
        tx: &Transaction<'_>,
        now_ms: i64,
        limit: usize,
    ) -> rusqlite::Result<GcBatchOutcome> {
        let first = self.next_phase.load(Ordering::Relaxed);
        for offset in 0..PHASES {
            let phase = (first + offset) % PHASES;
            // A failed phase rolls back, but must not block the other phases.
            self.next_phase
                .store((phase + 1) % PHASES, Ordering::Relaxed);
            let mut events = Vec::new();
            let changed = match phase {
                0 => prune_tokens(tx, now_ms, limit)?,
                1 => prune_operations(tx, now_ms, limit / 2)?,
                2 => prune_messages(tx, now_ms, limit / 2)?,
                3 => {
                    events = prune_blobs(tx, now_ms, limit.saturating_sub(1) / 2)?;
                    events.len()
                }
                _ => prune_events(tx, now_ms, limit.saturating_sub(1))?,
            };
            if changed > 0 {
                return Ok(GcBatchOutcome {
                    events,
                    has_more: true,
                });
            }
        }
        Ok(GcBatchOutcome::default())
    }

    fn fetch_pending_blob_tombstones(
        &self,
        connection: &Connection,
        after: Option<&BlobTombstone>,
        limit: usize,
    ) -> rusqlite::Result<Vec<BlobTombstone>> {
        let (time, id) = after.map_or((i64::MIN, ""), |cursor| {
            (cursor.deleted_at_ms, cursor.record.blob_id.as_str())
        });
        let mut statement = connection.prepare_cached(PENDING_BLOB_TOMBSTONES)?;
        statement
            .query_map(params![time, id, limit], |row| {
                Ok(BlobTombstone {
                    record: blob_record(row, id_at(row, 0)?)?,
                    deleted_at_ms: row.get(3)?,
                })
            })?
            .collect()
    }

    fn ack_blob_tombstone(
        &self,
        tx: &Transaction<'_>,
        observed: &BlobTombstone,
    ) -> rusqlite::Result<bool> {
        let path = observed
            .record
            .disk_rel_path
            .to_str()
            .ok_or_else(|| encode_error(io::Error::other("invalid blob path encoding")))?;
        Ok(tx.execute(
            "DELETE FROM blobs WHERE blob_id = ?1 AND deleted_at_ms = ?2
                                 AND disk_rel_path = ?3 AND size_bytes = ?4",
            params![
                observed.record.blob_id.as_str(),
                observed.deleted_at_ms,
                path,
                observed.record.size_bytes
            ],
        )? == 1)
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
                        let record = blob_record(row, id.clone())?;
                        let deleted_at_ms: Option<i64> = row.get(3)?;
                        Ok(match deleted_at_ms {
                            Some(deleted_at_ms) => BlobState::Tombstone(BlobTombstone {
                                record,
                                deleted_at_ms,
                            }),
                            None => BlobState::Live(record),
                        })
                    })
                    .optional()?;
                Ok((id.clone(), state.unwrap_or(BlobState::Absent)))
            })
            .collect()
    }
}

fn blob_record(row: &Row<'_>, blob_id: BlobId) -> rusqlite::Result<BlobRecord> {
    Ok(BlobRecord {
        blob_id,
        disk_rel_path: row.get::<_, String>(1)?.into(),
        size_bytes: row.get(2)?,
    })
}

fn prune_tokens(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    tx.execute(
        DELETE_EXPIRED_TOKENS,
        params![now.saturating_sub(DAY_MS), limit],
    )
}

fn prune_operations(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    tx.execute(
        "DELETE FROM operations WHERE operation_id IN (
             SELECT operation_id FROM operations WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2
         )",
        params![now, limit],
    )
}

fn prune_messages(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    let detached = tx.execute(
        "UPDATE hub_messages SET in_reply_to = NULL WHERE rowid IN (
             SELECT child.rowid FROM hub_messages child WHERE child.in_reply_to IN (
                 SELECT message_id FROM hub_messages WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2
             ) LIMIT ?2
         )",
        params![now, limit],
    )?;
    let removed = tx.execute(
        "DELETE FROM hub_messages WHERE message_id IN (
             SELECT expired.message_id FROM (
                 SELECT message_id FROM hub_messages WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2
             ) expired
             WHERE NOT EXISTS (SELECT 1 FROM hub_messages child WHERE child.in_reply_to = expired.message_id)
         )",
        params![now, limit],
    )?;
    Ok(detached + removed)
}

struct ExpiredBlob {
    id: BlobId,
    name: String,
    mime_type: String,
    size: u64,
    sha256: String,
    owner: String,
    node_id: Option<NodeId>,
    thread_id: Option<ThreadId>,
    attached_at_ms: Option<i64>,
}

fn prune_blobs(
    tx: &Transaction<'_>,
    now: i64,
    limit: usize,
) -> rusqlite::Result<Vec<EventEnvelope>> {
    let mut statement = tx.prepare_cached(
        "SELECT blob_id, original_name, mime_type, size_bytes, sha256, owner_actor_id,
                scope_node_id, scope_thread_id, attached_at_ms
         FROM blobs WHERE deleted_at_ms IS NULL AND expires_at_ms <= ?1
         ORDER BY expires_at_ms LIMIT ?2",
    )?;
    let candidates = statement
        .query_map(params![now, limit], |row| {
            Ok(ExpiredBlob {
                id: id_at(row, 0)?,
                name: row.get(1)?,
                mime_type: row.get(2)?,
                size: row.get(3)?,
                sha256: row.get(4)?,
                owner: row.get(5)?,
                node_id: optional_id_at(row, 6)?,
                thread_id: optional_id_at(row, 7)?,
                attached_at_ms: row.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let hub_id = hub_id(tx)?;
    let mut events = Vec::with_capacity(candidates.len());
    for blob in candidates {
        tx.execute(
            "UPDATE blobs SET deleted_at_ms = ?1 WHERE blob_id = ?2",
            params![now, blob.id.as_str()],
        )?;
        events.push(record(
            tx,
            &hub_id,
            NewEvent {
                kind: EventKind::BlobDeleted,
                aggregate_type: AggregateType::Blob,
                aggregate_id: blob.id.as_str(),
                actor_id: None,
                created_at_ms: now,
                data: json!({
                    "blob_id": blob.id,
                    "original_name": blob.name,
                    "mime_type": blob.mime_type,
                    "size_bytes": blob.size,
                    "sha256": blob.sha256,
                }),
                audience: blob_audience(
                    &blob.owner,
                    blob.attached_at_ms,
                    blob.node_id,
                    blob.thread_id,
                ),
            },
        )?);
    }
    if let Some(last) = events.last() {
        advance_head(tx, last.seq)?;
    }
    Ok(events)
}

fn prune_events(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    let (floor, head): (u64, u64) = tx.query_row(
        "SELECT events_pruned_through_seq, events_head_seq FROM hub_meta WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // The sequence span is a cheap upper bound on the count. When the bound
    // exceeds the cap, find the actual oldest excess row without relying on dense IDs.
    let count_boundary: u64 = if head.saturating_sub(floor) > EVENT_CAP {
        tx.query_row(
            "SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?1",
            [EVENT_CAP],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0)
    } else {
        0
    };
    let cutoff = now.saturating_sub(EVENT_RETENTION);
    let mut statement = tx.prepare_cached(
        "SELECT seq, created_at_ms FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2",
    )?;
    let mut rows = statement.query(params![floor, limit])?;
    let mut through = floor;
    while let Some(row) = rows.next()? {
        let seq: u64 = row.get(0)?;
        let created_at_ms: i64 = row.get(1)?;
        if seq > count_boundary && created_at_ms > cutoff {
            break;
        }
        through = seq;
    }
    drop(rows);
    if through == floor {
        return Ok(0);
    }
    let removed = tx.execute(
        "DELETE FROM events WHERE seq > ?1 AND seq <= ?2",
        params![floor, through],
    )?;
    tx.execute(
        "UPDATE hub_meta SET events_pruned_through_seq = MAX(events_pruned_through_seq, ?1) WHERE id = 1",
        [through],
    )?;
    Ok(removed)
}
