use std::{
    collections::BTreeMap,
    io,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use cooplet_protocol::{AggregateType, AttachmentRef, BlobId, EventKind, NodeId, ThreadId};
use cooplet_storage::{BlobRecord, BlobState, BlobTombstone, EventNotice, GcBatchOutcome, GcHooks};
use rusqlite::{
    Connection, ErrorCode as SqliteErrorCode, OptionalExtension, Row, Transaction, params,
};

use super::{
    events::{BlobScope, NewEvent, advance_head, blob_event_data, encode_error, record},
    hub_id,
    sql::{parse_col, parse_opt_col},
};

const DAY_MS: i64 = 86_400_000;
const EVENT_RETENTION: i64 = 14 * DAY_MS;
const EVENT_CAP: u64 = 500_000;
const PHASES: usize = 7;

pub(super) const PENDING_BLOB_TOMBSTONES: &str =
    "SELECT blob_id, disk_rel_path, size_bytes, deleted_at_ms FROM blobs
     WHERE deleted_at_ms IS NOT NULL AND (deleted_at_ms, blob_id) > (?1, ?2)
     ORDER BY deleted_at_ms, blob_id LIMIT ?3";
// Post expiry is monotone within a board. This index order picks the same
// oldest prefix without sorting the entire expired range for each GC batch.
pub(super) const EXPIRED_POST_PREFIX: &str = "SELECT MAX(seq) FROM (
    SELECT seq FROM thread_posts WHERE thread_id=?1 AND expires_at_ms<=?2
    ORDER BY expires_at_ms,seq LIMIT ?3
)";
pub(super) const DELETE_EXPIRED_TOKENS: &str = "DELETE FROM auth_tokens WHERE token_id IN (
         SELECT token_id FROM auth_tokens
         WHERE expires_at_ms <= ?1 OR revoked_at_ms <= ?1 OR consumed_at_ms <= ?1
         LIMIT ?2
     )";

#[derive(Clone, Copy)]
struct EventCountCache {
    floor: u64,
    head: u64,
    min_count: u64,
    max_count: u64,
}

#[derive(Default)]
pub(super) struct HubGcHooks {
    next_phase: AtomicUsize,
    event_count_cache: Mutex<Option<EventCountCache>>,
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
                    // Blob pruning inserts blob.deleted events. Isolate DiskFull in a
                    // savepoint so pure-DELETE phases can reclaim pages in the same cycle.
                    tx.execute_batch("SAVEPOINT gc_blobs")?;
                    match prune_blobs(tx, now_ms, limit.saturating_sub(1) / 2) {
                        Ok(created) => {
                            tx.execute_batch("RELEASE SAVEPOINT gc_blobs")?;
                            events = created;
                            events.len()
                        }
                        Err(error)
                            if error.sqlite_error_code() == Some(SqliteErrorCode::DiskFull) =>
                        {
                            tx.execute_batch(
                                "ROLLBACK TO SAVEPOINT gc_blobs; RELEASE SAVEPOINT gc_blobs",
                            )?;
                            tracing::warn!("Skipping blob tombstone events while database is full");
                            0
                        }
                        Err(error) => {
                            let _ = tx.execute_batch(
                                "ROLLBACK TO SAVEPOINT gc_blobs; RELEASE SAVEPOINT gc_blobs",
                            );
                            return Err(error);
                        }
                    }
                }
                4 => self.prune_events_cached(tx, now_ms, limit.saturating_sub(1), EVENT_CAP)?,
                5 => prune_thread_posts(tx, now_ms, limit.saturating_sub(1))?,
                _ => prune_closed_threads(tx, now_ms, limit)?,
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
                    record: blob_record(row, parse_col(row, 0)?)?,
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
        let mut statement = tx.prepare_cached(
            "DELETE FROM blobs WHERE blob_id = ?1 AND deleted_at_ms = ?2
                                 AND disk_rel_path = ?3 AND size_bytes = ?4",
        )?;
        Ok(statement.execute(params![
            observed.record.blob_id.as_str(),
            observed.deleted_at_ms,
            path,
            observed.record.size_bytes
        ])? == 1)
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

impl HubGcHooks {
    fn prune_events_cached(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        limit: usize,
        cap: u64,
    ) -> rusqlite::Result<usize> {
        let mut cache = self
            .event_count_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_events(tx, now, limit, cap, &mut cache)
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
    let mut statement = tx.prepare_cached(DELETE_EXPIRED_TOKENS)?;
    statement.execute(params![now.saturating_sub(DAY_MS), limit])
}

fn prune_operations(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    let mut statement = tx.prepare_cached(
        "DELETE FROM operations WHERE operation_id IN (
             SELECT operation_id FROM operations WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2
         )",
    )?;
    statement.execute(params![now, limit])
}

fn prune_messages(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    let mut detach = tx.prepare_cached(
        "UPDATE hub_messages SET in_reply_to = NULL WHERE rowid IN (
             SELECT child.rowid FROM hub_messages child WHERE child.in_reply_to IN (
                 SELECT message_id FROM hub_messages WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2
             ) LIMIT ?2
         )",
    )?;
    let detached = detach.execute(params![now, limit])?;
    let mut delete = tx.prepare_cached(
        "DELETE FROM hub_messages WHERE message_id IN (
             SELECT expired.message_id FROM (
                 SELECT message_id FROM hub_messages WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2
             ) expired
             WHERE NOT EXISTS (SELECT 1 FROM hub_messages child WHERE child.in_reply_to = expired.message_id)
         )",
    )?;
    let removed = delete.execute(params![now, limit])?;
    Ok(detached + removed)
}

fn prune_thread_posts(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    let candidate: Option<ThreadId> = tx
        .query_row(
            "SELECT p.thread_id FROM thread_posts p JOIN threads t USING(thread_id)
         WHERE p.expires_at_ms<=?1 AND t.status='open' ORDER BY p.expires_at_ms LIMIT 1",
            [now],
            |row| parse_col(row, 0),
        )
        .optional()?;
    let Some(id) = candidate else { return Ok(0) };
    let through: Option<u64> = tx.query_row(
        EXPIRED_POST_PREFIX,
        params![id.as_str(), now, limit],
        |row| row.get(0),
    )?;
    let Some(through) = through else { return Ok(0) };
    let removed = tx.execute(
        "DELETE FROM thread_posts WHERE thread_id=?1 AND seq<=?2",
        params![id.as_str(), through],
    )?;
    tx.execute(
        "UPDATE threads SET floor_seq=MAX(floor_seq,?1) WHERE thread_id=?2",
        params![through, id.as_str()],
    )?;
    Ok(removed + 1)
}

fn prune_closed_threads(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let candidate: Option<(ThreadId, i64)> = tx.query_row(
        "SELECT thread_id,expires_at_ms FROM threads
         WHERE status='closed' AND expires_at_ms IS NOT NULL AND (expires_at_ms=0 OR expires_at_ms<=?1)
         ORDER BY expires_at_ms LIMIT 1",
        [now], |row| Ok((parse_col(row, 0)?, row.get(1)?)),
    ).optional()?;
    let Some((id, expires_at)) = candidate else {
        return Ok(0);
    };
    if expires_at != 0 {
        // A durable tombstone keeps removed historical membership from restoring access
        // after a wall-clock step backwards during a later cleanup batch.
        return tx.execute(
            "UPDATE threads SET expires_at_ms=0 WHERE thread_id=?1",
            [id.as_str()],
        );
    }
    for sql in [
        "DELETE FROM thread_posts WHERE rowid IN (SELECT rowid FROM thread_posts WHERE thread_id=?1 LIMIT ?2)",
        "DELETE FROM thread_pins WHERE rowid IN (SELECT rowid FROM thread_pins WHERE thread_id=?1 LIMIT ?2)",
        "UPDATE blobs SET scope_thread_id=NULL WHERE rowid IN (SELECT rowid FROM blobs WHERE scope_thread_id=?1 LIMIT ?2)",
        "DELETE FROM thread_members WHERE rowid IN (SELECT rowid FROM thread_members WHERE thread_id=?1 LIMIT ?2)",
    ] {
        let touched = tx.execute(sql, params![id.as_str(), limit])?;
        if touched != 0 {
            return Ok(touched);
        }
    }
    tx.execute("DELETE FROM threads WHERE thread_id=?1", [id.as_str()])
}

struct ExpiredBlob {
    attachment: AttachmentRef,
    owner: String,
    scope: BlobScope,
}

fn prune_blobs(tx: &Transaction<'_>, now: i64, limit: usize) -> rusqlite::Result<Vec<EventNotice>> {
    let mut statement = tx.prepare_cached(
        "SELECT blob_id, original_name, mime_type, size_bytes, sha256, owner_actor_id,
                scope_node_id, scope_peer_node_id, scope_thread_id, attached_at_ms
         FROM blobs WHERE deleted_at_ms IS NULL AND expires_at_ms <= ?1
         ORDER BY expires_at_ms LIMIT ?2",
    )?;
    let candidates = statement
        .query_map(params![now, limit], |row| {
            let blob_id: BlobId = parse_col(row, 0)?;
            let node_id: Option<NodeId> = parse_opt_col(row, 6)?;
            let peer_id: Option<NodeId> = parse_opt_col(row, 7)?;
            let thread_id: Option<ThreadId> = parse_opt_col(row, 8)?;
            let attached_at_ms: Option<i64> = row.get(9)?;
            Ok(ExpiredBlob {
                attachment: AttachmentRef {
                    blob_id,
                    name: row.get(1)?,
                    mime_type: row.get(2)?,
                    size_bytes: row.get(3)?,
                    sha256: row.get(4)?,
                },
                owner: row.get(5)?,
                scope: BlobScope::from_columns(attached_at_ms, node_id, peer_id, thread_id),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let hub_id = hub_id(tx)?;
    let mut events = Vec::with_capacity(candidates.len());
    let mut mark_deleted =
        tx.prepare_cached("UPDATE blobs SET deleted_at_ms = ?1 WHERE blob_id = ?2")?;
    for blob in candidates {
        mark_deleted.execute(params![now, blob.attachment.blob_id.as_str()])?;
        events.push(record(
            tx,
            &hub_id,
            NewEvent {
                kind: EventKind::BlobDeleted,
                aggregate_type: AggregateType::Blob,
                aggregate_id: blob.attachment.blob_id.as_str(),
                actor_id: None,
                created_at_ms: now,
                data: blob_event_data(&blob.attachment),
                audience: blob.scope.event_audience(&blob.owner),
            },
        )?);
    }
    if let Some(last) = events.last() {
        advance_head(tx, last.seq)?;
    }
    Ok(events)
}

fn prune_events(
    tx: &Transaction<'_>,
    now: i64,
    limit: usize,
    cap: u64,
    cache: &mut Option<EventCountCache>,
) -> rusqlite::Result<usize> {
    let mut meta = tx.prepare_cached(
        "SELECT events_pruned_through_seq, events_head_seq FROM hub_meta WHERE id = 1",
    )?;
    let (floor, head): (u64, u64) = meta.query_row([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    // The span bounds the count even with sequence gaps. When the span exceeds the
    // cap, reuse rollback-safe min/max count bounds before scanning the index.
    let (mut excess, bounds) = if head.saturating_sub(floor) <= cap {
        *cache = None;
        (0, None)
    } else if let Some(cached) = *cache
        && cached.floor == floor
        && head >= cached.head
    {
        let min_count = cached.min_count;
        let max_count = cached.max_count.saturating_add(head - cached.head);
        if min_count.saturating_sub(cap) >= limit as u64 {
            (min_count - cap, Some((min_count, max_count)))
        } else if max_count <= cap {
            (0, Some((min_count, max_count)))
        } else {
            let count: u64 = tx.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
            (count.saturating_sub(cap), Some((count, count)))
        }
    } else {
        let count: u64 = tx.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
        (count.saturating_sub(cap), Some((count, count)))
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
        if excess == 0 && created_at_ms > cutoff {
            break;
        }
        excess = excess.saturating_sub(1);
        through = seq;
    }
    drop(rows);
    if through == floor {
        if let Some((min_count, max_count)) = bounds {
            *cache = Some(EventCountCache {
                floor,
                head,
                min_count,
                max_count,
            });
        }
        return Ok(0);
    }
    let mut delete = tx.prepare_cached("DELETE FROM events WHERE seq > ?1 AND seq <= ?2")?;
    let removed = delete.execute(params![floor, through])?;
    let mut update_floor = tx.prepare_cached(
        "UPDATE hub_meta SET events_pruned_through_seq = MAX(events_pruned_through_seq, ?1) WHERE id = 1",
    )?;
    update_floor.execute([through])?;
    // Keying on the new floor invalidates the cache automatically if this tx rolls back.
    *cache = bounds.map(|(min_count, max_count)| EventCountCache {
        floor: through,
        head,
        min_count: min_count.saturating_sub(removed as u64),
        max_count: max_count.saturating_sub(removed as u64),
    });
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_thread_tombstone_survives_a_wall_clock_step_back_between_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut db = Connection::open_in_memory()?;
        db.execute_batch(include_str!("v1.sql"))?;
        let id = ThreadId::generate()?;
        db.execute(
            "INSERT INTO threads(thread_id,title,created_by_actor_id,status,head_seq,created_at_ms,
              updated_at_ms,closed_at_ms,expires_at_ms) VALUES(?1,'Closed','owner','closed',1,10,20,20,100)",
            [id.as_str()],
        )?;
        db.execute(
            "INSERT INTO thread_members(thread_id,member_key,participant_id,handle,role,wake_policy,joined_at_ms)
             VALUES(?1,'owner','owner','owner','owner','mentions_only',10)",
            [id.as_str()],
        )?;
        db.execute(
            "INSERT INTO thread_posts(thread_id,seq,post_id,author_actor_id,author_kind,author_handle,
             body,created_at_ms,expires_at_ms) VALUES(?1,1,'post','owner','human','owner','body',10,90)",
            [id.as_str()],
        )?;
        let tx = db.transaction()?;
        assert_eq!(prune_closed_threads(&tx, 101, 1)?, 1);
        tx.commit()?;
        let before: (i64, u64) = db.query_row(
            "SELECT expires_at_ms,(SELECT count(*) FROM thread_members) FROM threads",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(before, (0, 1));
        for _ in 0..3 {
            let tx = db.transaction()?;
            assert_eq!(prune_closed_threads(&tx, 5, 1)?, 1);
            tx.commit()?;
        }
        let remaining: u64 = db.query_row("SELECT count(*) FROM threads", [], |row| row.get(0))?;
        assert_eq!(remaining, 0);
        Ok(())
    }

    #[test]
    fn event_cap_prunes_only_excess_rows_even_with_sequence_gaps() -> rusqlite::Result<()> {
        let mut db = Connection::open_in_memory()?;
        db.execute_batch(include_str!("v1.sql"))?;
        let tx = db.transaction()?;
        tx.execute_batch(
            "INSERT INTO hub_meta VALUES (1, 'hub', 'Hub', 'key', 0, 100, 1, 1);
             INSERT INTO events(seq,event_id,kind,aggregate_type,aggregate_id,payload_json,created_at_ms)
             SELECT value, value, 'node.status', 'node', 'node', '{}', 1
               FROM json_each('[1,4,7,15,100]');"
        )?;
        let mut cache = None;
        assert_eq!(prune_events(&tx, 1, 1, 3, &mut cache)?, 1);
        assert_eq!(prune_events(&tx, 1, 20, 3, &mut cache)?, 1);
        assert_eq!(prune_events(&tx, 1, 20, 3, &mut cache)?, 0);
        assert_eq!(
            tx.query_row("SELECT events_pruned_through_seq FROM hub_meta", [], |r| {
                r.get::<_, u64>(0)
            })?,
            4
        );
        Ok(())
    }

    #[test]
    fn event_count_cache_invalidates_on_rollback_and_skips_recount_in_steady_state()
    -> rusqlite::Result<()> {
        let mut db = Connection::open_in_memory()?;
        db.execute_batch(include_str!("v1.sql"))?;
        db.execute_batch(
            "INSERT INTO hub_meta VALUES (1, 'hub', 'Hub', 'key', 0, 10, 1, 1);
             INSERT INTO events(seq,event_id,kind,aggregate_type,aggregate_id,payload_json,created_at_ms)
             SELECT value, value, 'node.status', 'node', 'node', '{}', 1
               FROM json_each('[1,2,3,4,5,6,7,8,9,10]');"
        )?;
        let mut cache = None;
        {
            let tx = db.transaction()?;
            assert_eq!(prune_events(&tx, 1, 2, 3, &mut cache)?, 2);
            // Roll back the transaction; cache holds floor=2 while DB stays at floor=0.
        }
        assert_eq!(cache.map(|entry| entry.floor), Some(2));
        let tx = db.transaction()?;
        assert_eq!(prune_events(&tx, 1, 2, 3, &mut cache)?, 2);
        assert_eq!(prune_events(&tx, 1, 2, 3, &mut cache)?, 2);
        assert_eq!(prune_events(&tx, 1, 2, 3, &mut cache)?, 2);
        assert_eq!(prune_events(&tx, 1, 2, 3, &mut cache)?, 1);
        assert_eq!(prune_events(&tx, 1, 2, 3, &mut cache)?, 0);
        tx.commit()?;
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, u64>(0))?,
            3
        );
        Ok(())
    }
}
