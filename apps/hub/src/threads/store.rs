use cooplet_protocol::{
    AggregateType, ErrorCode, EventKind, HubId, NodeId, OperationId, ServiceScope, ThreadId,
    ThreadStatus, ThreadSummary, canonical_sha256, validate_idempotency_key,
};
use cooplet_storage::{EventNotice, StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    AppState,
    auth::{AuthenticatedCaller, reverify_caller_tx},
    error::HubError,
    storage::{
        events::{self, EventAudience, NewEvent},
        operations::{self, FinishOperation, NewOperation},
        sql::{json_byte_len, parse_col, read_verified_tx_with_clock},
    },
};

use super::MAX_PAGE_BYTES;

pub(super) async fn verified_read<T, F>(
    state: &AppState,
    caller: &AuthenticatedCaller,
    read: F,
) -> Result<T, HubError>
where
    T: Send + 'static,
    F: FnOnce(&Transaction<'_>, &AuthenticatedCaller, i64) -> Result<T, HubError> + Send + 'static,
{
    read_verified_tx_with_clock(&state.reads, caller, state.wall_clock.clone(), read).await
}

pub(super) fn page_limit(limit: Option<u32>) -> Result<usize, HubError> {
    let limit = limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return Err(HubError::invalid("The page limit must be 1 through 100."));
    }
    Ok(limit as usize)
}

pub(super) fn page_budget_bytes(value: &impl Serialize) -> Result<usize, HubError> {
    // Includes room for the largest cursor and optional pagination fields.
    Ok(json_byte_len(value)? + 260)
}

pub(super) fn add_to_page(
    bytes: &mut usize,
    item: &impl Serialize,
    len: usize,
) -> Result<bool, HubError> {
    let size = json_byte_len(item)? + usize::from(len != 0);
    if *bytes + size > MAX_PAGE_BYTES {
        return Ok(false);
    }
    *bytes += size;
    Ok(true)
}

pub(super) fn not_found() -> HubError {
    HubError::new(ErrorCode::NotFound, "The thread or resource was not found.")
}

pub(super) fn forbidden(detail: &str) -> HubError {
    HubError::new(ErrorCode::Forbidden, detail)
}

pub(super) fn too_large(detail: &str) -> HubError {
    HubError::new(ErrorCode::PayloadTooLarge, detail)
}

pub(super) fn conflict(detail: &str, reason: &str) -> HubError {
    HubError::new(ErrorCode::OperationConflict, detail).with_extension("reason", reason)
}

pub(super) fn require_open(board: &ThreadSummary) -> Result<(), HubError> {
    if board.status == ThreadStatus::Closed {
        return Err(conflict("The thread is closed.", "thread_closed"));
    }
    Ok(())
}

pub(super) fn validate_cursor(board: &ThreadSummary, cursor: u64) -> Result<(), HubError> {
    if cursor < board.floor_seq {
        return Err(
            HubError::new(ErrorCode::ResyncRequired, "The thread cursor was pruned.")
                .with_extension("thread_id", board.thread_id.as_str())
                .with_extension("floor_seq", board.floor_seq)
                .with_extension("head_seq", board.head_seq)
                .with_extension("reason", "cursor_pruned"),
        );
    }
    if cursor > board.head_seq {
        return Err(HubError::invalid(
            "The cursor exceeds the current thread head.",
        ));
    }
    Ok(())
}

pub(crate) fn can_access_thread_scope(
    db: &Connection,
    caller: &AuthenticatedCaller,
    thread_id: &ThreadId,
    now_ms: i64,
) -> rusqlite::Result<bool> {
    Ok(thread_scope_expiry(db, caller, thread_id, now_ms)?.is_some())
}

/// A live thread returns its expiry, or i64::MAX for an open board.
pub(crate) fn thread_scope_expiry(
    db: &Connection,
    caller: &AuthenticatedCaller,
    thread_id: &ThreadId,
    now: i64,
) -> rusqlite::Result<Option<i64>> {
    thread_scope_expiry_inner(db, caller, thread_id, now, false)
}

fn thread_scope_expiry_inner(
    db: &Connection,
    caller: &AuthenticatedCaller,
    thread_id: &ThreadId,
    now: i64,
    allow_left_member: bool,
) -> rusqlite::Result<Option<i64>> {
    let expiry = db
        .prepare_cached(
            "SELECT COALESCE(expires_at_ms,9223372036854775807) FROM threads
             WHERE thread_id=?1 AND (expires_at_ms IS NULL OR (expires_at_ms != 0 AND expires_at_ms > ?2))",
        )?
        .query_row(params![thread_id.as_str(), now], |row| row.get(0))
        .optional()?;
    if expiry.is_none() || caller.require_scope(ServiceScope::Read).is_err() {
        return Ok(None);
    }
    if caller.is_global_reader() {
        return Ok(expiry);
    }
    let member_sql = if allow_left_member {
        "SELECT EXISTS (SELECT 1 FROM thread_members WHERE thread_id=?1 AND member_key=?2)"
    } else {
        "SELECT EXISTS (SELECT 1 FROM thread_members WHERE thread_id=?1 AND member_key=?2 AND state!='left')"
    };
    let member: bool = db
        .prepare_cached(member_sql)?
        .query_row(params![thread_id.as_str(), caller.actor_id()], |row| {
            row.get(0)
        })?;
    if !member {
        return Ok(None);
    }
    if caller.source_node_id().is_none()
        && let Some(allowed) = caller.allowed_nodes()
    {
        let mut stmt = db.prepare_cached(
            "SELECT substr(member_key,1,instr(member_key,'/')-1)
             FROM thread_members WHERE thread_id=?1 AND instr(member_key,'/') > 0",
        )?;
        let mut rows = stmt.query([thread_id.as_str()])?;
        while let Some(row) = rows.next()? {
            let node_id: NodeId = parse_col(row, 0)?;
            if !allowed.contains(&node_id) {
                return Ok(None);
            }
        }
    }
    Ok(expiry)
}

pub(super) fn summary_from_row(row: &Row<'_>) -> rusqlite::Result<ThreadSummary> {
    Ok(ThreadSummary {
        thread_id: parse_col(row, 0)?,
        title: row.get(1)?,
        topic: row.get(2)?,
        created_by_actor_id: row.get(3)?,
        status: parse_col(row, 4)?,
        head_seq: row.get(5)?,
        floor_seq: row.get(6)?,
        created_at_ms: row.get(7)?,
        updated_at_ms: row.get(8)?,
        closed_at_ms: row.get(9)?,
    })
}

pub(super) fn require_read(
    db: &Connection,
    caller: &AuthenticatedCaller,
    id: &ThreadId,
    now: i64,
) -> Result<(), HubError> {
    if can_access_thread_scope(db, caller, id, now)? {
        Ok(())
    } else {
        Err(not_found())
    }
}

pub(super) fn effective_floor(
    db: &Connection,
    thread_id: &ThreadId,
    floor: u64,
    now: i64,
) -> rusqlite::Result<u64> {
    db.prepare_cached(
        "SELECT MAX(?2,COALESCE((SELECT seq FROM thread_posts
          WHERE thread_id=?1 AND expires_at_ms<=?3 ORDER BY expires_at_ms DESC,seq DESC LIMIT 1),0))",
    )?
    .query_row(params![thread_id.as_str(), floor, now], |row| row.get(0))
}

pub(super) fn board(db: &Connection, id: &ThreadId, now: i64) -> Result<ThreadSummary, HubError> {
    let mut board = db.query_row(
        "SELECT title,topic,created_by_actor_id,status,head_seq,floor_seq,created_at_ms,updated_at_ms,closed_at_ms
         FROM threads WHERE thread_id=?1", [id.as_str()], |row| Ok(ThreadSummary {
            thread_id: id.clone(), title: row.get(0)?, topic: row.get(1)?,
            created_by_actor_id: row.get(2)?, status: parse_col(row, 3)?,
            head_seq: row.get(4)?, floor_seq: row.get(5)?, created_at_ms: row.get(6)?,
            updated_at_ms: row.get(7)?, closed_at_ms: row.get(8)?,
        }),
    ).optional()?.ok_or_else(not_found)?;
    board.floor_seq = effective_floor(db, id, board.floor_seq, now)?;
    Ok(board)
}

#[derive(Clone)]
pub(super) struct Operation {
    key: String,
    kind: &'static str,
    digest: String,
    thread_id: Option<ThreadId>,
}

impl Operation {
    pub(super) fn new(
        key: &str,
        kind: &'static str,
        thread_id: Option<&ThreadId>,
        body: &impl Serialize,
    ) -> Result<Self, HubError> {
        validate_idempotency_key(key).map_err(|error| HubError::invalid(error.to_string()))?;
        Ok(Self {
            key: key.to_owned(),
            kind,
            digest: canonical_sha256(&(thread_id, body))?,
            thread_id: thread_id.cloned(),
        })
    }

    fn replay<T: DeserializeOwned>(
        &self,
        db: &Connection,
        caller: &AuthenticatedCaller,
        now: i64,
    ) -> Result<Option<T>, HubError> {
        let Some(saved) = operations::load(db, caller.actor_id(), &self.key)? else {
            return Ok(None);
        };
        // Check operation key conflicts before visibility checks so leaving a
        // prior thread cannot mask a 409 operation_conflict as 404.
        saved.check_match(self.kind, &self.digest)?;
        if saved.kind.starts_with("thread.") {
            let id = saved
                .resource_id
                .as_deref()
                .ok_or_else(|| HubError::internal("Thread receipt has no resource ID."))?;
            let allow_left = saved.kind == "thread.member.leave";
            if thread_scope_expiry_inner(db, caller, &ThreadId::parse(id)?, now, allow_left)?
                .is_none()
            {
                return Err(not_found());
            }
        }
        let receipt = saved
            .result_json
            .as_deref()
            .ok_or_else(|| HubError::internal("Thread receipt has no result."))?;
        Ok(Some(serde_json::from_str(receipt)?))
    }

    fn invalidates_acl(&self) -> bool {
        matches!(
            self.kind,
            "thread.close" | "thread.member.join" | "thread.member.leave"
        )
    }
}

pub(super) struct Mutation<'a> {
    pub(super) tx: &'a Transaction<'a>,
    pub(super) caller: &'a AuthenticatedCaller,
    pub(super) now: i64,
    hub_id: &'a HubId,
    events: Vec<EventNotice>,
}

impl Mutation<'_> {
    pub(super) fn event(
        &mut self,
        thread_id: &ThreadId,
        kind: EventKind,
        data: &impl Serialize,
    ) -> Result<(), HubError> {
        self.events.push(events::record(
            self.tx,
            self.hub_id,
            NewEvent {
                kind,
                aggregate_type: AggregateType::Thread,
                aggregate_id: thread_id.as_str(),
                actor_id: Some(self.caller.actor_id()),
                created_at_ms: self.now,
                data: serde_json::to_value(data)?,
                audience: EventAudience::Thread {
                    thread_id: thread_id.clone(),
                },
            },
        )?);
        Ok(())
    }
}

pub(super) fn invalidate_acl(state: &AppState) {
    state
        .thread_acl_watch
        .send_modify(|revision| *revision = revision.wrapping_add(1));
}

pub(super) async fn mutate<T, F>(
    state: &AppState,
    caller: &AuthenticatedCaller,
    operation: Operation,
    apply: F,
) -> Result<T, HubError>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    F: FnOnce(&mut Mutation<'_>, &OperationId) -> Result<(ThreadId, T), HubError> + Send + 'static,
{
    let state_copy = state.clone();
    let caller = caller.clone();
    state
        .background
        .spawn(async move {
            let state = state_copy;
            let write_operation = operation.clone();
            let write_caller = caller.clone();
            let hub_id = state.meta.hub_id.clone();
            let clock = state.wall_clock.clone();
            let result = state
                .db
                .write(move |tx| {
                    let now = clock();
                    reverify_caller_tx(tx, &write_caller, now)?;
                    write_caller.require_scope(ServiceScope::Collaborate)?;
                    if write_operation.kind == "thread.member.leave" {
                        if let Some(data) = write_operation.replay(tx, &write_caller, now)? {
                            return Ok(WriteOutcome::new((data, None)));
                        }
                        if let Some(id) = &write_operation.thread_id {
                            require_read(tx, &write_caller, id, now)?;
                        }
                    } else {
                        if let Some(id) = &write_operation.thread_id {
                            require_read(tx, &write_caller, id, now)?;
                        }
                        if let Some(data) = write_operation.replay(tx, &write_caller, now)? {
                            return Ok(WriteOutcome::new((data, None)));
                        }
                    }
                    let operation_id = OperationId::generate().map_err(HubError::from)?;
                    operations::insert(
                        tx,
                        NewOperation {
                            operation_id: &operation_id,
                            actor_id: write_caller.actor_id(),
                            key: &write_operation.key,
                            kind: write_operation.kind,
                            request_sha256: &write_operation.digest,
                            target_node_id: None,
                            state: "accepted",
                            resource_id: None,
                            result_json: None,
                            now_ms: now,
                        },
                    )?;
                    let mut ctx = Mutation {
                        tx,
                        caller: &write_caller,
                        now,
                        hub_id: &hub_id,
                        events: Vec::new(),
                    };
                    let (thread_id, data) = apply(&mut ctx, &operation_id)?;
                    let receipt = serde_json::to_string(&data).map_err(HubError::from)?;
                    if !operations::finish(
                        tx,
                        FinishOperation {
                            operation_id: &operation_id,
                            state: "succeeded",
                            resource_id: Some(thread_id.as_str()),
                            result_json: Some(&receipt),
                            error_json: None,
                            now_ms: now,
                        },
                    )? {
                        return Err(HubError::internal("The thread receipt was not stored.").into());
                    }
                    if let Some(last) = ctx.events.last() {
                        events::advance_head(tx, last.seq)?;
                    }
                    Ok::<_, WriteError<HubError>>(WriteOutcome::with_events(
                        (data, Some(thread_id)),
                        ctx.events,
                    ))
                })
                .await;
            match result {
                Ok((data, changed)) => {
                    if let Some(thread_id) = changed {
                        if operation.invalidates_acl() {
                            invalidate_acl(&state);
                        }
                        if matches!(operation.kind, "thread.post" | "thread.member.resume") {
                            super::dispatch::wake_thread(&state, thread_id).await;
                        }
                    }
                    Ok(data)
                }
                Err(WriteFailure::Storage(
                    error @ (StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)),
                )) => {
                    if operation.invalidates_acl() {
                        invalidate_acl(&state);
                    }
                    let thread_id = operation.thread_id.clone();
                    let kind = operation.kind;
                    let replay = verified_read(&state, &caller, move |tx, caller, now| {
                        operation.replay(tx, caller, now)
                    })
                    .await?;
                    if let Some(data) = replay {
                        if let Some(thread_id) = thread_id
                            && matches!(kind, "thread.post" | "thread.member.resume")
                        {
                            super::dispatch::wake_thread(&state, thread_id).await;
                        }
                        return Ok(data);
                    }
                    Err(error.into())
                }
                Err(error) => Err(error.into()),
            }
        })?
        .await
        .map_err(HubError::internal)?
}

pub(super) async fn update<T, F>(
    state: &AppState,
    caller: &AuthenticatedCaller,
    apply: F,
) -> Result<T, HubError>
where
    T: Send + 'static,
    F: FnOnce(&mut Mutation<'_>) -> Result<T, HubError> + Send + 'static,
{
    let caller = caller.clone();
    let hub_id = state.meta.hub_id.clone();
    let clock = state.wall_clock.clone();
    let result = state
        .db
        .write(move |tx| {
            let now = clock();
            reverify_caller_tx(tx, &caller, now)?;
            let mut ctx = Mutation {
                tx,
                caller: &caller,
                now,
                hub_id: &hub_id,
                events: Vec::new(),
            };
            let value = apply(&mut ctx)?;
            if let Some(last) = ctx.events.last() {
                events::advance_head(tx, last.seq)?;
            }
            Ok::<_, WriteError<HubError>>(WriteOutcome::with_events(value, ctx.events))
        })
        .await?;
    Ok(result)
}
