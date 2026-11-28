mod dispatch;
mod members;
mod posts;
mod store;

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use cooplet_protocol::{
    AuthorKind, ErrorCode, EventKind, MAX_THREAD_INITIAL_MEMBERS, NodeId, NodeRpcOperation,
    NodeRpcResult, ServiceScope, ThreadCloseRequest, ThreadCreateRequest, ThreadId,
    ThreadListQuery, ThreadListResponse, ThreadStatus, ThreadSummary,
};
use rusqlite::params;

use crate::{AppState, auth::AuthenticatedCaller, error::HubError, storage::sql::json_byte_len};

pub(crate) use dispatch::{
    Dispatch, on_participants_idle, participant_idle, pause_node_pending, reconcile_fences,
    recover_pending, wake,
};
pub(crate) use members::{join, leave, list_members, resume};
pub(crate) use posts::{delete_pin, post, read, set_pin, wait};
pub(crate) use store::can_access_thread_scope;
pub(super) use store::thread_scope_expiry;

pub(crate) const RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
const MAX_PAGE_BYTES: usize = 192 * 1024;
const MAX_POST_BYTES: usize = 64 * 1024;
const MAX_PIN_BYTES: usize = 64 * 1024;
const MAX_TITLE_BYTES: usize = 8 * 1024;

pub(crate) async fn create(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    body: ThreadCreateRequest,
) -> Result<ThreadSummary, HubError> {
    caller.require_scope(ServiceScope::Collaborate)?;
    if caller.author_kind() == Some(AuthorKind::Agent) && !state.agents_can_create_threads {
        return Err(store::forbidden(
            "Agents cannot create threads on this Hub.",
        ));
    }
    if body.title.trim().is_empty() {
        return Err(HubError::invalid("A thread title is required."));
    }
    let topic_bytes = body
        .topic
        .as_ref()
        .map(json_byte_len)
        .transpose()?
        .unwrap_or(0);
    if json_byte_len(&body.title)? + topic_bytes > MAX_TITLE_BYTES {
        return Err(store::too_large("Thread title and topic exceed 8 KiB."));
    }
    if body.members.len() > MAX_THREAD_INITIAL_MEMBERS {
        return Err(HubError::invalid(
            "A thread can be created with at most 64 initial members.",
        ));
    }
    let operation = store::Operation::new(key, "thread.create", None, &body)?;
    store::mutate(state, caller, operation, move |ctx, _| {
        let thread_id = ThreadId::generate()?;
        let actor_id = ctx.caller.actor_id().to_owned();
        let created = ThreadSummary {
            thread_id: thread_id.clone(),
            title: body.title,
            topic: body.topic,
            created_by_actor_id: actor_id,
            status: ThreadStatus::Open,
            head_seq: 0,
            floor_seq: 0,
            created_at_ms: ctx.now,
            updated_at_ms: ctx.now,
            closed_at_ms: None,
        };
        ctx.tx.execute(
            "INSERT INTO threads(thread_id,title,topic,created_by_actor_id,status,created_at_ms,updated_at_ms)
             VALUES(?1,?2,?3,?4,'open',?5,?5)",
            params![thread_id.as_str(), created.title, created.topic, created.created_by_actor_id, ctx.now],
        )?;
        ctx.event(&thread_id, EventKind::ThreadCreated, &created)?;
        members::add_creator(ctx, &thread_id, body.creator_handle)?;
        let mut keys = BTreeSet::new();
        for member in body.members {
            if !keys.insert(member.member_key.clone()) || member.member_key == ctx.caller.actor_id() {
                return Err(HubError::invalid("A thread member was specified more than once."));
            }
            members::add(ctx, &thread_id, 0, member)?;
        }
        Ok((thread_id, created))
    }).await
}

pub(crate) async fn list(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: ThreadListQuery,
) -> Result<ThreadListResponse, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    let limit = store::page_limit(query.limit)?;
    store::verified_read(state, caller, move |db, caller, now| {
        let mut result = ThreadListResponse {
            items: Vec::new(),
            next_after: None,
        };
        let mut bytes = store::page_budget_bytes(&result)?;
        let global = caller.is_global_reader();
        let allowed_json = if !global && caller.source_node_id().is_none() {
            caller
                .allowed_nodes()
                .map(|nodes| {
                    let ids: Vec<&str> = nodes.iter().map(NodeId::as_str).collect();
                    serde_json::to_string(&ids)
                })
                .transpose()?
        } else {
            None
        };
        let after = query.after.as_ref().map_or("", ThreadId::as_str);
        let status = query.status.map(ThreadStatus::as_str);
        let fetch_limit = limit + 1;
        let mut statement = db.prepare_cached(if global {
            "SELECT thread_id,title,topic,created_by_actor_id,status,head_seq,floor_seq,
                    created_at_ms,updated_at_ms,closed_at_ms
             FROM threads
             WHERE thread_id>?1 AND (?2 IS NULL OR status=?2)
               AND (expires_at_ms IS NULL OR (expires_at_ms!=0 AND expires_at_ms>?3))
             ORDER BY thread_id LIMIT ?4"
        } else if allowed_json.is_some() {
            "SELECT t.thread_id,t.title,t.topic,t.created_by_actor_id,t.status,t.head_seq,t.floor_seq,
                    t.created_at_ms,t.updated_at_ms,t.closed_at_ms
             FROM thread_members m JOIN threads t USING(thread_id)
             WHERE m.member_key=?4 AND m.thread_id>?1 AND m.state!='left' AND (?2 IS NULL OR t.status=?2)
               AND (t.expires_at_ms IS NULL OR (t.expires_at_ms!=0 AND t.expires_at_ms>?3))
               AND NOT EXISTS (
                   SELECT 1 FROM thread_members tm
                   WHERE tm.thread_id=t.thread_id AND instr(tm.member_key,'/')>0
                     AND substr(tm.member_key,1,instr(tm.member_key,'/')-1)
                         NOT IN (SELECT value FROM json_each(?5))
               )
             ORDER BY m.thread_id LIMIT ?6"
        } else {
            "SELECT t.thread_id,t.title,t.topic,t.created_by_actor_id,t.status,t.head_seq,t.floor_seq,
                    t.created_at_ms,t.updated_at_ms,t.closed_at_ms
             FROM thread_members m JOIN threads t USING(thread_id)
             WHERE m.member_key=?4 AND m.thread_id>?1 AND m.state!='left' AND (?2 IS NULL OR t.status=?2)
               AND (t.expires_at_ms IS NULL OR (t.expires_at_ms!=0 AND t.expires_at_ms>?3))
             ORDER BY m.thread_id LIMIT ?5"
        })?;
        let mut rows = if global {
            statement.query(params![after, status, now, fetch_limit])?
        } else if let Some(allowed) = &allowed_json {
            statement.query(params![
                after,
                status,
                now,
                caller.actor_id(),
                allowed,
                fetch_limit
            ])?
        } else {
            statement.query(params![after, status, now, caller.actor_id(), fetch_limit])?
        };
        while let Some(row) = rows.next()? {
            if result.items.len() >= limit {
                result.next_after = result.items.last().map(|item| item.thread_id.clone());
                break;
            }
            let mut board = store::summary_from_row(row)?;
            board.floor_seq = store::effective_floor(db, &board.thread_id, board.floor_seq, now)?;
            if !store::add_to_page(&mut bytes, &board, result.items.len())? {
                result.next_after = result.items.last().map(|item| item.thread_id.clone());
                break;
            }
            result.items.push(board);
        }
        Ok(result)
    })
    .await
}

pub(crate) async fn get(
    state: &AppState,
    caller: &AuthenticatedCaller,
    thread_id: ThreadId,
) -> Result<ThreadSummary, HubError> {
    store::verified_read(state, caller, move |db, caller, now| {
        store::require_read(db, caller, &thread_id, now)?;
        store::board(db, &thread_id, now)
    })
    .await
}

pub(crate) async fn close(
    state: &AppState,
    caller: &AuthenticatedCaller,
    thread_id: ThreadId,
    key: &str,
) -> Result<ThreadSummary, HubError> {
    let operation = store::Operation::new(
        key,
        "thread.close",
        Some(&thread_id),
        &ThreadCloseRequest {},
    )?;
    store::mutate(state, caller, operation, move |ctx, _| {
        let board = store::board(ctx.tx, &thread_id, ctx.now)?;
        members::require_manager(ctx, &thread_id)?;
        store::require_open(&board)?;
        let board = close_board(ctx, &thread_id)?;
        Ok((thread_id, board))
    })
    .await
}

fn close_board(
    ctx: &mut store::Mutation<'_>,
    thread_id: &ThreadId,
) -> Result<ThreadSummary, HubError> {
    ctx.tx.execute(
        "UPDATE threads SET status='closed', closed_at_ms=?1, updated_at_ms=?1, expires_at_ms=?2
         WHERE thread_id=?3",
        params![
            ctx.now,
            ctx.now.saturating_add(RETENTION_MS),
            thread_id.as_str()
        ],
    )?;
    let board = store::board(ctx.tx, thread_id, ctx.now)?;
    ctx.event(thread_id, EventKind::ThreadClosed, &board)?;
    Ok(board)
}

pub(crate) async fn dispatch_rpc(
    state: &AppState,
    caller: &AuthenticatedCaller,
    operation: NodeRpcOperation,
) -> Result<NodeRpcResult, HubError> {
    use NodeRpcOperation as Request;
    use NodeRpcResult as Response;

    Ok(match operation {
        Request::ThreadCreate {
            operation_key,
            body,
        } => Response::ThreadCreate {
            body: create(state, caller, &operation_key, body).await?,
        },
        Request::ThreadList { query } => Response::ThreadList {
            body: list(state, caller, query).await?,
        },
        Request::ThreadGet { thread_id } => Response::ThreadGet {
            body: get(state, caller, thread_id).await?,
        },
        Request::ThreadClose {
            thread_id,
            operation_key,
        } => Response::ThreadClose {
            body: close(state, caller, thread_id, &operation_key).await?,
        },
        Request::ThreadPost {
            thread_id,
            operation_key,
            body,
        } => Response::ThreadPost {
            body: post(state, caller, thread_id, &operation_key, body).await?,
        },
        Request::ThreadRead { thread_id, query } => Response::ThreadRead {
            body: read(state, caller, thread_id, query).await?,
        },
        Request::ThreadPinSet {
            thread_id,
            pin_key,
            operation_key,
            body,
        } => Response::ThreadPinSet {
            body: set_pin(state, caller, thread_id, pin_key, &operation_key, body).await?,
        },
        Request::ThreadPinDelete {
            thread_id,
            pin_key,
            operation_key,
            query,
        } => Response::ThreadPinDelete {
            body: delete_pin(state, caller, thread_id, pin_key, &operation_key, query).await?,
        },
        Request::ThreadWait { thread_id, body } => Response::ThreadWait {
            body: wait(state, caller, thread_id, body).await?,
        },
        Request::ThreadMembersList { thread_id, query } => Response::ThreadMembersList {
            body: list_members(state, caller, thread_id, query).await?,
        },
        Request::ThreadMemberJoin {
            thread_id,
            operation_key,
            body,
        } => Response::ThreadMemberJoin {
            body: join(state, caller, thread_id, &operation_key, body).await?,
        },
        Request::ThreadMemberLeave {
            thread_id,
            operation_key,
            body,
        } => Response::ThreadMemberLeave {
            body: leave(state, caller, thread_id, &operation_key, body).await?,
        },
        Request::ThreadMemberResume {
            thread_id,
            operation_key,
            body,
        } => Response::ThreadMemberResume {
            body: resume(state, caller, thread_id, &operation_key, body).await?,
        },
        _ => {
            return Err(HubError::new(
                ErrorCode::InvalidRequest,
                "Expected a Threads operation.",
            ));
        }
    })
}
