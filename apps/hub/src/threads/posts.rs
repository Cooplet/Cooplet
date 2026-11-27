use std::collections::{BTreeSet, HashSet};

use cooplet_protocol::{
    ErrorCode, EventKind, MemberView, MessageId, OperationId, QuestionPayload, ThreadId,
    ThreadPinDeleteQuery, ThreadPinDeleteResponse, ThreadPinSetRequest, ThreadPinView,
    ThreadPostReceipt, ThreadPostRequest, ThreadPostWire, ThreadReadQuery, ThreadReadResponse,
    ThreadSummary, ThreadWaitRequest, ThreadWaitResponse, ThreadWakePolicy, validate_pin_key,
};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::{Value, json};

use crate::{
    AppState,
    auth::AuthenticatedCaller,
    error::HubError,
    storage::{
        events::question_answered_data,
        sql::{json_byte_len, parse_col, parse_json, parse_opt_col, parse_opt_json},
    },
};

use super::{
    MAX_PIN_BYTES, MAX_POST_BYTES, RETENTION_MS, members,
    store::{self, Mutation, Operation},
};

const POST_COLUMNS: &str =
    "seq,post_id,author_actor_id,author_kind,author_handle,in_reply_to_post_id,
    body,mentions_json,attachments_json,question_json,answer_json,created_at_ms";

pub(super) fn post_from_row(row: &Row<'_>) -> rusqlite::Result<ThreadPostWire> {
    Ok(ThreadPostWire {
        seq: row.get(0)?,
        post_id: parse_col(row, 1)?,
        author_actor_id: row.get(2)?,
        author_kind: parse_col(row, 3)?,
        author_handle: row.get(4)?,
        in_reply_to_post_id: parse_opt_col(row, 5)?,
        body: row.get(6)?,
        mentions: parse_json(row, 7)?,
        attachments: parse_json(row, 8)?,
        question: parse_opt_json(row, 9)?,
        answer: parse_opt_json(row, 10)?,
        created_at_ms: row.get(11)?,
    })
}

pub(crate) async fn post(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    key: &str,
    body: ThreadPostRequest,
) -> Result<ThreadPostReceipt, HubError> {
    let operation = Operation::new(key, "thread.post", Some(&id), &body)?;
    store::mutate(state, caller, operation, move |ctx, op_id| {
        let author = members::require_contributor(ctx, &id, true)?;
        let board = store::board(ctx.tx, &id, ctx.now)?;
        store::require_open(&board)?;
        let post = compose_post(ctx, &board, author, body)?;
        store_post(ctx, &id, &post, op_id)?;
        ctx.posted_seq = Some(post.seq);
        let receipt = ThreadPostReceipt {
            post_id: post.post_id.clone(),
            seq: post.seq,
            created_at_ms: post.created_at_ms,
        };
        let mut event = serde_json::to_value(&post)?;
        event["thread_id"] = json!(id);
        ctx.event(&id, EventKind::ThreadPostCreated, &event)?;
        if let (Some(question_id), Some(answer)) = (&post.in_reply_to_post_id, &post.answer) {
            let data = question_answered_data(
                question_id,
                &post.post_id,
                ctx.caller.actor_id(),
                answer,
                Some(&id),
            );
            ctx.event(&id, EventKind::QuestionAnswered, &data)?;
        }
        Ok((id, receipt))
    })
    .await
}

fn compose_post(
    ctx: &Mutation<'_>,
    board: &ThreadSummary,
    author: MemberView,
    body: ThreadPostRequest,
) -> Result<ThreadPostWire, HubError> {
    if body.attachments.len() > 64 {
        return Err(HubError::invalid("A post can have at most 64 attachments."));
    }
    if body.body.trim().is_empty()
        && body.question.is_none()
        && body.answer.is_none()
        && body.attachments.is_empty()
    {
        return Err(HubError::invalid("An empty post has no content."));
    }
    let mentions = resolve_mentions(ctx.tx, &board.thread_id, &body.mentions, &body.body)?;
    let preceding: Option<i64> = ctx
        .tx
        .query_row(
            "SELECT created_at_ms FROM thread_posts WHERE thread_id=?1 ORDER BY seq DESC LIMIT 1",
            [board.thread_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let created_at_ms = ctx.now.max(preceding.unwrap_or(board.updated_at_ms));
    let author_kind = ctx
        .caller
        .author_kind()
        .ok_or_else(|| store::forbidden("A verified author is required."))?;
    let post = ThreadPostWire {
        seq: board
            .head_seq
            .checked_add(1)
            .filter(|seq| i64::try_from(*seq).is_ok())
            .ok_or_else(|| HubError::internal("Thread post sequence overflow."))?,
        post_id: MessageId::generate()?,
        author_actor_id: author.member_key,
        author_kind,
        author_handle: author.handle,
        in_reply_to_post_id: body.in_reply_to,
        body: body.body,
        mentions,
        attachments: body.attachments,
        question: body.question,
        answer: body.answer,
        created_at_ms,
    };
    post.validate()
        .map_err(|error| HubError::invalid(error.to_string()))?;
    if json_byte_len(&post)? > MAX_POST_BYTES {
        return Err(store::too_large("A thread post exceeds 64 KiB."));
    }
    if let Some(parent) = &post.in_reply_to_post_id {
        let question = live_parent(ctx, board, parent)?;
        if let Some(answer) = &post.answer {
            let question =
                question.ok_or_else(|| HubError::invalid("The reply target is not a question."))?;
            if question
                .expires_at_ms
                .is_some_and(|expiry| expiry <= ctx.now)
            {
                return Err(HubError::new(
                    ErrorCode::QuestionExpired,
                    "The thread question expired.",
                ));
            }
            answer
                .validate_against(&question)
                .map_err(|error| HubError::invalid(error.to_string()))?;
            let existing: Option<String> = ctx.tx.query_row(
                "SELECT post_id FROM thread_posts WHERE in_reply_to_post_id=?1 AND answer_json IS NOT NULL",
                [parent.as_str()], |row| row.get(0),
            ).optional()?;
            if let Some(existing) = existing {
                return Err(HubError::new(
                    ErrorCode::QuestionAlreadyAnswered,
                    "The thread question already has an answer.",
                )
                .with_extension("existing_answer_id", existing));
            }
        }
    } else if post.answer.is_some() {
        return Err(HubError::invalid("An answer requires in_reply_to."));
    }
    Ok(post)
}

fn live_parent(
    ctx: &Mutation<'_>,
    board: &ThreadSummary,
    post_id: &MessageId,
) -> Result<Option<QuestionPayload>, HubError> {
    ctx.tx
        .query_row(
            "SELECT question_json FROM thread_posts WHERE post_id=?1 AND thread_id=?2
         AND seq>?3 AND expires_at_ms>?4",
            params![
                post_id.as_str(),
                board.thread_id.as_str(),
                board.floor_seq,
                ctx.now
            ],
            |row| parse_opt_json(row, 0),
        )
        .optional()?
        .ok_or_else(store::not_found)
}

fn resolve_mentions(
    db: &Connection,
    id: &ThreadId,
    explicit: &[String],
    body: &str,
) -> Result<Vec<String>, HubError> {
    let mut found = BTreeSet::new();
    let mut key_query = db.prepare_cached(
        "SELECT member_key FROM thread_members WHERE thread_id=?1 AND member_key=?2 AND state!='left'",
    )?;
    let mut handle_query = db.prepare_cached(
        "SELECT member_key FROM thread_members WHERE thread_id=?1 AND handle=?2 AND state!='left'",
    )?;
    for mention in explicit {
        if mention.eq_ignore_ascii_case("all") {
            found.insert("all".to_owned());
            continue;
        }
        if let Some(member_key) = key_query
            .query_row(params![id.as_str(), mention], |row| row.get(0))
            .optional()?
        {
            found.insert(member_key);
            continue;
        }
        let handle = mention.to_ascii_lowercase();
        let known = handle_query
            .query_row(params![id.as_str(), handle], |row| row.get(0))
            .optional()?;
        found.insert(
            known.ok_or_else(|| HubError::invalid("The explicit mention is not a live member."))?,
        );
    }
    let mut seen = HashSet::new();
    for token in body_mention_tokens(body) {
        let lower = token.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            continue;
        }
        if lower == "all" {
            found.insert(lower);
        } else if let Some(member_key) = handle_query
            .query_row(params![id.as_str(), lower], |row| row.get(0))
            .optional()?
        {
            found.insert(member_key);
        }
    }
    Ok(found.into_iter().collect())
}

fn body_mention_tokens(body: &str) -> impl Iterator<Item = &str> {
    fn handle_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, '_' | '-')
    }
    body.match_indices('@').filter_map(move |(index, _)| {
        // A mention starts at a word boundary, so e-mail addresses are ordinary text.
        if body[..index]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            return None;
        }
        let rest = &body[index + 1..];
        let end = rest.find(|c| !handle_char(c)).unwrap_or(rest.len());
        (1..=64).contains(&end).then_some(&rest[..end])
    })
}

fn store_post(
    ctx: &Mutation<'_>,
    id: &ThreadId,
    post: &ThreadPostWire,
    op_id: &OperationId,
) -> Result<(), HubError> {
    let expiry = post.created_at_ms.saturating_add(RETENTION_MS);
    let mentions = serde_json::to_string(&post.mentions)?;
    attach_blobs(ctx, id, post, expiry)?;
    ctx.tx.execute(
        "INSERT INTO thread_posts(thread_id,seq,post_id,author_actor_id,author_kind,author_handle,
            in_reply_to_post_id,body,mentions_json,attachments_json,question_json,answer_json,operation_id,
            created_at_ms,expires_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![id.as_str(), post.seq, post.post_id.as_str(), post.author_actor_id, post.author_kind.as_str(),
            post.author_handle, post.in_reply_to_post_id.as_ref().map(MessageId::as_str), post.body,
            mentions, serde_json::to_string(&post.attachments)?,
            post.question.as_ref().map(serde_json::to_string).transpose()?,
            post.answer.as_ref().map(serde_json::to_string).transpose()?,
            op_id.as_str(), post.created_at_ms, expiry],
    )?;
    if expiry > ctx.now {
        defer_direct_wakes(ctx, id, post, &mentions)?;
    }
    ctx.tx.execute(
        "UPDATE threads SET head_seq=?1, updated_at_ms=?2 WHERE thread_id=?3",
        params![post.seq, post.created_at_ms, id.as_str()],
    )?;
    Ok(())
}

fn defer_direct_wakes(
    ctx: &Mutation<'_>,
    id: &ThreadId,
    post: &ThreadPostWire,
    mentions: &str,
) -> rusqlite::Result<()> {
    // An armed explicit recovery also owns new matching wakes while the Node
    // is offline or a prefix is in flight. Keep their horizon with the post.
    ctx.tx.execute(
        "UPDATE thread_members AS m SET resume_through_seq=?2
         WHERE m.thread_id=?1 AND m.state='active' AND m.delivery_mode='direct_now'
           AND m.resume_through_seq<?2 AND m.node_id IS NOT NULL
           AND m.role!='observer' AND m.wake_policy!='muted' AND m.member_key!=?3
           AND (m.wake_policy='all' OR EXISTS(
               SELECT 1 FROM json_each(?4) WHERE value IN ('all',m.member_key)
           ))",
        params![id.as_str(), post.seq, post.author_actor_id, mentions],
    )?;
    Ok(())
}

fn attach_blobs(
    ctx: &Mutation<'_>,
    id: &ThreadId,
    post: &ThreadPostWire,
    expiry: i64,
) -> Result<(), HubError> {
    let mut seen = HashSet::with_capacity(post.attachments.len());
    for attachment in &post.attachments {
        if !seen.insert(&attachment.blob_id) {
            return Err(HubError::invalid(
                "An attachment was specified more than once.",
            ));
        }
        let stored = ctx.tx.query_row(
            "SELECT owner_actor_id,attached_at_ms,scope_thread_id,scope_node_id,scope_peer_node_id,
                    original_name,mime_type,size_bytes,sha256 FROM blobs
             WHERE blob_id=?1 AND expires_at_ms>?2 AND deleted_at_ms IS NULL",
            params![attachment.blob_id.as_str(), ctx.now],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<String>>(2)?, row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?, row.get::<_, String>(5)?,
                row.get::<_, String>(6)?, row.get::<_, u64>(7)?, row.get::<_, String>(8)?)),
        ).optional()?.ok_or_else(store::not_found)?;
        let (owner, attached, thread_scope, node_scope, peer_scope, name, mime, bytes, sha) =
            stored;
        let valid_scope = node_scope.is_none()
            && peer_scope.is_none()
            && match (attached, thread_scope.as_deref()) {
                (None, None) => owner == ctx.caller.actor_id(),
                (Some(_), Some(scoped)) => scoped == id.as_str(),
                _ => false,
            };
        if !valid_scope {
            return Err(if thread_scope.is_some() {
                store::not_found()
            } else {
                store::forbidden("This credential cannot attach the file.")
            });
        }
        if attachment.name != name
            || attachment.mime_type != mime
            || attachment.size_bytes != bytes
            || attachment.sha256 != sha
        {
            return Err(HubError::invalid(
                "Attachment metadata does not match the stored file.",
            ));
        }
        ctx.tx.execute(
            "UPDATE blobs SET scope_thread_id=?1, attached_at_ms=COALESCE(attached_at_ms,?2),
             expires_at_ms=MAX(expires_at_ms,?3) WHERE blob_id=?4",
            params![id.as_str(), ctx.now, expiry, attachment.blob_id.as_str()],
        )?;
        if attached.is_none() {
            crate::blobs::mark_blob_attached(ctx.tx, &attachment.blob_id)?;
        }
    }
    Ok(())
}

pub(crate) async fn read(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    query: ThreadReadQuery,
) -> Result<ThreadReadResponse, HubError> {
    let limit = store::page_limit(query.limit)?;
    store::verified_read(state, caller, move |db, caller, now| {
        store::require_read(db, caller, &id, now)?;
        let board = store::board(db, &id, now)?;
        let since = query.since_seq.unwrap_or(board.floor_seq);
        store::validate_cursor(&board, since)?;
        let pins = if query.include_pins {
            list_pins(db, &id)?
        } else {
            Vec::new()
        };
        let mut out = ThreadReadResponse {
            head_seq: board.head_seq,
            floor_seq: board.floor_seq,
            thread: board,
            pins,
            posts: Vec::new(),
            has_more: false,
            next_since_seq: None,
        };
        let bytes = store::page_budget_bytes(&out)?;
        let page = page_posts(db, &id, (since, out.head_seq), limit, bytes)?;
        out.posts = page.posts;
        out.has_more = page.next.is_some();
        out.next_since_seq = page.next;
        Ok(out)
    })
    .await
}

pub(super) fn last_relevant_seq(
    db: &Connection,
    id: &ThreadId,
    since: u64,
    head: u64,
    member: &MemberView,
) -> rusqlite::Result<Option<u64>> {
    if member.wake_policy == ThreadWakePolicy::Muted {
        return Ok(None);
    }
    db.query_row(
        "SELECT seq FROM thread_posts WHERE thread_id=?1 AND seq>?2 AND seq<=?3 AND author_actor_id!=?4
         AND (?5='all' OR EXISTS(SELECT 1 FROM json_each(mentions_json) WHERE value IN ('all',?4)))
         ORDER BY seq DESC LIMIT 1",
        params![id.as_str(), since, head, member.member_key, member.wake_policy.as_str()],
        |row| row.get(0),
    ).optional()
}

pub(crate) async fn wait(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    body: ThreadWaitRequest,
) -> Result<ThreadWaitResponse, HubError> {
    let state_copy = state.clone();
    let caller = caller.clone();
    state
        .background
        .spawn(async move {
            let thread_id = id.clone();
            let response = store::update(&state_copy, &caller, move |ctx| {
                store::require_read(ctx.tx, ctx.caller, &id, ctx.now)?;
                let mut member = members::require_contributor(ctx, &id, false)?;
                let board = store::board(ctx.tx, &id, ctx.now)?;
                store::require_open(&board)?;
                store::validate_cursor(&board, body.since_seq)?;
                if let Some(policy) = body.wake_policy.map(ThreadWakePolicy::from) {
                    member.wake_policy = policy;
                }
                ctx.tx.execute(
                    "UPDATE thread_members SET confirmed_seq=MAX(confirmed_seq,?1),wake_policy=?2,
                     resume_through_seq=CASE WHEN resume_through_seq<=?1 THEN NULL ELSE resume_through_seq END
                     WHERE thread_id=?3 AND member_key=?4",
                    params![
                        body.since_seq,
                        member.wake_policy.as_str(),
                        id.as_str(),
                        member.member_key
                    ],
                )?;
                if body.wake_policy.is_some() {
                    let updated = members::member(ctx.tx, &id, &member.member_key)?
                        .ok_or_else(store::not_found)?;
                    ctx.event(
                        &id,
                        EventKind::ThreadMemberUpdated,
                        &members::event_data(&id, &updated),
                    )?;
                }
                let head_seq = board.head_seq;
                let Some(through) =
                    last_relevant_seq(ctx.tx, &id, body.since_seq, head_seq, &member)?
                else {
                    return Ok(ThreadWaitResponse::Waiting { head_seq });
                };
                let mut out = ThreadWaitResponse::Ready {
                    head_seq,
                    floor_seq: board.floor_seq,
                    posts: Vec::new(),
                    has_more: false,
                    next_since_seq: None,
                };
                let bytes = store::page_budget_bytes(&out)?;
                let page = page_posts(ctx.tx, &id, (body.since_seq, through), 20, bytes)?;
                if let ThreadWaitResponse::Ready {
                    posts,
                    has_more,
                    next_since_seq,
                    ..
                } = &mut out
                {
                    *posts = page.posts;
                    *has_more = page.next.is_some();
                    *next_since_seq = page.next;
                }
                Ok(out)
            })
            .await?;
            super::dispatch::wake_thread(&state_copy, thread_id, None).await;
            Ok(response)
        })?
        .await
        .map_err(HubError::internal)?
}

struct PostPage {
    posts: Vec<ThreadPostWire>,
    next: Option<u64>,
}

fn page_posts(
    db: &Connection,
    id: &ThreadId,
    range: (u64, u64),
    limit: usize,
    mut bytes: usize,
) -> Result<PostPage, HubError> {
    let mut page = PostPage {
        posts: Vec::new(),
        next: None,
    };
    let mut stmt = db.prepare_cached(&format!(
        "SELECT {POST_COLUMNS} FROM thread_posts WHERE thread_id=?1 AND seq>?2 AND seq<=?3 ORDER BY seq LIMIT ?4"
    ))?;
    let mut rows = stmt.query(params![id.as_str(), range.0, range.1, limit + 1])?;
    while let Some(row) = rows.next()? {
        let post = post_from_row(row)?;
        if page.posts.len() >= limit || !store::add_to_page(&mut bytes, &post, page.posts.len())? {
            page.next = page.posts.last().map(|post| post.seq);
            break;
        }
        page.posts.push(post);
    }
    Ok(page)
}

pub(super) fn delivery_posts(
    db: &Connection,
    id: &ThreadId,
    floor: u64,
    through: u64,
    limit: usize,
) -> rusqlite::Result<Vec<ThreadPostWire>> {
    let mut stmt = db.prepare_cached(&format!(
        "SELECT {POST_COLUMNS} FROM thread_posts WHERE thread_id=?1 AND seq>?2 AND seq<=?3 ORDER BY seq DESC LIMIT ?4"
    ))?;
    stmt.query_map(params![id.as_str(), floor, through, limit], post_from_row)?
        .collect()
}

fn list_pins(db: &Connection, id: &ThreadId) -> rusqlite::Result<Vec<ThreadPinView>> {
    let mut stmt = db.prepare_cached(
        "SELECT pin_key,post_id,value_json,version,updated_by_actor_id,updated_at_ms
         FROM thread_pins WHERE thread_id=?1 ORDER BY pin_key",
    )?;
    stmt.query_map([id.as_str()], |row| {
        Ok(ThreadPinView {
            thread_id: id.clone(),
            pin_key: row.get(0)?,
            post_id: parse_opt_col(row, 1)?,
            value_json: parse_json(row, 2)?,
            version: row.get(3)?,
            updated_by_actor_id: row.get(4)?,
            updated_at_ms: row.get(5)?,
        })
    })?
    .collect()
}

fn check_version(expected: Option<u64>, current: Option<u64>) -> Result<(), HubError> {
    if expected.is_some_and(|expected| expected != current.unwrap_or(0)) {
        return Err(
            HubError::new(ErrorCode::PinConflict, "The pin version does not match.")
                .with_extension("current_version", json!(current)),
        );
    }
    Ok(())
}

fn advance_version(ctx: &Mutation<'_>, id: &ThreadId) -> rusqlite::Result<u64> {
    ctx.tx.query_row(
        "UPDATE threads SET pin_version_clock=pin_version_clock+1 WHERE thread_id=?1 RETURNING pin_version_clock",
        [id.as_str()], |row| row.get(0),
    )
}

pub(crate) async fn set_pin(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    pin_key: String,
    key: &str,
    body: ThreadPinSetRequest,
) -> Result<ThreadPinView, HubError> {
    let operation = Operation::new(key, "thread.pin.set", Some(&id), &(&pin_key, &body))?;
    store::mutate(state, caller, operation, move |ctx, _| {
        members::require_contributor(ctx, &id, true)?;
        let board = store::board(ctx.tx, &id, ctx.now)?;
        store::require_open(&board)?;
        validate_pin_key(&pin_key).map_err(|error| HubError::invalid(error.to_string()))?;
        if body.value_json.is_null() {
            return Err(HubError::invalid("Pin value_json cannot be null."));
        }
        if let Some(post_id) = &body.post_id { live_parent(ctx, &board, post_id)?; }
        let mut pins = list_pins(ctx.tx, &id)?;
        let position = pins.iter().position(|pin| pin.pin_key == pin_key);
        check_version(body.expected_version, position.map(|i| pins[i].version))?;
        if position.is_none() && pins.len() >= 32 {
            return Err(HubError::invalid("A thread can have at most 32 pins."));
        }
        let pin = ThreadPinView {
            thread_id: id.clone(), pin_key, post_id: body.post_id, value_json: body.value_json,
            version: advance_version(ctx, &id)?, updated_by_actor_id: ctx.caller.actor_id().to_owned(),
            updated_at_ms: ctx.now,
        };
        if let Some(i) = position { pins[i] = pin.clone(); } else { pins.push(pin.clone()); }
        if json_byte_len(&pins)? > MAX_PIN_BYTES {
            return Err(store::too_large("Thread pins exceed 64 KiB."));
        }
        ctx.tx.execute(
            "INSERT INTO thread_pins(thread_id,pin_key,post_id,value_json,version,updated_by_actor_id,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT(thread_id,pin_key) DO UPDATE SET post_id=excluded.post_id,value_json=excluded.value_json,
              version=excluded.version,updated_by_actor_id=excluded.updated_by_actor_id,updated_at_ms=excluded.updated_at_ms",
            params![id.as_str(), pin.pin_key, pin.post_id.as_ref().map(MessageId::as_str),
                serde_json::to_string(&pin.value_json)?, pin.version, pin.updated_by_actor_id, pin.updated_at_ms],
        )?;
        let mut event = json!({
            "thread_id": id,
            "pin_key": pin.pin_key,
            "version": pin.version,
            "deleted": false,
            "value_json": pin.value_json,
            "updated_by": pin.updated_by_actor_id,
        });
        if let Some(post_id) = &pin.post_id {
            event["post_id"] = json!(post_id);
        }
        ctx.event(&id, EventKind::ThreadPinUpdated, &event)?;
        Ok((id, pin))
    }).await
}

pub(crate) async fn delete_pin(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    pin_key: String,
    key: &str,
    query: ThreadPinDeleteQuery,
) -> Result<ThreadPinDeleteResponse, HubError> {
    let operation = Operation::new(key, "thread.pin.delete", Some(&id), &(&pin_key, &query))?;
    store::mutate(state, caller, operation, move |ctx, _| {
        members::require_contributor(ctx, &id, true)?;
        let board = store::board(ctx.tx, &id, ctx.now)?;
        store::require_open(&board)?;
        validate_pin_key(&pin_key).map_err(|error| HubError::invalid(error.to_string()))?;
        let current: Option<u64> = ctx
            .tx
            .query_row(
                "SELECT version FROM thread_pins WHERE thread_id=?1 AND pin_key=?2",
                params![id.as_str(), pin_key],
                |row| row.get(0),
            )
            .optional()?;
        check_version(query.expected_version, current)?;
        let version = if current.is_some() {
            ctx.tx.execute(
                "DELETE FROM thread_pins WHERE thread_id=?1 AND pin_key=?2",
                params![id.as_str(), pin_key],
            )?;
            advance_version(ctx, &id)?
        } else {
            0
        };
        let result = ThreadPinDeleteResponse {
            thread_id: id.clone(),
            pin_key,
            deleted: true,
            version,
        };
        if version != 0 {
            let mut event = serde_json::to_value(&result)?;
            event["value_json"] = Value::Null;
            event["updated_by"] = json!(ctx.caller.actor_id());
            ctx.event(&id, EventKind::ThreadPinUpdated, &event)?;
        }
        Ok((id, result))
    })
    .await
}
