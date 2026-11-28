use cooplet_protocol::{
    DeliveryMode, EventKind, MemberView, NodeId, ParticipantId, PendingThreadDelivery,
    ServicePrincipalId, ServiceScope, ThreadId, ThreadMemberJoinRequest, ThreadMemberLeaveRequest,
    ThreadMemberResumeRequest, ThreadMemberState, ThreadMembersQuery, ThreadMembersResponse,
    ThreadRole, ThreadWakePolicy, UserId,
};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::{Value, json};

use crate::{
    AppState,
    auth::AuthenticatedCaller,
    error::HubError,
    storage::sql::{parse_col, parse_opt_col},
};

use super::{
    close_board, dispatch, posts,
    store::{self, Mutation, Operation},
};

pub(super) struct MemberAddress {
    pub(super) node_id: Option<NodeId>,
    participant_id: String,
    label: String,
}

impl MemberAddress {
    fn resolve(db: &Connection, caller: &AuthenticatedCaller, key: &str) -> Result<Self, HubError> {
        if let Some((node_id, participant_id)) = key.split_once('/') {
            let node_id = NodeId::parse(node_id)?;
            let participant = ParticipantId::parse(participant_id)?;
            if caller.source_node_id().is_none() && !caller.allows_node(&node_id) {
                return Err(store::forbidden(
                    "This credential cannot add the requested node.",
                ));
            }
            let name = db
                .query_row(
                    "SELECT p.name FROM node_participants p JOIN nodes n USING(node_id)
                 WHERE p.node_id=?1 AND p.participant_id=?2 AND n.revoked_at_ms IS NULL
                   AND p.kind IN ('human','agent') AND p.synced_epoch=n.session_epoch",
                    params![node_id.as_str(), participant.as_str()],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(store::not_found)?;
            Ok(Self {
                node_id: Some(node_id),
                participant_id: participant.as_str().to_owned(),
                label: name,
            })
        } else {
            if UserId::parse(key).is_err() && ServicePrincipalId::parse(key).is_err() {
                return Err(HubError::invalid("Invalid thread member key."));
            }
            let (participant_id, label) = db
                .query_row(
                    "SELECT COALESCE(participant_id,principal_id),username FROM principals
                 WHERE principal_id=?1 AND disabled_at_ms IS NULL",
                    [key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(store::not_found)?;
            Ok(Self {
                node_id: None,
                participant_id,
                label,
            })
        }
    }
}

const MEMBER_COLUMNS: &str =
    "member_key,node_id,participant_id,handle,role,wake_policy,delivery_mode,state,
    delivery_generation,confirmed_seq,pending_delivery_id,pending_from_seq,pending_to_seq";

fn member_from_row(row: &Row<'_>) -> rusqlite::Result<MemberView> {
    let pending = match parse_opt_col(row, 10)? {
        Some(delivery_id) => Some(PendingThreadDelivery {
            delivery_id,
            from_seq: row.get(11)?,
            to_seq: row.get(12)?,
        }),
        None => None,
    };
    Ok(MemberView {
        member_key: row.get(0)?,
        node_id: parse_opt_col(row, 1)?,
        participant_id: row.get(2)?,
        handle: row.get(3)?,
        role: parse_col(row, 4)?,
        wake_policy: parse_col(row, 5)?,
        delivery_mode: parse_col(row, 6)?,
        state: parse_col(row, 7)?,
        delivery_generation: row.get(8)?,
        confirmed_seq: row.get(9)?,
        pending,
    })
}

pub(super) fn event_data(id: &ThreadId, member: &MemberView) -> Value {
    json!({
        "thread_id": id,
        "member_key": member.member_key,
        "participant_id": member.participant_id,
        "handle": member.handle,
        "role": member.role,
        "wake_policy": member.wake_policy,
        "state": member.state,
        "confirmed_seq": member.confirmed_seq,
        "delivery_generation": member.delivery_generation,
    })
}

pub(super) fn member(
    db: &Connection,
    id: &ThreadId,
    key: &str,
) -> rusqlite::Result<Option<MemberView>> {
    db.prepare_cached(&format!(
        "SELECT {MEMBER_COLUMNS} FROM thread_members WHERE thread_id=?1 AND member_key=?2"
    ))?
    .query_row(params![id.as_str(), key], member_from_row)
    .optional()
}

pub(super) fn self_member(ctx: &Mutation<'_>, id: &ThreadId) -> Result<MemberView, HubError> {
    member(ctx.tx, id, ctx.caller.actor_id())?
        .filter(|member| member.state != ThreadMemberState::Left)
        .ok_or_else(|| store::forbidden("This operation requires live thread membership."))
}

pub(super) fn require_manager(ctx: &Mutation<'_>, id: &ThreadId) -> Result<(), HubError> {
    ctx.caller.require_scope(ServiceScope::Collaborate)?;
    if ctx.caller.is_admin() || self_member(ctx, id)?.role == ThreadRole::Owner {
        return Ok(());
    }
    Err(store::forbidden(
        "Only the thread owner or Hub administrator can manage members.",
    ))
}

pub(super) fn require_contributor(
    ctx: &Mutation<'_>,
    id: &ThreadId,
) -> Result<MemberView, HubError> {
    ctx.caller.require_scope(ServiceScope::Collaborate)?;
    let member = self_member(ctx, id)?;
    if member.role == ThreadRole::Observer {
        return Err(store::forbidden(
            "Observers cannot contribute to this thread.",
        ));
    }
    Ok(member)
}

fn handle_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

pub(super) fn normalize_handle(handle: &str) -> Option<String> {
    let lower = handle.to_ascii_lowercase();
    ((1..=64).contains(&lower.len()) && lower != "all" && lower.bytes().all(handle_char))
        .then_some(lower)
}

fn default_handle(address: &MemberAddress) -> Result<String, HubError> {
    let handle: String = address
        .label
        .bytes()
        .filter(|byte| handle_char(*byte))
        .take(64)
        .map(char::from)
        .collect();
    normalize_handle(&handle)
        .or_else(|| normalize_handle(&address.participant_id))
        .ok_or_else(|| HubError::invalid("A valid explicit member handle is required."))
}

pub(super) fn add_creator(
    ctx: &mut Mutation<'_>,
    thread_id: &ThreadId,
    creator_handle: Option<String>,
) -> Result<(), HubError> {
    let request = ThreadMemberJoinRequest {
        member_key: ctx.caller.actor_id().to_owned(),
        handle: creator_handle,
        role: Some(ThreadRole::Owner),
        wake_policy: None,
        delivery_mode: None,
    };
    add(ctx, thread_id, 0, request)?;
    Ok(())
}

pub(super) fn add(
    ctx: &mut Mutation<'_>,
    id: &ThreadId,
    head: u64,
    body: ThreadMemberJoinRequest,
) -> Result<MemberView, HubError> {
    let address = MemberAddress::resolve(ctx.tx, ctx.caller, &body.member_key)?;
    let existing = member(ctx.tx, id, &body.member_key)?;
    if existing
        .as_ref()
        .is_some_and(|member| member.state != ThreadMemberState::Left)
    {
        return Err(store::conflict(
            "The actor is already a member.",
            "already_member",
        ));
    }
    let role = body.role.unwrap_or(ThreadRole::Member);
    let wake = body.wake_policy.unwrap_or(ThreadWakePolicy::MentionsOnly);
    let mode = body.delivery_mode.unwrap_or(DeliveryMode::QueueUntilIdle);

    if let Some(existing) = existing {
        if let Some(requested_handle) = &body.handle {
            let normalized = normalize_handle(requested_handle)
                .ok_or_else(|| HubError::invalid("Invalid member handle."))?;
            if normalized != existing.handle {
                return Err(store::conflict(
                    "A rejoining member must keep its original handle.",
                    "handle_conflict",
                ));
            }
        }
        let disputed = existing.pending.is_some();
        dispatch::reset_member_on_rejoin(
            ctx.tx,
            id,
            &body.member_key,
            address.node_id.as_ref(),
            role,
            wake,
            mode,
            disputed,
            if disputed {
                existing.confirmed_seq
            } else {
                head
            },
            ctx.now,
        )?;
    } else {
        let handle = match body.handle {
            Some(handle) => normalize_handle(&handle)
                .ok_or_else(|| HubError::invalid("Invalid member handle."))?,
            None => default_handle(&address)?,
        };
        let taken: bool = ctx.tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM thread_members WHERE thread_id=?1 AND handle=?2)",
            params![id.as_str(), handle],
            |row| row.get(0),
        )?;
        if taken {
            return Err(store::conflict(
                "The member handle is already reserved. Specify a distinct handle.",
                "handle_conflict",
            ));
        }
        ctx.tx.execute(
            "INSERT INTO thread_members(thread_id,member_key,participant_id,node_id,handle,role,
              wake_policy,delivery_mode,confirmed_seq,joined_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                id.as_str(),
                body.member_key,
                address.participant_id,
                address.node_id.as_ref().map(NodeId::as_str),
                handle,
                role.as_str(),
                wake.as_str(),
                mode.as_str(),
                head,
                ctx.now
            ],
        )?;
    }
    let saved = member(ctx.tx, id, &body.member_key)?.ok_or_else(store::not_found)?;
    ctx.event(id, EventKind::ThreadMemberUpdated, &event_data(id, &saved))?;
    Ok(saved)
}

pub(crate) async fn list_members(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    query: ThreadMembersQuery,
) -> Result<ThreadMembersResponse, HubError> {
    let limit = store::page_limit(query.limit)?;
    store::verified_read(state, caller, move |db, caller, now| {
        store::require_read(db, caller, &id, now)?;
        let mut page = ThreadMembersResponse {
            items: Vec::new(),
            next_after: None,
        };
        let mut bytes = store::page_budget_bytes(&page)?;
        let mut stmt = db.prepare_cached(&format!(
            "SELECT {MEMBER_COLUMNS} FROM thread_members WHERE thread_id=?1 AND member_key>?2
             ORDER BY member_key LIMIT ?3"
        ))?;
        let mut rows = stmt.query(params![
            id.as_str(),
            query.after.as_deref().unwrap_or(""),
            limit + 1
        ])?;
        while let Some(row) = rows.next()? {
            let member = member_from_row(row)?;
            if page.items.len() >= limit
                || !store::add_to_page(&mut bytes, &member, page.items.len())?
            {
                page.next_after = page.items.last().map(|item| item.member_key.clone());
                break;
            }
            page.items.push(member);
        }
        Ok(page)
    })
    .await
}

pub(crate) async fn join(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    key: &str,
    body: ThreadMemberJoinRequest,
) -> Result<MemberView, HubError> {
    let operation = Operation::new(key, "thread.member.join", Some(&id), &body)?;
    store::mutate(state, caller, operation, move |ctx, _| {
        let board = store::board(ctx.tx, &id, ctx.now)?;
        require_manager(ctx, &id)?;
        store::require_open(&board)?;
        let saved = add(ctx, &id, board.head_seq, body)?;
        Ok((id, saved))
    })
    .await
}

pub(crate) async fn leave(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    key: &str,
    body: ThreadMemberLeaveRequest,
) -> Result<MemberView, HubError> {
    let operation = Operation::new(key, "thread.member.leave", Some(&id), &body)?;
    store::mutate(state, caller, operation, move |ctx, _| {
        let key = body
            .member_key
            .as_deref()
            .unwrap_or_else(|| ctx.caller.actor_id());
        if key != ctx.caller.actor_id() {
            require_manager(ctx, &id)?;
        }
        let board = store::board(ctx.tx, &id, ctx.now)?;
        store::require_open(&board)?;
        let existing = member(ctx.tx, &id, key)?.ok_or_else(store::not_found)?;
        if existing.state == ThreadMemberState::Left {
            return Err(store::conflict(
                "The actor has already left the thread.",
                "already_left",
            ));
        }
        let (count, owners): (u64, u64) = ctx.tx.query_row(
            "SELECT count(*),COALESCE(sum(role='owner'),0) FROM thread_members WHERE thread_id=?1 AND state!='left'",
            [id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if existing.role == ThreadRole::Owner && owners == 1 && count > 1 {
            return Err(store::conflict(
                "The last owner cannot leave while other members remain.",
                "last_owner",
            ));
        }
        dispatch::reset_member_on_leave(ctx.tx, &id, key, ctx.now)?;
        let saved = member(ctx.tx, &id, key)?.ok_or_else(store::not_found)?;
        ctx.event(&id, EventKind::ThreadMemberUpdated, &event_data(&id, &saved))?;
        if count == 1 {
            close_board(ctx, &id)?;
        }
        Ok((id, saved))
    })
    .await
}

pub(crate) async fn resume(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: ThreadId,
    key: &str,
    body: ThreadMemberResumeRequest,
) -> Result<MemberView, HubError> {
    let operation = Operation::new(key, "thread.member.resume", Some(&id), &body)?;
    store::mutate(state, caller, operation, move |ctx, _| {
        ctx.caller.require_scope(ServiceScope::Collaborate)?;
        let key = body
            .member_key
            .as_deref()
            .unwrap_or_else(|| ctx.caller.actor_id());
        if key != ctx.caller.actor_id() {
            require_manager(ctx, &id)?;
        }
        let board = store::board(ctx.tx, &id, ctx.now)?;
        store::require_open(&board)?;
        let existing = member(ctx.tx, &id, key)?.ok_or_else(store::not_found)?;
        if existing.state != ThreadMemberState::PausedUnknown {
            return Err(store::conflict(
                "The member has no uncertain delivery to resume.",
                "not_paused",
            ));
        }
        store::validate_cursor(&board, body.after_seq)?;
        let generation = existing
            .delivery_generation
            .checked_add(1)
            .filter(|seq| i64::try_from(*seq).is_ok())
            .ok_or_else(|| HubError::internal("Thread delivery generation overflow."))?;
        let relevant = if existing.node_id.is_some() && existing.role != ThreadRole::Observer {
            posts::last_relevant_seq(ctx.tx, &id, body.after_seq, board.head_seq, &existing)?
        } else {
            None
        };
        dispatch::apply_resume_schedule(
            ctx.tx,
            &id,
            key,
            existing.delivery_mode,
            generation,
            body.after_seq,
            board.head_seq,
            relevant,
        )?;
        let saved = member(ctx.tx, &id, key)?.ok_or_else(store::not_found)?;
        ctx.event(
            &id,
            EventKind::ThreadMemberUpdated,
            &event_data(&id, &saved),
        )?;
        Ok((id, saved))
    })
    .await
}
