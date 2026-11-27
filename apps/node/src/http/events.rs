use std::{collections::VecDeque, time::Duration};

use axum::{
    Json,
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use cooplet_client::NDJSON_MEDIA_TYPE;
use cooplet_protocol::{
    ErrorCode, EventEnvelope, EventKind, EventStreamFrame, EventsPage, NodeId, ProblemDetails,
    TokenId,
};
use cooplet_storage::{EventNotice, ReadPool};
use futures_util::stream::unfold;
use http::header::CONTENT_TYPE;
use rusqlite::{Connection, ToSql, params};
use serde::Deserialize;
use tokio::{
    sync::{broadcast, watch},
    time::{Instant, sleep_until},
};
use utoipa::IntoParams;

use super::{Caller, NodeState, Query};
use crate::{
    auth::{Auth, Principal},
    error::{NodeError, Result},
    now_ms,
    storage::{MAX_EVENT_FRAME_BYTES, json_encoded_len, read_event},
};

const PAGE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PAGE_SIZE: u16 = 100;
const MAX_PAGE_SIZE: u16 = 500;
const CATCH_UP_BATCH: u16 = 256;
const PING_INTERVAL: Duration = Duration::from_secs(15);
const AUTH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct EventListQuery {
    /// Exclusive cursor, with zero starting at the oldest retained event.
    #[serde(default)]
    after: u64,
    #[param(minimum = 1, maximum = 500)]
    limit: Option<u16>,
    /// Comma-separated event kind names.
    kinds: Option<String>,
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct EventStreamQuery {
    /// Exclusive cursor, with zero starting at the oldest retained event.
    #[serde(default)]
    after: u64,
    /// Comma-separated event kind names.
    kinds: Option<String>,
}

#[utoipa::path(
    get, path = "/v1/events", params(EventListQuery), security(("bearer" = [])),
    responses(
        (status = 200, description = "Events in sequence order", body = EventsPage),
        (status = 400, description = "Invalid query", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 401, description = "Invalid bearer credential", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 403, description = "Global read permission is required", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 410, description = "Cursor lies outside the retained window", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(crate) async fn list(
    State(state): State<NodeState>,
    Caller(principal): Caller,
    Query(query): Query<EventListQuery>,
) -> Result<Json<EventsPage>> {
    require_reader(&principal)?;
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(NodeError::new(
            ErrorCode::InvalidRequest,
            "Event limit must be 1 through 500",
        ));
    }
    let kinds = parse_kinds(query.kinds.as_deref())?;
    match read_page(
        &state.reads,
        &state.info.node_id,
        Cursor::Open(query.after),
        kinds.as_deref(),
        limit,
    )
    .await?
    {
        Snapshot::Page(page) => Ok(Json(page)),
        Snapshot::Gap(window) => Err(window.problem()),
    }
}

#[utoipa::path(
    get, path = "/v1/events/stream", params(EventStreamQuery), security(("bearer" = [])),
    responses(
        (status = 200, description = "Line-delimited event, ping, or terminal resync_required frames", body = EventStreamFrame, content_type = "application/x-ndjson"),
        (status = 400, description = "Invalid query", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 401, description = "Invalid bearer credential", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 403, description = "Global read permission is required", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 410, description = "Cursor lies outside the retained window", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(crate) async fn stream(
    State(state): State<NodeState>,
    Caller(principal): Caller,
    Query(query): Query<EventStreamQuery>,
) -> Result<Response> {
    require_reader(&principal)?;
    let kinds = parse_kinds(query.kinds.as_deref())?;
    let events = state.db.subscribe();
    let first = read_page(
        &state.reads,
        &state.info.node_id,
        Cursor::Open(query.after),
        kinds.as_deref(),
        CATCH_UP_BATCH,
    )
    .await?;
    let page = match first {
        Snapshot::Page(page) => page,
        Snapshot::Gap(window) => return Err(window.problem()),
    };

    let now = Instant::now();
    let session = StreamState {
        reads: state.reads,
        node: state.info.node_id,
        kinds,
        token: principal.token_id,
        events,
        shutdown: state.shutdown.subscribe(),
        cursor: page.next_seq,
        pending: page.items.into(),
        catch_up: page.has_more,
        authenticate_at: now + AUTH_INTERVAL,
        ping_at: now + PING_INTERVAL,
    };
    let frames = unfold(Some(session), |session| async move {
        let mut session = session?;
        match session.next_frame().await {
            Ok(None) => None,
            Ok(Some(frame)) => {
                let terminal = matches!(frame, EventStreamFrame::ResyncRequired { .. });
                let line = encode_frame(&frame);
                Some((line, (!terminal).then_some(session)))
            }
            Err(error) => Some((Err(error), None)),
        }
    });
    Ok((
        [(CONTENT_TYPE, NDJSON_MEDIA_TYPE)],
        Body::from_stream(frames),
    )
        .into_response())
}

fn require_reader(principal: &Principal) -> Result<()> {
    if principal.can_read_all() {
        Ok(())
    } else {
        Err(NodeError::new(
            ErrorCode::Forbidden,
            "Reading the event log requires Node-wide read permission",
        ))
    }
}

fn parse_kinds(csv: Option<&str>) -> Result<Option<Vec<EventKind>>> {
    csv.map(|csv| {
        let mut kinds = Vec::new();
        for value in csv.split(',') {
            let kind: EventKind = value
                .parse()
                .map_err(|_| NodeError::new(ErrorCode::InvalidRequest, "Unknown event kind"))?;
            if !kinds.contains(&kind) {
                kinds.push(kind);
            }
        }
        Ok(kinds)
    })
    .transpose()
}

fn matches_kinds(filter: Option<&[EventKind]>, kind: EventKind) -> bool {
    filter.is_none_or(|kinds| kinds.contains(&kind))
}

#[derive(Clone, Copy)]
enum Cursor {
    Open(u64),
    Follow(u64),
}

#[derive(Clone, Copy)]
struct Window {
    floor: u64,
    head: u64,
}

impl Window {
    fn position(self, cursor: Cursor) -> Option<u64> {
        let after = match cursor {
            Cursor::Open(0) => return Some(self.floor),
            Cursor::Open(after) | Cursor::Follow(after) => after,
        };
        (self.floor <= after && after <= self.head).then_some(after)
    }

    fn problem(self) -> NodeError {
        NodeError::new(
            ErrorCode::ResyncRequired,
            "Event cursor lies outside the retained window",
        )
        .with_extension("oldest_available_seq", self.floor + 1)
        .with_extension("latest_seq", self.head)
    }

    fn frame(self) -> EventStreamFrame {
        EventStreamFrame::ResyncRequired {
            oldest_available_seq: self.floor + 1,
            latest_seq: self.head,
            reason: "cursor_pruned".into(),
        }
    }

    fn page(self, items: Vec<EventEnvelope>, has_more: bool) -> EventsPage {
        let next_seq = if has_more {
            items.last().map_or(self.floor, |event| event.seq)
        } else {
            self.head
        };
        EventsPage {
            items,
            next_seq,
            oldest_available_seq: self.floor + 1,
            latest_seq: self.head,
            has_more,
        }
    }

    fn empty_page_bytes(self, next_seq: u64, has_more: bool) -> rusqlite::Result<usize> {
        let mut page = self.page(Vec::new(), has_more);
        page.next_seq = next_seq;
        json_bytes(&page)
    }
}

enum Snapshot {
    Page(EventsPage),
    Gap(Window),
}

async fn read_page(
    reads: &ReadPool,
    node: &NodeId,
    cursor: Cursor,
    kinds: Option<&[EventKind]>,
    limit: u16,
) -> Result<Snapshot> {
    let node = node.clone();
    let kinds = kinds.map(ToOwned::to_owned);
    reads
        .read(move |conn| {
            snapshot(conn, &node, cursor, kinds.as_deref(), limit).map_err(Into::into)
        })
        .await
        .map_err(Into::into)
}

pub(crate) fn events_query_sql(kind_count: usize) -> String {
    match kind_count {
        0 => "SELECT seq,event_id,kind,aggregate_type,aggregate_id,actor_id,payload_json,created_at_ms \
              FROM events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3"
            .to_owned(),
        1 => "SELECT seq,event_id,kind,aggregate_type,aggregate_id,actor_id,payload_json,created_at_ms \
              FROM events WHERE kind = ?3 AND seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?4"
            .to_owned(),
        many => {
            // Merge distinct per-kind index scans (`idx_events_kind_seq`) directly
            // in sequence order with zero temporary B-trees.
            let mut sql = String::new();
            for index in 0..many {
                if index > 0 {
                    sql.push_str(" UNION ALL ");
                }
                let param = index + 3;
                use std::fmt::Write as _;
                let _ = write!(
                    sql,
                    "SELECT seq,event_id,kind,aggregate_type,aggregate_id,actor_id,payload_json,created_at_ms \
                     FROM events WHERE kind = ?{param} AND seq > ?1 AND seq <= ?2"
                );
            }
            let limit_param = many + 3;
            use std::fmt::Write as _;
            let _ = write!(sql, " ORDER BY seq LIMIT ?{limit_param}");
            sql
        }
    }
}

fn snapshot(
    conn: &Connection,
    node: &NodeId,
    cursor: Cursor,
    kinds: Option<&[EventKind]>,
    limit: u16,
) -> rusqlite::Result<Snapshot> {
    let tx = conn.unchecked_transaction()?;
    let window = tx.query_row(
        "SELECT events_pruned_through_seq, events_head_seq FROM node_meta WHERE id = 1",
        [],
        |row| {
            Ok(Window {
                floor: row.get(0)?,
                head: row.get(1)?,
            })
        },
    )?;
    let Some(after) = window.position(cursor) else {
        return Ok(Snapshot::Gap(window));
    };

    let fetch_limit = u32::from(limit) + 1;
    let mut query;
    let mut rows = match kinds {
        None | Some([]) => {
            let sql = events_query_sql(0);
            query = tx.prepare_cached(&sql)?;
            query.query(params![after, window.head, fetch_limit])?
        }
        Some([single]) => {
            let sql = events_query_sql(1);
            query = tx.prepare_cached(&sql)?;
            query.query(params![after, window.head, single.as_str(), fetch_limit])?
        }
        Some(many) => {
            let sql = events_query_sql(many.len());
            query = tx.prepare_cached(&sql)?;
            let mut bindings: Vec<&dyn ToSql> = Vec::with_capacity(many.len() + 3);
            bindings.push(&after);
            bindings.push(&window.head);
            let kind_strs: Vec<&'static str> = many.iter().map(|kind| kind.as_str()).collect();
            for kind in &kind_strs {
                bindings.push(kind);
            }
            bindings.push(&fetch_limit);
            query.query(bindings.as_slice())?
        }
    };
    let mut items = Vec::new();
    let mut bytes = 0;
    let mut has_more = false;

    while let Some(row) = rows.next()? {
        if items.len() == usize::from(limit) {
            has_more = true;
            break;
        }
        let event = read_event(row, node)?;
        let extra = usize::from(!items.is_empty()) + json_bytes(&event)?;
        if bytes + extra + window.empty_page_bytes(event.seq, true)? > PAGE_BYTES {
            has_more = true;
            break;
        }
        bytes += extra;
        items.push(event);
    }
    // At end of scan the cursor becomes the head, including filtered rows.
    // That cursor and the final boolean have a slightly different byte cost.
    if !has_more && bytes + window.empty_page_bytes(window.head, false)? > PAGE_BYTES {
        items.pop();
        has_more = true;
    }
    if has_more && items.is_empty() {
        return Err(rusqlite::Error::ToSqlConversionFailure(
            std::io::Error::other("An event exceeds the page budget").into(),
        ));
    }
    Ok(Snapshot::Page(window.page(items, has_more)))
}

fn json_bytes(value: &impl serde::Serialize) -> rusqlite::Result<usize> {
    json_encoded_len(value)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

fn encode_frame(frame: &EventStreamFrame) -> Result<Bytes> {
    let mut bytes = serde_json::to_vec(frame).map_err(NodeError::internal)?;
    if bytes.len() > MAX_EVENT_FRAME_BYTES {
        return Err(NodeError::internal(
            "An event exceeds the NDJSON frame budget",
        ));
    }
    bytes.push(b'\n');
    Ok(bytes.into())
}

struct StreamState {
    reads: ReadPool,
    node: NodeId,
    kinds: Option<Vec<EventKind>>,
    token: TokenId,
    events: broadcast::Receiver<EventNotice>,
    shutdown: watch::Receiver<bool>,
    cursor: u64,
    pending: VecDeque<EventEnvelope>,
    catch_up: bool,
    authenticate_at: Instant,
    ping_at: Instant,
}

impl StreamState {
    fn handle_notice(&mut self, notice: EventNotice) {
        if notice.seq <= self.cursor {
            return;
        }
        if !self.catch_up
            && notice.seq == self.cursor + 1
            && !matches_kinds(self.kinds.as_deref(), notice.kind)
        {
            self.cursor = notice.seq;
            return;
        }
        self.catch_up = true;
    }

    async fn next_frame(&mut self) -> Result<Option<EventStreamFrame>> {
        loop {
            if *self.shutdown.borrow() {
                return Ok(None);
            }
            let now = Instant::now();
            if now >= self.authenticate_at {
                if !Auth::is_active(&self.reads, self.token.clone(), now_ms()?).await? {
                    return Ok(None);
                }
                self.authenticate_at = now + AUTH_INTERVAL;
            }
            let ping = now >= self.ping_at;
            // A quiet writer can prune without posting a broadcast event.
            if self.pending.is_empty() && (self.catch_up || ping) {
                match read_page(
                    &self.reads,
                    &self.node,
                    Cursor::Follow(self.cursor),
                    self.kinds.as_deref(),
                    CATCH_UP_BATCH,
                )
                .await?
                {
                    Snapshot::Gap(window) => return Ok(Some(window.frame())),
                    Snapshot::Page(page) => {
                        self.cursor = page.next_seq;
                        self.pending = page.items.into();
                        self.catch_up = page.has_more;
                    }
                }
            }
            if ping {
                self.ping_at = now + PING_INTERVAL;
                return Ok(Some(EventStreamFrame::Ping {}));
            }
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(EventStreamFrame::Event { event }));
            }

            tokio::select! {
                biased;
                _ = self.shutdown.changed() => return Ok(None),
                () = sleep_until(self.ping_at.min(self.authenticate_at)) => {},
                notification = self.events.recv() => match notification {
                    Ok(notice) => {
                        self.handle_notice(notice);
                        loop {
                            match self.events.try_recv() {
                                Ok(next) => self.handle_notice(next),
                                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                                    self.catch_up = true;
                                    break;
                                }
                                Err(broadcast::error::TryRecvError::Empty) => break,
                                Err(broadcast::error::TryRecvError::Closed) => return Ok(None),
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => self.catch_up = true,
                    Err(broadcast::error::RecvError::Closed) => return Ok(None),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
