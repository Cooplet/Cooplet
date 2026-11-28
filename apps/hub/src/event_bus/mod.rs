mod stream;

#[cfg(test)]
mod tests;

use std::{collections::HashMap, sync::Arc};

use axum::response::Response;
use bytes::Bytes;
use cooplet_protocol::{
    AggregateType, ErrorCode, EventEnvelope, EventId, EventKind, EventStreamFrame, EventsPage,
    HubId, ServiceScope, SourceKind, ThreadId,
};
use cooplet_storage::ReadPool;
use rusqlite::{Connection, Row, ToSql, params};
use serde::{Deserialize, de::IgnoredAny};
use tokio::sync::watch;
use utoipa::IntoParams;

use crate::{
    AppState,
    auth::AuthenticatedCaller,
    error::HubError,
    storage::{
        events::{EventAudience, StoredEvent},
        sql::{json_byte_len, read_verified_tx_with_clock},
    },
};

pub(crate) use stream::EventWake;

const PAGE_SCAN_LIMIT: usize = 512;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_INDEXED_KINDS: usize = 8;

const SELECT_ALL_EVENTS: &str = "
    SELECT seq, event_id, kind, aggregate_type, aggregate_id, actor_id,
           payload_json, created_at_ms
      FROM events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3";
const SELECT_SINGLE_KIND_EVENTS: &str = "
    SELECT seq, event_id, kind, aggregate_type, aggregate_id, actor_id,
           payload_json, created_at_ms
      FROM events WHERE kind = ?1 AND seq > ?2 AND seq <= ?3 ORDER BY seq LIMIT ?4";

fn multi_kind_query_sql(kind_count: usize) -> String {
    // Merge distinct per-kind index scans (`idx_hub_events_kind_seq`) directly
    // in sequence order with zero temporary B-trees or point lookups.
    let mut sql = String::new();
    for index in 0..kind_count {
        if index > 0 {
            sql.push_str(" UNION ALL ");
        }
        let param = index + 3;
        use std::fmt::Write as _;
        let _ = write!(
            sql,
            "SELECT seq, event_id, kind, aggregate_type, aggregate_id, actor_id, \
             payload_json, created_at_ms \
             FROM events WHERE kind = ?{param} AND seq > ?1 AND seq <= ?2"
        );
    }
    let limit_param = kind_count + 3;
    use std::fmt::Write as _;
    let _ = write!(sql, " ORDER BY seq LIMIT ?{limit_param}");
    sql
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct EventsQuery {
    /// Last scanned sequence from the previous page, or zero to start at the retained floor.
    #[serde(default)]
    pub(crate) after: u64,
    /// Maximum number of events in a page.
    #[param(minimum = 1, maximum = 500, default = 100)]
    pub(crate) limit: Option<u16>,
    /// Comma-separated event kinds.
    pub(crate) kinds: Option<String>,
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct EventStreamQuery {
    /// Last scanned sequence, or zero to start at the retained floor.
    #[serde(default)]
    pub(crate) after: u64,
    /// Comma-separated event kinds.
    pub(crate) kinds: Option<String>,
}

fn parse_kinds(csv: Option<&str>) -> Result<Vec<EventKind>, HubError> {
    let Some(csv) = csv else {
        return Ok(Vec::new());
    };
    let mut kinds: Vec<EventKind> = csv
        .split(',')
        .map(|kind| {
            kind.parse::<EventKind>()
                .map_err(|_| HubError::invalid("Each event kind must be a known label."))
        })
        .collect::<Result<_, _>>()?;
    kinds.sort_unstable_by_key(|kind| kind.as_str());
    kinds.dedup();
    Ok(kinds)
}

impl EventsQuery {
    fn validated(&self) -> Result<(usize, Vec<EventKind>), HubError> {
        let limit = self.limit.unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(HubError::invalid(
                "The event page limit must be 1 through 500.",
            ));
        }
        Ok((usize::from(limit), parse_kinds(self.kinds.as_deref())?))
    }
}

pub(crate) async fn page(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: EventsQuery,
) -> Result<EventsPage, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    let (limit, kinds) = query.validated()?;
    let reader = Reader::new(state, caller, kinds);
    reader
        .snapshot_page(
            Cursor::Initial(query.after),
            Budget {
                rows: PAGE_SCAN_LIMIT,
                items: limit,
            },
        )
        .await?
        .into_page()
}

pub(crate) async fn stream(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: EventStreamQuery,
    revocations: watch::Receiver<u64>,
) -> Result<Response, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    let kinds = parse_kinds(query.kinds.as_deref())?;
    let reader = Reader::new(state, caller, kinds);
    stream::response(
        reader,
        query.after,
        state.event_wake.subscribe(),
        revocations,
        state.thread_acl_watch.subscribe(),
        state.shutdown.clone(),
    )
    .await
}

struct Reader {
    reads: ReadPool,
    caller: AuthenticatedCaller,
    hub_id: HubId,
    kinds: Vec<EventKind>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl Reader {
    fn new(state: &AppState, caller: &AuthenticatedCaller, kinds: Vec<EventKind>) -> Arc<Self> {
        Arc::new(Self {
            reads: state.reads.clone(),
            caller: caller.clone(),
            hub_id: state.meta.hub_id.clone(),
            kinds,
            clock: state.wall_clock.clone(),
        })
    }

    async fn snapshot_page(
        self: &Arc<Self>,
        cursor: Cursor,
        budget: Budget,
    ) -> Result<PageSnapshot, HubError> {
        let reader = Arc::clone(self);
        read_verified_tx_with_clock(
            &self.reads,
            &self.caller,
            self.clock.clone(),
            move |tx, _, now| read_page_snapshot(tx, &reader, cursor, budget, now),
        )
        .await
    }

    async fn snapshot_stream(
        self: &Arc<Self>,
        cursor: Cursor,
        budget: Budget,
    ) -> Result<StreamSnapshot, HubError> {
        let reader = Arc::clone(self);
        read_verified_tx_with_clock(
            &self.reads,
            &self.caller,
            self.clock.clone(),
            move |tx, _, now| read_stream_snapshot(tx, &reader, cursor, budget, now),
        )
        .await
    }
}

#[derive(Clone, Copy)]
enum Cursor {
    Initial(u64),
    Resume(u64),
}

#[derive(Clone, Copy)]
struct Budget {
    rows: usize,
    items: usize,
}

#[derive(Clone, Copy)]
struct Watermarks {
    floor: u64,
    head: u64,
}

impl Watermarks {
    fn read(db: &Connection) -> Result<Self, HubError> {
        let mut statement = db.prepare_cached(
            "SELECT events_pruned_through_seq, events_head_seq FROM hub_meta WHERE id = 1",
        )?;
        let value = statement.query_row([], |row| {
            Ok(Self {
                floor: row.get(0)?,
                head: row.get(1)?,
            })
        })?;
        if value.floor > value.head {
            return Err(HubError::internal("Inconsistent event watermarks"));
        }
        Ok(value)
    }

    fn oldest(self) -> u64 {
        // SQLite sequence numbers fit in i64, so the successor fits in u64.
        self.floor + 1
    }

    fn resolve(self, cursor: Cursor) -> Option<u64> {
        match cursor {
            Cursor::Initial(0) => Some(self.floor),
            Cursor::Initial(seq) | Cursor::Resume(seq) => {
                (self.floor..=self.head).contains(&seq).then_some(seq)
            }
        }
    }

    fn problem(self) -> HubError {
        HubError::new(
            ErrorCode::ResyncRequired,
            "The event cursor is outside the retained sequence window.",
        )
        .with_extension("oldest_available_seq", self.oldest())
        .with_extension("latest_seq", self.head)
    }
}

enum PageSnapshot {
    Page(EventsPage),
    CursorLost(Watermarks),
}

impl PageSnapshot {
    fn into_page(self) -> Result<EventsPage, HubError> {
        match self {
            Self::Page(page) => Ok(page),
            Self::CursorLost(watermarks) => Err(watermarks.problem()),
        }
    }
}

struct StreamBatch {
    start: u64,
    frames: Vec<BufferedFrame>,
    next_seq: u64,
    has_more: bool,
}

type BufferedFrame = (u64, Option<i64>, Bytes);

enum StreamSnapshot {
    Batch(StreamBatch),
    CursorLost(Watermarks),
}

fn read_page_snapshot(
    db: &Connection,
    reader: &Reader,
    cursor: Cursor,
    budget: Budget,
    now: i64,
) -> Result<PageSnapshot, HubError> {
    let watermarks = Watermarks::read(db)?;
    let Some(start) = watermarks.resolve(cursor) else {
        return Ok(PageSnapshot::CursorLost(watermarks));
    };
    let mut page = EventsPage {
        items: Vec::new(),
        next_seq: watermarks.head,
        oldest_available_seq: watermarks.oldest(),
        latest_seq: watermarks.head,
        has_more: false,
    };
    // Reserve the maximum envelope overhead: next_seq cannot have more digits than head,
    // and false is one byte longer than true. Count every serialized item and comma below.
    let mut page_bytes = json_byte_len(&page)?;
    let (next_seq, has_more) = scan_events(
        db,
        reader,
        start,
        watermarks.head,
        budget.rows,
        now,
        |event, _| {
            if page.items.len() >= budget.items {
                return Ok(false);
            }
            let item_bytes = json_byte_len(&event)? + usize::from(!page.items.is_empty());
            if page_bytes + item_bytes > MAX_PAGE_BYTES {
                return Ok(false);
            }
            page_bytes += item_bytes;
            page.items.push(event);
            Ok(true)
        },
    )?;
    page.next_seq = next_seq;
    page.has_more = has_more;
    Ok(PageSnapshot::Page(page))
}

fn read_stream_snapshot(
    db: &Connection,
    reader: &Reader,
    cursor: Cursor,
    budget: Budget,
    now: i64,
) -> Result<StreamSnapshot, HubError> {
    let watermarks = Watermarks::read(db)?;
    let Some(start) = watermarks.resolve(cursor) else {
        return Ok(StreamSnapshot::CursorLost(watermarks));
    };
    let mut frames = Vec::new();
    let mut buffered_bytes = 0usize;
    let (next_seq, has_more) = scan_events(
        db,
        reader,
        start,
        watermarks.head,
        budget.rows,
        now,
        |event, expiry| {
            if frames.len() >= budget.items {
                return Ok(false);
            }
            let seq = event.seq;
            let mut encoded = serde_json::to_vec(&EventStreamFrame::Event { event })?;
            encoded.push(b'\n');
            if !frames.is_empty() && buffered_bytes + encoded.len() > MAX_PAGE_BYTES {
                return Ok(false);
            }
            buffered_bytes += encoded.len();
            frames.push((seq, expiry, Bytes::from(encoded)));
            Ok(true)
        },
    )?;
    Ok(StreamSnapshot::Batch(StreamBatch {
        start,
        frames,
        next_seq,
        has_more,
    }))
}

type Projected = (EventEnvelope, Option<i64>);

struct Projection<'a> {
    db: &'a Connection,
    reader: &'a Reader,
    now: i64,
    thread_expiries: HashMap<ThreadId, Option<i64>>,
}

impl Projection<'_> {
    // Some(None) is public for the life of this authorization snapshot.
    fn audience_expiry(
        &mut self,
        audience: &EventAudience,
    ) -> Result<Option<Option<i64>>, HubError> {
        let caller = &self.reader.caller;
        let visible = match audience {
            EventAudience::OwnerOnly { actor_id } => {
                !actor_id.is_empty() && (caller.actor_id() == actor_id || caller.is_global_reader())
            }
            EventAudience::Nodes { node_ids } => {
                !node_ids.is_empty() && node_ids.iter().all(|id| caller.allows_node(id))
            }
            EventAudience::Thread { .. } if caller.is_global_reader() => true,
            EventAudience::Thread { thread_id } => {
                let expiry = if let Some(expiry) = self.thread_expiries.get(thread_id) {
                    *expiry
                } else {
                    let expiry =
                        crate::threads::thread_scope_expiry(self.db, caller, thread_id, self.now)?;
                    self.thread_expiries.insert(thread_id.clone(), expiry);
                    expiry
                };
                return Ok(expiry.map(|at_ms| (at_ms != i64::MAX).then_some(at_ms)));
            }
            EventAudience::GlobalArchiveOnly => caller.is_global_reader(),
        };
        Ok(visible.then_some(None))
    }
}

fn scan_events(
    db: &Connection,
    reader: &Reader,
    start: u64,
    head: u64,
    row_limit: usize,
    now: i64,
    accept: impl FnMut(EventEnvelope, Option<i64>) -> Result<bool, HubError>,
) -> Result<(u64, bool), HubError> {
    let mut projection = Projection {
        db,
        reader,
        now,
        thread_expiries: HashMap::new(),
    };
    if reader.kinds.len() == 1 {
        let mut statement = db.prepare_cached(SELECT_SINGLE_KIND_EVENTS)?;
        let mut rows = statement.query(params![
            reader.kinds[0].as_str(),
            start,
            head,
            row_limit + 1
        ])?;
        return scan_rows(&mut rows, &mut projection, start, head, row_limit, accept);
    }
    if (2..=MAX_INDEXED_KINDS).contains(&reader.kinds.len()) {
        let sql = multi_kind_query_sql(reader.kinds.len());
        let mut statement = db.prepare_cached(&sql)?;
        let fetch_limit = row_limit + 1;
        let kind_strs: Vec<&'static str> = reader.kinds.iter().map(|kind| kind.as_str()).collect();
        let mut bindings: Vec<&dyn ToSql> = Vec::with_capacity(kind_strs.len() + 3);
        bindings.push(&start);
        bindings.push(&head);
        for kind in &kind_strs {
            bindings.push(kind);
        }
        bindings.push(&fetch_limit);
        let mut rows = statement.query(bindings.as_slice())?;
        return scan_rows(&mut rows, &mut projection, start, head, row_limit, accept);
    }
    let mut statement = db.prepare_cached(SELECT_ALL_EVENTS)?;
    let mut rows = statement.query(params![start, head, row_limit + 1])?;
    scan_rows(&mut rows, &mut projection, start, head, row_limit, accept)
}

fn scan_rows(
    rows: &mut rusqlite::Rows<'_>,
    projection: &mut Projection<'_>,
    start: u64,
    head: u64,
    row_limit: usize,
    mut accept: impl FnMut(EventEnvelope, Option<i64>) -> Result<bool, HubError>,
) -> Result<(u64, bool), HubError> {
    let mut next_seq = start;
    for _ in 0..row_limit {
        let Some(row) = rows.next()? else {
            return Ok((head, false));
        };
        let seq: u64 = row.get(0)?;
        if let Some((event, expiry)) = project(row, projection, seq)?
            && !accept(event, expiry)?
        {
            // Leave this event strictly after next_seq so it remains visible next time.
            return Ok((next_seq, true));
        }
        next_seq = seq;
    }
    let has_more = rows.next()?.is_some();
    if !has_more {
        next_seq = head;
    }
    Ok((next_seq, has_more))
}

fn project(
    row: &Row<'_>,
    projection: &mut Projection<'_>,
    seq: u64,
) -> Result<Option<Projected>, HubError> {
    let Ok(kind) = row
        .get_ref(2)?
        .as_str()
        .map_err(HubError::internal)?
        .parse::<EventKind>()
    else {
        return Ok(None);
    };
    if !projection.reader.kinds.is_empty() && !projection.reader.kinds.contains(&kind) {
        return Ok(None);
    }
    let raw = row.get_ref(6)?.as_str().map_err(HubError::internal)?;
    // Skip building a JSON DOM for events outside the caller's audience.
    let Ok(head) = serde_json::from_str::<StoredEvent<IgnoredAny>>(raw) else {
        return Ok(None);
    };
    let Some(expiry) = projection.audience_expiry(&head.audience)? else {
        return Ok(None);
    };
    let Ok(mut stored) = serde_json::from_str::<StoredEvent>(raw) else {
        return Ok(None);
    };
    let can_manage_node = matches!(&stored.audience, EventAudience::Nodes { node_ids } if node_ids.len() == 1)
        && projection
            .reader
            .caller
            .require_scope(ServiceScope::Manage)
            .is_ok();
    if matches!(
        kind,
        EventKind::ParticipantCreated
            | EventKind::ParticipantStateChanged
            | EventKind::ParticipantAwaitingApproval
    ) && !can_manage_node
    {
        crate::bridge::redact_approval_value(&mut stored.data);
    }
    let event_id = row
        .get::<_, String>(1)?
        .parse::<EventId>()
        .map_err(HubError::internal)?;
    let aggregate_type = row
        .get::<_, String>(3)?
        .parse::<AggregateType>()
        .map_err(HubError::internal)?;
    Ok(Some((
        EventEnvelope {
            seq,
            event_id,
            source_kind: SourceKind::Hub,
            source_id: projection.reader.hub_id.clone().into(),
            kind,
            aggregate_type,
            aggregate_id: row.get(4)?,
            actor_id: row.get(5)?,
            payload: stored.data,
            created_at_ms: row.get(7)?,
        },
        expiry,
    )))
}
