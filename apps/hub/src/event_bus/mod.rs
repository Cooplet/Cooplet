mod stream;

#[cfg(test)]
mod tests;

use std::{collections::HashSet, sync::Arc};

use axum::response::Response;
use cooplet_protocol::{
    AggregateType, ErrorCode, EventEnvelope, EventId, EventKind, EventsPage, HubId, ServiceScope,
    SourceKind, ThreadId,
};
use cooplet_storage::ReadPool;
use rusqlite::{Connection, Row, params};
use serde::Deserialize;
use tokio::sync::watch;
use utoipa::IntoParams;

use crate::{
    AppState,
    auth::{self, AuthenticatedCaller},
    error::HubError,
    now_ms,
    storage::events::{EventAudience, StoredEvent},
};

pub(crate) use stream::EventWake;

const PAGE_SCAN_LIMIT: usize = 512;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct EventsQuery {
    /// Last scanned sequence from the previous page, or zero to start at the retained floor.
    #[serde(default)]
    pub(crate) after: u64,
    /// Maximum number of events in a page.
    #[param(minimum = 1, maximum = 500)]
    pub(crate) limit: Option<u16>,
    /// Comma-separated event kinds.
    pub(crate) kinds: Option<String>,
}

impl EventsQuery {
    fn validated(&self) -> Result<Filter, HubError> {
        let limit = self.limit.unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(HubError::invalid(
                "The event page limit must be 1 through 500.",
            ));
        }
        let kinds = if let Some(csv) = &self.kinds {
            csv.split(',')
                .map(|kind| {
                    kind.parse::<EventKind>()
                        .map_err(|_| HubError::invalid("Each event kind must be a known label."))
                })
                .collect::<Result<_, _>>()?
        } else {
            HashSet::new()
        };
        Ok(Filter {
            limit: usize::from(limit),
            kinds,
        })
    }
}

struct Filter {
    limit: usize,
    kinds: HashSet<EventKind>,
}

pub(crate) async fn page(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: EventsQuery,
) -> Result<EventsPage, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    let Filter { kinds, limit } = query.validated()?;
    let reader = Reader::new(state, caller, kinds);
    reader
        .snapshot(
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
    query: EventsQuery,
    revocations: watch::Receiver<u64>,
) -> Result<Response, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    let filter = query.validated()?;
    let reader = Reader::new(state, caller, filter.kinds);
    stream::response(
        reader,
        query.after,
        query.limit.map(|_| filter.limit),
        state.event_wake.subscribe(),
        revocations,
        state.shutdown.clone(),
    )
    .await
}

/// A deliberately restricted ACL until the Hub has a Threads membership lifecycle.
pub(crate) fn can_access_thread_scope(caller: &AuthenticatedCaller, _thread_id: &ThreadId) -> bool {
    caller.is_global_reader()
}

fn visible(caller: &AuthenticatedCaller, audience: &EventAudience) -> bool {
    match audience {
        EventAudience::OwnerOnly { actor_id } => {
            !actor_id.is_empty() && (caller.actor_id() == actor_id || caller.is_global_reader())
        }
        EventAudience::Nodes { node_ids } => {
            !node_ids.is_empty() && node_ids.iter().all(|id| caller.allows_node(id))
        }
        EventAudience::Thread { thread_id } => can_access_thread_scope(caller, thread_id),
        EventAudience::GlobalArchiveOnly => caller.is_global_reader(),
    }
}

struct Reader {
    reads: ReadPool,
    caller: AuthenticatedCaller,
    hub_id: HubId,
    kinds: HashSet<EventKind>,
}

impl Reader {
    fn new(state: &AppState, caller: &AuthenticatedCaller, kinds: HashSet<EventKind>) -> Arc<Self> {
        Arc::new(Self {
            reads: state.reads.clone(),
            caller: caller.clone(),
            hub_id: state.meta.hub_id.clone(),
            kinds,
        })
    }

    async fn snapshot(
        self: &Arc<Self>,
        cursor: Cursor,
        budget: Budget,
    ) -> Result<Snapshot, HubError> {
        let reader = Arc::clone(self);
        self.reads
            .read(move |connection| {
                let tx = connection.unchecked_transaction()?;
                let result = auth::reverify_caller_tx(&tx, &reader.caller, now_ms())
                    .and_then(|()| read_snapshot(&tx, &reader, cursor, budget));
                tx.commit()?;
                Ok(result)
            })
            .await?
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
        let value = db.query_row(
            "SELECT events_pruned_through_seq, events_head_seq FROM hub_meta WHERE id = 1",
            [],
            |row| {
                Ok(Self {
                    floor: row.get(0)?,
                    head: row.get(1)?,
                })
            },
        )?;
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

enum Snapshot {
    Page(EventsPage),
    CursorLost(Watermarks),
}

impl Snapshot {
    fn into_page(self) -> Result<EventsPage, HubError> {
        match self {
            Self::Page(page) => Ok(page),
            Self::CursorLost(watermarks) => Err(watermarks.problem()),
        }
    }
}

fn read_snapshot(
    db: &Connection,
    reader: &Reader,
    cursor: Cursor,
    budget: Budget,
) -> Result<Snapshot, HubError> {
    let watermarks = Watermarks::read(db)?;
    let Some(start) = watermarks.resolve(cursor) else {
        return Ok(Snapshot::CursorLost(watermarks));
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
    let mut page_bytes = serde_json::to_vec(&page)?.len();
    page.next_seq = start;
    let mut statement = db.prepare_cached(
        "SELECT seq, event_id, kind, aggregate_type, aggregate_id, actor_id,
                payload_json, created_at_ms
         FROM events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3",
    )?;
    let mut rows = statement.query(params![start, watermarks.head, budget.rows + 1])?;
    for _ in 0..budget.rows {
        let Some(row) = rows.next()? else {
            page.next_seq = watermarks.head;
            return Ok(Snapshot::Page(page));
        };
        let seq = row.get(0)?;
        if let Some(event) = project(row, reader, seq)? {
            let item_bytes =
                serde_json::to_vec(&event)?.len() + usize::from(!page.items.is_empty());
            if page.items.len() >= budget.items || page_bytes + item_bytes > MAX_PAGE_BYTES {
                // Leave this event strictly after next_seq so it remains visible next time.
                page.has_more = true;
                return Ok(Snapshot::Page(page));
            }
            page_bytes += item_bytes;
            page.items.push(event);
        }
        page.next_seq = seq;
    }
    page.has_more = rows.next()?.is_some();
    if !page.has_more {
        page.next_seq = watermarks.head;
    }
    Ok(Snapshot::Page(page))
}

fn project(row: &Row<'_>, reader: &Reader, seq: u64) -> Result<Option<EventEnvelope>, HubError> {
    let Ok(kind) = row
        .get_ref(2)?
        .as_str()
        .map_err(HubError::internal)?
        .parse::<EventKind>()
    else {
        return Ok(None);
    };
    if !reader.kinds.is_empty() && !reader.kinds.contains(&kind) {
        return Ok(None);
    }
    let raw = row.get_ref(6)?.as_str().map_err(HubError::internal)?;
    // Unknown or missing stored audiences are invisible, including to archive readers.
    let Ok(stored) = serde_json::from_str::<StoredEvent>(raw) else {
        return Ok(None);
    };
    if !visible(&reader.caller, &stored.audience) {
        return Ok(None);
    }
    let event_id = row
        .get::<_, String>(1)?
        .parse::<EventId>()
        .map_err(HubError::internal)?;
    let aggregate_type = row
        .get::<_, String>(3)?
        .parse::<AggregateType>()
        .map_err(HubError::internal)?;
    Ok(Some(EventEnvelope {
        seq,
        event_id,
        source_kind: SourceKind::Hub,
        source_id: reader.hub_id.clone().into(),
        kind,
        aggregate_type,
        aggregate_id: row.get(4)?,
        actor_id: row.get(5)?,
        payload: stored.data,
        created_at_ms: row.get(7)?,
    }))
}
