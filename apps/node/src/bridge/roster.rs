use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use cooplet_protocol::{
    BridgeFrame, BridgePayload, CommandOutcome, CommandResPayload, ErrorCode, HelloAckPayload,
    MAX_ROSTER_BYTES, MAX_ROSTER_CATALOG_BYTES, MAX_ROSTER_FENCE_BYTES, MAX_ROSTER_FENCES,
    MAX_ROSTER_FRAMES, MAX_ROSTER_PARTICIPANT_BYTES, MAX_ROSTER_TEMPLATE_BYTES,
    MAX_ROSTER_TEMPLATES, MAX_WS_FRAME_BYTES, ParticipantId, ParticipantKind, ParticipantState,
    ParticipantWire, RosterSyncPayload, TemplateSummary, ThreadFenceReport, validate_actor_handle,
    validate_display_name,
};
use cooplet_storage::ReadPool;
use rusqlite::{Connection, OptionalExtension, Row};
use serde::Serialize;

use crate::{
    clients::{PendingApprovalRecord, list_template_summaries},
    error::{NodeError, Result},
    now_ms,
};

use super::{HubConnection, execution};

pub(super) const MAX_PARTICIPANTS: usize = 4096;

const PARTICIPANT_COLUMNS: &str = "participant_id, kind, name, client_kind, workspace_dir,
    state, capabilities_json, CASE WHEN state = 'awaiting_approval'
    THEN json_extract(adapter_binding_json, '$.core.pending_approval') END";

pub(super) struct Snapshot {
    pub(super) cursor: u64,
    pub(super) participants: HashSet<ParticipantId>,
    pub(super) frames: Vec<String>,
}

pub(super) async fn capture(
    reads: &ReadPool,
    workspace: &Path,
    hub: &HubConnection,
    epoch: u64,
    hello: HelloAckPayload,
) -> Result<Snapshot> {
    let templates = templates(workspace.to_owned()).await?;
    let hub_id = hub.hub_id().clone();
    let node_id = hub.node_id().clone();
    let now = now_ms()?;

    reads
        .read(move |conn| {
            Ok(build_snapshot(conn, epoch, templates, |tx| {
                execution::receipts(tx, &hub_id, &node_id, &hello.pending_op_ids, now)
            }))
        })
        .await?
}

fn build_snapshot(
    conn: &Connection,
    epoch: u64,
    templates: Vec<TemplateSummary>,
    receipts: impl FnOnce(&Connection) -> rusqlite::Result<Vec<CommandResPayload>>,
) -> Result<Snapshot> {
    let tx = conn.unchecked_transaction()?;
    let cursor = head(&tx)?;
    let mut participants = HashSet::new();
    let mut builder = Builder::new(epoch)?;
    {
        let mut statement = tx.prepare(&format!(
            "SELECT {PARTICIPANT_COLUMNS} FROM participants ORDER BY participant_id LIMIT ?1"
        ))?;
        let mut rows = statement.query([MAX_PARTICIPANTS + 1])?;
        while let Some(row) = rows.next()? {
            if participants.len() == MAX_PARTICIPANTS {
                return Err(roster_limit());
            }
            let participant = participant_row(row)?;
            participants.insert(participant.participant_id.clone());
            builder.push(RequiredEntry::Participant(participant))?;
        }
    }
    for template in templates {
        builder.push(RequiredEntry::Template(template))?;
    }
    let mut fences = 0usize;
    execution::visit_fences(&tx, |fence| {
        if fences >= MAX_ROSTER_FENCES {
            return Err(roster_limit());
        }
        fences += 1;
        builder.push(RequiredEntry::Fence(fence))
    })?;
    for receipt in receipts(&tx)? {
        builder.receipt(receipt)?;
    }
    Ok(Snapshot {
        cursor,
        participants,
        frames: builder.finish()?,
    })
}

async fn templates(workspace: PathBuf) -> Result<Vec<TemplateSummary>> {
    tokio::task::spawn_blocking(move || {
        let templates = list_template_summaries(&workspace)?;
        #[derive(Serialize)]
        struct Catalog<'a> {
            templates: &'a [TemplateSummary],
            workflows: [(); 0],
        }
        if templates.len() > MAX_ROSTER_TEMPLATES
            || encoded_size(&Catalog {
                templates: &templates,
                workflows: [],
            })? > MAX_ROSTER_CATALOG_BYTES
        {
            return Err(roster_limit());
        }
        Ok(templates)
    })
    .await
    .map_err(NodeError::internal)?
}

pub(super) fn head(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT events_head_seq FROM node_meta WHERE id = 1",
        [],
        |row| row.get(0),
    )
}

pub(super) fn lookup_participant(
    conn: &Connection,
    participant_id: &ParticipantId,
) -> rusqlite::Result<Option<ParticipantWire>> {
    conn.prepare_cached(&format!(
        "SELECT {PARTICIPANT_COLUMNS} FROM participants WHERE participant_id = ?1"
    ))?
    .query_row([participant_id.as_str()], participant_row)
    .optional()
}

fn participant_row(row: &Row<'_>) -> rusqlite::Result<ParticipantWire> {
    let id: ParticipantId = row.get::<_, String>(0)?.parse().map_err(bad_data)?;
    let kind: ParticipantKind = row.get::<_, String>(1)?.parse().map_err(bad_data)?;
    let name = participant_name(&id, kind, row.get(2)?);
    let state: ParticipantState = row.get::<_, String>(5)?.parse().map_err(bad_data)?;
    let binding = row
        .get::<_, Option<String>>(7)?
        .map(|json| serde_json::from_str::<PendingApprovalRecord>(&json))
        .transpose()
        .map_err(bad_data)?
        .map(|pending| pending.as_wait());
    let mut participant = ParticipantWire {
        participant_id: id,
        kind,
        name,
        client_kind: row.get(3)?,
        workspace_dir: row.get(4)?,
        state,
        capabilities: serde_json::from_str(&row.get::<_, String>(6)?).map_err(bad_data)?,
        wait_kind: None,
        approval_id: None,
        action_hint: None,
        summary: None,
        can_respond_remotely: None,
    };
    if let Some(wait) = binding {
        participant.wait_kind = Some(wait.kind);
        participant.approval_id = wait.approval_id;
        participant.action_hint = wait.action_hint;
        participant.summary = wait.summary;
        participant.can_respond_remotely = Some(wait.can_respond_remotely);
    }
    Ok(participant)
}

pub(super) fn participant_name(id: &ParticipantId, kind: ParticipantKind, name: String) -> String {
    let invalid_agent_name = kind == ParticipantKind::Agent
        && (validate_actor_handle(&name).is_err() || name.eq_ignore_ascii_case("operator"));
    if invalid_agent_name || validate_display_name(&name).is_err() {
        id.to_string()
    } else {
        name
    }
}

enum RequiredEntry {
    Participant(ParticipantWire),
    Template(TemplateSummary),
    Fence(ThreadFenceReport),
}

impl RequiredEntry {
    fn size(&self) -> Result<usize> {
        let (size, limit) = match self {
            Self::Participant(row) => (encoded_size(row)?, MAX_ROSTER_PARTICIPANT_BYTES),
            Self::Template(row) => (encoded_size(row)?, MAX_ROSTER_TEMPLATE_BYTES),
            Self::Fence(row) => (encoded_size(row)?, MAX_ROSTER_FENCE_BYTES),
        };
        if size > limit {
            return Err(roster_limit());
        }
        // Reserve a comma even for the first element of each array.
        Ok(size + 1)
    }

    fn insert(self, chunk: &mut RosterSyncPayload) {
        match self {
            Self::Participant(row) => chunk.participants.push(row),
            Self::Template(row) => chunk.templates.push(row),
            Self::Fence(row) => chunk.unresolved_thread_fences.push(row),
        }
    }
}

struct Builder {
    epoch: u64,
    empty_size: usize,
    total_bytes: usize,
    current_bytes: usize,
    chunk: RosterSyncPayload,
    frames: Vec<String>,
}

impl Builder {
    fn new(epoch: u64) -> Result<Self> {
        let empty_size = BridgeFrame::encode_json(epoch, &empty_chunk(MAX_ROSTER_FRAMES - 2))
            .map_err(|_| roster_limit())?
            .len();
        Ok(Self {
            epoch,
            empty_size,
            total_bytes: 0,
            current_bytes: empty_size,
            chunk: empty_chunk(0),
            frames: Vec::new(),
        })
    }

    fn push(&mut self, row: RequiredEntry) -> Result<()> {
        let bytes = row.size()?;
        if !self.allocate(bytes)? {
            return Err(roster_limit());
        }
        row.insert(&mut self.chunk);
        Ok(())
    }

    fn receipt(&mut self, row: CommandResPayload) -> Result<()> {
        if row.outcome == CommandOutcome::Unknown || row.validate().is_err() {
            return Ok(());
        }
        let bytes = encoded_size(&row)? + 1;
        if self.allocate(bytes)? {
            self.chunk.resolved_op_receipts.push(row);
        }
        Ok(())
    }

    fn allocate(&mut self, bytes: usize) -> Result<bool> {
        if self.empty_size + bytes > MAX_WS_FRAME_BYTES {
            return Ok(false);
        }
        if self.current_bytes + bytes > MAX_WS_FRAME_BYTES {
            if self.chunk.chunk_index + 1 >= MAX_ROSTER_FRAMES
                || self.total_bytes + self.current_bytes + self.empty_size + bytes
                    > MAX_ROSTER_BYTES
            {
                return Ok(false);
            }
            self.seal(false)?;
        }
        if self.total_bytes + self.current_bytes + bytes > MAX_ROSTER_BYTES {
            return Ok(false);
        }
        self.current_bytes += bytes;
        Ok(true)
    }

    fn seal(&mut self, final_chunk: bool) -> Result<()> {
        self.chunk.is_final = final_chunk;
        let frame =
            BridgeFrame::encode_json(self.epoch, &self.chunk).map_err(|_| roster_limit())?;
        self.total_bytes += frame.len();
        self.frames
            .push(String::from_utf8(frame).map_err(NodeError::internal)?);
        self.chunk = empty_chunk(self.chunk.chunk_index + 1);
        self.current_bytes = self.empty_size;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<String>> {
        self.seal(true)?;
        Ok(self.frames)
    }
}

fn empty_chunk(chunk_index: u32) -> RosterSyncPayload {
    RosterSyncPayload {
        chunk_index,
        is_final: false,
        participants: Vec::new(),
        templates: Vec::new(),
        workflows: Vec::new(),
        unresolved_thread_fences: Vec::new(),
        resolved_op_receipts: Vec::new(),
    }
}

fn encoded_size(value: &impl Serialize) -> Result<usize> {
    crate::storage::json_encoded_len(value).map_err(NodeError::internal)
}

fn roster_limit() -> NodeError {
    NodeError::new(
        ErrorCode::PayloadTooLarge,
        "The Node roster exceeds the bridge protocol limits",
    )
}

pub(super) fn bad_data(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_roster_in_order_and_skip_oversized_receipt()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut builder = Builder::new(17)?;
        let ids: Vec<_> = (0..60).map(|index| format!("template-{index}")).collect();
        for id in &ids {
            builder.push(RequiredEntry::Template(TemplateSummary {
                template_id: id.clone(),
                title: "Template".into(),
                description: Some("x".repeat(5000)),
                client_kind: "mock".into(),
            }))?;
        }
        builder.receipt(CommandResPayload {
            op_id: cooplet_protocol::OperationId::generate()?,
            outcome: CommandOutcome::Succeeded,
            resource_id: None,
            result: Some(serde_json::json!({"message": "x".repeat(MAX_WS_FRAME_BYTES)})),
            error: None,
        })?;
        let frames = builder.finish()?;
        assert_eq!(frames.len(), 2);
        let mut collected = Vec::new();
        for (index, payload) in frames.iter().enumerate() {
            assert!(payload.len() <= MAX_WS_FRAME_BYTES);
            let frame = BridgeFrame::from_json(payload.as_bytes())?;
            assert_eq!(frame.epoch, 17);
            let chunk = frame.decode_payload::<RosterSyncPayload>()?;
            assert_eq!(usize::try_from(chunk.chunk_index)?, index);
            assert_eq!(chunk.is_final, index == frames.len() - 1);
            assert!(chunk.resolved_op_receipts.is_empty());
            collected.extend(
                chunk
                    .templates
                    .into_iter()
                    .map(|template| template.template_id),
            );
        }
        assert_eq!(collected, ids);
        Ok(())
    }
}
