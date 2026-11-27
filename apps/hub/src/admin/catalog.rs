use std::{io, ops::Bound, str::FromStr};

use cooplet_protocol::{
    ErrorCode, NodeId, ParticipantId, ParticipantState, ServicePrincipalId, ServiceScope, TokenId,
    UserId,
};
use rusqlite::{Row, Rows, params};
use serde::Serialize;

use crate::{
    AppState,
    auth::AuthenticatedCaller,
    bridge::{ApprovalView, RosterCatalog},
    credentials::enrollment_node_id,
    error::HubError,
    storage::sql::{
        conversion_error, json_byte_len, parse_col, parse_json, parse_opt_col, parse_opt_json,
        parse_variant, read_tx,
    },
};

use super::model::{
    ApiTokenKind, CatalogPage, CatalogQuery, EnrollmentIssueSummary, EnrollmentTokenSummary,
    HubParticipantSummary, IdentityKind, NodeCatalogQuery, NodeStatus, NodeSummary,
    PrincipalSummary, TokenSummary,
};

const MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PAGE_LIMIT: usize = 100;
const LIST_PRINCIPALS: &str = "
    SELECT principal_id, kind, username, display_name, participant_id, scopes_json,
           allowed_nodes_json, disabled_at_ms, created_at_ms, updated_at_ms
      FROM principals
     WHERE principal_id > ?1
     ORDER BY principal_id LIMIT ?2";
const PRINCIPAL_TOKENS: &str =
    "SELECT token_id, principal_id, token_kind, is_admin, label, scopes_json,
            allowed_nodes_json, expires_at_ms, revoked_at_ms, created_at_ms
     FROM auth_tokens
     WHERE principal_id = ?1 AND token_id > ?2 AND token_kind != 'enrollment_token'
     ORDER BY token_id LIMIT ?3";
const ENROLLMENT_TOKENS: &str =
    "SELECT token_id, principal_id, label, expires_at_ms, consumed_at_ms,
            revoked_at_ms, created_at_ms
     FROM auth_tokens
     WHERE token_kind = 'enrollment_token' AND token_id > ?1
     ORDER BY token_id LIMIT ?2";
const LIST_NODES_UNSCOPED: &str = "
    SELECT node_id, display_name, status, generation, session_epoch,
           capabilities_json, last_seen_at_ms, enrolled_at_ms, revoked_at_ms, roster_catalog_json
      FROM nodes WHERE node_id > ?1
     ORDER BY node_id LIMIT ?2";
const LIST_NODES_SCOPED: &str = "
    SELECT node_id, display_name, status, generation, session_epoch,
           capabilities_json, last_seen_at_ms, enrolled_at_ms, revoked_at_ms, roster_catalog_json
      FROM nodes WHERE node_id > ?1 AND node_id IN (SELECT value FROM json_each(?2))
     ORDER BY node_id LIMIT ?3";
const LIST_PARTICIPANTS_UNSCOPED: &str = "
    SELECT p.node_id, p.participant_id, p.kind, p.name, p.client_kind, p.workspace_dir,
           p.state, p.capabilities_json, p.synced_epoch, p.updated_at_ms, p.approval_json
      FROM node_participants p JOIN nodes n USING (node_id)
     WHERE (p.node_id, p.participant_id) > (?1, ?2)
       AND n.revoked_at_ms IS NULL AND n.status != 'revoked'
     ORDER BY p.node_id, p.participant_id LIMIT ?3";
const LIST_PARTICIPANTS_SCOPED: &str = "
    SELECT p.node_id, p.participant_id, p.kind, p.name, p.client_kind, p.workspace_dir,
           p.state, p.capabilities_json, p.synced_epoch, p.updated_at_ms, p.approval_json
      FROM nodes n CROSS JOIN node_participants p ON p.node_id = n.node_id
     WHERE n.node_id IN (SELECT value FROM json_each(?3)) AND n.node_id >= ?1
       AND p.participant_id > CASE WHEN n.node_id = ?1 THEN ?2 ELSE '' END
       AND n.revoked_at_ms IS NULL AND n.status != 'revoked'
     ORDER BY n.node_id, p.participant_id LIMIT ?4";

pub(crate) async fn list_principals(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: CatalogQuery,
) -> Result<CatalogPage<PrincipalSummary>, HubError> {
    caller.require_admin()?;
    let limit = page_limit(query.limit)?;
    if let Some(cursor) = &query.after {
        valid_principal_id(cursor)?;
    }

    read_tx(&state.reads, move |tx| {
        let mut statement = tx.prepare_cached(LIST_PRINCIPALS)?;
        let mut rows = statement.query(params![query.after.unwrap_or_default(), limit + 1])?;
        Ok(collect_page(&mut rows, limit, |row| {
            let entry = principal_summary(row)?;
            let cursor = entry.principal_id.clone();
            Ok((entry, cursor))
        })?)
    })
    .await
}

pub(crate) async fn list_tokens(
    state: &AppState,
    caller: &AuthenticatedCaller,
    principal_id: &str,
    query: CatalogQuery,
) -> Result<CatalogPage<TokenSummary>, HubError> {
    caller.require_admin()?;
    valid_principal_id(principal_id)?;
    let principal_id = principal_id.to_owned();
    let limit = page_limit(query.limit)?;
    parse_optional_cursor::<TokenId>(query.after.as_deref())?;

    read_tx(&state.reads, move |tx| {
        let mut exists =
            tx.prepare_cached("SELECT EXISTS(SELECT 1 FROM principals WHERE principal_id = ?1)")?;
        if !exists.query_row([&principal_id], |row| row.get::<_, bool>(0))? {
            return Err(HubError::new(ErrorCode::NotFound, "Principal not found"));
        }
        let mut statement = tx.prepare_cached(PRINCIPAL_TOKENS)?;
        let mut rows = statement.query(params![
            principal_id,
            query.after.unwrap_or_default(),
            limit + 1
        ])?;
        Ok(collect_page(&mut rows, limit, |row| {
            let entry = token_summary(row)?;
            let cursor = entry.token_id.to_string();
            Ok((entry, cursor))
        })?)
    })
    .await
}

pub(crate) async fn list_enrollment(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: CatalogQuery,
) -> Result<CatalogPage<EnrollmentTokenSummary>, HubError> {
    caller.require_admin()?;
    let limit = page_limit(query.limit)?;
    parse_optional_cursor::<TokenId>(query.after.as_deref())?;

    read_tx(&state.reads, move |tx| {
        let mut statement = tx.prepare_cached(ENROLLMENT_TOKENS)?;
        let mut rows = statement.query(params![query.after.unwrap_or_default(), limit + 1])?;
        Ok(collect_page(&mut rows, limit, |row| {
            let token_id: TokenId = parse_col(row, 0)?;
            let cursor = token_id.to_string();
            let node_id = enrollment_node_id(&token_id).map_err(|_| {
                conversion_error(0, io::Error::other("invalid enrollment token ID"))
            })?;
            let entry = EnrollmentTokenSummary {
                summary: EnrollmentIssueSummary {
                    token_id,
                    node_id,
                    label: row.get(2)?,
                    expires_at_ms: row.get(3)?,
                    created_at_ms: row.get(6)?,
                },
                principal_id: row.get(1)?,
                consumed_at_ms: row.get(4)?,
                revoked_at_ms: row.get(5)?,
            };
            Ok((entry, cursor))
        })?)
    })
    .await
}

pub(crate) async fn list_nodes(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: NodeCatalogQuery,
) -> Result<CatalogPage<NodeSummary>, HubError> {
    let limit = page_limit(query.limit)?;
    let after = query
        .after
        .as_deref()
        .map(NodeId::parse)
        .transpose()
        .map_err(|_| HubError::invalid("Invalid catalog cursor"))?;
    let allowed = match catalog_allowed_nodes(
        caller,
        query.node_id.as_ref(),
        after.as_ref().map(Bound::Excluded),
    )? {
        NodeScope::Empty => {
            return Ok(CatalogPage {
                items: Vec::new(),
                next_after: None,
            });
        }
        NodeScope::All => None,
        NodeScope::Subset(json) => Some(json),
    };

    let after_cursor = after.map_or_else(String::new, |id| id.to_string());
    read_tx(&state.reads, move |tx| {
        let mut statement = match &allowed {
            None => tx.prepare_cached(LIST_NODES_UNSCOPED)?,
            Some(_) => tx.prepare_cached(LIST_NODES_SCOPED)?,
        };
        let mut rows = match &allowed {
            None => statement.query(params![after_cursor, limit + 1])?,
            Some(json) => statement.query(params![after_cursor, json, limit + 1])?,
        };
        Ok(collect_page(&mut rows, limit, |row| {
            let node_id: NodeId = parse_col(row, 0)?;
            let cursor = node_id.to_string();
            let catalog: RosterCatalog = parse_opt_json(row, 9)?.unwrap_or_default();
            let entry = NodeSummary {
                node_id,
                display_name: row.get(1)?,
                status: parse_variant(row, 2, NodeStatus::parse)?,
                generation: row.get(3)?,
                session_epoch: row.get(4)?,
                capabilities: parse_json(row, 5)?,
                templates: catalog.templates,
                workflows: catalog.workflows,
                last_seen_at_ms: row.get(6)?,
                enrolled_at_ms: row.get(7)?,
                revoked_at_ms: row.get(8)?,
            };
            Ok((entry, cursor))
        })?)
    })
    .await
}

pub(crate) async fn list_participants(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: NodeCatalogQuery,
) -> Result<CatalogPage<HubParticipantSummary>, HubError> {
    let limit = page_limit(query.limit)?;
    let (after_node, after_participant) = if let Some(after) = query.after {
        let (node_id, participant_id) = after
            .split_once('.')
            .ok_or_else(|| HubError::invalid("Invalid participant catalog cursor"))?;
        let node_id =
            NodeId::parse(node_id).map_err(|_| HubError::invalid("Invalid node ID in cursor"))?;
        ParticipantId::parse(participant_id)
            .map_err(|_| HubError::invalid("Invalid participant ID in cursor"))?;
        (Some(node_id), participant_id.to_owned())
    } else {
        (None, String::new())
    };
    let allowed = match catalog_allowed_nodes(
        caller,
        query.node_id.as_ref(),
        after_node.as_ref().map(Bound::Included),
    )? {
        NodeScope::Empty => {
            return Ok(CatalogPage {
                items: Vec::new(),
                next_after: None,
            });
        }
        NodeScope::All => None,
        NodeScope::Subset(json) => Some(json),
    };

    let after_node = after_node.map_or_else(String::new, |id| id.to_string());
    let can_manage = caller.require_scope(ServiceScope::Manage).is_ok();
    read_tx(&state.reads, move |tx| {
        let mut statement = match &allowed {
            None => tx.prepare_cached(LIST_PARTICIPANTS_UNSCOPED)?,
            Some(_) => tx.prepare_cached(LIST_PARTICIPANTS_SCOPED)?,
        };
        let mut rows = match &allowed {
            None => statement.query(params![after_node, after_participant, limit + 1])?,
            Some(json) => {
                statement.query(params![after_node, after_participant, json, limit + 1])?
            }
        };
        Ok(collect_page(&mut rows, limit, |row| {
            let state: ParticipantState = parse_col(row, 6)?;
            let node_id: NodeId = parse_col(row, 0)?;
            let participant_id: ParticipantId = parse_col(row, 1)?;
            let cursor = format!("{node_id}.{participant_id}");
            let mut approval: ApprovalView = parse_opt_json(row, 10)?.unwrap_or_default();
            // The catalog query has already intersected the caller's allowed nodes.
            if state == ParticipantState::AwaitingApproval && !can_manage {
                approval.redact();
            }
            let entry = HubParticipantSummary {
                node_id,
                participant_id,
                kind: parse_col(row, 2)?,
                name: row.get(3)?,
                client_kind: row.get(4)?,
                workspace_dir: row.get(5)?,
                state,
                intake_state: state.intake_state(),
                capabilities: parse_json(row, 7)?,
                wait_kind: approval.wait_kind,
                approval_id: approval.approval_id,
                action_hint: approval.action_hint,
                summary: approval.summary,
                can_respond_remotely: approval.can_respond_remotely,
                synced_epoch: row.get(8)?,
                updated_at_ms: row.get(9)?,
            };
            Ok((entry, cursor))
        })?)
    })
    .await
}

enum NodeScope {
    All,
    Empty,
    Subset(String),
}

fn catalog_allowed_nodes(
    caller: &AuthenticatedCaller,
    node_id: Option<&NodeId>,
    lower_bound: Option<Bound<&NodeId>>,
) -> Result<NodeScope, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    let matches_bound = |candidate: &NodeId| match lower_bound {
        None => true,
        Some(Bound::Excluded(bound)) => candidate > bound,
        Some(Bound::Included(bound)) => candidate >= bound,
        Some(Bound::Unbounded) => true,
    };
    if let Some(id) = node_id {
        if !caller.allows_node(id) {
            return Err(HubError::new(
                ErrorCode::Forbidden,
                "Node is outside the allowed scope",
            ));
        }
        return if matches_bound(id) {
            Ok(NodeScope::Subset(serde_json::to_string(&[id])?))
        } else {
            Ok(NodeScope::Empty)
        };
    }
    let Some(allowed) = caller.allowed_nodes() else {
        return Ok(NodeScope::All);
    };
    let lower = lower_bound.unwrap_or(Bound::Unbounded);
    let eligible: Vec<&NodeId> = allowed
        .range::<NodeId, _>((lower, Bound::Unbounded))
        .collect();
    if eligible.is_empty() {
        return Ok(NodeScope::Empty);
    }
    Ok(NodeScope::Subset(serde_json::to_string(&eligible)?))
}

pub(super) fn valid_principal_id(value: &str) -> Result<(), HubError> {
    if UserId::parse(value).is_ok() || ServicePrincipalId::parse(value).is_ok() {
        Ok(())
    } else {
        Err(HubError::invalid("Invalid principal ID"))
    }
}

fn parse_optional_cursor<T: FromStr>(cursor: Option<&str>) -> Result<(), HubError> {
    if cursor.is_some_and(|value| value.parse::<T>().is_err()) {
        return Err(HubError::invalid("Invalid catalog cursor"));
    }
    Ok(())
}

fn page_limit(limit: Option<usize>) -> Result<usize, HubError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if !(1..=500).contains(&limit) {
        return Err(HubError::invalid("Catalog limit must be between 1 and 500"));
    }
    Ok(limit)
}

fn collect_page<T: Serialize>(
    rows: &mut Rows<'_>,
    limit: usize,
    mut decode: impl FnMut(&Row<'_>) -> rusqlite::Result<(T, String)>,
) -> rusqlite::Result<CatalogPage<T>> {
    let mut items = Vec::new();
    let mut previous_cursor = None;
    let mut page_bytes = br#"{"items":[]}"#.len();
    let mut has_more = false;
    while let Some(row) = rows.next()? {
        if items.len() == limit {
            has_more = true;
            break;
        }
        let (item, cursor) = decode(row)?;
        let item_bytes = json_byte_len(&item).map_err(|error| conversion_error(0, error))?;
        let candidate_bytes = page_bytes + item_bytes + usize::from(!items.is_empty());

        let cursor_json_len = json_byte_len(&cursor).map_err(|error| conversion_error(0, error))?;
        let cursor_bytes = b",\"next_after\":".len() + cursor_json_len;
        if candidate_bytes + cursor_bytes > MAX_CATALOG_BYTES {
            if candidate_bytes <= MAX_CATALOG_BYTES && rows.next()?.is_none() {
                items.push(item);
                break;
            }
            if items.is_empty() {
                return Err(conversion_error(
                    0,
                    io::Error::other("catalog entry exceeds page size"),
                ));
            }
            has_more = true;
            break;
        }

        page_bytes = candidate_bytes;
        previous_cursor = Some(cursor);
        items.push(item);
    }

    Ok(CatalogPage {
        items,
        next_after: previous_cursor.filter(|_| has_more),
    })
}

pub(super) fn principal_summary(row: &Row<'_>) -> rusqlite::Result<PrincipalSummary> {
    Ok(PrincipalSummary {
        principal_id: row.get(0)?,
        kind: parse_variant(row, 1, IdentityKind::parse)?,
        username: row.get(2)?,
        display_name: row.get(3)?,
        participant_id: parse_opt_col(row, 4)?,
        scopes: parse_json(row, 5)?,
        allowed_nodes: parse_opt_json(row, 6)?,
        disabled_at_ms: row.get(7)?,
        created_at_ms: row.get(8)?,
        updated_at_ms: row.get(9)?,
    })
}

fn token_summary(row: &Row<'_>) -> rusqlite::Result<TokenSummary> {
    let expiry: i64 = row.get(7)?;
    let durable = expiry == i64::MAX;
    Ok(TokenSummary {
        token_id: parse_col(row, 0)?,
        principal_id: row.get(1)?,
        token_kind: parse_variant(row, 2, ApiTokenKind::parse)?,
        is_admin: row.get(3)?,
        label: row.get(4)?,
        scopes: parse_json(row, 5)?,
        allowed_nodes: parse_opt_json(row, 6)?,
        durable,
        expires_at_ms: (!durable).then_some(expiry),
        revoked_at_ms: row.get(8)?,
        created_at_ms: row.get(9)?,
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{
        ENROLLMENT_TOKENS, LIST_NODES_SCOPED, LIST_NODES_UNSCOPED, LIST_PARTICIPANTS_SCOPED,
        LIST_PARTICIPANTS_UNSCOPED, MAX_CATALOG_BYTES, PRINCIPAL_TOKENS, collect_page,
    };
    use rusqlite::{Connection, params};
    use serde::Serialize;

    #[derive(Serialize)]
    struct RowValue {
        id: String,
        text: String,
    }

    fn explain(
        connection: &Connection,
        query: &str,
        bindings: &[&dyn rusqlite::ToSql],
    ) -> rusqlite::Result<String> {
        let mut statement = connection.prepare(&format!("EXPLAIN QUERY PLAN {query}"))?;
        let plan = statement
            .query_map(bindings, |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .join("\n");
        Ok(plan)
    }

    #[test]
    fn catalogs_seek_by_cursor_without_temp_sort() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(include_str!("../storage/v1.sql"))?;
        let node_list = r#"["node_00000000000000000000000001"]"#;
        for (query, bindings, expected) in [
            (
                PRINCIPAL_TOKENS,
                params!["principal", "cursor", 6],
                "idx_auth_tokens_principal_page (principal_id=? AND token_id>?)",
            ),
            (
                ENROLLMENT_TOKENS,
                params!["cursor", 6],
                "idx_auth_tokens_enrollment_page (token_id>?)",
            ),
            (
                LIST_NODES_UNSCOPED,
                params!["", 6],
                "SEARCH nodes USING INDEX sqlite_autoindex_nodes_1 (node_id>?)",
            ),
            (
                LIST_NODES_SCOPED,
                params!["", node_list, 6],
                "SEARCH nodes USING INDEX sqlite_autoindex_nodes_1 (node_id=?)",
            ),
            (
                LIST_PARTICIPANTS_UNSCOPED,
                params!["", "", 6],
                "SEARCH p USING INDEX sqlite_autoindex_node_participants_1",
            ),
            (
                LIST_PARTICIPANTS_SCOPED,
                params!["", "", node_list, 6],
                "SEARCH p USING INDEX sqlite_autoindex_node_participants_1 (node_id=? AND participant_id>?)",
            ),
        ] {
            let plan = explain(&connection, query, bindings)?;
            assert!(plan.contains(expected), "{plan}");
            assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
        }
        Ok(())
    }

    #[test]
    fn catalog_limits_serialized_bytes_without_skipping_the_next_item() -> Result<(), Box<dyn Error>>
    {
        let connection = Connection::open_in_memory()?;
        connection.execute("CREATE TABLE items (id TEXT PRIMARY KEY, text TEXT)", [])?;
        let text = "\"".repeat(MAX_CATALOG_BYTES / 3);
        for id in ["first", "second", "third"] {
            connection.execute("INSERT INTO items VALUES (?1, ?2)", params![id, text])?;
        }

        let mut statement =
            connection.prepare("SELECT id, text FROM items WHERE id > ?1 ORDER BY id")?;
        let decode = |row: &rusqlite::Row<'_>| {
            let id: String = row.get(0)?;
            Ok((
                RowValue {
                    id: id.clone(),
                    text: row.get(1)?,
                },
                id,
            ))
        };
        let page = collect_page(&mut statement.query([""])?, 3, decode)?;
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.next_after.as_deref(), Some("first"));
        assert!(serde_json::to_vec(&page)?.len() <= MAX_CATALOG_BYTES);

        let page = collect_page(&mut statement.query(["first"])?, 3, decode)?;
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.next_after.as_deref(), Some("second"));
        let page = collect_page(&mut statement.query(["second"])?, 3, decode)?;
        assert_eq!(page.items.len(), 1);
        assert!(page.next_after.is_none());
        Ok(())
    }
}
