use std::{error::Error, io, str::FromStr};

use cooplet_protocol::{
    ErrorCode, NodeId, ParticipantId, ParticipantState, ServicePrincipalId, ServiceScope, TokenId,
    UserId,
};
use rusqlite::{Connection, Row, Rows, Transaction, params, types::Type};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    AppState, auth::AuthenticatedCaller, credentials::enrollment_node_id, error::HubError,
};

use super::model::{
    ApiTokenKind, CatalogPage, CatalogQuery, EnrollmentIssueSummary, EnrollmentTokenSummary,
    HubParticipantSummary, IdentityKind, NodeCatalogQuery, NodeStatus, NodeSummary,
    PrincipalSummary, TokenSummary,
};

const MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PAGE_LIMIT: usize = 100;

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

    read_snapshot(state, move |tx| {
        let mut statement = tx.prepare(
            "SELECT principal_id, kind, username, display_name, participant_id, scopes_json,
                    allowed_nodes_json, disabled_at_ms, created_at_ms, updated_at_ms
               FROM principals
              WHERE principal_id > ?1
              ORDER BY principal_id LIMIT ?2",
        )?;
        let mut rows = statement.query(params![query.after.unwrap_or_default(), limit + 1])?;
        collect_page(&mut rows, limit, |row| {
            let entry = principal_summary(row)?;
            let cursor = entry.principal_id.clone();
            Ok((entry, cursor))
        })
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

    read_snapshot(state, move |tx| {
        let exists = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM principals WHERE principal_id = ?1)",
            [&principal_id],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Ok(None);
        }
        let mut statement = tx.prepare(
            "SELECT token_id, principal_id, token_kind, is_admin, label, scopes_json,
                    allowed_nodes_json, expires_at_ms, revoked_at_ms, created_at_ms
               FROM auth_tokens
              WHERE principal_id = ?1 AND token_id > ?2 AND token_kind != 'enrollment_token'
              ORDER BY token_id LIMIT ?3",
        )?;
        let mut rows = statement.query(params![
            principal_id,
            query.after.unwrap_or_default(),
            limit + 1
        ])?;
        collect_page(&mut rows, limit, |row| {
            let entry = token_summary(row)?;
            let cursor = entry.token_id.to_string();
            Ok((entry, cursor))
        })
        .map(Some)
    })
    .await?
    .ok_or_else(|| HubError::new(ErrorCode::NotFound, "Principal not found"))
}

pub(crate) async fn list_enrollment(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: CatalogQuery,
) -> Result<CatalogPage<EnrollmentTokenSummary>, HubError> {
    caller.require_admin()?;
    let limit = page_limit(query.limit)?;
    parse_optional_cursor::<TokenId>(query.after.as_deref())?;

    read_snapshot(state, move |tx| {
        let mut statement = tx.prepare(
            "SELECT token_id, principal_id, label, expires_at_ms, consumed_at_ms,
                    revoked_at_ms, created_at_ms
               FROM auth_tokens
              WHERE token_kind = 'enrollment_token' AND token_id > ?1
              ORDER BY token_id LIMIT ?2",
        )?;
        let mut rows = statement.query(params![query.after.unwrap_or_default(), limit + 1])?;
        collect_page(&mut rows, limit, |row| {
            let token_id: TokenId = parse_text(row, 0)?;
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
        })
    })
    .await
}

pub(crate) async fn list_nodes(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: NodeCatalogQuery,
) -> Result<CatalogPage<NodeSummary>, HubError> {
    let allowed = catalog_allowed_nodes(caller, query.node_id.as_ref())?;
    let limit = page_limit(query.limit)?;
    parse_optional_cursor::<NodeId>(query.after.as_deref())?;

    read_snapshot(state, move |tx| {
        let mut statement = tx.prepare(
            "SELECT node_id, display_name, status, generation, session_epoch,
                    capabilities_json, last_seen_at_ms, enrolled_at_ms, revoked_at_ms
               FROM nodes
              WHERE node_id > ?1
                AND (?2 IS NULL OR node_id = ?2)
                AND (?3 IS NULL OR node_id IN (SELECT value FROM json_each(?3)))
              ORDER BY node_id LIMIT ?4",
        )?;
        let mut rows = statement.query(params![
            query.after.unwrap_or_default(),
            query.node_id.as_ref().map(NodeId::as_str),
            allowed,
            limit + 1,
        ])?;
        collect_page(&mut rows, limit, |row| {
            let node_id: NodeId = parse_text(row, 0)?;
            let cursor = node_id.to_string();
            let entry = NodeSummary {
                node_id,
                display_name: row.get(1)?,
                status: parse_variant(row, 2, NodeStatus::parse)?,
                generation: row.get(3)?,
                session_epoch: row.get(4)?,
                capabilities: parse_json(row, 5)?,
                last_seen_at_ms: row.get(6)?,
                enrolled_at_ms: row.get(7)?,
                revoked_at_ms: row.get(8)?,
            };
            Ok((entry, cursor))
        })
    })
    .await
}

pub(crate) async fn list_participants(
    state: &AppState,
    caller: &AuthenticatedCaller,
    query: NodeCatalogQuery,
) -> Result<CatalogPage<HubParticipantSummary>, HubError> {
    let allowed = catalog_allowed_nodes(caller, query.node_id.as_ref())?;
    let limit = page_limit(query.limit)?;
    let (after_node, after_participant) = if let Some(after) = query.after {
        let (node_id, participant_id) = after
            .split_once('.')
            .ok_or_else(|| HubError::invalid("Invalid participant catalog cursor"))?;
        NodeId::parse(node_id).map_err(|_| HubError::invalid("Invalid node ID in cursor"))?;
        ParticipantId::parse(participant_id)
            .map_err(|_| HubError::invalid("Invalid participant ID in cursor"))?;
        (node_id.to_owned(), participant_id.to_owned())
    } else {
        (String::new(), String::new())
    };

    read_snapshot(state, move |tx| {
        let mut statement = tx.prepare(
            "SELECT p.node_id, p.participant_id, p.kind, p.name, p.client_kind, p.workspace_dir,
                    p.state, p.capabilities_json, p.synced_epoch, p.updated_at_ms
               FROM node_participants p JOIN nodes n USING (node_id)
              WHERE (p.node_id, p.participant_id) > (?1, ?2)
                AND (?3 IS NULL OR p.node_id = ?3)
                AND (?4 IS NULL OR p.node_id IN (SELECT value FROM json_each(?4)))
                AND n.revoked_at_ms IS NULL AND n.status != 'revoked'
              ORDER BY p.node_id, p.participant_id LIMIT ?5",
        )?;
        let mut rows = statement.query(params![
            after_node,
            after_participant,
            query.node_id.as_ref().map(NodeId::as_str),
            allowed,
            limit + 1,
        ])?;
        collect_page(&mut rows, limit, |row| {
            let state: ParticipantState = parse_text(row, 6)?;
            let node_id: NodeId = parse_text(row, 0)?;
            let participant_id: ParticipantId = parse_text(row, 1)?;
            let cursor = format!("{node_id}.{participant_id}");
            let entry = HubParticipantSummary {
                node_id,
                participant_id,
                kind: parse_text(row, 2)?,
                name: row.get(3)?,
                client_kind: row.get(4)?,
                workspace_dir: row.get(5)?,
                state,
                intake_state: state.intake_state(),
                capabilities: parse_json(row, 7)?,
                synced_epoch: row.get(8)?,
                updated_at_ms: row.get(9)?,
            };
            Ok((entry, cursor))
        })
    })
    .await
}

fn catalog_allowed_nodes(
    caller: &AuthenticatedCaller,
    node_id: Option<&NodeId>,
) -> Result<Option<String>, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    if node_id.is_some_and(|id| !caller.allows_node(id)) {
        return Err(HubError::new(
            ErrorCode::Forbidden,
            "Node is outside the allowed scope",
        ));
    }
    caller
        .allowed_nodes()
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
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

async fn read_snapshot<T, F>(state: &AppState, read: F) -> Result<T, HubError>
where
    T: Send + 'static,
    F: FnOnce(&Transaction<'_>) -> rusqlite::Result<T> + Send + 'static,
{
    state
        .reads
        .read(move |connection: &Connection| {
            let tx = connection.unchecked_transaction()?;
            let value = read(&tx)?;
            tx.commit()?;
            Ok(value)
        })
        .await
        .map_err(Into::into)
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
        let item_bytes = serde_json::to_vec(&item).map_err(|error| conversion_error(0, error))?;
        let candidate_bytes = page_bytes + item_bytes.len() + usize::from(!items.is_empty());

        let cursor_json =
            serde_json::to_vec(&cursor).map_err(|error| conversion_error(0, error))?;
        let cursor_bytes = b",\"next_after\":".len() + cursor_json.len();
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
        participant_id: row
            .get::<_, Option<String>>(4)?
            .map(|id| ParticipantId::parse(&id).map_err(|error| conversion_error(4, error)))
            .transpose()?,
        scopes: parse_json(row, 5)?,
        allowed_nodes: parse_optional_json(row, 6)?,
        disabled_at_ms: row.get(7)?,
        created_at_ms: row.get(8)?,
        updated_at_ms: row.get(9)?,
    })
}

fn token_summary(row: &Row<'_>) -> rusqlite::Result<TokenSummary> {
    let expiry: i64 = row.get(7)?;
    let durable = expiry == i64::MAX;
    Ok(TokenSummary {
        token_id: parse_text(row, 0)?,
        principal_id: row.get(1)?,
        token_kind: parse_variant(row, 2, ApiTokenKind::parse)?,
        is_admin: row.get(3)?,
        label: row.get(4)?,
        scopes: parse_json(row, 5)?,
        allowed_nodes: parse_optional_json(row, 6)?,
        durable,
        expires_at_ms: (!durable).then_some(expiry),
        revoked_at_ms: row.get(8)?,
        created_at_ms: row.get(9)?,
    })
}

fn parse_variant<T>(
    row: &Row<'_>,
    index: usize,
    parse: impl FnOnce(&str) -> Option<T>,
) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    parse(&text).ok_or_else(|| conversion_error(index, io::Error::other("invalid catalog enum")))
}

fn parse_text<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    let text: String = row.get(index)?;
    text.parse().map_err(|error| conversion_error(index, error))
}

fn parse_json<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    serde_json::from_str(&text).map_err(|error| conversion_error(index, error))
}

fn parse_optional_json<T: DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    row.get::<_, Option<String>>(index)?
        .map(|text| serde_json::from_str(&text).map_err(|error| conversion_error(index, error)))
        .transpose()
}

fn conversion_error(index: usize, error: impl Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{MAX_CATALOG_BYTES, collect_page};
    use rusqlite::{Connection, params};
    use serde::Serialize;

    #[derive(Serialize)]
    struct RowValue {
        id: String,
        text: String,
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
