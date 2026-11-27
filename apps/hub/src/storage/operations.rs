use cooplet_protocol::{ErrorCode, OperationId, ProblemDetails};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;

use crate::error::HubError;

pub(crate) const OPERATION_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

pub(crate) struct StoredOperation {
    pub(crate) operation_id: String,
    pub(crate) kind: String,
    pub(crate) request_sha256: String,
    pub(crate) target_node_id: Option<String>,
    pub(crate) state: String,
    pub(crate) resource_id: Option<String>,
    pub(crate) result_json: Option<String>,
    pub(crate) error_json: Option<String>,
}

impl StoredOperation {
    pub(crate) fn id(&self) -> Result<OperationId, HubError> {
        OperationId::parse(&self.operation_id).map_err(HubError::internal)
    }

    pub(crate) fn check_match(&self, kind: &str, digest: &str) -> Result<(), HubError> {
        if self.kind != kind || self.request_sha256 != digest {
            return Err(HubError::new(
                ErrorCode::OperationConflict,
                "Idempotency key was used for a different request.",
            ));
        }
        Ok(())
    }

    pub(crate) fn in_progress_error(&self, detail: &'static str) -> Result<HubError, HubError> {
        Ok(HubError::new(ErrorCode::OperationInProgress, detail)
            .with_extension("operation_id", self.id()?.as_str()))
    }

    pub(crate) fn failed_error(&self) -> Result<HubError, HubError> {
        let data = self
            .error_json
            .as_deref()
            .ok_or_else(|| HubError::internal("failed operation has no recorded error"))?;
        Ok(HubError::from_problem(serde_json::from_str::<
            ProblemDetails,
        >(data)?))
    }

    pub(crate) fn unknown_error(
        &self,
        fallback_detail: &'static str,
        include_message_id: bool,
    ) -> Result<HubError, HubError> {
        let id = self.id()?;
        if let Some(data) = self.error_json.as_deref() {
            let mut problem: ProblemDetails = serde_json::from_str(data)?;
            problem
                .extensions
                .entry("operation_id".into())
                .or_insert_with(|| Value::from(id.as_str()));
            if include_message_id && let Some(resource_id) = &self.resource_id {
                problem
                    .extensions
                    .entry("message_id".into())
                    .or_insert_with(|| Value::from(resource_id.as_str()));
            }
            return Ok(HubError::from_problem(problem));
        }
        let mut error = HubError::new(ErrorCode::DeliveryUnknown, fallback_detail)
            .with_extension("operation_id", id.as_str());
        if include_message_id && let Some(resource_id) = &self.resource_id {
            error = error.with_extension("message_id", resource_id.as_str());
        }
        Ok(error)
    }
}

pub(crate) fn load(
    conn: &Connection,
    actor_id: &str,
    key: &str,
) -> rusqlite::Result<Option<StoredOperation>> {
    let mut statement = conn.prepare_cached(
        "SELECT operation_id, kind, request_sha256, target_node_id, state, resource_id, result_json, error_json
         FROM operations WHERE actor_id = ?1 AND operation_key = ?2",
    )?;
    statement
        .query_row(params![actor_id, key], |row| {
            Ok(StoredOperation {
                operation_id: row.get(0)?,
                kind: row.get(1)?,
                request_sha256: row.get(2)?,
                target_node_id: row.get(3)?,
                state: row.get(4)?,
                resource_id: row.get(5)?,
                result_json: row.get(6)?,
                error_json: row.get(7)?,
            })
        })
        .optional()
}

pub(crate) struct NewOperation<'a> {
    pub(crate) operation_id: &'a OperationId,
    pub(crate) actor_id: &'a str,
    pub(crate) key: &'a str,
    pub(crate) kind: &'a str,
    pub(crate) request_sha256: &'a str,
    pub(crate) target_node_id: Option<&'a str>,
    pub(crate) state: &'a str,
    pub(crate) resource_id: Option<&'a str>,
    pub(crate) result_json: Option<&'a str>,
    pub(crate) now_ms: i64,
}

pub(crate) fn insert(tx: &Transaction<'_>, op: NewOperation<'_>) -> rusqlite::Result<()> {
    let mut statement = tx.prepare_cached(
        "INSERT INTO operations (
             operation_id, actor_id, operation_key, kind, request_sha256, target_node_id, state,
             resource_id, result_json, created_at_ms, updated_at_ms, expires_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11)",
    )?;
    statement.execute(params![
        op.operation_id.as_str(),
        op.actor_id,
        op.key,
        op.kind,
        op.request_sha256,
        op.target_node_id,
        op.state,
        op.resource_id,
        op.result_json,
        op.now_ms,
        op.now_ms.saturating_add(OPERATION_TTL_MS),
    ])?;
    Ok(())
}

pub(crate) struct FinishOperation<'a> {
    pub(crate) operation_id: &'a OperationId,
    pub(crate) state: &'a str,
    pub(crate) resource_id: Option<&'a str>,
    pub(crate) result_json: Option<&'a str>,
    pub(crate) error_json: Option<&'a str>,
    pub(crate) now_ms: i64,
}

pub(crate) fn finish(tx: &Transaction<'_>, op: FinishOperation<'_>) -> rusqlite::Result<bool> {
    let mut statement = tx.prepare_cached(
        "UPDATE operations
            SET state = ?2, resource_id = ?3, result_json = ?4, error_json = ?5, updated_at_ms = ?6
          WHERE operation_id = ?1 AND state IN ('accepted', 'in_progress')",
    )?;
    let changed = statement.execute(params![
        op.operation_id.as_str(),
        op.state,
        op.resource_id,
        op.result_json,
        op.error_json,
        op.now_ms,
    ])?;
    Ok(changed == 1)
}
