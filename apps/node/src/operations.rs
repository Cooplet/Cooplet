use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use cooplet_protocol::{
    ErrorCode, OperationId, ProblemDetails, canonical_sha256, validate_idempotency_key,
};
use cooplet_storage::{DbActor, ReadPool, StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    error::{NodeError, Result},
    now_ms,
};

const RECEIPT_TTL_MS: i64 = 14 * 24 * 60 * 60 * 1000;
pub const MAX_PROBLEM_BYTES: usize = 65_536;
const REQUEST_ID_FIELD_BYTES: usize = 46;
type Key = (String, String);
type ActiveOperations = HashMap<Key, ActiveOperation>;

struct ActiveOperation {
    kind: &'static str,
    hash: Option<String>,
    committed_id: Option<OperationId>,
}

pub(crate) enum ReceiptLookup {
    Missing,
    Pending {
        id: String,
        resource_id: Option<String>,
    },
    Failed(NodeError),
    Unknown {
        id: String,
        problem: Option<NodeError>,
        resource_id: Option<String>,
    },
    Succeeded(Value),
}

impl ReceiptLookup {
    fn into_result(self, attempt: &OperationId) -> Result<Option<Value>> {
        match self {
            Self::Missing => Ok(None),
            Self::Pending { id, .. } if id == attempt.as_str() => {
                Err(in_progress().with_extension("operation_id", id))
            }
            Self::Pending { id, .. } => Err(unconfirmed(id)),
            Self::Failed(problem) => Err(problem),
            Self::Unknown { id, problem, .. } => Err(problem.unwrap_or_else(|| unconfirmed(id))),
            Self::Succeeded(value) => Ok(Some(value)),
        }
    }
}

#[derive(Clone)]
pub struct OperationBook {
    db: DbActor,
    reads: ReadPool,
    active: Arc<Mutex<ActiveOperations>>,
}

#[derive(Clone)]
pub struct Operation {
    pub id: OperationId,
    pub actor_id: String,
    pub key: String,
    pub kind: &'static str,
    pub hash: String,
}

pub struct OperationGuard {
    pub operation: Operation,
    active: Arc<Mutex<ActiveOperations>>,
}

fn lock_active(active: &Mutex<ActiveOperations>) -> MutexGuard<'_, ActiveOperations> {
    active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        lock_active(&self.active)
            .remove(&(self.operation.actor_id.clone(), self.operation.key.clone()));
    }
}

impl OperationGuard {
    pub(crate) fn mark_committed(&self) {
        if let Some(active) = lock_active(&self.active)
            .get_mut(&(self.operation.actor_id.clone(), self.operation.key.clone()))
        {
            active.committed_id = Some(self.operation.id.clone());
        }
    }
}

impl OperationBook {
    pub fn new(db: DbActor, reads: ReadPool) -> Self {
        Self {
            db,
            reads,
            active: Arc::default(),
        }
    }

    pub fn guard(
        &self,
        actor_id: &str,
        key: &str,
        kind: &'static str,
        request: &(impl Serialize + ?Sized),
    ) -> Result<OperationGuard> {
        let hash = canonical_sha256(request).map_err(NodeError::internal)?;
        self.guard_with_hash(actor_id, key, kind, Some(hash))
    }

    /// Guard an operation whose request hash is only known after streaming
    /// completes (for example blob uploads). Serializes concurrent attempts on
    /// `(actor_id, key)` and immediately rejects cross-kind conflicts.
    pub(crate) fn guard_unhashed(
        &self,
        actor_id: &str,
        key: &str,
        kind: &'static str,
    ) -> Result<OperationGuard> {
        self.guard_with_hash(actor_id, key, kind, None)
    }

    fn guard_with_hash(
        &self,
        actor_id: &str,
        key: &str,
        kind: &'static str,
        hash: Option<String>,
    ) -> Result<OperationGuard> {
        validate_idempotency_key(key)
            .map_err(|_| NodeError::new(ErrorCode::InvalidRequest, "Invalid Idempotency-Key"))?;
        let operation = Operation {
            id: OperationId::generate()?,
            actor_id: actor_id.into(),
            key: key.into(),
            kind,
            hash: hash.clone().unwrap_or_default(),
        };
        match lock_active(&self.active).entry((actor_id.into(), key.into())) {
            Entry::Vacant(entry) => {
                entry.insert(ActiveOperation {
                    kind,
                    hash,
                    committed_id: None,
                });
            }
            Entry::Occupied(entry) => {
                let active = entry.get();
                let hash_conflict = matches!(
                    (&active.hash, &hash),
                    (Some(existing), Some(incoming)) if existing != incoming
                );
                return Err(if active.kind != kind || hash_conflict {
                    conflict()
                } else {
                    let problem = in_progress();
                    match &active.committed_id {
                        Some(id) => problem.with_extension("operation_id", id.as_str()),
                        None => problem,
                    }
                });
            }
        }
        Ok(OperationGuard {
            operation,
            active: self.active.clone(),
        })
    }

    pub async fn lookup(&self, operation: &Operation) -> Result<Option<Value>> {
        self.lookup_on_abandon(operation, |_, _, _| Ok(())).await
    }

    pub(crate) async fn lookup_on_abandon<F>(
        &self,
        operation: &Operation,
        on_abandon: F,
    ) -> Result<Option<Value>>
    where
        F: Fn(&Transaction<'_>, &str, i64) -> rusqlite::Result<()> + Send + Sync + 'static,
    {
        let receipt = self.lookup_state(operation).await?;
        let ReceiptLookup::Pending { id, .. } = &receipt else {
            return receipt.into_result(&operation.id);
        };
        if id == operation.id.as_str() {
            return receipt.into_result(&operation.id);
        }

        // The caller holds the operation key until its detached work finishes.
        // A pending receipt belonging to a different attempt has lost its owner.
        let id = id.clone();
        let operation = operation.clone();
        let on_abandon = Arc::new(on_abandon);
        let resolved = self
            .finalize(move |tx| {
                let current = operation.lookup_state(tx);
                if let Ok(ReceiptLookup::Pending { id, .. }) = &current
                    && id != operation.id.as_str()
                {
                    let now = now_ms().map_err(NodeError::from)?;
                    abandon_pending(tx, id, now)?;
                    on_abandon(tx, id, now)?;
                }
                Ok(WriteOutcome::new(
                    current.and_then(|receipt| receipt.into_result(&operation.id)),
                ))
            })
            .await;
        match resolved {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(operation_id = id, %error, "Could not finalize an unconfirmed operation");
                Err(unconfirmed(id))
            }
        }
    }

    pub(crate) async fn lookup_state(&self, operation: &Operation) -> Result<ReceiptLookup> {
        let operation = operation.clone();
        self.reads
            .read(move |conn| Ok(operation.lookup_state(conn)))
            .await?
    }

    /// Wait briefly for capacity on a finalizing write after an external effect.
    pub async fn finalize<T, F>(&self, write: F) -> Result<T>
    where
        T: Send + 'static,
        F: Fn(&Transaction<'_>) -> std::result::Result<WriteOutcome<T>, WriteError<NodeError>>
            + Send
            + Sync
            + 'static,
    {
        let write = Arc::new(write);
        let mut delay = Duration::from_millis(5);
        for _ in 0..5 {
            let attempt = write.clone();
            match self.db.write(move |tx| attempt(tx)).await {
                Err(WriteFailure::Storage(StorageError::Overloaded)) => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                other => return other.map_err(Into::into),
            }
        }
        self.db.write(move |tx| write(tx)).await.map_err(Into::into)
    }
}

impl Operation {
    pub fn lookup(&self, conn: &Connection) -> Result<Option<Value>> {
        self.lookup_state(conn)?.into_result(&self.id)
    }

    pub(crate) fn lookup_state(&self, conn: &Connection) -> Result<ReceiptLookup> {
        let entry = conn
            .query_row(
                "SELECT operation_id, kind, request_sha256, state, result_json, error_json, resource_id
             FROM operations WHERE actor_id = ?1 AND operation_key = ?2",
                params![self.actor_id, self.key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, kind, hash, state, value, error, resource_id)) = entry else {
            return Ok(ReceiptLookup::Missing);
        };
        if kind != self.kind || hash != self.hash {
            return Err(conflict());
        }
        match state.as_str() {
            "accepted" | "in_progress" => Ok(ReceiptLookup::Pending { id, resource_id }),
            "succeeded" => decode(value).map(ReceiptLookup::Succeeded),
            "failed" => Ok(ReceiptLookup::Failed(NodeError(Box::new(decode(error)?)))),
            "unknown" => {
                let problem = error
                    .map(|error| decode(Some(error)).map(|details| NodeError(Box::new(details))))
                    .transpose()?;
                Ok(ReceiptLookup::Unknown {
                    id,
                    problem,
                    resource_id,
                })
            }
            _ => Err(NodeError::internal("Invalid stored operation state")),
        }
    }

    pub fn reserve(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        resource_id: Option<&str>,
    ) -> rusqlite::Result<()> {
        tx.execute(
            "INSERT INTO operations
             (operation_id,actor_id,operation_key,kind,request_sha256,state,resource_id,created_at_ms,updated_at_ms,expires_at_ms)
             VALUES (?1,?2,?3,?4,?5,'in_progress',?6,?7,?7,?8)",
            params![self.id.as_str(), self.actor_id, self.key, self.kind, self.hash,
                    resource_id, now, now.saturating_add(RECEIPT_TTL_MS)],
        )?;
        Ok(())
    }

    pub fn succeed(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        value: &impl Serialize,
    ) -> rusqlite::Result<()> {
        let json = serde_json::to_string(value).map_err(sql_json)?;
        self.finish(tx, now, "succeeded", Some(&json), None)
    }

    pub fn fail(&self, tx: &Transaction<'_>, now: i64, error: &NodeError) -> rusqlite::Result<()> {
        let json = serialize_problem(&error.0).map_err(sql_json)?;
        self.finish(tx, now, "failed", None, Some(&json))
    }

    pub fn unknown(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        error: Option<&NodeError>,
    ) -> rusqlite::Result<()> {
        let json = error
            .map(|error| serialize_problem(&error.0))
            .transpose()
            .map_err(sql_json)?;
        self.finish(tx, now, "unknown", None, json.as_deref())
    }

    fn finish(
        &self,
        tx: &Transaction<'_>,
        now: i64,
        state: &str,
        value: Option<&str>,
        error: Option<&str>,
    ) -> rusqlite::Result<()> {
        let updated = tx.execute(
            "UPDATE operations SET state = ?1, result_json = ?2, error_json = ?3, updated_at_ms = ?4
             WHERE operation_id = ?5 AND state IN ('accepted','in_progress')",
            params![state, value, error, now, self.id.as_str()],
        )?;
        if updated != 1 {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        }
        Ok(())
    }
}

pub(crate) fn abandon_pending(tx: &Transaction<'_>, id: &str, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "UPDATE operations SET state = 'unknown', updated_at_ms = ?2
         WHERE operation_id = ?1 AND state IN ('accepted','in_progress')",
        params![id, now],
    )?;
    Ok(())
}

pub fn serialize_problem(problem: &ProblemDetails) -> serde_json::Result<String> {
    serde_json::to_string(&bound_problem(problem.clone())?)
}

/// Bound a `ProblemDetails` payload so that adding `request_id` still stays
/// within `MAX_PROBLEM_BYTES`, preserving structured correlation IDs.
pub(crate) fn bound_problem(mut problem: ProblemDetails) -> serde_json::Result<ProblemDetails> {
    problem.request_id = None;
    let limit = MAX_PROBLEM_BYTES - REQUEST_ID_FIELD_BYTES;
    if crate::storage::json_encoded_len(&problem)? <= limit {
        return Ok(problem);
    }

    if problem.detail.len() > 1024 {
        problem.detail = problem.detail.chars().take(256).collect();
    }
    problem.instance = None;
    for (name, value) in &mut problem.extensions {
        if is_preserved_extension(name) {
            continue;
        }
        if let Value::String(text) = value
            && text.len() > 256
        {
            *text = text.chars().take(64).collect();
        }
    }
    if crate::storage::json_encoded_len(&problem)? <= limit {
        return Ok(problem);
    }

    problem
        .extensions
        .retain(|name, _| is_preserved_extension(name));
    Ok(problem)
}

fn is_preserved_extension(name: &str) -> bool {
    matches!(
        name,
        "approval_id"
            | "operation_id"
            | "message_id"
            | "existing_answer_id"
            | "oldest_available_seq"
            | "latest_seq"
            | "limit_bytes"
    )
}

fn decode<T: DeserializeOwned>(json: Option<String>) -> Result<T> {
    let json = json.ok_or_else(|| NodeError::internal("Missing stored operation result"))?;
    serde_json::from_str(&json).map_err(NodeError::internal)
}

fn sql_json(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn conflict() -> NodeError {
    NodeError::new(
        ErrorCode::OperationConflict,
        "The key is already bound to a different request",
    )
}

fn in_progress() -> NodeError {
    NodeError::new(
        ErrorCode::OperationInProgress,
        "The earlier attempt is still in progress",
    )
}

fn unconfirmed(operation_id: String) -> NodeError {
    NodeError::new(
        ErrorCode::DeliveryUnknown,
        "The earlier attempt may have taken effect",
    )
    .with_extension("operation_id", operation_id)
}

#[cfg(test)]
mod tests {
    use cooplet_protocol::RequestId;

    use super::*;

    #[test]
    fn problem_budget_preserves_ids_and_reserves_room_for_request_id()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut problem = ProblemDetails::new(ErrorCode::AwaitingApproval, "")
            .with_extension("approval_id", "public-approval-id");
        let overhead = serde_json::to_vec(&problem)?.len();
        problem.detail = "x".repeat(MAX_PROBLEM_BYTES - REQUEST_ID_FIELD_BYTES - overhead);
        let exact = serialize_problem(&problem)?;
        assert_eq!(exact.len(), MAX_PROBLEM_BYTES - REQUEST_ID_FIELD_BYTES);

        let mut framed: ProblemDetails = serde_json::from_str(&exact)?;
        framed.request_id = Some(RequestId::generate()?);
        assert_eq!(serde_json::to_vec(&framed)?.len(), MAX_PROBLEM_BYTES);

        problem.detail.push('x');
        problem
            .extensions
            .insert("action_hint".into(), "\"\n".repeat(40_000).into());
        let bounded = serialize_problem(&problem)?;
        assert!(bounded.len() < MAX_PROBLEM_BYTES - REQUEST_ID_FIELD_BYTES);
        let decoded: ProblemDetails = serde_json::from_str(&bounded)?;
        assert_eq!(decoded.code, ErrorCode::AwaitingApproval);
        assert_eq!(decoded.extensions["approval_id"], "public-approval-id");
        Ok(())
    }
}
