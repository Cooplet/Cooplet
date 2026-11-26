use rusqlite::{Connection, TransactionBehavior};

use crate::StorageError;

#[derive(Clone, Copy, Debug)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    /// Trusted application SQL executed inside one writer transaction.
    /// Transaction control and connection-wide PRAGMAs belong to the engine.
    pub sql: &'static str,
}

pub(crate) fn target_version(migrations: &[Migration]) -> Result<u32, StorageError> {
    let target = u32::try_from(migrations.len())
        .ok()
        .filter(|&target| target <= i32::MAX as u32)
        .ok_or(StorageError::InvalidConfig("too many migrations"))?;
    if migrations
        .iter()
        .zip(1..)
        .any(|(migration, version)| migration.version != version)
    {
        return Err(StorageError::InvalidConfig(
            "migration versions must start at 1 and be contiguous",
        ));
    }
    Ok(target)
}

pub(crate) fn schema_hygiene(connection: &Connection) -> Result<(), StorageError> {
    let forbidden: Option<String> = connection.query_row(
        "SELECT name FROM sqlite_schema
         WHERE type = 'trigger'
            OR (type = 'table' AND (lower(name) IN ('node_storage', 'hub_storage') OR lower(name) GLOB '*_storage_rows'))
         UNION ALL SELECT name FROM sqlite_temp_schema WHERE type = 'trigger' LIMIT 1",
        [], |row| row.get(0),
    ).map(Some).or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        other => Err(other),
    })?;
    if let Some(name) = forbidden {
        return Err(StorageError::IncompatibleDatabase(format!(
            "forbidden schema object {name}"
        )));
    }
    Ok(())
}

pub(crate) fn migrate(
    connection: &mut Connection,
    migrations: &[Migration],
    from: u32,
) -> Result<(), StorageError> {
    for migration in migrations.iter().skip(from as usize) {
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(migration.sql)?;
        if tx.is_autocommit() {
            return Err(StorageError::IncompatibleDatabase(format!(
                "migration {} ended the transaction early",
                migration.version
            )));
        }
        schema_hygiene(&tx)?;
        tx.pragma_update(None, "user_version", migration.version)?;
        tx.commit()?;
    }
    Ok(())
}
