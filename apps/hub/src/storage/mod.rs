use std::{
    error::Error,
    fs::{self, File, Metadata},
    io::{self, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use cooplet_platform::{AtomicPublish, PrivateFs, PublishOutcome, random_bytes};
use cooplet_protocol::{HubId, ParticipantId, TokenId, UserId};
use cooplet_storage::{
    BlobStoreConfig, Migration, StorageConfig, StorageEngine, StorageError, WriteOutcome,
};
use rusqlite::{Connection, Row, params, types::Type};

use crate::credentials::{Master, mint_bearer};

pub(crate) mod events;
mod gc;
mod recovery;

#[cfg(test)]
mod tests;

const DATABASE: &str = "hub.sqlite3";
const MASTER_KEY: &str = "hub.master.key";
const ADMIN_TOKEN: &str = "admin.token";
const BLOB_DIR: &str = "blobs";
const ADMIN_SCOPES: &str = r#"["read","collaborate","manage"]"#;
const MIGRATION_V1: Migration = Migration {
    version: 1,
    name: "initial Hub schema",
    sql: include_str!("v1.sql"),
};

type StartupResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
pub(crate) struct HubMeta {
    pub(crate) hub_id: HubId,
    pub(crate) display_name: String,
}

pub(crate) struct HubStorage {
    pub(crate) engine: StorageEngine,
    pub(crate) meta: HubMeta,
    pub(crate) master: Master,
}

pub(crate) struct InitResult {
    pub(crate) hub_id: HubId,
    pub(crate) token_path: Option<PathBuf>,
}

pub(crate) async fn init(state_dir: &Path, display_name: &str) -> StartupResult<InitResult> {
    if display_name.trim().is_empty()
        || display_name.len() > 128
        || display_name.chars().any(char::is_control)
    {
        return Err(io::Error::other("invalid Hub display name").into());
    }
    let state_dir: PathBuf = state_dir.components().collect();
    let existing = if metadata(&state_dir)?.is_some() {
        PrivateFs::validate_private_dir(&state_dir)?;
        let existing = metadata(&state_dir.join(DATABASE))?.is_some_and(|m| m.len() != 0);
        if !existing {
            check_clean_bootstrap(&state_dir, false)?;
        }
        existing
    } else {
        false
    };
    let mut config = StorageConfig::new(state_dir.join(DATABASE));
    config.read_pool_size = 1;
    let engine = if existing {
        // The checked open also starts the maintenance driver. Park its first
        // tick until this bootstrap command releases the database.
        config.gc_interval = Duration::MAX;
        let state_dir = state_dir.clone();
        StorageEngine::open_existing_managed_checked(
            config,
            &[MIGRATION_V1],
            Arc::new(gc::HubGcHooks::default()),
            None,
            move |connection| check_existing_init(connection, &state_dir),
        )
        .await?
    } else {
        StorageEngine::open(config, &[MIGRATION_V1]).await?
    };
    let result = initialize_locked(&state_dir, display_name, &engine).await;
    let shutdown = engine.shutdown().await;
    let result = result?;
    shutdown?;
    Ok(result)
}

async fn initialize_locked(
    state_dir: &Path,
    display_name: &str,
    engine: &StorageEngine,
) -> StartupResult<InitResult> {
    let checked_path = state_dir.to_owned();
    let existing = engine
        .reads()
        .read(move |connection| check_v1_bootstrap(connection, &checked_path))
        .await?;
    if let Some(meta) = existing {
        return Ok(InitResult {
            hub_id: meta.hub_id,
            token_path: None,
        });
    }
    for name in [MASTER_KEY, ADMIN_TOKEN] {
        PrivateFs::clean_interrupted_writes(state_dir, name)?;
    }

    let hub_id = HubId::generate()?;
    let principal_id = UserId::generate()?;
    let participant_id = ParticipantId::generate()?;
    let token_id = TokenId::generate()?;
    let master = Master::generate()?;
    let token = mint_bearer(&master, token_id.clone())?;
    let dir = PrivateFs::open_dir_nofollow(state_dir)?;
    publish_secret(&dir, MASTER_KEY, master.as_bytes())?;
    publish_secret(
        &dir,
        ADMIN_TOKEN,
        format!("{}\n", token.secret.as_str()).as_bytes(),
    )?;

    let expected_hub_id = hub_id.clone();
    let fingerprint = master.fingerprint();
    let expected_fingerprint = fingerprint.clone();
    let display_name = display_name.to_owned();
    let result = engine.db().write_sqlite(move |tx| {
        let now = crate::now_ms();
        tx.execute(
            "INSERT INTO hub_meta (id, hub_id, display_name, master_key_fingerprint, created_at_ms, updated_at_ms)
             VALUES (1, ?1, ?2, ?3, ?4, ?4)",
            params![expected_hub_id.as_str(), display_name, fingerprint, now],
        )?;
        tx.execute(
            "INSERT INTO principals (principal_id, kind, username, display_name, participant_id,
                                     scopes_json, created_at_ms, updated_at_ms)
             VALUES (?1, 'human_admin', 'admin', 'Hub Admin', ?2, ?3, ?4, ?4)",
            params![principal_id.as_str(), participant_id.as_str(), ADMIN_SCOPES, now],
        )?;
        tx.execute(
            "INSERT INTO auth_tokens (token_id, principal_id, token_kind, is_admin, verifier_hmac,
                                      scopes_json, expires_at_ms, created_at_ms)
             VALUES (?1, ?2, 'bearer_api', 1, ?3, ?4, ?5, ?6)",
            params![token_id.as_str(), principal_id.as_str(), token.verifier, ADMIN_SCOPES, i64::MAX, now],
        )?;
        Ok(WriteOutcome::new(()))
    }).await;
    if let Err(error) = result {
        // The singleton, principal and verifier commit together. Verify an uncertain
        // commit without ever generating or publishing a replacement credential.
        let committed = matches!(
            error,
            StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)
        ) && engine.reads().read(read_meta).await.is_ok_and(|meta| {
            meta.is_some_and(|(meta, fingerprint)| {
                meta.hub_id == hub_id && fingerprint == expected_fingerprint
            })
        });
        if !committed {
            return Err(error.into());
        }
    }
    Ok(InitResult {
        hub_id,
        token_path: Some(state_dir.join(ADMIN_TOKEN)),
    })
}

pub(crate) async fn open(
    state_dir: &Path,
    max_blob_bytes: u64,
    max_total_blob_bytes: u64,
    max_page_count: u32,
) -> StartupResult<HubStorage> {
    let mut config = StorageConfig::new(state_dir.join(DATABASE));
    config.read_pool_size = 8;
    config.broadcast_capacity = 64;
    config.max_page_count = max_page_count;
    let blob_config = BlobStoreConfig {
        root: state_dir.join(BLOB_DIR),
        max_blob_bytes,
        max_total_blob_bytes,
    };
    let checked = Arc::new(Mutex::new(None));
    let destination = checked.clone();
    let state_dir = state_dir.to_owned();
    let engine = StorageEngine::open_existing_managed_checked(
        config,
        &[MIGRATION_V1],
        Arc::new(gc::HubGcHooks::default()),
        Some(blob_config),
        move |connection| {
            if user_version(connection)? != MIGRATION_V1.version {
                return Err(incompatible("expected an initialized Hub schema"));
            }
            check_v1_schema(connection)?;
            let (meta, fingerprint) =
                read_meta(connection)?.ok_or_else(|| incompatible("Hub identity is missing"))?;
            let master = check_master(&state_dir, &fingerprint)?;
            *destination
                .lock()
                .map_err(|_| incompatible("Hub startup check failed"))? = Some((meta, master));
            Ok(())
        },
    )
    .await?;
    let identity = checked.lock().ok().and_then(|mut checked| checked.take());
    let Some((meta, master)) = identity else {
        engine.shutdown().await?;
        return Err(incompatible("Hub startup did not verify its identity").into());
    };
    if let Err(error) = recovery::recover(&engine, &meta.hub_id).await {
        let _ = engine.shutdown().await;
        return Err(error.into());
    }
    Ok(HubStorage {
        engine,
        meta,
        master,
    })
}

fn check_existing_init(connection: &Connection, state_dir: &Path) -> Result<(), StorageError> {
    match user_version(connection)? {
        0 => {
            if !schema_objects(connection)?.is_empty() {
                return Err(incompatible(
                    "version 0 Hub database must have an empty schema",
                ));
            }
            check_clean_bootstrap(state_dir, true)
        }
        version if version == MIGRATION_V1.version => {
            check_v1_bootstrap(connection, state_dir).map(|_| ())
        }
        _ => Err(incompatible("unsupported Hub schema version")),
    }
}

fn check_v1_bootstrap(
    connection: &Connection,
    state_dir: &Path,
) -> Result<Option<HubMeta>, StorageError> {
    let schema = check_v1_schema(connection)?;
    if let Some((meta, fingerprint)) = read_meta(connection)? {
        check_master(state_dir, &fingerprint)?;
        return Ok(Some(meta));
    }
    // Only use names from the trusted migration when probing for retained data.
    for table in schema.iter().filter(|object| object.kind == "table") {
        let query = format!("SELECT EXISTS (SELECT 1 FROM \"{}\")", table.name);
        if connection.query_row(&query, [], |row| row.get::<_, bool>(0))? {
            return Err(partial_state());
        }
    }
    check_clean_bootstrap(state_dir, true)?;
    Ok(None)
}

fn user_version(connection: &Connection) -> rusqlite::Result<u32> {
    connection.pragma_query_value(None, "user_version", |row| row.get(0))
}

#[derive(PartialEq, Eq)]
struct SchemaObject {
    kind: String,
    name: String,
    sql: String,
}

fn schema_objects(connection: &Connection) -> rusqlite::Result<Vec<SchemaObject>> {
    let mut statement = connection.prepare(
        "SELECT type, name, sql FROM main.sqlite_schema
         WHERE substr(name, 1, 7) <> 'sqlite_' ORDER BY type, name",
    )?;
    statement
        .query_map([], |row| {
            Ok(SchemaObject {
                kind: row.get(0)?,
                name: row.get(1)?,
                sql: row.get(2)?,
            })
        })?
        .collect()
}

fn check_v1_schema(connection: &Connection) -> Result<Vec<SchemaObject>, StorageError> {
    // SQLite normalizes the DDL it stores in sqlite_schema. Build the reference
    // with the same engine and migration, without modifying the file database.
    let reference = Connection::open_in_memory()?;
    reference.execute_batch(MIGRATION_V1.sql)?;
    let expected = schema_objects(&reference)?;
    if schema_objects(connection)? != expected {
        return Err(incompatible("Hub schema does not match migration v1"));
    }
    Ok(expected)
}

fn read_meta(connection: &Connection) -> Result<Option<(HubMeta, String)>, StorageError> {
    let mut statement = connection.prepare_cached(
        "SELECT id, hub_id, display_name, master_key_fingerprint FROM hub_meta LIMIT 2",
    )?;
    let mut rows = statement.query([])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let id: i64 = row.get(0)?;
    let meta = HubMeta {
        hub_id: id_at(row, 1)?,
        display_name: row.get(2)?,
    };
    let fingerprint: String = row.get(3)?;
    if id != 1 || fingerprint.is_empty() || rows.next()?.is_some() {
        return Err(incompatible("invalid Hub identity record"));
    }
    Ok(Some((meta, fingerprint)))
}

fn check_master(state_dir: &Path, fingerprint: &str) -> Result<Master, StorageError> {
    let master = Master::from_file(&state_dir.join(MASTER_KEY))?;
    if master.fingerprint() != fingerprint {
        return Err(incompatible("Hub master key does not match hub.sqlite3"));
    }
    Ok(master)
}

fn incompatible(detail: &str) -> StorageError {
    StorageError::IncompatibleDatabase(detail.into())
}

fn partial_state() -> StorageError {
    incompatible(
        "partial Hub state; inspect the state directory and use a clean directory to initialize a new Hub",
    )
}

fn metadata(path: &Path) -> io::Result<Option<Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn check_clean_bootstrap(state_dir: &Path, database_is_open: bool) -> Result<(), StorageError> {
    for entry in fs::read_dir(state_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(partial_state());
        };
        match name {
            DATABASE | "hub.lock" => {}
            "hub.sqlite3-shm" | "hub.sqlite3-wal" if database_is_open => {}
            BLOB_DIR => check_empty_blob_dir(&entry.path())?,
            _ if is_staging_name(name) && entry.file_type()?.is_file() => {}
            _ => return Err(partial_state()),
        }
    }
    Ok(())
}

fn check_empty_blob_dir(root: &Path) -> Result<(), StorageError> {
    PrivateFs::validate_private_dir(root)?;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !matches!(entry.file_name().to_str(), Some("objects" | "staging")) {
            return Err(partial_state());
        }
        PrivateFs::validate_private_dir(&entry.path())?;
        if fs::read_dir(entry.path())?.next().transpose()?.is_some() {
            return Err(partial_state());
        }
    }
    Ok(())
}

fn is_staging_name(name: &str) -> bool {
    let Some((prefix, suffix)) = name.rsplit_once('.') else {
        return false;
    };
    matches!(prefix, ".tmp.hub.master.key" | ".tmp.admin.token")
        && suffix.len() == 32
        && suffix
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn publish_secret(dir: &File, target: &str, bytes: &[u8]) -> Result<(), StorageError> {
    let mut random = [0; 16];
    random_bytes(&mut random)?;
    let staging = format!(".tmp.{target}.{:032x}", u128::from_be_bytes(random));
    let mut file = PrivateFs::create_private_file_at(dir, &staging)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        match AtomicPublish::publish_noreplace(dir, &staging, dir, target)? {
            PublishOutcome::Published => Ok(()),
            PublishOutcome::AlreadyExists => Err(partial_state()),
        }
    })();
    if result.is_err() {
        let _ = PrivateFs::unlink_file_at(dir, &staging);
    }
    result
}

fn id_at<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    parse_id(row.get(index)?, index)
}

fn optional_id_at<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<T>>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    row.get::<_, Option<String>>(index)?
        .map(|text| parse_id(text, index))
        .transpose()
}

fn parse_id<T>(text: String, index: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    text.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}

fn hub_id(connection: &Connection) -> rusqlite::Result<HubId> {
    connection.query_row("SELECT hub_id FROM hub_meta WHERE id = 1", [], |row| {
        id_at(row, 0)
    })
}
