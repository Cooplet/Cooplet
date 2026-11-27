use std::{
    fs::{self, Metadata, OpenOptions, Permissions},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

use cooplet_platform::{FileLock, PlatformError, PrivateFs, current_euid};
use rusqlite::{Connection, OpenFlags, config::DbConfig};

use crate::{Migration, StorageConfig, StorageError, SynchronousMode, migrate};

const PAGE_SIZE: u32 = 4096;
const MAX_SQLITE_PAGES: u32 = 4_294_967_294;
const STATEMENT_CACHE_CAPACITY: usize = 128;
const READ_ONLY_FLAGS: OpenFlags = OpenFlags::SQLITE_OPEN_READ_ONLY
    .union(OpenFlags::SQLITE_OPEN_NO_MUTEX)
    .union(OpenFlags::SQLITE_OPEN_NOFOLLOW);

type Check = Box<dyn FnOnce(&Connection) -> Result<(), StorageError> + Send>;

pub(crate) struct ExistingDatabaseCheck {
    pub(crate) require_database: bool,
    pub(crate) validate: Check,
}

pub(crate) fn validate_config(config: &StorageConfig) -> Result<(), StorageError> {
    if config.read_pool_size == 0
        || config.read_pool_size as usize > tokio::sync::Semaphore::MAX_PERMITS
        || config.write_queue_capacity == 0
        || config.write_queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
        || config.broadcast_capacity == 0
        || config.broadcast_capacity > usize::MAX / 2
        || config.gc_interval.is_zero()
        || config.max_page_count == 0
        || config.max_page_count > MAX_SQLITE_PAGES
        || config.journal_size_limit_bytes > i64::MAX as u64
        || config.wal_autocheckpoint_pages > i32::MAX as u32
        || config.busy_timeout.as_millis() > i32::MAX as u128
    {
        return Err(StorageError::InvalidConfig(
            "invalid queue, timeout or SQLite limit",
        ));
    }
    Ok(())
}

pub(crate) fn open_writer(
    config: &mut StorageConfig,
    migrations: &[Migration],
    blob_root: Option<&Path>,
    existing_check: Option<ExistingDatabaseCheck>,
) -> Result<(Connection, FileLock), StorageError> {
    validate_config(config)?;
    let target = migrate::target_version(migrations)?;
    let existing_only = existing_check
        .as_ref()
        .is_some_and(|check| check.require_database);
    let path = database_path(&config.db_path, existing_only)?;
    if existing_only {
        // Avoid creating an orphan sidecar lock when the database is missing or empty.
        require_existing(database_metadata(&path)?.as_ref())?;
    }
    let lock = FileLock::acquire_exclusive(&path.with_extension("lock"))?;
    let metadata = database_metadata(&path)?;
    let size = metadata.as_ref().map(Metadata::len);
    if existing_only {
        require_existing(metadata.as_ref())?;
    }

    let fresh = size.unwrap_or(0) == 0;
    let from = if fresh {
        if let Some(root) = blob_root {
            crate::blob_store::check_before_database_open(root, &path, true)?;
        }
        if size.is_none() {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            file.set_permissions(Permissions::from_mode(0o600))?;
        }
        0
    } else {
        let preflight = open_startup_preflight(&path, config, &lock)?;
        let from = validate_preflight(&preflight, config, target)?;
        if let Some(check) = existing_check {
            (check.validate)(&preflight)?;
        }
        if let Some(root) = blob_root {
            crate::blob_store::check_before_database_open(root, &path, false)?;
        }
        from
    };

    lock.verify_path()?;
    if !fresh
        && database_metadata(&path)?.as_ref().map(file_identity)
            != metadata.as_ref().map(file_identity)
    {
        return Err(StorageError::IncompatibleDatabase(
            "database file changed during startup validation".into(),
        ));
    }
    let mut connection = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    if fresh {
        connection.pragma_update(None, "page_size", PAGE_SIZE)?;
        connection.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    }
    common_pragmas(&connection, config)?;
    let journal: String =
        connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal.eq_ignore_ascii_case("wal") {
        return Err(StorageError::IncompatibleDatabase(format!(
            "could not set WAL mode: {journal}"
        )));
    }
    let synchronous = match config.synchronous {
        SynchronousMode::Full => "FULL",
        SynchronousMode::Normal => "NORMAL",
    };
    connection.pragma_update(None, "synchronous", synchronous)?;
    connection.pragma_update(None, "wal_autocheckpoint", config.wal_autocheckpoint_pages)?;
    connection.pragma_update(None, "journal_size_limit", config.journal_size_limit_bytes)?;
    connection.pragma_update(None, "cache_size", -16_000)?;
    let limit_sql = format!("PRAGMA max_page_count = {}", config.max_page_count);
    let effective: u32 = connection.query_row(&limit_sql, [], |row| row.get(0))?;
    if effective != config.max_page_count {
        return Err(StorageError::PageLimit {
            requested: config.max_page_count,
            effective,
        });
    }
    verify_header(&connection)?;
    let foreign_keys: u32 = pragma(&connection, "foreign_keys")?;
    if foreign_keys != 1 {
        return Err(StorageError::IncompatibleDatabase(
            "foreign key checks are unavailable".into(),
        ));
    }
    migrate::migrate(&mut connection, migrations, from)?;
    if fresh && target == 0 {
        connection.pragma_update(None, "user_version", 0)?;
    }
    config.db_path = path;
    Ok((connection, lock))
}

pub(crate) fn check_existing<T>(
    config: &StorageConfig,
    target: u32,
    check: impl FnOnce(&Connection) -> Result<T, StorageError>,
) -> Result<T, StorageError> {
    let path = database_path(&config.db_path, true)?;
    require_existing(database_metadata(&path)?.as_ref())?;
    let lock = FileLock::acquire_existing_shared(&path.with_extension("lock"))?;
    require_existing(database_metadata(&path)?.as_ref())?;
    let connection = open_startup_preflight(&path, config, &lock)?;
    validate_preflight(&connection, config, target)?;
    check(&connection)
}

fn database_path(input: &Path, existing_only: bool) -> Result<PathBuf, StorageError> {
    let parent = input
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file = input
        .file_name()
        .filter(|name| {
            name.as_encoded_bytes().ends_with(b".sqlite3") && name.len() > b".sqlite3".len()
        })
        .ok_or(StorageError::InvalidConfig(
            "database must have a .sqlite3 basename",
        ))?;
    let parent = if existing_only {
        match PrivateFs::validate_private_dir(parent) {
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(StorageError::DatabaseMissing);
            }
            result => result?,
        }
    } else {
        PrivateFs::ensure_private_dir(parent)?
    };
    Ok(parent.join(file))
}

fn require_existing(metadata: Option<&Metadata>) -> Result<(), StorageError> {
    match metadata.map(Metadata::len) {
        None => Err(StorageError::DatabaseMissing),
        Some(0) => Err(StorageError::IncompatibleDatabase(
            "expected a non-empty existing database".into(),
        )),
        Some(_) => Ok(()),
    }
}

fn validate_preflight(
    preflight: &Connection,
    config: &StorageConfig,
    target: u32,
) -> Result<u32, StorageError> {
    verify_header(preflight)?;
    let from: i64 = pragma(preflight, "user_version")?;
    if !(0..=i64::from(target)).contains(&from) {
        return Err(StorageError::IncompatibleDatabase(format!(
            "user_version {from} exceeds supported range 0..={target}"
        )));
    }
    check_page_limit(pragma(preflight, "page_count")?, config.max_page_count)?;
    migrate::schema_hygiene(preflight)?;
    Ok(from as u32)
}

fn database_metadata(path: &Path) -> Result<Option<Metadata>, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_file()
                && metadata.nlink() == 1
                && metadata.uid() == current_euid()
                && metadata.mode() & 0o7777 == 0o600 =>
        {
            Ok(Some(metadata))
        }
        Ok(_) => Err(StorageError::IncompatibleDatabase(
            "expected a regular single-link 0600 database owned by the current user".into(),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn file_identity(file: &Metadata) -> (u64, u64) {
    (file.dev(), file.ino())
}

pub(crate) fn open_readonly(
    path: &Path,
    config: &StorageConfig,
) -> Result<Connection, StorageError> {
    let connection = Connection::open_with_flags(path, READ_ONLY_FLAGS)?;
    configure_readonly(&connection, config)?;
    Ok(connection)
}

fn open_startup_preflight(
    path: &Path,
    config: &StorageConfig,
    lock: &FileLock,
) -> Result<Connection, StorageError> {
    lock.verify_path()?;
    let has_wal = match fs::symlink_metadata(wal_path(path)) {
        Ok(metadata) if metadata.is_file() => metadata.len() != 0,
        Ok(_) => {
            return Err(StorageError::IncompatibleDatabase(
                "expected a regular WAL file".into(),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let connection = if has_wal {
        // The sidecar excludes writers while this read runs. The exclusive
        // pager mode builds each reader's WAL index in memory before the first read.
        let connection = Connection::open_with_flags_and_vfs(path, READ_ONLY_FLAGS, "unix-none")?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
        connection.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        connection
    } else {
        // SQLite's regular WAL reader can create a WAL and a shared-memory
        // file even when both are absent. The sidecar keeps this file stable.
        Connection::open_with_flags(
            immutable_uri(path),
            READ_ONLY_FLAGS | OpenFlags::SQLITE_OPEN_URI,
        )?
    };
    configure_readonly(&connection, config)?;
    Ok(connection)
}

fn immutable_uri(path: &Path) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut uri = String::from("file:");
    // Paths can contain arbitrary Unix bytes and SQLite URI delimiters.
    for &byte in path.as_os_str().as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-' | b'~') {
            uri.push(char::from(byte));
        } else {
            uri.push('%');
            uri.push(char::from(HEX[usize::from(byte >> 4)]));
            uri.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    uri
}

fn configure_readonly(connection: &Connection, config: &StorageConfig) -> Result<(), StorageError> {
    connection.pragma_update(None, "query_only", true)?;
    common_pragmas(connection, config)
}

fn common_pragmas(connection: &Connection, config: &StorageConfig) -> Result<(), StorageError> {
    connection.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    connection.busy_timeout(config.busy_timeout)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

fn verify_header(connection: &Connection) -> Result<(), StorageError> {
    let page_size: u32 = pragma(connection, "page_size")?;
    let auto_vacuum: u32 = pragma(connection, "auto_vacuum")?;
    if page_size != PAGE_SIZE || auto_vacuum != 2 {
        return Err(StorageError::IncompatibleDatabase(format!(
            "expected page_size {PAGE_SIZE} and incremental auto_vacuum, found {page_size} and {auto_vacuum}"
        )));
    }
    Ok(())
}

fn check_page_limit(pages: u32, requested: u32) -> Result<(), StorageError> {
    if pages > requested {
        Err(StorageError::PageLimit {
            requested,
            effective: pages,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn pragma<T: rusqlite::types::FromSql>(
    connection: &Connection,
    name: &str,
) -> rusqlite::Result<T> {
    connection.pragma_query_value(None, name, |row| row.get(0))
}

pub(crate) fn wal_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push("-wal");
    PathBuf::from(path)
}
