use std::{
    fs::{self, Metadata, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use cooplet_platform::{FileLock, PlatformError, PrivateFs, current_euid};
use rusqlite::{Connection, OpenFlags};

use crate::{Migration, StorageConfig, StorageError, SynchronousMode, migrate};

const PAGE_SIZE: u32 = 4096;
const MAX_SQLITE_PAGES: u32 = 4_294_967_294;

pub(crate) type ExistingDatabaseCheck =
    Box<dyn FnOnce(&Connection) -> Result<(), StorageError> + Send>;

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
    let input = &config.db_path;
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
    let existing_only = existing_check.is_some();
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
    let path = parent.join(file);
    let lock = FileLock::acquire_exclusive(&path.with_extension("lock"))?;
    let metadata = database_metadata(&path)?;
    let size = metadata.as_ref().map(Metadata::len);
    if existing_only {
        match size {
            None => return Err(StorageError::DatabaseMissing),
            Some(0) => {
                return Err(StorageError::IncompatibleDatabase(
                    "expected a non-empty existing database".into(),
                ));
            }
            Some(_) => {}
        }
    }

    let fresh = size.unwrap_or(0) == 0;
    let from = if fresh {
        if let Some(root) = blob_root {
            crate::blob_store::check_before_database_create(root, &path, &lock)?;
        }
        if size.is_none() {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
        }
        0
    } else {
        let preflight = open_readonly(&path, config)?;
        verify_header(&preflight)?;
        let from: i64 = pragma(&preflight, "user_version")?;
        if !(0..=i64::from(target)).contains(&from) {
            return Err(StorageError::IncompatibleDatabase(format!(
                "user_version {from} exceeds supported range 0..={target}"
            )));
        }
        check_page_limit(pragma(&preflight, "page_count")?, config.max_page_count)?;
        migrate::schema_hygiene(&preflight)?;
        if let Some(check) = existing_check {
            check(&preflight)?;
        }
        from as u32
    };

    lock.verify_path()?;
    if existing_only
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

fn database_metadata(path: &Path) -> Result<Option<Metadata>, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_file() && metadata.nlink() == 1 && metadata.uid() == current_euid() =>
        {
            Ok(Some(metadata))
        }
        Ok(_) => Err(StorageError::IncompatibleDatabase(
            "expected a regular single-link database owned by the current user".into(),
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
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    connection.pragma_update(None, "query_only", true)?;
    common_pragmas(&connection, config)?;
    Ok(connection)
}

fn common_pragmas(connection: &Connection, config: &StorageConfig) -> Result<(), StorageError> {
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
