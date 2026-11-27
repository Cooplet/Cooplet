use std::{
    collections::HashSet,
    env, fs, io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{NodeId, NodeInfoResponse, ParticipantId, PrincipalKind};
use cooplet_storage::{BlobStoreConfig, StorageConfig, StorageEngine, StorageError, WriteOutcome};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::net::TcpListener;

use crate::{
    auth::{
        Auth, Credential, MASTER_FILE, OPERATOR_FILE, platform_io, publish_master, read_master,
    },
    now_ms,
    storage::{MIGRATIONS, NodeGcHooks, recover},
};

pub const CREDS_DIR: &str = "creds";

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub state_dir: PathBuf,
    pub workspace_dir: PathBuf,
    pub port: Option<u16>,
    pub display_name: Option<String>,
    pub max_blob_bytes: u64,
    pub max_total_blob_bytes: u64,
}

impl NodeConfig {
    pub fn new(state_dir: PathBuf, workspace_dir: PathBuf) -> Self {
        let blob_defaults = BlobStoreConfig::new(state_dir.join("blobs"));
        Self {
            state_dir,
            workspace_dir,
            port: None,
            display_name: None,
            max_blob_bytes: blob_defaults.max_blob_bytes,
            max_total_blob_bytes: blob_defaults.max_total_blob_bytes,
        }
    }
}

pub struct OpenNode {
    pub engine: StorageEngine,
    pub listener: TcpListener,
    pub info: NodeInfoResponse,
    pub auth: Auth,
    pub operator: Credential,
    pub state_dir: PathBuf,
    pub workspace_dir: PathBuf,
}

type VerifiedNode = (NodeInfoResponse, Auth, Credential);

impl OpenNode {
    pub async fn start(config: NodeConfig) -> Result<Self, StorageError> {
        let input_state = config.state_dir.clone();
        let input_workspace = config.workspace_dir.clone();
        let (state_dir, workspace_dir) = tokio::task::spawn_blocking(move || {
            let state_dir = absolute_path(&input_state)?;
            let workspace_dir = fs::canonicalize(input_workspace)?;
            if !workspace_dir.is_dir() {
                return Err(StorageError::InvalidConfig(
                    "workspace directory must be a directory",
                ));
            }
            if let Some(parent) = state_dir.parent() {
                fs::create_dir_all(parent)?;
            }
            Ok((state_dir, workspace_dir))
        })
        .await??;

        let storage = StorageConfig::new(state_dir.join("node.sqlite3"));
        let mut blobs = BlobStoreConfig::new(state_dir.join("blobs"));
        blobs.max_blob_bytes = config.max_blob_bytes;
        blobs.max_total_blob_bytes = config.max_total_blob_bytes;
        let hooks = Arc::new(NodeGcHooks::default());
        let directory = state_dir.clone();
        let port = config.port;
        let preflight_verified: Arc<Mutex<Option<VerifiedNode>>> = Arc::default();
        let preflight_slot = preflight_verified.clone();
        let engine = StorageEngine::open_managed_checked(
            storage,
            MIGRATIONS,
            hooks.clone(),
            Some(blobs),
            move |conn| preflight(conn, &directory, port, &preflight_slot),
        )
        .await?;

        let ready = (async {
            let dir_to_validate = state_dir.clone();
            let state_dir = tokio::task::spawn_blocking(move || {
                PrivateFs::validate_private_dir(&dir_to_validate)
            })
            .await??;
            let now = now_ms()?;
            let verified = preflight_verified
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let (listener, info, auth, operator) = bootstrap(
                &engine,
                &state_dir,
                config.port,
                config.display_name,
                now,
                verified,
            )
            .await?;
            recover(&engine, now).await?;
            let retained_runtime = engine
                .reads()
                .read(|conn| {
                    let mut stmt = conn.prepare(
                        "SELECT participant_id, generation FROM participants
                         WHERE kind = 'agent' AND state != 'stopped'",
                    )?;
                    let keys = stmt
                        .query_map([], |row| {
                            let id: String = row.get(0)?;
                            let generation: u64 = row.get(1)?;
                            Ok(format!("{id}.g{generation}"))
                        })?
                        .collect::<rusqlite::Result<HashSet<_>>>()?;
                    Ok(keys)
                })
                .await?;
            let sweep_dir = state_dir.clone();
            tokio::task::spawn_blocking(move || {
                sweep_startup_artifacts(&sweep_dir, &retained_runtime)
            })
            .await?;
            Ok::<_, StorageError>((state_dir, listener, info, auth, operator))
        })
        .await;
        match ready {
            Ok((state_dir, listener, info, auth, operator)) => {
                hooks.mark_ready();
                Ok(Self {
                    engine,
                    listener,
                    info,
                    auth,
                    operator,
                    state_dir,
                    workspace_dir,
                })
            }
            Err(error) => {
                if let Err(close_error) = engine.shutdown().await {
                    tracing::warn!(%close_error, "Could not close Node storage after startup failure");
                }
                Err(error)
            }
        }
    }
}

pub fn default_state_dir() -> io::Result<PathBuf> {
    xdg_dir("XDG_STATE_HOME", ".local/state").map(|base| base.join("cooplet"))
}

pub fn xdg_dir(variable: &str, home_relative: &str) -> io::Result<PathBuf> {
    if let Some(base) = env::var_os(variable)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return Ok(base);
    }
    env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .map(|base| base.join(home_relative))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Could not locate the home directory",
            )
        })
}

pub fn operator_path(state_dir: &Path) -> PathBuf {
    state_dir.join(CREDS_DIR).join(OPERATOR_FILE)
}

fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn read_info(conn: &Connection) -> rusqlite::Result<Option<NodeInfoResponse>> {
    conn.query_row(
        "SELECT node_id, display_name FROM node_meta WHERE id = 1",
        [],
        |row| {
            let node_id: String = row.get(0)?;
            Ok(NodeInfoResponse {
                node_id: node_id
                    .parse()
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                display_name: row.get(1)?,
                protocol_version: 1,
            })
        },
    )
    .optional()
}

fn preflight(
    conn: &Connection,
    dir: &Path,
    port: Option<u16>,
    verified: &Mutex<Option<VerifiedNode>>,
) -> Result<(), StorageError> {
    let version: u32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version == 0 {
        return Ok(());
    }
    let Some(info) = read_info(conn)? else {
        return Ok(());
    };
    let (auth, operator) = verified_operator_with_auth(conn, dir, &info, port)?;
    *verified
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((info, auth, operator));
    Ok(())
}

/// Verify the saved operator of a stopped Node without opening a writer.
pub async fn check_offline_state(dir: &Path) -> Result<Credential, StorageError> {
    let dir = dir.to_owned();
    StorageEngine::check_existing(
        StorageConfig::new(dir.join("node.sqlite3")),
        MIGRATIONS,
        move |conn| {
            let version: u32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if version == 0 {
                return Err(StorageError::IncompatibleDatabase(
                    "The Node database has not been initialized".into(),
                ));
            }
            let info = read_info(conn)?.ok_or_else(|| {
                StorageError::IncompatibleDatabase(
                    "The Node does not have a committed identity".into(),
                )
            })?;
            verified_operator_with_auth(conn, &dir, &info, None).map(|(_, operator)| operator)
        },
    )
    .await
}

fn verified_operator_with_auth(
    conn: &Connection,
    dir: &Path,
    info: &NodeInfoResponse,
    port: Option<u16>,
) -> Result<(Auth, Credential), StorageError> {
    let (auth, operator) = load_operator(dir, info, port)?;
    if !auth.operator_matches(conn, &operator, now_ms()?)? {
        return Err(StorageError::IncompatibleDatabase(
            "The saved operator credential does not match this database".into(),
        ));
    }
    Ok((auth, operator))
}

fn load_operator(
    dir: &Path,
    info: &NodeInfoResponse,
    port: Option<u16>,
) -> Result<(Auth, Credential), StorageError> {
    let incompatible = |error: io::Error| StorageError::IncompatibleDatabase(error.to_string());
    PrivateFs::validate_private_dir(&dir.join(CREDS_DIR))
        .map_err(platform_io)
        .map_err(incompatible)?;
    let key = read_master(dir).map_err(incompatible)?;
    let operator = Credential::read(&operator_path(dir)).map_err(incompatible)?;
    if operator.node_id != info.node_id {
        return Err(StorageError::IncompatibleDatabase(
            "The saved credential identifies a different Node".into(),
        ));
    }
    verify_port(&operator, port)?;
    Ok((Auth::new(key, info.node_id.clone()), operator))
}

fn verify_port(credential: &Credential, port: Option<u16>) -> Result<(), StorageError> {
    let saved = credential.address()?.port();
    if port.is_some_and(|port| port != saved) {
        return Err(StorageError::IncompatibleDatabase(format!(
            "Requested port does not match the saved Node port {saved}"
        )));
    }
    Ok(())
}

async fn bootstrap(
    engine: &StorageEngine,
    dir: &Path,
    port: Option<u16>,
    display_name: Option<String>,
    now: i64,
    verified: Option<VerifiedNode>,
) -> Result<(TcpListener, NodeInfoResponse, Auth, Credential), StorageError> {
    if let Some((info, auth, operator)) = verified {
        let listener = TcpListener::bind(operator.address()?).await?;
        return Ok((listener, info, auth, operator));
    }

    let state_dir = dir.to_owned();
    let (creds, existing_operator, address) = tokio::task::spawn_blocking(move || {
        let creds = PrivateFs::ensure_private_dir(&state_dir.join(CREDS_DIR))?;
        PrivateFs::clean_interrupted_writes(&state_dir, MASTER_FILE)?;
        PrivateFs::clean_interrupted_writes(&creds, OPERATOR_FILE)?;
        let existing_operator = optional_file(Credential::read(&creds.join(OPERATOR_FILE)))?;
        let address = if let Some(operator) = &existing_operator {
            verify_port(operator, port)?;
            operator.address()?
        } else {
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, port.unwrap_or(0))
        };
        Ok::<_, StorageError>((creds, existing_operator, address))
    })
    .await??;

    let listener = TcpListener::bind(address).await?;
    let SocketAddr::V4(address) = listener.local_addr()? else {
        return Err(StorageError::InvalidConfig(
            "Node requires an IPv4 loopback listener",
        ));
    };

    let state_dir = dir.to_owned();
    let (key, operator, resolved_name) = tokio::task::spawn_blocking(move || {
        let key = match optional_file(read_master(&state_dir))? {
            Some(key) => key,
            None => publish_master(&state_dir)?,
        };
        let operator = match existing_operator {
            Some(operator) => operator,
            None => {
                let node_id = NodeId::generate().map_err(io::Error::other)?;
                let participant_id = ParticipantId::generate().map_err(io::Error::other)?;
                let credential = Credential::new(node_id, participant_id, address)?;
                credential.publish(&creds, OPERATOR_FILE)?;
                credential
            }
        };
        PrivateFs::open_dir_nofollow(&state_dir)?.sync_all()?;
        PrivateFs::open_dir_nofollow(&creds)?.sync_all()?;
        let resolved_name = display_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(hostname);
        Ok::<_, StorageError>((key, operator, resolved_name))
    })
    .await??;

    let info = NodeInfoResponse {
        node_id: operator.node_id.clone(),
        display_name: resolved_name,
        protocol_version: 1,
    };
    let auth = Auth::new(key, info.node_id.clone());
    let write_auth = auth.clone();
    let write_info = info.clone();
    let write_operator = operator.clone();
    engine.db().write_sqlite(move |tx| {
        tx.execute(
            "INSERT INTO node_meta (id,node_id,display_name,created_at_ms,updated_at_ms) VALUES (1,?1,?2,?3,?3)",
            params![write_info.node_id.as_str(), write_info.display_name, now],
        )?;
        tx.execute(
            "INSERT INTO participants (participant_id,kind,name,state,created_at_ms,updated_at_ms)
             VALUES (?1,'human','operator','idle',?2,?2)",
            params![write_operator.participant_id.as_str(), now],
        )?;
        write_auth.insert_credential(tx, &write_operator, PrincipalKind::Operator, now)?;
        Ok(WriteOutcome::new(()))
    }).await?;
    Ok((listener, info, auth, operator))
}

fn optional_file<T>(value: io::Result<T>) -> io::Result<Option<T>> {
    match value {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn sweep_startup_artifacts(state_dir: &Path, retained_runtime: &HashSet<String>) {
    sweep_revoked_agent_creds(&state_dir.join(CREDS_DIR));
    sweep_orphan_runtime_dirs(&state_dir.join("runtime"), retained_runtime);
}

fn sweep_revoked_agent_creds(creds_dir: &Path) {
    let Ok(dir_fd) = PrivateFs::open_dir_nofollow(creds_dir) else {
        return;
    };
    let Ok(entries) = fs::read_dir(creds_dir) else {
        return;
    };
    let mut removed = false;
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !is_agent_credential_artifact(&name) {
            continue;
        }
        if entry.file_type().is_ok_and(|ft| ft.is_file()) {
            match PrivateFs::unlink_file_at(&dir_fd, &name) {
                Ok(true) => removed = true,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, file = name, "Could not sweep revoked agent credential");
                }
            }
        }
    }
    if removed && let Err(error) = dir_fd.sync_all() {
        tracing::warn!(%error, "Could not sync credentials directory after startup sweep");
    }
}

fn is_agent_credential_artifact(name: &str) -> bool {
    if name == OPERATOR_FILE {
        return false;
    }
    if is_agent_cred_filename(name) {
        return true;
    }
    if let Some(rest) = name.strip_prefix(".tmp.")
        && let Some((target, suffix)) = rest.rsplit_once('.')
        && suffix.len() == 32
        && suffix
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return is_agent_cred_filename(target);
    }
    false
}

fn is_agent_cred_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    is_agent_generation_key(stem)
}

fn is_agent_generation_key(name: &str) -> bool {
    let Some((participant, generation)) = name.rsplit_once(".g") else {
        return false;
    };
    ParticipantId::parse(participant).is_ok()
        && !generation.is_empty()
        && generation.bytes().all(|byte| byte.is_ascii_digit())
}

fn sweep_orphan_runtime_dirs(runtime_root: &Path, retained: &HashSet<String>) {
    use std::os::unix::fs::MetadataExt as _;

    let Ok(dir_fd) = PrivateFs::open_dir_nofollow(runtime_root) else {
        return;
    };
    let (Ok(fd_meta), Ok(path_meta)) = (dir_fd.metadata(), fs::symlink_metadata(runtime_root))
    else {
        return;
    };
    if !path_meta.is_dir() || (fd_meta.dev(), fd_meta.ino()) != (path_meta.dev(), path_meta.ino()) {
        return;
    }
    let Ok(entries) = fs::read_dir(runtime_root) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !is_agent_generation_key(&name) || retained.contains(&name) {
            continue;
        }
        let path = runtime_root.join(&name);
        if fs::symlink_metadata(&path)
            .is_ok_and(|meta| !meta.file_type().is_symlink() && meta.is_dir())
            && let Err(error) = fs::remove_dir_all(&path)
        {
            tracing::warn!(%error, dir = name, "Could not remove orphaned runtime directory");
        }
    }
}

fn hostname() -> String {
    #[cfg(target_os = "linux")]
    if let Ok(name) = fs::read_to_string("/proc/sys/kernel/hostname") {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "cooplet-node".into())
}
