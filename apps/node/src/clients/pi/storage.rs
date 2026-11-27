use std::{
    env, fs, io,
    io::{BufRead as _, BufReader, Read as _},
    os::unix::fs::MetadataExt as _,
    path::{Path, PathBuf},
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::ParticipantId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    clients::{NativeSessionKey, OpenRollback, SessionOpenMode},
    error::Result,
};

use super::{KIND, unavailable};

const MAX_HEADER_BYTES: u64 = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub(super) session_file: PathBuf,
}

#[derive(Deserialize)]
struct Header {
    #[serde(rename = "type")]
    kind: String,
    id: String,
    cwd: PathBuf,
}

pub(super) struct SessionFile {
    pub(super) agent_dir: PathBuf,
    pub(super) workspace: PathBuf,
    pub(super) path: PathBuf,
    pub(super) expected_id: Option<String>,
}

impl SessionFile {
    pub(super) fn prepare(
        workspace: &Path,
        participant: &ParticipantId,
        mode: SessionOpenMode,
        rollback: &mut Option<OpenRollback>,
    ) -> Result<Self> {
        let workspace = canonical_directory(workspace)
            .ok_or_else(|| unavailable("Could not resolve the Pi workspace"))?;
        let agent_dir = agent_directory(&workspace)
            .ok_or_else(|| unavailable("Could not resolve the Pi agent directory"))?;
        let creating = matches!(mode, SessionOpenMode::Create);
        if creating {
            fs::create_dir_all(&agent_dir)
                .map_err(|_| unavailable("Could not create the Pi agent directory"))?;
        }
        let agent_dir = canonical_directory(&agent_dir)
            .ok_or_else(|| unavailable("Could not resolve the Pi agent directory"))?;
        let store = agent_dir.join("cooplet-sessions");
        let store = if creating {
            PrivateFs::ensure_private_dir(&store)
        } else {
            PrivateFs::validate_private_dir(&store)
        }
        .map_err(|_| unavailable("Could not open the private Pi session store"))?;
        let filename = format!("{participant}.jsonl");
        let path = store.join(&filename);

        let expected_id = match mode {
            SessionOpenMode::Create => {
                let directory = PrivateFs::open_dir_nofollow(&store)
                    .map_err(|_| unavailable("Could not open the private Pi session store"))?;
                let file = PrivateFs::create_private_file_at(&directory, &filename)
                    .map_err(|_| unavailable("Could not reserve a new Pi session file"))?;
                let reserved_path = path.clone();
                let synced = file.sync_all().and_then(|()| directory.sync_all());
                *rollback = Some(Box::new(move || {
                    let expected = file.metadata()?;
                    let actual = match fs::symlink_metadata(&reserved_path) {
                        Ok(actual) => actual,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                        Err(error) => return Err(error),
                    };
                    if (actual.dev(), actual.ino()) != (expected.dev(), expected.ino()) {
                        return Err(io::Error::other("The reserved native session file changed"));
                    }
                    PrivateFs::unlink_file_at(&directory, &filename).map_err(io::Error::other)?;
                    directory.sync_all()
                }));
                synced.map_err(|_| unavailable("Could not sync the Pi session reservation"))?;
                None
            }
            SessionOpenMode::Resume {
                native_key,
                previous_binding,
            } => {
                native_key.validate(KIND)?;
                let binding: Binding = serde_json::from_value(previous_binding)
                    .map_err(|_| unavailable("The saved Pi session binding is invalid"))?;
                if native_key.store_namespace != store.to_string_lossy()
                    || binding.session_file.as_os_str() != path.as_os_str()
                    || read_header_id(&path, &workspace)? != native_key.native_session_id
                {
                    return Err(unavailable(
                        "The saved Pi conversation could not be verified",
                    ));
                }
                Some(native_key.native_session_id)
            }
        };
        Ok(Self {
            agent_dir,
            workspace,
            path,
            expected_id,
        })
    }

    pub(super) fn verify(&self, state: &Value) -> Result<NativeSessionKey> {
        let id = read_header_id(&self.path, &self.workspace)?;
        let state_file = state
            .get("sessionFile")
            .and_then(Value::as_str)
            .map(Path::new);
        if state.get("sessionId").and_then(Value::as_str) != Some(id.as_str())
            || state_file.map(Path::as_os_str) != Some(self.path.as_os_str())
            || self
                .expected_id
                .as_ref()
                .is_some_and(|expected| *expected != id)
        {
            return Err(unavailable(
                "Pi did not open the expected native conversation",
            ));
        }
        let store = self
            .path
            .parent()
            .and_then(Path::to_str)
            .ok_or_else(|| unavailable("The Pi session store path is not valid UTF-8"))?;
        Ok(NativeSessionKey {
            client_kind: KIND.into(),
            store_namespace: store.into(),
            native_session_id: id,
        })
    }
}

fn agent_directory(workspace: &Path) -> Option<PathBuf> {
    let home = || env::home_dir().filter(|path| path.is_absolute());
    let Some(configured) = env::var_os("PI_CODING_AGENT_DIR").filter(|path| !path.is_empty())
    else {
        return Some(home()?.join(".pi/agent"));
    };
    let configured = Path::new(&configured);
    if let Ok(relative) = configured.strip_prefix("~") {
        return Some(home()?.join(relative));
    }
    Some(workspace.join(configured))
}

fn canonical_directory(path: &Path) -> Option<PathBuf> {
    let path = path.canonicalize().ok()?;
    (path.is_dir() && path.to_str().is_some()).then_some(path)
}

fn read_header_id(file: &Path, workspace: &Path) -> Result<String> {
    let invalid_header = || unavailable("The private Pi session header could not be verified");
    let file = PrivateFs::open_private_file(file).map_err(|_| invalid_header())?;
    let mut line = Vec::new();
    BufReader::new(file)
        .take(MAX_HEADER_BYTES + 1)
        .read_until(b'\n', &mut line)
        .map_err(|_| invalid_header())?;
    if line.len() as u64 > MAX_HEADER_BYTES || !line.ends_with(b"\n") {
        return Err(invalid_header());
    }
    let header: Header = serde_json::from_slice(&line).map_err(|_| invalid_header())?;
    if header.kind != "session"
        || header.cwd.as_os_str() != workspace.as_os_str()
        || header.id.trim().is_empty()
        || header.id.len() > 256
    {
        return Err(invalid_header());
    }
    Ok(header.id)
}
