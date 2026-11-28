use std::{
    env, fs, io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{AgyAdapterConfig, invalid};
use crate::{
    clients::{EffectiveAgentOptions, NativeSessionKey, SessionOpenMode},
    error::Result,
};

const INVALID_STORE: &str =
    "Agy session store is missing, invalid, or outside its Gemini directory";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoreBinding {
    schema: u32,
    pub gemini_dir: PathBuf,
    pub app_data_dir: PathBuf,
}

impl StoreBinding {
    pub(super) fn resolve(config: &AgyAdapterConfig, mode: &SessionOpenMode) -> Result<Self> {
        match mode {
            SessionOpenMode::Create => Self::for_create(config),
            SessionOpenMode::Resume {
                native_key,
                previous_binding,
            } => {
                native_key.validate("agy")?;
                let mut binding: Self = serde_json::from_value(previous_binding.clone())
                    .map_err(|_| invalid(INVALID_STORE))?;
                if binding.schema != 1 || !binding.gemini_dir.is_absolute() {
                    return Err(invalid(INVALID_STORE));
                }
                binding.check_identity(native_key)?;
                Ok(binding)
            }
        }
    }

    fn for_create(config: &AgyAdapterConfig) -> Result<Self> {
        let gemini = config.gemini_dir.as_deref().unwrap_or(Path::new(".gemini"));
        path_components(gemini, true)?;
        let gemini_dir = if gemini.is_absolute() {
            gemini.to_owned()
        } else {
            let home = env::var_os("HOME")
                .map(PathBuf::from)
                .filter(|home| home.is_absolute() && home.to_str().is_some())
                .ok_or_else(|| invalid("Agy requires an absolute UTF-8 HOME"))?;
            home.join(gemini)
        };
        let app_data_dir = config
            .app_data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("antigravity-cli"));
        let mut binding = Self {
            schema: 1,
            gemini_dir,
            app_data_dir,
        };
        binding.check_paths(false)?;
        Ok(binding)
    }

    pub(super) fn check_paths(&mut self, require_store: bool) -> Result<Option<String>> {
        path_components(&self.gemini_dir, true)?;
        path_components(&self.app_data_dir, false)?;

        let mut prefix = PathBuf::new();
        let mut gemini = None;
        for part in self.gemini_dir.components() {
            prefix.push(part);
            match existing_dir(&prefix)? {
                Some(canonical) => gemini = Some(canonical),
                None if !require_store => return Ok(None),
                None => return Err(invalid(INVALID_STORE)),
            }
        }
        let gemini = gemini.ok_or_else(|| invalid(INVALID_STORE))?;
        self.gemini_dir = gemini.clone();
        let mut store = gemini.clone();
        let mut prefix = gemini.clone();
        for part in self.app_data_dir.components() {
            prefix.push(part);
            match existing_dir(&prefix)? {
                Some(canonical) if canonical.starts_with(&gemini) => store = canonical,
                None if !require_store => return Ok(None),
                _ => return Err(invalid(INVALID_STORE)),
            }
        }
        Ok(Some(path_text(&store)?.to_owned()))
    }

    pub(super) fn check_identity(&mut self, key: &NativeSessionKey) -> Result<()> {
        let store = self.check_paths(true)?;
        if store.as_deref() != Some(&key.store_namespace) {
            return Err(invalid(INVALID_STORE));
        }
        Ok(())
    }
}

fn path_components(path: &Path, allow_absolute: bool) -> Result<()> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty()
        || (!allow_absolute && path.is_absolute())
        || bytes
            .split(|byte| *byte == b'/')
            .any(|part| part == b"." || part == b"..")
    {
        return Err(invalid(INVALID_STORE));
    }
    path_text(path)?;
    Ok(())
}

fn existing_dir(path: &Path) -> Result<Option<PathBuf>> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(invalid(INVALID_STORE)),
        Ok(_) => {
            let canonical = fs::canonicalize(path).map_err(|_| invalid(INVALID_STORE))?;
            if !canonical.is_dir() {
                return Err(invalid(INVALID_STORE));
            }
            path_text(&canonical)?;
            Ok(Some(canonical))
        }
    }
}

pub(super) fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| invalid("Agy paths must be valid UTF-8"))
}

pub(super) fn executable(path: &Path) -> bool {
    path.is_file() && cooplet_platform::can_execute(path).unwrap_or(false)
}

pub(super) fn validate_options(options: &EffectiveAgentOptions) -> Result<()> {
    if !matches!(options.approval.as_deref(), None | Some("never")) {
        return Err(invalid("Agy approval must be never"));
    }
    if !matches!(
        options.sandbox.as_deref(),
        None | Some("none" | "danger-full-access")
    ) {
        return Err(invalid("Agy requires a full-access workspace"));
    }
    if [&options.model, &options.effort]
        .into_iter()
        .flatten()
        .any(|value| value.trim().is_empty())
    {
        return Err(invalid("Agy model and effort must be nonempty"));
    }
    Ok(())
}
