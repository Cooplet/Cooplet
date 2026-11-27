use std::{
    collections::BTreeMap,
    fmt, fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{ErrorCode, TemplateSummary};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};
use utoipa::ToSchema;

use crate::{
    config::xdg_dir,
    error::{NodeError, Result},
};

const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_INSTRUCTIONS_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<String>,
}

impl<'de> Deserialize<'de> for AgentOverrides {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[allow(dead_code, reason = "Serde's remote derive describes named fields")]
        #[derive(Deserialize)]
        #[serde(remote = "AgentOverrides", deny_unknown_fields)]
        struct Fields {
            model: Option<String>,
            effort: Option<String>,
            approval: Option<String>,
            sandbox: Option<String>,
        }

        struct NamedFields;

        impl<'de> Visitor<'de> for NamedFields {
            type Value = AgentOverrides;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object with agent override fields")
            }

            fn visit_map<M: MapAccess<'de>>(
                self,
                map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                Fields::deserialize(MapAccessDeserializer::new(map))
            }
        }

        deserializer.deserialize_map(NamedFields)
    }
}

pub type EffectiveAgentOptions = AgentOverrides;

impl AgentOverrides {
    pub fn merged(&self, overrides: &Self) -> EffectiveAgentOptions {
        Self {
            model: overrides.model.clone().or_else(|| self.model.clone()),
            effort: overrides.effort.clone().or_else(|| self.effort.clone()),
            approval: overrides.approval.clone().or_else(|| self.approval.clone()),
            sandbox: overrides.sandbox.clone().or_else(|| self.sandbox.clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTemplate {
    pub client: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub options: AgentOverrides,
    pub instructions: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: u8,
    client: String,
    title: Option<String>,
    description: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    approval: Option<String>,
    sandbox: Option<String>,
    instructions_file: Option<String>,
}

pub fn load_template(workspace_dir: &Path, template_id: &str) -> Result<AgentTemplate> {
    load_with_config_mode(workspace_dir, template_id, user_config_dir, true)
}

pub fn load_template_options(workspace_dir: &Path, template_id: &str) -> Result<AgentTemplate> {
    load_with_config_mode(workspace_dir, template_id, user_config_dir, false)
}

pub fn list_templates(workspace_dir: &Path) -> Result<Vec<TemplateSummary>> {
    list_with_config(workspace_dir, user_config_dir())
}

fn user_config_dir() -> Option<PathBuf> {
    xdg_dir("XDG_CONFIG_HOME", ".config").ok()
}

#[cfg(test)]
fn load_with_config(
    workspace_dir: &Path,
    template_id: &str,
    user_config: impl FnOnce() -> Option<PathBuf>,
) -> Result<AgentTemplate> {
    load_with_config_mode(workspace_dir, template_id, user_config, true)
}

fn load_with_config_mode(
    workspace_dir: &Path,
    template_id: &str,
    user_config: impl FnOnce() -> Option<PathBuf>,
    load_instructions: bool,
) -> Result<AgentTemplate> {
    if !valid_template_id(template_id) {
        return Err(
            NodeError::new(ErrorCode::InvalidRequest, "Invalid agent template ID")
                .with_extension("reason", "invalid_template_id"),
        );
    }

    let workspace = workspace_root(workspace_dir)?;
    if let Some(root) = template_root(&workspace, [".cooplet", "agents"])?
        && let Some(template) = load_in_root(&workspace, &root, template_id, load_instructions)?
    {
        return Ok(template);
    }

    if let Some(config) = config_root(user_config())?
        && let Some(root) = template_root(&config, ["cooplet", "agents"])?
        && let Some(template) = load_in_root(&config, &root, template_id, load_instructions)?
    {
        return Ok(template);
    }

    Err(
        NodeError::new(ErrorCode::InvalidRequest, "Agent template was not found")
            .with_extension("reason", "template_not_found"),
    )
}

fn list_with_config(
    workspace_dir: &Path,
    config_dir: Option<PathBuf>,
) -> Result<Vec<TemplateSummary>> {
    let mut templates = BTreeMap::new();
    let workspace = workspace_root(workspace_dir)?;
    if let Some(root) = template_root(&workspace, [".cooplet", "agents"])? {
        list_in_root(&workspace, &root, &mut templates)?;
    }
    if let Some(config) = config_root(config_dir)?
        && let Some(root) = template_root(&config, ["cooplet", "agents"])?
    {
        list_in_root(&config, &root, &mut templates)?;
    }
    Ok(templates.into_values().flatten().collect())
}

fn workspace_root(path: &Path) -> Result<PathBuf> {
    let root = fs::canonicalize(path).map_err(|error| {
        invalid_labeled(
            path,
            "workspace",
            "could not resolve workspace directory",
            error,
        )
    })?;
    require_directory_labeled(root, "workspace")
}

fn config_root(path: Option<PathBuf>) -> Result<Option<PathBuf>> {
    let Some(path) = path else { return Ok(None) };
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(invalid_labeled(
                &path,
                "config",
                "could not inspect template config directory",
                error,
            ));
        }
    }
    let canonical = fs::canonicalize(&path).map_err(|error| {
        invalid_labeled(
            &path,
            "config",
            "could not resolve template config directory",
            error,
        )
    })?;
    require_directory_labeled(canonical, "config").map(Some)
}

fn template_root(base: &Path, components: [&str; 2]) -> Result<Option<PathBuf>> {
    let mut path = base.to_path_buf();
    for component in components {
        let Some(canonical) = existing_canonical(&path.join(component), Some(base))? else {
            return Ok(None);
        };
        path = require_directory_labeled(canonical, component)?;
    }
    Ok(Some(path))
}

fn valid_template_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && !matches!(id, "." | "..")
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn load_in_root(
    base: &Path,
    search_root: &Path,
    id: &str,
    load_instructions: bool,
) -> Result<Option<AgentTemplate>> {
    let Some(dir) = existing_canonical(&search_root.join(id), Some(base))? else {
        return Ok(None);
    };
    let dir = require_directory_labeled(dir, id)?;
    let manifest_path = dir.join("agent.json");
    let path = existing_canonical(&manifest_path, Some(&dir))?
        .ok_or_else(|| invalid_static(&manifest_path, "agent.json", "manifest is missing"))?;
    let bytes = read_file(base, &path, "agent.json", MAX_MANIFEST_BYTES)?;
    if bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        != Some(b'{')
    {
        return Err(invalid_static(
            &path,
            "agent.json",
            "expected a JSON object",
        ));
    }

    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| invalid_labeled(&path, "agent.json", "invalid manifest JSON", error))?;
    if manifest.schema != 1 || manifest.client.trim().is_empty() {
        return Err(invalid_static(
            &path,
            "agent.json",
            "expected schema 1 and a nonempty client kind",
        ));
    }
    let instructions = if load_instructions {
        read_instructions(base, &dir, manifest.instructions_file.as_deref())?
    } else {
        None
    };
    Ok(Some(AgentTemplate {
        client: manifest.client,
        title: manifest.title,
        description: manifest.description,
        options: AgentOverrides {
            model: manifest.model,
            effort: manifest.effort,
            approval: manifest.approval,
            sandbox: manifest.sandbox,
        },
        instructions,
    }))
}

fn read_instructions(base: &Path, dir: &Path, explicit: Option<&str>) -> Result<Option<String>> {
    let relative = Path::new(explicit.unwrap_or("instructions.md"));
    if relative.as_os_str().is_empty()
        || !relative
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(invalid_static(
            &dir.join("agent.json"),
            "agent.json",
            "instructions_file must be a relative path inside the template",
        ));
    }

    let path = dir.join(relative);
    let label = file_label(&path);
    let canonical = resolve_instructions_path(dir, relative)?;
    if let Some(file) = canonical {
        let bytes = read_file(base, &file, label, MAX_INSTRUCTIONS_BYTES)?;
        return String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| invalid_static(&file, label, "expected UTF-8 instructions"));
    }
    if explicit.is_some() {
        return Err(invalid_static(&path, label, "instructions file is missing"));
    }
    Ok(None)
}

fn resolve_instructions_path(dir: &Path, relative: &Path) -> Result<Option<PathBuf>> {
    let mut path = dir.to_path_buf();
    let mut parts = relative.components().peekable();
    while let Some(part) = parts.next() {
        let Some(canonical) = existing_canonical(&path.join(part), Some(dir))? else {
            return Ok(None);
        };
        path = if parts.peek().is_some() {
            let label = part.as_os_str().to_str().unwrap_or("template");
            require_directory_labeled(canonical, label)?
        } else {
            canonical
        };
    }
    Ok(Some(path))
}

fn list_in_root(
    base: &Path,
    root: &Path,
    templates: &mut BTreeMap<String, Option<TemplateSummary>>,
) -> Result<()> {
    for entry in fs::read_dir(root).map_err(|error| {
        invalid_labeled(root, "agents", "could not read template directory", error)
    })? {
        let entry = entry.map_err(|error| {
            invalid_labeled(root, "agents", "could not read template directory", error)
        })?;
        let Ok(id) = entry.file_name().into_string() else {
            continue;
        };
        if !valid_template_id(&id) || templates.contains_key(&id) {
            continue;
        }
        // A regular file is not a template. Its name still shadows the fallback
        // root because an explicit lookup would fail at this path.
        if entry
            .file_type()
            .map_err(|error| invalid_io(&entry.path(), "could not inspect template entry", error))?
            .is_file()
        {
            templates.insert(id, None);
            continue;
        }
        let Some(template) = load_in_root(base, root, &id, true)? else {
            continue;
        };
        templates.insert(
            id.clone(),
            Some(TemplateSummary {
                template_id: id.clone(),
                title: template.title.unwrap_or(id),
                description: template.description,
                client_kind: template.client,
            }),
        );
    }
    Ok(())
}

fn existing_canonical(path: &Path, inside: Option<&Path>) -> Result<Option<PathBuf>> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(invalid_io(path, "could not inspect template path", error)),
    }

    let canonical = fs::canonicalize(path)
        .map_err(|error| invalid_io(path, "could not resolve template path", error))?;
    if inside.is_some_and(|root| !canonical.starts_with(root)) {
        return Err(invalid(path, "path resolves outside its template root"));
    }
    Ok(Some(canonical))
}

fn require_directory_labeled(path: PathBuf, label: &str) -> Result<PathBuf> {
    let metadata = fs::metadata(&path).map_err(|error| {
        invalid_labeled(&path, label, "could not inspect template directory", error)
    })?;
    if !metadata.is_dir() {
        return Err(invalid_static(&path, label, "expected a directory"));
    }
    Ok(path)
}

fn read_file(base: &Path, path: &Path, label: &str, limit: usize) -> Result<Vec<u8>> {
    let relative = path
        .strip_prefix(base)
        .map_err(|_| invalid_static(path, label, "path resolves outside its template root"))?;
    let file = PrivateFs::open_confined_regular_file(base, relative, None)
        .map_err(|error| invalid_labeled(path, label, "could not read template file", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| invalid_labeled(path, label, "could not read template file", error))?;
    if metadata.len() > limit as u64 {
        return Err(invalid_static(
            path,
            label,
            &format!("file exceeds {limit} bytes"),
        ));
    }

    let mut bytes = Vec::new();
    file.take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| invalid_labeled(path, label, "could not read template file", error))?;

    if bytes.len() > limit {
        return Err(invalid_static(
            path,
            label,
            &format!("file exceeds {limit} bytes"),
        ));
    }
    Ok(bytes)
}

fn file_label(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .unwrap_or("template")
}

fn invalid_static(path: &Path, label: &str, reason: &str) -> NodeError {
    tracing::warn!(path = %path.display(), reason, "Invalid agent template");
    NodeError::new(
        ErrorCode::InvalidRequest,
        format!("Agent template ({label}): {reason}"),
    )
    .with_extension("reason", "invalid_template")
}

fn invalid(path: &Path, reason: &str) -> NodeError {
    invalid_static(path, file_label(path), reason)
}

fn invalid_labeled(path: &Path, label: &str, reason: &str, error: impl fmt::Display) -> NodeError {
    tracing::warn!(path = %path.display(), %error, reason, "Invalid agent template");
    NodeError::new(
        ErrorCode::InvalidRequest,
        format!("Agent template ({label}): {reason}"),
    )
    .with_extension("reason", "invalid_template")
}

fn invalid_io(path: &Path, reason: &str, error: impl fmt::Display) -> NodeError {
    invalid_labeled(path, file_label(path), reason, error)
}

#[cfg(test)]
mod tests;
