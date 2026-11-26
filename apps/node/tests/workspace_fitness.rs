use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;
use toml::Value;

type TestResult = Result<(), Box<dyn Error>>;
type PolicyResult = Result<(), String>;

const PROTOCOL: &str = "cooplet-protocol";
const PLATFORM: &str = "cooplet-platform";
const STORAGE: &str = "cooplet-storage";
const CLIENT: &str = "cooplet-client";
const NODE: &str = "cooplet-node";
const HUB: &str = "cooplet-hub";
const TELEGRAM: &str = "cooplet-telegram";
const HISTORY: &str = "cooplet-history";

const MEMBERS: [(&str, &str); 8] = [
    (PROTOCOL, "packages/cooplet-protocol"),
    (PLATFORM, "packages/cooplet-platform"),
    (STORAGE, "packages/cooplet-storage"),
    (CLIENT, "packages/cooplet-client"),
    (NODE, "apps/node"),
    (HUB, "apps/hub"),
    (TELEGRAM, "apps/telegram"),
    (HISTORY, "apps/history"),
];

const CLIPPY_DENIES: [&str; 8] = [
    "unwrap_used",
    "expect_used",
    "panic",
    "todo",
    "unimplemented",
    "await_holding_lock",
    "dbg_macro",
    "exit",
];

#[derive(Deserialize)]
struct Metadata {
    workspace_root: PathBuf,
    workspace_members: Vec<String>,
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    version: String,
    manifest_path: PathBuf,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    // Cargo reports the actual package name here, even for a renamed dependency.
    name: String,
    path: Option<PathBuf>,
    kind: Option<DependencyKind>,
    target: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum DependencyKind {
    Dev,
    Build,
}

#[derive(Deserialize)]
struct Lockfile {
    package: Vec<LockedPackage>,
}

#[derive(Deserialize)]
struct LockedPackage {
    name: String,
    version: String,
    source: Option<String>,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn metadata(root: &Path) -> Result<Metadata, Box<dyn Error>> {
    let output = Command::new(env!("CARGO"))
        .current_dir(root)
        .args([
            "metadata",
            "--manifest-path",
            "Cargo.toml",
            "--no-deps",
            "--format-version",
            "1",
            "--locked",
            "--offline",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn read_toml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Box<dyn Error>> {
    let input = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    toml::from_str(&input)
        .map_err(|error| format!("cannot parse {}: {error}", path.display()).into())
}

#[test]
fn gate_crate_dag() -> TestResult {
    let root = workspace_root().canonicalize()?;
    let graph = metadata(&root)?;
    validate_dag(&graph, &root)?;
    validate_local_lock_packages(&graph, &read_toml(&root.join("Cargo.lock"))?)?;
    reject_path_overrides(&root)?;
    Ok(())
}

fn validate_dag(graph: &Metadata, root: &Path) -> PolicyResult {
    if graph.workspace_root != root {
        return Err(format!(
            "unexpected Cargo workspace root {}",
            graph.workspace_root.display()
        ));
    }

    let expected: BTreeSet<_> = MEMBERS
        .iter()
        .map(|(name, path)| (*name, root.join(path).join("Cargo.toml")))
        .collect();
    let actual: BTreeSet<_> = graph
        .packages
        .iter()
        .map(|package| (package.name.as_str(), package.manifest_path.clone()))
        .collect();
    let member_ids: BTreeSet<_> = graph.workspace_members.iter().collect();
    let package_ids: BTreeSet<_> = graph.packages.iter().map(|package| &package.id).collect();
    if actual != expected
        || graph.packages.len() != MEMBERS.len()
        || graph.workspace_members.len() != MEMBERS.len()
        || member_ids.len() != MEMBERS.len()
        || member_ids != package_ids
    {
        return Err(format!(
            "unexpected workspace members: {actual:?}; expected {expected:?}"
        ));
    }

    for package in &graph.packages {
        for dependency in &package.dependencies {
            validate_dependency(&package.name, dependency, root)?;
        }
    }
    Ok(())
}

fn validate_dependency(from: &str, dependency: &Dependency, root: &Path) -> PolicyResult {
    let to = dependency.name.as_str();
    let kind = match dependency.kind {
        None => "normal",
        Some(DependencyKind::Dev) => "dev",
        Some(DependencyKind::Build) => "build",
    };
    let fail = |reason: &str| {
        let target = dependency.target.as_deref().unwrap_or("all targets");
        format!("{from} -> {to} ({kind}, {target}): {reason}")
    };
    let member = MEMBERS.iter().find(|(name, _)| *name == to);
    if let Some(path) = &dependency.path {
        let canonical = path.canonicalize().map_err(|error| {
            fail(&format!(
                "cannot resolve local path {}: {error}",
                path.display()
            ))
        })?;
        let matches_member = member.is_some_and(|(_, expected)| {
            path.starts_with(root) && canonical == root.join(expected)
        });
        if !matches_member {
            return Err(fail(&format!(
                "local dependency {} does not match a registered workspace member",
                path.display()
            )));
        }
        let allowed: &[&str] = match from {
            STORAGE => &[PROTOCOL, PLATFORM],
            CLIENT => &[PROTOCOL],
            NODE => &[PROTOCOL, PLATFORM, STORAGE, CLIENT],
            HUB => &[PROTOCOL, PLATFORM, STORAGE],
            TELEGRAM | HISTORY => &[PROTOCOL, PLATFORM, CLIENT],
            _ => &[],
        };
        let hub_dev_client =
            from == HUB && to == CLIENT && dependency.kind == Some(DependencyKind::Dev);
        if !allowed.contains(&to) && !hub_dev_client {
            return Err(fail("forbidden internal dependency"));
        }
    } else {
        if member.is_some() {
            return Err(fail("internal dependency must use its workspace path"));
        }
        let forbidden: &[&str] = match from {
            PROTOCOL => &["tokio", "rusqlite", "axum", "reqwest", "libc"],
            CLIENT => &["rusqlite", "axum"],
            _ => &[],
        };
        if forbidden.contains(&to) {
            return Err(fail("forbidden external dependency"));
        }
    }
    Ok(())
}

fn validate_local_lock_packages(graph: &Metadata, lockfile: &Lockfile) -> PolicyResult {
    let mut expected: BTreeSet<_> = graph
        .packages
        .iter()
        .map(|package| (package.name.as_str(), package.version.as_str()))
        .collect();

    for package in lockfile
        .package
        .iter()
        .filter(|package| package.source.is_none())
    {
        if !expected.remove(&(package.name.as_str(), package.version.as_str())) {
            return Err(format!(
                "unexpected local package in Cargo.lock: {} {}",
                package.name, package.version
            ));
        }
    }
    if !expected.is_empty() {
        return Err(format!(
            "missing local packages in Cargo.lock: {expected:?}"
        ));
    }
    Ok(())
}

fn reject_path_overrides(root: &Path) -> TestResult {
    let manifest: Value = read_toml(&root.join("Cargo.toml"))?;
    reject_override_tables(&manifest, "Cargo.toml")?;
    for file in [".cargo/config", ".cargo/config.toml"] {
        let path = root.join(file);
        match path.symlink_metadata() {
            Ok(_) => {
                let config: Value = read_toml(&path)?;
                reject_override_tables(&config, file)?;
                if config.get("paths").is_some() {
                    return Err(format!("{file}: Cargo path overrides are forbidden").into());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn reject_override_tables(document: &Value, file: &str) -> PolicyResult {
    for key in ["patch", "replace"] {
        if document.get(key).is_some_and(has_path_entry) {
            return Err(format!(
                "{file}: path overrides under [{key}] are forbidden"
            ));
        }
    }
    Ok(())
}

fn has_path_entry(value: &Value) -> bool {
    value.as_table().is_some_and(|table| {
        table.get("path").is_some_and(Value::is_str) || table.values().any(has_path_entry)
    })
}

#[test]
fn gate_banned_crates() -> TestResult {
    validate_banned_crates(&read_toml(&workspace_root().join("Cargo.lock"))?)?;
    Ok(())
}

fn validate_banned_crates(lockfile: &Lockfile) -> PolicyResult {
    const FAMILIES: [&str; 6] = [
        "rquickjs",
        "openssl",
        "native-tls",
        "cxx",
        "aws-lc-sys",
        "aws-lc-fips-sys",
    ];
    const NAMES: [&str; 4] = ["cmake", "clang-sys", "bindgen", "r2d2"];

    let mut banned = Vec::new();
    for package in &lockfile.package {
        if NAMES.contains(&package.name.as_str())
            || FAMILIES
                .iter()
                .any(|prefix| package.name.starts_with(prefix))
        {
            banned.push(format!("{} {}", package.name, package.version));
        }
    }
    if !banned.is_empty() {
        return Err(format!(
            "banned crates in Cargo.lock: {}",
            banned.join(", ")
        ));
    }
    Ok(())
}

#[test]
fn gate_unsafe_and_clippy() -> TestResult {
    let root = workspace_root();
    let manifest: Value = read_toml(&root.join("Cargo.toml"))?;
    validate_workspace_policy(&manifest)?;
    for (name, path) in MEMBERS {
        let manifest: Value = read_toml(&root.join(path).join("Cargo.toml"))?;
        validate_member_lints(name, &manifest)?;
    }
    let clippy_config: Value = read_toml(&root.join("clippy.toml"))?;
    for option in ["allow-unwrap-in-consts", "allow-expect-in-consts"] {
        assert_eq!(
            value_at(&clippy_config, &[option]).and_then(Value::as_bool),
            Some(false),
            "clippy.toml must set {option} = false"
        );
    }
    Ok(())
}

fn validate_workspace_policy(manifest: &Value) -> PolicyResult {
    let expected_members: BTreeSet<_> = MEMBERS.iter().map(|(_, path)| *path).collect();
    let entries = value_at(manifest, &["workspace", "members"])
        .and_then(Value::as_array)
        .ok_or("Cargo.toml: workspace.members must list the explicit member paths")?;
    let actual_members: BTreeSet<_> = entries.iter().filter_map(Value::as_str).collect();
    if entries.len() != MEMBERS.len() || actual_members != expected_members {
        return Err("Cargo.toml: workspace.members must list the explicit member paths".into());
    }
    if value_at(manifest, &["workspace", "lints", "rust", "unsafe_code"]).and_then(Value::as_str)
        != Some("forbid")
    {
        return Err("Cargo.toml: workspace.lints.rust.unsafe_code must be forbid".into());
    }
    validate_clippy_denies(
        "Cargo.toml workspace",
        value_at(manifest, &["workspace", "lints", "clippy"]),
        &CLIPPY_DENIES,
    )
}

fn validate_member_lints(name: &str, manifest: &Value) -> PolicyResult {
    if name == PLATFORM {
        let clippy = value_at(manifest, &["lints", "clippy"]);
        validate_clippy_denies(name, clippy, &CLIPPY_DENIES)?;
        validate_clippy_denies(name, clippy, &["undocumented_unsafe_blocks"])?;
    } else if value_at(manifest, &["lints", "workspace"]).and_then(Value::as_bool) != Some(true) {
        return Err(format!("{name}: must inherit the workspace lints"));
    }
    Ok(())
}

fn validate_clippy_denies(owner: &str, clippy: Option<&Value>, required: &[&str]) -> PolicyResult {
    for lint in required {
        let level = clippy.and_then(|table| table.get(lint)).and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("level").and_then(Value::as_str))
        });
        if level != Some("deny") {
            return Err(format!("{owner}: clippy::{lint} must be deny"));
        }
    }
    Ok(())
}

fn value_at<'a>(document: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(document, |value, key| value.get(key))
}

#[test]
fn rejects_forbidden_internal_edges() -> TestResult {
    let root = workspace_root().canonicalize()?;
    for (from, to, path) in [
        (TELEGRAM, STORAGE, "packages/cooplet-storage"),
        (HISTORY, STORAGE, "packages/cooplet-storage"),
        (NODE, HUB, "apps/hub"),
        (PROTOCOL, NODE, "apps/node"),
        (CLIENT, STORAGE, "packages/cooplet-storage"),
    ] {
        // Cargo emits all declared edges, including optional and inactive paths.
        let dependency: Dependency = serde_json::from_value(serde_json::json!({
            "name": to,
            "rename": "renamed-dependency",
            "path": root.join(path),
            "kind": "build",
            "optional": true,
            "target": "cfg(target_os = \"none\")"
        }))?;
        assert_eq!(
            validate_dependency(from, &dependency, &root),
            Err(format!(
                "{from} -> {to} (build, cfg(target_os = \"none\")): forbidden internal dependency"
            ))
        );
    }
    Ok(())
}

#[test]
fn hub_can_use_client_only_in_dev_dependencies() -> TestResult {
    let root = workspace_root().canonicalize()?;
    let mut dependency = Dependency {
        name: CLIENT.into(),
        path: Some(root.join("packages/cooplet-client")),
        kind: Some(DependencyKind::Dev),
        target: None,
    };
    validate_dependency(HUB, &dependency, &root)?;
    for (kind, target) in [
        (None, None),
        (Some(DependencyKind::Build), None),
        (None, Some("cfg(target_os = \"linux\")")),
    ] {
        dependency.kind = kind;
        dependency.target = target.map(str::to_owned);
        let result = validate_dependency(HUB, &dependency, &root);
        assert!(result.is_err_and(|error| error.contains("forbidden internal dependency")));
    }
    Ok(())
}

#[test]
fn rejects_dependency_identity_mismatches() -> TestResult {
    let root = workspace_root().canonicalize()?;
    for (name, path, diagnostic) in [
        ("tokio", None, "forbidden external dependency"),
        ("axum", None, "forbidden external dependency"),
        (
            CLIENT,
            None,
            "internal dependency must use its workspace path",
        ),
        (
            CLIENT,
            Some("packages/cooplet-protocol"),
            "does not match a registered workspace member",
        ),
        (
            "unregistered",
            Some("contracts"),
            "does not match a registered workspace member",
        ),
        (
            CLIENT,
            Some(".."),
            "does not match a registered workspace member",
        ),
    ] {
        let dependency: Dependency = serde_json::from_value(serde_json::json!({
            "name": name,
            "rename": "harmless-alias",
            "path": path.map(|path| root.join(path)),
        }))?;
        let result = validate_dependency(PROTOCOL, &dependency, &root);
        assert!(
            result.is_err_and(|error| error.contains(diagnostic)),
            "{name} {path:?}"
        );
    }
    Ok(())
}

#[test]
fn rejects_unregistered_local_lock_packages_and_path_overrides() -> TestResult {
    let root = workspace_root().canonicalize()?;
    let graph = metadata(&root)?;
    let mut lock: Lockfile = read_toml(&root.join("Cargo.lock"))?;
    lock.package.push(LockedPackage {
        name: "local-helper".into(),
        version: "0.1.0".into(),
        source: None,
    });
    assert_eq!(
        validate_local_lock_packages(&graph, &lock),
        Err("unexpected local package in Cargo.lock: local-helper 0.1.0".into())
    );
    let patch = toml::from_str("[patch.crates-io]\nserde = { path = 'local-helper' }")?;
    assert_eq!(
        reject_override_tables(&patch, ".cargo/config.toml"),
        Err(".cargo/config.toml: path overrides under [patch] are forbidden".into())
    );
    Ok(())
}

#[test]
fn rejects_banned_names_in_resolved_lockfile() -> TestResult {
    let input = r#"
version = 4

[[package]]
name = "cc"
version = "1.2.3"

[[package]]
name = "ring"
version = "0.17.0"
"#;
    validate_banned_crates(&toml::from_str(input)?)?;

    for name in [
        "rquickjs",
        "rquickjs-sys",
        "openssl-sys",
        "openssl",
        "native-tls",
        "native-tls-crate",
        "cmake",
        "cxx",
        "cxx-build",
        "cxxbridge-cmd",
        "aws-lc-sys",
        "aws-lc-fips-sys",
        "clang-sys",
        "bindgen",
        "r2d2",
    ] {
        let input = format!("{input}\n[[package]]\nname = \"{name}\"\nversion = \"1.0.0\"\n");
        assert_eq!(
            validate_banned_crates(&toml::from_str(&input)?),
            Err(format!("banned crates in Cargo.lock: {name} 1.0.0"))
        );
    }
    assert!(toml::from_str::<Lockfile>("version = 4\n[[package]]\nname = 'invalid'").is_err());
    Ok(())
}

#[test]
fn rejects_missing_lint_settings() -> TestResult {
    let root = workspace_root();
    let mut workspace: Value = read_toml(&root.join("Cargo.toml"))?;
    workspace["workspace"]["lints"]["clippy"]["unwrap_used"] = Value::String("warn".into());
    assert_eq!(
        validate_workspace_policy(&workspace),
        Err("Cargo.toml workspace: clippy::unwrap_used must be deny".into())
    );
    let member = toml::from_str("[lints]\nworkspace = false")?;
    assert_eq!(
        validate_member_lints(PROTOCOL, &member),
        Err("cooplet-protocol: must inherit the workspace lints".into())
    );
    let mut platform: Value = read_toml(&root.join("packages/cooplet-platform/Cargo.toml"))?;
    platform["lints"]["clippy"]["undocumented_unsafe_blocks"] = Value::String("warn".into());
    assert_eq!(
        validate_member_lints(PLATFORM, &platform),
        Err("cooplet-platform: clippy::undocumented_unsafe_blocks must be deny".into())
    );
    Ok(())
}
