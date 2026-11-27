use std::{error::Error, fs, os::unix::fs::symlink};

use cooplet_protocol::ParticipantId;

use super::*;

type TestResult = std::result::Result<(), Box<dyn Error>>;

struct Tree(PathBuf);

impl Tree {
    fn new() -> std::result::Result<Self, Box<dyn Error>> {
        let path =
            std::env::temp_dir().join(format!("cooplet-templates-{}", ParticipantId::generate()?));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn workspace(&self) -> PathBuf {
        self.0.join("workspace")
    }

    fn config(&self) -> PathBuf {
        self.0.join("config")
    }

    fn template(&self, name: &str, manifest: &[u8]) -> std::result::Result<PathBuf, io::Error> {
        write_template(
            &self.workspace().join(".cooplet/agents").join(name),
            manifest,
        )
    }

    fn user_template(
        &self,
        name: &str,
        manifest: &[u8],
    ) -> std::result::Result<PathBuf, io::Error> {
        write_template(&self.config().join("cooplet/agents").join(name), manifest)
    }

    fn load(&self, id: &str) -> Result<AgentTemplate> {
        load_with_config(&self.workspace(), id, || Some(self.config()))
    }

    fn list(&self) -> Result<Vec<TemplateSummary>> {
        list_with_config(&self.workspace(), Some(self.config()))
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write_template(path: &Path, manifest: &[u8]) -> std::result::Result<PathBuf, io::Error> {
    fs::create_dir_all(path)?;
    fs::write(path.join("agent.json"), manifest)?;
    Ok(path.to_path_buf())
}

#[test]
fn discovers_workspace_and_user_templates_with_overrides() -> TestResult {
    let tree = Tree::new()?;
    let local = tree.template(
        "task.main",
        br#"{"schema":1,"client":"local","model":"original","effort":"low","instructions_file":"notes/task.md"}"#,
    )?;
    fs::create_dir(local.join("notes"))?;
    fs::write(local.join("notes/task.md"), "Instructions\n")?;
    tree.user_template("task.main", br#"{"schema":1,"client":"shadow"}"#)?;
    let user = tree.user_template(
        "user",
        br#"{"schema":1,"client":"user","title":"User template"}"#,
    )?;
    fs::write(user.join("instructions.md"), "Use the defaults")?;

    let local = tree.load("task.main")?;
    assert_eq!(local.client, "local");
    assert_eq!(local.instructions.as_deref(), Some("Instructions\n"));
    let effective = local.options.merged(&AgentOverrides {
        effort: Some("high".into()),
        ..AgentOverrides::default()
    });
    assert_eq!(effective.model.as_deref(), Some("original"));
    assert_eq!(effective.effort.as_deref(), Some("high"));
    assert_eq!(
        tree.load("user")?.instructions.as_deref(),
        Some("Use the defaults")
    );

    fs::write(
        tree.workspace().join(".cooplet/agents/README.md"),
        "Agent catalog",
    )?;
    tree.user_template("README.md", br#"{"schema":1,"client":"shadowed"}"#)?;
    assert!(tree.load("README.md").is_err());
    let listed = tree.list()?;
    assert_eq!(
        listed
            .iter()
            .map(|template| (
                template.template_id.as_str(),
                template.title.as_str(),
                template.client_kind.as_str()
            ))
            .collect::<Vec<_>>(),
        [
            ("task.main", "task.main", "local"),
            ("user", "User template", "user")
        ],
    );
    Ok(())
}

#[test]
fn damaged_workspace_template_never_falls_back() -> TestResult {
    let tree = Tree::new()?;
    let local = tree.template("user", br#"{"schema":1,"client":"local"}"#)?;
    tree.user_template("user", br#"{"schema":1,"client":"user"}"#)?;
    fs::remove_file(local.join("agent.json"))?;
    assert!(tree.load("user").is_err_and(|error| {
        error.0.detail.contains("agent.json")
            && !error
                .0
                .detail
                .contains(&tree.0.to_string_lossy().into_owned())
    }));
    assert!(tree.list().is_err());

    fs::remove_dir(local)?;
    assert_eq!(tree.load("user")?.client, "user");

    // Malformed JSON, missing workspace, and non-directory config root never leak host paths or OS/serde errors.
    let broken = tree.template("broken", b"{not-valid-json")?;
    let json_err = tree
        .load("broken")
        .err()
        .ok_or("expected malformed JSON error")?;
    assert_eq!(
        json_err.0.detail,
        "Agent template (agent.json): invalid manifest JSON"
    );
    assert!(
        !json_err
            .0
            .detail
            .contains(&tree.0.to_string_lossy().into_owned())
    );
    fs::remove_dir_all(broken)?;

    let missing_ws = tree.0.join("secret-missing-workspace");
    let ws_err = load_with_config(&missing_ws, "user", || Some(tree.config()))
        .err()
        .ok_or("expected missing workspace error")?;
    assert_eq!(
        ws_err.0.detail,
        "Agent template (workspace): could not resolve workspace directory"
    );
    assert!(!ws_err.0.detail.contains("secret-missing-workspace"));
    assert!(!ws_err.0.detail.contains("os error"));

    let bad_config = tree.0.join("secret-home-file");
    fs::write(&bad_config, b"not-a-dir")?;
    let cfg_err = load_with_config(&tree.workspace(), "absent", || Some(bad_config.clone()))
        .err()
        .ok_or("expected non-directory config error")?;
    assert_eq!(
        cfg_err.0.detail,
        "Agent template (config): expected a directory"
    );
    assert!(!cfg_err.0.detail.contains("secret-home-file"));

    // In-root symlink to the workspace root itself must report logical "agent.json", never the workspace basename.
    let secret_ws = tree.0.join("secret-repo-name");
    let secret_agents = secret_ws.join(".cooplet/agents");
    fs::create_dir_all(&secret_agents)?;
    symlink(&secret_ws, secret_agents.join("loop"))?;
    symlink(&secret_ws, secret_ws.join("agent.json"))?;
    let loop_err = load_with_config(&secret_ws, "loop", || None)
        .err()
        .ok_or("expected symlinked directory manifest error")?;
    assert_eq!(
        loop_err.0.detail,
        "Agent template (agent.json): could not read template file"
    );
    assert!(!loop_err.0.detail.contains("secret-repo-name"));
    Ok(())
}

#[test]
fn containment_applies_to_roots_and_every_file_component() -> TestResult {
    let tree = Tree::new()?;
    let local = tree.template("agent", br#"{"schema":1,"client":"local"}"#)?;
    let outside = tree.0.join("outside");
    fs::create_dir(&outside)?;
    fs::write(outside.join("instructions.md"), "outside instructions")?;

    symlink(
        outside.join("instructions.md"),
        local.join("instructions.md"),
    )?;
    assert!(
        tree.load("agent")
            .is_err_and(|error| error.0.detail.contains("outside"))
    );
    fs::remove_file(local.join("instructions.md"))?;

    symlink(&outside, local.join("notes"))?;
    fs::write(
        local.join("agent.json"),
        br#"{"schema":1,"client":"local","instructions_file":"notes/instructions.md"}"#,
    )?;
    assert!(
        tree.load("agent")
            .is_err_and(|error| error.0.detail.contains("outside"))
    );
    fs::remove_file(local.join("notes"))?;

    fs::remove_file(local.join("agent.json"))?;
    symlink(outside.join("absent"), local.join("agent.json"))?;
    assert!(
        tree.load("agent")
            .is_err_and(|error| error.0.detail.contains("agent.json"))
    );

    let agents = tree.workspace().join(".cooplet/agents");
    fs::remove_dir_all(&agents)?;
    symlink(&outside, agents)?;
    assert!(
        tree.load("agent")
            .is_err_and(|error| error.0.detail.contains("outside"))
    );
    assert!(tree.list().is_err());
    Ok(())
}

#[test]
fn validates_limits_ids_and_named_json() -> TestResult {
    let tree = Tree::new()?;
    let manifest = br#"{"schema":1,"client":"local"}"#;
    let path = tree.template("agent", manifest)?;

    for id in [
        "",
        ".",
        "..",
        "foreign/agent",
        "/abs",
        "a b",
        &"x".repeat(65),
    ] {
        assert!(
            tree.load(id)
                .is_err_and(|error| error.code() == ErrorCode::InvalidRequest)
        );
    }
    for bad_manifest in [
        br#"[1,"local"]"#.as_slice(),
        br#"{"schema":2,"client":"local"}"#,
        br#"{"schema":1,"client":"local","allowed_overrides":{}}"#,
        br#"{"schema":1,"client":"a","client":"b"}"#,
        br#"{"schema":1,"client":"local","instructions_file":"missing.md"}"#,
        br#"{"schema":1,"client":"local","instructions_file":"../secret"}"#,
        br#"{"schema":1,"client":"local","instructions_file":"/tmp/secret"}"#,
    ] {
        fs::write(path.join("agent.json"), bad_manifest)?;
        assert!(tree.load("agent").is_err());
    }

    let mut padded = manifest.to_vec();
    padded.resize(MAX_MANIFEST_BYTES, b' ');
    fs::write(path.join("agent.json"), &padded)?;
    fs::write(
        path.join("instructions.md"),
        vec![b'a'; MAX_INSTRUCTIONS_BYTES],
    )?;
    assert_eq!(
        tree.load("agent")?.instructions.as_ref().map(String::len),
        Some(MAX_INSTRUCTIONS_BYTES)
    );

    fs::write(
        path.join("instructions.md"),
        vec![b'a'; MAX_INSTRUCTIONS_BYTES + 1],
    )?;
    assert!(
        tree.load("agent")
            .is_err_and(|error| error.0.detail.contains("instructions.md"))
    );
    padded.push(b' ');
    fs::write(path.join("agent.json"), padded)?;
    assert!(
        tree.load("agent")
            .is_err_and(|error| error.0.detail.contains("agent.json"))
    );

    for bad_overrides in [
        r#"["model","effort","approval","sandbox"]"#,
        r#"{"model":"first","model":"second"}"#,
        r#"{"command":"unrecognized"}"#,
    ] {
        assert!(serde_json::from_str::<AgentOverrides>(bad_overrides).is_err());
    }
    Ok(())
}

#[test]
fn roster_uses_only_manifest_metadata_and_skips_invalid_templates() -> TestResult {
    let tree = Tree::new()?;
    tree.template(
        "draft",
        br#"{"schema":1,"client":"mock","instructions_file":"missing.md"}"#,
    )?;
    tree.template("broken", b"not a manifest")?;
    tree.user_template("broken", br#"{"schema":1,"client":"mock"}"#)?;
    tree.template(
        "blank-title",
        br#"{"schema":1,"client":"mock","title":" "}"#,
    )?;
    assert!(tree.list().is_err());
    let summary = list_with_mode(&tree.workspace(), Some(tree.config()), ListMode::Roster)?;
    assert_eq!(summary.len(), 1);
    assert_eq!(summary[0].template_id, "draft");
    Ok(())
}
