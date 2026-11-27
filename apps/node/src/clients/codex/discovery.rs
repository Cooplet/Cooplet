use std::{
    env, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use cooplet_platform::{ProcessHandle, SpawnError, TerminateOutcome};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::unix::pipe::{Receiver as ProbeOutput, Sender as ProbeInput},
    time::{sleep, timeout},
};

use super::{CAPABILITIES, KIND};
use crate::clients::DiscoveryReport;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const HELP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROBE_BYTES: u64 = 128 * 1024;

pub(super) fn executable(override_path: Option<&Path>) -> Option<PathBuf> {
    fn checked(path: &Path) -> Option<PathBuf> {
        let absolute = path.canonicalize().ok()?;
        let file = absolute.metadata().ok()?;
        (file.is_file() && file.permissions().mode() & 0o111 != 0).then_some(absolute)
    }

    match override_path {
        Some(path) => checked(path),
        None => env::var_os("PATH")
            .and_then(|path| env::split_paths(&path).find_map(|dir| checked(&dir.join("codex")))),
    }
}

pub(super) fn initialize_params() -> Value {
    json!({"clientInfo": {
        "name": "cooplet",
        "version": env!("CARGO_PKG_VERSION"),
    }})
}

pub(super) async fn discover(override_path: Option<&Path>) -> DiscoveryReport {
    let Some(binary) = executable(override_path) else {
        return report(
            None,
            None,
            Some(if override_path.is_some() {
                "COOPLET_CODEX_BIN does not point to an executable file"
            } else {
                "Codex was not found on PATH"
            }),
        );
    };
    if binary.to_str().is_none() {
        return report(None, None, Some("The Codex executable path is not UTF-8"));
    }

    let result = async {
        let app_help = help(&binary, &["app-server", "--help"]).await?;
        let cli_help = help(&binary, &["--help"]).await?;
        if !has_flag(&app_help, "--listen") || !has_flag(&cli_help, "--remote") {
            return Err("Codex lacks the required --listen or --remote flag");
        }

        let params = initialize_params();
        let mut cmd = Command::new(&binary);
        cmd.args(["app-server", "--stdio"]).stdin(Stdio::piped());
        run_probe(cmd, PROBE_TIMEOUT, false, |mut input, output| async move {
            let input = input
                .as_mut()
                .ok_or_else(|| io::Error::other("missing probe stdin"))?;
            let packet = json!({"id": 1, "method": "initialize", "params": params});
            input.write_all(packet.to_string().as_bytes()).await?;
            input.write_all(b"\n").await?;
            let mut line = Vec::new();
            BufReader::new(output.take(MAX_PROBE_BYTES))
                .read_until(b'\n', &mut line)
                .await?;
            let message: Value = serde_json::from_slice(&line)?;
            if message["id"] != 1
                || !message["result"]["codexHome"]
                    .as_str()
                    .is_some_and(|home| !home.is_empty())
            {
                return Err(io::Error::other("invalid initialize response"));
            }
            Ok(message["result"]["userAgent"]
                .as_str()
                .and_then(safe_version))
        })
        .await
        .ok_or("Codex did not complete the stdio initialize probe")
    }
    .await;
    match result {
        Ok(version) => report(Some(binary), version, None),
        Err(detail) => report(Some(binary), None, Some(detail)),
    }
}

fn report(
    binary_path: Option<PathBuf>,
    version: Option<String>,
    detail: Option<&str>,
) -> DiscoveryReport {
    DiscoveryReport {
        client_kind: KIND.into(),
        available: detail.is_none(),
        binary_path,
        version,
        capabilities: if detail.is_none() {
            CAPABILITIES
        } else {
            Default::default()
        },
        detail: detail.map(str::to_owned),
    }
}

fn safe_version(agent: &str) -> Option<String> {
    // The native User-Agent can carry OS and terminal details.
    let name = agent.split_ascii_whitespace().next()?;
    (name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-/+".contains(&b)))
    .then(|| name.to_owned())
}

fn has_flag(help: &str, flag: &str) -> bool {
    help.lines()
        .any(|line| line.trim_start().split_ascii_whitespace().next() == Some(flag))
}

async fn help(binary: &Path, args: &[&str]) -> Result<String, &'static str> {
    let mut command = Command::new(binary);
    command.args(args);
    run_probe(command, HELP_TIMEOUT, true, |_, output| async move {
        let mut help = String::new();
        output
            .take(MAX_PROBE_BYTES)
            .read_to_string(&mut help)
            .await?;
        Ok(help)
    })
    .await
    .ok_or("Could not read Codex CLI help")
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct WireFeatures {
    pub(super) output_schema: bool,
    pub(super) tool_output: bool,
}

pub(super) async fn wire_features(binary: &Path, directory: &Path) -> WireFeatures {
    let mut command = Command::new(binary);
    command
        .args(["app-server", "generate-json-schema", "--out"])
        .arg(directory);
    let generated = run_probe(command, PROBE_TIMEOUT, true, |_, output| async move {
        // Wait for command completion and bound output even if the CLI is broken.
        let bytes =
            tokio::io::copy(&mut output.take(MAX_PROBE_BYTES), &mut tokio::io::sink()).await?;
        if bytes == MAX_PROBE_BYTES {
            return Err(io::Error::other("schema generator output limit"));
        }
        Ok(())
    })
    .await
    .is_some();

    let schema_path = directory.join("v2/TurnStartParams.json");
    let schema = async {
        if !generated {
            return None;
        }
        let file = tokio::fs::File::open(schema_path).await.ok()?;
        let mut bytes = Vec::new();
        file.take(MAX_PROBE_BYTES)
            .read_to_end(&mut bytes)
            .await
            .ok()?;
        serde_json::from_slice::<Value>(&bytes).ok()
    }
    .await;
    let _ = tokio::fs::remove_dir_all(directory).await;
    let Some(schema) = schema else {
        return WireFeatures::default();
    };
    let tool = schema.pointer("/definitions/TurnToolOutput/properties");
    WireFeatures {
        output_schema: schema.pointer("/properties/outputSchema").is_some(),
        tool_output: schema.pointer("/properties/toolOutput").is_some()
            && tool.is_some_and(|v| {
                ["namespace", "name", "output", "retain"]
                    .into_iter()
                    .all(|field| v.get(field).is_some())
            }),
    }
}

async fn run_probe<T, F, Fut>(
    mut command: Command,
    deadline: Duration,
    require_success: bool,
    read: F,
) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce(Option<ProbeInput>, ProbeOutput) -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<T>> + Send,
{
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    // This task retains the process group if the caller abandons the probe.
    tokio::spawn(async move {
        let mut handle = tokio::task::spawn_blocking(move || {
            match ProcessHandle::spawn_isolated_group(&mut command) {
                Ok(handle) => Some(handle),
                Err(SpawnError::CleanupPending { mut cleanup, .. }) => {
                    let _ = cleanup.terminate_and_reap(Duration::ZERO);
                    None
                }
                Err(_) => None,
            }
        })
        .await
        .ok()??;
        let result = timeout(deadline, async {
            let input = handle
                .take_stdin()
                .map(|pipe| ProbeInput::from_owned_fd(pipe.into()))
                .transpose()
                .ok()?;
            let output = ProbeOutput::from_owned_fd(handle.take_stdout()?.into()).ok()?;
            let result = read(input, output).await.ok()?;
            if require_success {
                while !handle.observe_exit().ok()? {
                    sleep(Duration::from_millis(5)).await;
                }
            }
            Some(result)
        })
        .await
        .ok()
        .flatten();
        let clean = tokio::task::spawn_blocking(move || {
            handle
                .terminate_and_reap(Duration::from_millis(50))
                .is_ok_and(|result| {
                    result.outcome != TerminateOutcome::CleanupUnverified
                        && (!require_success
                            || result.leader_status.is_some_and(|status| status.success()))
                })
        })
        .await
        .ok()?;
        result.filter(|_| clean)
    })
    .await
    .ok()
    .flatten()
}
