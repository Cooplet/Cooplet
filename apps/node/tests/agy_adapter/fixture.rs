use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use super::TestResult;

const SESSION_ID: &str = "fixture-session";

#[test]
#[ignore = "runs as an ACP subprocess of the integration tests"]
fn acp_worker() -> TestResult {
    let root = PathBuf::from(env::var("AGY_FIXTURE_ROOT")?);
    let args: Vec<_> = env::var("AGY_FIXTURE_ARGS")?
        .split('\n')
        .map(str::to_owned)
        .collect();
    let mut transport = Fixture {
        trace: OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("wire.ndjson"))?,
        output: File::create("/dev/fd/3")?,
        root,
        mcp: Value::Null,
    };
    let managed = args
        .iter()
        .any(|arg| arg == "--dangerously-skip-permissions");
    transport.record(&json!({
        "pid": std::process::id(), "mode": if managed { "session" } else { "discovery" },
        "args": args, "inherited_home": env::var_os("HOME").map(|value| value.into_string().ok()),
        "cooplet_env_clean": env::vars_os().all(|(key, _)| !key.as_encoded_bytes().starts_with(b"COOPLET_"))
    }))?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut prompt = None;
    let mut duplex_next = false;
    let mut cancel_count = 0;
    let mut model = "default".to_owned();
    let mut effort = "standard".to_owned();
    loop {
        let received = if std::mem::take(&mut duplex_next) {
            let mut prefix = [0_u8; 256];
            input.read_exact(&mut prefix)?;
            if transport.mcp.is_object() {
                transport.send(&json!({
                    "jsonrpc":"2.0", "id":"duplex-request",
                    "method":"fs/read_text_file", "params":{}
                }))?;
            }
            let title = "x".repeat(32 * 1024);
            for _ in 0..12 {
                transport.update(
                    SESSION_ID,
                    json!({
                        "sessionUpdate":"session_info_update", "title":title
                    }),
                )?;
            }
            receive(&mut io::Cursor::new(prefix).chain(&mut input))?
        } else {
            receive(&mut input)?
        };
        let frame = match received {
            Some(value) => value,
            None => {
                transport.record(&json!({"stdin_eof": true}))?;
                return Ok(());
            }
        };
        transport.record(&frame)?;
        let id = &frame["id"];
        match frame["method"].as_str() {
            Some("initialize") => {
                let discovery = transport.mode("discovery-mode")?;
                if discovery == "hang" {
                    thread::sleep(Duration::from_secs(30));
                    return Ok(());
                }
                let mut caps = json!({
                    "loadSession": true,
                    "_meta": {"antigravity":{"systemPrompt":{"append":discovery != "no-append"}}}
                });
                if discovery != "load-only" {
                    caps["sessionCapabilities"] = json!({"resume":{}});
                }
                transport.reply(
                    id,
                    json!({
                        "protocolVersion": if discovery == "wrong-version" { 2 } else { 1 },
                        "agentInfo":{"version": "1.5.18"},
                        "agentCapabilities": caps
                    }),
                )?;
                duplex_next = managed && transport.mode("open-mode")? == "duplex";
            }
            Some("session/new" | "session/resume" | "session/load") => {
                transport.mcp = frame["params"]["mcpServers"][0].clone();
                if frame["method"] == "session/new" {
                    let gemini = arg_value(&args, "--gemini-dir")?;
                    let store = arg_value(&args, "--app-data-dir")?;
                    fs::create_dir_all(Path::new(gemini).join(store))?;
                    transport.reply(
                        id,
                        json!({"sessionId":SESSION_ID,"configOptions":options(&model,&effort)}),
                    )?;
                } else {
                    transport.noise()?;
                    transport.text(SESSION_ID, "agent_message_chunk", "historical output")?;
                    transport.reply(id, json!({}))?;
                }
                if transport.mode("open-mode")? == "idle-update" {
                    transport.wait_for_release()?;
                    transport.text(SESSION_ID, "agent_message_chunk", "idle-output")?;
                    transport.send(&json!({
                        "jsonrpc":"2.0", "id":"idle-barrier", "method":"fixture/barrier"
                    }))?;
                }

                let prompt_mode = transport.mode("prompt-mode")?;
                duplex_next = prompt_mode == "duplex";
                if prompt_mode == "partial" {
                    let mut prefix = [0_u8; 256];
                    input.read_exact(&mut prefix)?;
                    transport.record(&json!({"partial_prompt": true}))?;
                    return Ok(());
                }
            }
            Some("session/set_config_option") => {
                let value = frame["params"]["value"]
                    .as_str()
                    .ok_or("Missing config value")?;
                match frame["params"]["configId"].as_str() {
                    Some("model") if value == "chosen" => model = value.to_owned(),
                    Some("thought_level") if value == "high" => effort = value.to_owned(),
                    _ => {
                        transport.error(id, -32602)?;
                        continue;
                    }
                }
                transport.reply(id, json!({"configOptions":options(&model,&effort)}))?;
            }
            Some("session/prompt") => {
                let mode = transport.mode("prompt-mode")?;
                match mode.as_str() {
                    "hold" => {
                        transport.wait_for_release()?;
                        transport.terminal(id, "end_turn")?;
                    }
                    "cancel" | "end-on-cancel" | "cancel-timeout" | "cancel-on-retry" => {
                        cancel_count = 0;
                        prompt = Some((id.clone(), mode));
                    }
                    "reject" => transport.error(id, -32602)?,
                    "auth-error" => transport.error(id, -32000)?,
                    "rpc-error" => transport.error(id, -32603)?,
                    "eof" => return Ok(()),
                    "after-ack-eof" => {
                        transport.wait_for_release()?;
                        return Ok(());
                    }
                    "permission" | "after-ack-permission" => {
                        if mode == "after-ack-permission" {
                            transport.wait_for_release()?;
                        }
                        transport.send(&json!({
                            "jsonrpc":"2.0","id":id,"method":"session/request_permission","params":{}
                        }))?;
                        transport.record(&json!({"fault_sent":true}))?;
                    }
                    "text-limit" => {
                        transport.wait_for_release()?;
                        let text = "x".repeat(1024 * 1024);
                        transport.text(SESSION_ID, "agent_message_chunk", &text)?;
                        transport.text(SESSION_ID, "agent_message_chunk", &text)?;
                        transport.text(SESSION_ID, "agent_message_chunk", "overflow")?;
                        transport.record(&json!({"fault_sent":true}))?;
                    }
                    "frame-limit" => {
                        transport.wait_for_release()?;
                        transport
                            .output
                            .write_all(&vec![b'x'; 4 * 1024 * 1024 + 1])?;
                        transport.record(&json!({"fault_sent":true}))?;
                    }
                    "after-ack-rpc-error" => {
                        transport.wait_for_release()?;
                        transport.error(id, -32603)?;
                    }
                    "mcp" => {
                        let who = run_mcp(&transport.mcp)?;
                        transport.record(&json!({"whoami":who}))?;
                        transport.text(SESSION_ID, "agent_message_chunk", "MCP succeeded")?;
                        transport.terminal(id, "end_turn")?;
                    }
                    "noise" => {
                        transport.send(&json!({
                            "jsonrpc":"2.0","id":id,"method":"fs/read_text_file","params":{}
                        }))?;
                        transport.record(&json!({"server_reply":receive(&mut input)?}))?;
                        transport.reply(&json!(99999), json!({"stopReason":"end_turn"}))?;
                        transport.noise()?;
                        transport.text("other-session", "agent_message_chunk", "wrong-session")?;
                        transport.text(SESSION_ID, "agent_message_chunk", "first")?;
                        transport.text(SESSION_ID, "agent_message_chunk", " second")?;
                        transport.terminal(id, "end_turn")?;
                    }
                    _ => transport.terminal(id, "end_turn")?,
                }
            }
            Some("session/cancel") => {
                cancel_count += 1;
                if cancel_count == 1
                    && prompt
                        .as_ref()
                        .is_some_and(|(_, mode)| mode == "cancel-on-retry")
                {
                    continue;
                }
                let (id, mode) = prompt.take().ok_or("Cancel sent without a prompt")?;
                if mode == "cancel-timeout" {
                    transport.wait_for_release()?;
                }
                transport.terminal(
                    &id,
                    if mode == "end-on-cancel" {
                        "end_turn"
                    } else {
                        "cancelled"
                    },
                )?;
            }
            None if frame["id"] == "idle-barrier" && frame["error"]["code"] == -32601 => {}
            None if frame["id"] == "duplex-request" && frame["error"]["code"] == -32601 => {
                transport.record(&json!({"duplex_reply":true}))?;
            }
            _ => return Err("Unexpected request from Cooplet".into()),
        }
    }
}

struct Fixture {
    root: PathBuf,
    trace: File,
    output: File,
    mcp: Value,
}

impl Fixture {
    fn record(&mut self, frame: &Value) -> TestResult {
        let mut line = serde_json::to_vec(frame)?;
        line.push(b'\n');
        self.trace.write_all(&line)?;
        Ok(())
    }

    fn send(&mut self, frame: &Value) -> TestResult {
        send(&mut self.output, frame)
    }

    fn reply(&mut self, id: &Value, result: Value) -> TestResult {
        self.send(&json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    fn error(&mut self, id: &Value, code: i64) -> TestResult {
        eprintln!("fixture diagnostic redaction marker");
        self.send(&json!({"jsonrpc":"2.0","id":id,
            "error":{"code":code,"message":"fixture private marker","data":"fixture secret marker"}}))
    }

    fn terminal(&mut self, id: &Value, reason: &str) -> TestResult {
        self.reply(id, json!({"stopReason": reason}))
    }

    fn update(&mut self, session: &str, update: Value) -> TestResult {
        self.send(&json!({"jsonrpc":"2.0","method":"session/update",
            "params":{"sessionId":session,"update":update}}))
    }

    fn text(&mut self, session: &str, kind: &str, text: &str) -> TestResult {
        self.update(
            session,
            json!({"sessionUpdate":kind,"content":{"type":"text","text":text}}),
        )
    }

    fn noise(&mut self) -> TestResult {
        for kind in [
            "agent_thought_chunk",
            "user_message_chunk",
            "tool_call",
            "tool_call_update",
        ] {
            self.text(SESSION_ID, kind, "non-turn-text")?;
        }
        self.update(
            SESSION_ID,
            json!({"sessionUpdate":"session_info_update","title":"title-text"}),
        )?;
        self.update(SESSION_ID, json!({"sessionUpdate":"agent_message_chunk","content":{"type":"image","text":"image"}}))
    }

    fn mode(&self, file: &str) -> TestResult<String> {
        match fs::read_to_string(self.root.join(file)) {
            Ok(mode) => Ok(mode),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(String::new()),
            Err(error) => Err(error.into()),
        }
    }

    fn wait_for_release(&self) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if self.root.join("release").try_exists()? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("Test did not release the fixture".into());
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

fn arg_value<'a>(args: &'a [String], arg: &str) -> TestResult<&'a str> {
    args.windows(2)
        .find(|pair| pair[0] == arg)
        .map(|pair| pair[1].as_str())
        .ok_or_else(|| format!("Missing {arg}").into())
}

fn options(model: &str, effort: &str) -> Value {
    json!([
        {"id":"model","currentValue":model,"options":[
            {"group":"models","options":[{"value":"chosen"},{"value":"default"}]}
        ]},
        {"id":"thought_level","currentValue":effort,"options":[{"value":"standard"},{"value":"high"}]}
    ])
}

fn send(output: &mut impl Write, frame: &Value) -> TestResult {
    writeln!(output, "{frame}")?;
    output.flush()?;
    Ok(())
}

fn receive(input: &mut impl BufRead) -> TestResult<Option<Value>> {
    let mut line = String::new();
    if input.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&line)?))
}

fn run_mcp(descriptor: &Value) -> TestResult<Value> {
    let command = descriptor["command"]
        .as_str()
        .ok_or("Missing MCP command")?;
    let args: Vec<String> = serde_json::from_value(descriptor["args"].clone())?;
    let mut child = Command::new(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut input = child.stdin.take().ok_or("No MCP input")?;
    let mut output = BufReader::new(child.stdout.take().ok_or("No MCP output")?);
    send(
        &mut input,
        &json!({"jsonrpc":"2.0","id":1,"method":"initialize",
        "params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"acp-fixture","version":"1"}}}),
    )?;
    let initialized = receive(&mut output)?.ok_or("MCP initialize ended without a reply")?;
    assert!(initialized["result"]["protocolVersion"].is_string());
    send(
        &mut input,
        &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )?;
    send(
        &mut input,
        &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
    )?;
    let tools = receive(&mut output)?.ok_or("No MCP tool list")?;
    assert!(
        tools["result"]["tools"]
            .as_array()
            .is_some_and(|list| list.iter().any(|tool| tool["name"] == "whoami"))
    );
    send(
        &mut input,
        &json!({"jsonrpc":"2.0","id":3,"method":"tools/call",
        "params":{"name":"whoami","arguments":{}}}),
    )?;
    let result = receive(&mut output)?.ok_or("No MCP tool result")?;
    assert_eq!(result["result"]["isError"], false);
    let whoami = serde_json::from_str(
        result["result"]["content"][0]["text"]
            .as_str()
            .ok_or("Missing whoami content")?,
    )?;
    drop(input);
    assert!(child.wait()?.success());
    Ok(whoami)
}
