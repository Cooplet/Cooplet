use std::{collections::BTreeMap, io};

use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use super::tools::{ToolContext, ToolError, tool_list};

const VERSION: &str = "2025-11-25";
const MAX_INPUT_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Lifecycle {
    #[default]
    Initial,
    AwaitingNotification,
    Ready,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Id {
    Integer(i64),
    Unsigned(u64),
    String(String),
}

fn present_id<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Id>, D::Error> {
    Id::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    jsonrpc: String,
    #[serde(default, deserialize_with = "present_id")]
    id: Option<Id>,
    method: String,
    #[serde(default = "empty_object")]
    params: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyParams {
    #[serde(rename = "_meta")]
    _meta: Option<BTreeMap<String, Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InitializeParams {
    protocol_version: String,
    #[serde(rename = "capabilities")]
    _capabilities: BTreeMap<String, Value>,
    client_info: Implementation,
    #[serde(rename = "_meta")]
    _meta: Option<BTreeMap<String, Value>>,
}

#[derive(Deserialize)]
struct Implementation {
    name: String,
    version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListParams {
    cursor: Option<String>,
    #[serde(rename = "_meta")]
    _meta: Option<BTreeMap<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CallParams {
    name: String,
    #[serde(default = "empty_object")]
    arguments: Value,
    #[serde(rename = "_meta")]
    _meta: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RpcError {
    code: i32,
    message: &'static str,
}

const PARSE_ERROR: RpcError = RpcError {
    code: -32700,
    message: "Parse error",
};
const INVALID_REQUEST: RpcError = RpcError {
    code: -32600,
    message: "Invalid request",
};
const METHOD_NOT_FOUND: RpcError = RpcError {
    code: -32601,
    message: "Method not found",
};
const INVALID_PARAMS: RpcError = RpcError {
    code: -32602,
    message: "Invalid params",
};
const INTERNAL_ERROR: RpcError = RpcError {
    code: -32603,
    message: "Internal error",
};
const NOT_INITIALIZED: RpcError = RpcError {
    code: -32002,
    message: "MCP session has not initialized",
};
const FRAME_TOO_LARGE: RpcError = RpcError {
    code: -32000,
    message: "JSON-RPC input exceeds 2 MiB",
};

enum Frame {
    Message,
    TooLarge,
    End,
}

pub(super) async fn serve(
    mut reader: impl AsyncBufRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    tools: &ToolContext,
) -> io::Result<()> {
    let mut lifecycle = Lifecycle::default();
    let mut bytes = Vec::new();
    loop {
        let response = match read_frame(&mut reader, &mut bytes, MAX_INPUT_BYTES).await? {
            Frame::Message => dispatch(&mut lifecycle, &bytes, tools).await,
            Frame::TooLarge => Some(error_response(None, FRAME_TOO_LARGE)),
            Frame::End => break,
        };
        if let Some(response) = response {
            let mut line = serde_json::to_vec(&response).map_err(io::Error::other)?;
            line.push(b'\n');
            writer.write_all(&line).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

// Bound allocation even when a line exceeds the limit. Discard the rest of
// that line so a subsequent valid frame still has its original boundaries.
// EOF drops an unfinished frame, even if its contents happen to be valid JSON.
async fn read_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
    output: &mut Vec<u8>,
    limit: usize,
) -> io::Result<Frame> {
    output.clear();
    let mut oversized = false;
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(Frame::End);
        }
        let newline = chunk.iter().position(|byte| *byte == b'\n');
        let body_bytes = newline.unwrap_or(chunk.len());
        if !oversized {
            if body_bytes > limit - output.len() {
                oversized = true;
            } else {
                output.extend_from_slice(&chunk[..body_bytes]);
            }
        }
        reader.consume(body_bytes + usize::from(newline.is_some()));
        if newline.is_some() {
            return Ok(if oversized {
                Frame::TooLarge
            } else {
                Frame::Message
            });
        }
    }
}

async fn dispatch(state: &mut Lifecycle, bytes: &[u8], tools: &ToolContext) -> Option<Value> {
    let request = match parse_request(bytes) {
        Ok(request) => request,
        Err(error) => return Some(error_response(None, error)),
    };
    let Some(id) = request.id.as_ref() else {
        if request.method == "notifications/initialized"
            && *state == Lifecycle::AwaitingNotification
            && params::<EmptyParams>(request.params).is_ok()
        {
            *state = Lifecycle::Ready;
        }
        return None;
    };

    let response = match request.method.as_str() {
        "ping" => params::<EmptyParams>(request.params).map(|_| json!({})),
        "initialize" => initialize(state, request.params),
        _ if *state != Lifecycle::Ready => Err(NOT_INITIALIZED),
        "tools/list" => params::<ListParams>(request.params).and_then(|params| {
            if params.cursor.is_some() {
                return Err(INVALID_PARAMS);
            }
            Ok(json!({ "tools": tool_list() }))
        }),
        "tools/call" => match params::<CallParams>(request.params) {
            Ok(params) => call_tool(tools, params).await,
            Err(error) => Err(error),
        },
        _ => Err(METHOD_NOT_FOUND),
    };
    Some(match response {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(error) => error_response(Some(id), error),
    })
}

fn parse_request(bytes: &[u8]) -> Result<Request, RpcError> {
    if bytes.iter().copied().find(|b| !b.is_ascii_whitespace()) != Some(b'{') {
        return match serde_json::from_slice::<Value>(bytes) {
            Ok(_) => Err(INVALID_REQUEST),
            Err(_) => Err(PARSE_ERROR),
        };
    }
    let request: Request =
        serde_json::from_slice(bytes).map_err(|error| match error.classify() {
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => PARSE_ERROR,
            _ => INVALID_REQUEST,
        })?;
    if request.jsonrpc != "2.0" || request.method.is_empty() {
        return Err(INVALID_REQUEST);
    }
    Ok(request)
}

fn initialize(state: &mut Lifecycle, raw: Value) -> Result<Value, RpcError> {
    if *state != Lifecycle::Initial {
        return Err(INVALID_REQUEST);
    }
    if !raw.get("clientInfo").is_some_and(Value::is_object) {
        return Err(INVALID_PARAMS);
    }
    let init: InitializeParams = params(raw)?;
    if init.protocol_version.is_empty()
        || init.client_info.name.is_empty()
        || init.client_info.version.is_empty()
    {
        return Err(INVALID_PARAMS);
    }
    *state = Lifecycle::AwaitingNotification;
    // This server supports one version. The client decides whether it can
    // continue with this version when it requested a different one.
    Ok(json!({
        "protocolVersion": VERSION,
        "capabilities": {"tools": {"listChanged": false}},
        "serverInfo": {"name": "cooplet-node", "version": env!("CARGO_PKG_VERSION")},
    }))
}

fn params<T: DeserializeOwned>(raw: Value) -> Result<T, RpcError> {
    if !raw.is_object() {
        return Err(INVALID_PARAMS);
    }
    serde_json::from_value(raw).map_err(|_| INVALID_PARAMS)
}

fn empty_object() -> Value {
    json!({})
}

async fn call_tool(tools: &ToolContext, params: CallParams) -> Result<Value, RpcError> {
    let outcome = tools
        .call(&params.name, params.arguments)
        .await
        .ok_or(INVALID_PARAMS)?;
    tool_result(outcome)
}

fn tool_result(result: Result<Value, ToolError>) -> Result<Value, RpcError> {
    let (text, is_error) = match result {
        Ok(value) => (serde_json::to_string(&value), false),
        Err(error) => (serde_json::to_string(&error), true),
    };
    let text = text.map_err(|_| INTERNAL_ERROR)?;
    Ok(json!({"content": [{"type": "text", "text": text}], "isError": is_error}))
}

fn error_response(id: Option<&Id>, error: RpcError) -> Value {
    json!({
        "jsonrpc": "2.0", "id": id,
        "error": {"code": error.code, "message": error.message},
    })
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, routing::post};
    use cooplet_client::{BearerToken, ClientConfig, CoopletClient, Destination};
    use cooplet_protocol::{ErrorCode, NodeId, OperationId, ParticipantId};
    use tokio::{io::BufReader, net::TcpListener};

    use super::*;

    #[tokio::test]
    async fn bounded_lines_recover_after_oversize_and_finish_at_eof() -> io::Result<()> {
        let mut reader = BufReader::with_capacity(3, &b"1234\n12345\n{}\nlast"[..]);
        let mut line = Vec::new();

        assert!(matches!(
            read_frame(&mut reader, &mut line, 4).await?,
            Frame::Message
        ));
        assert_eq!(line, b"1234");
        assert!(matches!(
            read_frame(&mut reader, &mut line, 4).await?,
            Frame::TooLarge
        ));
        assert!(line.len() <= 4);
        assert!(matches!(
            read_frame(&mut reader, &mut line, 4).await?,
            Frame::Message
        ));
        assert_eq!(line, b"{}");
        assert!(matches!(
            read_frame(&mut reader, &mut line, 4).await?,
            Frame::End
        ));
        let mut reader = BufReader::with_capacity(3, &b"12345"[..]);
        assert!(matches!(
            read_frame(&mut reader, &mut line, 4).await?,
            Frame::End
        ));
        Ok(())
    }

    #[tokio::test]
    async fn complete_json_without_lf_never_invokes_the_tool() -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?).parse()?;
        let client = CoopletClient::new(ClientConfig::new(
            origin,
            Destination::LocalNode,
            Some(BearerToken::new("tool-test-token")?),
        ))?;
        let participant = ParticipantId::generate()?;
        let call = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "send_message",
                "arguments": {
                    "to": participant,
                    "body": "hello",
                    "operation_key": "framing-test",
                },
            },
        });
        let tools = ToolContext::new(client, NodeId::generate()?, participant, PathBuf::new());

        let requests = Arc::new(AtomicUsize::new(0));
        let requests_in_handler = requests.clone();
        let operation_id = OperationId::generate()?;
        let router = Router::new().route(
            "/v1/messages",
            post(move || {
                requests_in_handler.fetch_add(1, Ordering::Relaxed);
                let operation_id = operation_id.clone();
                async move {
                    Json(json!({
                        "operation_id": operation_id,
                        "delivery_state": "delivered",
                    }))
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await });

        let handshake = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
"#;
        let mut input = handshake.to_vec();
        input.extend(serde_json::to_vec(&call)?);
        let mut output = Vec::new();
        serve(BufReader::new(input.as_slice()), &mut output, &tools).await?;
        assert_eq!(requests.load(Ordering::Relaxed), 0);
        assert_eq!(std::str::from_utf8(&output)?.lines().count(), 1);

        input.push(b'\n');
        output.clear();
        serve(BufReader::new(input.as_slice()), &mut output, &tools).await?;
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        let response = std::str::from_utf8(&output)?
            .lines()
            .last()
            .ok_or_else(|| io::Error::other("missing tool response"))?;
        let response: Value = serde_json::from_str(response)?;
        assert_eq!(response["id"], 2);
        assert_eq!(response["result"]["isError"], false);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn stdio_lifecycle_and_tool_arguments_produce_only_rpc_lines()
    -> Result<(), Box<dyn Error>> {
        let client = CoopletClient::new(ClientConfig::new(
            "http://127.0.0.1:1".parse()?,
            Destination::LocalNode,
            None,
        ))?;
        let tools = ToolContext::new(
            client,
            NodeId::generate()?,
            ParticipantId::generate()?,
            PathBuf::new(),
        );
        let mut input = vec![b' '; MAX_INPUT_BYTES + 1];
        input.extend_from_slice(br#"
{"jsonrpc":"2.0","id":0,"method":"tools/list"}
{"jsonrpc":"2.0","id":"ping","method":"ping"}
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"unsupported","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":3,"method":"tools/list"}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"send_message","arguments":{"to":"someone","body":"hello","operation_key":"test-key","author":"other"}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"whoami","arguments":null}}
{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"approve","arguments":{}}}
"#);
        let mut output = Vec::new();
        serve(BufReader::new(input.as_slice()), &mut output, &tools).await?;
        let lines: Vec<Value> = std::str::from_utf8(&output)?
            .lines()
            .map(serde_json::from_str)
            .collect::<serde_json::Result<_>>()?;
        assert_eq!(lines.len(), 9);
        assert_eq!(lines[0]["error"]["code"], FRAME_TOO_LARGE.code);
        assert_eq!(lines[1]["error"]["code"], NOT_INITIALIZED.code);
        assert_eq!(lines[2]["id"], "ping");
        assert_eq!(lines[2]["result"], json!({}));
        assert_eq!(lines[3]["result"]["protocolVersion"], VERSION);
        assert_eq!(lines[4]["error"]["code"], NOT_INITIALIZED.code);

        let tools = lines[5]["result"]["tools"]
            .as_array()
            .ok_or_else(|| io::Error::other("missing tools"))?;
        assert_eq!(tools.len(), 11);
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool["name"].as_str())
                .collect::<Vec<_>>(),
            [
                Some("whoami"),
                Some("list_participants"),
                Some("send_message"),
                Some("ask_user"),
                Some("blob_put"),
                Some("thread_create"),
                Some("thread_post"),
                Some("thread_read"),
                Some("thread_pin"),
                Some("thread_wait"),
                Some("thread_members"),
            ]
        );
        for tool in tools {
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
        }
        for line in &lines[6..8] {
            assert_eq!(line["result"]["isError"], true);
            let payload = line["result"]["content"][0]["text"]
                .as_str()
                .ok_or_else(|| io::Error::other("missing tool error"))?;
            let error: Value = serde_json::from_str(payload)?;
            assert_eq!(error["code"], ErrorCode::InvalidRequest.as_str());
            assert_eq!(error["retryable"], false);
        }
        assert_eq!(lines[8]["error"]["code"], INVALID_PARAMS.code);
        Ok(())
    }

    #[test]
    fn rpc_ids_and_envelope_reject_arrays_and_null_ids() {
        for wire in [
            br#"["2.0",1,"ping",{}]"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#,
            br#"{"jsonrpc":"2.0","id":1.5,"method":"ping"}"#,
            br#"{"jsonrpc":"1.0","id":1,"method":"ping"}"#,
            br#"{"jsonrpc":"2.0","id":1,"id":2,"method":"ping"}"#,
        ] {
            assert_eq!(parse_request(wire).err(), Some(INVALID_REQUEST));
        }
        assert_eq!(parse_request(b"{").err(), Some(PARSE_ERROR));
        assert!(
            parse_request(br#"{"jsonrpc":"2.0","id":18446744073709551615,"method":"ping"}"#)
                .is_ok()
        );
    }
}
