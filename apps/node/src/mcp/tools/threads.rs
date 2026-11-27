use cooplet_client::{CoopletClient, api_path};
use cooplet_protocol::{
    MessageId, ThreadCreateRequest, ThreadId, ThreadMemberJoinRequest, ThreadMemberLeaveRequest,
    ThreadMemberResumeRequest, ThreadMembersQuery, ThreadPinDeleteQuery, ThreadPinSetRequest,
    ThreadPostRequest, ThreadReadQuery, ThreadWaitRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{
    ToolError, ToolResult, answer_schema, attachments_schema, decode, get, object,
    operation_key_schema, question_schema, valid_key,
};

#[derive(Clone, Copy)]
pub(super) enum ThreadTool {
    Create,
    Post,
    Read,
    Pin,
    Wait,
    Members,
}

impl ThreadTool {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Create => "thread_create",
            Self::Post => "thread_post",
            Self::Read => "thread_read",
            Self::Pin => "thread_pin",
            Self::Wait => "thread_wait",
            Self::Members => "thread_members",
        }
    }

    pub(super) fn definition(self) -> (&'static str, Value) {
        let thread_id = json!({"type":"string", "pattern":ThreadId::PATTERN});
        let message_id = json!({"type":"string", "pattern":MessageId::PATTERN});
        let seq = json!({"type":"integer", "minimum":0});
        let limit = json!({"type":"integer", "minimum":1, "maximum":100, "default":20});
        match self {
            Self::Create => (
                "Create a shared thread and optionally add initial members.",
                object(
                    json!({
                        "title": {"type":"string", "minLength":1},
                        "topic": {"type":"string"},
                        "creator_handle": {"type":"string"},
                        "members": {"type":"array", "items": object(member_fields(), &["member_key"])},
                        "operation_key": operation_key_schema(),
                    }),
                    &["title", "operation_key"],
                ),
            ),
            Self::Post => (
                "Publish a post, question, or answer. Use blob_put with target hub for attachments.",
                object(
                    json!({
                        "thread_id": thread_id,
                        "body": {"type":"string"},
                        "mentions": {"type":"array", "items":{"type":"string"}},
                        "attachments": attachments_schema(),
                        "question": question_schema(),
                        "answer": answer_schema(),
                        "in_reply_to": message_id,
                        "operation_key": operation_key_schema(),
                    }),
                    &["thread_id", "body", "operation_key"],
                ),
            ),
            Self::Read => (
                "Read posts after a sequence number, optionally including pins.",
                object(
                    json!({
                        "thread_id": thread_id,
                        "since_seq": seq,
                        "limit": limit,
                        "include_pins": {"type":"boolean", "default":false},
                    }),
                    &["thread_id"],
                ),
            ),
            Self::Pin => {
                let mut schema = object(
                    json!({
                        "thread_id": thread_id,
                        "action": {"type":"string", "enum":["set", "delete"]},
                        "pin_key": {"type":"string", "pattern":"^[a-zA-Z0-9._:-]{1,64}$"},
                        "value_json": {"not":{"type":"null"}},
                        "post_id": message_id,
                        "expected_version": seq,
                        "operation_key": operation_key_schema(),
                    }),
                    &["thread_id", "action", "pin_key", "operation_key"],
                );
                schema["oneOf"] = json!([
                    {"properties":{"action":{"const":"set"}}, "required":["value_json"]},
                    {"properties":{"action":{"const":"delete"}}, "not":{"anyOf":[
                        {"required":["value_json"]}, {"required":["post_id"]}
                    ]}},
                ]);
                (
                    "Set or remove a pin with an optional version check.",
                    schema,
                )
            }
            Self::Wait => (
                "Record progress and return pending posts immediately when available.",
                object(
                    json!({
                        "thread_id": thread_id,
                        "since_seq": seq,
                        "wake_policy": {"type":"string", "enum":["mentions_only", "all"]},
                    }),
                    &["thread_id", "since_seq"],
                ),
            ),
            Self::Members => {
                let mut props = member_fields();
                props["thread_id"] = thread_id;
                props["action"] =
                    json!({"type":"string", "enum":["list", "join", "leave", "resume"]});
                props["limit"] = limit;
                props["after"] = json!({"type":"string"});
                props["after_seq"] = seq;
                props["operation_key"] = operation_key_schema();
                let mut schema = object(props, &["thread_id", "action"]);
                schema["oneOf"] = json!([
                    {"properties":{"action":{"const":"list"}}},
                    {"properties":{"action":{"const":"join"}}, "required":["member_key", "operation_key"]},
                    {"properties":{"action":{"const":"leave"}}, "required":["operation_key"]},
                    {"properties":{"action":{"const":"resume"}}, "required":["after_seq", "operation_key"]},
                ]);
                (
                    "List members, add or remove a member, or resume uncertain delivery.",
                    schema,
                )
            }
        }
    }

    pub(super) async fn call(self, client: &CoopletClient, input: Value) -> ToolResult {
        let Value::Object(mut args) = input else {
            return Err(ToolError::invalid("Tool arguments must be an object"));
        };
        match self {
            Self::Create => {
                let key = take_key(&mut args)?;
                let body: ThreadCreateRequest = decode(Value::Object(args))?;
                post(client, "/v1/threads", &body, Some(&key)).await
            }
            Self::Post => {
                let thread_id: ThreadId = take(&mut args, "thread_id")?;
                let key = take_key(&mut args)?;
                let body: ThreadPostRequest = decode(Value::Object(args))?;
                let path = path(&["threads", thread_id.as_str(), "posts"])?;
                post(client, &path, &body, Some(&key)).await
            }
            Self::Read => {
                let thread_id: ThreadId = take(&mut args, "thread_id")?;
                let body: ThreadReadQuery = decode(Value::Object(args))?;
                let query = params([
                    ("since_seq", as_param(body.since_seq)),
                    ("limit", as_param(body.limit)),
                    ("include_pins", as_param(body.include_pins.then_some(true))),
                ]);
                let path = path(&["threads", thread_id.as_str(), "posts"])?;
                get(client, &path, &query).await
            }
            Self::Pin => pin(client, args).await,
            Self::Wait => {
                let thread_id: ThreadId = take(&mut args, "thread_id")?;
                let body: ThreadWaitRequest = decode(Value::Object(args))?;
                let path = path(&["threads", thread_id.as_str(), "wait"])?;
                post(client, &path, &body, None).await
            }
            Self::Members => members(client, args).await,
        }
    }
}

fn member_fields() -> Value {
    json!({
        "member_key": {"type":"string", "minLength":1},
        "handle": {"type":"string"},
        "role": {"type":"string", "enum":["owner", "member", "observer"]},
        "wake_policy": {"type":"string", "enum":["mentions_only", "all", "muted"]},
        "delivery_mode": {"type":"string", "enum":["direct_now", "queue_until_idle"]},
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PinAction {
    Set,
    Delete,
}

async fn pin(client: &CoopletClient, mut args: Map<String, Value>) -> ToolResult {
    let thread_id: ThreadId = take(&mut args, "thread_id")?;
    let action: PinAction = take(&mut args, "action")?;
    let pin_key: String = take(&mut args, "pin_key")?;
    let key = take_key(&mut args)?;
    let path = path(&["threads", thread_id.as_str(), "pins", &pin_key])?;
    let result = match action {
        PinAction::Set => {
            let body: ThreadPinSetRequest = decode(Value::Object(args))?;
            client.put_json(&path, &body, Some(&key)).await
        }
        PinAction::Delete => {
            let query: ThreadPinDeleteQuery = decode(Value::Object(args))?;
            let query = params([("expected_version", as_param(query.expected_version))]);
            client
                .delete_json_with_query(&path, &query, Some(&key))
                .await
        }
    };
    result.map_err(|error| ToolError::from_client(error, true))
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MembersAction {
    List,
    Join,
    Leave,
    Resume,
}

async fn members(client: &CoopletClient, mut args: Map<String, Value>) -> ToolResult {
    let thread_id: ThreadId = take(&mut args, "thread_id")?;
    let action: MembersAction = take(&mut args, "action")?;
    let id = thread_id.as_str();
    match action {
        MembersAction::List => {
            let body: ThreadMembersQuery = decode(Value::Object(args))?;
            let query = params([("limit", as_param(body.limit)), ("after", body.after)]);
            get(client, &path(&["threads", id, "members"])?, &query).await
        }
        MembersAction::Join => {
            let key = take_key(&mut args)?;
            let body: ThreadMemberJoinRequest = decode(Value::Object(args))?;
            post(
                client,
                &path(&["threads", id, "members", "join"])?,
                &body,
                Some(&key),
            )
            .await
        }
        MembersAction::Leave => {
            let key = take_key(&mut args)?;
            let body: ThreadMemberLeaveRequest = decode(Value::Object(args))?;
            post(
                client,
                &path(&["threads", id, "members", "leave"])?,
                &body,
                Some(&key),
            )
            .await
        }
        MembersAction::Resume => {
            let key = take_key(&mut args)?;
            let body: ThreadMemberResumeRequest = decode(Value::Object(args))?;
            post(
                client,
                &path(&["threads", id, "members", "resume"])?,
                &body,
                Some(&key),
            )
            .await
        }
    }
}

async fn post(
    client: &CoopletClient,
    path: &str,
    body: &impl Serialize,
    key: Option<&str>,
) -> ToolResult {
    client
        .post_json(path, body, key)
        .await
        .map_err(|error| ToolError::from_client(error, true))
}

fn take<T: serde::de::DeserializeOwned>(
    args: &mut Map<String, Value>,
    field: &str,
) -> ToolResult<T> {
    let value = args
        .remove(field)
        .ok_or_else(|| ToolError::invalid("Missing required tool argument"))?;
    serde_json::from_value(value).map_err(|_| ToolError::invalid("Invalid tool argument"))
}

fn take_key(args: &mut Map<String, Value>) -> ToolResult<String> {
    let key: String = take(args, "operation_key")?;
    valid_key(&key)?;
    Ok(key)
}

fn path(segments: &[&str]) -> ToolResult<String> {
    api_path(segments).map_err(|error| ToolError::from_client(error, false))
}

fn as_param(value: Option<impl ToString>) -> Option<String> {
    value.map(|value| value.to_string())
}

fn params<const N: usize>(pairs: [(&str, Option<String>); N]) -> Vec<(&str, String)> {
    pairs
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect()
}
