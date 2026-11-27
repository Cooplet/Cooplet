use clap::Subcommand;
use cooplet_client::{CoopletClient, api_path};
use cooplet_protocol::{
    AnswerPayload, AttachmentRef, DeliveryMode, MessageId, QuestionPayload, ThreadCloseRequest,
    ThreadCreateRequest, ThreadId, ThreadMemberJoinRequest, ThreadMemberLeaveRequest,
    ThreadMemberResumeRequest, ThreadPinSetRequest, ThreadPostRequest, ThreadRole, ThreadStatus,
    ThreadWaitRequest, ThreadWakePolicy,
};
use serde_json::Value;

use super::{CliResult, json_arg, operation_key_or_generate, print_json};

#[derive(Subcommand)]
pub(super) enum ThreadCommand {
    /// Create a shared thread
    Create {
        #[arg(long)]
        title: String,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        creator_handle: Option<String>,
        /// Initial member as JSON. Repeat for multiple members.
        #[arg(long, value_parser = json_arg::<ThreadMemberJoinRequest>)]
        member: Vec<ThreadMemberJoinRequest>,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// List accessible threads
    List {
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        after: Option<ThreadId>,
        #[arg(long)]
        status: Option<ThreadStatus>,
    },
    /// Show a thread's summary
    Get { thread_id: ThreadId },
    /// Close a thread
    Close {
        thread_id: ThreadId,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Publish a post
    Post {
        thread_id: ThreadId,
        #[arg(long)]
        body: String,
        #[arg(long)]
        mention: Vec<String>,
        /// Hub attachment reference as JSON. Repeat for multiple files.
        #[arg(long, value_parser = json_arg::<AttachmentRef>)]
        attach: Vec<AttachmentRef>,
        #[arg(long, conflicts_with = "answer", value_parser = json_arg::<QuestionPayload>)]
        question: Option<QuestionPayload>,
        #[arg(long, value_parser = json_arg::<AnswerPayload>)]
        answer: Option<AnswerPayload>,
        #[arg(long)]
        in_reply_to: Option<MessageId>,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Read a page of posts, optionally including pins
    Read {
        thread_id: ThreadId,
        #[arg(long)]
        since_seq: Option<u64>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        include_pins: bool,
    },
    /// Set or remove a pin, optionally checking its current version
    Pin {
        thread_id: ThreadId,
        pin_key: String,
        #[arg(long, required_unless_present = "delete", conflicts_with = "delete", value_parser = json_arg::<Value>)]
        value: Option<Value>,
        #[arg(long)]
        delete: bool,
        #[arg(long, requires = "value")]
        post_id: Option<MessageId>,
        #[arg(long)]
        expected_version: Option<u64>,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Record the read cursor and return any available posts immediately
    Wait {
        thread_id: ThreadId,
        #[arg(long)]
        since_seq: u64,
        #[arg(long)]
        wake_policy: Option<ThreadWakePolicy>,
    },
    /// List the thread's members
    Members {
        thread_id: ThreadId,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        after: Option<String>,
    },
    /// Add a member to the thread
    Join {
        thread_id: ThreadId,
        member_key: String,
        #[arg(long)]
        handle: Option<String>,
        #[arg(long)]
        role: Option<ThreadRole>,
        #[arg(long)]
        wake_policy: Option<ThreadWakePolicy>,
        #[arg(long)]
        mode: Option<DeliveryMode>,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Leave the thread or remove a member
    Leave {
        thread_id: ThreadId,
        #[arg(long)]
        member_key: Option<String>,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Resume a paused delivery from an explicit cursor
    Resume {
        thread_id: ThreadId,
        #[arg(long)]
        member_key: Option<String>,
        #[arg(long)]
        after_seq: u64,
        #[arg(long)]
        operation_key: Option<String>,
    },
}

pub(super) async fn run(client: &CoopletClient, command: ThreadCommand) -> CliResult {
    use ThreadCommand::*;

    let result: Value = match command {
        Create {
            title,
            topic,
            creator_handle,
            member: members,
            operation_key,
        } => {
            let body = ThreadCreateRequest {
                title,
                topic,
                creator_handle,
                members,
            };
            post(client, "/v1/threads", &body, operation_key).await?
        }
        List {
            limit,
            after,
            status,
        } => {
            let query = params([
                ("limit", as_param(limit)),
                ("after", as_param(after)),
                ("status", as_param(status)),
            ]);
            client.get_json("/v1/threads", &query).await?
        }
        Get { thread_id } => {
            client
                .get_json(&api_path(&["threads", thread_id.as_str()])?, &[])
                .await?
        }
        Close {
            thread_id,
            operation_key,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "close"])?;
            post(client, &path, &ThreadCloseRequest {}, operation_key).await?
        }
        Post {
            thread_id,
            body,
            mention: mentions,
            attach: attachments,
            question,
            answer,
            in_reply_to,
            operation_key,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "posts"])?;
            let body = ThreadPostRequest {
                body,
                mentions,
                attachments,
                question,
                answer,
                in_reply_to,
            };
            post(client, &path, &body, operation_key).await?
        }
        Read {
            thread_id,
            since_seq,
            limit,
            include_pins,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "posts"])?;
            let query = params([
                ("since_seq", as_param(since_seq)),
                ("limit", as_param(limit)),
                ("include_pins", as_param(include_pins.then_some(true))),
            ]);
            client.get_json(&path, &query).await?
        }
        Pin {
            thread_id,
            pin_key,
            value,
            delete,
            post_id,
            expected_version,
            operation_key,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "pins", &pin_key])?;
            let key = operation_key_or_generate(operation_key)?;
            if delete {
                let query = params([("expected_version", as_param(expected_version))]);
                client
                    .delete_json_with_query(&path, &query, Some(&key))
                    .await?
            } else {
                let value_json = value.ok_or("A pin requires --value or --delete")?;
                let body = ThreadPinSetRequest {
                    value_json,
                    post_id,
                    expected_version,
                };
                client.put_json(&path, &body, Some(&key)).await?
            }
        }
        Wait {
            thread_id,
            since_seq,
            wake_policy,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "wait"])?;
            let body = ThreadWaitRequest {
                since_seq,
                wake_policy,
            };
            client.post_json(&path, &body, None).await?
        }
        Members {
            thread_id,
            limit,
            after,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "members"])?;
            let query = params([("limit", as_param(limit)), ("after", after)]);
            client.get_json(&path, &query).await?
        }
        Join {
            thread_id,
            member_key,
            handle,
            role,
            wake_policy,
            mode: delivery_mode,
            operation_key,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "members", "join"])?;
            let body = ThreadMemberJoinRequest {
                member_key,
                handle,
                role,
                wake_policy,
                delivery_mode,
            };
            post(client, &path, &body, operation_key).await?
        }
        Leave {
            thread_id,
            member_key,
            operation_key,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "members", "leave"])?;
            post(
                client,
                &path,
                &ThreadMemberLeaveRequest { member_key },
                operation_key,
            )
            .await?
        }
        Resume {
            thread_id,
            member_key,
            after_seq,
            operation_key,
        } => {
            let path = api_path(&["threads", thread_id.as_str(), "members", "resume"])?;
            let body = ThreadMemberResumeRequest {
                member_key,
                after_seq,
            };
            post(client, &path, &body, operation_key).await?
        }
    };
    print_json(&result)
}

async fn post(
    client: &CoopletClient,
    path: &str,
    body: &impl serde::Serialize,
    operation_key: Option<String>,
) -> CliResult<Value> {
    let key = operation_key_or_generate(operation_key)?;
    Ok(client.post_json(path, body, Some(&key)).await?)
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
