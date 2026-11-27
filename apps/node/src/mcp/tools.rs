use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Seek},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, ClientError, CoopletClient, UploadVerification, api_path,
};
use cooplet_protocol::{
    AnswerPayload, AttachmentRef, DeliveryMode, ErrorCode, MessageId, NodeId, OperationId,
    ParticipantId, ParticipantKind, PrincipalKind, ProblemDetails, QuestionPayload, RunId,
    ServicePrincipalId, ThreadId, UserId, WhoamiResponse, validate_attachment_name,
    validate_idempotency_key,
};
use cooplet_storage::DEFAULT_MAX_BLOB_BYTES;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::delivery::{SendMessageRequest, SendMessageResponse};

mod threads;

#[derive(Debug, Serialize)]
pub(super) struct ToolError {
    code: ErrorCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    retryable: bool,
    detail: &'static str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    extensions: BTreeMap<&'static str, Value>,
}

impl ToolError {
    fn new(code: ErrorCode, detail: &'static str) -> Self {
        Self {
            code,
            status: None,
            retryable: code.default_retryable(),
            detail,
            extensions: BTreeMap::new(),
        }
    }

    fn invalid(detail: &'static str) -> Self {
        Self::new(ErrorCode::InvalidRequest, detail)
    }

    fn from_problem(problem: ProblemDetails) -> Self {
        let mut error = Self::new(problem.code, problem.code.title());
        error.status = Some(problem.status);
        error.retryable = problem.retryable;
        // Return typed correlation IDs. Arbitrary strings from an HTTP error
        // can contain local paths or credentials.
        for field in ["message_id", "existing_answer_id"] {
            if let Some(value) = problem.extensions.get(field).and_then(Value::as_str)
                && MessageId::parse(value).is_ok()
            {
                error
                    .extensions
                    .insert(field, Value::String(value.to_owned()));
            }
        }
        if let Some(value) = problem
            .extensions
            .get("operation_id")
            .and_then(Value::as_str)
            && OperationId::parse(value).is_ok()
        {
            error
                .extensions
                .insert("operation_id", Value::String(value.to_owned()));
        }
        if let Some(value) = problem.extensions.get("thread_id").and_then(Value::as_str)
            && ThreadId::parse(value).is_ok()
        {
            error
                .extensions
                .insert("thread_id", Value::String(value.to_owned()));
        }
        for field in ["floor_seq", "head_seq", "current_version"] {
            if let Some(value) = problem.extensions.get(field)
                && (value.is_u64() || (field == "current_version" && value.is_null()))
            {
                error.extensions.insert(field, value.clone());
            }
        }
        if let Some(reason) = problem.extensions.get("reason").and_then(Value::as_str)
            && reason.len() <= 64
            && reason.contains('_')
            && reason
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        {
            error
                .extensions
                .insert("reason", Value::String(reason.to_owned()));
        }
        error
    }

    fn from_client(error: ClientError, mutation: bool) -> Self {
        match error {
            ClientError::Problem(problem) => Self::from_problem(*problem),
            ClientError::InvalidInput(_) => {
                Self::invalid("The tool arguments are not a valid Node request")
            }
            ClientError::BlobTooLarge { .. } => {
                Self::new(ErrorCode::BlobTooLarge, "The file exceeds the upload limit")
            }
            ClientError::BlobDigestMismatch { .. } | ClientError::BlobLengthMismatch { .. } => {
                Self::new(
                    ErrorCode::BlobDigestMismatch,
                    "The file changed during upload or its receipt did not match",
                )
            }
            _ if mutation => Self::new(
                ErrorCode::DeliveryUnknown,
                "The Node did not confirm the operation outcome",
            ),
            _ => Self::new(
                ErrorCode::InternalError,
                "Could not read from the local Node",
            ),
        }
    }
}

type ToolResult<T = Value> = Result<T, ToolError>;

#[derive(Clone, Copy)]
enum Tool {
    Whoami,
    ListParticipants,
    SendMessage,
    AskUser,
    BlobPut,
    Thread(threads::ThreadTool),
}

impl Tool {
    const ALL: [Self; 11] = [
        Self::Whoami,
        Self::ListParticipants,
        Self::SendMessage,
        Self::AskUser,
        Self::BlobPut,
        Self::Thread(threads::ThreadTool::Create),
        Self::Thread(threads::ThreadTool::Post),
        Self::Thread(threads::ThreadTool::Read),
        Self::Thread(threads::ThreadTool::Pin),
        Self::Thread(threads::ThreadTool::Wait),
        Self::Thread(threads::ThreadTool::Members),
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Whoami => "whoami",
            Self::ListParticipants => "list_participants",
            Self::SendMessage => "send_message",
            Self::AskUser => "ask_user",
            Self::BlobPut => "blob_put",
            Self::Thread(tool) => tool.name(),
        }
    }

    fn definition(self) -> Value {
        let (description, schema) = match self {
            Self::Whoami => (
                "Read the authenticated participant identity.",
                object(json!({}), &[]),
            ),
            Self::ListParticipants => (
                "List participants and their intake states.",
                object(
                    json!({
                        "after": {"type":"string", "pattern": ParticipantId::PATTERN},
                        "limit": {"type":"integer", "minimum":1, "maximum":500},
                    }),
                    &[],
                ),
            ),
            Self::SendMessage => (
                "Send a message or answer from the authenticated participant. Keep the operation key when checking an earlier attempt.",
                message_schema(false),
            ),
            Self::AskUser => (
                "Send a question to the local human operator.",
                message_schema(true),
            ),
            Self::BlobPut => (
                "Upload a workspace file and return its attachment reference.",
                object(
                    json!({
                        "path": {"type":"string", "minLength":1, "description":"Path relative to the participant workspace."},
                        "target": {"type":"string", "enum":["local", "hub"], "default":"local"},
                        "operation_key": operation_key_schema(),
                    }),
                    &["path", "operation_key"],
                ),
            ),
            Self::Thread(tool) => tool.definition(),
        };
        json!({"name": self.name(), "description": description, "inputSchema": schema})
    }
}

pub(super) fn tool_list() -> Vec<Value> {
    Tool::ALL.into_iter().map(Tool::definition).collect()
}

fn object(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn operation_key_schema() -> Value {
    json!({"type":"string", "minLength":8, "maxLength":128, "pattern": "^[!-~]+$"})
}

fn attachments_schema() -> Value {
    let attachments = object(
        json!({
            "blob_id": {"type":"string", "pattern": cooplet_protocol::BlobId::PATTERN},
            "name": {"type":"string"},
            "mime_type": {"type":"string"},
            "size_bytes": {"type":"integer", "minimum":0},
            "sha256": {"type":"string", "pattern":"^[0-9a-f]{64}$"},
        }),
        &["blob_id", "name", "mime_type", "size_bytes", "sha256"],
    );
    json!({"type":"array", "items": attachments, "maxItems":64})
}

fn question_schema() -> Value {
    let option = object(
        json!({
            "id": {"type":"string", "minLength":1, "maxLength":64},
            "label": {"type":"string", "minLength":1, "maxLength":64},
            "description": {"type":"string", "maxLength":256},
        }),
        &["id", "label"],
    );
    object(
        json!({
            "prompt": {"type":"string", "minLength":1, "maxLength":2000},
            "mode": {"type":"string", "enum":["single", "multi", "text"]},
            "options": {"type":"array", "items": option, "maxItems":16},
            "allow_free_text": {"type":"boolean"},
            "expires_at_ms": {"type":"integer", "minimum":1},
        }),
        &["prompt", "mode"],
    )
}

fn answer_schema() -> Value {
    object(
        json!({
            "selected_option_ids": {"type":"array", "items":{"type":"string"}, "maxItems":16},
            "text": {"type":"string", "minLength":1, "maxLength":8000},
        }),
        &[],
    )
}

fn message_schema(question: bool) -> Value {
    let mut schema = object(
        json!({
            "operation_key": operation_key_schema(),
            "body": {"type":"string"},
            "attachments": attachments_schema(),
            "delivery_mode": {"type":"string", "enum":["direct_now", "queue_until_idle"], "default":"direct_now"},
            "queue_ttl_ms": {
                "type":"integer", "minimum":300000, "maximum":3600000,
                "description":"Only valid for queue_until_idle."
            },
        }),
        &[],
    );
    let props = &mut schema["properties"];
    if question {
        props["question"] = question_schema();
        schema["required"] = json!(["question", "operation_key"]);
    } else {
        props["to"] = json!({
            "type":"string",
            "description":"Local actor ID or a qualified <node_id>/<participant_id> or <hub_id>/<participant_id> address.",
            "anyOf":[
                {"pattern":ParticipantId::PATTERN},
                {"pattern":UserId::PATTERN},
                {"pattern":ServicePrincipalId::PATTERN},
                {"pattern":RunId::PATTERN},
                {"pattern":format!("{}{}",
                    cooplet_protocol::SourceId::PATTERN.trim_end_matches('$'),
                    ParticipantId::PATTERN.replacen('^', "/", 1)
                )},
            ],
        });
        props["in_reply_to"] = json!({"type":"string", "pattern": MessageId::PATTERN});
        props["answer"] = answer_schema();
        schema["required"] = json!(["to", "body", "operation_key"]);
    }
    schema
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WhoamiArgs {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    after: Option<ParticipantId>,
    limit: Option<u16>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendArgs {
    to: String,
    body: String,
    operation_key: String,
    in_reply_to: Option<MessageId>,
    #[serde(default)]
    attachments: Vec<AttachmentRef>,
    answer: Option<AnswerPayload>,
    delivery_mode: Option<DeliveryMode>,
    queue_ttl_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AskArgs {
    question: QuestionPayload,
    operation_key: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    attachments: Vec<AttachmentRef>,
    delivery_mode: Option<DeliveryMode>,
    queue_ttl_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobArgs {
    path: PathBuf,
    operation_key: String,
    #[serde(default)]
    target: BlobTarget,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BlobTarget {
    #[default]
    Local,
    Hub,
}

#[derive(Deserialize)]
struct ParticipantPage {
    items: Vec<ParticipantEntry>,
    next_after: Option<ParticipantId>,
}

#[derive(Deserialize)]
struct ParticipantEntry {
    participant_id: ParticipantId,
    kind: ParticipantKind,
    name: String,
}

#[derive(Deserialize)]
struct Workspace {
    workspace_dir: Option<PathBuf>,
}

pub(super) struct ToolContext {
    client: CoopletClient,
    node_id: NodeId,
    participant_id: ParticipantId,
    private_tree: PathBuf,
}

impl ToolContext {
    pub(super) fn new(
        client: CoopletClient,
        node_id: NodeId,
        participant_id: ParticipantId,
        private_tree: PathBuf,
    ) -> Self {
        Self {
            client,
            node_id,
            participant_id,
            private_tree,
        }
    }

    pub(super) async fn call(&self, name: &str, input: Value) -> Option<ToolResult> {
        let tool = Tool::ALL.into_iter().find(|tool| tool.name() == name)?;
        Some(self.dispatch(tool, input).await)
    }

    async fn dispatch(&self, tool: Tool, input: Value) -> ToolResult {
        match tool {
            Tool::Whoami => {
                decode::<WhoamiArgs>(input)?;
                self.whoami().await
            }
            Tool::ListParticipants => self.list(decode(input)?).await,
            Tool::SendMessage => self.send_message(decode(input)?).await,
            Tool::AskUser => self.ask_user(decode(input)?).await,
            Tool::BlobPut => self.blob_put(decode(input)?).await,
            Tool::Thread(tool) => tool.call(&self.client, input).await,
        }
    }

    async fn whoami(&self) -> ToolResult {
        let response = self
            .client
            .whoami()
            .await
            .map_err(|err| ToolError::from_client(err, false))?;
        if !matches_identity(&response, &self.node_id, &self.participant_id) {
            return Err(ToolError::new(
                ErrorCode::Unauthorized,
                "The credential no longer matches this participant",
            ));
        }
        to_value(response)
    }

    async fn list(&self, args: ListArgs) -> ToolResult {
        let mut query = Vec::new();
        if let Some(after) = args.after {
            query.push(("after", after.to_string()));
        }
        if let Some(limit) = args.limit {
            if !(1..=500).contains(&limit) {
                return Err(ToolError::invalid("limit must be between 1 and 500"));
            }
            query.push(("limit", limit.to_string()));
        }
        get(&self.client, "/v1/participants", &query).await
    }

    async fn send_message(&self, args: SendArgs) -> ToolResult {
        self.post_message(
            &args.operation_key,
            SendMessageRequest {
                to: args.to,
                body: args.body,
                in_reply_to: args.in_reply_to,
                attachments: args.attachments,
                question: None,
                answer: args.answer,
                delivery_mode: args.delivery_mode.unwrap_or(DeliveryMode::DirectNow),
                queue_ttl_ms: args.queue_ttl_ms,
            },
        )
        .await
    }

    async fn ask_user(&self, args: AskArgs) -> ToolResult {
        valid_key(&args.operation_key)?;
        args.question
            .validate()
            .map_err(|_| ToolError::invalid("The question is invalid"))?;
        let operator = self.find_operator().await?;
        self.post_message(
            &args.operation_key,
            SendMessageRequest {
                to: operator.to_string(),
                body: args.body,
                in_reply_to: None,
                attachments: args.attachments,
                question: Some(args.question),
                answer: None,
                delivery_mode: args.delivery_mode.unwrap_or(DeliveryMode::DirectNow),
                queue_ttl_ms: args.queue_ttl_ms,
            },
        )
        .await
    }

    async fn post_message(&self, key: &str, body: SendMessageRequest) -> ToolResult {
        valid_key(key)?;
        let receipt: SendMessageResponse = self
            .client
            .post_json("/v1/messages", &body, Some(key))
            .await
            .map_err(|err| ToolError::from_client(err, true))?;
        to_value(receipt)
    }

    async fn find_operator(&self) -> ToolResult<ParticipantId> {
        let mut query = Vec::new();
        let mut previous: Option<ParticipantId> = None;
        loop {
            let page: ParticipantPage = get(&self.client, "/v1/participants", &query).await?;
            if let Some(item) = page
                .items
                .into_iter()
                .find(|item| item.kind == ParticipantKind::Human && item.name == "operator")
            {
                return Ok(item.participant_id);
            }
            let Some(next) = page.next_after else {
                return Err(ToolError::new(
                    ErrorCode::NotFound,
                    "The local human operator was not found",
                ));
            };
            if previous.as_ref().is_some_and(|before| before >= &next) {
                return Err(ToolError::new(
                    ErrorCode::InternalError,
                    "The participant catalog returned an invalid cursor",
                ));
            }
            query = vec![("after", next.to_string())];
            previous = Some(next);
        }
    }

    async fn blob_put(&self, args: BlobArgs) -> ToolResult {
        valid_key(&args.operation_key)?;
        let filename = args
            .path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| ToolError::invalid("path must name a workspace file"))?
            .to_owned();
        if args.path.is_absolute() {
            return Err(ToolError::invalid("path must be relative to the workspace"));
        }
        validate_attachment_name(&filename)
            .map_err(|_| ToolError::invalid("Invalid attachment filename"))?;
        let participant = api_path(&["participants", self.participant_id.as_str()])
            .map_err(|err| ToolError::from_client(err, false))?;
        let workspace: Workspace = get(&self.client, &participant, &[]).await?;
        let workspace = workspace.workspace_dir.ok_or_else(|| {
            ToolError::new(
                ErrorCode::Forbidden,
                "The participant has no workspace directory",
            )
        })?;
        let private = self.private_tree.clone();
        let mime_type = mime_type(&args.path).to_string();
        let target = args.target;
        let file = tokio::task::spawn_blocking(move || {
            prepare_file(&workspace, &args.path, &private, target)
        })
        .await
        .map_err(|_| ToolError::new(ErrorCode::InternalError, "Could not prepare the upload"))??;
        let spec = BlobUploadSpec {
            filename,
            mime_type,
            verification: match target {
                BlobTarget::Local => UploadVerification::LocalSha256,
                BlobTarget::Hub => UploadVerification::HubReceipt,
            },
            expected_sha256: file.sha256,
            idempotency_key: Some(args.operation_key),
            content_length: Some(file.bytes),
            participant_id: None,
        };
        let reader = tokio::fs::File::from_std(file.file);
        let result = match target {
            BlobTarget::Local => self.client.upload_blob(spec, reader).await,
            BlobTarget::Hub => self.client.upload_bridge_blob(spec, reader).await,
        }
        .map_err(|err| ToolError::from_client(err, true))?;
        match (target, result) {
            (
                BlobTarget::Local,
                BlobUploadOutcome::VerifiedLocal(receipt)
                | BlobUploadOutcome::AcknowledgedWithoutLocalVerification(receipt),
            )
            | (BlobTarget::Hub, BlobUploadOutcome::CompleteWithHubDigest(receipt)) => {
                to_value(receipt)
            }
            _ => Err(ToolError::new(
                ErrorCode::DeliveryUnknown,
                "The Node did not return the expected upload receipt",
            )),
        }
    }
}

fn valid_key(key: &str) -> ToolResult<()> {
    validate_idempotency_key(key).map_err(|_| ToolError::invalid("Invalid operation_key"))
}

fn matches_identity(
    identity: &WhoamiResponse,
    node_id: &NodeId,
    participant_id: &ParticipantId,
) -> bool {
    identity.principal_kind == PrincipalKind::Participant
        && identity.node_id.as_ref() == Some(node_id)
        && identity.participant_id.as_ref() == Some(participant_id)
}

async fn get<T: DeserializeOwned>(
    client: &CoopletClient,
    path: &str,
    query: &[(&str, String)],
) -> ToolResult<T> {
    client
        .get_json(path, query)
        .await
        .map_err(|error| ToolError::from_client(error, false))
}

fn decode<T: DeserializeOwned>(input: Value) -> ToolResult<T> {
    if !input.is_object() {
        return Err(ToolError::invalid("Tool arguments must be an object"));
    }
    serde_json::from_value(input).map_err(|_| {
        ToolError::invalid("Tool arguments have missing, unexpected, or invalid fields")
    })
}

fn to_value(input: impl Serialize) -> ToolResult {
    serde_json::to_value(input)
        .map_err(|_| ToolError::new(ErrorCode::InternalError, "Could not encode the Node result"))
}

pub(super) fn private_tree(credential: &Path) -> io::Result<PathBuf> {
    let credential = credential.canonicalize()?;
    credential
        .parent()
        .and_then(Path::parent)
        .map(Path::to_owned)
        .ok_or_else(|| io::Error::other("Invalid credential location"))
}

struct UploadFile {
    file: File,
    bytes: u64,
    sha256: Option<String>,
}

fn prepare_file(
    workspace: &Path,
    relative: &Path,
    private: &Path,
    target: BlobTarget,
) -> ToolResult<UploadFile> {
    let invalid_path = || ToolError::invalid("path must refer to a regular workspace file");
    let read_error = || ToolError::invalid("Could not read the workspace file");
    let forbidden = || {
        ToolError::new(
            ErrorCode::Forbidden,
            "path is outside the permitted workspace files",
        )
    };
    if relative.is_absolute() {
        return Err(invalid_path());
    }
    let workspace = workspace.canonicalize().map_err(|_| invalid_path())?;
    let private_meta = private.symlink_metadata().map_err(|_| read_error())?;
    if !private_meta.is_dir() {
        return Err(read_error());
    }
    let excluded_private = Some((private_meta.dev(), private_meta.ino()));
    let path = workspace.join(relative);
    let resolved = path.canonicalize().map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => {
            ToolError::new(ErrorCode::NotFound, "The workspace file does not exist")
        }
        _ => invalid_path(),
    })?;
    if !resolved.starts_with(&workspace) || resolved.starts_with(private) {
        return Err(forbidden());
    }
    let confined_relative = resolved.strip_prefix(&workspace).map_err(|_| forbidden())?;
    let mut file = cooplet_platform::PrivateFs::open_confined_regular_file(
        &workspace,
        confined_relative,
        excluded_private,
    )
    .map_err(|error| match error {
        cooplet_platform::PlatformError::PathExcluded => forbidden(),
        cooplet_platform::PlatformError::Io(io_err) if io_err.kind() == io::ErrorKind::NotFound => {
            ToolError::new(ErrorCode::NotFound, "The workspace file does not exist")
        }
        _ => invalid_path(),
    })?;
    let opened = file.metadata().map_err(|_| read_error())?;
    if !opened.is_file() {
        return Err(invalid_path());
    }
    if opened.nlink() != 1 {
        return Err(ToolError::new(
            ErrorCode::Forbidden,
            "The upload file must have a single hard link",
        ));
    }
    let too_large = || ToolError::new(ErrorCode::BlobTooLarge, "The file exceeds the upload limit");
    if opened.len() > DEFAULT_MAX_BLOB_BYTES {
        return Err(too_large());
    }
    if matches!(target, BlobTarget::Hub) {
        return Ok(UploadFile {
            file,
            bytes: opened.len(),
            sha256: None,
        });
    }
    let mut hasher = Sha256::new();
    let bytes = io::copy(
        &mut (&mut file).take(DEFAULT_MAX_BLOB_BYTES + 1),
        &mut hasher,
    )
    .map_err(|_| read_error())?;
    if bytes > DEFAULT_MAX_BLOB_BYTES {
        return Err(too_large());
    }
    if bytes != opened.len() {
        return Err(ToolError::new(
            ErrorCode::BlobDigestMismatch,
            "The file changed while preparing its upload",
        ));
    }
    file.rewind().map_err(|_| read_error())?;
    Ok(UploadFile {
        file,
        bytes,
        sha256: Some(format!("{:x}", hasher.finalize())),
    })
}

fn mime_type(path: &Path) -> &'static str {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("txt" | "log") => "text/plain",
        Some("md" | "markdown") => "text/markdown",
        Some("csv") => "text/csv",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("zip") => "application/zip",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs, os::unix::fs::symlink};

    use cooplet_protocol::RequestId;

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> io::Result<Self> {
            let id = RequestId::generate().map_err(io::Error::other)?;
            let root = std::env::temp_dir().join(format!("cooplet-mcp-{id}"));
            fs::create_dir(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn upload_checks_canonical_workspace_and_excludes_private_tree() -> TestResult {
        let fixture = Fixture::new()?;
        let workspace = fixture.0.join("workspace");
        let private = workspace.join("node-state");
        let creds = private.join("creds");
        let credential = creds.join("part.g1.json");
        fs::create_dir_all(&creds)?;
        fs::write(&credential, "credential")?;
        fs::write(private.join("node.master.key"), "private")?;
        fs::write(fixture.0.join("outside.txt"), "outside")?;
        fs::write(workspace.join("upload.TXT"), "hello")?;
        symlink("node-state", workspace.join("alias"))?;
        symlink("../outside.txt", workspace.join("outside-link.txt"))?;
        symlink("upload.TXT", workspace.join("inside-link.txt"))?;

        fs::write(workspace.join("shared.txt"), "shared fixture")?;
        fs::hard_link(
            workspace.join("shared.txt"),
            workspace.join("shared-copy.txt"),
        )?;
        let protected = private_tree(&credential)?;
        assert_eq!(protected, private.canonicalize()?);
        for candidate in [
            "node-state/creds/part.g1.json",
            "node-state/node.master.key",
            "alias/creds/part.g1.json",
            "../outside.txt",
            "outside-link.txt",
            "shared-copy.txt",
        ] {
            let rejected = prepare_file(
                &workspace,
                Path::new(candidate),
                &protected,
                BlobTarget::Hub,
            );
            assert_eq!(
                rejected.err().map(|error| error.code),
                Some(ErrorCode::Forbidden)
            );
        }
        for candidate in [&workspace.join("upload.TXT"), Path::new("node-state/..")] {
            assert!(prepare_file(&workspace, candidate, &protected, BlobTarget::Local).is_err());
        }
        for candidate in ["upload.TXT", "inside-link.txt"] {
            let result = prepare_file(
                &workspace,
                Path::new(candidate),
                &protected,
                BlobTarget::Local,
            )
            .map_err(|error| io::Error::other(error.detail))?;
            let mut bytes = Vec::new();
            (&result.file).read_to_end(&mut bytes)?;
            assert_eq!(bytes, b"hello");
            assert_eq!(result.bytes, 5);
            assert_eq!(
                result.sha256.as_deref(),
                Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
            );
            assert_eq!(mime_type(Path::new(candidate)), "text/plain");
        }
        let relay = prepare_file(
            &workspace,
            Path::new("upload.TXT"),
            &protected,
            BlobTarget::Hub,
        )
        .map_err(|error| io::Error::other(error.detail))?;
        assert_eq!(relay.bytes, 5);
        assert!(relay.sha256.is_none());
        let mut bytes = Vec::new();
        (&relay.file).read_to_end(&mut bytes)?;
        assert_eq!(bytes, b"hello");
        Ok(())
    }

    #[test]
    fn tool_argument_error_never_echoes_fields_or_values() -> TestResult {
        for field in ["from", "author"] {
            let mut args =
                json!({"to": "recipient", "body": "message", "operation_key": "test-key"});
            args[field] = json!("very-sensitive-value");
            let error = decode::<SendArgs>(args)
                .err()
                .ok_or_else(|| io::Error::other("unexpected accepted field"))?;
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            let rendered = serde_json::to_string(&error)?;
            assert!(!rendered.contains("very-sensitive-value"));
        }
        let problem =
            ProblemDetails::new(ErrorCode::QuestionAlreadyAnswered, "very-sensitive-value")
                .with_extension("existing_answer_id", "msg_00000000000000000000000000")
                .with_extension("operation_id", "very-sensitive-value")
                .with_extension("secret", "very-sensitive-value");
        let error = ToolError::from_problem(problem);
        assert_eq!(error.status, Some(409));
        assert_eq!(
            error.extensions["existing_answer_id"],
            "msg_00000000000000000000000000"
        );
        assert!(!serde_json::to_string(&error)?.contains("very-sensitive-value"));
        Ok(())
    }

    #[test]
    fn thread_problem_keeps_safe_resync_and_pin_extensions() {
        let thread = "thr_00000000000000000000000000";
        let resync =
            ProblemDetails::new(ErrorCode::ResyncRequired, "The thread history was pruned")
                .with_extension("thread_id", thread)
                .with_extension("floor_seq", 12)
                .with_extension("head_seq", 15)
                .with_extension("reason", "cursor_pruned");
        let error = ToolError::from_problem(resync);
        assert_eq!(error.status, Some(410));
        assert_eq!(error.extensions["thread_id"], thread);
        assert_eq!(error.extensions["floor_seq"], 12);
        assert_eq!(error.extensions["head_seq"], 15);
        assert_eq!(error.extensions["reason"], "cursor_pruned");

        let conflict = ProblemDetails::new(ErrorCode::PinConflict, "The pin changed")
            .with_extension("current_version", Value::Null)
            .with_extension("reason", "internal/path");
        let error = ToolError::from_problem(conflict);
        assert_eq!(error.status, Some(409));
        assert_eq!(error.extensions["current_version"], Value::Null);
        assert!(!error.extensions.contains_key("reason"));
    }
}
