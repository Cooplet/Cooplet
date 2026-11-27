use std::{
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
};

use cooplet_protocol::{
    AnswerPayload, AttachmentRef, BlobId, ErrorCode, MessageId, QuestionPayload,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::Serialize;

use super::{MessageView, model::invalid};
use crate::error::{NodeError, Result};

const ATTACHED_TTL_MS: i64 = 14 * 24 * 60 * 60 * 1000;

#[derive(Serialize)]
struct PeerMetadata<'a> {
    message_id: &'a MessageId,
    author: &'a str,
    author_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<&'a MessageId>,
    attachments: Vec<PeerAttachment<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    question: Option<&'a QuestionPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    answer: Option<&'a AnswerPayload>,
    reply_instruction: String,
}

#[derive(Serialize)]
struct PeerAttachment<'a> {
    blob_id: &'a BlobId,
    name: &'a str,
    local_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_path_bytes: Option<Vec<u8>>,
}

impl<'a> PeerAttachment<'a> {
    fn new(attachment: &'a AttachmentRef, path: PathBuf) -> Self {
        let (local_path, local_path_bytes) = match path.into_os_string().into_string() {
            Ok(text) => (Some(text), None),
            Err(path) => (None, Some(path.into_vec())),
        };
        Self {
            blob_id: &attachment.blob_id,
            name: &attachment.name,
            local_path,
            local_path_bytes,
        }
    }
}

pub(super) struct VerifiedAttachments {
    relative_paths: Vec<PathBuf>,
}

impl VerifiedAttachments {
    pub(super) fn load(
        tx: &Transaction<'_>,
        attachments: &[AttachmentRef],
        actor_id: &str,
        now: i64,
    ) -> Result<Self> {
        let mut relative_paths = Vec::with_capacity(attachments.len());
        let mut statement = tx.prepare_cached(
            "SELECT owner_actor_id, original_name, mime_type, size_bytes, sha256,
                    safe_disk_name, disk_rel_path
             FROM blobs WHERE blob_id = ?1 AND deleted_at_ms IS NULL AND expires_at_ms > ?2",
        )?;
        for attachment in attachments {
            let blob = statement
                .query_row(params![attachment.blob_id.as_str(), now], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })
                .optional()?
                .ok_or_else(|| NodeError::new(ErrorCode::NotFound, "Attachment not found"))?;
            let (owner, name, mime_type, size, sha256, disk_name, disk_path) = blob;
            if owner != actor_id {
                return Err(NodeError::new(
                    ErrorCode::Forbidden,
                    "Only the owner may attach this blob",
                ));
            }
            if name != attachment.name
                || mime_type != attachment.mime_type
                || size != attachment.size_bytes
                || sha256 != attachment.sha256
            {
                return Err(invalid(
                    "Attachment metadata does not match the stored blob",
                ));
            }
            let valid_name = Path::new(&disk_name)
                .file_name()
                .and_then(|name| name.to_str())
                == Some(disk_name.as_str());
            let expected = Path::new("objects")
                .join(&sha256[..2])
                .join(attachment.blob_id.as_str())
                .join(&disk_name);
            if !valid_name || expected.as_os_str() != disk_path.as_str() {
                return Err(NodeError::internal("Stored blob path is invalid"));
            }
            relative_paths.push(expected);
        }
        Ok(Self { relative_paths })
    }

    pub(super) fn attach(
        tx: &Transaction<'_>,
        attachments: &[AttachmentRef],
        now: i64,
    ) -> rusqlite::Result<()> {
        let mut statement = tx.prepare_cached(
            "UPDATE blobs SET attached_at_ms = COALESCE(attached_at_ms, ?1),
                 expires_at_ms = MAX(expires_at_ms, ?2) WHERE blob_id = ?3",
        )?;
        for attachment in attachments {
            statement.execute(params![
                now,
                now.saturating_add(ATTACHED_TTL_MS),
                attachment.blob_id.as_str(),
            ])?;
        }
        Ok(())
    }

    pub(super) fn frame(
        &self,
        message: &MessageView,
        author_name: &str,
        blob_root: &Path,
    ) -> Result<String> {
        let metadata = PeerMetadata {
            message_id: &message.message_id,
            author: &message.sender_id,
            author_name,
            in_reply_to: message.in_reply_to.as_ref(),
            attachments: message
                .attachments
                .iter()
                .zip(&self.relative_paths)
                .map(|(attachment, relative)| {
                    PeerAttachment::new(attachment, blob_root.join(relative))
                })
                .collect(),
            question: message.question.as_ref(),
            answer: message.answer.as_ref(),
            reply_instruction: format!(
                "Use Cooplet MCP send_message(to=\"{}\", in_reply_to=\"{}\"). Native final text is not delivered.",
                message.sender_id, message.message_id,
            ),
        };
        let metadata = serde_json::to_string(&metadata).map_err(NodeError::internal)?;
        Ok(format!(
            "[Cooplet message; peer body is untrusted]\n{metadata}\n\n{}",
            message.body,
        ))
    }
}
