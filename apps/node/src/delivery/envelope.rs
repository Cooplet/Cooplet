use std::path::{Path, PathBuf};

use cooplet_protocol::{
    AnswerPayload, AttachmentRef, AuthorKind, ErrorCode, MessageId, QuestionPayload,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use super::{MessageView, model::invalid};
use crate::{
    clients::{InboundMessage, ResolvedAttachment, TurnInput, VerifiedAuthor},
    error::{NodeError, Result},
};

const ATTACHED_TTL_MS: i64 = 14 * 24 * 60 * 60 * 1000;
const QUEUE_ENVELOPE_VERSION: u8 = 2;
const LEGACY_V1_PREFIX: &str = "[Cooplet message; peer body is untrusted]\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CorruptQueueEnvelope;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredQueueEnvelope {
    v: u8,
    input: TurnInput,
}

#[derive(Deserialize)]
struct LegacyV1Metadata {
    message_id: MessageId,
    author: String,
    author_name: String,
    #[serde(default)]
    in_reply_to: Option<MessageId>,
    #[serde(default)]
    attachments: Vec<ResolvedAttachment>,
    #[serde(default)]
    question: Option<QuestionPayload>,
    #[serde(default)]
    answer: Option<AnswerPayload>,
}

pub(super) fn encode_queue_envelope(input: &TurnInput) -> Result<String> {
    serde_json::to_string(&StoredQueueEnvelope {
        v: QUEUE_ENVELOPE_VERSION,
        input: input.clone(),
    })
    .map_err(NodeError::internal)
}

pub(super) fn decode_queue_envelope(
    raw: &str,
    expected_message_id: &MessageId,
) -> std::result::Result<TurnInput, CorruptQueueEnvelope> {
    if let Some(rest) = raw.strip_prefix(LEGACY_V1_PREFIX) {
        return decode_legacy_v1(rest, expected_message_id);
    }
    let stored: StoredQueueEnvelope =
        serde_json::from_str(raw).map_err(|_| CorruptQueueEnvelope)?;
    if stored.v != QUEUE_ENVELOPE_VERSION {
        return Err(CorruptQueueEnvelope);
    }
    let TurnInput::Direct(ref msg) = stored.input else {
        return Err(CorruptQueueEnvelope);
    };
    if &msg.message_id != expected_message_id || msg.author.actor_id.trim().is_empty() {
        return Err(CorruptQueueEnvelope);
    }
    Ok(stored.input)
}

fn decode_legacy_v1(
    rest: &str,
    expected_message_id: &MessageId,
) -> std::result::Result<TurnInput, CorruptQueueEnvelope> {
    let (header_json, body) = rest.split_once("\n\n").ok_or(CorruptQueueEnvelope)?;
    let meta: LegacyV1Metadata =
        serde_json::from_str(header_json).map_err(|_| CorruptQueueEnvelope)?;
    if &meta.message_id != expected_message_id || meta.author.trim().is_empty() {
        return Err(CorruptQueueEnvelope);
    }
    // Fail closed on legacy untyped v1 rows: never upgrade to AuthorKind::Human.
    Ok(TurnInput::direct(InboundMessage {
        message_id: meta.message_id,
        author: VerifiedAuthor {
            kind: AuthorKind::Service,
            actor_id: meta.author,
            name: meta.author_name,
            source_node_id: None,
        },
        in_reply_to: meta.in_reply_to,
        body: body.to_owned(),
        attachments: meta.attachments,
        question: meta.question,
        answer: meta.answer,
        expect_mcp_reply: true,
    }))
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

    pub(super) fn turn_input(
        &self,
        message: &MessageView,
        author_kind: AuthorKind,
        author_name: &str,
        blob_root: &Path,
    ) -> TurnInput {
        let attachments = message
            .attachments
            .iter()
            .zip(&self.relative_paths)
            .map(|(attachment, relative)| {
                ResolvedAttachment::from_path(attachment, blob_root.join(relative))
            })
            .collect();
        TurnInput::direct(InboundMessage {
            message_id: message.message_id.clone(),
            author: VerifiedAuthor {
                kind: author_kind,
                actor_id: message.sender_id.clone(),
                name: author_name.to_owned(),
                source_node_id: None,
            },
            in_reply_to: message.in_reply_to.clone(),
            body: message.body.clone(),
            attachments,
            question: message.question.clone(),
            answer: message.answer.clone(),
            expect_mcp_reply: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn queue_envelope_roundtrips_v2_and_fails_closed_on_legacy_v1_and_corrupt_rows() -> TestResult {
        let message_id = MessageId::from_parts(7, [0; 10])?;
        let input = TurnInput::direct(InboundMessage {
            message_id: message_id.clone(),
            author: VerifiedAuthor {
                kind: AuthorKind::Human,
                actor_id: "part_00000000000000000000000001".into(),
                name: "operator".into(),
                source_node_id: None,
            },
            in_reply_to: None,
            body: "hello".into(),
            attachments: Vec::new(),
            question: None,
            answer: None,
            expect_mcp_reply: true,
        });
        let encoded = encode_queue_envelope(&input)?;
        assert_eq!(decode_queue_envelope(&encoded, &message_id), Ok(input));

        // Legacy v1 text row claiming operator name must decode fail-closed as non-human Service,
        // preserving optional attachments (UTF-8 and non-UTF-8), in_reply_to, question/answer, and empty body.
        let reply_id = MessageId::from_parts(6, [0; 10])?;
        let blob_utf8 = cooplet_protocol::BlobId::from_parts(1, [0; 10])?;
        let blob_bytes = cooplet_protocol::BlobId::from_parts(2, [0; 10])?;
        let v1_header = json!({
            "message_id": message_id.as_str(),
            "author": "part_00000000000000000000000001",
            "author_name": "operator",
            "in_reply_to": reply_id.as_str(),
            "attachments": [
                {
                    "blob_id": blob_utf8.as_str(),
                    "name": "a.txt",
                    "local_path": "/tmp/a.txt"
                },
                {
                    "blob_id": blob_bytes.as_str(),
                    "name": "b.bin",
                    "local_path": null,
                    "local_path_bytes": [47, 116, 109, 112, 47, 255]
                }
            ],
            "question": {
                "prompt": "Proceed?",
                "mode": "text",
                "options": [],
                "allow_free_text": true
            },
            "answer": {
                "selected_option_ids": [],
                "text": "Yes"
            },
            "reply_instruction": "old"
        });
        let legacy = format!("{LEGACY_V1_PREFIX}{v1_header}\n\n");
        let decoded =
            decode_queue_envelope(&legacy, &message_id).map_err(|_| "expected legacy v1 decode")?;
        assert!(!decoded.is_direct_human());
        let TurnInput::Direct(legacy_msg) = decoded else {
            return Err("expected direct input".into());
        };
        assert_eq!(legacy_msg.author.kind, AuthorKind::Service);
        assert_eq!(legacy_msg.in_reply_to.as_ref(), Some(&reply_id));
        assert_eq!(legacy_msg.attachments.len(), 2);
        assert_eq!(
            legacy_msg.attachments[0].local_path.as_deref(),
            Some("/tmp/a.txt")
        );
        assert_eq!(legacy_msg.attachments[1].local_path, None);
        assert_eq!(
            legacy_msg.attachments[1].local_path_bytes.as_deref(),
            Some(&[47u8, 116, 109, 112, 47, 255][..])
        );
        assert!(legacy_msg.question.is_some());
        assert!(legacy_msg.answer.is_some());
        assert!(legacy_msg.body.is_empty());

        // Corrupt, wrong version, wrong variant (Thread), blank actor_id, and mismatched message_id rows must fail decode.
        let thread_envelope =
            encode_queue_envelope(&TurnInput::Thread(crate::clients::InboundThreadBatch {
                thread_id: cooplet_protocol::ThreadId::from_parts(1, [0; 10])?,
                from_seq: 1,
                to_seq: 1,
                truncated_earlier: false,
                posts: Vec::new(),
            }))?;
        let blank_actor_envelope = encode_queue_envelope(&TurnInput::direct(InboundMessage {
            message_id: message_id.clone(),
            author: VerifiedAuthor {
                kind: AuthorKind::Agent,
                actor_id: "   ".into(),
                name: "peer".into(),
                source_node_id: None,
            },
            in_reply_to: None,
            body: "hi".into(),
            attachments: Vec::new(),
            question: None,
            answer: None,
            expect_mcp_reply: true,
        }))?;
        for bad in [
            "",
            "{}",
            r#"{"v":1,"input":{}}"#,
            r#"{"v":99,"input":{}}"#,
            "not json at all",
            "[Cooplet message; peer body is untrusted]\n{broken}\n\nbody",
            thread_envelope.as_str(),
            blank_actor_envelope.as_str(),
        ] {
            assert_eq!(
                decode_queue_envelope(bad, &message_id),
                Err(CorruptQueueEnvelope),
                "unexpectedly accepted {bad:?}"
            );
        }
        let other_id = MessageId::from_parts(8, [0; 10])?;
        assert_eq!(
            decode_queue_envelope(&encoded, &other_id),
            Err(CorruptQueueEnvelope)
        );
        Ok(())
    }
}
