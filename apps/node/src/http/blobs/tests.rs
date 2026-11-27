use std::{error::Error, fs};

use cooplet_client::{BlobUploadOutcome, BlobUploadSpec, ClientError, UploadVerification};
use cooplet_platform::PrivateFs;
use cooplet_protocol::{NodeId, PrincipalKind};
use futures_util::FutureExt;

use super::*;
use crate::{
    auth::Credential,
    clients::AdapterRegistry,
    config::{NodeConfig, operator_path},
    daemon::Node,
};

type TestResult = std::result::Result<(), Box<dyn Error + Send + Sync>>;

#[test]
fn upload_filename_is_decoded_once_and_rejects_incomplete_or_invalid_encodings() -> TestResult {
    assert_eq!(decode_filename("r%C3%A9sum%C3%A9%252Etxt")?, "résumé%2Etxt");
    for invalid in ["%", "%x0", "%00file", "%09file", "%ff"] {
        assert!(
            matches!(decode_filename(invalid), Err(error) if error.code() == ErrorCode::InvalidRequest)
        );
    }
    Ok(())
}

#[tokio::test]
async fn aborted_body_is_not_a_successful_eof() {
    let body = Body::from_stream(futures_util::stream::iter([
        Ok(Bytes::from_static(b"partial")),
        Err(io::Error::from(io::ErrorKind::ConnectionReset)),
    ]));
    let (_shutdown, stopping) = watch::channel(false);
    let result = BodyReader::new(upload_chunks(body, stopping))
        .read_to_end(&mut Vec::new())
        .await;
    assert!(matches!(
        result,
        Err(error) if error.get_ref().is_some_and(|cause| cause.is::<UploadBodyError>())
    ));
}

#[tokio::test]
async fn shutdown_cancels_an_upload_waiting_for_a_body_frame() -> TestResult {
    let body = Body::from_stream(futures_util::stream::pending::<io::Result<Bytes>>());
    let (shutdown, stopping) = watch::channel(false);
    let mut chunks = upload_chunks(body, stopping);
    assert!(chunks.next().now_or_never().is_none());
    shutdown.send_replace(true);
    let failure = chunks
        .next()
        .await
        .ok_or_else(|| io::Error::other("shutdown produced a successful EOF"))?;
    assert!(
        failure
            .map_err(body_error)
            .is_err_and(|error| error.code() == ErrorCode::Overloaded)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn trickle_upload_exceeding_total_timeout_is_aborted() -> TestResult {
    let start = tokio::time::Instant::now();
    // Send 1 byte every 20s (< 30s per-chunk timeout) to verify the 300s total cap triggers.
    let trickle = futures_util::stream::unfold((), |()| async {
        tokio::time::sleep(Duration::from_secs(20)).await;
        Some((Ok::<Bytes, io::Error>(Bytes::from_static(b"a")), ()))
    });
    let (_shutdown, stopping) = watch::channel(false);
    let mut chunks = upload_chunks(Body::from_stream(trickle), stopping);
    let mut received = 0usize;
    let error = loop {
        match chunks.next().await {
            Some(Ok(_)) => received += 1,
            Some(Err(err)) => break body_error(err),
            None => return Err(io::Error::other("trickle stream ended without timeout").into()),
        }
    };
    assert!((14..=15).contains(&received), "received={received}");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(error.0.extensions["reason"], "upload_timeout");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= UPLOAD_TOTAL_TIMEOUT && elapsed < UPLOAD_TOTAL_TIMEOUT + UPLOAD_CHUNK_TIMEOUT
    );
    Ok(())
}

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn client_replays_an_upload_at_full_quota_without_reading_the_body() -> TestResult {
    let scratch = Scratch(PrivateFs::ensure_private_dir(
        &std::env::temp_dir().join(NodeId::generate()?.as_str()),
    )?);
    let mut config = NodeConfig::new(scratch.0.join("state"), scratch.0.clone());
    let credential_path = operator_path(&config.state_dir);
    let bytes = b"blob-v0";
    config.max_blob_bytes = 32;
    config.max_total_blob_bytes = bytes.len() as u64;
    let node = Node::open(config, AdapterRegistry::default()).await?;
    let shutdown = node.state.shutdown.clone();
    let reads = node.state.reads.clone();
    let storage = node.state.blobs.clone();
    let task = tokio::spawn(node.serve());

    let assertions = async {
        let (client, _) = Credential::read(&credential_path)?.connect(PrincipalKind::Operator).await?;
        let first = BlobUploadSpec {
            filename: "../résumé.txt".into(),
            mime_type: "text/plain; charset=utf-8".into(),
            verification: UploadVerification::LocalSha256,
            expected_sha256: None,
            idempotency_key: Some("blob-replay-key".into()),
            content_length: None,
        };
        let BlobUploadOutcome::VerifiedLocal(attachment) = client.upload_blob(first.clone(), bytes.as_slice()).await? else {
            return Err(io::Error::other("First upload did not reach verified EOF").into());
        };
        assert_eq!(attachment.name, first.filename);

        let complete_claims = BlobUploadSpec {
            content_length: Some(bytes.len() as u64),
            expected_sha256: Some(attachment.sha256.clone()),
            ..first.clone()
        };
        let (reader, mut stalled) = tokio::io::duplex(8);
        tokio::io::AsyncWriteExt::write_all(&mut stalled, &bytes[..1]).await?;
        let replay = tokio::time::timeout(
            std::time::Duration::from_secs(5), client.upload_blob(complete_claims, reader),
        ).await??;
        assert!(matches!(
            replay,
            BlobUploadOutcome::AcknowledgedWithoutLocalVerification(current) if current == attachment
        ));
        let replay = client.upload_blob(first.clone(), bytes.as_slice()).await?;
        assert!(matches!(replay, BlobUploadOutcome::VerifiedLocal(current) if current == attachment));

        let changed = client.upload_blob(first.clone(), b"new".as_slice()).await;
        assert_problem(changed, ErrorCode::OperationConflict)?;
        let fresh = BlobUploadSpec { idempotency_key: Some("fresh-blob-key".into()), ..first };
        let result = client.upload_blob(fresh, b"x".as_slice()).await;
        assert_problem(result, ErrorCode::StorageFull)?;
        let large = BlobUploadSpec {
            filename: "oversized.bin".into(),
            mime_type: "application/octet-stream".into(),
            verification: UploadVerification::LocalSha256,
            expected_sha256: None,
            idempotency_key: None,
            content_length: None,
        };
        let result = client.upload_blob(large, [0u8; 33].as_slice()).await;
        assert_problem(result, ErrorCode::BlobTooLarge)?;

        let mut downloaded = Vec::new();
        client.download_blob_to_writer(&attachment.blob_id, Some(&attachment.sha256), 32, &mut downloaded).await?;
        assert_eq!(downloaded, bytes);
        assert_eq!(storage.used_bytes(), bytes.len() as u64);
        let observed = reads.read(|conn| {
            Ok(conn.query_row(
                "SELECT (SELECT count(*) FROM blobs), (SELECT count(*) FROM operations),
                        (SELECT count(*) FROM events WHERE kind = 'blob.stored')",
                [],
                |row| Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?, row.get::<_, u32>(2)?)),
            )?)
        }).await?;
        assert_eq!(observed, (1, 1, 1));
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    }.await;
    shutdown.send_replace(true);
    task.await??;
    assertions
}

fn assert_problem(
    result: std::result::Result<BlobUploadOutcome, ClientError>,
    expected: ErrorCode,
) -> TestResult {
    match result {
        Err(ClientError::Problem(problem)) if problem.code == expected => Ok(()),
        result => Err(io::Error::other(format!("Expected {expected}, received {result:?}")).into()),
    }
}

#[tokio::test]
async fn concurrent_same_key_cross_kind_receipts_and_slot_limit_are_enforced() -> TestResult {
    use cooplet_storage::WriteOutcome;

    let scratch = Scratch(PrivateFs::ensure_private_dir(
        &std::env::temp_dir().join(NodeId::generate()?.as_str()),
    )?);
    let config = NodeConfig::new(scratch.0.join("state"), scratch.0.clone());
    let credential_path = operator_path(&config.state_dir);
    let node = Node::open(config, AdapterRegistry::default()).await?;
    let db = node.state.db.clone();
    let auth = node.state.auth.clone();
    let tasks = node.state.tasks.clone();
    let endpoint = Credential::read(&credential_path)?.address()?;
    let node_id = node.state.info.node_id.clone();
    let shutdown = node.state.shutdown.clone();
    let task = tokio::spawn(node.serve());

    let assertions = async {
        let (client, _) = Credential::read(&credential_path)?
            .connect(PrincipalKind::Operator)
            .await?;
        let actor_id = client.whoami().await?.actor_id;

        // 1. Cross-kind conflict rejects before reading the body stream.
        let seed_actor = actor_id.clone();
        let bytes = b"hello-blob";
        let expected_sha: String = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let expected_hash =
            fingerprint("check.txt", "text/plain", bytes.len() as u64, &expected_sha)?;
        let pending_hash = expected_hash.clone();
        let unknown_hash = expected_hash;
        db.write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO operations (operation_id, actor_id, operation_key, kind, request_sha256,
                                         state, created_at_ms, updated_at_ms, expires_at_ms)
                 VALUES ('op_msg', ?1, 'cross-kind-key', 'message.send', 'abc', 'succeeded', 1, 1, 9999999999999),
                        ('op_pend', ?1, 'pending-blob-key', 'blob.put', ?2, 'in_progress', 1, 1, 9999999999999),
                        ('op_unk', ?1, 'unknown-blob-key', 'blob.put', ?3, 'unknown', 1, 1, 9999999999999)",
                params![seed_actor, pending_hash, unknown_hash],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

        let (reader, _stalled) = tokio::io::duplex(8);
        let cross_spec = BlobUploadSpec {
            filename: "a.txt".into(),
            mime_type: "text/plain".into(),
            verification: UploadVerification::LocalSha256,
            expected_sha256: None,
            idempotency_key: Some("cross-kind-key".into()),
            content_length: None,
        };
        let cross = tokio::time::timeout(
            Duration::from_secs(5),
            client.upload_blob(cross_spec, reader),
        )
        .await?;
        assert_problem(cross, ErrorCode::OperationConflict)?;

        // 2. Stored pending/unknown receipts check request_sha256 before returning state.
        let check_spec = |key: &str| BlobUploadSpec {
            filename: "check.txt".into(),
            mime_type: "text/plain".into(),
            verification: UploadVerification::LocalSha256,
            expected_sha256: None,
            idempotency_key: Some(key.into()),
            content_length: None,
        };
        assert_problem(
            client
                .upload_blob(check_spec("pending-blob-key"), bytes.as_slice())
                .await,
            ErrorCode::OperationInProgress,
        )?;
        assert_problem(
            client
                .upload_blob(check_spec("pending-blob-key"), b"different".as_slice())
                .await,
            ErrorCode::OperationConflict,
        )?;
        assert_problem(
            client
                .upload_blob(check_spec("unknown-blob-key"), bytes.as_slice())
                .await,
            ErrorCode::DeliveryUnknown,
        )?;
        assert_problem(
            client
                .upload_blob(check_spec("unknown-blob-key"), b"different".as_slice())
                .await,
            ErrorCode::OperationConflict,
        )?;

        // 3. Concurrent same-key upload returns 409 operation_in_progress via guard_unhashed.
        let (first_reader, mut first_writer) = tokio::io::duplex(64);
        tokio::io::AsyncWriteExt::write_all(&mut first_writer, b"part1").await?;
        let first_client = client.clone();
        let same_key_spec = BlobUploadSpec {
            filename: "same.txt".into(),
            mime_type: "text/plain".into(),
            verification: UploadVerification::LocalSha256,
            expected_sha256: None,
            idempotency_key: Some("live-same-key".into()),
            content_length: Some(11),
        };
        let first_task = tokio::spawn({
            let spec = same_key_spec.clone();
            async move { first_client.upload_blob(spec, first_reader).await }
        });
        // Wait until first_task has entered put_inner and acquired its upload slot (past guard_unhashed).
        tokio::time::timeout(Duration::from_secs(5), async {
            while tasks.available_upload_slots() == 8 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let duplicate_spec = BlobUploadSpec {
            content_length: None,
            ..same_key_spec.clone()
        };
        assert_problem(
            client.upload_blob(duplicate_spec, b"second".as_slice()).await,
            ErrorCode::OperationInProgress,
        )?;
        tokio::io::AsyncWriteExt::write_all(&mut first_writer, b"-part2").await?;
        drop(first_writer);
        let uploaded = match tokio::time::timeout(Duration::from_secs(5), first_task).await?? {
            Ok(BlobUploadOutcome::VerifiedLocal(att)) => att,
            other => {
                return Err(
                    io::Error::other(format!("Unexpected upload outcome: {other:?}")).into(),
                )
            }
        };
        assert_eq!(uploaded.size_bytes, 11);

        // 4. Concurrent upload semaphore rejects 9th simultaneous upload with 503 Overloaded.
        let mut holders = Vec::new();
        let mut upload_tasks = Vec::new();
        for idx in 0..8 {
            let (reader, mut writer) = tokio::io::duplex(64);
            tokio::io::AsyncWriteExt::write_all(&mut writer, b"x").await?;
            holders.push(writer);
            let c = client.clone();
            let spec = BlobUploadSpec {
                filename: format!("slot-{idx}.bin"),
                mime_type: "application/octet-stream".into(),
                verification: UploadVerification::LocalSha256,
                expected_sha256: None,
                idempotency_key: None,
                content_length: Some(2),
            };
            upload_tasks.push(tokio::spawn(async move { c.upload_blob(spec, reader).await }));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while tasks.available_upload_slots() > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let overflow_spec = BlobUploadSpec {
            filename: "overflow.bin".into(),
            mime_type: "application/octet-stream".into(),
            verification: UploadVerification::LocalSha256,
            expected_sha256: None,
            idempotency_key: None,
            content_length: None,
        };
        assert_problem(
            client.upload_blob(overflow_spec, b"z".as_slice()).await,
            ErrorCode::Overloaded,
        )?;
        // Replay without complete claims must also acquire an upload slot and get 503 when saturated.
        let unverified_replay = BlobUploadSpec {
            content_length: None,
            expected_sha256: None,
            ..same_key_spec.clone()
        };
        assert_problem(
            client
                .upload_blob(unverified_replay, b"part1-part2".as_slice())
                .await,
            ErrorCode::Overloaded,
        )?;
        // Replay with complete claims verifies against stored receipt without needing a slot or body.
        let claimed_replay = BlobUploadSpec {
            content_length: Some(uploaded.size_bytes),
            expected_sha256: Some(uploaded.sha256.clone()),
            ..same_key_spec.clone()
        };
        let (stalled_reader, mut stalled_writer) = tokio::io::duplex(8);
        tokio::io::AsyncWriteExt::write_all(&mut stalled_writer, b"p").await?;
        let fast_replay = tokio::time::timeout(
            Duration::from_secs(5),
            client.upload_blob(claimed_replay, stalled_reader),
        )
        .await??;
        assert!(matches!(
            fast_replay,
            BlobUploadOutcome::AcknowledgedWithoutLocalVerification(current) if current == uploaded
        ));
        for mut writer in holders {
            tokio::io::AsyncWriteExt::write_all(&mut writer, b"y").await?;
        }
        for handle in upload_tasks {
            assert!(matches!(
                handle.await??,
                BlobUploadOutcome::VerifiedLocal(_)
            ));
        }

        // 5. Partial-index blob ACL + inactive participant without Collaborate scope denied.
        let peer_id = cooplet_protocol::ParticipantId::generate()?;
        let stranger_id = cooplet_protocol::ParticipantId::generate()?;
        let inactive_id = cooplet_protocol::ParticipantId::generate()?;
        let peer_cred = Credential::new(node_id.clone(), peer_id.clone(), endpoint)?;
        let stranger_cred = Credential::new(node_id.clone(), stranger_id.clone(), endpoint)?;
        let inactive_cred = Credential::new(node_id.clone(), inactive_id.clone(), endpoint)?;
        let attachments_json = serde_json::to_string(&vec![uploaded.clone()])?;
        let pid = peer_id.clone();
        let sid = stranger_id.clone();
        let iid = inactive_id.clone();
        let peer_insert = peer_cred.clone();
        let stranger_insert = stranger_cred.clone();
        let inactive_insert = inactive_cred.clone();
        let sender = actor_id;
        let auth_writer = auth.clone();
        db.write_sqlite(move |tx| {
            for (part, cred) in [
                (&pid, &peer_insert),
                (&sid, &stranger_insert),
                (&iid, &inactive_insert),
            ] {
                tx.execute(
                    "INSERT INTO participants(participant_id, kind, name, state, created_at_ms, updated_at_ms)
                     VALUES (?1, 'agent', ?1, 'idle', 1, 1)",
                    [part.as_str()],
                )?;
                auth_writer.insert_credential(tx, cred, PrincipalKind::Participant, 1)?;
            }
            tx.execute(
                "UPDATE participant_credentials SET scopes_json = '[]' WHERE participant_id = ?1",
                [iid.as_str()],
            )?;
            for recipient in [pid.as_str(), iid.as_str()] {
                let msg_id = cooplet_protocol::MessageId::generate()
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                tx.execute(
                    "INSERT INTO messages(message_id, sender_id, recipient_id, body, attachments_json,
                                          delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                     VALUES (?1, ?2, ?3, 'attached', ?4, 'direct_now', 'delivered', 1, 9999999999999)",
                    params![msg_id.as_str(), sender, recipient, attachments_json],
                )?;
            }
            Ok(WriteOutcome::new(()))
        })
        .await?;
        let peer_client = peer_cred.connect(PrincipalKind::Participant).await?.0;
        let stranger_client = stranger_cred.connect(PrincipalKind::Participant).await?.0;
        let inactive_client = inactive_cred.connect(PrincipalKind::Participant).await?.0;

        let mut downloaded = Vec::new();
        peer_client
            .download_blob_to_writer(
                &uploaded.blob_id,
                Some(&uploaded.sha256),
                64,
                &mut downloaded,
            )
            .await?;
        assert_eq!(downloaded, b"part1-part2");
        assert!(matches!(
            stranger_client
                .download_blob_to_writer(&uploaded.blob_id, None, 64, &mut Vec::new())
                .await,
            Err(ClientError::Problem(problem)) if problem.code == ErrorCode::Forbidden
        ));
        assert!(matches!(
            inactive_client
                .download_blob_to_writer(&uploaded.blob_id, None, 64, &mut Vec::new())
                .await,
            Err(ClientError::Problem(problem)) if problem.code == ErrorCode::Forbidden
        ));
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    }
    .await;
    shutdown.send_replace(true);
    task.await??;
    assertions
}
