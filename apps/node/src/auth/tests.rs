use std::{error::Error, fs, path::PathBuf};

use cooplet_client::ClientError;
use cooplet_storage::WriteOutcome;
use serde_json::{Value, json};

use super::*;
use crate::{
    clients::AdapterRegistry,
    config::{NodeConfig, OpenNode, operator_path},
    daemon::Node,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn adapter_client(node: &Node, scope: ServiceScope) -> TestResult<CoopletClient> {
    let mut secret = [0; 32];
    random_bytes(&mut secret)?;
    let token = hex(&secret);
    let verifier = hex(&node
        .state
        .auth
        .mac(PrincipalKind::Adapter, &token)?
        .finalize()
        .into_bytes());
    let token_id = TokenId::generate()?;
    node.state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO participant_credentials
             (token_id, principal_kind, actor_id, scopes_json, verifier_hmac, created_at_ms)
             VALUES (?1, 'adapter', ?2, json_array(?2), ?3, 0)",
                params![token_id.as_str(), scope.as_str(), verifier],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    Ok(CoopletClient::new(ClientConfig::new(
        format!("http://{}", node.address()?).parse()?,
        Destination::LocalNode,
        Some(BearerToken::new(&token)?),
    ))?)
}

fn problem(result: std::result::Result<Value, ClientError>, expected: ErrorCode) {
    assert!(matches!(result, Err(ClientError::Problem(error)) if error.code == expected));
}

#[tokio::test]
async fn delegated_bearers_obey_http_scope_and_operator_only_shutdown() -> TestResult {
    let root = Scratch(PrivateFs::ensure_private_dir(
        &std::env::temp_dir().join(NodeId::generate()?.as_str()),
    )?);
    let state = root.0.join("state");
    let node = Node::open(
        NodeConfig::new(state.clone(), root.0.clone()),
        AdapterRegistry::default(),
    )
    .await?;
    let operator = Credential::read(&operator_path(&state))?.participant_id;
    let read = adapter_client(&node, ServiceScope::Read).await?;
    let manage = adapter_client(&node, ServiceScope::Manage).await?;
    let shutdown = node.state.shutdown.clone();
    let task = tokio::spawn(node.serve());
    let message = json!({"to":operator, "body":"fixture"});
    let agent = json!({"client_kind":"unregistered"});
    assert_eq!(read.whoami().await?.principal_kind, PrincipalKind::Adapter);
    let _: Value = read.get_json("/v1/events", &[]).await?;
    problem(
        read.post_json("/v1/messages", &message, Some("read-message"))
            .await,
        ErrorCode::Forbidden,
    );
    problem(
        read.post_json("/v1/agents", &agent, Some("read-agent"))
            .await,
        ErrorCode::Forbidden,
    );
    let sent: Value = manage
        .post_json("/v1/messages", &message, Some("manage-message"))
        .await?;
    assert_eq!(sent["delivery_state"], "delivered");
    let page: Value = read.get_json("/v1/messages", &[]).await?;
    assert_eq!(page["items"][0]["message_id"], sent["message_id"]);
    // Manage reaches adapter validation on the empty registry.
    problem(
        manage
            .post_json("/v1/agents", &agent, Some("manage-agent"))
            .await,
        ErrorCode::InvalidRequest,
    );
    problem(
        manage
            .post_json("/v1/node/stop", &json!({}), Some("manage-stop"))
            .await,
        ErrorCode::Forbidden,
    );
    assert_eq!(manage.health().await?.status, "ok");
    shutdown.send_replace(true);
    task.await??;
    Ok(())
}

#[tokio::test]
async fn auth_lookup_fails_closed_when_participant_kind_mismatches_principal_kind() -> TestResult {
    let root = Scratch(PrivateFs::ensure_private_dir(
        &std::env::temp_dir().join(NodeId::generate()?.as_str()),
    )?);
    let state = root.0.join("state");
    let node = OpenNode::start(NodeConfig::new(state.clone(), root.0.clone())).await?;
    let now = crate::now_ms()?;
    let op_cred = Credential::read(&operator_path(&state))?;

    // Valid operator credential maps to AuthorKind::Human.
    let operator = node
        .auth
        .authenticate(node.engine.reads(), op_cred.token.clone(), now)
        .await?;
    assert_eq!(operator.author_kind(), AuthorKind::Human);

    // If an agent row is tampered into having an operator credential (or vice versa),
    // lookup must fail closed with Unauthorized.
    let agent_id = ParticipantId::generate()?;
    let forged_op_cred = Credential::new(
        node.info.node_id.clone(),
        agent_id.clone(),
        op_cred.address()?,
    )?;
    let forged_part_cred = Credential::new(
        node.info.node_id.clone(),
        op_cred.participant_id.clone(),
        op_cred.address()?,
    )?;
    let auth = node.auth.clone();
    let aid = agent_id.clone();
    let fop = forged_op_cred.clone();
    let fpart = forged_part_cred.clone();
    node.engine
        .db()
        .write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO participants (participant_id, kind, name, state, created_at_ms, updated_at_ms)
                 VALUES (?1, 'agent', 'tampered-agent', 'idle', ?2, ?2)",
                params![aid.as_str(), now],
            )?;
            auth.insert_credential(tx, &fop, PrincipalKind::Operator, now)?;
            auth.insert_credential(tx, &fpart, PrincipalKind::Participant, now)?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let err_op = node
        .auth
        .authenticate(node.engine.reads(), forged_op_cred.token, now)
        .await
        .err()
        .ok_or("Operator credential linked to agent row must fail closed")?;
    assert_eq!(err_op.code(), ErrorCode::Unauthorized);

    let err_part = node
        .auth
        .authenticate(node.engine.reads(), forged_part_cred.token, now)
        .await
        .err()
        .ok_or("Participant credential linked to human row must fail closed")?;
    assert_eq!(err_part.code(), ErrorCode::Unauthorized);

    // Adapter and WorkflowRun credentials linked to a participant row must also fail closed.
    for (principal_kind, kind_str) in [
        (PrincipalKind::Adapter, "adapter"),
        (PrincipalKind::WorkflowRun, "workflow_run"),
    ] {
        let mut raw = [0u8; 32];
        random_bytes(&mut raw)?;
        let token = hex(&raw);
        let verifier = hex(&node
            .auth
            .mac(principal_kind, &token)?
            .finalize()
            .into_bytes());
        let tok_id = TokenId::generate()?;
        let op_pid = op_cred.participant_id.clone();
        node.engine
            .db()
            .write_sqlite(move |tx| {
                tx.execute(
                    "INSERT INTO participant_credentials
                     (token_id, participant_id, principal_kind, actor_id, scopes_json, verifier_hmac, created_at_ms)
                     VALUES (?1, ?2, ?3, 'tampered', '[\"read\"]', ?4, ?5)",
                    params![tok_id.as_str(), op_pid.as_str(), kind_str, verifier, now],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        let err = node
            .auth
            .authenticate(node.engine.reads(), token, now)
            .await
            .err()
            .ok_or("Adapter/WorkflowRun linked to participant row must fail closed")?;
        assert_eq!(err.code(), ErrorCode::Unauthorized);
    }

    node.engine.shutdown().await?;
    Ok(())
}
