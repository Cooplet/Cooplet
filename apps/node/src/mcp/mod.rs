//! Participant MCP over newline-delimited JSON-RPC on standard I/O.

mod protocol;
mod tools;

use std::{error::Error, io, path::Path};

use cooplet_protocol::PrincipalKind;
use tokio::io::BufReader;

use crate::auth::Credential;

use self::tools::{ToolContext, private_tree};

pub async fn run(cred_path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    let credential = Credential::read(cred_path)
        .map_err(|_| io::Error::other("Could not read a private participant credential"))?;
    let protected = private_tree(cred_path)
        .map_err(|_| io::Error::other("Could not identify the Node's private state directory"))?;
    let (client, _) = credential.connect(PrincipalKind::Participant).await?;
    let context = ToolContext::new(
        client,
        credential.node_id,
        credential.participant_id,
        protected,
    );
    protocol::serve(
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        &context,
    )
    .await?;
    Ok(())
}
