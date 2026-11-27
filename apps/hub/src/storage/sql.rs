use std::{collections::BTreeSet, error::Error, io, str::FromStr};

use cooplet_protocol::NodeId;
use cooplet_storage::ReadPool;
use rusqlite::{Row, Transaction, types::Type};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    auth::{AuthenticatedCaller, reverify_caller_tx},
    error::HubError,
    now_ms,
};

use super::events::EventAudience;

pub(crate) fn parse_col<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    let text: String = row.get(index)?;
    text.parse().map_err(|error| conversion_error(index, error))
}

pub(crate) fn parse_opt_col<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<T>>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    row.get::<_, Option<String>>(index)?
        .map(|text| text.parse().map_err(|error| conversion_error(index, error)))
        .transpose()
}

pub(crate) fn parse_json<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    serde_json::from_str(&text).map_err(|error| conversion_error(index, error))
}

pub(crate) fn parse_opt_json<T: DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    row.get::<_, Option<String>>(index)?
        .map(|text| serde_json::from_str(&text).map_err(|error| conversion_error(index, error)))
        .transpose()
}

pub(crate) fn parse_variant<T>(
    row: &Row<'_>,
    index: usize,
    parse: impl FnOnce(&str) -> Option<T>,
) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    parse(&text).ok_or_else(|| conversion_error(index, io::Error::other("invalid database enum")))
}

pub(crate) fn conversion_error(
    index: usize,
    error: impl Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
}

struct ByteCounter(usize);

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("serialized JSON length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn json_byte_len<T: Serialize>(value: &T) -> serde_json::Result<usize> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub(crate) async fn read_tx<T, F>(reads: &ReadPool, read: F) -> Result<T, HubError>
where
    T: Send + 'static,
    F: FnOnce(&Transaction<'_>) -> Result<T, HubError> + Send + 'static,
{
    reads
        .read(move |connection| {
            let tx = connection.unchecked_transaction()?;
            let value = read(&tx);
            tx.commit()?;
            Ok(value)
        })
        .await?
}

pub(crate) async fn read_verified_tx<T, F>(
    reads: &ReadPool,
    caller: &AuthenticatedCaller,
    read: F,
) -> Result<T, HubError>
where
    T: Send + 'static,
    F: FnOnce(&Transaction<'_>, &AuthenticatedCaller, i64) -> Result<T, HubError> + Send + 'static,
{
    let caller = caller.clone();
    read_tx(reads, move |tx| {
        let now = now_ms();
        reverify_caller_tx(tx, &caller, now)?;
        read(tx, &caller, now)
    })
    .await
}

pub(crate) fn dialog_node_ids(
    sender_node: Option<NodeId>,
    target_node: Option<NodeId>,
    parent_sender_node: Option<NodeId>,
    parent_target_node: Option<NodeId>,
) -> Vec<NodeId> {
    let nodes: BTreeSet<NodeId> = [
        sender_node,
        target_node,
        parent_sender_node,
        parent_target_node,
    ]
    .into_iter()
    .flatten()
    .collect();
    nodes.into_iter().collect()
}

pub(crate) fn nodes_audience(nodes: &[NodeId]) -> EventAudience {
    if nodes.is_empty() {
        EventAudience::GlobalArchiveOnly
    } else {
        EventAudience::Nodes {
            node_ids: nodes.to_vec(),
        }
    }
}
