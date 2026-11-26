use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use cooplet_protocol::{EventEnvelope, EventKind, EventStreamFrame, EventsPage};
use futures_core::Stream;
use futures_util::StreamExt;
use http::{Method, StatusCode, header::ACCEPT};

use crate::{
    ClientError, CoopletClient,
    client::{ResponseChunks, timeout},
    error::media_type,
};

pub const NDJSON_MEDIA_TYPE: &str = "application/x-ndjson";

#[derive(Clone, Debug, Default)]
pub struct EventsQuery {
    pub after: u64,
    pub limit: Option<u16>,
    pub kinds: Vec<EventKind>,
}

impl CoopletClient {
    pub async fn list_events(&self, query: &EventsQuery) -> Result<EventsPage, ClientError> {
        let mut params = event_query(query.after, &query.kinds);
        if let Some(limit) = query.limit {
            if !(1..=500).contains(&limit) {
                return Err(ClientError::invalid(
                    "event page limit must be 1 through 500",
                ));
            }
            params.push(("limit", limit.to_string()));
        }
        let url = self.url("/v1/events", &params)?;
        let request = self.protected(Method::GET, url, None).await?;
        let page: EventsPage = self.execute_json(request, true, true).await?;
        for event in &page.items {
            validate_event(event)?;
        }
        Ok(page)
    }

    pub async fn stream_events(
        &self,
        after: u64,
        kinds: &[EventKind],
    ) -> Result<EventStream, ClientError> {
        let url = self.url("/v1/events/stream", &event_query(after, kinds))?;
        let request = self
            .protected(Method::GET, url, None)
            .await?
            .header(ACCEPT, NDJSON_MEDIA_TYPE);
        let response = timeout(
            self.config.request_timeout,
            "EventBus response headers",
            async { Ok(request.send().await?) },
        )
        .await?;
        if response.status() != StatusCode::OK {
            return Err(self.failure(response, true).await);
        }
        if !media_type(&response).is_some_and(|media| media.eq_ignore_ascii_case(NDJSON_MEDIA_TYPE))
        {
            return Err(ClientError::protocol("expected an NDJSON response"));
        }
        Ok(EventStream {
            chunks: Some(Box::pin(response.bytes_stream())),
            current: Bytes::new(),
            line: Vec::new(),
            limit: self.config.max_ndjson_line_bytes,
        })
    }
}

fn event_query(after: u64, kinds: &[EventKind]) -> Vec<(&'static str, String)> {
    let mut query = vec![("after", after.to_string())];
    if !kinds.is_empty() {
        query.push((
            "kinds",
            kinds
                .iter()
                .map(|kind| kind.as_str())
                .collect::<Vec<_>>()
                .join(","),
        ));
    }
    query
}

/// Decode one frame without mapping a terminal resync frame to an error.
pub fn parse_ndjson_line(line: &[u8]) -> Result<EventStreamFrame, ClientError> {
    let frame =
        serde_json::from_slice(line).map_err(|_| ClientError::protocol("invalid NDJSON frame"))?;
    if let EventStreamFrame::Event { event } = &frame {
        validate_event(event)?;
    }
    Ok(frame)
}

fn validate_event(event: &EventEnvelope) -> Result<(), ClientError> {
    event
        .validate()
        .map_err(|_| ClientError::protocol("invalid EventBus event"))
}

pub struct EventStream {
    chunks: Option<ResponseChunks>,
    current: Bytes,
    line: Vec<u8>,
    limit: usize,
}

impl EventStream {
    pub async fn next_frame(&mut self) -> Option<Result<EventStreamFrame, ClientError>> {
        self.next().await
    }

    fn terminate(
        &mut self,
        error: ClientError,
    ) -> Poll<Option<Result<EventStreamFrame, ClientError>>> {
        self.chunks = None;
        self.current = Bytes::new();
        self.line.clear();
        Poll::Ready(Some(Err(error)))
    }
}

impl Stream for EventStream {
    type Item = Result<EventStreamFrame, ClientError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        loop {
            let Some(chunks) = &mut stream.chunks else {
                return Poll::Ready(None);
            };
            if stream.current.is_empty() {
                match chunks.as_mut().poll_next(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(bytes))) => stream.current = bytes,
                    Poll::Ready(Some(Err(error))) => return stream.terminate(error.into()),
                    Poll::Ready(None) => {
                        stream.chunks = None;
                        if stream.line.is_empty() {
                            return Poll::Ready(None);
                        }
                        return stream.terminate(ClientError::protocol("truncated NDJSON frame"));
                    }
                }
            }
            let newline = stream.current.iter().position(|&byte| byte == b'\n');
            let line_bytes = newline.unwrap_or(stream.current.len());
            let segment = &stream.current[..line_bytes];
            let cr = segment.last().or_else(|| stream.line.last()) == Some(&b'\r');
            let allowance = stream.limit + usize::from(cr);
            if line_bytes > allowance.saturating_sub(stream.line.len()) {
                return stream.terminate(ClientError::protocol("NDJSON line exceeds limit"));
            }
            stream.line.extend_from_slice(segment);
            stream
                .current
                .advance(line_bytes + usize::from(newline.is_some()));
            if newline.is_none() {
                continue;
            }
            if cr {
                stream.line.pop();
            }
            if stream.line.is_empty() {
                continue;
            }
            let frame = parse_ndjson_line(&stream.line);
            stream.line.clear();
            return match frame {
                Ok(EventStreamFrame::ResyncRequired {
                    oldest_available_seq,
                    latest_seq,
                    reason,
                }) => stream.terminate(ClientError::resync(
                    oldest_available_seq,
                    latest_seq,
                    reason,
                )),
                Ok(frame) => Poll::Ready(Some(Ok(frame))),
                Err(error) => stream.terminate(error),
            };
        }
    }
}
