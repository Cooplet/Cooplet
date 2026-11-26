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
        let (request, pin_epoch) = self.protected(Method::GET, url, None).await?;
        let page: EventsPage = self.execute_json(request, Some(pin_epoch), true).await?;
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
        let (request, pin_epoch) = self.protected(Method::GET, url, None).await?;
        let request = request.header(ACCEPT, NDJSON_MEDIA_TYPE);
        let response = timeout(
            self.config.request_timeout,
            "EventBus response headers",
            async { Ok(request.send().await?) },
        )
        .await?;
        if response.status() != StatusCode::OK {
            return Err(self.failure(response, true, pin_epoch).await);
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

const LINE_SHRINK_THRESHOLD: usize = 64 * 1024;

/// Decode one frame without mapping a terminal resync frame to an error.
pub fn parse_ndjson_line(line: &[u8]) -> Result<EventStreamFrame, ClientError> {
    let frame =
        serde_json::from_slice(line).map_err(|_| ClientError::protocol("invalid NDJSON frame"))?;
    match &frame {
        EventStreamFrame::Event { event } => validate_event(event)?,
        EventStreamFrame::ResyncRequired {
            oldest_available_seq,
            latest_seq,
            ..
        } if *oldest_available_seq == 0 || *oldest_available_seq > latest_seq.saturating_add(1) => {
            return Err(ClientError::protocol("inconsistent EventBus watermarks"));
        }
        EventStreamFrame::ResyncRequired { .. } | EventStreamFrame::Ping {} => {}
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
        self.line = Vec::new();
        Poll::Ready(Some(Err(error)))
    }

    fn yield_frame(
        &mut self,
        frame: Result<EventStreamFrame, ClientError>,
    ) -> Poll<Option<Result<EventStreamFrame, ClientError>>> {
        match frame {
            Ok(EventStreamFrame::ResyncRequired {
                oldest_available_seq,
                latest_seq,
                reason,
            }) => self.terminate(ClientError::resync(
                oldest_available_seq,
                latest_seq,
                reason,
            )),
            Ok(frame) => Poll::Ready(Some(Ok(frame))),
            Err(error) => self.terminate(error),
        }
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
            if stream.line.is_empty()
                && let Some(pos) = newline
            {
                let segment = &stream.current[..pos];
                let trimmed = segment.strip_suffix(b"\r").unwrap_or(segment);
                if trimmed.len() > stream.limit {
                    return stream.terminate(ClientError::protocol("NDJSON line exceeds limit"));
                }
                if trimmed.is_empty() {
                    stream.current.advance(pos + 1);
                    continue;
                }
                let frame = parse_ndjson_line(trimmed);
                stream.current.advance(pos + 1);
                return stream.yield_frame(frame);
            }
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
            if stream.line.capacity() > LINE_SHRINK_THRESHOLD {
                stream.line = Vec::new();
            }
            return stream.yield_frame(frame);
        }
    }
}
