//! Typed HTTP and NDJSON access to Cooplet Node and Hub APIs.

mod auth;
mod blobs;
mod client;
mod error;
mod events;

pub use auth::{BearerToken, IDEMPOTENCY_KEY, X_COOPLET_FILENAME, X_COOPLET_HUB, X_COOPLET_SHA256};
pub use blobs::{
    BlobDownload, BlobUploadOutcome, BlobUploadSpec, DownloadedBlob, UploadVerification,
};
pub use client::{ClientConfig, CoopletClient, Destination, api_path};
pub use error::ClientError;
pub use events::{EventStream, EventsQuery, NDJSON_MEDIA_TYPE, parse_ndjson_line};
