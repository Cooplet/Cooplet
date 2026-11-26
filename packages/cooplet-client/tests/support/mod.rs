#![allow(dead_code)]

use std::{collections::BTreeMap, io, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};
use url::Url;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const HUB_ID: &str = "hub_00000000000000000000000001";
pub const OTHER_HUB_ID: &str = "hub_00000000000000000000000002";
pub const BLOB_ID: &str = "blob_00000000000000000000000001";
pub const HELLO_SHA: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
pub const EMPTY_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const HUB_INFO: &[u8] =
    br#"{"hub_id":"hub_00000000000000000000000001","display_name":"Hub","protocol_version":1}"#;
pub const NODE_INFO: &[u8] =
    br#"{"node_id":"node_00000000000000000000000001","display_name":"Node","protocol_version":1}"#;

pub struct Server {
    pub url: Url,
    pub listener: TcpListener,
}

impl Server {
    pub async fn bind() -> TestResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        Ok(Self {
            url: Url::parse(&format!("http://{}/", listener.local_addr()?))?,
            listener,
        })
    }

    pub async fn accept(&self) -> TestResult<(Head, Peer)> {
        let (socket, _) =
            tokio::time::timeout(Duration::from_secs(2), self.listener.accept()).await??;
        let mut peer = Peer(BufStream::new(socket));
        let request = peer.line().await?;
        let mut headers = BTreeMap::<String, Vec<String>>::new();
        loop {
            let line = peer.line().await?;
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(':').ok_or("invalid request header")?;
            headers
                .entry(name.to_ascii_lowercase())
                .or_default()
                .push(value.trim().into());
        }
        Ok((Head { request, headers }, peer))
    }
}

pub struct Head {
    pub request: String,
    headers: BTreeMap<String, Vec<String>>,
}

impl Head {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.first().map(String::as_str)
    }

    pub fn count(&self, name: &str) -> usize {
        self.headers.get(name).map_or(0, Vec::len)
    }
}

pub struct Peer(pub BufStream<TcpStream>);

impl Peer {
    async fn line(&mut self) -> TestResult<String> {
        let mut line = String::new();
        let read =
            tokio::time::timeout(Duration::from_secs(2), self.0.read_line(&mut line)).await??;
        if read == 0 || !line.ends_with("\r\n") {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "HTTP fixture line ended early",
            )
            .into());
        }
        line.truncate(line.len() - 2);
        Ok(line)
    }

    pub async fn read_body(&mut self, head: &Head) -> TestResult<Vec<u8>> {
        if let Some(length) = head.header("content-length") {
            let mut body = vec![0; length.parse()?];
            tokio::time::timeout(Duration::from_secs(2), self.0.read_exact(&mut body)).await??;
            return Ok(body);
        }
        let mut body = Vec::new();
        if head.header("transfer-encoding") != Some("chunked") {
            return Ok(body);
        }
        loop {
            let length = usize::from_str_radix(&self.line().await?, 16)?;
            if length == 0 {
                assert!(self.line().await?.is_empty());
                return Ok(body);
            }
            let offset = body.len();
            body.resize(offset + length, 0);
            tokio::time::timeout(
                Duration::from_secs(2),
                self.0.read_exact(&mut body[offset..]),
            )
            .await??;
            assert!(self.line().await?.is_empty());
        }
    }

    pub async fn raw(&mut self, bytes: &[u8]) -> TestResult {
        self.0.write_all(bytes).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn respond(
        mut self,
        status: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> TestResult {
        self.raw(
            format!(
                "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
        for (name, value) in headers {
            self.raw(format!("{name}: {value}\r\n").as_bytes()).await?;
        }
        self.raw(b"\r\n").await?;
        self.raw(body).await?;
        self.0.shutdown().await?;
        Ok(())
    }

    pub async fn start_chunks(&mut self, media: &str, headers: &[(&str, &str)]) -> TestResult {
        self.raw(format!("HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nContent-Type: {media}\r\n").as_bytes()).await?;
        for (name, value) in headers {
            self.raw(format!("{name}: {value}\r\n").as_bytes()).await?;
        }
        self.raw(b"\r\n").await?;
        Ok(())
    }

    pub async fn chunk(&mut self, bytes: &[u8]) -> TestResult {
        self.raw(format!("{:x}\r\n", bytes.len()).as_bytes())
            .await?;
        self.raw(bytes).await?;
        self.raw(b"\r\n").await?;
        Ok(())
    }
}

pub fn receipt(size: u64, digest: &str) -> String {
    format!(
        r#"{{"blob_id":"{BLOB_ID}","name":"name.bin","mime_type":"application/octet-stream","size_bytes":{size},"sha256":"{digest}"}}"#
    )
}
