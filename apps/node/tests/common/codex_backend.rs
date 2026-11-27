use std::{error::Error, fs, io, path::Path, time::Duration};

use axum::{Json, Router, http::header, response::IntoResponse, routing::post};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};

type Test<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub struct Backend {
    task: JoinHandle<io::Result<()>>,
    requests: mpsc::Receiver<Value>,
}

impl Backend {
    pub async fn start(home: &Path) -> Test<Self> {
        let server = TcpListener::bind("127.0.0.1:0").await?;
        let address = server.local_addr()?;
        let (requests, captured) = mpsc::channel(8);
        let router = Router::new().route(
            "/v1/responses",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    let _ = requests.try_send(body);
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        canned_response(),
                    )
                        .into_response()
                }
            }),
        );
        fs::write(
            home.join("config.toml"),
            format!(
                r#"
model = "gpt-5"
model_provider = "fixture"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = "http://{address}/v1"
wire_api = "responses"
supports_websockets = false
"#
            ),
        )?;
        Ok(Self {
            task: tokio::spawn(axum::serve(server, router).into_future()),
            requests: captured,
        })
    }

    pub async fn request(&mut self) -> Test<Value> {
        Ok(timeout(Duration::from_secs(15), self.requests.recv())
            .await?
            .ok_or_else(|| io::Error::other("missing native backend request"))?)
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn canned_response() -> String {
    let text = r#"{"answer":42}"#;
    let item = json!({
        "id":"msg_fixture", "type":"message", "role":"assistant",
        "status":"completed", "phase":"final_answer",
        "content":[{"type":"output_text","text":text,"annotations":[]}],
    });
    let packets = [
        json!({"type":"response.created","response":{"id":"resp_fixture","status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item": {
            "id":"msg_fixture","type":"message","role":"assistant","status":"in_progress","content":[]
        }}),
        json!({"type":"response.output_text.delta","item_id":"msg_fixture","output_index":0,"content_index":0,"delta":text}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_fixture","status":"completed","output":[item]}}),
    ];
    packets
        .into_iter()
        .map(|packet| format!("data: {packet}\n\n"))
        .collect()
}
