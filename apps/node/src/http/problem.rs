use axum::{
    body::Body,
    response::{IntoResponse, Response},
};
use cooplet_protocol::{ErrorCode, PROBLEM_JSON_MEDIA_TYPE, RequestId};
use http::{HeaderValue, StatusCode, header::CONTENT_TYPE};

use crate::{
    error::NodeError,
    operations::{MAX_PROBLEM_BYTES, bound_problem},
};

impl IntoResponse for NodeError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let json = bound_problem(*self.0).ok().and_then(|mut problem| {
            problem.request_id = RequestId::generate().ok();
            serde_json::to_vec(&problem)
                .ok()
                .filter(|wire| wire.len() <= MAX_PROBLEM_BYTES)
        });
        match json {
            Some(json) => problem_response(status, Body::from(json)),
            None => problem_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from(concat!(
                    r#"{"type":"https://cooplet.dev/problems/internal-error","title":"Internal error","status":500,"#,
                    r#""detail":"The Node could not complete the request","code":"internal_error","retryable":true}"#,
                )),
            ),
        }
    }
}

fn problem_response(status: StatusCode, body: Body) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(PROBLEM_JSON_MEDIA_TYPE),
    );
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            http::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer"),
        );
    }
    response
}

pub fn invalid_request(detail: impl Into<String>) -> NodeError {
    NodeError::new(ErrorCode::InvalidRequest, detail)
}

pub fn forbidden() -> NodeError {
    NodeError::new(
        ErrorCode::Forbidden,
        "This credential does not have the required Node scope",
    )
}

pub fn payload_too_large() -> NodeError {
    NodeError::new(
        ErrorCode::PayloadTooLarge,
        "The request exceeds its body limit",
    )
}
