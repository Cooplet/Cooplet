use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use cooplet_protocol::{HubId, PROBLEM_JSON_MEDIA_TYPE, ProblemDetails, RunId};
use serde_json::Value;
use utoipa::{
    Modify, OpenApi, PartialSchema, ToSchema,
    openapi::{
        Components, Content, ObjectBuilder, Ref, RefOr, Required, Type,
        header::Header,
        path::{Operation, ParameterBuilder, ParameterIn},
        response::Response,
        schema::{AllOfBuilder, AnyOfBuilder},
        security::{Http, HttpAuthScheme, SecurityScheme},
    },
};

use crate::{
    error::HubError,
    http::{
        BEARER_CHALLENGE, EmptyJson,
        handlers::{self, admin_routes, command_routes, thread_routes},
    },
};

static HUB_OPENAPI: OnceLock<Bytes> = OnceLock::new();
static OPENAPI_GENERATION: Mutex<()> = Mutex::new(());

#[derive(OpenApi)]
#[openapi(
    info(title = "Cooplet Hub API", version = "1"),
    paths(
        handlers::info, handlers::openapi, handlers::health, handlers::whoami,
        admin_routes::list_principals, admin_routes::create_principal,
        admin_routes::list_tokens, admin_routes::issue_token, admin_routes::revoke_token,
        admin_routes::disable_principal, admin_routes::list_enrollment, admin_routes::issue_enrollment,
        admin_routes::revoke_enrollment, admin_routes::redeem_enrollment, admin_routes::revoke_node,
        admin_routes::list_nodes, admin_routes::list_participants,
        command_routes::send_message, command_routes::get_message, command_routes::create_agent,
        command_routes::stop_agent, command_routes::respond_approval,
        command_routes::run_workflow, command_routes::cancel_workflow,
        handlers::upload_blob, handlers::download_blob, handlers::events_page, handlers::events_stream,
        thread_routes::create, thread_routes::list, thread_routes::read_thread, thread_routes::close,
        thread_routes::post_message, thread_routes::read_posts, thread_routes::set_pin, thread_routes::delete_pin,
        thread_routes::wait, thread_routes::list_members, thread_routes::join, thread_routes::leave, thread_routes::resume,
        crate::bridge::connect,
    ),
    components(schemas(ProblemDetails, HubId, RunId)),
    security(("hub_bearer" = [])),
    modifiers(&HttpConventions)
)]
struct HubApiDoc;

pub(crate) fn openapi_bytes() -> Result<&'static Bytes, HubError> {
    if let Some(bytes) = HUB_OPENAPI.get() {
        return Ok(bytes);
    }
    // Only the first successful request renders the static document. Cache
    // hits bypass this lock and a rendering error leaves the cache empty.
    let _generation = OPENAPI_GENERATION.lock().map_err(HubError::internal)?;
    if let Some(bytes) = HUB_OPENAPI.get() {
        return Ok(bytes);
    }
    let rendered = render_openapi()?;
    Ok(HUB_OPENAPI.get_or_init(|| rendered))
}

fn render_openapi() -> serde_json::Result<Bytes> {
    let mut document = serde_json::to_value(HubApiDoc::openapi())?;
    sort_objects(&mut document);
    let mut bytes = serde_json::to_vec_pretty(&document)?;
    bytes.push(b'\n');
    Ok(Bytes::from(bytes))
}

fn sort_objects(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.sort_keys();
            object.values_mut().for_each(sort_objects);
        }
        Value::Array(array) => array.iter_mut().for_each(sort_objects),
        _ => {}
    }
}

struct HttpConventions;

impl Modify for HttpConventions {
    fn modify(&self, document: &mut utoipa::openapi::OpenApi) {
        let components = document.components.get_or_insert_with(Components::new);
        components.add_security_scheme(
            "hub_bearer",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
        // Either selector may be supplied alone, and a request may specify both.
        if let Some(schema) = components.schemas.get_mut("CreateAgentRequest") {
            let mut selector = AnyOfBuilder::new();
            for name in ["client_kind", "template_id"] {
                selector = selector.item(
                    ObjectBuilder::new()
                        .property(name, ObjectBuilder::new().schema_type(Type::String))
                        .required(name),
                );
            }
            *schema = AllOfBuilder::new()
                .item(schema.clone())
                .item(selector)
                .into();
        }

        for (path, item) in &mut document.paths.paths {
            let public = matches!(path.as_str(), "/v1/info" | "/v1/openapi.json");
            let requires_bearer = !public && path != "/v1/enrollment/token";
            item.parameters.get_or_insert_with(Vec::new).push(
                ParameterBuilder::new()
                    .name("X-Cooplet-Hub")
                    .parameter_in(ParameterIn::Header)
                    .required(if public { Required::False } else { Required::True })
                    .description(Some("Expected Hub ID. A mismatched or duplicated pin is rejected before credential verification."))
                    .schema(Some(HubId::schema()))
                    .build(),
            );

            for operation in [
                item.post.as_mut(),
                item.put.as_mut(),
                item.patch.as_mut(),
                item.delete.as_mut(),
            ]
            .into_iter()
            .flatten()
            {
                describe_mutating(operation, path);
            }

            for operation in [
                item.get.as_mut(),
                item.post.as_mut(),
                item.put.as_mut(),
                item.patch.as_mut(),
                item.delete.as_mut(),
            ]
            .into_iter()
            .flatten()
            {
                describe_responses(operation, public, requires_bearer);
            }
        }
    }
}

fn describe_mutating(operation: &mut Operation, path: &str) {
    if path == "/v1/blobs" {
        let parameters = operation.parameters.get_or_insert_with(Vec::new);
        parameters.push(
            ParameterBuilder::new()
                .name("X-Cooplet-Sha256")
                .parameter_in(ParameterIn::Header)
                .required(Required::False)
                .description(Some("Lowercase SHA-256 hex digest."))
                .schema(Some(
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .pattern(Some("^[0-9a-f]{64}$")),
                ))
                .build(),
        );
        parameters.push(
            ParameterBuilder::new()
                .name("Content-Length")
                .parameter_in(ParameterIn::Header)
                .required(Required::False)
                .description(Some("Length of the file body in bytes."))
                .schema(Some(
                    ObjectBuilder::new()
                        .schema_type(Type::Integer)
                        .minimum(Some(0)),
                ))
                .build(),
        );
    }
    if !matches!(path, "/v1/enrollment/token" | "/v1/threads/{id}/wait") {
        operation.parameters.get_or_insert_with(Vec::new).push(
            ParameterBuilder::new()
                .name("Idempotency-Key")
                .parameter_in(ParameterIn::Header)
                .required(if path == "/v1/blobs" { Required::False } else { Required::True })
                .description(Some("8 to 128 visible ASCII characters. Reuse the same key and body after an ambiguous request outcome."))
                .schema(Some(ObjectBuilder::new().schema_type(Type::String).pattern(Some("^[!-~]{8,128}$"))))
                .build(),
        );
    }

    if let Some(body) = operation.request_body.as_mut() {
        let empty_object = RefOr::Ref(Ref::from_schema_name(EmptyJson::name()));
        match path {
            "/v1/blobs" => body.required = Some(Required::True),
            "/v1/enrollment/token" => body.description = Some("JSON body. Limit 16 KiB.".into()),
            _ if body
                .content
                .values()
                .any(|media| media.schema.as_ref() == Some(&empty_object)) =>
            {
                body.required = Some(Required::False);
                body.description =
                    Some("Optional empty JSON object. Omit the body or send {}.".into());
            }
            _ => {
                body.description
                    .get_or_insert_with(|| "JSON body. Limit 64 KiB.".into());
            }
        }
    }
}

fn describe_responses(operation: &mut Operation, public: bool, requires_bearer: bool) {
    let unauthorized = if public {
        "Duplicate Authorization header."
    } else {
        "Credential missing, invalid, expired, or revoked."
    };
    for (status, description) in [
        ("400", "Invalid request."),
        ("401", unauthorized),
        ("409", "Hub identity mismatch or route-specific conflict."),
        ("413", "Request body exceeds the route limit."),
        ("500", "Unexpected Hub failure."),
        (
            "503",
            "Hub temporarily unavailable or unable to confirm the delivery outcome.",
        ),
    ] {
        operation
            .responses
            .responses
            .entry(status.into())
            .or_insert_with(|| Response::new(description).into());
    }

    for (status, response) in &mut operation.responses.responses {
        let RefOr::T(response) = response else {
            continue;
        };
        if status.starts_with(['4', '5']) {
            response
                .content
                .entry(PROBLEM_JSON_MEDIA_TYPE.into())
                .or_insert_with(|| {
                    Content::new(Some(Ref::from_schema_name(ProblemDetails::name())))
                });
        }
        response
            .headers
            .insert("X-Cooplet-Hub".into(), Header::new(HubId::schema()));
        response.headers.insert(
            "Cache-Control".into(),
            Header::new(
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .enum_values(Some(["no-store"])),
            ),
        );
        if status == "401" && requires_bearer {
            response.headers.insert(
                "WWW-Authenticate".into(),
                Header::new(
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .enum_values(Some([BEARER_CHALLENGE])),
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use super::*;

    #[test]
    fn gate_openapi_drift() -> Result<(), Box<dyn Error>> {
        let contract =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contracts/openapi-hub.json");
        let expected = std::fs::read(contract)?;
        let generated = openapi_bytes()?;
        assert!(
            generated.as_ref() == expected.as_slice(),
            "Regenerate contracts/openapi-hub.json with cooplet-hub openapi"
        );
        Ok(())
    }

    #[test]
    fn openapi_has_resolvable_schema_references() -> Result<(), Box<dyn Error>> {
        fn check(value: &Value, root: &Value) {
            match value {
                Value::Object(object) => {
                    if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                        && let Some(pointer) = reference.strip_prefix('#')
                    {
                        assert!(
                            root.pointer(pointer).is_some(),
                            "OpenAPI has a missing reference: {reference}"
                        );
                    }
                    for child in object.values() {
                        check(child, root);
                    }
                }
                Value::Array(array) => array.iter().for_each(|value| check(value, root)),
                _ => {}
            }
        }

        let value: Value = serde_json::from_slice(openapi_bytes()?)?;
        assert_eq!(value["openapi"], "3.1.0");
        check(&value, &value);
        Ok(())
    }

    #[test]
    fn http_conventions_cover_all_mutating_methods_and_non_nullable_blob_headers()
    -> Result<(), Box<dyn Error>> {
        use utoipa::openapi::path::{OperationBuilder, PathItem};

        let mut doc = utoipa::openapi::OpenApiBuilder::new().build();
        let mut synthetic = PathItem::default();
        synthetic.put = Some(OperationBuilder::new().build());
        synthetic.patch = Some(OperationBuilder::new().build());
        synthetic.delete = Some(OperationBuilder::new().build());
        doc.paths.paths.insert("/v1/items/{id}".into(), synthetic);
        HttpConventions.modify(&mut doc);
        let item = doc
            .paths
            .paths
            .get("/v1/items/{id}")
            .ok_or("missing synthetic path")?;
        for operation in [item.put.as_ref(), item.patch.as_ref(), item.delete.as_ref()] {
            let operation = operation.ok_or("missing synthetic operation")?;
            let has_idempotency = operation
                .parameters
                .as_ref()
                .is_some_and(|params| params.iter().any(|p| p.name == "Idempotency-Key"));
            assert!(has_idempotency);
            assert!(operation.responses.responses.contains_key("401"));
            assert!(operation.responses.responses.contains_key("503"));
        }

        let value: Value = serde_json::from_slice(openapi_bytes()?)?;
        let blob_params = value["paths"]["/v1/blobs"]["post"]["parameters"]
            .as_array()
            .ok_or("missing /v1/blobs parameters")?;
        for (name, expected_type) in [
            ("X-Cooplet-Sha256", "string"),
            ("Content-Length", "integer"),
        ] {
            let param = blob_params
                .iter()
                .find(|param| param["name"] == name)
                .ok_or("missing optional blob header")?;
            assert_eq!(param["required"], false);
            assert_eq!(param["schema"]["type"], expected_type);
        }
        let stream_params = value["paths"]["/v1/events/stream"]["get"]["parameters"]
            .as_array()
            .ok_or("missing /v1/events/stream parameters")?;
        assert!(!stream_params.iter().any(|param| param["name"] == "limit"));
        let page_limit = value["paths"]["/v1/events"]["get"]["parameters"]
            .as_array()
            .ok_or("missing /v1/events parameters")?
            .iter()
            .find(|param| param["name"] == "limit")
            .ok_or("missing limit parameter")?;
        assert_eq!(page_limit["schema"]["default"], 100);
        assert_eq!(
            value["components"]["schemas"]["MessageRequest"]["properties"]["attachments"]["maxItems"],
            64
        );
        Ok(())
    }
}
