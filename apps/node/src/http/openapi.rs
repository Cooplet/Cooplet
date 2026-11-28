use std::sync::OnceLock;

use cooplet_protocol::{PROBLEM_JSON_MEDIA_TYPE, ProblemDetails};
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::{
        Content, Ref, RefOr, Required, Response, ResponseBuilder, Schema,
        path::ParameterIn,
        schema::{SchemaType, Type},
        security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
    },
};

use super::{blobs, bridge, events, routes, threads};

#[derive(OpenApi)]
#[openapi(
    info(title = "Cooplet Node API", version = "1.0.0"),
    paths(
        routes::info, routes::openapi, routes::health, routes::whoami,
        routes::participants, routes::participant, routes::create_agent,
        routes::resume_agent, routes::stop_agent, routes::approval, routes::attach_plan,
        routes::messages_list, routes::message_get, routes::send_message,
        events::list, events::stream, blobs::put, blobs::get, routes::stop_node,
        bridge::enroll, blobs::relay, threads::create, threads::list, threads::get,
        threads::close, threads::post, threads::read, threads::pin_set, threads::pin_delete,
        threads::wait, threads::members, threads::join, threads::leave, threads::resume,
    ),
    components(schemas(ProblemDetails)),
    modifiers(&HttpResponses)
)]
struct NodeApi;

struct HttpResponses;

impl Modify for HttpResponses {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                "bearer",
                SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
            );

        // All mounted operations use the bounded request readers. Authentication
        // and write admission add further responses before the domain handler.
        for (path, item) in &mut openapi.paths.paths {
            for (operation, write) in [
                (&mut item.get, false),
                (&mut item.post, true),
                (&mut item.put, true),
                (&mut item.delete, true),
            ] {
                let Some(operation) = operation else { continue };
                let authenticated = operation
                    .security
                    .as_ref()
                    .is_some_and(|security| !security.is_empty());
                let responses = &mut operation.responses.responses;
                for (status, description) in [
                    ("400", "Invalid request"),
                    ("413", "Request body exceeds the route limit"),
                    ("503", "Node temporarily unavailable or outcome unconfirmed"),
                    ("default", "Other Node errors"),
                ] {
                    responses
                        .entry(status.into())
                        .or_insert_with(|| problem(description));
                }
                if authenticated {
                    responses
                        .entry("401".into())
                        .or_insert_with(|| problem("A valid bearer credential is required"));
                    if !matches!(path.as_str(), "/v1/health" | "/v1/whoami") {
                        responses
                            .entry("403".into())
                            .or_insert_with(|| problem("Caller lacks permission"));
                    }
                }
                if write {
                    responses.entry("409".into()).or_insert_with(|| {
                        problem("The request conflicts with current state or a receipt")
                    });
                    responses
                        .entry("507".into())
                        .or_insert_with(|| problem("Storage capacity exhausted"));
                }
                if path.contains("{") {
                    responses
                        .entry("404".into())
                        .or_insert_with(|| problem("Resource not found"));
                }
                if let Some(body) = operation.request_body.as_mut() {
                    normalize_optional_request_body(body);
                }
                if let Some(params) = operation.parameters.as_mut() {
                    for param in params {
                        if matches!(param.parameter_in, ParameterIn::Header)
                            && !matches!(param.required, Required::True)
                            && let Some(schema) = param.schema.as_mut()
                        {
                            strip_null_type(schema);
                        }
                    }
                }
            }
        }
    }
}

fn normalize_optional_request_body(body: &mut utoipa::openapi::request_body::RequestBody) {
    let mut unwrapped_optional = false;
    for content in body.content.values_mut() {
        let Some(RefOr::T(Schema::OneOf(one_of))) = content.schema.as_ref() else {
            continue;
        };
        let mut non_null = Vec::new();
        let mut saw_null = false;
        for item in &one_of.items {
            if is_null_schema(item) {
                saw_null = true;
            } else {
                non_null.push(item.clone());
            }
        }
        if saw_null && non_null.len() == 1 {
            content.schema = non_null.pop();
            unwrapped_optional = true;
        }
    }
    if unwrapped_optional {
        body.required = Some(Required::False);
    }
}

fn is_null_schema(schema: &RefOr<Schema>) -> bool {
    matches!(
        schema,
        RefOr::T(Schema::Object(obj)) if matches!(obj.schema_type, SchemaType::Type(Type::Null))
    )
}

fn strip_null_type(schema: &mut RefOr<Schema>) {
    if let RefOr::T(Schema::Object(obj)) = schema
        && let SchemaType::Array(types) = &mut obj.schema_type
    {
        types.retain(|ty| !matches!(ty, Type::Null));
        if types.len() == 1 {
            obj.schema_type = SchemaType::Type(types[0].clone());
        }
    }
}

fn problem(description: &str) -> RefOr<Response> {
    ResponseBuilder::new()
        .description(description)
        .content(
            PROBLEM_JSON_MEDIA_TYPE,
            Content::new(Some(Ref::from_schema_name(ProblemDetails::name()))),
        )
        .build()
        .into()
}

pub fn openapi_json() -> Result<&'static str, &'static str> {
    static REPORT: OnceLock<Result<String, String>> = OnceLock::new();
    REPORT
        .get_or_init(|| {
            serde_json::to_string_pretty(&NodeApi::openapi())
                .map(|mut json| {
                    json.push('\n');
                    json
                })
                .map_err(|error| error.to_string())
        })
        .as_deref()
        .map_err(String::as_str)
}
