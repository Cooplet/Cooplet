use std::{env, time::Duration};

use serde_json::{Value, json};
use tokio::time::timeout;

use super::transport::Session;

const CHATGPT_BOOTSTRAP: &str = "https://chatgpt.com/backend-api";
const CHATGPT_CODEX: &str = "https://chatgpt.com/backend-api/codex";

pub(super) fn environment_allows_named_output() -> bool {
    // Treat an explicitly set override as ambiguous, including an empty value.
    env::var_os("OPENAI_BASE_URL").is_none()
}

pub(super) async fn named_output_revision(
    session: &Session,
    opened: &Value,
    cwd: &str,
    environment_allows: bool,
) -> Option<u64> {
    if !environment_allows
        || opened["modelProvider"] != "openai"
        || opened["thread"]["modelProvider"] != "openai"
    {
        return None;
    }
    timeout(Duration::from_secs(4), async {
        // Startup routing discovery can emit account/updated while the first
        // account/read is pending. Confirm once more on the new observation.
        for _ in 0..2 {
            let revision = session.snapshot().route_revision?;
            let (account, config, requirements, providers) = tokio::try_join!(
                session.rpc("account/read", json!({"refreshToken": false})),
                session.rpc("config/read", json!({"cwd": cwd, "includeLayers": false})),
                session.rpc("configRequirements/read", json!({})),
                session.rpc("modelProvider/list", json!({})),
            )
            .ok()?;
            if Some(revision) == session.snapshot().route_revision {
                return compatible(&account, &config, &requirements, &providers)
                    .then_some(revision);
            }
        }
        None
    })
    .await
    .ok()
    .flatten()
}

fn compatible(account: &Value, config: &Value, requirements: &Value, providers: &Value) -> bool {
    if account["requiresOpenaiAuth"] != true
        || account["account"]["type"] != "chatgpt"
        || account["workspaceRouting"]["backendOrigin"] != "https://chatgpt.com"
        || account["workspaceRouting"]["accountRoutingOverride"] != "NO_CONSTRAINT"
    {
        return false;
    }
    let config = &config["config"];
    let required = &requirements["requirements"];
    if !config.is_object()
        || requirements.get("requirements").is_none()
        || !(required.is_null() || required.is_object())
    {
        return false;
    }
    if !absent_or(&config["model_provider"], "openai")
        || !absent_or(&required["modelProvider"], "openai")
        || !absent_or(&config["openai_base_url"], CHATGPT_CODEX)
        || !absent_or(
            &config["model_providers"]["openai"]["base_url"],
            CHATGPT_CODEX,
        )
        || !absent_or(
            &required["modelProviders"]["openai"]["baseUrl"],
            CHATGPT_CODEX,
        )
        || !bootstrap_url(&config["chatgpt_base_url"])
        || !bootstrap_url(&required["chatgptBaseUrl"])
    {
        return false;
    }
    let Some(providers) = providers["data"].as_array() else {
        return false;
    };
    let mut openai = providers.iter().filter(|entry| entry["id"] == "openai");
    openai.next().is_some_and(|entry| {
        entry.get("baseUrl").is_some() && absent_or(&entry["baseUrl"], CHATGPT_CODEX)
    }) && openai.next().is_none()
}

fn absent_or(value: &Value, expected: &str) -> bool {
    value.is_null()
        || value.as_str().is_some_and(|value| {
            value == expected
                || value.strip_suffix('/') == Some(expected) && expected.starts_with("https://")
        })
}

fn bootstrap_url(value: &Value) -> bool {
    absent_or(value, CHATGPT_BOOTSTRAP) || absent_or(value, CHATGPT_CODEX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_output_requires_confirmed_account_and_effective_chatgpt_route() {
        let account = json!({"requiresOpenaiAuth":true,"account":{"type":"chatgpt"},
            "workspaceRouting":{"backendOrigin":"https://chatgpt.com","accountRoutingOverride":"NO_CONSTRAINT"}});
        let config = json!({"config":{"model_provider":"openai","chatgpt_base_url":"https://chatgpt.com/backend-api/"}});
        let requirements = json!({"requirements":null});
        let providers = json!({"data":[{"id":"openai","baseUrl":null}]});
        assert!(compatible(&account, &config, &requirements, &providers));

        for account in [
            json!({"requiresOpenaiAuth":true,"account":{"type":"apiKey"}}),
            json!({"requiresOpenaiAuth":true,"account":{"type":"chatgpt"}}),
            json!({"requiresOpenaiAuth":true,"account":{"type":"chatgpt"},
                "workspaceRouting":{"backendOrigin":"https://gov.chatgpt.com","accountRoutingOverride":"us_cr"}}),
        ] {
            assert!(!compatible(&account, &config, &requirements, &providers));
        }
        for config in [
            json!({"config":{"model_provider":"custom"}}),
            json!({"config":{"model_provider":"openai","openai_base_url":"https://custom.example/v1"}}),
            json!({"config":{"model_provider":"openai","chatgpt_base_url":"https://chatgpt.com/backend-api?proxy=1"}}),
        ] {
            assert!(!compatible(&account, &config, &requirements, &providers));
        }
        assert!(!compatible(&account, &config, &json!({}), &providers));
        assert!(!compatible(
            &account,
            &config,
            &json!({"requirements":{"chatgptBaseUrl":"https://custom.example"}}),
            &providers
        ));
        assert!(!compatible(
            &account,
            &config,
            &requirements,
            &json!({"data":[{"id":"openai"}]})
        ));
    }
}
