use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ValidationError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuestionMode {
    Single,
    Multi,
    Text,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuestionOption {
    pub id: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuestionPayload {
    pub prompt: String,
    pub mode: QuestionMode,
    #[serde(default)]
    pub options: Vec<QuestionOption>,
    #[serde(default)]
    pub allow_free_text: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
}

impl QuestionPayload {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.prompt.len() > 2000 || self.prompt.trim().is_empty() {
            return Err(ValidationError("question prompt"));
        }
        if self.mode == QuestionMode::Text {
            return if self.options.is_empty() {
                Ok(())
            } else {
                Err(ValidationError("options on text question"))
            };
        }
        if !(2..=16).contains(&self.options.len()) {
            return Err(ValidationError("question option count"));
        }
        let mut ids = HashSet::with_capacity(self.options.len());
        for option in &self.options {
            if !(1..=64).contains(&option.id.len())
                || !option.id.bytes().all(|byte| byte.is_ascii_graphic())
                || !ids.insert(&option.id)
            {
                return Err(ValidationError("question option ID"));
            }
            if option.label.trim().is_empty() || option.label.chars().count() > 64 {
                return Err(ValidationError("question option label"));
            }
            if option
                .description
                .as_ref()
                .is_some_and(|text| text.chars().count() > 256)
            {
                return Err(ValidationError("question option description"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AnswerPayload {
    #[serde(default)]
    pub selected_option_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

impl AnswerPayload {
    pub fn validate_against(&self, question: &QuestionPayload) -> Result<(), ValidationError> {
        question.validate()?;
        let text = self.text.as_deref();
        if text.is_some_and(|text| text.trim().is_empty() || text.len() > 8000) {
            return Err(ValidationError("answer text"));
        }
        let count = self.selected_option_ids.len();
        let valid_shape = match question.mode {
            QuestionMode::Single => {
                (count == 1 && text.is_none())
                    || (count == 0 && question.allow_free_text && text.is_some())
            }
            QuestionMode::Multi => count > 0 && (text.is_none() || question.allow_free_text),
            QuestionMode::Text => count == 0 && text.is_some(),
        };
        if !valid_shape || count > question.options.len() {
            return Err(ValidationError("answer shape"));
        }
        let mut ids = HashSet::with_capacity(count);
        for id in &self.selected_option_ids {
            if !ids.insert(id) || !question.options.iter().any(|option| option.id == *id) {
                return Err(ValidationError("answer option ID"));
            }
        }
        Ok(())
    }
}
