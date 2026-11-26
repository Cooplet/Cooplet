use std::collections::HashSet;

use crate::ValidationError;

crate::wire::wire_string_enum! {
    pub enum QuestionMode {
        Single => "single",
        Multi => "multi",
        Text => "text",
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct QuestionOption {
        pub id: String,
        pub label: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
    }
}

crate::wire::wire_object! {
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
}

fn valid_option_id(id: &str) -> bool {
    (1..=64).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_graphic())
}

impl QuestionPayload {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.prompt.len() > 2000 || self.prompt.trim().is_empty() {
            return Err(ValidationError("question prompt"));
        }
        if self.expires_at_ms.is_some_and(|ms| ms <= 0) {
            return Err(ValidationError("question expiry"));
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
            if !valid_option_id(&option.id) || !ids.insert(&option.id) {
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

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct AnswerPayload {
        #[serde(default)]
        pub selected_option_ids: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub text: Option<String>,
    }
}

impl AnswerPayload {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self
            .text
            .as_deref()
            .is_some_and(|text| text.trim().is_empty() || text.len() > 8000)
        {
            return Err(ValidationError("answer text"));
        }
        if self.selected_option_ids.len() > 16 {
            return Err(ValidationError("answer option count"));
        }
        let mut ids = HashSet::with_capacity(self.selected_option_ids.len());
        for id in &self.selected_option_ids {
            if !valid_option_id(id) || !ids.insert(id) {
                return Err(ValidationError("answer option ID"));
            }
        }
        if self.selected_option_ids.is_empty() && self.text.is_none() {
            return Err(ValidationError("empty answer"));
        }
        Ok(())
    }

    pub fn validate_against(&self, question: &QuestionPayload) -> Result<(), ValidationError> {
        question.validate()?;
        self.validate()?;
        let text = self.text.as_deref();
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
        for id in &self.selected_option_ids {
            if !question.options.iter().any(|option| option.id == *id) {
                return Err(ValidationError("answer option ID"));
            }
        }
        Ok(())
    }
}
