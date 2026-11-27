use nanocode_backend_antigravity_client::Json;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Thinking {
    Level(&'static str),
    Budget(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    pub model: String,
    pub display: String,
    pub model_enum: &'static str,
    pub gemini: bool,
    pub max_output_tokens: u32,
    pub thinking: Thinking,
    pub compaction_limit: u64,
    pub compaction_output_tokens: u32,
    pub max_user_requests: usize,
    pub claude_read_file: bool,
    pub skip_signature: bool,
}

pub const SKIP_SIGNATURE: &str = "c2tpcF90aG91Z2h0X3NpZ25hdHVyZV92YWxpZGF0b3I=";

impl Profile {
    pub fn for_model(model: &str) -> Self {
        let lower = model.to_ascii_lowercase();
        let base = Self {
            model: model.to_owned(),
            display: model.to_owned(),
            model_enum: "MODEL_PLACEHOLDER_M467",
            gemini: true,
            max_output_tokens: 65_536,
            thinking: Thinking::Level("HIGH"),
            compaction_limit: 512_000,
            compaction_output_tokens: 32_768,
            max_user_requests: 10,
            claude_read_file: false,
            skip_signature: true,
        };
        if lower.starts_with("claude") {
            return Self {
                model_enum: "MODEL_PLACEHOLDER_M359",
                gemini: false,
                max_output_tokens: 8_192,
                thinking: Thinking::Budget(4_096),
                compaction_limit: 256_000,
                claude_read_file: true,
                skip_signature: false,
                ..base
            };
        }
        if lower.starts_with("gemini-4-neon") {
            return Self {
                display: "Gemini 4 Neon (High)".to_owned(),
                ..base
            };
        }
        if lower.starts_with("gemini-3") || lower.starts_with("gemini-2") {
            return Self {
                display: display_name(model),
                compaction_limit: 256_000,
                ..base
            };
        }
        Self {
            display: display_name(model),
            ..base
        }
    }

    pub fn with_level(mut self, level: Option<&'static str>) -> Self {
        if let (Some(level), Thinking::Level(_)) = (level, &self.thinking) {
            self.thinking = Thinking::Level(level);
            if self.model.to_ascii_lowercase().starts_with("gemini-4-neon") {
                let label = match level {
                    "LOW" | "MINIMAL" => "Low",
                    "MEDIUM" => "Medium",
                    "XHIGH" => "Extra High",
                    _ => "High",
                };
                self.display = format!("Gemini 4 Neon ({label})");
            }
        }
        self
    }

    pub fn generation(&self, max_output_tokens: u32) -> Json {
        let thinking = match &self.thinking {
            Thinking::Level(level) => Json::object([
                ("includeThoughts", Json::Bool(true)),
                ("thinkingLevel", Json::str(*level)),
            ]),
            Thinking::Budget(budget) => Json::object([
                ("includeThoughts", Json::Bool(true)),
                ("thinkingBudget", Json::int(i64::from(*budget))),
            ]),
        };
        Json::object([
            ("maxOutputTokens", Json::int(i64::from(max_output_tokens))),
            ("thinkingConfig", thinking),
        ])
    }
}

fn display_name(model: &str) -> String {
    model
        .split('-')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().chain(chars).collect::<String>(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
