use std::{borrow::Cow, sync::Arc};

use crate::client::{
    CONTEXT_WINDOW_TOKENS, Model, OpenAiAuth, ReasoningMode, ResponsesHistory, ResponsesTransport,
    Thinking, pricing::ServiceTier, responses::StrictJsonSchema,
};

const SOL_SYSTEM_PROMPT: &str = include_str!("../../prompts/sol.md");
const SOL_62_SYSTEM_PROMPT: &str = include_str!("../../prompts/sol-6.2.md");
const LUNA_SYSTEM_PROMPT: &str = include_str!("../../prompts/luna.md");
const GLM_SYSTEM_PROMPT: &str = include_str!("../../prompts/glm.md");
const ASTRA_SYSTEM_PROMPT: &str = include_str!("../../prompts/astra.md");
const UNKNOWN_MODEL_SYSTEM_PROMPT: &str = include_str!("../../prompts/codex-unknown-model.md");

pub(crate) fn prefixed_model_id(prefix: Option<&str>, model: Model) -> Cow<'static, str> {
    let name = match model {
        Model::Glm53 => "glm-5.3",
        _ => model.as_str(),
    };
    match prefix {
        Some(prefix) => Cow::Owned(format!("{prefix}/{name}")),
        None => Cow::Borrowed(model.as_str()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CyberAccessProgram {
    Standard,
    DaybreakBlue,
    DaybreakRed,
}

impl CyberAccessProgram {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::DaybreakBlue => "daybreak_blue",
            Self::DaybreakRed => "daybreak_red",
        }
    }
}

#[derive(Clone)]
pub struct ModelConfig {
    pub model: Model,

    pub model_id_prefix: Option<Arc<str>>,

    pub auth: OpenAiAuth,

    pub reasoning_mode: ReasoningMode,

    pub thinking: Thinking,

    #[doc(hidden)]
    pub thinking_explicit: bool,

    pub service_tier: ServiceTier,

    pub cyber_access_program: Option<CyberAccessProgram>,

    pub model_cyber_access_programs: Vec<(Model, CyberAccessProgram)>,

    pub context_window_tokens: u64,

    pub auto_compact_token_limit: Option<u64>,

    pub max_output_tokens: Option<u64>,

    pub omit_custom_model_reasoning: bool,

    pub custom_tool_output_tokens: Option<usize>,

    pub lean_custom_tool_arguments: bool,

    pub persist_finish: bool,

    pub finish_check: bool,

    pub verification_requested: bool,
    pub single_verification: bool,

    pub working_state: Option<crate::session::working_state::WorkingStatePolicy>,

    pub headless: bool,

    pub codesign: bool,

    pub compaction_thinking: Option<Thinking>,

    pub responses_transport: ResponsesTransport,

    pub websocket_warmup: bool,

    pub raw_api_events: bool,

    pub tool_input_deltas: bool,

    pub hedging: Option<Arc<nanocode_hedge::Hedging>>,

    pub responses_history: ResponsesHistory,

    pub store_responses: bool,

    pub strict_json_schema: Option<StrictJsonSchema>,

    pub websocket_url: String,

    pub api_base_url: String,

    pub host_transport: Option<Arc<dyn crate::client::transport::host::HostTransport>>,

    pub chat_gateway: Option<Arc<crate::client::transport::ChatGateway>>,

    #[cfg(not(target_family = "wasm"))]
    pub http_layer: Option<Arc<dyn nanocode_http::layer::Layer>>,

    pub responses_gateway: Option<Arc<crate::client::transport::ResponsesGateway>>,

    pub system_prompt: Option<Arc<str>>,

    pub model_system_prompts: Vec<(Model, Arc<str>)>,

    pub additional_instructions: Option<Arc<str>>,
}

const SKILLS_SECTION_HEADING: &str = "# Using skills";
const SKILL_CATALOG_HEADING: &str = "### Available skills";
const UNSERVED_SECTION_HEADINGS: [&str; 1] = ["# Plugins and Apps"];
const HEADLESS_SECTION_HEADINGS: [&str; 4] = [
    "# When to ask the user for permission",
    "# Personality",
    "## Intermediate commentary",
    "## Final answer",
];
const HEADLESS_DROPPED_RULES: [&str; 2] = [
    "- For multiline PR descriptions",
    "- Keep implementation details out of product",
];
const HEADLESS_USER_SECTION: &str = "# Working with the user";
const HEADLESS_USER_PARAGRAPHS: [&str; 2] = [
    "When you run out of context",
    "Compaction does not end the task",
];
const HEADLESS_TASK: &str = concat!(
    "<headless_task>\n",
    "Nobody will reply in this session. Scripts you cannot see check the result after you finish, ",
    "in a fresh process, on inputs that can differ from the examples. Before your final message:\n",
    "- Run what you deliver as such a check would: a new process, with the output files of your own ",
    "earlier runs removed first. Prefer the standard library and what was installed before you ",
    "started over packages you add, since the check may run without them.\n",
    "- Leave the state the task asks for in place: each output at the exact path named, services ",
    "running in a way that outlasts your session, and repositories, branches and data not reset or ",
    "deleted after your own tests.\n",
    "- Keep the paths, names, units and conventions the task gives. Do not infer an offset, base, ",
    "scale or unit change from an example alone: use values as the input records them, in the ",
    "units the task names. Call a named library through its documented mode for the job, with its ",
    "defaults.\n",
    "- When you edit existing content, change only what was asked: diff the result against the ",
    "original and confirm nothing else changed, including quotes, delimiters and whitespace.\n",
    "- Check the result once by a method that does not share your assumptions, such as the tool ",
    "the task names or an input whose right answer you know, not only a checker you wrote ",
    "alongside it.\n",
    "</headless_task>"
);
const HEADLESS_TASK_SINGLE_VERIFICATION: &str = concat!(
    "<headless_task>\n",
    "Nobody will reply in this session. Scripts you cannot see check the result after you finish, ",
    "in a fresh process, on inputs that can differ from the examples.\n",
    "- Prefer the standard library and what was installed before you started over packages you ",
    "add, since the check may run without them.\n",
    "- Leave the state the task asks for in place: each output at the exact path named, services ",
    "running in a way that outlasts your session, and repositories, branches and data not reset or ",
    "deleted after your own tests.\n",
    "- Keep the paths, names, units and conventions the task gives. Do not infer an offset, base, ",
    "scale or unit change from an example alone: use values as the input records them, in the ",
    "units the task names. Call a named library through its documented mode for the job, with its ",
    "defaults.\n",
    "- When you edit existing content, change only what was asked, including quotes, delimiters ",
    "and whitespace.\n",
    "- Before your final message, verify once: run the deliverable as the check would, in a new ",
    "process with the outputs of your earlier runs removed, on a case whose right answer you know ",
    "or with the tool the task names. Fix and run it again only if that run fails; do not repeat ",
    "checks that passed.\n",
    "</headless_task>"
);
const VERIFICATION_REQUEST: &str = concat!(
    "<verification_request>\n",
    "The user asks you to test and verify the implementation of every task in this session. ",
    "Before your final message, run the tests and checks that come with the task, and test the ",
    "result directly on the cases the task states. Fix what fails, then run them again.\n",
    "</verification_request>"
);

fn heading_level(line: &str) -> Option<usize> {
    let level = line.bytes().take_while(|byte| *byte == b'#').count();
    (level > 0 && line.as_bytes().get(level) == Some(&b' ')).then_some(level)
}

fn without_sections(prompt: &str, headings: &[&str]) -> String {
    let mut kept = String::with_capacity(prompt.len());
    let mut skipping: Option<usize> = None;
    for line in prompt.split_inclusive('\n') {
        let text = line.trim_end_matches(['\n', '\r']);
        if let Some(level) = heading_level(text)
            && skipping.is_none_or(|skipped| level <= skipped)
        {
            skipping = headings.contains(&text).then_some(level);
        }
        if skipping.is_none() {
            kept.push_str(line);
        }
    }
    let trimmed = kept.trim_end().len();
    kept.truncate(trimmed);
    kept
}

fn keep_section_paragraphs(prompt: &str, heading: &str, starts: &[&str]) -> String {
    let mut inside = false;
    let mut kept = Vec::new();
    for paragraph in prompt.split("\n\n") {
        let first = paragraph.lines().next().unwrap_or_default();
        if let Some(level) = heading_level(first) {
            if level == 1 {
                inside = first == heading;
            }
            kept.push(paragraph);
            continue;
        }
        if !inside || starts.iter().any(|start| paragraph.starts_with(start)) {
            kept.push(paragraph);
        }
    }
    kept.join("\n\n")
}

impl ModelConfig {
    pub(crate) fn wire_model_id(&self, model: Model) -> Cow<'static, str> {
        prefixed_model_id(self.model_id_prefix.as_deref(), model)
    }

    #[doc(hidden)]
    pub fn retarget(&mut self, model: Model) {
        if let Some(gateway) = self.responses_gateway.as_deref()
            && self.model_id_prefix.as_deref() == gateway.model_id_prefix_for(self.model)
        {
            self.model_id_prefix = gateway.model_id_prefix_for(model).map(Arc::from);
        }
        self.model = model;
    }

    #[must_use]
    pub fn cyber_access_program_for(&self, model: Model) -> Option<CyberAccessProgram> {
        self.model_cyber_access_programs
            .iter()
            .find(|(configured, _)| *configured == model)
            .map(|(_, program)| *program)
            .or(self.cyber_access_program)
    }

    #[must_use]
    pub const fn supports_reasoning_effort_updates(&self, model: Model) -> bool {
        model.supports_reasoning_effort_updates() && self.model_id_prefix.is_none()
    }

    #[must_use]
    pub const fn headless_tuning(&self) -> bool {
        self.headless && self.codesign
    }

    #[must_use]
    pub const fn orchestration() -> &'static str {
        "local_code_mode"
    }

    #[must_use]
    #[doc(hidden)]
    pub fn base_system_prompt(&self) -> Cow<'_, str> {
        let base = self.system_prompt.as_deref().unwrap_or(match self.model {
            Model::Sol62 | Model::Astra62 => SOL_62_SYSTEM_PROMPT,
            Model::Astra | Model::Terra => ASTRA_SYSTEM_PROMPT,
            Model::Glm53 | Model::Kimi | Model::Mimo => GLM_SYSTEM_PROMPT,
            Model::Custom(_) => UNKNOWN_MODEL_SYSTEM_PROMPT,
            Model::Sol | Model::DaybreakBlue => SOL_SYSTEM_PROMPT,
            Model::Luna => LUNA_SYSTEM_PROMPT,
            _ => ASTRA_SYSTEM_PROMPT,
        });
        let base =
            if self.system_prompt.is_none() && matches!(self.model, Model::Kimi | Model::Mimo) {
                Cow::Owned(base.replacen(
                    "powered by Z.ai GLM-5.3",
                    &format!("powered by {}", self.model.as_str()),
                    1,
                ))
            } else {
                Cow::Borrowed(base)
            };
        if self.system_prompt.is_some() || !self.codesign {
            return base;
        }
        let mut dropped: Vec<&str> =
            if crate::sim::mutated(crate::sim::Mutation::UnservedSectionsKept) {
                Vec::new()
            } else {
                UNSERVED_SECTION_HEADINGS.to_vec()
            };
        if !self
            .additional_instructions
            .as_deref()
            .is_some_and(|instructions| instructions.contains(SKILL_CATALOG_HEADING))
        {
            dropped.push(SKILLS_SECTION_HEADING);
        }
        if self.headless_tuning() {
            dropped.extend(HEADLESS_SECTION_HEADINGS);
        }
        let base = if base
            .lines()
            .any(|line| dropped.contains(&line.trim_end_matches('\r')))
        {
            Cow::Owned(without_sections(&base, &dropped))
        } else {
            base
        };
        if !self.headless_tuning() || !base.lines().any(|line| line == HEADLESS_USER_SECTION) {
            return base;
        }
        let kept = keep_section_paragraphs(&base, HEADLESS_USER_SECTION, &HEADLESS_USER_PARAGRAPHS);
        Cow::Owned(
            kept.split_inclusive('\n')
                .filter(|line| {
                    !HEADLESS_DROPPED_RULES
                        .iter()
                        .any(|rule| line.starts_with(rule))
                })
                .collect(),
        )
    }

    #[must_use]
    pub fn system_prompt(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "{}\n\n{}",
            self.base_system_prompt(),
            self.host_instructions()
        ))
    }

    #[doc(hidden)]
    pub fn host_instructions(&self) -> String {
        let mut instructions = self.standing_instructions();
        if self.headless_tuning() {
            if !instructions.is_empty() {
                instructions.push_str("\n\n");
            }
            instructions.push_str(if self.single_verification {
                HEADLESS_TASK_SINGLE_VERIFICATION
            } else {
                HEADLESS_TASK
            });
        }
        if self.verification_requested {
            if !instructions.is_empty() {
                instructions.push_str("\n\n");
            }
            instructions.push_str(VERIFICATION_REQUEST);
        }
        instructions
    }

    fn standing_instructions(&self) -> String {
        let mut instructions = String::new();
        if let Some(additional) = self.additional_instructions.as_deref()
            && !additional.is_empty()
        {
            instructions.push_str(additional);
            if matches!(self.model, Model::Custom(_)) {
                return instructions;
            }
            instructions.push_str("\n\n");
        }
        if !self.codesign || matches!(self.model, Model::Custom(_)) {
            return instructions;
        }
        instructions.push_str(&format!(
            "<runtime_model_identity>\nmodel_id: {}\n\
             This is the runtime-selected model serving this agent. When asked which model \
             you are, report this model ID. Nanocode and Codex are product or harness names, \
             not the underlying model. Other agents and models listed in tools can differ \
             from this agent. Use this current runtime identity over conflicting identity \
             claims in earlier conversation or inherited instructions.\n\
             </runtime_model_identity>",
            self.model.as_str(),
        ));
        instructions.push_str("\n\nWrite numeric answers in plain decimal (16.35, not 1.635e+01) unless the task asks for another format.");
        instructions
    }

    #[must_use]
    pub fn search_endpoint(&self) -> String {
        format!("{}/alpha/search", self.api_base_url.trim_end_matches('/'))
    }
}

impl Default for ModelConfig {
    fn default() -> Self {
        #[cfg(not(target_family = "wasm"))]
        {
            let mut config = Self {
                auth: OpenAiAuth::chatgpt(),
                ..Self::api_key_defaults()
            };
            let mode = config.auth.mode();
            super::apply_mode_defaults(&mut config, mode);
            config
        }
        #[cfg(target_family = "wasm")]
        Self::api_key_defaults()
    }
}

impl ModelConfig {
    fn api_key_defaults() -> Self {
        Self {
            model: Model::default(),
            model_id_prefix: None,
            auth: OpenAiAuth::api_key(String::new()),
            reasoning_mode: ReasoningMode::default(),
            thinking: Thinking::default(),
            thinking_explicit: false,
            service_tier: ServiceTier::Standard,
            cyber_access_program: None,
            context_window_tokens: CONTEXT_WINDOW_TOKENS,
            auto_compact_token_limit: None,
            max_output_tokens: None,
            omit_custom_model_reasoning: false,
            custom_tool_output_tokens: None,
            lean_custom_tool_arguments: false,
            persist_finish: false,
            finish_check: false,
            verification_requested: false,
            single_verification: false,
            working_state: None,
            headless: false,
            codesign: true,
            compaction_thinking: None,
            responses_transport: ResponsesTransport::default(),
            websocket_warmup: false,
            raw_api_events: false,
            tool_input_deltas: false,
            hedging: None,
            responses_history: ResponsesHistory::default(),
            store_responses: false,
            strict_json_schema: None,
            websocket_url: "wss://api.openai.com/v1/responses".to_owned(),
            api_base_url: "https://api.openai.com/v1".to_owned(),
            host_transport: None,
            chat_gateway: None,
            #[cfg(not(target_family = "wasm"))]
            http_layer: None,
            responses_gateway: None,
            system_prompt: None,
            model_system_prompts: Vec::new(),
            model_cyber_access_programs: Vec::new(),
            additional_instructions: None,
        }
    }
}
