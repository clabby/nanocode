use std::{borrow::Cow, fmt, sync::Arc};

use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use serde::{Deserialize, Serialize};

use crate::{
    Answer, ChoiceProbability, ChoiceValue, Decision, DecisionError, DecisionInput, DecisionModel,
    DecisionRequest, DecisionUsage, ImageDetail, InputPart, LevelProbability, Outcome, Question,
    QuestionKind,
};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum OpenAiDecisionModel {
    #[default]
    Luna,
}

impl OpenAiDecisionModel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Luna => "gpt-6-luna",
        }
    }
}

impl fmt::Display for OpenAiDecisionModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone)]
pub struct OpenAiDecisions {
    client: reqwest::Client,
    endpoint: Arc<str>,
    api_key: Arc<str>,
    model: OpenAiDecisionModel,
}

impl OpenAiDecisions {
    pub fn new(api_key: impl Into<Arc<str>>) -> Self {
        Self::builder(api_key).build()
    }

    pub fn from_env() -> Result<Self, std::env::VarError> {
        std::env::var("OPENAI_API_KEY").map(Self::new)
    }

    pub fn builder(api_key: impl Into<Arc<str>>) -> OpenAiDecisionsBuilder {
        OpenAiDecisionsBuilder {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_owned(),
            model: OpenAiDecisionModel::default(),
            client: None,
        }
    }

    #[must_use]
    pub const fn model(&self) -> OpenAiDecisionModel {
        self.model
    }

    async fn post(&self, body: &WireRequest<'_>) -> Result<WireDecision, DecisionError> {
        let response = self
            .client
            .post(&*self.endpoint)
            .header(USER_AGENT, concat!("nanocode/", env!("CARGO_PKG_VERSION")))
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .json(body)
            .send()
            .await
            .map_err(transport)?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(transport)?;
        if !status.is_success() {
            return Err(DecisionError::Api {
                status: status.as_u16(),
                message: error_message(&bytes),
            });
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::InvalidResponse(error.to_string()))
    }
}

impl fmt::Debug for OpenAiDecisions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiDecisions")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl DecisionModel for OpenAiDecisions {
    async fn decide(&self, request: &DecisionRequest) -> Result<Decision, DecisionError> {
        let response = self.post(&WireRequest::new(self.model, request)).await?;
        let answers = response
            .answers
            .into_iter()
            .map(WireAnswer::into_answer)
            .collect::<Result<_, _>>()?;
        let usage = response.usage.map(|usage| DecisionUsage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
        });
        Decision::new(request, response.model, answers, usage)
    }
}

#[must_use]
pub struct OpenAiDecisionsBuilder {
    api_key: Arc<str>,
    base_url: String,
    model: OpenAiDecisionModel,
    client: Option<reqwest::Client>,
}

impl fmt::Debug for OpenAiDecisionsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiDecisionsBuilder")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl OpenAiDecisionsBuilder {
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub const fn model(mut self, model: OpenAiDecisionModel) -> Self {
        self.model = model;
        self
    }

    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    pub fn build(self) -> OpenAiDecisions {
        let client = self.client.unwrap_or_else(|| {
            nanocode_tls::install_default_rustls_crypto_provider();
            reqwest::Client::new()
        });
        OpenAiDecisions {
            client,
            endpoint: format!("{}/decisions", self.base_url.trim_end_matches('/')).into(),
            api_key: self.api_key,
            model: self.model,
        }
    }
}

fn transport(error: reqwest::Error) -> DecisionError {
    DecisionError::Transport(Box::new(error))
}

fn error_message(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct Envelope {
        error: Detail,
    }
    #[derive(Deserialize)]
    struct Detail {
        message: String,
    }
    if let Ok(envelope) = serde_json::from_slice::<Envelope>(body) {
        return envelope.error.message;
    }
    String::from_utf8_lossy(body).into_owned()
}

#[derive(Serialize)]
struct WireRequest<'a> {
    model: &'static str,
    input: WireInput<'a>,
    questions: Vec<WireQuestion<'a>>,
}

impl<'a> WireRequest<'a> {
    fn new(model: OpenAiDecisionModel, request: &'a DecisionRequest) -> Self {
        let input = match request.input() {
            DecisionInput::Text(text) => WireInput::Text(Cow::Borrowed(text)),
            DecisionInput::Record(record) => WireInput::Text(Cow::Owned(
                serde_json::Value::Object(record.clone()).to_string(),
            )),
            DecisionInput::Parts(parts) => WireInput::Messages([WireMessage {
                role: "user",
                content: parts.iter().map(WirePart::from).collect(),
            }]),
        };
        Self {
            model: model.as_str(),
            input,
            questions: request.questions().iter().map(WireQuestion::from).collect(),
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum WireInput<'a> {
    Text(Cow<'a, str>),
    Messages([WireMessage<'a>; 1]),
}

#[derive(Serialize)]
struct WireMessage<'a> {
    role: &'static str,
    content: Vec<WirePart<'a>>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WirePart<'a> {
    InputText {
        text: &'a str,
    },
    InputImage {
        image_url: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
}

impl<'a> From<&'a InputPart> for WirePart<'a> {
    fn from(part: &'a InputPart) -> Self {
        match part {
            InputPart::Text { text } => Self::InputText { text },
            InputPart::Image { image_url, detail } => Self::InputImage {
                image_url,
                detail: *detail,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireQuestion<'a> {
    Predicate {
        name: &'a str,
        instructions: &'a str,
    },
    Choice {
        name: &'a str,
        instructions: &'a str,
        choices: Vec<WireDescribed<'a, &'a ChoiceValue>>,
    },
    Score {
        name: &'a str,
        instructions: &'a str,
        levels: Vec<WireLevel<'a>>,
    },
}

impl<'a> From<&'a Question> for WireQuestion<'a> {
    fn from(question: &'a Question) -> Self {
        let name = &question.name;
        let instructions = &question.instructions;
        match &question.kind {
            QuestionKind::Predicate => Self::Predicate { name, instructions },
            QuestionKind::Choice { choices } => Self::Choice {
                name,
                instructions,
                choices: choices
                    .iter()
                    .map(|choice| WireDescribed {
                        value: &choice.value,
                        description: choice.description.as_deref(),
                    })
                    .collect(),
            },
            QuestionKind::Score { levels } => Self::Score {
                name,
                instructions,
                levels: levels
                    .iter()
                    .map(|level| WireLevel {
                        label: &level.label,
                        description: level.description.as_deref(),
                    })
                    .collect(),
            },
        }
    }
}

#[derive(Serialize)]
struct WireDescribed<'a, T> {
    value: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
}

#[derive(Serialize)]
struct WireLevel<'a> {
    label: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
}

#[derive(Deserialize)]
struct WireDecision {
    model: String,
    answers: Vec<WireAnswer>,
    usage: Option<WireUsage>,
}

#[derive(Deserialize)]
struct WireUsage {
    input_tokens: u64,
    output_tokens: u64,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireAnswer {
    Predicate {
        name: Option<String>,
        probability: f64,
    },
    Choice {
        name: Option<String>,
        choice: ChoiceValue,
        confidence: f64,
        probabilities: Vec<WireChoiceProbability>,
    },
    Score {
        name: Option<String>,
        score: f64,
        confidence: f64,
        probabilities: Vec<WireLevelProbability>,
    },
    Refusal {
        name: Option<String>,
    },
}

#[derive(Deserialize)]
struct WireChoiceProbability {
    value: ChoiceValue,
    probability: f64,
}

#[derive(Deserialize)]
struct WireLevelProbability {
    value: u32,
    label: String,
    probability: f64,
}

impl WireAnswer {
    fn into_answer(self) -> Result<Answer, DecisionError> {
        let (name, outcome) = match self {
            Self::Predicate { name, probability } => (name, Outcome::Predicate { probability }),
            Self::Choice {
                name,
                choice,
                confidence,
                probabilities,
            } => (
                name,
                Outcome::Choice {
                    choice,
                    confidence,
                    probabilities: probabilities
                        .into_iter()
                        .map(|entry| ChoiceProbability {
                            value: entry.value,
                            probability: entry.probability,
                        })
                        .collect(),
                },
            ),
            Self::Score {
                name,
                score,
                confidence,
                probabilities,
            } => (
                name,
                Outcome::Score {
                    score,
                    confidence,
                    probabilities: probabilities
                        .into_iter()
                        .map(|entry| LevelProbability {
                            index: entry.value,
                            label: entry.label,
                            probability: entry.probability,
                        })
                        .collect(),
                },
            ),
            Self::Refusal { name } => (name, Outcome::Refusal),
        };
        let name = name.ok_or_else(|| {
            DecisionError::InvalidResponse("an answer is missing its question name".to_owned())
        })?;
        Ok(Answer { name, outcome })
    }
}
