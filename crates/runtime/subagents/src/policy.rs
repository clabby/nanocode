use nanocode_agent_core::{Model, ModelSelection, ReasoningMode, Thinking};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpawnPolicy {
    pub models: Option<Vec<ModelSelection>>,
    pub model_ladder: Vec<ModelSelection>,
    pub min_thinking: Option<Thinking>,
    pub max_thinking: Option<Thinking>,
    pub bounded_by_parent_thinking: bool,
    pub max_reasoning_mode: Option<ReasoningMode>,
}

impl Default for SpawnPolicy {
    fn default() -> Self {
        Self {
            models: None,
            model_ladder: [Model::Luna, Model::Sol, Model::Astra]
                .into_iter()
                .map(ModelSelection::Codex)
                .collect(),
            min_thinking: Some(Thinking::Low),
            max_thinking: None,
            bounded_by_parent_thinking: true,
            max_reasoning_mode: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpawnPolicyError {
    ModelNotOffered {
        requested: ModelSelection,
    },
    ModelExceedsParent {
        requested: ModelSelection,
        parent: ModelSelection,
    },
    ThinkingBelowMinimum {
        requested: Thinking,
        minimum: Thinking,
    },
    ThinkingExceedsMaximum {
        requested: Thinking,
        maximum: Thinking,
    },
    ThinkingExceedsParent {
        requested: Thinking,
        parent: Thinking,
    },
    ParentThinkingUnknown,
    ReasoningModeExceedsMaximum {
        requested: ReasoningMode,
        maximum: ReasoningMode,
    },
    ReasoningModeExceedsParent {
        requested: ReasoningMode,
        parent: ReasoningMode,
    },
    ChildSettingsUnknown {
        reason: String,
    },
}

impl fmt::Display for SpawnPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelNotOffered { requested } => write!(
                formatter,
                "model {} ({}) is not offered to subagents",
                requested.as_str(),
                requested.family()
            ),
            Self::ModelExceedsParent { requested, parent } => write!(
                formatter,
                "model {} ranks above the spawning agent's model {}",
                requested.as_str(),
                parent.as_str()
            ),
            Self::ThinkingBelowMinimum { requested, minimum } => write!(
                formatter,
                "{requested} thinking is below the subagent minimum of {minimum}"
            ),
            Self::ThinkingExceedsMaximum { requested, maximum } => write!(
                formatter,
                "{requested} thinking exceeds the subagent maximum of {maximum}"
            ),
            Self::ThinkingExceedsParent { requested, parent } => write!(
                formatter,
                "{requested} thinking exceeds the spawning agent's own {parent} thinking"
            ),
            Self::ParentThinkingUnknown => formatter.write_str(
                "the spawning agent's thinking is not recorded, so its children cannot be bounded by it",
            ),
            Self::ReasoningModeExceedsMaximum { requested, maximum } => write!(
                formatter,
                "{requested} reasoning mode exceeds the subagent maximum of {maximum}"
            ),
            Self::ReasoningModeExceedsParent { requested, parent } => write!(
                formatter,
                "{requested} reasoning mode exceeds the spawning agent's own {parent} reasoning mode"
            ),
            Self::ChildSettingsUnknown { reason } => write!(
                formatter,
                "the child's effective model and thinking could not be read for policy admission: {reason}"
            ),
        }
    }
}

impl std::error::Error for SpawnPolicyError {}

impl From<SpawnPolicyError> for std::io::Error {
    fn from(error: SpawnPolicyError) -> Self {
        Self::other(error)
    }
}

pub(crate) struct SpawnParent {
    pub(crate) model: ModelSelection,
    pub(crate) registered: bool,
    pub(crate) thinking: Option<Thinking>,
    pub(crate) reasoning_mode: ReasoningMode,
}

impl SpawnPolicy {
    pub fn is_unrestricted(&self) -> bool {
        self.models.is_none()
            && self.model_ladder.is_empty()
            && self.min_thinking.is_none()
            && self.max_thinking.is_none()
            && !self.bounded_by_parent_thinking
            && self.max_reasoning_mode.is_none()
    }

    pub fn offered_models(&self) -> Option<&[ModelSelection]> {
        self.models.as_deref()
    }

    pub fn offered_thinking(&self) -> Vec<Thinking> {
        Thinking::ALL
            .into_iter()
            .filter(|thinking| {
                self.min_thinking
                    .is_none_or(|minimum| rank(*thinking) >= rank(minimum))
                    && self
                        .max_thinking
                        .is_none_or(|maximum| rank(*thinking) <= rank(maximum))
            })
            .collect()
    }

    pub fn admit_model(
        &self,
        parent: &ModelSelection,
        model: &ModelSelection,
    ) -> Result<(), SpawnPolicyError> {
        if let Some(models) = &self.models
            && !models.contains(model)
        {
            return Err(SpawnPolicyError::ModelNotOffered {
                requested: model.clone(),
            });
        }
        if let (Some(parent_rank), Some(child_rank)) =
            (self.ladder_rank(parent), self.ladder_rank(model))
            && child_rank > parent_rank
        {
            return Err(SpawnPolicyError::ModelExceedsParent {
                requested: model.clone(),
                parent: parent.clone(),
            });
        }
        Ok(())
    }

    pub fn admit_thinking(
        &self,
        parent: Option<Thinking>,
        thinking: Thinking,
    ) -> Result<(), SpawnPolicyError> {
        if let Some(minimum) = self.min_thinking
            && rank(thinking) < rank(minimum)
        {
            return Err(SpawnPolicyError::ThinkingBelowMinimum {
                requested: thinking,
                minimum,
            });
        }
        if let Some(maximum) = self.max_thinking
            && rank(thinking) > rank(maximum)
        {
            return Err(SpawnPolicyError::ThinkingExceedsMaximum {
                requested: thinking,
                maximum,
            });
        }
        if let Some(parent) = parent
            && rank(thinking) > rank(parent)
        {
            return Err(SpawnPolicyError::ThinkingExceedsParent {
                requested: thinking,
                parent,
            });
        }
        Ok(())
    }

    pub(crate) fn admit(
        &self,
        parent: &SpawnParent,
        model: Option<&ModelSelection>,
        thinking: Option<Thinking>,
    ) -> Result<(), SpawnPolicyError> {
        if let Some(model) = model {
            self.admit_model(&parent.model, model)?;
        }
        if let Some(thinking) = thinking {
            let bound = if self.bounded_by_parent_thinking && parent.registered {
                Some(
                    parent
                        .thinking
                        .ok_or(SpawnPolicyError::ParentThinkingUnknown)?,
                )
            } else {
                None
            };
            self.admit_thinking(bound, thinking)?;
            if self.max_thinking.is_none() && !parent.registered {
                self.admit_thinking(parent.thinking, thinking)?;
            }
        }
        Ok(())
    }

    pub(crate) fn admit_reasoning_mode(
        &self,
        parent: &SpawnParent,
        requested: ReasoningMode,
    ) -> Result<(), SpawnPolicyError> {
        let maximum = self.max_reasoning_mode.unwrap_or(parent.reasoning_mode);
        if requested == ReasoningMode::Pro && maximum == ReasoningMode::Standard {
            return Err(SpawnPolicyError::ReasoningModeExceedsMaximum { requested, maximum });
        }
        if parent.registered
            && requested == ReasoningMode::Pro
            && parent.reasoning_mode == ReasoningMode::Standard
        {
            return Err(SpawnPolicyError::ReasoningModeExceedsParent {
                requested,
                parent: parent.reasoning_mode,
            });
        }
        Ok(())
    }

    fn ladder_rank(&self, model: &ModelSelection) -> Option<usize> {
        self.model_ladder.iter().position(|rung| {
            rung == model
                || rung
                    .as_codex()
                    .zip(model.as_codex())
                    .is_some_and(|(rung, model)| rung == model)
        })
    }
}

impl crate::Registry {
    pub fn spawn_policy(&self) -> SpawnPolicy {
        SpawnPolicy::clone(&self.spawn_policy.get())
    }

    pub fn set_spawn_policy(&self, policy: SpawnPolicy) {
        self.spawn_policy.set(policy);
    }

    pub fn update_spawn_policy(&self, update: impl FnMut(&mut SpawnPolicy)) {
        self.spawn_policy.update(update);
    }
}

const fn rank(thinking: Thinking) -> u8 {
    thinking as u8
}
