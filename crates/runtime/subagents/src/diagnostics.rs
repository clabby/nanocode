use serde::Serialize;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionErrorCode {
    NotChild,
    MissingInstructionRevision,
    InactiveTurn,

    SteeringInProgress,

    StaleTurnToken,
    AlreadyAccepted,
    SchemaValidation,
    MissingResult,
}

#[derive(Clone, Debug, Serialize)]
pub struct CompletionError {
    pub code: CompletionErrorCode,
    pub recoverable: bool,
    pub message: &'static str,
    pub recovery: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_turn_token: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<String>,
}

impl CompletionError {
    pub(crate) const fn new(
        code: CompletionErrorCode,
        recoverable: bool,
        message: &'static str,
        recovery: &'static str,
    ) -> Self {
        Self {
            code,
            recoverable,
            message,
            recovery,
            current_turn_token: None,
            details: Vec::new(),
        }
    }

    pub(crate) fn with_details(mut self, details: Vec<String>) -> Self {
        self.details = details
            .into_iter()
            .take(4)
            .map(|mut detail| {
                detail.truncate(detail.floor_char_boundary(512));
                detail
            })
            .collect();
        self
    }
}

impl fmt::Display for CompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.message, self.recovery)?;
        if let Some(token) = self.current_turn_token {
            write!(formatter, " Current turn_token: {token}.")?;
        }
        for detail in &self.details {
            write!(formatter, " {detail}.")?;
        }
        Ok(())
    }
}

impl std::error::Error for CompletionError {}

impl From<CompletionError> for std::io::Error {
    fn from(error: CompletionError) -> Self {
        Self::new(std::io::ErrorKind::InvalidInput, error)
    }
}
