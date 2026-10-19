use nanocode_agent_core::{NanocodeError, Result, SessionInfo};
use serde_json::{Value, value::RawValue};
use std::{future::Future, pin::Pin, sync::Arc};

#[cfg(not(target_family = "wasm"))]
pub type PolicyFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[cfg(target_family = "wasm")]
pub type PolicyFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

pub enum Admission {
    Execute,

    Resume,

    Completed { checkpoint: Value, output: Value },

    Failed { checkpoint: Value, error: String },

    Cancelled,
}

pub enum Step {
    Execute,

    Interrupted,

    Replay(Value),
}

pub struct ClaudeSteer {
    pub message_id: Option<String>,

    pub index: u32,

    pub accepted_after_model_call_index: u32,

    pub model_call_index: Option<u32>,

    pub input_json: String,
}

pub trait ClaudeExecutionPolicy: Send + Sync {
    fn state_id(&self) -> &str;
    fn admit(
        &self,
        id: String,
        input: Value,
        automatic: bool,
    ) -> PolicyFuture<'_, (String, Admission)>;
    fn begin_attempt(&self, id: String) -> PolicyFuture<'_, ()>;

    fn supports_steering(&self) -> bool {
        false
    }

    fn identified_steer_accepted(
        &self,
        _message_id: String,
        _input_json: String,
    ) -> PolicyFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn accept_steer(
        &self,
        _id: String,
        _message_id: Option<String>,
        _after: u32,
        _input_json: String,
    ) -> PolicyFuture<'_, Option<u32>> {
        Box::pin(async {
            Err(NanocodeError::InvalidRequest(
                "Claude execution policy does not support accept_steer".into(),
            ))
        })
    }
    fn retained_steers(&self, _id: String) -> PolicyFuture<'_, Vec<ClaudeSteer>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn withdraw_steer(&self, _id: String, _index: u32) -> PolicyFuture<'_, ()> {
        Box::pin(async {
            Err(NanocodeError::InvalidRequest(
                "Claude execution policy does not support withdraw_steer".into(),
            ))
        })
    }
    fn bind_steer(&self, _id: String, _index: u32, _boundary: u32) -> PolicyFuture<'_, ()> {
        Box::pin(async {
            Err(NanocodeError::InvalidRequest(
                "Claude execution policy does not support bind_steer".into(),
            ))
        })
    }
    fn continuation(&self, id: String) -> PolicyFuture<'_, Option<Value>>;
    fn advance(&self, id: String, state: Value) -> PolicyFuture<'_, ()>;

    #[allow(clippy::boxed_local)]
    fn advance_encoded(&self, id: String, state: Box<RawValue>) -> PolicyFuture<'_, ()> {
        match serde_json::from_str(state.get()) {
            Ok(state) => self.advance(id, state),
            Err(error) => Box::pin(async move {
                Err(NanocodeError::InvalidRequest(format!(
                    "invalid encoded Claude execution state: {error}"
                )))
            }),
        }
    }
    fn begin_step(
        &self,
        id: String,
        step_id: String,
        kind: String,
        input: Value,
    ) -> PolicyFuture<'_, Step>;

    #[allow(clippy::boxed_local)]
    fn begin_step_encoded(
        &self,
        id: String,
        step_id: String,
        kind: String,
        input: Box<RawValue>,
    ) -> PolicyFuture<'_, Step> {
        match serde_json::from_str(input.get()) {
            Ok(input) => self.begin_step(id, step_id, kind, input),
            Err(error) => Box::pin(async move {
                Err(NanocodeError::InvalidRequest(format!(
                    "invalid encoded Claude execution state: {error}"
                )))
            }),
        }
    }
    fn complete_step(&self, id: String, step_id: String, output: Value) -> PolicyFuture<'_, ()>;
    fn complete(&self, id: String, checkpoint: Value, output: Value) -> PolicyFuture<'_, ()>;
    fn fail(&self, id: String, checkpoint: Value, error: String) -> PolicyFuture<'_, ()>;
    fn fail_attempt(&self, id: String, checkpoint: Value, error: String) -> PolicyFuture<'_, ()> {
        self.fail(id, checkpoint, error)
    }
    fn cancel(&self, id: String, checkpoint: Value) -> PolicyFuture<'_, ()>;

    fn cancel_unstarted(&self, _id: String) -> PolicyFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn release(&self, id: String) -> PolicyFuture<'_, ()>;
    fn shutdown(&self) -> PolicyFuture<'_, ()>;
    fn checkpoint(&self, state: Value) -> PolicyFuture<'_, ()>;

    fn developer_notices(&self, _id: String) -> PolicyFuture<'_, Vec<(String, String)>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn initial_checkpoint(
        &self,
        _state: Value,
        _model: nanocode_agent_core::ModelSelection,
    ) -> PolicyFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn discard_initial(&self) -> PolicyFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn branch(&self, _child: &SessionInfo) -> Result<Option<Arc<dyn ClaudeExecutionPolicy>>> {
        Ok(None)
    }
}
