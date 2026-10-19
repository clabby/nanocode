use crate::client::ResponseError;
pub use crate::client::transport::ResponsesError;
pub use nanocode_agent_core::{
    CompactionRecovery, ExecutionPolicyDisposition, NanocodeError, Result,
};

pub trait ErrorExt {
    fn responses_error(&self) -> Option<&ResponsesError>;

    fn rejected_request_repair(&self) -> crate::client::__private::RejectedRequestRepair<'_>;
}
impl ErrorExt for NanocodeError {
    fn responses_error(&self) -> Option<&ResponsesError> {
        match self {
            Self::Response(error) => error
                .as_any()
                .downcast_ref::<ResponseError>()
                .and_then(ResponseError::responses_error),
            Self::Shutdown(error) => error.responses_error(),
            _ => None,
        }
    }
    fn rejected_request_repair(&self) -> crate::client::__private::RejectedRequestRepair<'_> {
        use crate::client::__private::RejectedRequestRepair;
        RejectedRequestRepair {
            replace_images: self.requires_image_repair(),
            ..self
                .responses_error()
                .map(RejectedRequestRepair::for_error)
                .unwrap_or_default()
        }
    }
}
