use std::sync::Arc;

use serde_json::Value;

use crate::{
    Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolOutputContent, ToolResult,
    embedded::HostFuture,
};

pub trait FunctionHandler: Send + Sync + 'static {
    fn call<'a>(
        &'a self,
        name: &'a str,
        input: Value,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, Result<Value, String>>;
}

pub struct FunctionTool {
    name: String,
    description: String,
    parameters: Value,
    handler: Arc<dyn FunctionHandler>,
    parallel: bool,
    content_results: bool,
}

impl FunctionTool {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: Value,
        handler: Arc<dyn FunctionHandler>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
            handler,
            parallel: false,
            content_results: false,
        }
    }

    #[must_use]
    pub const fn parallel_tool_calls(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    #[must_use]
    pub const fn content_results(mut self, enabled: bool) -> Self {
        self.content_results = enabled;
        self
    }

    fn output(&self, output: Value) -> ToolOutput {
        match output {
            Value::String(output) => ToolOutput::text(output),
            Value::Object(mut fields) if self.content_results && fields.contains_key("content") => {
                let structured = fields.remove("structured_result");
                let content = fields.remove("content").unwrap_or_default();
                if let Some(field) = fields.keys().next() {
                    return ToolOutput::error(format!(
                        "{} returned a content result with the unknown field {field}",
                        self.name
                    ));
                }
                match (
                    serde_json::from_value::<Vec<ToolOutputContent>>(content),
                    structured,
                ) {
                    (Ok(content), Some(structured)) => {
                        ToolOutput::content(content).with_structured_result(structured)
                    }
                    (Ok(content), None) => ToolOutput::content(content),
                    (Err(error), _) => ToolOutput::error(format!(
                        "{} returned an invalid content result: {error}",
                        self.name
                    )),
                }
            }
            output => ToolOutput::from_json(output, true),
        }
    }
}

impl Tool for FunctionTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            self.name.as_str(),
            self.description.as_str(),
            self.parameters.clone(),
        )
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        self.parallel
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        context: ToolContext<'life1>,
    ) -> HostFuture<'async_trait, ToolResult>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let input = input.decode_json()?;
            Ok(match self.handler.call(&self.name, input, context).await {
                Ok(output) => self.output(output),
                Err(error) => ToolOutput::error(error),
            })
        })
    }
}
