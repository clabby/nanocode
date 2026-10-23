#[cfg(not(target_family = "wasm"))]
mod agents_md;

#[cfg(not(target_family = "wasm"))]
#[path = "native.rs"]
mod platform;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
#[path = "web.rs"]
mod platform;

use std::sync::Arc;

#[derive(Clone)]
pub struct ExecutionEnvironment {
    pub(crate) current_date: Arc<dyn Fn() -> String + Send + Sync>,
    pub(crate) timezone: Arc<str>,
    pub(crate) project_instructions: Option<Arc<str>>,
    pub(crate) working_directory: Option<Arc<str>>,
    pub(crate) shell: Option<Arc<str>>,
}

impl ExecutionEnvironment {
    #[must_use]
    pub fn new(current_date: impl Into<Arc<str>>, timezone: impl Into<Arc<str>>) -> Self {
        let current_date: Arc<str> = current_date.into();
        Self::dated(timezone, move || current_date.to_string())
    }

    #[must_use]
    pub fn dated(
        timezone: impl Into<Arc<str>>,
        current_date: impl Fn() -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            current_date: Arc::new(current_date),
            timezone: timezone.into(),
            project_instructions: None,
            working_directory: None,
            shell: None,
        }
    }

    #[must_use]
    pub fn project_instructions(mut self, instructions: impl Into<Arc<str>>) -> Self {
        self.project_instructions = Some(instructions.into());
        self
    }

    #[must_use]
    pub fn working_directory(mut self, working_directory: impl Into<Arc<str>>) -> Self {
        self.working_directory = Some(working_directory.into());
        self
    }

    #[must_use]
    pub fn shell(mut self, shell: impl Into<Arc<str>>) -> Self {
        self.shell = Some(shell.into());
        self
    }

    pub(crate) fn current_date(&self) -> String {
        (self.current_date)()
    }
}

impl std::fmt::Debug for ExecutionEnvironment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionEnvironment")
            .field("current_date", &self.current_date())
            .field("timezone", &self.timezone)
            .field("project_instructions", &self.project_instructions)
            .field("working_directory", &self.working_directory)
            .field("shell", &self.shell)
            .finish()
    }
}

pub(crate) use platform::ContextSource;
pub(super) use platform::ContextSourceConfig;
