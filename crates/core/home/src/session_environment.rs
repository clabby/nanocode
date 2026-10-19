use std::{
    ffi::{OsStr, OsString},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionEnvironment {
    session_id: Arc<str>,
    root_session_id: Arc<str>,
}

impl SessionEnvironment {
    pub const SESSION_ID_VAR: &'static str = "NANOCODE_SESSION_ID";

    pub const ROOT_SESSION_ID_VAR: &'static str = "NANOCODE_ROOT_SESSION_ID";

    pub const VARIABLES: [&'static str; 2] = [Self::SESSION_ID_VAR, Self::ROOT_SESSION_ID_VAR];

    #[must_use]
    pub fn new(session_id: impl AsRef<str>, root_session_id: impl AsRef<str>) -> Self {
        Self {
            session_id: Arc::from(session_id.as_ref()),
            root_session_id: Arc::from(root_session_id.as_ref()),
        }
    }

    #[must_use]
    pub fn root(session_id: impl AsRef<str>) -> Self {
        let session_id: Arc<str> = Arc::from(session_id.as_ref());
        Self {
            root_session_id: Arc::clone(&session_id),
            session_id,
        }
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn root_session_id(&self) -> &str {
        &self.root_session_id
    }

    #[must_use]
    pub fn variables(&self) -> [(&'static str, &str); 2] {
        [
            (Self::SESSION_ID_VAR, &self.session_id),
            (Self::ROOT_SESSION_ID_VAR, &self.root_session_id),
        ]
    }

    #[must_use]
    pub fn os_variables(&self) -> Vec<(OsString, OsString)> {
        self.variables()
            .into_iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value)))
            .collect()
    }

    #[must_use]
    pub fn controls(name: &OsStr) -> bool {
        Self::VARIABLES.iter().any(|candidate| name == *candidate)
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn apply(&self, command: &mut std::process::Command) {
        command.envs(self.variables());
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn clear(command: &mut std::process::Command) {
        for name in Self::VARIABLES {
            command.env_remove(name);
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn apply_or_clear(session: Option<&Self>, command: &mut std::process::Command) {
        match session {
            Some(session) => session.apply(command),
            None => Self::clear(command),
        }
    }
}
