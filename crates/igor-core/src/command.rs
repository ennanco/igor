use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{DomainError, ErrorCode, Result};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShellPolicy {
    #[default]
    Direct,
    Shell,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentInheritance {
    None,
    #[default]
    Minimal,
    All,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct EnvironmentPolicy {
    pub set: BTreeMap<String, String>,
    pub remove: Vec<String>,
    pub inherit: EnvironmentInheritance,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandSpec {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: PathBuf,
    #[serde(default)]
    pub shell: ShellPolicy,
    #[serde(default)]
    pub environment: EnvironmentPolicy,
}

impl CommandSpec {
    pub fn validate(&self) -> Result<()> {
        if self.program.is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidCommand,
                "command.program",
                "must not be empty",
            ));
        }
        if self.cwd.as_os_str().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidCommand,
                "command.cwd",
                "must not be empty",
            ));
        }
        if let Some(name) = self
            .environment
            .remove
            .iter()
            .find(|name| self.environment.set.contains_key(*name))
        {
            return Err(DomainError::validation(
                ErrorCode::InvalidCommand,
                "command.environment",
                &format!("variable {name:?} cannot be both set and removed"),
            ));
        }
        Ok(())
    }
}

#[must_use]
pub fn environment_variable_is_sensitive(name: &str) -> bool {
    let uppercase = name.to_ascii_uppercase();
    uppercase.starts_with("IGOR_")
        || uppercase.starts_with("TELEGRAM_")
        || uppercase.starts_with("OPENCODE_")
        || uppercase.starts_with("ANTHROPIC_")
        || uppercase.starts_with("OPENAI_")
        || uppercase.starts_with("GEMINI_")
        || uppercase.starts_with("GOOGLE_API_")
        || uppercase.starts_with("CLAUDE_")
        || uppercase.starts_with("CODEX_")
        || uppercase.starts_with("CURSOR_")
        || uppercase.starts_with("AIDER_")
        || uppercase.starts_with("CONTINUE_")
        || uppercase.contains("TOKEN")
        || uppercase.contains("SECRET")
        || uppercase.contains("PASSWORD")
        || uppercase.contains("CREDENTIAL")
        || uppercase.contains("COOKIE")
        || uppercase.contains("SESSION")
        || uppercase.contains("JWT")
        || uppercase.ends_with("_KEY")
        || uppercase.ends_with("_PAT")
        || uppercase.ends_with("_DSN")
        || uppercase.ends_with("_AUTH")
        || uppercase == "DATABASE_URL"
        || uppercase.starts_with("AWS_ACCESS_KEY")
        || matches!(
            uppercase.as_str(),
            "SSH_AUTH_SOCK"
                | "GPG_AGENT_INFO"
                | "XAUTHORITY"
                | "KUBECONFIG"
                | "DOCKER_CONFIG"
                | "NETRC"
                | "PIP_CONFIG_FILE"
                | "NPM_CONFIG_USERCONFIG"
                | "GIT_ASKPASS"
                | "SSH_ASKPASS"
                | "PGPASSFILE"
                | "PGSERVICEFILE"
                | "BOTO_CONFIG"
                | "AWS_CONFIG_FILE"
                | "AZURE_CONFIG_DIR"
                | "CLOUDSDK_CONFIG"
                | "GNUPGHOME"
        )
}

#[cfg(test)]
mod tests {
    use super::environment_variable_is_sensitive;

    #[test]
    fn detects_service_agent_and_credential_environment_variables() {
        for name in [
            "IGOR_STATE_DIR",
            "TELEGRAM_BOT_TOKEN",
            "OPENCODE_CONFIG",
            "CLAUDE_CODE_ENTRYPOINT",
            "CODEX_HOME",
            "AWS_SESSION_TOKEN",
            "SSH_AUTH_SOCK",
            "GIT_ASKPASS",
            "PGPASSFILE",
            "DATABASE_URL",
        ] {
            assert!(environment_variable_is_sensitive(name), "{name}");
        }
        for name in ["HOME", "LANG", "PATH", "SAFE_VALUE"] {
            assert!(!environment_variable_is_sensitive(name), "{name}");
        }
    }
}
