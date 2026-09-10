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
#[serde(default)]
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
