use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessIsolation {
    #[default]
    ProcessGroup,
    SystemdUserUnit,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProcessExecutorSpec {
    pub isolation: ProcessIsolation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MountAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DockerMount {
    pub source: PathBuf,
    pub target: PathBuf,
    pub access: MountAccess,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DockerExecutorSpec {
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default)]
    pub mounts: Vec<DockerMount>,
    #[serde(default = "default_true")]
    pub remove_container: bool,
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "settings", rename_all = "snake_case")]
pub enum ExecutorSpec {
    Process(ProcessExecutorSpec),
    Docker(DockerExecutorSpec),
}

impl Default for ExecutorSpec {
    fn default() -> Self {
        Self::Process(ProcessExecutorSpec::default())
    }
}
