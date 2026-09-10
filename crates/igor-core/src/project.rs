use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::{
    ActionRetryPolicy, AttemptRetryPolicy, DeliveryRetryPolicy, DomainError, ErrorCategory,
    ErrorCode, ProjectId, ResourceRequest, Result,
};

pub const PROJECT_CONFIG_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub root: PathBuf,
    pub config_path: PathBuf,
}

impl Project {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "project.name",
                "must not be empty",
            ));
        }
        if self.root.as_os_str().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "project.root",
                "must not be empty",
            ));
        }
        if self.config_path.as_os_str().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "project.config_path",
                "must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReportTrigger {
    #[default]
    FamilyCompleted,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReportMode {
    #[default]
    ReplaceGeneratedFile,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportGenerator {
    Deterministic,
    #[default]
    Opencode,
    Pi,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectPaths {
    pub artifact_root: PathBuf,
    pub cleanup_roots: Vec<PathBuf>,
}

impl Default for ProjectPaths {
    fn default() -> Self {
        Self {
            artifact_root: PathBuf::from("artifacts"),
            cleanup_roots: vec![PathBuf::from("artifacts")],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReportConfig {
    pub enabled: bool,
    pub trigger: ReportTrigger,
    pub output: PathBuf,
    pub mode: ReportMode,
    pub generator: ReportGenerator,
    pub prompt_file: PathBuf,
}

impl Default for ReportConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            trigger: ReportTrigger::FamilyCompleted,
            output: PathBuf::from("RESULTS.md"),
            mode: ReportMode::ReplaceGeneratedFile,
            generator: ReportGenerator::Opencode,
            prompt_file: PathBuf::from(".igor/report-prompt.md"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProjectConfig {
    pub schema_version: u32,
    pub resources: ResourceRequest,
    pub paths: ProjectPaths,
    pub report: ReportConfig,
    pub attempt_retry: AttemptRetryPolicy,
    pub action_retry: ActionRetryPolicy,
    pub delivery_retry: DeliveryRetryPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectConfigDocument {
    schema_version: u32,
    #[serde(default)]
    resources: ResourceRequest,
    #[serde(default)]
    paths: ProjectPaths,
    #[serde(default)]
    report: ReportConfig,
    #[serde(default)]
    attempt_retry: AttemptRetryPolicy,
    #[serde(default)]
    action_retry: ActionRetryPolicy,
    #[serde(default)]
    delivery_retry: DeliveryRetryPolicy,
}

impl Default for ProjectConfigDocument {
    fn default() -> Self {
        Self {
            schema_version: PROJECT_CONFIG_VERSION,
            resources: ResourceRequest::default(),
            paths: ProjectPaths::default(),
            report: ReportConfig::default(),
            attempt_retry: AttemptRetryPolicy::default(),
            action_retry: ActionRetryPolicy::default(),
            delivery_retry: DeliveryRetryPolicy::default(),
        }
    }
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self::from_document(ProjectConfigDocument::default())
    }
}

impl ProjectConfig {
    pub fn from_json(json: &str) -> Result<Self> {
        let document: ProjectConfigDocument =
            serde_json::from_str(json).map_err(|error| DomainError::InvalidJson {
                category: ErrorCategory::Serialization,
                code: ErrorCode::InvalidJson,
                contract: "project config".into(),
                reason: error.to_string(),
            })?;
        Self::try_from_document(document)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PROJECT_CONFIG_VERSION {
            return Err(DomainError::unsupported(
                ErrorCode::UnsupportedProjectConfigVersion,
                "project config",
                self.schema_version,
                PROJECT_CONFIG_VERSION,
            ));
        }
        self.resources.validate()?;
        self.attempt_retry.0.validate()?;
        self.action_retry.0.validate()?;
        self.delivery_retry.0.validate()
    }

    fn try_from_document(document: ProjectConfigDocument) -> Result<Self> {
        let config = Self::from_document(document);
        config.validate()?;
        Ok(config)
    }

    fn from_document(document: ProjectConfigDocument) -> Self {
        Self {
            schema_version: document.schema_version,
            resources: document.resources,
            paths: document.paths,
            report: document.report,
            attempt_retry: document.attempt_retry,
            action_retry: document.action_retry,
            delivery_retry: document.delivery_retry,
        }
    }
}

impl<'de> Deserialize<'de> for ProjectConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let document = ProjectConfigDocument::deserialize(deserializer)?;
        Self::try_from_document(document).map_err(de::Error::custom)
    }
}
