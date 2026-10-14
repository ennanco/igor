use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
};

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
    #[default]
    Deterministic,
    Opencode,
    Pi,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectPaths {
    pub artifact_root: PathBuf,
    pub cleanup_roots: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProvenanceConfig {
    pub scientific_configurations: Vec<PathBuf>,
    pub immutable_inputs: Vec<PathBuf>,
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
            enabled: false,
            trigger: ReportTrigger::FamilyCompleted,
            output: PathBuf::from("RESULTS.md"),
            mode: ReportMode::ReplaceGeneratedFile,
            generator: ReportGenerator::Deterministic,
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
    pub provenance: ProvenanceConfig,
    pub attempt_retry: AttemptRetryPolicy,
    pub action_retry: ActionRetryPolicy,
    pub delivery_retry: DeliveryRetryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub results: Option<ResultsConfig>,
    #[serde(default, skip_serializing_if = "RetentionConfig::is_default")]
    pub retention: RetentionConfig,
    #[serde(default, skip_serializing_if = "ArtifactRetentionConfig::is_default")]
    pub artifacts: ArtifactRetentionConfig,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailedRetention {
    #[default]
    DeleteAfterDiagnosis,
    Keep,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupersededRetention {
    #[default]
    DeleteAfterReplacement,
    Keep,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionConfig {
    pub failed_artifacts: FailedRetention,
    pub failed_full_logs: FailedRetention,
    pub failed_log_tail_kb: u32,
    pub compress_success_logs: bool,
    pub delete_repair_worktrees: bool,
    pub superseded_artifacts: SupersededRetention,
}

pub const DEFAULT_FAILED_LOG_TAIL_KB: u32 = 512;
pub const MAX_FAILED_LOG_TAIL_KB: u32 = 16 * 1024;

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            failed_artifacts: FailedRetention::default(),
            failed_full_logs: FailedRetention::default(),
            failed_log_tail_kb: DEFAULT_FAILED_LOG_TAIL_KB,
            compress_success_logs: true,
            delete_repair_worktrees: true,
            superseded_artifacts: SupersededRetention::default(),
        }
    }
}

impl RetentionConfig {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArtifactRetentionConfig {
    pub keep_on_success: Vec<String>,
    pub keep_on_failure: Vec<String>,
}

impl ArtifactRetentionConfig {
    fn is_default(&self) -> bool {
        self.keep_on_success.is_empty() && self.keep_on_failure.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        for (field, patterns) in [
            ("artifacts.keep_on_success", &self.keep_on_success),
            ("artifacts.keep_on_failure", &self.keep_on_failure),
        ] {
            let mut seen = BTreeSet::new();
            for (index, pattern) in patterns.iter().enumerate() {
                if pattern.trim().is_empty()
                    || pattern.contains('\0')
                    || pattern
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == "..")
                    || !Path::new(pattern)
                        .components()
                        .all(|component| matches!(component, Component::Normal(_)))
                {
                    return Err(DomainError::validation(
                        ErrorCode::InvalidProject,
                        &format!("{field}[{index}]"),
                        "must be a non-empty attempt-relative pattern without empty, dot, or parent components",
                    ));
                }
                if !seen.insert(pattern) {
                    return Err(DomainError::validation(
                        ErrorCode::InvalidProject,
                        &format!("{field}[{index}]"),
                        "must not duplicate another pattern",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResultsConfig {
    pub extractor: Vec<String>,
    #[serde(default = "attempt_working_directory")]
    pub working_directory: String,
}

fn attempt_working_directory() -> String {
    "attempt".into()
}

impl ResultsConfig {
    pub fn validate(&self) -> Result<()> {
        if self.extractor.is_empty() || self.extractor[0].trim().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "results.extractor",
                "must contain a non-empty program",
            ));
        }
        if self.extractor.iter().any(|part| part.contains('\0')) {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "results.extractor",
                "must not contain NUL characters",
            ));
        }
        if self.working_directory != "attempt" {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "results.working_directory",
                "only 'attempt' is supported",
            ));
        }
        Ok(())
    }
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
    provenance: ProvenanceConfig,
    #[serde(default)]
    attempt_retry: AttemptRetryPolicy,
    #[serde(default)]
    action_retry: ActionRetryPolicy,
    #[serde(default)]
    delivery_retry: DeliveryRetryPolicy,
    #[serde(default)]
    results: Option<ResultsConfig>,
    #[serde(default)]
    retention: RetentionConfig,
    #[serde(default)]
    artifacts: ArtifactRetentionConfig,
}

impl Default for ProjectConfigDocument {
    fn default() -> Self {
        Self {
            schema_version: PROJECT_CONFIG_VERSION,
            resources: ResourceRequest::default(),
            paths: ProjectPaths::default(),
            report: ReportConfig::default(),
            provenance: ProvenanceConfig::default(),
            attempt_retry: AttemptRetryPolicy::default(),
            action_retry: ActionRetryPolicy::default(),
            delivery_retry: DeliveryRetryPolicy::default(),
            results: None,
            retention: RetentionConfig::default(),
            artifacts: ArtifactRetentionConfig::default(),
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
        self.delivery_retry.0.validate()?;
        if let Some(results) = &self.results {
            results.validate()?;
        }
        self.artifacts.validate()?;
        if self.retention.failed_log_tail_kb > MAX_FAILED_LOG_TAIL_KB {
            return Err(DomainError::validation(
                ErrorCode::InvalidProject,
                "retention.failed_log_tail_kb",
                "must not exceed 16384 KiB",
            ));
        }
        Ok(())
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
            provenance: document.provenance,
            attempt_retry: document.attempt_retry,
            action_retry: document.action_retry,
            delivery_retry: document.delivery_retry,
            results: document.results,
            retention: document.retention,
            artifacts: document.artifacts,
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
