use serde::{Deserialize, Serialize};

use crate::{
    AttemptId, AttemptRetryPolicy, CommandSpec, DomainError, ErrorCode, ExecutorSpec, FamilyId,
    GenerationId, JobId, ProjectId, ResourceRequest, Result,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Seed(pub u64);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationIdentity {
    pub id: GenerationId,
    pub number: u32,
    pub source_revision: String,
    pub protocol_digest: String,
}

impl GenerationIdentity {
    fn validate(&self) -> Result<()> {
        if self.number == 0 {
            return Err(DomainError::validation(
                ErrorCode::InvalidJob,
                "job.family.generation.number",
                "must be positive",
            ));
        }
        if self.source_revision.trim().is_empty() || self.protocol_digest.trim().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidJob,
                "job.family.generation",
                "source revision and protocol digest must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FamilyMembership {
    pub family_id: FamilyId,
    pub generation: GenerationIdentity,
    pub seed: Seed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobSpec {
    pub id: JobId,
    pub project_id: ProjectId,
    pub name: String,
    pub command: CommandSpec,
    #[serde(default)]
    pub executor: ExecutorSpec,
    #[serde(default)]
    pub resources: ResourceRequest,
    #[serde(default)]
    pub retry: AttemptRetryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<FamilyMembership>,
}

impl JobSpec {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidJob,
                "job.name",
                "must not be empty",
            ));
        }
        self.command.validate()?;
        self.resources.validate()?;
        self.retry.0.validate()?;
        if let Some(family) = &self.family {
            family.generation.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
pub enum SourceIdentity {
    GitRevision(String),
    SnapshotDigest(String),
}

impl SourceIdentity {
    fn validate(&self) -> Result<()> {
        let value = match self {
            Self::GitRevision(value) | Self::SnapshotDigest(value) => value,
        };
        if value.trim().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidAttempt,
                "attempt.source",
                "revision or snapshot digest must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConfigurationIdentity {
    pub project_digest: String,
    pub job_digest: String,
}

impl ConfigurationIdentity {
    fn validate(&self) -> Result<()> {
        if self.project_digest.trim().is_empty() || self.job_digest.trim().is_empty() {
            return Err(DomainError::validation(
                ErrorCode::InvalidAttempt,
                "attempt.configuration",
                "project and job digests must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResultContract {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extractor: Option<CommandSpec>,
}

impl ResultContract {
    fn validate(&self) -> Result<()> {
        if self.schema_version == 0 {
            return Err(DomainError::validation(
                ErrorCode::InvalidAttempt,
                "attempt.result.schema_version",
                "must be positive",
            ));
        }
        if let Some(extractor) = &self.extractor {
            extractor.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "AttemptSpecDocument")]
pub struct AttemptSpec {
    id: AttemptId,
    job_id: JobId,
    sequence: u32,
    command: CommandSpec,
    executor: ExecutorSpec,
    resources: ResourceRequest,
    family: Option<FamilyMembership>,
    source: SourceIdentity,
    configuration: ConfigurationIdentity,
    result: ResultContract,
}

impl AttemptSpec {
    pub fn from_job(
        id: AttemptId,
        sequence: u32,
        job: &JobSpec,
        source: SourceIdentity,
        configuration: ConfigurationIdentity,
        result: ResultContract,
    ) -> Result<Self> {
        job.validate()?;
        let attempt = Self {
            id,
            job_id: job.id,
            sequence,
            command: job.command.clone(),
            executor: job.executor.clone(),
            resources: job.resources.clone(),
            family: job.family.clone(),
            source,
            configuration,
            result,
        };
        attempt.validate()?;
        Ok(attempt)
    }

    pub fn validate(&self) -> Result<()> {
        if self.sequence == 0 {
            return Err(DomainError::validation(
                ErrorCode::InvalidAttempt,
                "attempt.sequence",
                "must be positive",
            ));
        }
        self.command.validate()?;
        self.resources.validate()?;
        self.source.validate()?;
        self.configuration.validate()?;
        self.result.validate()
    }

    #[must_use]
    pub const fn id(&self) -> AttemptId {
        self.id
    }

    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.job_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u32 {
        self.sequence
    }

    #[must_use]
    pub const fn command(&self) -> &CommandSpec {
        &self.command
    }

    #[must_use]
    pub const fn executor(&self) -> &ExecutorSpec {
        &self.executor
    }

    #[must_use]
    pub const fn resources(&self) -> &ResourceRequest {
        &self.resources
    }

    #[must_use]
    pub const fn family(&self) -> Option<&FamilyMembership> {
        self.family.as_ref()
    }

    #[must_use]
    pub const fn source(&self) -> &SourceIdentity {
        &self.source
    }

    #[must_use]
    pub const fn configuration(&self) -> &ConfigurationIdentity {
        &self.configuration
    }

    #[must_use]
    pub const fn result(&self) -> &ResultContract {
        &self.result
    }
}

#[derive(Deserialize)]
struct AttemptSpecDocument {
    id: AttemptId,
    job_id: JobId,
    sequence: u32,
    command: CommandSpec,
    executor: ExecutorSpec,
    resources: ResourceRequest,
    family: Option<FamilyMembership>,
    source: SourceIdentity,
    configuration: ConfigurationIdentity,
    result: ResultContract,
}

impl TryFrom<AttemptSpecDocument> for AttemptSpec {
    type Error = DomainError;

    fn try_from(document: AttemptSpecDocument) -> Result<Self> {
        let attempt = Self {
            id: document.id,
            job_id: document.job_id,
            sequence: document.sequence,
            command: document.command,
            executor: document.executor,
            resources: document.resources,
            family: document.family,
            source: document.source,
            configuration: document.configuration,
            result: document.result,
        };
        attempt.validate()?;
        Ok(attempt)
    }
}
