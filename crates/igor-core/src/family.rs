use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AttemptSpec, EnvironmentPolicy, ExecutorSpec, Family, FamilyId, FamilyMembership, Generation,
    GenerationId, GenerationIdentity, GitIdentity, JobExecution, JobFile, Project, ProjectConfig,
    ResourceRequest, Seed, SourceIdentity, Submission, SubmissionError, SubmissionInput,
    submission::{build_submission_with_source, capture_git, changed_worktree, digest_json},
};

pub const FAMILY_FILE_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyFile {
    pub schema_version: u32,
    pub name: String,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub allow_dirty: bool,
    pub execution: JobExecution,
    #[serde(default)]
    pub environment: EnvironmentPolicy,
    #[serde(default)]
    pub executor: Option<ExecutorSpec>,
    #[serde(default)]
    pub resources: Option<ResourceRequest>,
    #[serde(default)]
    pub scientific_configurations: Vec<PathBuf>,
    #[serde(default)]
    pub immutable_inputs: Vec<PathBuf>,
    pub members: Vec<FamilyMember>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyMember {
    pub seed: Seed,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub scientific_configurations: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct PreparedFamily {
    pub family: Family,
    pub generation: Generation,
    pub submissions: Vec<Submission>,
    pub file: FamilyFile,
}

fn invalid_family(reason: impl Into<String>) -> SubmissionError {
    SubmissionError::FamilyFile {
        path: PathBuf::from("family.toml"),
        reason: reason.into(),
    }
}

fn protocol_spec(
    file: &FamilyFile,
    submissions: &[Submission],
    project_digest: &str,
    source: &GitIdentity,
) -> Value {
    let members: Vec<_> = file
        .members
        .iter()
        .zip(submissions)
        .map(|(member, submission)| {
            json!({
                "seed": member.seed.0,
                "contents": submission.attempt.configuration().contents,
            })
        })
        .collect();
    json!({"family": file, "project_digest": project_digest, "members": members,
        "dirty_digest": source.dirty_digest})
}

fn protocol_identity(spec: &Value, file: &FamilyFile) -> Value {
    json!({
        "schema_version": file.schema_version,
        "execution": file.execution,
        "environment": file.environment,
        "executor": file.executor,
        "resources": file.resources,
        "scientific_configurations": file.scientific_configurations,
        "immutable_inputs": file.immutable_inputs,
        "member_contracts": file.members,
        "member_contents": spec["members"],
        "project_digest": spec["project_digest"],
        "dirty_digest": spec["dirty_digest"],
    })
}

pub(crate) fn generation_protocol_digest(
    spec: &Value,
    file: &FamilyFile,
) -> Result<String, SubmissionError> {
    digest_json(&protocol_identity(spec, file), "family protocol")
}

impl PreparedFamily {
    pub fn for_existing_family(
        mut self,
        family: Family,
        number: u32,
    ) -> Result<Self, SubmissionError> {
        self.validate()?;
        if self.generation.identity.number != 1
            || family.project_id != self.family.project_id
            || family.name != self.family.name
            || number <= 1
        {
            return Err(invalid_family(
                "successor must retain the family name and project and advance its generation",
            ));
        }
        self.family = family;
        self.generation.family_id = self.family.id;
        self.generation.identity.number = number;
        for (member, submission) in self.file.members.iter().zip(&mut self.submissions) {
            submission.job.family = Some(FamilyMembership {
                family_id: self.family.id,
                generation: self.generation.identity.clone(),
                seed: member.seed,
            });
            let previous = &submission.attempt;
            let mut configuration = previous.configuration().clone();
            configuration.job_digest = digest_json(&submission.job, "family member job")?;
            submission.attempt = AttemptSpec::from_job(
                previous.id(),
                previous.sequence(),
                &submission.job,
                previous.source().clone(),
                configuration,
                previous.result().clone(),
            )?;
        }
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), SubmissionError> {
        self.file.validate()?;
        if self.family.name != self.file.name
            || self.generation.family_id != self.family.id
            || self.submissions.len() != self.file.members.len()
            || self.generation.identity.number == 0
        {
            return Err(invalid_family(
                "generation does not contain exactly its required members",
            ));
        }
        let identity = &self.generation.identity;
        let first = self
            .submissions
            .first()
            .ok_or_else(|| invalid_family("empty generation"))?;
        let project_digest = &first.attempt.configuration().project_digest;
        let source = first.attempt.source();
        let SourceIdentity::Git(git) = source else {
            return Err(invalid_family("generation source is not Git"));
        };
        let spec = protocol_spec(&self.file, &self.submissions, project_digest, git);
        if self.generation.spec != spec
            || identity.protocol_digest != generation_protocol_digest(&spec, &self.file)?
        {
            return Err(invalid_family(
                "generation protocol identity does not match its members",
            ));
        }
        if git.revision != identity.source_revision {
            return Err(invalid_family(
                "generation source revision does not match Git",
            ));
        }
        for (member, submission) in self.file.members.iter().zip(&self.submissions) {
            let membership = FamilyMembership {
                family_id: self.family.id,
                generation: identity.clone(),
                seed: member.seed,
            };
            let job = &submission.job;
            let attempt = &submission.attempt;
            if job.project_id != self.family.project_id
                || job.family.as_ref() != Some(&membership)
                || attempt.family() != Some(&membership)
                || attempt.source() != source
                || attempt.job_id() != job.id
                || attempt.command() != &job.command
                || attempt.configuration().project_digest != *project_digest
                || attempt.configuration().job_digest != digest_json(job, "family member job")?
            {
                return Err(invalid_family(format!(
                    "seed {} has mixed generation, revision, or protocol invariants",
                    member.seed.0
                )));
            }
        }
        Ok(())
    }
}

pub fn build_family_generation(
    project: &Project,
    config: &ProjectConfig,
    file: FamilyFile,
) -> Result<PreparedFamily, SubmissionError> {
    file.validate()?;
    let before = capture_git(&project.root, file.allow_dirty)?;
    let inputs = file.clone().into_inputs(&project.root)?;
    let mut submissions = Vec::with_capacity(inputs.len());
    for (_, input) in inputs {
        submissions.push(build_submission_with_source(
            project, config, input, &before,
        )?);
    }
    let after = capture_git(&project.root, file.allow_dirty)?;
    if before != after {
        return Err(changed_worktree(&project.root));
    }
    let project_digest = submissions
        .first()
        .ok_or_else(|| invalid_family("empty generation"))?
        .attempt
        .configuration()
        .project_digest
        .clone();
    let spec = protocol_spec(&file, &submissions, &project_digest, &before);
    let family = Family {
        id: FamilyId::new(),
        project_id: project.id,
        name: file.name.clone(),
    };
    let generation = Generation {
        family_id: family.id,
        identity: GenerationIdentity {
            id: GenerationId::new(),
            number: 1,
            source_revision: before.revision.clone(),
            protocol_digest: generation_protocol_digest(&spec, &file)?,
        },
        spec,
    };
    for (member, submission) in file.members.iter().zip(&mut submissions) {
        submission.job.family = Some(FamilyMembership {
            family_id: family.id,
            generation: generation.identity.clone(),
            seed: member.seed,
        });
        let frozen = submission.attempt.configuration();
        let configuration = crate::ConfigurationIdentity {
            project_digest: frozen.project_digest.clone(),
            job_digest: digest_json(&submission.job, "family member job")?,
            contents: frozen.contents.clone(),
        };
        submission.attempt = AttemptSpec::from_job(
            submission.attempt.id(),
            1,
            &submission.job,
            SourceIdentity::Git(before.clone()),
            configuration,
            submission.attempt.result().clone(),
        )?;
    }
    let prepared = PreparedFamily {
        family,
        generation,
        submissions,
        file,
    };
    prepared.validate()?;
    Ok(prepared)
}

impl FamilyFile {
    pub fn validate(&self) -> Result<(), SubmissionError> {
        let invalid = |reason: String| SubmissionError::FamilyFile {
            path: PathBuf::from("family.toml"),
            reason,
        };
        if self.schema_version != FAMILY_FILE_VERSION {
            return Err(invalid(format!(
                "unsupported version {}; supported version is {FAMILY_FILE_VERSION}",
                self.schema_version
            )));
        }
        if self.name.trim().is_empty() {
            return Err(invalid("name must not be empty".into()));
        }
        if self.execution.program.as_deref().is_none_or(str::is_empty)
            || self.execution.shell_command.is_some()
        {
            return Err(invalid(
                "execution must use a direct program without a shell command".into(),
            ));
        }
        if self.members.is_empty() {
            return Err(invalid(
                "members must contain at least one required seed".into(),
            ));
        }
        let mut seeds = BTreeSet::new();
        for member in &self.members {
            if !seeds.insert(member.seed.0) {
                return Err(invalid(format!("duplicate seed {}", member.seed.0)));
            }
            if member.args.is_empty() && member.scientific_configurations.is_empty() {
                return Err(invalid(format!(
                    "seed {} needs arguments or scientific configurations",
                    member.seed.0
                )));
            }
        }
        Ok(())
    }

    pub fn into_inputs(
        self,
        project_root: &Path,
    ) -> Result<Vec<(Seed, SubmissionInput)>, SubmissionError> {
        self.validate()?;
        self.members
            .into_iter()
            .map(|member| {
                let mut execution = self.execution.clone();
                execution.args.extend(member.args);
                let input = JobFile {
                    schema_version: crate::JOB_FILE_VERSION,
                    name: Some(format!("{} / seed {}", self.name, member.seed.0)),
                    priority: self.priority,
                    allow_dirty: self.allow_dirty,
                    execution,
                    environment: self.environment.clone(),
                    executor: self.executor.clone(),
                    resources: self.resources.clone(),
                    scientific_configurations: self
                        .scientific_configurations
                        .iter()
                        .cloned()
                        .chain(member.scientific_configurations)
                        .collect(),
                    immutable_inputs: self.immutable_inputs.clone(),
                }
                .into_input(project_root)?;
                Ok((member.seed, input))
            })
            .collect()
    }
}

pub fn load_family_file(path: &Path) -> Result<FamilyFile, SubmissionError> {
    let contents = fs::read_to_string(path).map_err(|source| SubmissionError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let family: FamilyFile =
        toml::from_str(&contents).map_err(|error| SubmissionError::FamilyFile {
            path: path.to_path_buf(),
            reason: error.to_string(),
        })?;
    family.validate().map_err(|error| match error {
        SubmissionError::FamilyFile { reason, .. } => SubmissionError::FamilyFile {
            path: path.to_path_buf(),
            reason,
        },
        other => other,
    })?;
    Ok(family)
}
