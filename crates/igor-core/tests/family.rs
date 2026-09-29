use std::{error::Error, fs, path::Path, process::Command};

use igor_core::{
    FamilyFile, GenerationStatus, JobState, Project, ProjectConfig, ProjectId, SourceIdentity,
    StoredJob, SubmissionError, build_family_generation, initialize_project, load_family_file,
    load_project_config,
};
use tempfile::TempDir;

const FAMILY: &str = r#"
schema_version = 1
name = "gnn-frozen"
scientific_configurations = ["configs/shared.toml"]

[execution]
program = "/usr/bin/python3"
args = ["train.py", "--frozen"]

[[members]]
seed = 1
args = ["--seed", "1"]
scientific_configurations = ["configs/seed1.toml"]

[[members]]
seed = 2
args = ["--seed", "2"]
scientific_configurations = ["configs/seed2.toml"]
"#;

#[test]
fn versioned_family_file_expands_required_seeds_as_direct_commands() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let path = temporary.path().join("family.toml");
    fs::write(&path, FAMILY)?;
    let file = load_family_file(&path)?;
    let inputs = file.into_inputs(temporary.path())?;
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].0.0, 1);
    assert_eq!(inputs[1].0.0, 2);
    assert_eq!(
        inputs[0].1.command.args,
        ["train.py", "--frozen", "--seed", "1"]
    );
    assert_eq!(
        inputs[1].1.command.args,
        ["train.py", "--frozen", "--seed", "2"]
    );
    assert_eq!(inputs[0].1.command.program, "/usr/bin/python3");
    assert_eq!(inputs[0].1.scientific_configurations.len(), 2);
    assert_eq!(
        inputs[1].1.scientific_configurations[1].to_string_lossy(),
        "configs/seed2.toml"
    );
    assert_ne!(inputs[0].1.name, inputs[1].1.name);
    Ok(())
}

#[test]
fn family_rejects_ambiguous_or_non_comparable_members() -> Result<(), Box<dyn Error>> {
    let file: FamilyFile = toml::from_str(FAMILY)?;
    for (changed, reason) in [
        (
            FAMILY.replace("schema_version = 1", "schema_version = 2"),
            "unsupported version",
        ),
        (FAMILY.replace("seed = 2", "seed = 1"), "duplicate seed"),
        (
            FAMILY.replace(
                "program = \"/usr/bin/python3\"",
                "shell_command = \"python train.py\"",
            ),
            "direct program",
        ),
        (
            FAMILY.replace(
                "args = [\"--seed\", \"2\"]\nscientific_configurations = [\"configs/seed2.toml\"]",
                "",
            ),
            "needs arguments",
        ),
    ] {
        let parsed: FamilyFile = toml::from_str(&changed)?;
        let error = match parsed.validate() {
            Err(error) => error,
            Ok(()) => return Err(format!("accepted invalid family: {reason}").into()),
        };
        assert!(matches!(error, SubmissionError::FamilyFile { .. }));
        assert!(error.to_string().contains(reason));
    }
    let mut missing = file;
    missing.members.clear();
    assert!(missing.validate().is_err());
    assert!(
        toml::from_str::<FamilyFile>(&FAMILY.replace("seed = 2", "seed = 2\nunknown = 3")).is_err()
    );
    Ok(())
}

fn git(root: &Path, args: &[&str]) -> Result<(), Box<dyn Error>> {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .status()?;
    if !status.success() {
        return Err(format!("git {args:?} exited with {status}").into());
    }
    Ok(())
}

fn repository() -> Result<(TempDir, Project, ProjectConfig, FamilyFile), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("project");
    initialize_project(&root, false)?;
    fs::create_dir(root.join("configs"))?;
    for name in ["shared", "seed1", "seed2"] {
        fs::write(
            root.join(format!("configs/{name}.toml")),
            format!("name = '{name}'\n"),
        )?;
    }
    fs::write(root.join("family.toml"), FAMILY)?;
    git(&root, &["init", "-q"])?;
    git(&root, &["config", "user.email", "igor@example.invalid"])?;
    git(&root, &["config", "user.name", "Igor Test"])?;
    git(&root, &["add", "."])?;
    git(&root, &["commit", "-qm", "baseline"])?;
    let loaded = load_project_config(&root.join(".igor/project.toml"))?;
    let family = load_family_file(&root.join("family.toml"))?;
    let project = Project {
        id: ProjectId::new(),
        name: "family-project".into(),
        root,
        config_path: loaded.config_file,
    };
    Ok((temporary, project, loaded.config, family))
}

fn five_seed_family(mut file: FamilyFile) -> FamilyFile {
    let template = file.members[0].clone();
    file.members = (1..=5)
        .map(|seed| {
            let mut member = template.clone();
            member.seed.0 = seed as u64;
            member.args = vec!["--seed".into(), seed.to_string()];
            member
        })
        .collect();
    file
}

#[test]
fn generation_freezes_one_revision_and_protocol_for_all_seeds() -> Result<(), Box<dyn Error>> {
    let (_temporary, project, config, file) = repository()?;
    let prepared = build_family_generation(&project, &config, file)?;
    prepared.validate()?;
    assert_eq!(prepared.submissions.len(), 2);
    assert!(
        prepared
            .generation
            .identity
            .protocol_digest
            .starts_with("sha256:")
    );
    for submission in &prepared.submissions {
        let membership = submission.job.family.as_ref().ok_or("missing membership")?;
        assert_eq!(membership.family_id, prepared.family.id);
        assert_eq!(membership.generation, prepared.generation.identity);
        assert_eq!(submission.attempt.family(), Some(membership));
        assert!(
            matches!(submission.attempt.source(), SourceIdentity::Git(git)
            if git.revision == prepared.generation.identity.source_revision && !git.dirty)
        );
    }
    assert_ne!(
        prepared.submissions[0].attempt.configuration().job_digest,
        prepared.submissions[1].attempt.configuration().job_digest
    );
    Ok(())
}

#[test]
fn five_seed_generation_freezes_identity_revision_and_distinct_seeds() -> Result<(), Box<dyn Error>>
{
    let (_temporary, project, config, file) = repository()?;
    let prepared = build_family_generation(&project, &config, five_seed_family(file))?;
    prepared.validate()?;
    assert_eq!(prepared.submissions.len(), 5);
    for (index, submission) in prepared.submissions.iter().enumerate() {
        let expected_seed = (index + 1) as u64;
        let membership = submission.job.family.as_ref().ok_or("missing membership")?;
        assert_eq!(membership.seed.0, expected_seed);
        assert_eq!(membership.family_id, prepared.family.id);
        assert_eq!(membership.generation, prepared.generation.identity);
        assert_eq!(submission.attempt.family(), Some(membership));
        assert!(
            matches!(submission.attempt.source(), SourceIdentity::Git(git)
            if git.revision == prepared.generation.identity.source_revision && !git.dirty)
        );
    }
    Ok(())
}

#[test]
fn generation_rejects_mixed_revisions_protocols_and_member_contents() -> Result<(), Box<dyn Error>>
{
    let (_temporary, project, config, file) = repository()?;
    let original = build_family_generation(&project, &config, file.clone())?;
    let mut mixed = original.clone();
    mixed.submissions[1]
        .job
        .family
        .as_mut()
        .ok_or("missing membership")?
        .generation
        .source_revision = "other-revision".into();
    assert!(mixed.validate().is_err());

    let mut mixed = original.clone();
    mixed.submissions[1]
        .job
        .family
        .as_mut()
        .ok_or("missing membership")?
        .generation
        .protocol_digest = "sha256:other-protocol".into();
    assert!(mixed.validate().is_err());

    let mut mixed = original.clone();
    mixed.generation.identity.protocol_digest = "sha256:other-protocol".into();
    assert!(mixed.validate().is_err());

    let mut mixed = original;
    mixed.submissions[1]
        .job
        .command
        .args
        .push("different".into());
    assert!(mixed.validate().is_err());

    let baseline = build_family_generation(&project, &config, file.clone())?;
    let mut scheduling_only = file.clone();
    scheduling_only.name = "another-family-name".into();
    scheduling_only.priority = 100;
    let rescheduled = build_family_generation(&project, &config, scheduling_only)?;
    assert_eq!(
        baseline.generation.identity.protocol_digest,
        rescheduled.generation.identity.protocol_digest
    );

    let mut dirty = file.clone();
    dirty.allow_dirty = true;
    let before = build_family_generation(&project, &config, dirty.clone())?;
    fs::write(
        project.root.join("configs/seed2.toml"),
        "name = 'changed'\n",
    )?;
    assert!(build_family_generation(&project, &config, dirty.clone()).is_ok());
    assert!(build_family_generation(&project, &config, file).is_err());
    let after = build_family_generation(&project, &config, dirty)?;
    assert_ne!(
        before.generation.identity.protocol_digest,
        after.generation.identity.protocol_digest
    );
    Ok(())
}

fn stored_jobs(prepared: &igor_core::PreparedFamily, states: &[JobState]) -> Vec<StoredJob> {
    prepared
        .submissions
        .iter()
        .zip(states)
        .enumerate()
        .map(|(order, (submission, state))| StoredJob {
            spec: submission.job.clone(),
            state: *state,
            priority: submission.priority,
            submission_order: order as i64,
            submitted_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        })
        .collect()
}

#[test]
fn generation_assessment_tracks_required_seed_outcomes_and_current_job_states()
-> Result<(), Box<dyn Error>> {
    let (_temporary, project, config, file) = repository()?;
    let prepared = build_family_generation(&project, &config, file)?;
    let assess = |states: &[JobState]| -> Result<GenerationStatus, Box<dyn Error>> {
        Ok(prepared
            .generation
            .assess(&stored_jobs(&prepared, states))?)
    };

    assert_eq!(
        assess(&[JobState::Succeeded; 2])?,
        GenerationStatus::Comparable
    );
    assert_eq!(
        assess(&[JobState::Queued, JobState::Succeeded])?,
        GenerationStatus::Incomplete
    );
    assert_eq!(
        assess(&[JobState::Succeeded, JobState::Running])?,
        GenerationStatus::Incomplete
    );
    for terminal in [
        JobState::Failed,
        JobState::Cancelled,
        JobState::Lost,
        JobState::Superseded,
    ] {
        assert_eq!(
            assess(&[terminal, JobState::Succeeded])?,
            GenerationStatus::Complete
        );
    }
    assert_eq!(
        prepared
            .generation
            .assess(&stored_jobs(&prepared, &[JobState::Succeeded]))?,
        GenerationStatus::Incomplete
    );

    let mut retried = stored_jobs(&prepared, &[JobState::Failed, JobState::Succeeded]);
    assert_eq!(
        prepared.generation.assess(&retried)?,
        GenerationStatus::Complete
    );
    retried[0].state = JobState::Queued;
    assert_eq!(
        prepared.generation.assess(&retried)?,
        GenerationStatus::Incomplete
    );
    retried[0].state = JobState::Succeeded;
    assert_eq!(
        prepared.generation.assess(&retried)?,
        GenerationStatus::Comparable
    );
    Ok(())
}

#[test]
fn generation_assessment_rejects_duplicate_extra_and_mismatched_jobs() -> Result<(), Box<dyn Error>>
{
    let (_temporary, project, config, file) = repository()?;
    let prepared = build_family_generation(&project, &config, file)?;
    let successful = stored_jobs(&prepared, &[JobState::Succeeded; 2]);

    let mut altered_contract = prepared.generation.clone();
    altered_contract.spec["family"]["members"]
        .as_array_mut()
        .ok_or("missing frozen seed list")?
        .pop();
    assert!(altered_contract.assess(&successful).is_err());

    let mut duplicate = successful.clone();
    duplicate[1]
        .spec
        .family
        .as_mut()
        .ok_or("missing family membership")?
        .seed = duplicate[0]
        .spec
        .family
        .as_ref()
        .ok_or("missing family membership")?
        .seed;
    assert_eq!(
        prepared.generation.assess(&duplicate)?,
        GenerationStatus::Invalid
    );

    let mut extra = successful.clone();
    extra[1]
        .spec
        .family
        .as_mut()
        .ok_or("missing family membership")?
        .seed
        .0 = 99;
    assert_eq!(
        prepared.generation.assess(&extra)?,
        GenerationStatus::Invalid
    );

    for mutate in 0..4 {
        let mut mismatched = successful.clone();
        let membership = mismatched[1]
            .spec
            .family
            .as_mut()
            .ok_or("missing family membership")?;
        match mutate {
            0 => membership.family_id = igor_core::FamilyId::new(),
            1 => membership.generation.source_revision.push_str("-other"),
            2 => membership.generation.protocol_digest.push_str("-other"),
            _ => membership.generation.id = igor_core::GenerationId::new(),
        }
        assert_eq!(
            prepared.generation.assess(&mismatched)?,
            GenerationStatus::Invalid
        );
    }
    Ok(())
}

#[test]
fn five_seed_generation_assessment_covers_completeness_and_invalid_members()
-> Result<(), Box<dyn Error>> {
    let (_temporary, project, config, file) = repository()?;
    let prepared = build_family_generation(&project, &config, five_seed_family(file))?;
    let assess = |states: &[JobState]| -> Result<GenerationStatus, Box<dyn Error>> {
        Ok(prepared
            .generation
            .assess(&stored_jobs(&prepared, states))?)
    };

    assert_eq!(
        assess(&[JobState::Succeeded; 5])?,
        GenerationStatus::Comparable
    );
    assert_eq!(
        assess(&[JobState::Succeeded; 4])?,
        GenerationStatus::Incomplete
    );
    assert_eq!(
        assess(&[
            JobState::Succeeded,
            JobState::Succeeded,
            JobState::Failed,
            JobState::Succeeded,
            JobState::Succeeded,
        ])?,
        GenerationStatus::Complete
    );

    let successful = stored_jobs(&prepared, &[JobState::Succeeded; 5]);
    let mut duplicate = successful.clone();
    duplicate[4]
        .spec
        .family
        .as_mut()
        .ok_or("missing family")?
        .seed = duplicate[0]
        .spec
        .family
        .as_ref()
        .ok_or("missing family")?
        .seed;
    assert_eq!(
        prepared.generation.assess(&duplicate)?,
        GenerationStatus::Invalid
    );

    let mut extra = successful.clone();
    extra[4]
        .spec
        .family
        .as_mut()
        .ok_or("missing family")?
        .seed
        .0 = 6;
    assert_eq!(
        prepared.generation.assess(&extra)?,
        GenerationStatus::Invalid
    );

    for change_generation_id in [false, true] {
        let mut mixed = successful.clone();
        let identity = &mut mixed[4]
            .spec
            .family
            .as_mut()
            .ok_or("missing family")?
            .generation;
        if change_generation_id {
            identity.id = igor_core::GenerationId::new();
        } else {
            identity.source_revision.push_str("-other");
        }
        assert_eq!(
            prepared.generation.assess(&mixed)?,
            GenerationStatus::Invalid
        );
    }
    Ok(())
}
