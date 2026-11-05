use std::{error::Error, fs, os::unix::fs::symlink, path::PathBuf};

use igor_core::{
    FamilyId, GeneratedReportError, GenerationId, GenerationStatus, Project, ProjectId,
    ReportConfig, ReportContext, ReportGenerator, ReportPublicationError, ReportRenderError,
    render_report_markdown, stage_generated_report,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

fn fixture() -> Result<(TempDir, Project, ReportConfig, ReportContext), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let project = Project {
        id: ProjectId::new(),
        name: "report-project".into(),
        root: directory.path().to_path_buf(),
        config_path: directory.path().join(".igor/project.toml"),
    };
    let job_id = igor_core::JobId::new();
    let attempt_id = igor_core::AttemptId::new();
    let context = serde_json::from_value(json!({
        "schema_version":1,
        "family":{"id":FamilyId::new(), "project_id":project.id, "name":"a completed family"},
        "generation":{"identity":{"id":GenerationId::new(), "number":1, "source_revision":"revision", "protocol_digest":"sha256:protocol"},
            "status":"comparable", "counts":{"succeeded":1}},
        "required_seeds":[1],
        "members":[{"job_id":job_id, "name":"seed one", "seed":1, "state":"succeeded", "attempts":[{
            "attempt_id":attempt_id, "sequence":1, "state":"succeeded",
            "source":{"kind":"git", "identity":{"repository":"local", "root":project.root, "revision":"revision", "dirty":false}},
            "configuration":{"project_digest":"sha256:project", "job_digest":"sha256:job", "contents":[]},
            "metric_schema_version":1, "extractor":null,
            "metrics":{"attempt_id":attempt_id, "action_state":null, "last_error":null,
                "document":null, "selection":null, "metrics":[]}, "artifacts":[]
        }]}],
        "history":[], "repairs":[],
        "tools":{"igor_context_builder":"0.1.0", "extractor":null, "prompt":null, "provider":null, "model":null},
        "ready":true, "primary_metrics":[]
    }))?;
    let config = ReportConfig {
        enabled: true,
        ..Default::default()
    };
    Ok((directory, project, config, context))
}

fn not_owned(error: GeneratedReportError) -> bool {
    matches!(
        error,
        GeneratedReportError::Publication(ReportPublicationError::NotGenerated { .. })
    )
}

#[test]
fn omitted_report_configuration_is_disabled_without_filesystem_effects() -> TestResult {
    let (_directory, mut project, _, context) = fixture()?;
    let root = project.root.clone();
    project.root = root.join("does-not-exist");
    assert!(matches!(
        stage_generated_report(&project, &ReportConfig::default(), &context),
        Err(GeneratedReportError::Disabled)
    ));
    assert_eq!(fs::read_dir(root)?.count(), 0);
    Ok(())
}

#[test]
fn enabled_default_mode_atomically_creates_claim_and_exact_renderer_output() -> TestResult {
    let (_directory, project, config, context) = fixture()?;
    let expected = render_report_markdown(&context)?;
    let output = project.root.join("RESULTS.md");
    let cancelled = stage_generated_report(&project, &config, &context)?;
    let cancelled_path = cancelled.staging_path().to_path_buf();
    let cancelled_bytes = fs::read(&cancelled_path)?;
    drop(cancelled);
    assert!(!cancelled_path.exists());
    assert!(!output.exists());
    let staged = stage_generated_report(&project, &config, &context)?;
    let markdown = fs::read_to_string(staged.staging_path())?;
    assert_eq!(markdown.as_bytes(), cancelled_bytes.as_slice());
    let (claim, body) = markdown.split_once('\n').ok_or("generated-file claim")?;
    assert!(claim.starts_with(&format!(
        "<!-- igor:generated-file:v1 project={} output=sha256:",
        project.id
    )));
    assert!(claim.ends_with(&format!(
        "body=sha256:{:x} -->",
        Sha256::digest(expected.as_bytes())
    )));
    assert_eq!(body, expected);
    assert!(!output.exists());
    let published = staged.publish()?;
    assert_eq!(published.path, output);
    assert_eq!(fs::read(&output)?, markdown.as_bytes());
    assert_eq!(published.size_bytes, markdown.len() as u64);
    assert_eq!(
        published.sha256,
        format!("sha256:{:x}", Sha256::digest(markdown.as_bytes()))
    );
    assert_eq!(fs::read_dir(&project.root)?.count(), 1); // No sidecar or ownership ledger file.
    Ok(())
}

#[test]
fn unchanged_owned_report_can_be_replaced_with_relative_or_resolved_output() -> TestResult {
    let (_directory, project, mut config, mut context) = fixture()?;
    let first = stage_generated_report(&project, &config, &context)?.publish()?;
    config.output = project.root.join("RESULTS.md");
    context.tools.igor_context_builder = "new-builder-version".into();
    let second = stage_generated_report(&project, &config, &context)?.publish()?;
    assert_eq!(first.path, second.path);
    assert_ne!(first.sha256, second.sha256);
    let contents = fs::read_to_string(&second.path)?;
    assert_eq!(
        contents.split_once('\n').ok_or("claim")?.1,
        render_report_markdown(&context)?
    );
    assert_eq!(fs::read_dir(&project.root)?.count(), 1);
    Ok(())
}

#[test]
fn handwritten_files_and_unrecognized_claims_are_preserved_without_staging() -> TestResult {
    let (_directory, project, config, context) = fixture()?;
    let output = project.root.join("RESULTS.md");
    let ordinary_renderer_output = render_report_markdown(&context)?.into_bytes();
    let mut long_line = vec![b'x'; 1024 * 1024];
    long_line.extend_from_slice(b"\nhandwritten notes\n");
    for bytes in [
        b"# My handwritten results\nImportant notes.\n".to_vec(),
        vec![],
        vec![0xff, 0xfe, 0x00],
        long_line,
        ordinary_renderer_output,
        b"<!-- igor:generated-file:v2 -->\n# Notes\n".to_vec(),
        b"\xef\xbb\xbf<!-- igor:generated-report:v1 -->\n# Notes\n".to_vec(),
    ] {
        fs::write(&output, &bytes)?;
        assert!(not_owned(
            stage_generated_report(&project, &config, &context)
                .err()
                .ok_or("unmanaged file adopted")?
        ));
        assert_eq!(fs::read(&output)?, bytes);
        assert_eq!(fs::read_dir(&project.root)?.count(), 1);
    }
    Ok(())
}

#[test]
fn manual_edits_or_notes_on_a_generated_report_are_not_overwritten() -> TestResult {
    let (_directory, project, config, context) = fixture()?;
    let published = stage_generated_report(&project, &config, &context)?.publish()?;
    let original = fs::read_to_string(&published.path)?;
    let body_hash = format!(
        "body=sha256:{:x}",
        Sha256::digest(render_report_markdown(&context)?.as_bytes())
    );
    let mut non_utf8_body = original.as_bytes().to_vec();
    non_utf8_body.push(0xff);
    for bytes in [
        format!("{original}\n# My notes\nKeep these.\n").into_bytes(),
        original
            .replace("# Experiment results", "# Manually edited results")
            .into_bytes(),
        original
            .replacen("igor:generated-file:v1", "igor:generated-file:v2", 1)
            .into_bytes(),
        non_utf8_body,
        original
            .replacen(&body_hash, &format!("body=sha256:{}", "a".repeat(63)), 1)
            .into_bytes(),
        original
            .replacen(&body_hash, &format!("body=sha256:{}", "a".repeat(65)), 1)
            .into_bytes(),
        original
            .replacen(&body_hash, &format!("body=sha256:{}", "g".repeat(64)), 1)
            .into_bytes(),
        original.replacen(" -->\n", " -->", 1).into_bytes(),
        original
            .replacen(" -->\n", &format!("{} -->\n", " ".repeat(512)), 1)
            .into_bytes(),
    ] {
        fs::write(&published.path, &bytes)?;
        assert!(not_owned(
            stage_generated_report(&project, &config, &context)
                .err()
                .ok_or("edited generated file overwritten")?
        ));
        assert_eq!(fs::read(&published.path)?, bytes);
        assert_eq!(fs::read_dir(&project.root)?.count(), 1);
    }
    Ok(())
}

#[test]
fn copied_claims_do_not_authorize_another_project_or_output_path() -> TestResult {
    let (_directory, project, mut config, context) = fixture()?;
    let original = stage_generated_report(&project, &config, &context)?.publish()?;
    let bytes = fs::read(&original.path)?;
    config.output = PathBuf::from("custom.md");
    let custom = project.root.join("custom.md");
    fs::write(&custom, &bytes)?;
    assert!(not_owned(
        stage_generated_report(&project, &config, &context)
            .err()
            .ok_or("foreign path adopted")?
    ));
    assert_eq!(fs::read(&custom)?, bytes);
    let (_other_directory, other, other_config, other_context) = fixture()?;
    let other_output = other.root.join("RESULTS.md");
    fs::write(&other_output, &bytes)?;
    assert!(not_owned(
        stage_generated_report(&other, &other_config, &other_context)
            .err()
            .ok_or("foreign project adopted")?
    ));
    assert_eq!(fs::read(&other_output)?, bytes);
    assert_eq!(fs::read(&original.path)?, bytes);
    Ok(())
}

#[test]
fn late_handwritten_edits_and_other_generated_publications_fence_prepared_reports() -> TestResult {
    let (_directory, project, config, mut context) = fixture()?;
    let first = stage_generated_report(&project, &config, &context)?.publish()?;
    let prepared = stage_generated_report(&project, &config, &context)?;
    let mut notes = fs::read(&first.path)?;
    notes.extend_from_slice(b"\nMy late notes\n");
    fs::write(&first.path, &notes)?;
    assert!(matches!(
        prepared.publish(),
        Err(GeneratedReportError::Publication(
            ReportPublicationError::Changed { .. }
        ))
    ));
    assert_eq!(fs::read(&first.path)?, notes);
    // Reset this disposable fixture to an owned file for the competing-publication case.
    fs::remove_file(&first.path)?;
    stage_generated_report(&project, &config, &context)?.publish()?;
    let obsolete = stage_generated_report(&project, &config, &context)?;
    context.tools.igor_context_builder = "new-builder-version".into();
    let current = stage_generated_report(&project, &config, &context)?.publish()?;
    let bytes = fs::read(&current.path)?;
    assert!(matches!(
        obsolete.publish(),
        Err(GeneratedReportError::Publication(
            ReportPublicationError::Changed { .. }
        ))
    ));
    assert_eq!(fs::read(&current.path)?, bytes);
    Ok(())
}

#[test]
fn explicit_generators_and_invalid_contexts_do_not_fall_back_or_write() -> TestResult {
    let (_directory, project, mut config, context) = fixture()?;
    for generator in [ReportGenerator::Opencode, ReportGenerator::Pi] {
        config.generator = generator;
        assert!(matches!(
            stage_generated_report(&project, &config, &context),
            Err(GeneratedReportError::UnsupportedGenerator { .. })
        ));
    }
    config.generator = ReportGenerator::Deterministic;
    let mut wrong_project = context.clone();
    wrong_project.family.project_id = ProjectId::new();
    assert!(matches!(
        stage_generated_report(&project, &config, &wrong_project),
        Err(GeneratedReportError::ProjectMismatch)
    ));
    for status in [
        GenerationStatus::Incomplete,
        GenerationStatus::Complete,
        GenerationStatus::Invalid,
    ] {
        let mut context = context.clone();
        context.generation.status = status;
        assert!(matches!(
            stage_generated_report(&project, &config, &context),
            Err(GeneratedReportError::NotReady)
        ));
    }
    let mut provisional = context.clone();
    provisional.ready = false;
    assert!(matches!(
        stage_generated_report(&project, &config, &provisional),
        Err(GeneratedReportError::NotReady)
    ));
    provisional = context.clone();
    provisional.primary_metrics = None;
    assert!(matches!(
        stage_generated_report(&project, &config, &provisional),
        Err(GeneratedReportError::NotReady)
    ));
    provisional = context;
    provisional.schema_version = 2;
    assert!(matches!(
        stage_generated_report(&project, &config, &provisional),
        Err(GeneratedReportError::Render(
            ReportRenderError::UnsupportedContextVersion { .. }
        ))
    ));
    assert_eq!(fs::read_dir(&project.root)?.count(), 0);
    Ok(())
}

#[test]
fn configured_nested_output_is_supported_without_touching_handwritten_files() -> TestResult {
    let (_directory, project, mut config, context) = fixture()?;
    fs::create_dir(project.root.join("reports"))?;
    fs::write(
        project.root.join("README.md"),
        "handwritten project documentation",
    )?;
    fs::write(project.root.join("RESULTS.md"), "handwritten results")?;
    config.output = PathBuf::from("reports/summary.md");
    let published = stage_generated_report(&project, &config, &context)?.publish()?;
    assert_eq!(published.path, project.root.join("reports/summary.md"));
    assert_eq!(
        fs::read(project.root.join("README.md"))?,
        b"handwritten project documentation"
    );
    assert_eq!(
        fs::read(project.root.join("RESULTS.md"))?,
        b"handwritten results"
    );
    config.output = PathBuf::from("README.md");
    assert!(not_owned(
        stage_generated_report(&project, &config, &context)
            .err()
            .ok_or("README adopted")?
    ));
    config.output = PathBuf::from("../outside.md");
    assert!(matches!(
        stage_generated_report(&project, &config, &context),
        Err(GeneratedReportError::Publication(
            ReportPublicationError::UnsafePath { .. }
        ))
    ));
    Ok(())
}

#[test]
fn a_valid_claim_does_not_bypass_symlink_or_hardlink_protection() -> TestResult {
    for hardlink in [false, true] {
        let (_directory, project, config, context) = fixture()?;
        let published = stage_generated_report(&project, &config, &context)?.publish()?;
        let bytes = fs::read(&published.path)?;
        let archive = project.root.join("archive.md");
        fs::rename(&published.path, &archive)?;
        if hardlink {
            fs::hard_link(&archive, &published.path)?;
        } else {
            symlink(&archive, &published.path)?;
        }
        assert!(stage_generated_report(&project, &config, &context).is_err());
        assert_eq!(fs::read(&archive)?, bytes);
        assert_eq!(fs::read(&published.path)?, bytes);
        assert_eq!(fs::read_dir(&project.root)?.count(), 2);
    }
    Ok(())
}
