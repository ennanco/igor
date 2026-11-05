use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read},
    os::unix::ffi::OsStrExt,
    path::Path,
};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::report::context_ready;
use crate::report_publication::{
    LockedStagedReport, resolve_report_output, stage_authorized_report,
};
use crate::{
    Project, PublishedReport, ReportConfig, ReportContext, ReportGenerator, ReportMode,
    ReportPublicationError, ReportRenderError, StagedReport, render_report_markdown,
};

const MAX_CLAIM_LINE_BYTES: u64 = 384;

#[derive(Debug, Error)]
pub enum GeneratedReportError {
    #[error("report generation is disabled")]
    Disabled,
    #[error(
        "report generator {generator:?} is not supported by the deterministic generated-file mode"
    )]
    UnsupportedGenerator { generator: ReportGenerator },
    #[error("report context does not belong to the configured project")]
    ProjectMismatch,
    #[error("report context is provisional, not ready for generated-file publication")]
    NotReady,
    #[error(transparent)]
    Render(#[from] ReportRenderError),
    #[error(transparent)]
    Publication(#[from] ReportPublicationError),
}

/// A ready report whose previous destination was authorized as an unchanged generated file.
#[derive(Debug)]
pub struct StagedGeneratedReport {
    staged: StagedReport,
}

#[derive(Debug)]
pub(crate) struct LockedGeneratedReport {
    locked: LockedStagedReport,
}

impl LockedGeneratedReport {
    #[cfg(test)]
    pub(crate) fn inject_fault(&mut self, fault: crate::report_publication::PublicationFault) {
        self.locked.inject_fault(fault);
    }

    pub(crate) fn previous_hash(&self) -> Option<&str> {
        self.locked.previous_hash()
    }
    pub(crate) fn confirm_existing_guarded(
        &mut self,
        guard: impl FnOnce() -> Result<(), ReportPublicationError>,
    ) -> Result<PublishedReport, ReportPublicationError> {
        self.locked.confirm_existing_guarded(guard)
    }
    pub(crate) fn report(&self) -> &PublishedReport {
        self.locked.report()
    }
    pub(crate) fn publish_guarded(
        &mut self,
        guard: impl FnOnce() -> Result<(), ReportPublicationError>,
    ) -> Result<PublishedReport, ReportPublicationError> {
        self.locked.publish_guarded(guard)
    }
}

impl StagedGeneratedReport {
    pub(crate) fn lock_verified(self) -> Result<LockedGeneratedReport, GeneratedReportError> {
        Ok(LockedGeneratedReport {
            locked: self.staged.try_lock_verified()?,
        })
    }
    #[must_use]
    pub fn staging_path(&self) -> &Path {
        self.staged.staging_path()
    }

    #[must_use]
    pub fn report(&self) -> &PublishedReport {
        self.staged.report()
    }

    /// Revalidates the authorized file snapshot. Callers separately revalidate live context/actions.
    pub fn publish(self) -> Result<PublishedReport, GeneratedReportError> {
        Ok(self.staged.publish()?)
    }
}

/// Prepares the opt-in, deterministic `replace-generated-file` mode without adopting handwritten files.
pub fn stage_generated_report(
    project: &Project,
    config: &ReportConfig,
    context: &ReportContext,
) -> Result<StagedGeneratedReport, GeneratedReportError> {
    if !config.enabled {
        return Err(GeneratedReportError::Disabled);
    }
    match config.mode {
        ReportMode::ReplaceGeneratedFile => {}
    }
    if config.generator != ReportGenerator::Deterministic {
        return Err(GeneratedReportError::UnsupportedGenerator {
            generator: config.generator,
        });
    }
    if context.family.project_id != project.id {
        return Err(GeneratedReportError::ProjectMismatch);
    }
    if !context_ready(context) {
        return Err(GeneratedReportError::NotReady);
    }
    let body = render_report_markdown(context)?;
    let output = resolve_report_output(&project.root, &config.output)?;
    let relative = output
        .strip_prefix(&project.root)
        .map_err(|_| GeneratedReportError::ProjectMismatch)?;
    let output_hash = sha256(relative.as_os_str().as_bytes());
    let prefix = format!(
        "<!-- igor:generated-file:v1 project={} output={} body=",
        project.id, output_hash
    );
    let markdown = format!("{prefix}{} -->\n{body}", sha256(body.as_bytes()));
    let staged = stage_authorized_report(&project.root, &output, &markdown, |path, file| {
        authorize_file(path, file, &prefix)
    })?;
    Ok(StagedGeneratedReport { staged })
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn not_generated(path: &Path, reason: &'static str) -> ReportPublicationError {
    ReportPublicationError::NotGenerated {
        path: path.to_path_buf(),
        reason,
    }
}

fn ownership_io(path: &Path, source: io::Error) -> ReportPublicationError {
    ReportPublicationError::Io {
        operation: "validate generated-file ownership",
        path: path.to_path_buf(),
        source,
    }
}

fn authorize_file(
    path: &Path,
    file: &mut File,
    prefix: &str,
) -> Result<(), ReportPublicationError> {
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    Read::take(&mut reader, MAX_CLAIM_LINE_BYTES)
        .read_until(b'\n', &mut line)
        .map_err(|source| ownership_io(path, source))?;
    let expected = std::str::from_utf8(&line)
        .ok()
        .and_then(|line| line.strip_prefix(prefix))
        .and_then(|checksum| checksum.strip_suffix(" -->\n"))
        .filter(|checksum| {
            checksum.len() == 71
                && checksum.starts_with("sha256:")
                && checksum.as_bytes()[7..]
                    .iter()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        })
        .ok_or_else(|| not_generated(path, "no matching v1 project/output claim"))?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|source| ownership_io(path, source))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if format!("sha256:{:x}", digest.finalize()) != expected {
        return Err(not_generated(path, "generated report has been edited"));
    }
    Ok(())
}
