use std::{
    fs::{self, File, Metadata, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
};

use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct PublishedReport {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Error)]
pub enum ReportPublicationError {
    #[error("unsafe report path {path:?}: {reason}")]
    UnsafePath { path: PathBuf, reason: &'static str },
    #[error("report path changed since staging: {path:?}")]
    Changed { path: PathBuf },
    #[error("report output directory is busy: {path:?}")]
    Busy { path: PathBuf },
    #[error("report publication aborted: {reason}")]
    Aborted { reason: &'static str },
    #[error("report destination is not an unchanged Igor-generated file at {path:?}: {reason}")]
    NotGenerated { path: PathBuf, reason: &'static str },
    #[error("report operation {operation} failed for {path:?}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("report was published at {path:?}, but directory sync failed: {source}", path = .report.path)]
    PublishedButUnsynced {
        report: PublishedReport,
        #[source]
        source: io::Error,
    },
}

type Result<T> = std::result::Result<T, ReportPublicationError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    device: u64,
    inode: u64,
}

impl Identity {
    fn of(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct FileStamp {
    identity: Identity,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl FileStamp {
    fn of(metadata: &Metadata) -> Self {
        Self {
            identity: Identity::of(metadata),
            size: metadata.len(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Destination {
    stamp: FileStamp,
    sha256: String,
}

/// Owns one private temporary. Callers authorize output ownership and context separately.
#[derive(Debug)]
pub struct StagedReport {
    report: PublishedReport,
    temporary: PathBuf,
    file: File,
    identity: Identity,
    staged_stamp: FileStamp,
    directories: Vec<(PathBuf, Identity)>,
    parent: File,
    previous: Option<Destination>,
    renamed: bool,
}

#[derive(Debug)]
pub(crate) struct LockedStagedReport {
    staged: StagedReport,
    _lock: Flock<File>,
    #[cfg(test)]
    fault: Option<PublicationFault>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) enum PublicationFault {
    Rename,
    DirectorySync,
    BeforeRename(PathBuf),
    AfterRename(PathBuf),
}

#[cfg(test)]
pub(crate) fn pause_publication(marker: &Path) -> io::Result<()> {
    fs::write(marker, b"ready")?;
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

/// Stages a report beside its destination; the scoped output directory must already exist.
pub fn stage_report(project_root: &Path, output: &Path, markdown: &str) -> Result<StagedReport> {
    stage_with_writer(project_root, output, markdown.as_bytes(), |file, bytes| {
        file.write_all(bytes)
    })
}

pub(crate) fn stage_authorized_report(
    project_root: &Path,
    output: &Path,
    markdown: &str,
    authorize: impl FnOnce(&Path, &mut File) -> Result<()>,
) -> Result<StagedReport> {
    stage_with_policy(
        project_root,
        output,
        markdown.as_bytes(),
        |file, bytes| file.write_all(bytes),
        authorize,
    )
}

impl StagedReport {
    #[must_use]
    pub fn staging_path(&self) -> &Path {
        &self.temporary
    }

    #[must_use]
    pub fn report(&self) -> &PublishedReport {
        &self.report
    }

    /// Revalidates and atomically renames the staged file, then syncs the directory.
    pub fn publish(self) -> Result<PublishedReport> {
        self.publish_with(|from, to| fs::rename(from, to), File::sync_all)
    }

    fn validate_directories(&self) -> Result<()> {
        for (path, expected) in &self.directories {
            if directory_identity(path)? != *expected {
                return Err(changed(path));
            }
        }
        Ok(())
    }

    pub(crate) fn lock_verified(self) -> Result<LockedStagedReport> {
        self.verify_with_lock(FlockArg::LockExclusive)
    }

    pub(crate) fn try_lock_verified(self) -> Result<LockedStagedReport> {
        self.verify_with_lock(FlockArg::LockExclusiveNonblock)
    }

    fn verify_with_lock(mut self, operation: FlockArg) -> Result<LockedStagedReport> {
        // Serialize cooperating publishers before revalidating their destination snapshots.
        let lock_file = self
            .parent
            .try_clone()
            .map_err(|source| io_error("clone report directory", &self.report.path, source))?;
        let lock = Flock::lock(lock_file, operation).map_err(|(_, error)| {
            if error == nix::errno::Errno::EAGAIN {
                return ReportPublicationError::Busy {
                    path: self.report.path.clone(),
                };
            }
            io_error(
                "lock report directory",
                &self.report.path,
                io::Error::from_raw_os_error(error as i32),
            )
        })?;
        self.validate_directories()?;
        let before = regular_metadata(&self.file, &self.temporary)?;
        if Identity::of(&before) != self.identity
            || FileStamp::of(&before) != self.staged_stamp
            || self.report.size_bytes != before.len()
            || regular_identity(&self.temporary)? != self.identity
        {
            return Err(changed(&self.temporary));
        }
        let digest = hash_file(&mut self.file)
            .map_err(|source| io_error("verify staged bytes", &self.temporary, source))?;
        let after = regular_metadata(&self.file, &self.temporary)?;
        if FileStamp::of(&before) != FileStamp::of(&after) || digest != self.report.sha256 {
            return Err(changed(&self.temporary));
        }
        if destination(&self.report.path)? != self.previous {
            return Err(changed(&self.report.path));
        }
        Ok(LockedStagedReport {
            staged: self,
            _lock: lock,
            #[cfg(test)]
            fault: None,
        })
    }

    fn publish_with(
        self,
        rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
        sync_directory: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<PublishedReport> {
        self.lock_verified()?
            .publish_with(rename, sync_directory, || Ok(()))
    }
}

impl LockedStagedReport {
    #[cfg(test)]
    pub(crate) fn inject_fault(&mut self, fault: PublicationFault) {
        self.fault = Some(fault);
    }

    pub(crate) fn previous_hash(&self) -> Option<&str> {
        self.staged
            .previous
            .as_ref()
            .map(|previous| previous.sha256.as_str())
    }
    pub(crate) fn report(&self) -> &PublishedReport {
        &self.staged.report
    }

    pub(crate) fn publish_guarded(
        &mut self,
        guard: impl FnOnce() -> Result<()>,
    ) -> Result<PublishedReport> {
        #[cfg(test)]
        if let Some(fault) = self.fault.take() {
            return self.publish_with(
                |from, to| {
                    match &fault {
                        PublicationFault::Rename => {
                            return Err(io::Error::other("injected rename failure"));
                        }
                        PublicationFault::BeforeRename(marker) => pause_publication(marker)?,
                        _ => {}
                    }
                    fs::rename(from, to)?;
                    if let PublicationFault::AfterRename(marker) = &fault {
                        pause_publication(marker)?;
                    }
                    Ok(())
                },
                |directory| {
                    if matches!(fault, PublicationFault::DirectorySync) {
                        Err(io::Error::other("injected directory sync failure"))
                    } else {
                        directory.sync_all()
                    }
                },
                guard,
            );
        }
        self.publish_with(|from, to| fs::rename(from, to), File::sync_all, guard)
    }

    pub(crate) fn confirm_existing_guarded(
        &mut self,
        guard: impl FnOnce() -> Result<()>,
    ) -> Result<PublishedReport> {
        self.validate_final()?;
        let staged = &self.staged;
        let previous = staged
            .previous
            .as_ref()
            .ok_or_else(|| changed(&staged.report.path))?;
        if previous.sha256 != staged.report.sha256
            || previous.stamp.size != staged.report.size_bytes
        {
            return Err(changed(&staged.report.path));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&staged.report.path)
            .map_err(|source| io_error("open reconciled report", &staged.report.path, source))?;
        if FileStamp::of(&regular_metadata(&file, &staged.report.path)?) != previous.stamp {
            return Err(changed(&staged.report.path));
        }
        guard()?;
        file.sync_all()
            .and_then(|()| {
                #[cfg(test)]
                if matches!(self.fault, Some(PublicationFault::DirectorySync)) {
                    return Err(io::Error::other("injected directory sync failure"));
                }
                staged.parent.sync_all()
            })
            .map_err(|source| ReportPublicationError::PublishedButUnsynced {
                report: staged.report.clone(),
                source,
            })?;
        Ok(staged.report.clone())
    }

    fn validate_final(&self) -> Result<()> {
        let staged = &self.staged;
        staged.validate_directories()?;
        let current = destination_stamp(&staged.report.path)?;
        if FileStamp::of(&regular_metadata(&staged.file, &staged.temporary)?) != staged.staged_stamp
            || regular_identity(&staged.temporary)? != staged.identity
            || current.as_ref() != staged.previous.as_ref().map(|previous| &previous.stamp)
        {
            return Err(changed(&staged.report.path));
        }
        Ok(())
    }

    fn publish_with(
        &mut self,
        rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
        sync_directory: impl FnOnce(&File) -> io::Result<()>,
        guard: impl FnOnce() -> Result<()>,
    ) -> Result<PublishedReport> {
        self.validate_final()?;
        self.staged.rename_with(rename, sync_directory, guard)
    }
}

impl StagedReport {
    fn rename_with(
        &mut self,
        rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
        sync_directory: impl FnOnce(&File) -> io::Result<()>,
        guard: impl FnOnce() -> Result<()>,
    ) -> Result<PublishedReport> {
        self.validate_directories()?;
        if regular_identity(&self.temporary)? != self.identity {
            return Err(changed(&self.temporary));
        }
        guard()?;
        rename(&self.temporary, &self.report.path)
            .map_err(|source| io_error("rename staged report", &self.report.path, source))?;
        self.renamed = true;
        sync_directory(&self.parent).map_err(|source| {
            ReportPublicationError::PublishedButUnsynced {
                report: self.report.clone(),
                source,
            }
        })?;
        Ok(self.report.clone())
    }
}

impl Drop for StagedReport {
    fn drop(&mut self) {
        if !self.renamed
            && self.validate_directories().is_ok()
            && regular_identity(&self.temporary).is_ok_and(|identity| identity == self.identity)
            && fs::remove_file(&self.temporary).is_ok()
        {
            let _ = self.parent.sync_all();
        }
    }
}

fn stage_with_writer(
    project_root: &Path,
    output: &Path,
    bytes: &[u8],
    write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
) -> Result<StagedReport> {
    stage_with_policy(project_root, output, bytes, write, |_, _| Ok(()))
}

pub(crate) fn resolve_report_output(project_root: &Path, output: &Path) -> Result<PathBuf> {
    lexical_path(project_root, true)?;
    lexical_path(output, output.is_absolute())?;
    let target = if output.is_absolute() {
        output.to_path_buf()
    } else {
        project_root.join(output)
    };
    if target == project_root || !target.starts_with(project_root) {
        return Err(unsafe_path(
            &target,
            "output must be inside the project root",
        ));
    }
    Ok(target)
}

fn stage_with_policy(
    project_root: &Path,
    output: &Path,
    bytes: &[u8],
    write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
    authorize: impl FnOnce(&Path, &mut File) -> Result<()>,
) -> Result<StagedReport> {
    let target = resolve_report_output(project_root, output)?;
    let parent_path = target
        .parent()
        .ok_or_else(|| unsafe_path(&target, "output needs a parent"))?;
    let directories = directory_chain(parent_path)?;
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(parent_path)
        .map_err(|source| io_error("open report directory", parent_path, source))?;
    if directories.last().map(|(_, identity)| *identity)
        != Some(Identity::of(&parent.metadata().map_err(|source| {
            io_error("inspect report directory", parent_path, source)
        })?))
    {
        return Err(changed(parent_path));
    }
    let previous = destination_with(&target, authorize)?;
    let temporary = parent_path.join(format!(".igor-report-{}.tmp", Uuid::new_v4()));
    if temporary == target {
        return Err(changed(&target));
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&temporary)
        .map_err(|source| io_error("create report staging file", &temporary, source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect staging file", &temporary, source))?;
    let identity = Identity::of(&metadata);
    let mut staged = StagedReport {
        report: PublishedReport {
            path: target,
            size_bytes: bytes.len() as u64,
            sha256: format!("sha256:{:x}", Sha256::digest(bytes)),
        },
        temporary,
        file,
        identity,
        staged_stamp: FileStamp::of(&metadata),
        directories,
        parent,
        previous,
        renamed: false,
    };
    write(&mut staged.file, bytes)
        .map_err(|source| io_error("write staged report", &staged.temporary, source))?;
    staged
        .file
        .sync_all()
        .map_err(|source| io_error("sync staged report", &staged.temporary, source))?;
    staged.staged_stamp = FileStamp::of(&regular_metadata(&staged.file, &staged.temporary)?);
    staged
        .parent
        .sync_all()
        .map_err(|source| io_error("sync staging directory", &staged.temporary, source))?;
    Ok(staged)
}

fn lexical_path(path: &Path, absolute: bool) -> Result<()> {
    let bytes = path.as_os_str().as_bytes();
    let parts = if absolute {
        bytes
            .strip_prefix(b"/")
            .ok_or_else(|| unsafe_path(path, "project root must be absolute"))?
    } else {
        bytes
    };
    if (parts.is_empty() && path != Path::new("/"))
        || parts.contains(&0)
        || (!parts.is_empty()
            && parts
                .split(|byte| *byte == b'/')
                .any(|part| part.is_empty() || part == b"." || part == b".."))
    {
        return Err(unsafe_path(
            path,
            "empty, dot, parent or NUL components are not allowed",
        ));
    }
    Ok(())
}

fn directory_chain(path: &Path) -> Result<Vec<(PathBuf, Identity)>> {
    let mut chain = Vec::new();
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Normal(_) => current.push(component.as_os_str()),
            _ => {
                return Err(unsafe_path(
                    path,
                    "directory path is not absolute and normalized",
                ));
            }
        }
        chain.push((current.clone(), directory_identity(&current)?));
    }
    Ok(chain)
}

fn directory_identity(path: &Path) -> Result<Identity> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect directory chain", path, source))?;
    if !metadata.is_dir() {
        return Err(unsafe_path(
            path,
            "directory ancestors must be real directories",
        ));
    }
    Ok(Identity::of(&metadata))
}

fn regular_metadata(file: &File, path: &Path) -> Result<Metadata> {
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect report file", path, source))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(unsafe_path(
            path,
            "report files must be single-link regular files",
        ));
    }
    Ok(metadata)
}

fn regular_identity(path: &Path) -> Result<Identity> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect report path", path, source))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(unsafe_path(
            path,
            "report paths must be single-link regular files",
        ));
    }
    Ok(Identity::of(&metadata))
}

fn destination(path: &Path) -> Result<Option<Destination>> {
    destination_with(path, |_, _| Ok(()))
}

fn destination_stamp(path: &Path) -> Result<Option<FileStamp>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && metadata.nlink() == 1 => {
            Ok(Some(FileStamp::of(&metadata)))
        }
        Ok(_) => Err(unsafe_path(
            path,
            "destination must remain a single-link regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("revalidate report destination", path, error)),
    }
}

fn destination_with(
    path: &Path,
    authorize: impl FnOnce(&Path, &mut File) -> Result<()>,
) -> Result<Option<Destination>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && metadata.nlink() == 1 => {}
        Ok(_) => {
            return Err(unsafe_path(
                path,
                "destination must be a single-link regular file",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("inspect report destination", path, source)),
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|source| io_error("open report destination", path, source))?;
    let before = regular_metadata(&file, path)?;
    if regular_identity(path)? != Identity::of(&before) {
        return Err(changed(path));
    }
    authorize(path, &mut file)?;
    let sha256 =
        hash_file(&mut file).map_err(|source| io_error("hash report destination", path, source))?;
    let after = regular_metadata(&file, path)?;
    if FileStamp::of(&before) != FileStamp::of(&after) {
        return Err(changed(path));
    }
    Ok(Some(Destination {
        stamp: FileStamp::of(&after),
        sha256,
    }))
}

fn hash_file(file: &mut File) -> io::Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}

fn unsafe_path(path: &Path, reason: &'static str) -> ReportPublicationError {
    ReportPublicationError::UnsafePath {
        path: path.to_path_buf(),
        reason,
    }
}

fn changed(path: &Path) -> ReportPublicationError {
    ReportPublicationError::Changed {
        path: path.to_path_buf(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> ReportPublicationError {
    ReportPublicationError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn edits_after_hash_verification_are_rejected_by_the_final_metadata_fence() -> TestResult {
        let root = tempfile::tempdir()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let staged = stage_report(root.path(), &output, "new report")?;
        let temporary = staged.staging_path().to_path_buf();
        let mut locked = staged.lock_verified()?;
        fs::write(&temporary, "bad report")?;
        assert!(matches!(
            locked.publish_guarded(|| Ok(())),
            Err(ReportPublicationError::Changed { .. })
        ));
        assert_eq!(fs::read(&output)?, b"previous report");
        Ok(())
    }

    #[test]
    fn expired_publication_guard_aborts_immediately_before_rename() -> TestResult {
        let root = tempfile::tempdir()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let mut locked = stage_report(root.path(), &output, "new report")?.lock_verified()?;
        assert!(matches!(
            locked.publish_guarded(|| Err(ReportPublicationError::Aborted {
                reason: "report_lease_expired"
            })),
            Err(ReportPublicationError::Aborted { .. })
        ));
        assert_eq!(fs::read(&output)?, b"previous report");
        Ok(())
    }

    #[test]
    fn ownership_authorization_is_bound_to_the_captured_destination_snapshot() -> TestResult {
        let root = tempfile::tempdir()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous generated file")?;
        let result =
            stage_authorized_report(root.path(), &output, "new generated report", |path, _| {
                // A write racing the ownership check must not become an implicitly authorized snapshot.
                fs::write(path, "new handwritten replacement with different length")
                    .map_err(|source| io_error("fixture external edit", path, source))
            });
        assert!(matches!(
            result,
            Err(ReportPublicationError::Changed { .. })
        ));
        assert_eq!(
            fs::read(&output)?,
            b"new handwritten replacement with different length"
        );
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn failed_or_corrupt_staging_write_preserves_old_report_and_cleans_its_temp() -> TestResult {
        let root = tempfile::tempdir()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let result = stage_with_writer(
            root.path(),
            &output,
            b"new complete report",
            |file, bytes| {
                file.write_all(&bytes[..3])?;
                Err(io::Error::other("injected write failure"))
            },
        );
        assert!(matches!(
            result,
            Err(ReportPublicationError::Io {
                operation: "write staged report",
                ..
            })
        ));
        assert_eq!(fs::read(&output)?, b"previous report");
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        // A write that reports success but produces different bytes must still fail SHA verification.
        let staged = stage_with_writer(
            root.path(),
            &output,
            b"new complete report",
            |file, bytes| {
                let mut corrupted = bytes.to_vec();
                corrupted[0] = b'X';
                file.write_all(&corrupted)
            },
        )?;
        assert!(matches!(
            staged.publish(),
            Err(ReportPublicationError::Changed { .. })
        ));
        assert_eq!(fs::read(&output)?, b"previous report");
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn failed_rename_preserves_old_report_and_cleans_its_temp() -> TestResult {
        let root = tempfile::tempdir()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let staged = stage_report(root.path(), &output, "new complete report")?;
        let temporary = staged.staging_path().to_path_buf();
        let result = staged.publish_with(
            |_, _| Err(io::Error::other("injected rename failure")),
            File::sync_all,
        );
        assert!(matches!(
            result,
            Err(ReportPublicationError::Io {
                operation: "rename staged report",
                ..
            })
        ));
        assert_eq!(fs::read(&output)?, b"previous report");
        assert!(!temporary.exists());
        Ok(())
    }

    #[test]
    fn directory_sync_failure_after_rename_reports_already_published_identity() -> TestResult {
        let root = tempfile::tempdir()?;
        let output = root.path().join("RESULTS.md");
        fs::write(&output, "previous report")?;
        let staged = stage_report(root.path(), &output, "new complete report")?;
        let expected = staged.report().clone();
        let temporary = staged.staging_path().to_path_buf();
        let error = staged
            .publish_with(
                |from, to| fs::rename(from, to),
                |_| Err(io::Error::other("injected directory sync failure")),
            )
            .err()
            .ok_or("publication unexpectedly succeeded")?;
        let ReportPublicationError::PublishedButUnsynced { report, .. } = error else {
            return Err("post-rename error lost the published report identity".into());
        };
        assert_eq!(report, expected);
        assert_eq!(fs::read(&output)?, b"new complete report");
        assert!(!temporary.exists());
        Ok(())
    }
}
