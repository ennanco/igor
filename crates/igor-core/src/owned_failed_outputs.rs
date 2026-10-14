use super::success_log_archives::{
    archive_eligibility, check_archive_registration, digest, group_quiescent, raw_consumers,
};
use super::*;
use crate::RuntimePaths;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
};

fn conflict() -> PersistenceError {
    PersistenceError::Conflict {
        entity: "owned output identity",
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Namespace {
    attempt_id: AttemptId,
    created_empty: bool,
    project_root: PathBuf,
    root: PathBuf,
    dev: u64,
    ino: u64,
    stdout: PathBuf,
    stderr: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Output {
    namespace: Namespace,
    name: String,
    dev: u64,
    ino: u64,
    size: u64,
    sha256: String,
}
impl Output {
    fn id(&self, attempt: AttemptId) -> String {
        format!("{attempt}:{:x}", Sha256::digest(self.name.as_bytes()))
    }
    fn path(&self) -> PathBuf {
        self.namespace.root.join(&self.name)
    }
}

fn directory(path: &Path) -> PersistenceResult<fs::Metadata> {
    if !path.is_absolute()
        || !path.to_str().is_some_and(|p| {
            p == "/"
                || p.strip_prefix('/')
                    .is_some_and(|p| p.split('/').all(|c| !matches!(c, "" | "." | "..")))
        })
    {
        return Err(conflict());
    }
    let mut walked = PathBuf::new();
    for part in path.components() {
        if !matches!(part, Component::RootDir | Component::Normal(_)) {
            return Err(conflict());
        }
        walked.push(part);
        if !fs::symlink_metadata(&walked)?.file_type().is_dir() {
            return Err(conflict());
        }
    }
    Ok(fs::symlink_metadata(path)?)
}
fn verify_namespace(
    ns: &Namespace,
    attempt: AttemptId,
    paths: &RuntimePaths,
) -> PersistenceResult<()> {
    if ns.attempt_id != attempt
        || !ns.created_empty
        || ns.root != paths.log_dir.join(attempt.to_string())
        || ns.stdout != ns.root.join("stdout.log")
        || ns.stderr != ns.root.join("stderr.log")
    {
        return Err(conflict());
    }
    let m = directory(&ns.root)?;
    if (m.dev(), m.ino()) != (ns.dev, ns.ino)
        || m.uid() != fs::metadata("/proc/self")?.uid()
        || m.mode() & 0o777 != 0o700
    {
        return Err(conflict());
    }
    Ok(())
}
fn inspect(ns: &Namespace, name: &str) -> PersistenceResult<Output> {
    if !Path::new(name)
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
        || Path::new(name).components().count() != 1
    {
        return Err(conflict());
    }
    let path = ns.root.join(name);
    let before = fs::symlink_metadata(&path)?;
    if !before.file_type().is_file() || before.nlink() != 1 {
        return Err(conflict());
    }
    let parent = namespace_handle(ns)?;
    let file = File::from(
        nix::fcntl::openat(
            &parent,
            name,
            nix::fcntl::OFlag::O_RDONLY
                | nix::fcntl::OFlag::O_NOFOLLOW
                | nix::fcntl::OFlag::O_NONBLOCK,
            nix::sys::stat::Mode::empty(),
        )
        .map_err(std::io::Error::from)?,
    );
    let opened = file.metadata()?;
    if (before.dev(), before.ino()) != (opened.dev(), opened.ino())
        || !opened.is_file()
        || opened.nlink() != 1
    {
        return Err(conflict());
    }
    let (size, sha256) = digest(&file)?;
    let after = file.metadata()?;
    if size != before.len()
        || (
            before.len(),
            before.mtime(),
            before.mtime_nsec(),
            before.ctime(),
            before.ctime_nsec(),
        ) != (
            after.len(),
            after.mtime(),
            after.mtime_nsec(),
            after.ctime(),
            after.ctime_nsec(),
        )
    {
        return Err(conflict());
    }
    Ok(Output {
        namespace: ns.clone(),
        name: name.to_owned(),
        dev: before.dev(),
        ino: before.ino(),
        size,
        sha256,
    })
}

fn namespace_handle(ns: &Namespace) -> PersistenceResult<File> {
    directory(&ns.root)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&ns.root)?;
    let m = file.metadata()?;
    if (m.dev(), m.ino()) != (ns.dev, ns.ino) {
        return Err(conflict());
    }
    Ok(file)
}

async fn barrier(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    paths: &RuntimePaths,
) -> PersistenceResult<Option<&'static str>> {
    let row = archive_eligibility(tx, attempt).await?;
    let spec: AttemptSpec = from_json(row.get("spec_json"), "owned output spec")?;
    if spec.result().owned_namespace != Some(true)
        || spec.result().failed_artifact_retention.is_none()
    {
        return Ok(Some("legacy_policy_absent"));
    }
    if !matches!(row.get::<&str, _>("state"), "failed" | "cancelled" | "lost") {
        return Ok(Some("attempt_not_failed_terminal"));
    }
    if row.get::<i64, _>("registered") != 1 {
        return Ok(Some("project_unregistered"));
    }
    let complete: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM diagnosed_attempts WHERE attempt_id = ?1) AND EXISTS(SELECT 1 FROM failed_log_tails WHERE attempt_id = ?1 AND complete = 1)")
        .bind(attempt.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("check owned output diagnosis", e))?;
    if complete == 0 {
        return Ok(Some("diagnosis_or_complete_tail_pending"));
    }
    if row.get::<i64, _>("extracted") == 0 || raw_consumers(tx, attempt).await? {
        return Ok(Some("output_consumers_pending"));
    }
    let superseded: i64 = sqlx::query_scalar("SELECT j.state = 'superseded' FROM attempts a JOIN jobs j ON j.id = a.job_id WHERE a.id = ?")
        .bind(attempt.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("check owned output replacement", e))?;
    if superseded != 0 && !replacement_attempt_succeeded(tx, attempt).await? {
        return Ok(Some("current_replacement_pending"));
    }
    check_archive_registration(tx, attempt, paths, &row).await?;
    let group: Option<i64> = row.get("process_group_id");
    if tokio::task::spawn_blocking(move || group.map(group_quiescent).transpose()).await??
        == Some(false)
    {
        return Ok(Some("live_process_group"));
    }
    Ok(None)
}

async fn namespace(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    paths: &RuntimePaths,
) -> PersistenceResult<Namespace> {
    let json: String = sqlx::query_scalar(
        "SELECT identity_json FROM owned_output_namespaces WHERE attempt_id = ?",
    )
    .bind(attempt.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| db("read owned namespace", e))?
    .ok_or_else(conflict)?;
    let ns: Namespace = from_json(&json, "owned namespace")?;
    let project_root: String = sqlx::query_scalar(
        "SELECT p.root_path FROM attempts a JOIN projects p ON p.id = a.project_id WHERE a.id = ?",
    )
    .bind(attempt.to_string())
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| db("recheck owned project boundary", e))?;
    if ns.project_root != Path::new(&project_root)
        || ns.root.starts_with(&ns.project_root)
        || ns.project_root.starts_with(&ns.root)
    {
        return Err(conflict());
    }
    verify_namespace(&ns, attempt, paths)?;
    Ok(ns)
}

pub(super) async fn consent(
    tx: &mut Transaction<'_, Sqlite>,
    entry: &GcEntry,
    paths: &RuntimePaths,
) -> PersistenceResult<serde_json::Value> {
    let attempt = entry.attempt_id.ok_or_else(conflict)?;
    if entry.subject_id == format!("{attempt}:discovery") {
        let mut current = vec![];
        preview(tx, attempt, paths, &mut current, &mut vec![]).await?;
        return current
            .into_iter()
            .find(|e| e.subject_id == entry.subject_id && e.status == GcStatus::MetadataOnly)
            .map(|e| e.consent_identity)
            .ok_or_else(conflict);
    }
    let ns = namespace(tx, attempt, paths).await?;
    let prior: Option<String> = sqlx::query_scalar(
        "SELECT identity_json FROM owned_output_manifest WHERE id = ? AND attempt_id = ?",
    )
    .bind(&entry.subject_id)
    .bind(attempt.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| db("read owned consent", e))?;
    let allow_missing = prior_exists(tx, &entry.subject_id).await?;
    let entry = entry.clone();
    tokio::task::spawn_blocking(move || {
        let output: Output = match prior {
            Some(p) => from_json(&p, "owned output intent")?,
            None => {
                let path = &entry.targets.first().ok_or_else(conflict)?.path;
                if path.parent() != Some(ns.root.as_path()) {
                    return Err(conflict());
                }
                inspect(
                    &ns,
                    path.file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(conflict)?,
                )?
            }
        };
        if output.namespace != ns || output.id(attempt) != entry.subject_id {
            return Err(conflict());
        }
        match fs::symlink_metadata(output.path()) {
            Ok(_) if inspect(&ns, &output.name)? != output => return Err(conflict()),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && allow_missing => {}
            Err(e) => return Err(e.into()),
        }
        serde_json::to_value(output).map_err(|_| conflict())
    })
    .await?
}
async fn prior_exists(tx: &mut Transaction<'_, Sqlite>, id: &str) -> PersistenceResult<bool> {
    Ok(sqlx::query_scalar::<_,i64>("SELECT EXISTS(SELECT 1 FROM owned_output_manifest WHERE id = ? AND state IN ('prepared','completed'))").bind(id).fetch_one(&mut **tx).await.map_err(|e| db("check owned intent",e))? != 0)
}

pub(super) async fn preview(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    paths: &RuntimePaths,
    entries: &mut Vec<GcEntry>,
    batches: &mut Vec<GcArtifactBatch>,
) -> PersistenceResult<()> {
    let (project, json): (String, String) =
        sqlx::query_as("SELECT project_id, spec_json FROM attempts WHERE id = ?")
            .bind(attempt.to_string())
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db("read owned output attempt", e))?;
    let spec: AttemptSpec = from_json(&json, "owned spec")?;
    let project = parse_id(&project, "owned project")?;
    let reason = match barrier(tx, attempt, paths).await {
        Ok(reason) => reason,
        Err(error) => {
            let mut e = GcEntry::new(
                GcOperation::OwnedFailedOutputs,
                attempt.to_string(),
                project,
                Some(attempt),
            );
            GcEntry::unsafe_batch(std::slice::from_mut(&mut e), &error);
            entries.push(e);
            return Ok(());
        }
    };
    // No discovery before the durable diagnostic and consumer barriers.
    if let Some(reason) = reason {
        let mut e = GcEntry::new(
            GcOperation::OwnedFailedOutputs,
            attempt.to_string(),
            project,
            Some(attempt),
        );
        e.decide(
            if reason == "legacy_policy_absent" {
                GcStatus::Retain
            } else {
                GcStatus::Deferred
            },
            reason,
            GcEffect::Retain,
        );
        entries.push(e);
        return Ok(());
    }
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM owned_output_namespaces WHERE attempt_id = ?)",
    )
    .bind(attempt.to_string())
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| db("check namespace ownership", e))?;
    if exists == 0 {
        return Ok(());
    }
    let ns = match namespace(tx, attempt, paths).await {
        Ok(ns) => ns,
        Err(error) => {
            let mut e = GcEntry::new(
                GcOperation::OwnedFailedOutputs,
                attempt.to_string(),
                project,
                Some(attempt),
            );
            GcEntry::unsafe_batch(std::slice::from_mut(&mut e), &error);
            entries.push(e);
            return Ok(());
        }
    };
    let policy = spec
        .result()
        .failed_artifact_retention
        .as_ref()
        .ok_or_else(conflict)?;
    policy.validate()?;
    let mut builder = globset::GlobSetBuilder::new();
    for p in &policy.keep_on_failure {
        builder.add(globset::Glob::new(p).map_err(|_| conflict())?);
    }
    let keep = builder.build().map_err(|_| conflict())?;
    // Registered sources are in this same private directory; staging/publication
    // are separate namespaces and must never be swept even under a bad binding.
    if spec
        .result()
        .publication_root
        .as_ref()
        .is_some_and(|p| ns.root.starts_with(p) || p.starts_with(&ns.root))
        || ns
            .root
            .starts_with(paths.state_dir.join("artifact-staging"))
    {
        return Err(conflict());
    }
    let registered: Vec<String> = sqlx::query_scalar(
        "SELECT path FROM artifacts WHERE attempt_id = ? ORDER BY id LIMIT 4097",
    )
    .bind(attempt.to_string())
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| db("read registered owned exclusions", e))?;
    if registered.len() > 4096 {
        let mut e = GcEntry::new(
            GcOperation::OwnedFailedOutputs,
            attempt.to_string(),
            project,
            Some(attempt),
        );
        e.decide(
            GcStatus::Deferred,
            "registered_exclusions_bound",
            GcEffect::Retain,
        );
        entries.push(e);
        batches.push(GcArtifactBatch {
            attempt_id: attempt,
            rows_observed: 4096,
            truncated: true,
            continuation_after: None,
        });
        return Ok(());
    }
    if registered.iter().any(|p| !valid_artifact_relative_path(p)) {
        return Err(conflict());
    }
    let temporaries: Vec<String> = sqlx::query_scalar(
        "SELECT temp_path FROM success_log_archive_preparations WHERE attempt_id = ?",
    )
    .bind(attempt.to_string())
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| db("read owned control paths", e))?;
    let prior: Vec<(String,String,String)> = sqlx::query_as("SELECT id, identity_json, state FROM owned_output_manifest WHERE attempt_id = ? AND state = 'prepared' ORDER BY id LIMIT 65").bind(attempt.to_string()).fetch_all(&mut **tx).await.map_err(|e| db("read owned manifest",e))?;
    let policy = policy.clone();
    let checked_paths = paths.clone();
    let (mut discovered, batch) = tokio::task::spawn_blocking(
        move || -> PersistenceResult<(Vec<GcEntry>, GcArtifactBatch)> {
            let mut entries = vec![];
            let mut names = std::collections::BTreeMap::new();
            let had_pending = !prior.is_empty();
            for (id, json, state) in prior {
                let o: Output = from_json(&json, "owned manifest")?;
                names.insert(o.name.clone(), Some((id, o, state)));
            }
            // Late registration protects already-prepared leaves as well as
            // newly discovered files in this same writer snapshot.
            names.retain(|name, _| {
                !registered.iter().any(|p| {
                    Path::new(p)
                        .components()
                        .next()
                        .is_some_and(|c| c.as_os_str() == name.as_str())
                })
            });
            let mut snapshot = std::collections::BTreeMap::new();
            for child in fs::read_dir(&ns.root)? {
                if snapshot.len() == 4096 {
                    let mut e = GcEntry::new(GcOperation::OwnedFailedOutputs,attempt.to_string(),project,Some(attempt));
                    e.decide(GcStatus::Deferred,"namespace_children_bound",GcEffect::Retain);
                    return Ok((vec![e],GcArtifactBatch {attempt_id:attempt,rows_observed:4096,truncated:true,continuation_after:None}));
                }
                let child = child?;
                let name = child.file_name().into_string().map_err(|_|conflict())?;
                let m = fs::symlink_metadata(child.path())?;
                snapshot.insert(name.clone(),serde_json::json!([m.dev(),m.ino(),m.mode(),m.nlink(),m.len(),m.mtime(),m.mtime_nsec(),m.ctime(),m.ctime_nsec()]));
                if matches!(name.as_str(),"stdout.log"|"stderr.log"|"stdout.log.gz"|"stderr.log.gz"|".git") || temporaries.iter().any(|p|Path::new(p)==child.path()) || m.file_type().is_dir() {continue;}
                if registered.iter().any(|p| {
                    Path::new(p)
                        .components()
                        .next()
                        .is_some_and(|c| c.as_os_str() == name.as_str())
                }) {
                    continue;
                }
                if m.file_type().is_file() && m.nlink()==1 && (policy.policy == crate::FailedRetention::Keep || keep.is_match(&name)) {continue;}
                names.entry(name).or_insert(None);
            }
            let pending = !names.is_empty();
            verify_namespace(&ns,attempt,&checked_paths)?;
            let continuation = names.keys().nth(63).cloned();
            let truncated = names.len() > 64;
            let batch = GcArtifactBatch {
                attempt_id: attempt,
                rows_observed: names.len().min(64),
                truncated,
                continuation_after: continuation.filter(|_| truncated),
            };
            for (name, prior) in names.into_iter().take(64) {
                if prior.as_ref().is_some_and(|(_, _, s)| s == "completed") {
                    continue;
                }
                let id = format!("{attempt}:{:x}", Sha256::digest(name.as_bytes()));
                let mut e =
                    GcEntry::new(GcOperation::OwnedFailedOutputs, id, project, Some(attempt));
                let inspected = inspect(&ns, &name);
                let (identity, present) = match (inspected, prior) {
                    (Ok(current), Some((id, previous, _)))
                        if previous != current || id != current.id(attempt) =>
                    {
                        GcEntry::unsafe_batch(std::slice::from_mut(&mut e), &conflict());
                        entries.push(e);
                        continue;
                    }
                    (Ok(current), _) => (current, true),
                    (Err(PersistenceError::Io(ref error)), Some((_, previous, state)))
                        if error.kind() == std::io::ErrorKind::NotFound
                            && state == "prepared"
                            && previous.namespace == ns =>
                    {
                        (previous, false)
                    }
                    (Err(error), _) => {
                        GcEntry::unsafe_batch(std::slice::from_mut(&mut e), &error);
                        entries.push(e);
                        continue;
                    }
                };
                let retained =
                    policy.policy == crate::FailedRetention::Keep || keep.is_match(&name);
                e.decide(
                    if retained {
                        GcStatus::Retain
                    } else if present {
                        GcStatus::Ready
                    } else {
                        GcStatus::MetadataOnly
                    },
                    if retained {
                        "keep_policy"
                    } else {
                        "verified_owned_failed_output"
                    },
                    if retained {
                        GcEffect::Retain
                    } else if present {
                        GcEffect::Remove
                    } else {
                        GcEffect::ResumeMetadata
                    },
                );
                e.targets.push(GcTarget {
                    path: identity.path(),
                    bytes: Some(identity.size),
                    effect: e.effect,
                });
                entries.push(e);
            }
            if !pending && !had_pending {
                let mut e = GcEntry::new(GcOperation::OwnedFailedOutputs,format!("{attempt}:discovery"),project,Some(attempt));
                e.decide(GcStatus::MetadataOnly,"verified_owned_discovery_complete",GcEffect::ResumeMetadata);
                e.consent_identity = serde_json::json!({"namespace":ns,"children_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&snapshot).map_err(|_|conflict())?))});
                entries.push(e);
            }
            Ok((entries, batch))
        },
    )
    .await??;
    entries.append(&mut discovered);
    batches.push(batch);
    Ok(())
}

impl JobAttemptRepository<'_> {
    /// Create logs and commit fresh, empty namespace ownership before launching.
    /// Existing directories and legacy snapshots are never adopted.
    pub async fn prepare_attempt_logs(
        &self,
        attempt: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<(PathBuf, PathBuf)> {
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin owned namespace", e))?;
        sqlx::query("UPDATE attempts SET state = state WHERE id = ?")
            .bind(attempt.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| db("reserve owned namespace writer", e))?;
        let frozen: String = sqlx::query_scalar("SELECT spec_json FROM attempts WHERE id = ?")
            .bind(attempt.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| db("read namespace policy", e))?;
        let spec: AttemptSpec = from_json(&frozen, "namespace policy")?;
        let project_root: String = sqlx::query_scalar("SELECT p.root_path FROM attempts a JOIN projects p ON p.id = a.project_id WHERE a.id = ?").bind(attempt.to_string()).fetch_one(&mut *tx).await.map_err(|e|db("read namespace project boundary",e))?;
        // Validate all surviving ancestors before recursive creation.
        let mut walked = PathBuf::new();
        if !paths.log_dir.is_absolute() {
            return Err(conflict());
        }
        for part in paths.log_dir.components() {
            if !matches!(part, Component::RootDir | Component::Normal(_)) {
                return Err(conflict());
            }
            walked.push(part);
            match fs::symlink_metadata(&walked) {
                Ok(m) if m.file_type().is_dir() => {}
                Ok(_) => return Err(conflict()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(e.into()),
            }
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&paths.log_dir)?;
        directory(&paths.log_dir)?;
        let root = paths.log_dir.join(attempt.to_string());
        let fresh = match fs::DirBuilder::new().mode(0o700).create(&root) {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e.into()),
        };
        let m = directory(&root)?;
        let ns = Namespace {
            attempt_id: attempt,
            created_empty: fresh,
            project_root: PathBuf::from(&project_root),
            stdout: root.join("stdout.log"),
            stderr: root.join("stderr.log"),
            root,
            dev: m.dev(),
            ino: m.ino(),
        };
        if fresh
            && spec.result().owned_namespace == Some(true)
            && !ns.root.starts_with(&project_root)
            && !Path::new(&project_root).starts_with(&ns.root)
            && !ns
                .root
                .starts_with(paths.state_dir.join("artifact-staging"))
            && !spec
                .result()
                .publication_root
                .as_ref()
                .is_some_and(|p| ns.root.starts_with(p) || p.starts_with(&ns.root))
        {
            if fs::read_dir(&ns.root)?.next().is_some() {
                return Err(conflict());
            }
            File::open(&ns.root)?.sync_all()?;
            File::open(&paths.log_dir)?.sync_all()?;
            sqlx::query(
                "INSERT INTO owned_output_namespaces(attempt_id,identity_json) VALUES (?,?)",
            )
            .bind(attempt.to_string())
            .bind(json(&ns, "owned namespace")?)
            .execute(&mut *tx)
            .await
            .map_err(|e| db("record fresh owned namespace", e))?;
        }
        tx.commit()
            .await
            .map_err(|e| db("commit owned namespace before launch", e))?;
        Ok((ns.stdout, ns.stderr))
    }

    pub async fn owned_failed_output_candidates(
        &self,
        after: Option<AttemptId>,
    ) -> PersistenceResult<Vec<AttemptId>> {
        let ids: Vec<String> = sqlx::query_scalar("SELECT a.id FROM attempts a JOIN owned_output_namespaces n ON n.attempt_id = a.id JOIN diagnosed_attempts d ON d.attempt_id = a.id WHERE a.id > COALESCE(?, '') AND a.state IN ('failed','cancelled','lost') AND (NOT EXISTS(SELECT 1 FROM owned_output_discovery d WHERE d.attempt_id = a.id) OR EXISTS(SELECT 1 FROM owned_output_manifest m WHERE m.attempt_id = a.id AND state = 'prepared')) ORDER BY a.id LIMIT 32")
            .bind(after.map(|a|a.to_string())).fetch_all(&self.database.pool).await.map_err(|e| db("find owned failed outputs",e))?;
        ids.iter().map(|s| parse_id(s, "owned candidate")).collect()
    }
    pub async fn retire_owned_failed_outputs(
        &self,
        attempt: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<usize> {
        self.retire_owned_failed_outputs_selected(attempt, paths, None)
            .await
    }
    pub(super) async fn retire_owned_failed_outputs_selected(
        &self,
        attempt: AttemptId,
        paths: &RuntimePaths,
        selected: Option<&[GcEntry]>,
    ) -> PersistenceResult<usize> {
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin owned discovery", e))?;
        sqlx::query("UPDATE attempts SET state = state WHERE id = ?")
            .bind(attempt.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| db("reserve owned discovery", e))?;
        if barrier(&mut tx, attempt, paths).await?.is_some() {
            return Ok(0);
        }
        let drained: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM owned_output_discovery WHERE attempt_id = ?1) AND NOT EXISTS(SELECT 1 FROM owned_output_manifest WHERE attempt_id = ?1 AND state = 'prepared')").bind(attempt.to_string()).fetch_one(&mut *tx).await.map_err(|e|db("check drained owned namespace",e))?;
        if drained != 0 {
            return Ok(0);
        }
        let mut entries = vec![];
        preview(&mut tx, attempt, paths, &mut entries, &mut vec![]).await?;
        if !gc::validate_selection(&mut tx, paths, selected, true).await? {
            return Ok(0);
        }
        if let Some(entry) = entries.iter().find(|e| {
            e.subject_id == format!("{attempt}:discovery")
                && selected.is_none_or(|s| s.iter().any(|x| x.subject_id == e.subject_id))
        }) {
            let identity = consent(&mut tx, entry, paths).await?;
            namespace(&mut tx, attempt, paths).await?;
            sqlx::query("INSERT INTO owned_output_discovery(attempt_id,complete,identity_json) VALUES (?,1,?) ON CONFLICT DO NOTHING").bind(attempt.to_string()).bind(identity.to_string()).execute(&mut *tx).await.map_err(|e|db("complete owned discovery",e))?;
            tx.commit()
                .await
                .map_err(|e| db("commit owned discovery", e))?;
            return Ok(0);
        }
        let mut prepared = vec![];
        for entry in entries.into_iter().filter(|e| {
            matches!(e.status, GcStatus::Ready | GcStatus::MetadataOnly)
                && selected.is_none_or(|s| s.iter().any(|x| x.subject_id == e.subject_id))
        }) {
            let identity = consent(&mut tx, &entry, paths).await?;
            sqlx::query("INSERT INTO owned_output_manifest(id,attempt_id,identity_json,state) VALUES (?,?,?,'prepared') ON CONFLICT DO NOTHING")
                .bind(&entry.subject_id).bind(attempt.to_string()).bind(identity.to_string()).execute(&mut *tx).await.map_err(|e|db("prepare owned output intent",e))?;
            prepared.push(entry);
        }
        tx.commit()
            .await
            .map_err(|e| db("commit owned output intents", e))?;
        let mut count = 0;
        for entry in prepared {
            let mut tx = self
                .database
                .pool
                .begin()
                .await
                .map_err(|e| db("begin owned unlink", e))?;
            sqlx::query("UPDATE attempts SET state = state WHERE id = ?")
                .bind(attempt.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| db("reserve owned unlink", e))?;
            if barrier(&mut tx, attempt, paths).await?.is_some() {
                continue;
            }
            if let Some(s) = selected {
                let one: Vec<_> = s
                    .iter()
                    .filter(|e| e.subject_id == entry.subject_id)
                    .cloned()
                    .collect();
                if !gc::validate_selection(&mut tx, paths, Some(&one), false).await? {
                    continue;
                }
            }
            let (state, decided): (String, String) =
                sqlx::query_as("SELECT state,decided_at FROM owned_output_manifest WHERE id = ?")
                    .bind(&entry.subject_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| db("read owned intent state", e))?;
            if state == "completed" {
                continue;
            }
            let value = consent(&mut tx, &entry, paths).await?;
            let output: Output = serde_json::from_value(value.clone()).map_err(|_| conflict())?;
            // Recheck bindings, exclusions and frozen keep policy in this writer.
            let mut current = vec![];
            preview(&mut tx, attempt, paths, &mut current, &mut vec![]).await?;
            if !current.iter().any(|e| {
                e.subject_id == entry.subject_id
                    && matches!(e.status, GcStatus::Ready | GcStatus::MetadataOnly)
            }) {
                continue;
            }
            let paths = paths.clone();
            let (mut tx, result) = tokio::task::spawn_blocking(move || {
                let result = (|| -> PersistenceResult<()> {
                    verify_namespace(&output.namespace, attempt, &paths)?;
                    let parent = namespace_handle(&output.namespace)?;
                    match fs::symlink_metadata(output.path()) {
                        Ok(_) => {
                            if inspect(&output.namespace, &output.name)? != output {
                                return Err(conflict());
                            }
                            nix::unistd::unlinkat(
                                &parent,
                                output.name.as_str(),
                                nix::unistd::UnlinkatFlags::NoRemoveDir,
                            )
                            .map_err(std::io::Error::from)?;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                    parent.sync_all()?;
                    Ok(())
                })();
                (tx, result)
            })
            .await?;
            result?;
            cleanup_evidence::record(
                &mut tx,
                "failed_owned_output_retention",
                &entry.subject_id,
                "removed",
                &value,
                Some(&decided),
                None,
            )
            .await?;
            sqlx::query("UPDATE owned_output_manifest SET state = 'completed' WHERE id = ?")
                .bind(&entry.subject_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| db("complete owned output", e))?;
            let job: String = sqlx::query_scalar("SELECT job_id FROM attempts WHERE id = ?")
                .bind(attempt.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| db("read owned event job", e))?;
            let event = crate::Event::new(
                crate::EventId::new(),
                crate::EventKind::CleanupStateChanged,
                crate::EventPayload::new(
                    crate::EventKind::CleanupStateChanged,
                    1,
                    serde_json::json!({"attempt_id":attempt,"subject_id":entry.subject_id,"state":"removed","reason":"failed_owned_output_retention"}),
                )?,
            )?;
            insert_event(
                &mut tx,
                entry.project_id,
                Some(parse_id(&job, "owned job")?),
                Some(attempt),
                &event,
            )
            .await?;
            tx.commit()
                .await
                .map_err(|e| db("commit owned completion", e))?;
            count += 1;
        }
        Ok(count)
    }
}
