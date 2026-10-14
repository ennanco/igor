use std::{
    fs::File,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::io::AsyncReadExt;

use super::{
    Database, PersistenceError, PersistenceResult, db, from_json, insert_event, json, parse_id,
};
use crate::{Event, EventId, EventKind, EventPayload, RecoveryId, RuntimePaths};

/// Version 1 diagnosis: {schema_version:1, content:{...}}; validation:
/// {schema_version:1, results:[...]}. An optional candidate requires a commit revision.
#[derive(Debug, Clone)]
pub struct RepairWorktreeEvidence {
    pub patch: Vec<u8>,
    pub diagnosis: Value,
    pub validation_results: Value,
    pub changed_paths: Vec<String>,
    pub candidate: Option<Value>,
}

pub struct RepairWorktreeRepository<'a> {
    database: &'a Database,
}

impl Database {
    pub fn repair_worktrees(&self) -> RepairWorktreeRepository<'_> {
        RepairWorktreeRepository { database: self }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    root: DirectoryIdentity,
    repository: DirectoryIdentity,
    common: DirectoryIdentity,
    admin: DirectoryIdentity,
    worktree: DirectoryIdentity,
}

#[derive(Serialize)]
struct Registration {
    path: PathBuf,
    root: PathBuf,
    repository: PathBuf,
    common: PathBuf,
    admin: PathBuf,
    identity: Identity,
}

fn registration(row: &sqlx::sqlite::SqliteRow) -> PersistenceResult<Registration> {
    Ok(Registration {
        path: row.get::<String, _>("path").into(),
        root: row.get::<String, _>("root_path").into(),
        repository: row.get::<String, _>("repository_path").into(),
        common: row.get::<String, _>("common_dir").into(),
        admin: row.get::<String, _>("admin_dir").into(),
        identity: from_json(row.get("identity_json"), "worktree identity")?,
    })
}

fn conflict() -> PersistenceError {
    PersistenceError::Conflict {
        entity: "repair worktree identity or evidence",
    }
}

/// Check every ancestor, including the trusted root itself. Do not canonicalize away links.
fn directory(path: &Path) -> PersistenceResult<DirectoryIdentity> {
    if !path.is_absolute() || path.components().collect::<PathBuf>().as_os_str() != path.as_os_str()
    {
        return Err(conflict());
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        if !matches!(component, Component::RootDir | Component::Normal(_)) {
            return Err(conflict());
        }
        current.push(component);
        if !std::fs::symlink_metadata(&current)?.file_type().is_dir() {
            return Err(conflict());
        }
    }
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(DirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

/// Runs only inside the blocking worker owning the SQLite transaction. Async pipe reads
/// bound output without unsafe descriptor operations; errors kill and reap the child.
fn git(repository: &Path, args: &[&std::ffi::OsStr]) -> PersistenceResult<Vec<u8>> {
    tokio::runtime::Handle::current().block_on(async {
        let mut child = tokio::process::Command::new("/usr/bin/git")
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .arg("-C")
            .arg(repository)
            .args(["-c", "core.hooksPath=/dev/null"])
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            let stdout = child.stdout.take().ok_or_else(conflict)?;
            let mut output = Vec::new();
            stdout
                .take(4 * 1024 * 1024 + 1)
                .read_to_end(&mut output)
                .await?;
            if output.len() > 4 * 1024 * 1024 {
                return Err(conflict());
            }
            if !child.wait().await?.success() {
                return Err(conflict());
            }
            Ok(output)
        })
        .await
        .unwrap_or_else(|_| Err(conflict()));
        if result.is_err() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        result
    })
}

fn common_dir(repository: &Path) -> PersistenceResult<PathBuf> {
    let output = git(
        repository,
        &[
            "rev-parse".as_ref(),
            "--path-format=absolute".as_ref(),
            "--git-common-dir".as_ref(),
        ],
    )?;
    let text = std::str::from_utf8(&output).map_err(|_| conflict())?;
    let path = PathBuf::from(text.strip_suffix('\n').ok_or_else(conflict)?);
    directory(&path)?;
    Ok(path)
}

fn listed(repository: &Path, path: &Path) -> PersistenceResult<bool> {
    let output = git(
        repository,
        &[
            "worktree".as_ref(),
            "list".as_ref(),
            "--porcelain".as_ref(),
            "-z".as_ref(),
        ],
    )?;
    let expected = format!("worktree {}", path.to_str().ok_or_else(conflict)?);
    Ok(output
        .split(|byte| *byte == 0)
        .filter(|field| *field == expected.as_bytes())
        .count()
        == 1)
}

fn admin_dir(path: &Path, common: &Path) -> PersistenceResult<PathBuf> {
    let dot_git = path.join(".git");
    if !std::fs::symlink_metadata(&dot_git)?.file_type().is_file() {
        return Err(conflict());
    }
    let contents = std::fs::read_to_string(dot_git)?;
    let admin = PathBuf::from(
        contents
            .strip_prefix("gitdir: ")
            .and_then(|text| text.strip_suffix('\n'))
            .ok_or_else(conflict)?,
    );
    if admin.parent() != Some(common.join("worktrees").as_path()) {
        return Err(conflict());
    }
    directory(&admin)?;
    verify_admin_common(&admin, common)?;
    let backref = admin.join("gitdir");
    if !std::fs::symlink_metadata(&backref)?.file_type().is_file() {
        return Err(conflict());
    }
    if std::fs::read_to_string(backref)?.strip_suffix('\n') != path.join(".git").to_str() {
        return Err(conflict());
    }
    if common_dir(path)? != common {
        return Err(conflict());
    }
    Ok(admin)
}

fn verify_admin_common(admin: &Path, common: &Path) -> PersistenceResult<()> {
    let file = admin.join("commondir");
    if !std::fs::symlink_metadata(&file)?.file_type().is_file() {
        return Err(conflict());
    }
    let text = std::fs::read_to_string(file)?;
    let target = Path::new(text.strip_suffix('\n').ok_or_else(conflict)?);
    // Git's ordinary relative link or an exact absolute common-dir link only.
    if target.as_os_str() != Path::new("../..").as_os_str()
        && target.as_os_str() != common.as_os_str()
    {
        return Err(conflict());
    }
    Ok(())
}

fn commit(repository: &Path, revision: &str) -> PersistenceResult<String> {
    if revision.is_empty() {
        return Err(conflict());
    }
    let expression = format!("{revision}^{{commit}}");
    let output = git(
        repository,
        &[
            "rev-parse".as_ref(),
            "--verify".as_ref(),
            "--end-of-options".as_ref(),
            expression.as_ref(),
        ],
    )?;
    let oid = std::str::from_utf8(&output)
        .map_err(|_| conflict())?
        .strip_suffix('\n')
        .ok_or_else(conflict)?;
    if !matches!(oid.len(), 40 | 64) || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(conflict());
    }
    Ok(oid.to_owned())
}

fn protected_ref(recovery_id: RecoveryId) -> String {
    format!("refs/igor/repairs/{recovery_id}")
}

fn verify_candidate(
    repository: &Path,
    recovery_id: RecoveryId,
    candidate: &Value,
) -> PersistenceResult<()> {
    if candidate.is_null() {
        return Ok(());
    }
    let revision = candidate
        .get("revision")
        .and_then(Value::as_str)
        .ok_or_else(conflict)?;
    let reference = protected_ref(recovery_id);
    if candidate.get("schema_version") != Some(&Value::from(1))
        || candidate.get("preserved_ref").and_then(Value::as_str) != Some(reference.as_str())
        || commit(repository, revision)? != revision
    {
        return Err(conflict());
    }
    // An exact direct ref is required; symbolic refs and absent refs fail closed.
    let output = git(
        repository,
        &[
            "for-each-ref".as_ref(),
            "--format=%(objectname)%(symref)".as_ref(),
            reference.as_ref(),
        ],
    )?;
    if output != format!("{revision}\n").as_bytes() {
        return Err(conflict());
    }
    Ok(())
}

fn preserve_candidate(
    registration: &Registration,
    recovery_id: RecoveryId,
    candidate: &mut Value,
) -> PersistenceResult<()> {
    if candidate.is_null() {
        return Ok(());
    }
    if candidate.get("schema_version") != Some(&Value::from(1)) {
        return Err(conflict());
    }
    let revision = candidate
        .get("revision")
        .and_then(Value::as_str)
        .ok_or_else(conflict)?;
    let oid = commit(&registration.path, revision)?;
    let reference = protected_ref(recovery_id);
    candidate["revision"] = Value::from(oid.clone());
    candidate["preserved_ref"] = Value::from(reference.clone());
    // Compare-and-swap creation never overwrites an existing ref. Git versions supporting
    // reference fsync durably flush this update; older Git ignores the config key.
    let create = git(
        &registration.repository,
        &[
            "-c".as_ref(),
            "core.fsync=reference".as_ref(),
            "update-ref".as_ref(),
            "--no-deref".as_ref(),
            reference.as_ref(),
            oid.as_ref(),
            "".as_ref(),
        ],
    );
    if create.is_err() {
        verify_candidate(&registration.repository, recovery_id, candidate)?;
        git(
            &registration.repository,
            &[
                "-c".as_ref(),
                "core.fsync=reference".as_ref(),
                "update-ref".as_ref(),
                "--no-deref".as_ref(),
                reference.as_ref(),
                oid.as_ref(),
                oid.as_ref(),
            ],
        )?;
    }
    verify_candidate(&registration.repository, recovery_id, candidate)
}

impl RepairWorktreeRepository<'_> {
    /// The bool is the validated project retention flag, frozen for this registration.
    pub async fn register_repair_worktree(
        &self,
        recovery_id: RecoveryId,
        paths: &RuntimePaths,
        delete_on_release: bool,
    ) -> PersistenceResult<()> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin worktree registration", e))?;
        sqlx::query("UPDATE recoveries SET updated_at = updated_at WHERE id = ?")
            .bind(recovery_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|e| db("reserve worktree registration", e))?;
        let row = sqlx::query("SELECT a.project_id, p.root_path FROM recoveries r JOIN attempts a ON a.id = r.attempt_id JOIN projects p ON p.id = a.project_id WHERE r.id = ?")
            .bind(recovery_id.to_string()).fetch_optional(&mut *transaction).await.map_err(|e| db("read worktree project", e))?.ok_or(PersistenceError::NotFound { entity: "worktree recovery" })?;
        let repository = PathBuf::from(row.get::<String, _>("root_path"));
        let root = paths.worktree_dir.clone();
        let path = root.join(recovery_id.to_string());
        let (mut transaction, registration) = tokio::task::spawn_blocking(move || {
            let result = (|| {
                let root_identity = directory(&root)?;
                let repository_identity = directory(&repository)?;
                let worktree_identity = directory(&path)?;
                let common = common_dir(&repository)?;
                let admin = admin_dir(&path, &common)?;
                if path == repository || !listed(&repository, &path)? {
                    return Err(conflict());
                }
                let identity = Identity {
                    root: root_identity,
                    repository: repository_identity,
                    common: directory(&common)?,
                    admin: directory(&admin)?,
                    worktree: worktree_identity,
                };
                Ok(Registration {
                    path,
                    root,
                    repository,
                    common,
                    admin,
                    identity,
                })
            })();
            (transaction, result)
        })
        .await?;
        let registration: Registration = registration?;
        sqlx::query("INSERT INTO repair_worktrees (recovery_id, project_id, path, root_path, repository_path, common_dir, admin_dir, identity_json, delete_on_release) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(recovery_id.to_string()).bind(row.get::<String, _>("project_id"))
            .bind(registration.path.to_str().ok_or_else(conflict)?).bind(registration.root.to_str().ok_or_else(conflict)?)
            .bind(registration.repository.to_str().ok_or_else(conflict)?).bind(registration.common.to_str().ok_or_else(conflict)?)
            .bind(registration.admin.to_str().ok_or_else(conflict)?).bind(json(&registration.identity, "worktree identity")?).bind(delete_on_release)
            .execute(&mut *transaction).await.map_err(|e| db("register worktree", e))?;
        transaction
            .commit()
            .await
            .map_err(|e| db("commit worktree registration", e))
    }

    pub async fn release_repair_worktree(
        &self,
        recovery_id: RecoveryId,
        evidence: &RepairWorktreeEvidence,
    ) -> PersistenceResult<()> {
        if let Some(candidate) = &evidence.candidate
            && (candidate.get("schema_version") != Some(&Value::from(1))
                || candidate
                    .get("revision")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty))
        {
            return Err(conflict());
        }
        if (evidence.patch.is_empty() && !evidence.changed_paths.is_empty())
            || evidence.changed_paths.iter().any(|path| {
                path.is_empty()
                    || !Path::new(path)
                        .components()
                        .all(|part| matches!(part, Component::Normal(_)))
            })
        {
            return Err(conflict());
        }
        let mut records = serde_json::json!({"schema_version": 1, "diagnosis": evidence.diagnosis, "validation_results": evidence.validation_results, "changed_paths": evidence.changed_paths, "candidate": evidence.candidate});
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin worktree release", e))?;
        sqlx::query("UPDATE repair_worktrees SET state = state WHERE recovery_id = ? AND state = 'registered'")
            .bind(recovery_id.to_string()).execute(&mut *transaction).await.map_err(|e| db("reserve worktree release", e))?;
        let row = sqlx::query("SELECT w.*, p.root_path AS current_repository_path FROM repair_worktrees w JOIN projects p ON p.id = w.project_id WHERE w.recovery_id = ? AND w.state = 'registered'")
            .bind(recovery_id.to_string()).fetch_optional(&mut *transaction).await.map_err(|e| db("read worktree release", e))?.ok_or_else(conflict)?;
        let registered = registration(&row)?;
        if registered.repository != Path::new(row.get::<&str, _>("current_repository_path")) {
            return Err(conflict());
        }
        let (owned_transaction, result) = tokio::task::spawn_blocking(move || {
            let result: PersistenceResult<Value> = (|| {
                verify(&registered, false)?;
                preserve_candidate(&registered, recovery_id, &mut records["candidate"])?;
                Ok(records)
            })();
            (transaction, result)
        })
        .await?;
        transaction = owned_transaction;
        let records: Value = result?;
        let digest = format!("{:x}", Sha256::digest(&evidence.patch));
        let result = sqlx::query("UPDATE repair_worktrees SET state = 'released', patch = ?, patch_sha256 = ?, records_json = ?, released_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE recovery_id = ? AND state = 'registered'")
            .bind(&evidence.patch).bind(digest).bind(json(&records, "worktree release")?).bind(recovery_id.to_string())
            .execute(&mut *transaction).await.map_err(|e| db("release worktree", e))?;
        if result.rows_affected() != 1 {
            return Err(conflict());
        }
        transaction
            .commit()
            .await
            .map_err(|e| db("commit worktree release", e))
    }

    pub async fn cleanup_candidates(
        &self,
        after: Option<RecoveryId>,
    ) -> PersistenceResult<Vec<RecoveryId>> {
        let ids: Vec<String> = sqlx::query_scalar("SELECT recovery_id FROM repair_worktrees WHERE state IN ('released', 'cleaning') AND recovery_id > COALESCE(?, '') ORDER BY recovery_id LIMIT 32")
            .bind(after.map(|id| id.to_string())).fetch_all(&self.database.pool).await.map_err(|e| db("find worktree cleanup", e))?;
        ids.iter()
            .map(|id| parse_id(id, "worktree recovery"))
            .collect()
    }

    pub async fn cleanup_repair_worktree(
        &self,
        recovery_id: RecoveryId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<bool> {
        self.cleanup_repair_selected(recovery_id, paths, None).await
    }

    pub(super) async fn cleanup_repair_selected(
        &self,
        recovery_id: RecoveryId,
        paths: &RuntimePaths,
        selected: Option<&[super::GcEntry]>,
    ) -> PersistenceResult<bool> {
        // Commit intent first. A crash after Git removes bytes must remain distinguishable
        // from an unregistered/missing tree. The second reservation rechecks all DB guards.
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin worktree intent", e))?;
        sqlx::query("UPDATE repair_worktrees SET state = state WHERE recovery_id = ? AND state = 'released'")
            .bind(recovery_id.to_string()).execute(&mut *transaction).await.map_err(|e| db("reserve worktree intent", e))?;
        if !super::gc::validate_selection(&mut transaction, paths, selected, true).await? {
            return Ok(false);
        }
        let pending = sqlx::query("SELECT w.*, p.root_path AS current_repository_path, p.registered FROM repair_worktrees w JOIN projects p ON p.id = w.project_id WHERE w.recovery_id = ? AND w.state = 'released'")
            .bind(recovery_id.to_string()).fetch_optional(&mut *transaction).await.map_err(|e| db("preflight worktree intent", e))?;
        if let Some(row) = pending {
            if row.get::<i64, _>("registered") != 1 {
                return Ok(false);
            }
            if row.get::<bool, _>("delete_on_release") {
                let registered = registration(&row)?;
                let records: Value = from_json(row.get("records_json"), "worktree release")?;
                if registered.root.as_os_str() != paths.worktree_dir.as_os_str()
                    || registered.path != paths.worktree_dir.join(recovery_id.to_string())
                    || registered.repository
                        != Path::new(row.get::<&str, _>("current_repository_path"))
                {
                    return Err(conflict());
                }
                let (owned_transaction, result) = tokio::task::spawn_blocking(move || {
                    let result = (|| {
                        verify(&registered, false)?;
                        verify_candidate(&registered.repository, recovery_id, &records["candidate"])
                    })();
                    (transaction, result)
                })
                .await?;
                transaction = owned_transaction;
                result?;
            }
        }
        sqlx::query("UPDATE repair_worktrees SET state = CASE WHEN delete_on_release = 1 THEN 'cleaning' ELSE 'released' END WHERE recovery_id = ? AND state = 'released'")
            .bind(recovery_id.to_string()).execute(&mut *transaction).await.map_err(|e| db("record worktree intent", e))?;
        transaction
            .commit()
            .await
            .map_err(|e| db("commit worktree intent", e))?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin worktree cleanup", e))?;
        sqlx::query("UPDATE repair_worktrees SET state = state WHERE recovery_id = ? AND state IN ('released', 'cleaning')")
            .bind(recovery_id.to_string()).execute(&mut *transaction).await.map_err(|e| db("reserve worktree cleanup", e))?;
        if !super::gc::validate_selection(&mut transaction, paths, selected, false).await? {
            return Ok(false);
        }
        let row = sqlx::query("SELECT w.*, a.job_id, a.id AS attempt_id, p.registered FROM repair_worktrees w JOIN recoveries r ON r.id = w.recovery_id JOIN attempts a ON a.id = r.attempt_id JOIN projects p ON p.id = w.project_id WHERE w.recovery_id = ?")
            .bind(recovery_id.to_string()).fetch_optional(&mut *transaction).await.map_err(|e| db("read worktree cleanup", e))?.ok_or(PersistenceError::NotFound { entity: "repair worktree" })?;
        let state: &str = row.get("state");
        if !matches!(state, "released" | "cleaning") || row.get::<i64, _>("registered") != 1 {
            return Ok(false);
        }
        let patch: Vec<u8> = row.get("patch");
        if format!("{:x}", Sha256::digest(&patch)) != row.get::<String, _>("patch_sha256") {
            return Err(conflict());
        }
        let delete: bool = row.get("delete_on_release");
        if delete {
            let registration = registration(&row)?;
            let records: Value = from_json(row.get("records_json"), "worktree release")?;
            if registration.root.as_os_str() != paths.worktree_dir.as_os_str()
                || registration.path != paths.worktree_dir.join(recovery_id.to_string())
            {
                return Err(conflict());
            }
            let repository: String =
                sqlx::query_scalar("SELECT root_path FROM projects WHERE id = ?")
                    .bind(row.get::<String, _>("project_id"))
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(|e| db("recheck worktree project", e))?;
            if Path::new(&repository) != registration.repository {
                return Err(conflict());
            }
            let (owned_transaction, result) = tokio::task::spawn_blocking(move || {
                let result = (|| {
                    verify(&registration, true)?;
                    verify_candidate(&registration.repository, recovery_id, &records["candidate"])?;
                    remove(&registration)
                })();
                (transaction, result)
            })
            .await?;
            transaction = owned_transaction;
            result?;
        }
        let next = if delete { "removed" } else { "retained" };
        let identity = serde_json::json!({"registration": registration(&row)?, "patch_sha256":row.get::<String,_>("patch_sha256"), "records":from_json::<Value>(row.get("records_json"), "worktree release")?, "delete_on_release":delete});
        super::cleanup_evidence::record(
            &mut transaction,
            "repair_worktree_retention",
            &recovery_id.to_string(),
            next,
            &identity,
            None,
            None,
        )
        .await?;
        sqlx::query("UPDATE repair_worktrees SET state = ? WHERE recovery_id = ?")
            .bind(next)
            .bind(recovery_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|e| db("finish worktree cleanup", e))?;
        let event = Event::new(
            EventId::new(),
            EventKind::CleanupStateChanged,
            EventPayload::new(
                EventKind::CleanupStateChanged,
                1,
                serde_json::json!({"recovery_id": recovery_id, "from": state, "state": next, "reason": "repair_worktree_retention"}),
            )?,
        )?;
        insert_event(
            &mut transaction,
            parse_id(row.get("project_id"), "worktree project")?,
            Some(parse_id(row.get("job_id"), "worktree job")?),
            Some(parse_id(row.get("attempt_id"), "worktree attempt")?),
            &event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|e| db("commit worktree cleanup", e))?;
        Ok(delete)
    }
}

fn absent(path: &Path) -> PersistenceResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
    }
}

fn verify(registration: &Registration, allow_missing: bool) -> PersistenceResult<bool> {
    let r = registration;
    if directory(&r.root)? != r.identity.root
        || directory(&r.repository)? != r.identity.repository
        || directory(&r.common)? != r.identity.common
        || common_dir(&r.repository)? != r.common
    {
        return Err(conflict());
    }
    // Validate the admin parent even on replay; never follow a replacement link.
    let admin_parent = r.common.join("worktrees");
    if !absent(&admin_parent)? {
        directory(&admin_parent)?;
    }
    let missing_tree = absent(&r.path)?;
    let missing_admin = absent(&r.admin)?;
    if !allow_missing && (missing_tree || missing_admin) {
        return Err(conflict());
    }
    if !missing_tree
        && (directory(&r.path)? != r.identity.worktree || admin_dir(&r.path, &r.common)? != r.admin)
    {
        return Err(conflict());
    }
    if !missing_admin {
        if directory(&r.admin)? != r.identity.admin {
            return Err(conflict());
        }
        verify_admin_common(&r.admin, &r.common)?;
        let backref = r.admin.join("gitdir");
        if !std::fs::symlink_metadata(&backref)?.file_type().is_file()
            || std::fs::read_to_string(backref)?.strip_suffix('\n') != r.path.join(".git").to_str()
        {
            return Err(conflict());
        }
        if !listed(&r.repository, &r.path)? {
            return Err(conflict());
        }
    } else if !missing_tree || listed(&r.repository, &r.path)? {
        return Err(conflict());
    }
    Ok(!missing_admin)
}

fn remove(registration: &Registration) -> PersistenceResult<()> {
    let r = registration;
    if verify(r, true)? {
        git(
            &r.repository,
            &[
                "worktree".as_ref(),
                "remove".as_ref(),
                "--force".as_ref(),
                "--".as_ref(),
                r.path.as_os_str(),
            ],
        )?;
    }
    if !absent(&r.path)? || !absent(&r.admin)? {
        return Err(conflict());
    }
    File::open(&r.root)?.sync_all()?;
    let admin_parent = r.common.join("worktrees");
    // Git also removes the worktrees parent when deleting the last linked tree.
    if !absent(&admin_parent)? {
        directory(&admin_parent)?;
        File::open(admin_parent)?.sync_all()?;
    }
    File::open(&r.common)?.sync_all()?;
    Ok(())
}

pub(super) async fn preview(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    recovery_id: RecoveryId,
    paths: &RuntimePaths,
    output: &mut Vec<super::GcEntry>,
) -> PersistenceResult<()> {
    use super::{GcEffect as E, GcEntry, GcOperation as O, GcStatus as S, GcTarget};
    let row = sqlx::query("SELECT w.*, a.id AS attempt_id, r.state AS recovery_state, p.root_path AS current_repository_path, p.registered,
        EXISTS (SELECT 1 FROM actions ac WHERE ac.kind = 'diagnose_or_repair' AND ac.state IN ('pending', 'running') AND (ac.attempt_id = a.id OR (ac.attempt_id IS NULL AND (ac.job_id = a.job_id OR (ac.job_id IS NULL AND ac.project_id = a.project_id))))) AS active_action,
        EXISTS (SELECT 1 FROM agent_sessions s WHERE s.recovery_id = r.id AND (s.cleanup_state IN ('pending', 'running') OR s.lease_id IS NOT NULL OR s.lease_owner IS NOT NULL OR s.lease_expires_at IS NOT NULL)) AS active_session
        FROM repair_worktrees w JOIN recoveries r ON r.id = w.recovery_id JOIN attempts a ON a.id = r.attempt_id JOIN projects p ON p.id = w.project_id WHERE w.recovery_id = ?")
        .bind(recovery_id.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("read worktree GC eligibility", e))?;
    let mut entry = GcEntry::new(
        O::RepairWorktrees,
        recovery_id.to_string(),
        parse_id(row.get("project_id"), "GC project")?,
        Some(parse_id(row.get("attempt_id"), "GC attempt")?),
    );
    let result: PersistenceResult<()> = async {
        let state: &str = row.get("state");
        let barrier = if state == "registered" {
            Some("release_evidence_pending")
        } else if !matches!(
            row.get::<&str, _>("recovery_state"),
            "succeeded" | "failed" | "rejected" | "cancelled"
        ) {
            Some("recovery_nonterminal")
        } else if row.get::<i64, _>("active_action") != 0 {
            Some("active_repair_action")
        } else if row.get::<i64, _>("active_session") != 0 {
            Some("active_agent_session")
        } else if row.get::<i64, _>("registered") != 1 {
            Some("project_unregistered")
        } else {
            None
        };
        if let Some(reason) = barrier {
            entry.decide(S::Deferred, reason, E::Retain);
            return Ok(());
        }
        let patch: Vec<u8> = row.get("patch");
        if format!("{:x}", Sha256::digest(&patch)) != row.get::<String, _>("patch_sha256") {
            return Err(conflict());
        }
        if !row.get::<bool, _>("delete_on_release") {
            entry.decide(S::Retain, "keep_worktree_policy", E::Retain);
            return Ok(());
        }
        let registered = registration(&row)?;
        if registered.root.as_os_str() != paths.worktree_dir.as_os_str()
            || registered.path != paths.worktree_dir.join(recovery_id.to_string())
            || registered.repository != Path::new(row.get::<&str, _>("current_repository_path"))
        {
            return Err(conflict());
        }
        let records: Value = from_json(row.get("records_json"), "GC worktree evidence")?;
        let allow_missing = state == "cleaning";
        let targets = tokio::task::spawn_blocking(move || -> PersistenceResult<Vec<GcTarget>> {
            verify(&registered, allow_missing)?;
            verify_candidate(&registered.repository, recovery_id, &records["candidate"])?;
            let mut targets = vec![];
            for path in [registered.path, registered.admin] {
                if !absent(&path)? {
                    targets.push(GcTarget {
                        path,
                        effect: E::Remove,
                        bytes: None,
                    });
                }
            }
            Ok(targets)
        })
        .await??;
        entry.decide(
            if targets.is_empty() {
                S::MetadataOnly
            } else {
                S::Ready
            },
            "verified_released_worktree",
            if targets.is_empty() {
                E::ResumeMetadata
            } else {
                E::Remove
            },
        );
        entry.targets = targets;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        GcEntry::unsafe_batch(std::slice::from_mut(&mut entry), &error);
    }
    output.push(entry);
    Ok(())
}
