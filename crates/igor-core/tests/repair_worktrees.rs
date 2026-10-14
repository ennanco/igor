use std::{
    error::Error,
    fs,
    os::unix::fs::symlink,
    path::{Path, PathBuf},
    process::Command,
};

use igor_core::{Database, RecoveryId, RepairWorktreeEvidence, RuntimePaths};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;
#[path = "support/gc.rs"]
mod gc_support;

#[tokio::test]
async fn gc_execute_worktree_exact_recovery_and_failed_only_exclusion() -> TestResult {
    let f = Fixture::new().await?;
    f.register(true).await?;
    f.terminal("failed").await?;
    f.release().await?;
    let failed = f.database.gc_plan_filtered(None, &f.paths, true).await?;
    assert!(failed.entries.is_empty());
    let worklist = f.database.gc_plan(None, &f.paths).await?.worklist()?;
    assert_eq!(worklist.selections.len(), 1);
    let result = f
        .database
        .gc_execute(None, false, worklist.clone(), &f.paths)
        .await?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert!(!f.worktree.exists());
    assert_eq!(
        f.database
            .gc_execute(None, false, worklist, &f.paths)
            .await?
            .already_done,
        1
    );
    Ok(())
}

#[tokio::test]
async fn gc_worktrees_unregistered_project_defers_before_intent_and_retirement() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    for between_phases in [false, true] {
        let f = Fixture::new().await?;
        f.register(true).await?;
        f.terminal("failed").await?;
        f.release().await?;
        let admin = f.admin()?;
        let input = fs::read(f.worktree.join("input"))?;
        let gitdir = fs::read(f.worktree.join(".git"))?;
        let backref = fs::read(admin.join("gitdir"))?;
        let refs = git_output(&f.repo, &["show-ref"])?;
        if between_phases {
            sqlx::raw_sql("CREATE TRIGGER deregister AFTER UPDATE OF state ON repair_worktrees WHEN NEW.state = 'cleaning' AND OLD.state = 'released' BEGIN UPDATE projects SET registered = 0; END;").execute(f.database.pool()).await?;
            assert!(
                !f.database
                    .repair_worktrees()
                    .cleanup_repair_worktree(f.recovery, &f.paths)
                    .await?
            );
        } else {
            sqlx::query("UPDATE projects SET registered = 0")
                .execute(f.database.pool())
                .await?;
        }
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.temporary.path(), None).await?;
        let entry = plan
            .entries
            .iter()
            .find(|e| e.operation == O::RepairWorktrees)
            .ok_or("entry")?;
        assert_eq!(entry.status, S::Deferred);
        assert_eq!(entry.reason, "project_unregistered");
        assert!(
            !f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await?
        );
        assert_eq!(
            f.state().await?,
            if between_phases {
                "cleaning"
            } else {
                "released"
            }
        );
        assert_eq!(fs::read(f.worktree.join("input"))?, input);
        assert_eq!(fs::read(f.worktree.join(".git"))?, gitdir);
        assert_eq!(fs::read(admin.join("gitdir"))?, backref);
        assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
        assert!(f.worktree.is_dir() && admin.is_dir());
        if between_phases {
            sqlx::query("DROP TRIGGER deregister")
                .execute(f.database.pool())
                .await?;
        }
        sqlx::query("UPDATE projects SET registered = 1")
            .execute(f.database.pool())
            .await?;
        assert!(
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await?
        );
        assert!(!f.worktree.exists() && !admin.exists());
    }
    Ok(())
}

#[tokio::test]
async fn gc_worktrees_preview_registered_keep_delete_and_missing_replay() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    for case in ["registered", "keep", "delete", "missing", "cleaning"] {
        let f = Fixture::new().await?;
        f.register(case != "keep").await?;
        if case != "registered" {
            f.terminal("failed").await?;
            f.release().await?;
        }
        let admin = f.admin()?;
        let unrelated = f.paths.worktree_dir.join("unrelated");
        git(
            &f.repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                "--",
                unrelated.to_str().ok_or("unrelated path")?,
            ],
        )?;
        fs::create_dir_all(unrelated.join("private/nested"))?;
        fs::write(
            unrelated.join("private/nested/preserve"),
            b"unrelated bytes",
        )?;
        // Collapse the expected filesystem delta from independent fixture roots,
        // not the preview's target list or the persisted cleanup identity.
        let removed_roots = if case == "delete" {
            vec![f.worktree.clone(), admin.clone()]
        } else {
            vec![]
        };
        let candidate_ref = format!("refs/igor/repairs/{}", f.recovery);
        let candidate = git_output(&f.repo, &["rev-parse", "HEAD"])?;
        if case != "registered" {
            assert_eq!(
                git_output(&f.repo, &["rev-parse", &candidate_ref])?,
                candidate
            );
        }
        if matches!(case, "missing" | "cleaning") {
            if case == "cleaning" {
                sqlx::query("UPDATE repair_worktrees SET state = 'cleaning'")
                    .execute(f.database.pool())
                    .await?;
            }
            git(
                &f.repo,
                &[
                    "worktree",
                    "remove",
                    "--force",
                    "--",
                    f.worktree.to_str().ok_or("path")?,
                ],
            )?;
        }
        let refs_before = git_output(&f.repo, &["show-ref"])?;
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.temporary.path(), None).await?;
        assert_eq!(git_output(&f.repo, &["show-ref"])?, refs_before);
        let e = plan
            .entries
            .iter()
            .find(|e| e.operation == O::RepairWorktrees)
            .ok_or("entry")?;
        assert_eq!(
            e.status,
            match case {
                "registered" => S::Deferred,
                "keep" => S::Retain,
                "missing" => S::Unsafe,
                "cleaning" => S::MetadataOnly,
                _ => S::Ready,
            }
        );
        if case == "delete" {
            let targets: std::collections::BTreeSet<_> =
                e.targets.iter().map(|t| t.path.clone()).collect();
            assert_eq!(
                targets,
                [f.worktree.clone(), admin.clone()].into_iter().collect()
            );
            assert!(e.targets.iter().all(|t| t.bytes.is_none()));
            assert!(
                e.targets
                    .iter()
                    .all(|t| t.effect == igor_core::GcEffect::Remove)
            );
        } else {
            assert!(e.targets.is_empty());
        }
        assert_eq!(plan.known_copy_bytes, 0);
        let before = gc_support::files(f.temporary.path(), &f.paths.database)?;
        let mut result = f
            .database
            .gc_execute(None, false, plan.worklist()?, &f.paths)
            .await?;
        let selected = matches!(case, "keep" | "delete" | "cleaning");
        let entries = if selected {
            vec![igor_core::GcExecutionEntry {
                operation: O::RepairWorktrees,
                subject_id: f.recovery.to_string(),
                status: igor_core::GcExecutionStatus::Completed,
                cause: None,
            }]
        } else {
            vec![]
        };
        result
            .entries
            .sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
        assert_eq!(
            result,
            igor_core::GcExecution {
                confirmed: true,
                completed: usize::from(selected),
                entries,
                ..Default::default()
            },
            "{case}"
        );
        assert_eq!(
            gc_support::files(f.temporary.path(), &f.paths.database)?,
            before
                .into_iter()
                .filter(|(path, _, _)| !removed_roots.iter().any(|root| path.starts_with(root)))
                .collect::<Vec<_>>(),
            "{case}"
        );
        assert_eq!(git_output(&f.repo, &["show-ref"])?, refs_before);
        if case != "registered" {
            assert_eq!(
                git_output(&f.repo, &["rev-parse", &candidate_ref])?,
                candidate
            );
        }
    }
    Ok(())
}

fn git(repo: &Path, args: &[&str]) -> TestResult {
    git_output(repo, args).map(|_| ())
}

fn git_output(repo: &Path, args: &[&str]) -> Result<String, Box<dyn Error>> {
    let output = Command::new("/usr/bin/git")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!("git {args:?}: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn evidence() -> RepairWorktreeEvidence {
    RepairWorktreeEvidence {
        patch:
            b"diff --git a/input b/input\n--- a/input\n+++ b/input\n@@ -1 +1 @@\n-before\n+after\n"
                .to_vec(),
        diagnosis: json!({"schema_version": 1, "content": {"reason": "fixture"}}),
        validation_results: json!({"schema_version": 1, "results": [{"passed": true}]}),
        changed_paths: vec!["input".into()],
        candidate: None,
    }
}

struct Fixture {
    temporary: TempDir,
    database: Database,
    paths: RuntimePaths,
    repo: PathBuf,
    recovery: RecoveryId,
    attempt: String,
    worktree: PathBuf,
}

impl Fixture {
    async fn new() -> Result<Self, Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let root = temporary.path();
        let repo = root.join("project");
        fs::create_dir(&repo)?;
        git(&repo, &["init", "--quiet"])?;
        fs::write(repo.join("input"), "before\n")?;
        git(&repo, &["add", "--", "input"])?;
        git(&repo, &["commit", "--quiet", "-m", "fixture"])?;
        let paths = RuntimePaths {
            config_file: root.join("config"),
            state_dir: root.join("state"),
            runtime_dir: root.join("runtime"),
            database: root.join("igor.db"),
            log_dir: root.join("logs"),
            worktree_dir: root.join("worktrees"),
            report_dir: root.join("reports"),
            worker_socket: root.join("worker.sock"),
            supervisor_socket: root.join("supervisor.sock"),
            resource_dir: root.join("resources"),
        };
        fs::create_dir(&paths.worktree_dir)?;
        let database = Database::open(&paths.database).await?;
        let project = uuid::Uuid::new_v4().to_string();
        let job = uuid::Uuid::new_v4().to_string();
        let attempt = uuid::Uuid::new_v4().to_string();
        let recovery = RecoveryId::new();
        sqlx::query(
            "INSERT INTO projects (id, name, root_path, config_path) VALUES (?, 'fixture', ?, ?)",
        )
        .bind(&project)
        .bind(repo.to_str().ok_or("repo path")?)
        .bind(repo.join("project.toml").to_str().ok_or("config path")?)
        .execute(database.pool())
        .await?;
        sqlx::query("INSERT INTO jobs (id, project_id, name, state, submission_order, spec_json) VALUES (?, ?, 'fixture', 'failed', 1, '{}')")
            .bind(&job).bind(&project).execute(database.pool()).await?;
        sqlx::query("INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json) VALUES (?, ?, ?, 1, 'failed', '{}')")
            .bind(&attempt).bind(&job).bind(&project).execute(database.pool()).await?;
        sqlx::query("INSERT INTO recoveries (id, attempt_id, state, idempotency_key) VALUES (?, ?, 'diagnosing', ?)")
            .bind(recovery.to_string()).bind(&attempt).bind(recovery.to_string()).execute(database.pool()).await?;
        let worktree = paths.worktree_dir.join(recovery.to_string());
        git(
            &repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                "--",
                worktree.to_str().ok_or("worktree path")?,
            ],
        )?;
        Ok(Self {
            temporary,
            database,
            paths,
            repo,
            recovery,
            attempt,
            worktree,
        })
    }

    async fn register(&self, delete: bool) -> TestResult {
        self.database
            .repair_worktrees()
            .register_repair_worktree(self.recovery, &self.paths, delete)
            .await?;
        Ok(())
    }

    async fn terminal(&self, state: &str) -> TestResult {
        sqlx::query("UPDATE recoveries SET state = ? WHERE id = ?")
            .bind(state)
            .bind(self.recovery.to_string())
            .execute(self.database.pool())
            .await?;
        Ok(())
    }

    async fn release(&self) -> TestResult {
        let mut e = evidence();
        e.candidate = Some(
            json!({"schema_version":1, "revision":git_output(&self.worktree, &["rev-parse", "HEAD"])?}),
        );
        self.database
            .repair_worktrees()
            .release_repair_worktree(self.recovery, &e)
            .await?;
        Ok(())
    }

    async fn state(&self) -> Result<String, Box<dyn Error>> {
        Ok(
            sqlx::query_scalar("SELECT state FROM repair_worktrees WHERE recovery_id = ?")
                .bind(self.recovery.to_string())
                .fetch_one(self.database.pool())
                .await?,
        )
    }

    fn admin(&self) -> Result<PathBuf, Box<dyn Error>> {
        let contents = fs::read_to_string(self.worktree.join(".git"))?;
        Ok(contents
            .trim()
            .strip_prefix("gitdir: ")
            .ok_or("gitdir")?
            .into())
    }
}

#[tokio::test]
async fn registered_tree_and_nonterminal_recoveries_are_preserved() -> TestResult {
    let f = Fixture::new().await?;
    f.register(true).await?;
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_candidates(None)
            .await?
            .is_empty()
    );
    assert!(
        !f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await?
    );
    for state in ["pending", "diagnosing", "proposed", "approved", "applying"] {
        f.terminal(state).await?;
        assert!(f.release().await.is_err());
        assert_eq!(f.state().await?, "registered");
        assert!(f.worktree.join("input").is_file());
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_evidence_and_active_consumers_block_release() -> TestResult {
    let f = Fixture::new().await?;
    f.register(true).await?;
    f.terminal("failed").await?;
    for field in [
        "diagnosis",
        "validation",
        "candidate",
        "version",
        "path",
        "missing_revision",
        "empty_revision",
        "fake_revision",
        "blob_revision",
        "empty_patch",
        "null_candidate",
        "candidate_version",
    ] {
        let mut e = evidence();
        match field {
            "diagnosis" => e.diagnosis = json!({}),
            "validation" => e.validation_results = json!({"schema_version":1}),
            "candidate" => e.candidate = Some(json!({})),
            "version" => e.diagnosis = json!({"schema_version":true, "content":{}}),
            "missing_revision" => e.candidate = Some(json!({"schema_version":1})),
            "empty_revision" => e.candidate = Some(json!({"schema_version":1,"revision":""})),
            "fake_revision" => e.candidate = Some(json!({"schema_version":1,"revision":"fixture"})),
            "blob_revision" => {
                e.candidate = Some(json!({"schema_version":1,"revision":"HEAD:input"}))
            }
            "empty_patch" => e.patch.clear(),
            "null_candidate" => e.candidate = Some(json!(null)),
            "candidate_version" => {
                e.candidate = Some(json!({"schema_version":true,"revision":"HEAD"}))
            }
            _ => e.changed_paths = vec!["../escape".into()],
        }
        assert!(
            f.database
                .repair_worktrees()
                .release_repair_worktree(f.recovery, &e)
                .await
                .is_err()
        );
    }
    assert!(
        sqlx::query("UPDATE repair_worktrees SET state = 'released' WHERE recovery_id = ?")
            .bind(f.recovery.to_string())
            .execute(f.database.pool())
            .await
            .is_err()
    );
    let action = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO actions (id, project_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT ?, project_id, id, 'diagnose_or_repair', 'pending', '{}', ?, '2000-01-01' FROM attempts WHERE id = ?")
        .bind(&action).bind(&action).bind(&f.attempt).execute(f.database.pool()).await?;
    assert!(f.release().await.is_err());
    sqlx::query("UPDATE actions SET state = 'cancelled' WHERE id = ?")
        .bind(&action)
        .execute(f.database.pool())
        .await?;
    // Legacy/project-wide actions with no attempt association are also conservative barriers.
    sqlx::query("UPDATE actions SET attempt_id = NULL, state = 'pending' WHERE id = ?")
        .bind(&action)
        .execute(f.database.pool())
        .await?;
    assert!(f.release().await.is_err());
    sqlx::query("UPDATE actions SET state = 'cancelled' WHERE id = ?")
        .bind(&action)
        .execute(f.database.pool())
        .await?;
    let session = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO agent_sessions (id, recovery_id, provider, external_session_id, cleanup_state) VALUES (?, ?, 'fixture', ?, 'running')")
        .bind(&session).bind(f.recovery.to_string()).bind(&session).execute(f.database.pool()).await?;
    assert!(f.release().await.is_err());
    sqlx::query("UPDATE agent_sessions SET cleanup_state = 'succeeded' WHERE id = ?")
        .bind(&session)
        .execute(f.database.pool())
        .await?;
    sqlx::query("UPDATE agent_sessions SET lease_owner = 'still-active' WHERE id = ?")
        .bind(&session)
        .execute(f.database.pool())
        .await?;
    assert!(f.release().await.is_err());
    sqlx::query("UPDATE agent_sessions SET lease_owner = NULL WHERE id = ?")
        .bind(&session)
        .execute(f.database.pool())
        .await?;
    f.release().await?;
    assert!(
        sqlx::query("UPDATE actions SET state = 'pending' WHERE id = ?")
            .bind(action)
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE agent_sessions SET cleanup_state = 'running' WHERE id = ?")
            .bind(session)
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert!(f.terminal("applying").await.is_err());
    assert!(f.release().await.is_err());
    assert!(
        sqlx::query("UPDATE repair_worktrees SET patch = x'00'")
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE repair_worktrees SET delete_on_release = 0")
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM repair_worktrees")
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT OR REPLACE INTO repair_worktrees (recovery_id, project_id, path, root_path, repository_path, common_dir, admin_dir, identity_json, delete_on_release) SELECT recovery_id, project_id, path, root_path, repository_path, common_dir, admin_dir, identity_json, delete_on_release FROM repair_worktrees")
        .execute(f.database.pool()).await.is_err());
    assert_eq!(f.state().await?, "released");
    Ok(())
}

#[tokio::test]
async fn explicit_no_change_evidence_is_valid_and_registration_is_exact() -> TestResult {
    let f = Fixture::new().await?;
    let mut other_paths = f.paths.clone();
    other_paths.worktree_dir = f.temporary.path().join("empty-root");
    fs::create_dir(&other_paths.worktree_dir)?;
    assert!(
        f.database
            .repair_worktrees()
            .register_repair_worktree(f.recovery, &other_paths, true)
            .await
            .is_err()
    );
    f.register(true).await?;
    assert!(f.register(false).await.is_err());
    f.terminal("cancelled").await?;
    let mut e = evidence();
    e.patch.clear();
    e.changed_paths.clear();
    e.candidate = None;
    e.validation_results = json!({"schema_version":1, "results":[]});
    f.database
        .repair_worktrees()
        .release_repair_worktree(f.recovery, &e)
        .await?;
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &other_paths)
            .await
            .is_err()
    );
    assert!(f.worktree.exists());
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn candidate_discovery_is_bounded_and_keyset_paginated() -> TestResult {
    let f = Fixture::new().await?;
    let mut ids = Vec::new();
    for _ in 0..33 {
        let recovery = RecoveryId::new();
        sqlx::query("INSERT INTO recoveries (id, attempt_id, state, idempotency_key) VALUES (?, ?, 'failed', ?)")
            .bind(recovery.to_string()).bind(&f.attempt).bind(recovery.to_string()).execute(f.database.pool()).await?;
        let path = f.paths.worktree_dir.join(recovery.to_string());
        git(
            &f.repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                "--",
                path.to_str().ok_or("path")?,
            ],
        )?;
        f.database
            .repair_worktrees()
            .register_repair_worktree(recovery, &f.paths, true)
            .await?;
        f.database
            .repair_worktrees()
            .release_repair_worktree(recovery, &evidence())
            .await?;
        ids.push(recovery);
    }
    ids.sort_by_key(|id| id.to_string());
    let first = f
        .database
        .repair_worktrees()
        .cleanup_candidates(None)
        .await?;
    assert_eq!(first, ids[..32]);
    let second = f
        .database
        .repair_worktrees()
        .cleanup_candidates(first.last().copied())
        .await?;
    assert_eq!(second, ids[32..]);
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_candidates(second.last().copied())
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn keep_policy_retains_bytes_and_records_event() -> TestResult {
    let f = Fixture::new().await?;
    f.register(false).await?;
    f.terminal("rejected").await?;
    f.release().await?;
    assert!(
        !f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await?
    );
    assert_eq!(f.state().await?, "retained");
    let subject = f.recovery.to_string();
    let tombstone = f
        .database
        .cleanup_tombstone("repair_worktree_retention", &subject)
        .await?
        .ok_or("worktree tombstone")?;
    assert_eq!(tombstone["outcome"], "retained");
    f.database
        .repair_worktrees()
        .cleanup_repair_worktree(f.recovery, &f.paths)
        .await?;
    assert_eq!(
        f.database
            .cleanup_tombstone("repair_worktree_retention", &subject)
            .await?,
        Some(tombstone)
    );
    assert!(f.worktree.join("input").exists());
    assert!(f.admin()?.is_dir());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'"
        )
        .fetch_one(f.database.pool())
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn terminal_release_removes_only_registered_tree_and_keeps_evidence() -> TestResult {
    for terminal in ["succeeded", "failed", "rejected", "cancelled"] {
        let f = Fixture::new().await?;
        let other = f.paths.worktree_dir.join("other");
        git(
            &f.repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                "--",
                other.to_str().ok_or("other path")?,
            ],
        )?;
        f.register(true).await?;
        let admin = f.admin()?;
        f.terminal(terminal).await?;
        f.release().await?;
        assert_eq!(
            f.database
                .repair_worktrees()
                .cleanup_candidates(None)
                .await?,
            vec![f.recovery]
        );
        assert!(
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await?
        );
        assert!(!f.worktree.exists());
        assert!(!admin.exists());
        assert!(other.join("input").is_file());
        assert!(f.repo.join("input").is_file());
        assert_eq!(f.state().await?, "removed");
        let row = sqlx::query("SELECT patch, patch_sha256, records_json FROM repair_worktrees")
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(row.get::<Vec<u8>, _>("patch"), evidence().patch);
        assert_eq!(
            row.get::<String, _>("patch_sha256"),
            format!("{:x}", Sha256::digest(evidence().patch))
        );
        let records: serde_json::Value = serde_json::from_str(row.get("records_json"))?;
        assert_eq!(records["diagnosis"], evidence().diagnosis);
        assert_eq!(records["validation_results"], evidence().validation_results);
        assert_eq!(records["changed_paths"], json!(["input"]));
        assert_eq!(
            records["candidate"],
            json!({"schema_version":1, "revision":git_output(&f.repo, &["rev-parse", "HEAD"])?, "preserved_ref":format!("refs/igor/repairs/{}", f.recovery)})
        );
        assert!(
            !f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn detached_candidate_survives_cleanup_and_pruning_without_changing_main() -> TestResult {
    let f = Fixture::new().await?;
    let main_head = git_output(&f.repo, &["rev-parse", "HEAD"])?;
    let main_status = git_output(&f.repo, &["status", "--porcelain"])?;
    fs::write(f.worktree.join("input"), "after\n")?;
    git(&f.worktree, &["add", "input"])?;
    git(
        &f.worktree,
        &["commit", "--quiet", "-m", "detached candidate"],
    )?;
    let candidate = git_output(&f.worktree, &["rev-parse", "HEAD"])?;
    assert_ne!(candidate, main_head);
    f.register(true).await?;
    f.terminal("succeeded").await?;
    let mut e = evidence();
    e.candidate = Some(json!({"schema_version":1, "revision":"HEAD"}));
    f.database
        .repair_worktrees()
        .release_repair_worktree(f.recovery, &e)
        .await?;
    let reference = format!("refs/igor/repairs/{}", f.recovery);
    let records: String = sqlx::query_scalar("SELECT records_json FROM repair_worktrees")
        .fetch_one(f.database.pool())
        .await?;
    let records: serde_json::Value = serde_json::from_str(&records)?;
    assert_eq!(
        records["candidate"],
        json!({"schema_version":1, "revision":candidate, "preserved_ref":reference})
    );
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await?
    );
    assert!(!f.worktree.exists());
    git(&f.repo, &["reflog", "expire", "--expire=now", "--all"])?;
    git(&f.repo, &["gc", "--prune=now"])?;
    assert_eq!(
        git_output(&f.repo, &["rev-parse", "--verify", &reference])?,
        candidate
    );
    git(
        &f.repo,
        &["cat-file", "-e", &format!("{candidate}^{{commit}}")],
    )?;
    assert_eq!(git_output(&f.repo, &["rev-parse", "HEAD"])?, main_head);
    assert_eq!(
        git_output(&f.repo, &["status", "--porcelain"])?,
        main_status
    );
    assert_eq!(fs::read_to_string(f.repo.join("input"))?, "before\n");
    Ok(())
}

#[tokio::test]
async fn race_repair_worktree_cleanup_preserves_exclusive_ownership() -> TestResult {
    use std::{future::Future, future::poll_fn, sync::Arc, task::Poll, time::Duration};
    use tokio::sync::{Barrier, oneshot};

    let f = Fixture::new().await?;
    let main_head = git_output(&f.repo, &["rev-parse", "HEAD"])?;
    let main_status = git_output(&f.repo, &["status", "--porcelain"])?;
    let main_input = fs::read(f.repo.join("input"))?;
    let other = f.paths.worktree_dir.join("unregistered");
    git(
        &f.repo,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            "--",
            other.to_str().ok_or("other path")?,
        ],
    )?;
    let other_input = fs::read(other.join("input"))?;
    let other_gitdir = fs::read(other.join(".git"))?;
    let other_admin = PathBuf::from(
        fs::read_to_string(other.join(".git"))?
            .trim()
            .strip_prefix("gitdir: ")
            .ok_or("other gitdir")?,
    );
    let other_backref = fs::read(other_admin.join("gitdir"))?;
    fs::write(f.worktree.join("input"), "after\n")?;
    git(&f.worktree, &["add", "--", "input"])?;
    git(&f.worktree, &["commit", "--quiet", "-m", "race candidate"])?;
    let candidate = git_output(&f.worktree, &["rev-parse", "HEAD"])?;
    assert_ne!(candidate, main_head);
    f.register(true).await?;
    let admin = f.admin()?;
    f.terminal("succeeded").await?;
    f.release().await?;
    let registered_input = fs::read(f.worktree.join("input"))?;
    let registered_gitdir = fs::read(f.worktree.join(".git"))?;
    let registered_backref = fs::read(admin.join("gitdir"))?;
    let released_at: String = sqlx::query_scalar("SELECT released_at FROM repair_worktrees")
        .fetch_one(f.database.pool())
        .await?;

    // Open independent pools before taking the writer, so migrations cannot contend.
    let second = Database::open(&f.paths.database).await?;
    let third = Database::open(&f.paths.database).await?;
    let mut writer = f.database.pool().begin().await?;
    sqlx::query("UPDATE projects SET name = name")
        .execute(&mut *writer)
        .await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    let mut pending_signals = Vec::new();
    for database in [second, third] {
        let barrier = Arc::clone(&barrier);
        let paths = f.paths.clone();
        let recovery = f.recovery;
        let (pending_tx, pending_rx) = oneshot::channel();
        pending_signals.push(pending_rx);
        workers.push(async move {
            barrier.wait().await;
            let repository = database.repair_worktrees();
            let cleanup = repository.cleanup_repair_worktree(recovery, &paths);
            tokio::pin!(cleanup);
            let mut pending_tx = Some(pending_tx);
            poll_fn(|cx| {
                let result = cleanup.as_mut().poll(cx);
                if matches!(result, Poll::Pending)
                    && let Some(signal) = pending_tx.take()
                {
                    let _ = signal.send(());
                }
                result
            })
            .await
        });
    }
    let coordinator = async {
        barrier.wait().await;
        for signal in pending_signals {
            signal.await?;
        }
        assert_eq!(fs::read(f.worktree.join("input"))?, registered_input);
        assert_eq!(fs::read(f.worktree.join(".git"))?, registered_gitdir);
        assert_eq!(fs::read(admin.join("gitdir"))?, registered_backref);
        assert_eq!(fs::read(f.repo.join("input"))?, main_input);
        writer.commit().await?;
        Ok::<(), Box<dyn Error>>(())
    };
    let (first, second, coordinated) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(workers.remove(0), workers.remove(0), coordinator)
    })
    .await?;
    coordinated?;
    // Either legal bool split is fine; durable effects determine correctness.
    first?;
    second?;

    assert_eq!(f.state().await?, "removed");
    for removed in [&f.worktree, &admin] {
        assert_eq!(
            fs::symlink_metadata(removed)
                .err()
                .ok_or("registered worktree path still exists")?
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }
    assert_eq!(fs::read(f.repo.join("input"))?, main_input);
    assert_eq!(git_output(&f.repo, &["rev-parse", "HEAD"])?, main_head);
    assert_eq!(
        git_output(&f.repo, &["status", "--porcelain"])?,
        main_status
    );
    assert_eq!(fs::read(other.join("input"))?, other_input);
    assert_eq!(fs::read(other.join(".git"))?, other_gitdir);
    assert_eq!(fs::read(other_admin.join("gitdir"))?, other_backref);
    assert_eq!(git_output(&other, &["rev-parse", "HEAD"])?, main_head);
    let reference = format!("refs/igor/repairs/{}", f.recovery);
    assert_eq!(
        git_output(
            &f.repo,
            &["rev-parse", "--verify", &format!("{reference}^{{commit}}")]
        )?,
        candidate
    );
    let row =
        sqlx::query("SELECT patch, patch_sha256, records_json, released_at FROM repair_worktrees")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(row.get::<Vec<u8>, _>("patch"), evidence().patch);
    assert_eq!(
        row.get::<String, _>("patch_sha256"),
        format!("{:x}", Sha256::digest(evidence().patch))
    );
    assert_eq!(row.get::<String, _>("released_at"), released_at);
    let records: serde_json::Value = serde_json::from_str(row.get("records_json"))?;
    assert_eq!(records["diagnosis"], evidence().diagnosis);
    assert_eq!(records["validation_results"], evidence().validation_results);
    assert_eq!(records["changed_paths"], json!(["input"]));
    assert_eq!(
        records["candidate"],
        json!({"schema_version":1, "revision":candidate, "preserved_ref":reference})
    );
    let subject = f.recovery.to_string();
    let tombstone = f
        .database
        .cleanup_tombstone("repair_worktree_retention", &subject)
        .await?
        .ok_or("tombstone")?;
    assert_eq!(tombstone["outcome"], "removed");
    let event_time: String =
        sqlx::query_scalar("SELECT occurred_at FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    for replay in [false, true] {
        if replay {
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await?;
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM cleanup_tombstones")
                .fetch_one(f.database.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'"
            )
            .fetch_one(f.database.pool())
            .await?,
            1
        );
        assert_eq!(
            f.database
                .cleanup_tombstone("repair_worktree_retention", &subject)
                .await?,
            Some(tombstone.clone())
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT occurred_at FROM events WHERE kind = 'cleanup_state_changed'"
            )
            .fetch_one(f.database.pool())
            .await?,
            event_time
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT released_at FROM repair_worktrees")
                .fetch_one(f.database.pool())
                .await?,
            released_at
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_or_changed_protected_ref_blocks_cleanup_including_replay() -> TestResult {
    for cleaning in [false, true] {
        for missing in [false, true] {
            let f = Fixture::new().await?;
            f.register(true).await?;
            f.terminal("failed").await?;
            f.release().await?;
            if cleaning {
                sqlx::query("UPDATE repair_worktrees SET state = 'cleaning'")
                    .execute(f.database.pool())
                    .await?;
            }
            let reference = format!("refs/igor/repairs/{}", f.recovery);
            if missing {
                git(&f.repo, &["update-ref", "-d", &reference])?;
            } else {
                git(
                    &f.worktree,
                    &["commit", "--allow-empty", "--quiet", "-m", "different"],
                )?;
                let other = git_output(&f.worktree, &["rev-parse", "HEAD"])?;
                git(&f.repo, &["update-ref", &reference, &other])?;
            }
            assert!(
                f.database
                    .repair_worktrees()
                    .cleanup_repair_worktree(f.recovery, &f.paths)
                    .await
                    .is_err()
            );
            assert!(f.worktree.join("input").exists());
            assert!(f.admin()?.exists());
            assert_eq!(
                f.state().await?,
                if cleaning { "cleaning" } else { "released" }
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn release_never_overwrites_a_different_protected_ref_and_accepts_same_commit() -> TestResult
{
    let f = Fixture::new().await?;
    f.register(true).await?;
    f.terminal("failed").await?;
    let reference = format!("refs/igor/repairs/{}", f.recovery);
    let original = git_output(&f.repo, &["rev-parse", "HEAD"])?;
    git(&f.repo, &["update-ref", &reference, &original])?;
    git(
        &f.worktree,
        &["commit", "--allow-empty", "--quiet", "-m", "candidate"],
    )?;
    assert!(f.release().await.is_err());
    assert_eq!(f.state().await?, "registered");
    assert_eq!(git_output(&f.repo, &["rev-parse", &reference])?, original);
    let candidate = git_output(&f.worktree, &["rev-parse", "HEAD"])?;
    git(&f.repo, &["update-ref", &reference, &candidate])?;
    f.release().await?;
    assert_eq!(f.state().await?, "released");
    Ok(())
}

#[tokio::test]
async fn registration_rejects_symlinks_foreign_and_main_checkouts() -> TestResult {
    for case in ["leaf", "ancestor", "foreign", "main", "admin"] {
        let mut f = Fixture::new().await?;
        match case {
            "leaf" => {
                let moved = f.temporary.path().join("moved");
                fs::rename(&f.worktree, &moved)?;
                symlink(moved, &f.worktree)?;
            }
            "ancestor" => {
                let link = f.temporary.path().join("link");
                symlink(&f.paths.worktree_dir, &link)?;
                f.paths.worktree_dir = link;
            }
            "foreign" => {
                let foreign = Fixture::new().await?;
                sqlx::query("UPDATE projects SET root_path = ?")
                    .bind(foreign.repo.to_str().ok_or("foreign path")?)
                    .execute(f.database.pool())
                    .await?;
                assert!(f.register(true).await.is_err());
                continue;
            }
            "main" => {
                git(
                    &f.repo,
                    &[
                        "worktree",
                        "remove",
                        "--force",
                        "--",
                        f.worktree.to_str().ok_or("worktree path")?,
                    ],
                )?;
                fs::rename(&f.repo, &f.worktree)?;
                sqlx::query("UPDATE projects SET root_path = ?")
                    .bind(f.worktree.to_str().ok_or("main path")?)
                    .execute(f.database.pool())
                    .await?;
            }
            _ => {
                let admin = f.admin()?;
                let moved = f.temporary.path().join("admin");
                fs::rename(&admin, &moved)?;
                symlink(moved, admin)?;
            }
        }
        assert!(f.register(true).await.is_err(), "{case}");
        assert!(f.worktree.join("input").is_file() || case == "ancestor");
    }
    Ok(())
}

async fn metadata_no_completion(f: &Fixture) -> TestResult {
    assert!(
        f.database
            .cleanup_tombstone("repair_worktree_retention", &f.recovery.to_string())
            .await?
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'"
        )
        .fetch_one(f.database.pool())
        .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn metadata_leaf_symlinks_with_exact_valid_contents_are_rejected() -> TestResult {
    for leaf in [".git", "gitdir", "commondir"] {
        for released in [false, true] {
            let f = Fixture::new().await?;
            let admin = f.admin()?;
            if released {
                f.register(true).await?;
                f.terminal("failed").await?;
                f.release().await?;
            }
            let target = if leaf == ".git" {
                f.worktree.join(leaf)
            } else {
                admin.join(leaf)
            };
            let bytes = fs::read(&target)?;
            let external = f.temporary.path().join("external-metadata");
            let sentinel = f.temporary.path().join("sentinel");
            fs::write(&external, &bytes)?;
            fs::write(&sentinel, b"external sentinel")?;
            fs::remove_file(&target)?;
            symlink(&external, &target)?;
            let refs = git_output(&f.repo, &["show-ref"])?;
            if released {
                assert!(
                    f.database
                        .repair_worktrees()
                        .cleanup_repair_worktree(f.recovery, &f.paths)
                        .await
                        .is_err(),
                    "{leaf} cleanup"
                );
                assert_eq!(f.state().await?, "released");
            } else {
                assert!(f.register(true).await.is_err(), "{leaf} registration");
                assert_eq!(
                    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM repair_worktrees")
                        .fetch_one(f.database.pool())
                        .await?,
                    0
                );
            }
            assert_eq!(fs::read_link(&target)?, external);
            assert_eq!(fs::read(&target)?, bytes);
            assert_eq!(fs::read(&external)?, bytes);
            assert_eq!(fs::read(&sentinel)?, b"external sentinel");
            assert_eq!(fs::read(f.worktree.join("input"))?, b"before\n");
            assert!(admin.is_dir());
            assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
            metadata_no_completion(&f).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn path_escape_foreign_admin_and_backref_preserve_both_repositories() -> TestResult {
    for leaf in [".git", "gitdir"] {
        for phase in ["register", "candidate", "cleanup"] {
            let f = Fixture::new().await?;
            let foreign = Fixture::new().await?;
            let admin = f.admin()?;
            let foreign_admin = foreign.admin()?;
            if phase != "register" {
                f.register(true).await?;
                f.terminal("failed").await?;
                if phase == "cleanup" {
                    f.release().await?;
                }
            }
            let target = if leaf == ".git" {
                f.worktree.join(leaf)
            } else {
                admin.join(leaf)
            };
            let replacement = if leaf == ".git" {
                format!("gitdir: {}\n", foreign_admin.display())
            } else {
                format!("{}\n", foreign.worktree.join(".git").display())
            };
            fs::write(&target, &replacement)?;
            let refs = git_output(&f.repo, &["show-ref"])?;
            let foreign_refs = git_output(&foreign.repo, &["show-ref"])?;
            let foreign_git = fs::read(foreign.worktree.join(".git"))?;
            let foreign_backref = fs::read(foreign_admin.join("gitdir"))?;
            match phase {
                "register" => assert!(f.register(true).await.is_err()),
                "candidate" => {
                    let mut e = evidence();
                    e.candidate = Some(json!({"schema_version":1,"revision":"HEAD"}));
                    assert!(
                        f.database
                            .repair_worktrees()
                            .release_repair_worktree(f.recovery, &e)
                            .await
                            .is_err()
                    );
                    assert_eq!(f.state().await?, "registered");
                }
                _ => {
                    assert!(
                        f.database
                            .repair_worktrees()
                            .cleanup_repair_worktree(f.recovery, &f.paths)
                            .await
                            .is_err()
                    );
                    assert_eq!(f.state().await?, "released");
                }
            }
            assert_eq!(fs::read_to_string(&target)?, replacement);
            for tree in [&f.worktree, &foreign.worktree, &f.repo, &foreign.repo] {
                assert_eq!(fs::read(tree.join("input"))?, b"before\n");
            }
            assert!(admin.is_dir() && foreign_admin.is_dir());
            assert_eq!(fs::read(foreign.worktree.join(".git"))?, foreign_git);
            assert_eq!(fs::read(foreign_admin.join("gitdir"))?, foreign_backref);
            assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
            assert_eq!(git_output(&foreign.repo, &["show-ref"])?, foreign_refs);
            metadata_no_completion(&f).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn path_escape_dot_spelled_metadata_and_root_are_lexically_rejected() -> TestResult {
    for case in [
        "admin",
        "root",
        "backref",
        "absolute_common",
        "relative_common",
    ] {
        for released in [false, true] {
            let mut f = Fixture::new().await?;
            let admin = f.admin()?;
            if released {
                f.register(true).await?;
                f.terminal("failed").await?;
                f.release().await?;
            }
            let target = match case {
                "admin" => f.worktree.join(".git"),
                "backref" => admin.join("gitdir"),
                _ => admin.join("commondir"),
            };
            let replacement = match case {
                "admin" => format!(
                    "gitdir: {}/./{}\n",
                    admin.parent().ok_or("parent")?.display(),
                    admin.file_name().ok_or("leaf")?.to_str().ok_or("utf8")?
                ),
                "backref" => format!("{}/./.git\n", f.worktree.display()),
                "absolute_common" => format!("{}/./.git\n", f.repo.display()),
                "relative_common" => "..//..\n".to_owned(),
                _ => {
                    f.paths.worktree_dir =
                        PathBuf::from(format!("{}/./worktrees", f.temporary.path().display()));
                    String::new()
                }
            };
            if case != "root" {
                fs::write(&target, &replacement)?;
            }
            let refs = git_output(&f.repo, &["show-ref"])?;
            if released {
                assert!(
                    f.database
                        .repair_worktrees()
                        .cleanup_repair_worktree(f.recovery, &f.paths)
                        .await
                        .is_err(),
                    "{case} cleanup"
                );
                assert_eq!(f.state().await?, "released");
            } else {
                assert!(f.register(true).await.is_err(), "{case} registration");
            }
            assert_eq!(fs::read(f.worktree.join("input"))?, b"before\n");
            assert!(admin.is_dir());
            if case != "root" {
                assert_eq!(fs::read_to_string(&target)?, replacement);
            }
            assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
            metadata_no_completion(&f).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn metadata_exact_absolute_common_and_ordinary_relative_common_remain_valid() -> TestResult {
    for absolute in [false, true] {
        let f = Fixture::new().await?;
        let admin = f.admin()?;
        if absolute {
            fs::write(
                admin.join("commondir"),
                format!("{}\n", f.repo.join(".git").display()),
            )?;
        } else {
            assert_eq!(fs::read(admin.join("commondir"))?, b"../..\n");
        }
        f.register(true).await?;
        f.terminal("failed").await?;
        f.release().await?;
        assert!(
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await?
        );
        assert_eq!(f.state().await?, "removed");
        assert!(!f.worktree.exists() && !admin.exists());
    }
    Ok(())
}

#[tokio::test]
async fn path_escape_missing_replay_rejects_symlink_parents_before_metadata_completion()
-> TestResult {
    use igor_core::{GcOperation, GcStatus};
    for parent in ["root", "common", "worktrees"] {
        let f = Fixture::new().await?;
        let admin = f.admin()?;
        f.register(true).await?;
        f.terminal("failed").await?;
        f.release().await?;
        sqlx::query("CREATE TRIGGER fail_cleanup_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'fixture crash'); END")
            .execute(f.database.pool()).await?;
        assert!(
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await
                .is_err()
        );
        assert!(!f.worktree.exists() && !admin.exists());
        assert_eq!(f.state().await?, "cleaning");
        sqlx::query("DROP TRIGGER fail_cleanup_event")
            .execute(f.database.pool())
            .await?;
        let intent: String = sqlx::query_scalar("SELECT released_at FROM repair_worktrees")
            .fetch_one(f.database.pool())
            .await?;
        let refs = git_output(&f.repo, &["show-ref"])?;
        let target = match parent {
            "root" => f.paths.worktree_dir.clone(),
            "common" => f.repo.join(".git"),
            _ => f.repo.join(".git/worktrees"),
        };
        let moved = f.temporary.path().join("original-parent");
        if target.exists() {
            fs::rename(&target, &moved)?;
        }
        let external = f.temporary.path().join("external-directory");
        fs::create_dir(&external)?;
        fs::write(external.join("sentinel"), b"external sentinel")?;
        symlink(&external, &target)?;
        assert!(!admin.exists());
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.temporary.path(), None).await?;
        let entry = plan
            .entries
            .iter()
            .find(|e| e.operation == GcOperation::RepairWorktrees)
            .ok_or("entry")?;
        assert_eq!(entry.status, GcStatus::Unsafe, "{parent}");
        assert!(
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await
                .is_err(),
            "{parent}"
        );
        assert_eq!(f.state().await?, "cleaning");
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT released_at FROM repair_worktrees")
                .fetch_one(f.database.pool())
                .await?,
            intent
        );
        assert_eq!(fs::read_link(&target)?, external);
        assert_eq!(fs::read(external.join("sentinel"))?, b"external sentinel");
        assert_eq!(fs::read_dir(&external)?.count(), 1);
        metadata_no_completion(&f).await?;
        fs::remove_file(&target)?;
        if moved.exists() {
            fs::rename(&moved, &target)?;
        }
        assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
    }
    Ok(())
}

#[tokio::test]
async fn cleanup_rejects_replaced_or_renamed_identity() -> TestResult {
    for case in ["repo", "common", "admin", "tree", "renamed"] {
        let f = Fixture::new().await?;
        f.register(true).await?;
        f.terminal("failed").await?;
        f.release().await?;
        let target = match case {
            "repo" => f.repo.clone(),
            "common" => f.repo.join(".git"),
            "admin" => f.admin()?,
            _ => f.worktree.clone(),
        };
        let moved = f.temporary.path().join("moved");
        if case == "renamed" {
            git(
                &f.repo,
                &[
                    "worktree",
                    "move",
                    "--",
                    f.worktree.to_str().ok_or("tree")?,
                    moved.to_str().ok_or("moved")?,
                ],
            )?;
        } else {
            fs::rename(&target, &moved)?;
            if case == "admin" {
                symlink(&moved, &target)?;
            } else {
                fs::create_dir(&target)?;
            }
        }
        assert!(
            f.database
                .repair_worktrees()
                .cleanup_repair_worktree(f.recovery, &f.paths)
                .await
                .is_err(),
            "{case}"
        );
        assert_eq!(f.state().await?, "released");
        assert!(moved.exists());
    }
    Ok(())
}

#[tokio::test]
async fn absent_tree_requires_prior_intent_and_replays_remaining_git_admin() -> TestResult {
    use igor_core::{GcOperation, GcStatus};
    let f = Fixture::new().await?;
    f.register(true).await?;
    let admin = f.admin()?;
    f.terminal("failed").await?;
    f.release().await?;
    let refs = git_output(&f.repo, &["show-ref"])?;
    let saved = gc_support::readonly_plan(&f.database, &f.paths, f.temporary.path(), None)
        .await?
        .worklist()?;
    let mut saved_subjects: Vec<String> = saved
        .selections
        .iter()
        .filter(|s| s.expected.operation == GcOperation::RepairWorktrees)
        .map(|s| s.expected.subject_id.clone())
        .collect();
    saved_subjects.sort();
    assert_eq!(saved_subjects.len(), 1, "{saved_subjects:?}");
    assert_eq!(
        saved
            .selections
            .iter()
            .find(|s| s.expected.operation == GcOperation::RepairWorktrees)
            .ok_or("saved repair selection")?
            .expected
            .status,
        GcStatus::Ready
    );
    // Simulate removal of the tree before Git removed its admin record.
    fs::remove_file(f.worktree.join("input"))?;
    fs::remove_file(f.worktree.join(".git"))?;
    fs::remove_dir(&f.worktree)?;
    // Fail closed and partial: the missing tree is never accepted for cleanup,
    // so the remaining Git admin record and the protected candidate ref stand.
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await
            .is_err()
    );
    assert_eq!(f.state().await?, "released");
    assert!(admin.is_dir());
    assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);

    // Restarting preserves the partial state and Git contract. The missing tree
    // cannot authorize cleanup without a durable cleaning intent.
    f.database.pool().close().await;
    let restarted = Database::open(&f.paths.database).await?;
    let state: String =
        sqlx::query_scalar("SELECT state FROM repair_worktrees WHERE recovery_id = ?")
            .bind(f.recovery.to_string())
            .fetch_one(restarted.pool())
            .await?;
    assert_eq!(state, "released");
    assert!(admin.is_dir());
    assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM repair_worktrees")
            .fetch_one(restarted.pool())
            .await?,
        1
    );
    let fresh = gc_support::readonly_plan(&restarted, &f.paths, f.temporary.path(), None).await?;
    let repairs: Vec<_> = fresh
        .entries
        .iter()
        .filter(|entry| entry.operation == GcOperation::RepairWorktrees)
        .collect();
    assert_eq!(repairs.len(), 1);
    assert_eq!(repairs[0].subject_id, saved_subjects[0]);
    assert_eq!(repairs[0].status, GcStatus::Unsafe);
    assert!(
        fresh
            .worklist()?
            .selections
            .iter()
            .all(|selection| { selection.expected.operation != GcOperation::RepairWorktrees })
    );

    sqlx::query("UPDATE repair_worktrees SET state = 'cleaning' WHERE recovery_id = ?")
        .bind(f.recovery.to_string())
        .execute(restarted.pool())
        .await?;
    assert!(
        restarted
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await?
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM repair_worktrees WHERE recovery_id = ?")
            .bind(f.recovery.to_string())
            .fetch_one(restarted.pool())
            .await?;
    assert_eq!(state, "removed");
    assert!(!admin.exists());
    // Resume removed only the registered Git admin; candidate refs are untouched.
    assert_eq!(git_output(&f.repo, &["show-ref"])?, refs);
    Ok(())
}

#[tokio::test]
async fn event_rollback_after_removal_replays_persisted_intent() -> TestResult {
    let f = Fixture::new().await?;
    f.register(true).await?;
    let admin = f.admin()?;
    f.terminal("cancelled").await?;
    f.release().await?;
    sqlx::query("CREATE TRIGGER fail_cleanup_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'fixture crash'); END")
        .execute(f.database.pool()).await?;
    assert!(
        f.database
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await
            .is_err()
    );
    assert!(!f.worktree.exists());
    assert!(!admin.exists());
    assert_eq!(f.state().await?, "cleaning");
    sqlx::query("DROP TRIGGER fail_cleanup_event")
        .execute(f.database.pool())
        .await?;
    f.database.pool().close().await;
    let restarted = Database::open(&f.paths.database).await?;
    assert!(
        restarted
            .repair_worktrees()
            .cleanup_repair_worktree(f.recovery, &f.paths)
            .await?
    );
    let state: String = sqlx::query_scalar("SELECT state FROM repair_worktrees")
        .fetch_one(restarted.pool())
        .await?;
    assert_eq!(state, "removed");
    let subject = f.recovery.to_string();
    let tombstone = restarted
        .cleanup_tombstone("repair_worktree_retention", &subject)
        .await?
        .ok_or("worktree tombstone")?;
    assert_eq!(tombstone["outcome"], "removed");
    restarted
        .repair_worktrees()
        .cleanup_repair_worktree(f.recovery, &f.paths)
        .await?;
    assert_eq!(
        restarted
            .cleanup_tombstone("repair_worktree_retention", &subject)
            .await?,
        Some(tombstone)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'"
        )
        .fetch_one(restarted.pool())
        .await?,
        1
    );
    Ok(())
}
