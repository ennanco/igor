use super::*;

async fn negative_replay_prepare_superseded(f: &CleanupFixture) -> TestResult {
    f.succeed().await?;
    f.reject_events().await?;
    assert!(f.cleanup().await.is_err());
    f.allow_events().await?;
    for path in f.copies.iter().flatten() {
        assert_eq!(
            fs::symlink_metadata(path)
                .err()
                .ok_or("artifact copy remains present")?
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM superseded_cleanup_intents WHERE state = 'prepared'",
    )
    .fetch_one(f.family.database.pool())
    .await?;
    assert_eq!(count, 2);
    Ok(())
}

async fn negative_replay_assert_superseded_rejected(f: &CleanupFixture) -> TestResult {
    let database = &f.family.database;
    let before = gc_support::sql_snapshot(database).await?;
    let disk = gc_support::files(f.family._database.path(), &f.paths.database)?;
    let repository = gc_support::files(f.family._repository.path(), &f.paths.database)?;
    let plan =
        gc_support::readonly_plan(database, &f.paths, f.family._database.path(), None).await?;
    let entries: Vec<_> = plan
        .entries
        .iter()
        .filter(|e| e.operation == igor_core::GcOperation::SupersededArtifacts)
        .collect();
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .all(|e| e.status == igor_core::GcStatus::Unsafe && e.targets.is_empty()),
        "{entries:?}"
    );
    assert!(f.cleanup().await.is_err());
    assert_eq!(gc_support::sql_snapshot(database).await?, before);
    assert_eq!(
        gc_support::files(f.family._database.path(), &f.paths.database)?,
        disk
    );
    assert_eq!(
        gc_support::files(f.family._repository.path(), &f.paths.database)?,
        repository
    );
    assert!(race_cleanup_events(database).await?.is_empty());
    for artifact in &f.artifacts {
        assert!(
            database
                .cleanup_tombstone("superseded_artifact_retention", &artifact.id.to_string())
                .await?
                .is_none()
        );
        assert_eq!(
            database
                .artifacts()
                .get(artifact.id)
                .await?
                .ok_or("artifact")?
                .lifecycle,
            igor_core::ArtifactLifecycleState::Superseded
        );
    }
    Ok(())
}

#[tokio::test]
async fn negative_replay_superseded_absent_publication_rejects_linked_root_and_ancestor()
-> TestResult {
    use std::os::unix::fs::MetadataExt;
    for above_root in [false, true] {
        let f = CleanupFixture::new(delete_policy()).await?;
        negative_replay_prepare_superseded(&f).await?;
        let publication = f.copies[0][2]
            .parent()
            .ok_or("attempt")?
            .parent()
            .ok_or("publication")?;
        let link = if above_root {
            publication.parent().ok_or("project")?
        } else {
            publication
        };
        let external = TempDir::new()?;
        fs::write(
            external.path().join("sentinel"),
            b"external publication bytes",
        )?;
        fs::rename(link, link.with_extension("saved"))?;
        std::os::unix::fs::symlink(external.path(), link)?;
        let before = fs::symlink_metadata(link)?;
        assert!(before.file_type().is_symlink());
        let disk = gc_support::files(external.path(), &f.paths.database)?;
        assert_eq!(disk.len(), 2, "root and one live sentinel");
        for copies in &f.copies {
            assert_eq!(
                fs::symlink_metadata(&copies[2])
                    .err()
                    .ok_or("published copy is not absent")?
                    .kind(),
                std::io::ErrorKind::NotFound
            );
        }
        negative_replay_assert_superseded_rejected(&f).await?;
        let after = fs::symlink_metadata(link)?;
        assert!(after.file_type().is_symlink());
        assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
        assert_eq!(fs::read_link(link)?, external.path());
        assert_eq!(gc_support::files(external.path(), &f.paths.database)?, disk);
        assert_eq!(
            fs::read(
                link.with_extension("saved")
                    .join(if above_root { "artifacts" } else { "" })
                    .join(f.attempt().to_string())
                    .join("unrelated")
            )?,
            b"other"
        );
    }
    Ok(())
}

#[tokio::test]
async fn negative_replay_superseded_missing_linked_copy_rejects_whole_restored_batch() -> TestResult
{
    use std::os::unix::fs::MetadataExt;
    for source in [false, true] {
        let f = CleanupFixture::new(delete_policy()).await?;
        negative_replay_prepare_superseded(&f).await?;
        // Select the nested artifact so its source parent is exclusive to it.
        let missing = 1;
        let restored = 0;
        let link = if source {
            f.copies[missing][0]
                .parent()
                .ok_or("nested source")?
                .to_path_buf()
        } else {
            // Give the other artifact's stage copy a real counterpart through
            // the link; only the missing artifact lacks its final leaf.
            f.copies[missing][1].parent().ok_or("stage")?.to_path_buf()
        };
        let external = TempDir::new()?;
        fs::write(
            external.path().join("sentinel"),
            b"external restored batch bytes",
        )?;
        fs::rename(&link, link.with_extension("saved"))?;
        std::os::unix::fs::symlink(external.path(), &link)?;
        for path in &f.copies[restored] {
            fs::write(path, b"preserved model")?;
        }
        let metadata = fs::symlink_metadata(&link)?;
        assert!(metadata.file_type().is_symlink());
        let disk = gc_support::files(external.path(), &f.paths.database)?;
        assert_eq!(disk.len(), if source { 2 } else { 3 });
        assert_eq!(
            fs::symlink_metadata(&f.copies[missing][usize::from(!source)])
                .err()
                .ok_or("artifact leaf is not absent")?
                .kind(),
            std::io::ErrorKind::NotFound
        );
        negative_replay_assert_superseded_rejected(&f).await?;
        for path in &f.copies[restored] {
            assert_eq!(fs::read(path)?, b"preserved model");
        }
        let after = fs::symlink_metadata(&link)?;
        assert!(after.file_type().is_symlink());
        assert_eq!((after.dev(), after.ino()), (metadata.dev(), metadata.ino()));
        assert_eq!(fs::read_link(&link)?, external.path());
        assert_eq!(gc_support::files(external.path(), &f.paths.database)?, disk);
        race_assert_unregistered_and_streams(&f)?;
    }
    Ok(())
}

// Keep the external writer alive until both real operations have started and
// yielded. The timeout is only a watchdog; the barrier/oneshots drive handoff.
async fn race_behind_writer<A, B>(
    database_path: &std::path::Path,
    first: A,
    second: B,
) -> Result<(A::Output, B::Output), Box<dyn Error>>
where
    A: std::future::Future,
    B: std::future::Future,
{
    let external = Database::open(database_path).await?;
    let mut writer = external.pool().begin().await?;
    sqlx::query("UPDATE projects SET name = name")
        .execute(&mut *writer)
        .await?;
    let barrier = tokio::sync::Barrier::new(3);
    let (first_started, first_pending) = tokio::sync::oneshot::channel();
    let (second_started, second_pending) = tokio::sync::oneshot::channel();
    let first = race_poll_pending(&barrier, first_started, first);
    let second = race_poll_pending(&barrier, second_started, second);
    let release = async {
        barrier.wait().await;
        first_pending.await?;
        second_pending.await?;
        writer.commit().await?;
        Ok::<_, Box<dyn Error>>(())
    };
    let (first, second, released) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(biased; first, second, release)
    })
    .await?;
    released?;
    Ok((first, second))
}

async fn race_poll_pending<F: std::future::Future>(
    barrier: &tokio::sync::Barrier,
    started: tokio::sync::oneshot::Sender<()>,
    future: F,
) -> F::Output {
    barrier.wait().await;
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "operation finished before writer handoff"
        );
        std::task::Poll::Ready(())
    })
    .await;
    let _ = started.send(());
    future.await
}

fn race_assert_unregistered_and_streams(f: &CleanupFixture) -> TestResult {
    let logs = f.paths.log_dir.join(f.attempt().to_string());
    assert_eq!(fs::read(logs.join("stdout.log"))?, b"stdout");
    assert_eq!(fs::read(logs.join("stderr.log"))?, b"stderr");
    assert_eq!(
        fs::read(
            f.copies[0][2]
                .parent()
                .ok_or("publication")?
                .join("unrelated")
        )?,
        b"other"
    );
    Ok(())
}

type RaceCleanupEventRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);

async fn race_cleanup_events(database: &Database) -> Result<Vec<RaceCleanupEventRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT sequence, id, project_id, job_id, attempt_id, kind, payload_json, occurred_at
         FROM events WHERE kind = 'cleanup_state_changed' ORDER BY sequence",
    )
    .fetch_all(database.pool())
    .await
}

fn race_assert_removed_events(
    f: &CleanupFixture,
    events: &[RaceCleanupEventRow],
    removed: bool,
) -> TestResult {
    let mut actual = Vec::new();
    for (_, _, project_id, job_id, attempt_id, kind, payload, _) in events {
        assert_eq!(project_id, &f.family.prepared.family.project_id.to_string());
        assert_eq!(
            job_id.as_deref(),
            Some(f.family.prepared.submissions[0].job.id.to_string().as_str())
        );
        assert_eq!(
            attempt_id.as_deref(),
            Some(f.attempt().to_string().as_str())
        );
        assert_eq!(kind, "cleanup_state_changed");
        let payload: serde_json::Value = serde_json::from_str(payload)?;
        let artifact_id = payload["data"]["artifact_id"]
            .as_str()
            .ok_or("event artifact_id")?;
        assert_eq!(
            payload["data"],
            json!({"artifact_id":artifact_id,"attempt_id":f.attempt(),
                "reason":"superseded_artifact_retention","from":"superseded","state":"removed"})
        );
        actual.push(artifact_id.to_owned());
    }
    actual.sort();
    let mut expected: Vec<String> = if removed {
        f.artifacts.iter().map(|a| a.id.to_string()).collect()
    } else {
        Vec::new()
    };
    expected.sort();
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn race_publication_late_extraction_versus_superseded_cleanup() -> TestResult {
    for publication_first in [false, true] {
        let f = CleanupFixture::new(delete_policy()).await?;
        let database = &f.family.database;
        // Group publication has prepared the final files, but the DB has not
        // committed extraction yet. The owning old job is already superseded.
        sqlx::query("UPDATE artifacts SET state = 'staged', lifecycle_state = 'running' WHERE attempt_id = ?")
            .bind(f.attempt().to_string()).execute(database.pool()).await?;
        let attempt = &f.family.prepared.submissions[0].attempt;
        let frozen = serde_json::to_value(attempt)?;
        let action = ActionRecord {
            id: ActionId::new(),
            project_id: f.family.prepared.family.project_id,
            kind: "extract_metrics".into(),
            state: ActionState::Pending,
            spec: json!({"version":1,"extractor":frozen["result"]["extractor"]}),
            idempotency_key: format!("extract:{}", f.attempt()),
        };
        database.actions().insert(&action).await?;
        sqlx::query("UPDATE actions SET job_id = ?, attempt_id = ? WHERE id = ?")
            .bind(f.family.prepared.submissions[0].job.id.to_string())
            .bind(f.attempt().to_string())
            .bind(action.id.to_string())
            .execute(database.pool())
            .await?;
        let claim = database
            .actions()
            .claim_extraction("race-publication", Duration::from_secs(120))
            .await?
            .ok_or("extraction claim")?;
        let old_state: String = sqlx::query_scalar("SELECT state FROM jobs WHERE id = ?")
            .bind(f.family.prepared.submissions[0].job.id.to_string())
            .fetch_one(database.pool())
            .await?;
        assert_eq!(old_state, "superseded");
        for artifact in &f.artifacts {
            let staged = database
                .artifacts()
                .get(artifact.id)
                .await?
                .ok_or("staged artifact")?;
            assert_eq!(staged.state, igor_core::ArtifactPublicationState::Staged);
            assert_eq!(staged.lifecycle, igor_core::ArtifactLifecycleState::Running);
            assert_eq!(staged.metadata, artifact.metadata);
            assert!(
                !database
                    .artifacts()
                    .replacement_generation_succeeded(artifact.id)
                    .await?
            );
        }
        f.assert_present()?;
        let publisher = Database::open(&f.paths.database).await?;
        let cleaner = Database::open(&f.paths.database).await?;
        let document = json!({"schema_version":1,"status":"success","metrics":{},
            "artifacts":[{"path":"model.bin","role":"model"},
                         {"path":"nested/evidence.bin","role":"model"}]});
        let actions = publisher.actions();
        let artifacts = cleaner.artifacts();
        let publication = actions.complete_extraction(&claim, f.attempt(), &document);
        let cleanup = artifacts.cleanup_superseded_artifacts(f.attempt(), &f.paths);
        let (published, cleaned) = if publication_first {
            race_behind_writer(&f.paths.database, publication, cleanup).await?
        } else {
            let (cleaned, published) =
                race_behind_writer(&f.paths.database, cleanup, publication).await?;
            (published, cleaned)
        };
        published?;
        assert_eq!(cleaned?, 0);
        f.assert_present()?;
        let state: String = sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
            .bind(action.id.to_string())
            .fetch_one(database.pool())
            .await?;
        assert_eq!(state, "succeeded");
        let stored: String =
            sqlx::query_scalar("SELECT document_json FROM metric_documents WHERE attempt_id = ?")
                .bind(f.attempt().to_string())
                .fetch_one(database.pool())
                .await?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored)?,
            document
        );
        for artifact in &f.artifacts {
            let published = database
                .artifacts()
                .get(artifact.id)
                .await?
                .ok_or("artifact")?;
            assert_eq!(
                published.state,
                igor_core::ArtifactPublicationState::Published
            );
            assert_eq!(
                published.lifecycle,
                igor_core::ArtifactLifecycleState::Superseded
            );
            assert!(
                database
                    .cleanup_tombstone("superseded_artifact_retention", &artifact.id.to_string())
                    .await?
                    .is_none()
            );
        }
        race_assert_removed_events(&f, &race_cleanup_events(database).await?, false)?;
        f.succeed().await?;
        assert_eq!(f.cleanup().await?, 2);
        let events = race_cleanup_events(database).await?;
        race_assert_removed_events(&f, &events, true)?;
        assert_eq!(f.cleanup().await?, 0);
        assert_eq!(race_cleanup_events(database).await?, events);
        assert!(f.copies.iter().flatten().all(|p| !p.exists()));
        race_assert_unregistered_and_streams(&f)?;
        for artifact in &f.artifacts {
            let tombstone = database
                .cleanup_tombstone("superseded_artifact_retention", &artifact.id.to_string())
                .await?
                .ok_or("removed tombstone")?;
            assert_eq!(tombstone["outcome"], "removed");
        }
    }
    Ok(())
}

#[tokio::test]
async fn race_replacement_family_supersede_versus_confirmed_gc() -> TestResult {
    for replacement_first in [false, true] {
        let f = CleanupFixture::new(delete_policy()).await?;
        f.succeed().await?;
        let mut worklist = f
            .family
            .database
            .gc_plan(None, &f.paths)
            .await?
            .worklist()?;
        worklist
            .selections
            .retain(|s| s.expected.operation == igor_core::GcOperation::SupersededArtifacts);
        assert_eq!(worklist.selections.len(), 2);
        // Build a real G3 from the project's newly committed revision.
        let root = f.family._repository.path().join("project");
        fs::write(root.join("train.py"), "print('race third')\n")?;
        git(&root, &["add", "train.py"])?;
        git(&root, &["commit", "-qm", "race third"])?;
        let loaded = load_project_config(&root.join(".igor/project.toml"))?;
        let project = Project {
            id: f.family.prepared.family.project_id,
            name: "family-project".into(),
            root: root.clone(),
            config_path: loaded.config_file,
        };
        let third = build_family_generation(
            &project,
            &loaded.config,
            load_family_file(&root.join("family.toml"))?,
        )?
        .for_existing_family(f.family.prepared.family.clone(), 3)?;
        third.validate()?;
        let producer = Database::open(&f.paths.database).await?;
        let cleaner = Database::open(&f.paths.database).await?;
        let families = producer.families();
        let supersede = families.supersede(f.replacement.generation.identity.id, &third);
        let gc = cleaner.gc_execute(None, false, worklist.clone(), &f.paths);
        let (produced, result) = if replacement_first {
            race_behind_writer(&f.paths.database, supersede, gc).await?
        } else {
            let (result, produced) = race_behind_writer(&f.paths.database, gc, supersede).await?;
            (produced, result)
        };
        produced?;
        let result = result?;
        assert!(result.completed == 0 || result.completed == 2, "{result:?}");
        assert_eq!(result.deferred, 2 - result.completed, "{result:?}");
        let database = &f.family.database;
        let current: String = sqlx::query_scalar("SELECT id FROM generations WHERE family_id = ? ORDER BY generation_number DESC LIMIT 1")
            .bind(f.family.prepared.family.id.to_string()).fetch_one(database.pool()).await?;
        assert_eq!(current, third.generation.identity.id.to_string());
        let detail = database
            .families()
            .detail(f.family.prepared.family.id)
            .await?
            .ok_or("family detail")?;
        assert_eq!(
            detail.current_generation_id,
            Some(third.generation.identity.id)
        );
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs j JOIN attempts a ON a.job_id = j.id WHERE j.generation_id = ? AND j.state = 'queued' AND a.state = 'pending'",
        )
        .bind(third.generation.identity.id.to_string())
        .fetch_one(database.pool())
        .await?;
        assert_eq!(pending as usize, third.submissions.len());
        let mut tombstones = vec![];
        for (artifact, copies) in f.artifacts.iter().zip(&f.copies) {
            assert!(
                !database
                    .artifacts()
                    .replacement_generation_succeeded(artifact.id)
                    .await?
            );
            let tombstone = database
                .cleanup_tombstone("superseded_artifact_retention", &artifact.id.to_string())
                .await?;
            let record = database
                .artifacts()
                .get(artifact.id)
                .await?
                .ok_or("artifact")?;
            if result.completed == 2 {
                assert!(copies.iter().all(|p| !p.exists()));
                assert_eq!(record.lifecycle, igor_core::ArtifactLifecycleState::Removed);
                assert_eq!(
                    tombstone.as_ref().ok_or("removed tombstone")?["outcome"],
                    "removed"
                );
            } else {
                assert_eq!(
                    record.lifecycle,
                    igor_core::ArtifactLifecycleState::Superseded
                );
                for path in copies {
                    assert_eq!(fs::read(path)?, b"preserved model");
                }
                assert!(tombstone.is_none());
            }
            tombstones.push(tombstone);
        }
        let events = race_cleanup_events(database).await?;
        race_assert_removed_events(&f, &events, result.completed == 2)?;
        // Replay the original consent under G3 pending: it cannot start unlinking.
        let replay = cleaner.gc_execute(None, false, worklist, &f.paths).await?;
        assert_eq!(replay.completed, 0, "{replay:?}");
        assert_eq!(replay.already_done, result.completed, "{replay:?}");
        assert_eq!(replay.deferred, result.deferred, "{replay:?}");
        for ((artifact, copies), tombstone) in f.artifacts.iter().zip(&f.copies).zip(tombstones) {
            assert_eq!(
                database
                    .cleanup_tombstone("superseded_artifact_retention", &artifact.id.to_string())
                    .await?,
                tombstone
            );
            if result.completed == 0 {
                for path in copies {
                    assert_eq!(fs::read(path)?, b"preserved model");
                }
            } else {
                assert!(copies.iter().all(|p| !p.exists()));
            }
        }
        let replay_events = race_cleanup_events(database).await?;
        assert_eq!(replay_events, events);
        race_assert_unregistered_and_streams(&f)?;
    }
    Ok(())
}

#[tokio::test]
async fn gc_execute_superseded_fixed_batch_preserves_65th_and_new_sorted_artifact() -> TestResult {
    let f = CleanupFixture::new(delete_policy()).await?;
    f.succeed().await?;
    sqlx::query("UPDATE artifacts SET lifecycle_state = 'retained'")
        .execute(f.family.database.pool())
        .await?;
    let mut pending = None;
    let mut extra_paths = vec![];
    for i in 0..=65u128 {
        let mut a = f.artifacts[0].clone();
        a.id = uuid::Uuid::from_u128(i);
        a.path = format!("fixed-{i}.bin");
        a.lifecycle = igor_core::ArtifactLifecycleState::Superseded;
        let copies = [
            f.paths.log_dir.join(f.attempt().to_string()).join(&a.path),
            f.copies[0][1].with_file_name(a.id.to_string()),
            f.copies[0][2].with_file_name(a.id.to_string()),
        ];
        for path in &copies {
            fs::write(path, b"preserved model")?;
        }
        a.metadata = json!({"staged_path": copies[1], "published_path": copies[2]});
        if i == 0 {
            pending = Some(a);
            extra_paths.extend(copies);
        } else {
            f.family.database.artifacts().insert(&a).await?;
            if i == 65 {
                extra_paths.extend(copies);
            }
        }
    }
    let mut worklist = f
        .family
        .database
        .gc_plan(None, &f.paths)
        .await?
        .worklist()?;
    worklist
        .selections
        .retain(|s| s.expected.operation == igor_core::GcOperation::SupersededArtifacts);
    assert_eq!(worklist.selections.len(), 64);
    f.family
        .database
        .artifacts()
        .insert(&pending.ok_or("new artifact")?)
        .await?;
    let result = f
        .family
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 64, "{result:?}");
    assert!(extra_paths.iter().all(|p| p.is_file()));
    Ok(())
}

#[tokio::test]
async fn gc_execute_superseded_mixed_retention_and_failed_only_exclusion() -> TestResult {
    let f = CleanupFixture::new(
        json!({"policy":"delete-after-replacement", "keep_on_success":["model.bin"]}),
    )
    .await?;
    f.succeed().await?;
    let failed = f
        .family
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?;
    assert!(failed.entries.is_empty());
    let mut worklist = f
        .family
        .database
        .gc_plan(None, &f.paths)
        .await?
        .worklist()?;
    worklist
        .selections
        .retain(|s| s.expected.operation == igor_core::GcOperation::SupersededArtifacts);
    assert_eq!(worklist.selections.len(), 2);
    assert!(
        f.family
            .database
            .gc_execute(None, true, worklist.clone(), &f.paths)
            .await
            .is_err()
    );
    let result = f
        .family
        .database
        .gc_execute(None, false, worklist.clone(), &f.paths)
        .await?;
    assert_eq!(result.completed, 2, "{result:?}");
    for (artifact, copies) in f.artifacts.iter().zip(&f.copies) {
        assert!(
            copies
                .iter()
                .all(|p| p.exists() == (artifact.path == "model.bin"))
        );
    }
    let replay = f
        .family
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(replay.already_done, 2);
    Ok(())
}

#[tokio::test]
async fn gc_execute_superseded_confirmed_plan_does_not_authorize_new_incomplete_generation()
-> TestResult {
    let f = CleanupFixture::new(delete_policy()).await?;
    f.succeed().await?;
    let mut worklist = f
        .family
        .database
        .gc_plan(None, &f.paths)
        .await?
        .worklist()?;
    worklist
        .selections
        .retain(|s| s.expected.operation == igor_core::GcOperation::SupersededArtifacts);
    // A new current generation invalidates replacement comparability after confirmation.
    sqlx::query("UPDATE jobs SET state = 'failed' WHERE generation_id = ?")
        .bind(f.replacement.generation.identity.id.to_string())
        .execute(f.family.database.pool())
        .await?;
    let result = f
        .family
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 0, "{result:?}");
    assert_eq!(result.deferred, 2);
    assert!(f.copies.iter().flatten().all(|p| p.is_file()));
    Ok(())
}

#[tokio::test]
async fn gc_superseded_preview_preflight_rolls_back_keeps_and_checks_hidden_intent() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    for case in ["corrupt", "missing", "hidden", "symlink", "stale_intent"] {
        let f = CleanupFixture::new(
            json!({"policy":"delete-after-replacement", "keep_on_success":["model.bin"]}),
        )
        .await?;
        f.succeed().await?;
        let removed = f
            .artifacts
            .iter()
            .position(|a| a.path != "model.bin")
            .ok_or("removed")?;
        match case {
            "corrupt" => fs::write(&f.copies[removed][2], b"corrupt")?,
            "missing" => fs::remove_file(&f.copies[removed][0])?,
            "symlink" => {
                fs::remove_file(&f.copies[removed][2])?;
                std::os::unix::fs::symlink(&f.copies[removed][0], &f.copies[removed][2])?;
            }
            "hidden" => {
                let publication = f.copies[removed][2]
                    .parent()
                    .ok_or("parent")?
                    .parent()
                    .ok_or("publication")?;
                let hidden = publication
                    .join(".igor-publication-staging")
                    .join(f.attempt().to_string())
                    .join(f.artifacts[removed].id.to_string());
                fs::create_dir_all(hidden.parent().ok_or("parent")?)?;
                fs::write(hidden, b"hidden")?;
            }
            "stale_intent" => {
                sqlx::query("INSERT INTO superseded_cleanup_intents(artifact_id, identity_json, replacement_generation_id) VALUES (?, '{}', ?)").bind(f.artifacts[removed].id.to_string()).bind(f.replacement.generation.identity.id.to_string()).execute(f.family.database.pool()).await?;
            }
            _ => unreachable!(),
        }
        let plan = gc_support::readonly_plan(
            &f.family.database,
            &f.paths,
            f.family._database.path(),
            None,
        )
        .await?;
        let entries: Vec<_> = plan
            .entries
            .iter()
            .filter(|e| e.operation == O::SupersededArtifacts)
            .collect();
        assert!(
            entries
                .iter()
                .all(|e| e.status == S::Unsafe && e.targets.is_empty()),
            "{case}"
        );
        assert!(f.cleanup().await.is_err());
        for artifact in &f.artifacts {
            let kept = f
                .family
                .database
                .artifacts()
                .get(artifact.id)
                .await?
                .ok_or("artifact")?;
            assert_eq!(
                kept.lifecycle,
                igor_core::ArtifactLifecycleState::Superseded
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn gc_superseded_preview_matches_deferred_keep_and_removal() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    let mut actual_results = vec![];
    let mut expected_results = vec![];
    for policy in [
        delete_policy(),
        json!({"policy":"delete-after-replacement", "keep_on_success":["model.bin"]}),
        json!({"policy":"keep", "keep_on_success":[]}),
        serde_json::Value::Null,
    ] {
        let f = CleanupFixture::new(policy.clone()).await?;
        let plan = gc_support::readonly_plan(
            &f.family.database,
            &f.paths,
            f.family._database.path(),
            None,
        )
        .await?;
        let entries: Vec<_> = plan
            .entries
            .iter()
            .filter(|e| e.operation == O::SupersededArtifacts)
            .collect();
        assert_eq!(entries.len(), 2);
        for artifact in &f.artifacts {
            let retained = policy.is_null()
                || policy["policy"] == "keep"
                || (policy["keep_on_success"] == json!(["model.bin"])
                    && artifact.path == "model.bin");
            assert_eq!(
                entries
                    .iter()
                    .find(|e| e.subject_id == artifact.id.to_string())
                    .ok_or("artifact entry")?
                    .status,
                if retained { S::Retain } else { S::Deferred }
            );
        }
        f.succeed().await?;
        let plan = gc_support::readonly_plan(
            &f.family.database,
            &f.paths,
            f.family._database.path(),
            None,
        )
        .await?;
        let entries: Vec<_> = plan
            .entries
            .iter()
            .filter(|e| e.operation == O::SupersededArtifacts)
            .collect();
        let mut removed = std::collections::BTreeMap::new();
        for (artifact, copies) in f.artifacts.iter().zip(&f.copies) {
            let retained = policy.is_null()
                || policy["policy"] == "keep"
                || (policy["keep_on_success"] == json!(["model.bin"])
                    && artifact.path == "model.bin");
            let entry = entries
                .iter()
                .find(|e| e.subject_id == artifact.id.to_string())
                .ok_or("artifact entry")?;
            assert_eq!(entry.status, if retained { S::Retain } else { S::Ready });
            let expected: std::collections::BTreeMap<_, _> = if retained {
                Default::default()
            } else {
                copies
                    .iter()
                    .map(|path| Ok((path.clone(), Some(fs::read(path)?.len() as u64))))
                    .collect::<Result<_, std::io::Error>>()?
            };
            assert_eq!(entry.targets.len(), expected.len());
            assert_eq!(
                entry
                    .targets
                    .iter()
                    .map(|t| (t.path.clone(), t.bytes))
                    .collect::<std::collections::BTreeMap<_, _>>(),
                expected
            );
            assert!(
                entry
                    .targets
                    .iter()
                    .all(|t| t.effect == igor_core::GcEffect::Remove)
            );
            removed.extend(expected);
        }
        assert_eq!(
            plan.known_copy_bytes,
            removed.values().flatten().sum::<u64>()
        );
        let disk = gc_support::files(f.family._database.path(), &f.paths.database)?;
        let repo = gc_support::files(f.family._repository.path(), &f.paths.database)?;
        let result = f
            .family
            .database
            .gc_execute(None, false, plan.worklist()?, &f.paths)
            .await?;
        for (root, before) in [
            (f.family._database.path(), disk),
            (f.family._repository.path(), repo),
        ] {
            assert_eq!(
                gc_support::files(root, &f.paths.database)?,
                before
                    .into_iter()
                    .filter(|(path, _, _)| !removed.contains_key(path))
                    .collect::<Vec<_>>()
            );
        }
        let (actual, expected) = superseded_preview_execution(&f, result, true);
        actual_results.push((policy.clone(), actual));
        expected_results.push((policy, expected));
    }
    assert_eq!(actual_results, expected_results);
    Ok(())
}

#[tokio::test]
async fn gc_superseded_preview_prepared_replay_respects_new_current_generation() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    for absent in [false, true] {
        let f = CleanupFixture::new(delete_policy()).await?;
        f.succeed().await?;
        f.reject_events().await?;
        assert!(f.cleanup().await.is_err());
        f.allow_events().await?;
        if !absent {
            for copies in &f.copies {
                for path in copies {
                    fs::write(path, b"preserved model")?;
                }
            }
        }
        f.third_pending().await?;
        let plan = gc_support::readonly_plan(
            &f.family.database,
            &f.paths,
            f.family._database.path(),
            None,
        )
        .await?;
        let entries: Vec<_> = plan
            .entries
            .iter()
            .filter(|e| e.operation == O::SupersededArtifacts)
            .collect();
        assert!(
            entries
                .iter()
                .all(|e| e.status == if absent { S::MetadataOnly } else { S::Deferred })
        );
        assert!(entries.iter().all(|e| e.targets.is_empty()));
        assert_eq!(plan.known_copy_bytes, 0);
        let disk = gc_support::files(f.family._database.path(), &f.paths.database)?;
        let repo = gc_support::files(f.family._repository.path(), &f.paths.database)?;
        let result = f
            .family
            .database
            .gc_execute(None, false, plan.worklist()?, &f.paths)
            .await?;
        assert_eq!(
            gc_support::files(f.family._database.path(), &f.paths.database)?,
            disk
        );
        assert_eq!(
            gc_support::files(f.family._repository.path(), &f.paths.database)?,
            repo
        );
        if !absent {
            f.assert_present()?;
        }
        let (actual, expected) = superseded_preview_execution(&f, result, absent);
        assert_eq!(actual, expected);
    }
    Ok(())
}

fn superseded_preview_execution(
    f: &CleanupFixture,
    mut actual: igor_core::GcExecution,
    artifacts_selected: bool,
) -> (igor_core::GcExecution, igor_core::GcExecution) {
    use igor_core::{GcExecution, GcExecutionEntry, GcExecutionStatus as X, GcOperation as O};
    let mut entries = vec![];
    if artifacts_selected {
        entries.extend(f.artifacts.iter().map(|a| GcExecutionEntry {
            operation: O::SupersededArtifacts,
            subject_id: a.id.to_string(),
            status: X::Completed,
            cause: None,
        }));
    }
    entries.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    actual
        .entries
        .sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    (
        actual,
        GcExecution {
            confirmed: true,
            completed: entries.len(),
            entries,
            ..Default::default()
        },
    )
}

struct CleanupFixture {
    family: FamilyFixture,
    paths: igor_core::RuntimePaths,
    copies: Vec<[std::path::PathBuf; 3]>,
    artifacts: Vec<ArtifactRecord>,
    replacement: PreparedFamily,
}

impl CleanupFixture {
    async fn new(snapshot: serde_json::Value) -> Result<Self, Box<dyn Error>> {
        let mut family = family_fixture().await?;
        let root = family._database.path();
        let paths = igor_core::RuntimePaths {
            config_file: root.join("config"),
            state_dir: root.join("state"),
            runtime_dir: root.join("runtime"),
            database: root.join("nested/igor.db"),
            log_dir: root.join("logs"),
            worktree_dir: root.join("worktrees"),
            report_dir: root.join("reports"),
            worker_socket: root.join("worker.sock"),
            supervisor_socket: root.join("supervisor.sock"),
            resource_dir: root.join("resources"),
        };
        let publication = family._repository.path().join("project/artifacts");
        fs::write(
            family._repository.path().join("project/.git/info/exclude"),
            "/artifacts/\n",
        )?;
        for submission in &mut family.prepared.submissions {
            let mut spec = serde_json::to_value(&submission.attempt)?;
            spec["result"]["publication_root"] = json!(publication);
            spec["result"]["extractor"] = json!({"program":"python3", "args":["extract.py"],
                "cwd":family._repository.path().join("project"), "shell":"direct",
                "environment":{"inherit":"none","set":{}}});
            if snapshot.is_null() {
                spec["result"]
                    .as_object_mut()
                    .ok_or("result")?
                    .remove("superseded_artifact_retention");
            } else {
                spec["result"]["superseded_artifact_retention"] = snapshot.clone();
            }
            submission.attempt = serde_json::from_value(spec)?;
        }
        let database = &family.database;
        database.families().submit(&family.prepared).await?;
        sqlx::query("UPDATE attempts SET state = 'succeeded'")
            .execute(database.pool())
            .await?;
        sqlx::query("UPDATE jobs SET state = 'succeeded'")
            .execute(database.pool())
            .await?;
        let attempt = family.prepared.submissions[0].attempt.id();
        let attempt_dir = paths.log_dir.join(attempt.to_string());
        sqlx::query("INSERT INTO attempt_processes (attempt_id, job_id, project_id, pid, process_group_id, process_start_ticks, stdout_path, stderr_path) VALUES (?, ?, ?, 1, 1, 1, ?, ?)")
            .bind(attempt.to_string())
            .bind(family.prepared.submissions[0].job.id.to_string())
            .bind(family.prepared.family.project_id.to_string())
            .bind(attempt_dir.join("stdout.log").to_str())
            .bind(attempt_dir.join("stderr.log").to_str())
            .execute(database.pool()).await?;
        let mut copies = Vec::new();
        let mut artifacts = Vec::new();
        for name in ["model.bin", "nested/evidence.bin"] {
            let id = uuid::Uuid::new_v4();
            let files = [
                paths.log_dir.join(attempt.to_string()).join(name),
                paths
                    .state_dir
                    .join("artifact-staging")
                    .join(attempt.to_string())
                    .join(id.to_string()),
                publication.join(attempt.to_string()).join(id.to_string()),
            ];
            for file in &files {
                fs::create_dir_all(file.parent().ok_or("parent")?)?;
                fs::write(file, b"preserved model")?;
            }
            let artifact = ArtifactRecord {
                id,
                attempt_id: attempt,
                path: name.into(),
                role: ArtifactRole::Model,
                sha256: Some(format!(
                    "sha256:{:x}",
                    sha2::Sha256::digest(b"preserved model")
                )),
                size_bytes: Some(15),
                state: igor_core::ArtifactPublicationState::Published,
                lifecycle: igor_core::ArtifactLifecycleState::Published,
                diagnosis_required: false,
                metadata: json!({"staged_path": files[1], "published_path": files[2]}),
            };
            database.artifacts().insert(&artifact).await?;
            copies.push(files);
            artifacts.push(artifact);
        }
        fs::write(
            paths.log_dir.join(attempt.to_string()).join("stdout.log"),
            b"stdout",
        )?;
        fs::write(
            paths.log_dir.join(attempt.to_string()).join("stderr.log"),
            b"stderr",
        )?;
        fs::write(
            publication.join(attempt.to_string()).join("unrelated"),
            b"other",
        )?;
        let replacement = successor_for(&family).await?;
        database
            .families()
            .supersede(family.prepared.generation.identity.id, &replacement)
            .await?;
        Ok(Self {
            family,
            paths,
            copies,
            artifacts,
            replacement,
        })
    }

    fn attempt(&self) -> AttemptId {
        self.artifacts[0].attempt_id
    }
    async fn cleanup(&self) -> Result<usize, PersistenceError> {
        self.family
            .database
            .artifacts()
            .cleanup_superseded_artifacts(self.attempt(), &self.paths)
            .await
    }
    async fn succeed(&self) -> TestResult {
        sqlx::query("UPDATE jobs SET state = 'succeeded' WHERE generation_id = ?")
            .bind(self.replacement.generation.identity.id.to_string())
            .execute(self.family.database.pool())
            .await?;
        Ok(())
    }
    fn assert_present(&self) -> TestResult {
        for copies in &self.copies {
            for path in copies {
                assert_eq!(fs::read(path)?, b"preserved model");
            }
        }
        Ok(())
    }
    async fn reject_events(&self) -> TestResult {
        sqlx::raw_sql("CREATE TRIGGER reject_superseded_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'injected event rollback'); END;")
            .execute(self.family.database.pool()).await?;
        Ok(())
    }
    async fn allow_events(&self) -> TestResult {
        sqlx::query("DROP TRIGGER reject_superseded_event")
            .execute(self.family.database.pool())
            .await?;
        Ok(())
    }
    async fn third_pending(&self) -> TestResult {
        let root = self.family._repository.path().join("project");
        fs::write(root.join("train.py"), "print('third')\n")?;
        git(&root, &["add", "train.py"])?;
        git(&root, &["commit", "-qm", "third"])?;
        let loaded = load_project_config(&root.join(".igor/project.toml"))?;
        let project = Project {
            id: self.family.prepared.family.project_id,
            name: "family-project".into(),
            root: root.clone(),
            config_path: loaded.config_file,
        };
        let third = build_family_generation(
            &project,
            &loaded.config,
            load_family_file(&root.join("family.toml"))?,
        )?
        .for_existing_family(self.family.prepared.family.clone(), 3)?;
        self.family
            .database
            .families()
            .supersede(self.replacement.generation.identity.id, &third)
            .await?;
        Ok(())
    }
}

fn delete_policy() -> serde_json::Value {
    json!({"policy":"delete-after-replacement","keep_on_success":[]})
}

#[tokio::test]
async fn superseded_cleanup_rejects_migrated_runtime_logs_and_missing_persisted_paths() -> TestResult
{
    for missing in [false, true] {
        let fixture = CleanupFixture::new(delete_policy()).await?;
        fixture.succeed().await?;
        let database = &fixture.family.database;
        let mut paths = fixture.paths.clone();
        paths.log_dir = paths.log_dir.with_extension("migrated");
        let migrated = paths.log_dir.join(fixture.attempt().to_string());
        for copies in &fixture.copies {
            let relative = copies[0].strip_prefix(&fixture.paths.log_dir)?;
            let destination = paths.log_dir.join(relative);
            fs::create_dir_all(destination.parent().ok_or("parent")?)?;
            fs::copy(&copies[0], destination)?;
        }
        let source = fixture.paths.log_dir.join(fixture.attempt().to_string());
        for stream in ["stdout.log", "stderr.log"] {
            fs::copy(source.join(stream), migrated.join(stream))?;
        }
        if missing {
            sqlx::query("DELETE FROM attempt_processes WHERE attempt_id = ?")
                .bind(fixture.attempt().to_string())
                .execute(database.pool())
                .await?;
        }
        assert!(
            database
                .artifacts()
                .cleanup_superseded_artifacts(fixture.attempt(), &paths)
                .await
                .is_err()
        );
        fixture.assert_present()?;
        for copies in &fixture.copies {
            assert_eq!(
                fs::read(
                    paths
                        .log_dir
                        .join(copies[0].strip_prefix(&fixture.paths.log_dir)?)
                )?,
                b"preserved model"
            );
        }
        for directory in [&source, &migrated] {
            assert_eq!(fs::read(directory.join("stdout.log"))?, b"stdout");
            assert_eq!(fs::read(directory.join("stderr.log"))?, b"stderr");
        }
        for artifact in &fixture.artifacts {
            let current = database
                .artifacts()
                .get(artifact.id)
                .await?
                .ok_or("artifact")?;
            assert_eq!(
                current.state,
                igor_core::ArtifactPublicationState::Published
            );
            assert_eq!(
                current.lifecycle,
                igor_core::ArtifactLifecycleState::Superseded
            );
        }
        let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM superseded_cleanup_intents")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(intents, 0);
        let events: i64 =
            sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'")
                .fetch_one(database.pool())
                .await?;
        assert_eq!(events, 0);
        if missing {
            assert!(fixture.cleanup().await.is_err());
            fixture.assert_present()?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_rechecks_persisted_log_paths_after_preparation() -> TestResult {
    let fixture = CleanupFixture::new(delete_policy()).await?;
    fixture.succeed().await?;
    let database = &fixture.family.database;
    // Change the registered path as preparation commits, before the second writer reads it.
    sqlx::raw_sql("CREATE TRIGGER change_cleanup_log_path AFTER INSERT ON superseded_cleanup_intents BEGIN UPDATE attempt_processes SET stderr_path = stderr_path || '.moved' WHERE attempt_id = (SELECT attempt_id FROM artifacts WHERE id = NEW.artifact_id); END;")
        .execute(database.pool()).await?;
    assert!(fixture.cleanup().await.is_err());
    fixture.assert_present()?;
    let intents: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM superseded_cleanup_intents WHERE state = 'prepared'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(intents, 2);
    for artifact in &fixture.artifacts {
        let current = database
            .artifacts()
            .get(artifact.id)
            .await?
            .ok_or("artifact")?;
        assert_eq!(
            current.state,
            igor_core::ArtifactPublicationState::Published
        );
        assert_eq!(
            current.lifecycle,
            igor_core::ArtifactLifecycleState::Superseded
        );
    }
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(events, 0);
    sqlx::query("DROP TRIGGER change_cleanup_log_path")
        .execute(database.pool())
        .await?;
    sqlx::query("UPDATE attempt_processes SET stderr_path = ? WHERE attempt_id = ?")
        .bind(
            fixture
                .paths
                .log_dir
                .join(fixture.attempt().to_string())
                .join("stderr.log")
                .to_str(),
        )
        .bind(fixture.attempt().to_string())
        .execute(database.pool())
        .await?;
    assert_eq!(fixture.cleanup().await?, 2);
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_migration_v17_backfill_and_transaction_rollback() -> TestResult {
    let fixture = CleanupFixture::new(json!(null)).await?;
    let database = &fixture.family.database;
    let before = database
        .artifacts()
        .get(fixture.artifacts[0].id)
        .await?
        .ok_or("artifact")?;
    let mut downgrade = database.pool().begin().await?;
    sqlx::raw_sql(super::report_ledger_schema::REMOVE_REPORT_LEDGER)
        .execute(&mut *downgrade)
        .await?;
    sqlx::raw_sql(
        "DROP TABLE owned_output_discovery; DROP TABLE owned_output_manifest; DROP TABLE owned_output_namespaces;
          DROP TRIGGER retired_failed_tails_immutable; DROP TRIGGER retired_failed_tails_preserved; DROP TRIGGER retired_failed_tails_replacement_guard; DROP TABLE failed_log_retirements;
          DROP TABLE cleanup_tombstones; DROP TABLE failed_cleanup_intents;
         DROP TABLE success_log_archive_preparations;
         DROP TRIGGER success_log_archive_identity_update;
         DROP TRIGGER success_log_archive_identity_delete;
         DROP TABLE success_log_archives;
         DROP TRIGGER superseded_cleanup_insert_guard;
        DROP TRIGGER superseded_cleanup_identity_update;
        DROP TRIGGER superseded_cleanup_identity_delete;
        DROP TABLE superseded_cleanup_intents;
        DROP TRIGGER artifacts_replacement_update;
           DELETE FROM _sqlx_migrations WHERE version IN (18, 19, 20, 21, 22);",
    )
    .execute(&mut *downgrade)
    .await?;
    let v17 = include_str!("../../migrations/0017_superseded_artifact_barrier.sql");
    let update = v17
        .split_once("CREATE TRIGGER artifacts_replacement_update")
        .ok_or("v17 update")?
        .1
        .split_once("CREATE TRIGGER artifacts_replacement_delete")
        .ok_or("v17 delete")?
        .0;
    sqlx::raw_sql(&format!(
        "CREATE TRIGGER artifacts_replacement_update{update}"
    ))
    .execute(&mut *downgrade)
    .await
    .map_err(|e| format!("restore v17 trigger: {e}"))?;
    downgrade.commit().await?;
    assert_eq!(database.schema_version().await?, 17);
    let mut transaction = database.pool().begin().await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/0018_superseded_cleanup_intents.sql"
    ))
    .execute(&mut *transaction)
    .await
    .map_err(|e| format!("transactional v18 migration: {e}"))?;
    transaction.rollback().await?;
    let absent: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE name = 'superseded_cleanup_intents'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(absent, 0);
    assert_replacement_blocks_retirement(database, before.id).await?;
    database.pool().close().await;
    let upgraded = Database::open(&fixture.paths.database).await?;
    assert_eq!(upgraded.schema_version().await?, 23);
    assert_eq!(upgraded.artifacts().get(before.id).await?, Some(before));
    let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM superseded_cleanup_intents")
        .fetch_one(upgraded.pool())
        .await?;
    assert_eq!(intents, 0);
    assert_eq!(
        upgraded
            .artifacts()
            .cleanup_superseded_artifacts(fixture.attempt(), &fixture.paths)
            .await?,
        0
    );
    fixture.assert_present()?;
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_requires_current_exact_comparable_replacement() -> TestResult {
    let fixture = CleanupFixture::new(delete_policy()).await?;
    let database = &fixture.family.database;
    assert_eq!(
        database
            .artifacts()
            .superseded_cleanup_candidates(None)
            .await?,
        vec![fixture.attempt()]
    );
    assert!(
        database
            .artifacts()
            .superseded_cleanup_candidates(Some(fixture.attempt()))
            .await?
            .is_empty()
    );
    assert_eq!(fixture.cleanup().await?, 0);
    sqlx::query("UPDATE jobs SET state = 'failed' WHERE generation_id = ?")
        .bind(fixture.replacement.generation.identity.id.to_string())
        .execute(database.pool())
        .await?;
    assert_eq!(fixture.cleanup().await?, 0);
    fixture.succeed().await?;
    let id = fixture.replacement.submissions[1].job.id;
    let original: String = sqlx::query_scalar("SELECT spec_json FROM jobs WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(database.pool())
        .await?;
    for field in ["seed", "protocol_digest", "source_revision"] {
        let mut invalid: serde_json::Value = serde_json::from_str(&original)?;
        if field == "seed" {
            invalid["family"][field] = json!(1);
        } else {
            invalid["family"]["generation"][field] = json!("invalid");
        }
        sqlx::query("UPDATE jobs SET spec_json = ? WHERE id = ?")
            .bind(invalid.to_string())
            .bind(id.to_string())
            .execute(database.pool())
            .await?;
        assert_eq!(fixture.cleanup().await?, 0);
        fixture.assert_present()?;
    }
    sqlx::query("UPDATE jobs SET spec_json = ? WHERE id = ?")
        .bind(original)
        .bind(id.to_string())
        .execute(database.pool())
        .await?;
    // Live project configuration cannot override the frozen retention snapshot.
    fs::write(
        fixture
            .family
            ._repository
            .path()
            .join("project/.igor/project.toml"),
        "invalid live config",
    )?;
    assert_eq!(fixture.cleanup().await?, 2);
    for copies in &fixture.copies {
        for path in copies {
            assert!(!path.exists());
        }
    }
    assert_eq!(
        fs::read(
            fixture
                .paths
                .log_dir
                .join(fixture.attempt().to_string())
                .join("stdout.log")
        )?,
        b"stdout"
    );
    assert_eq!(
        fs::read(
            fixture
                .paths
                .log_dir
                .join(fixture.attempt().to_string())
                .join("stderr.log")
        )?,
        b"stderr"
    );
    assert_eq!(
        fs::read(
            fixture.copies[0][2]
                .parent()
                .ok_or("parent")?
                .join("unrelated")
        )?,
        b"other"
    );
    assert!(fixture.copies[1][0].parent().ok_or("parent")?.is_dir());
    assert_eq!(fixture.cleanup().await?, 0);
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(events, 2);
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_frozen_keep_patterns_and_legacy() -> TestResult {
    for (policy, retained) in [
        (json!(null), false),
        (json!({"policy":"keep","keep_on_success":[]}), true),
        (
            json!({"policy":"delete-after-replacement","keep_on_success":["**/*.bin"]}),
            true,
        ),
    ] {
        let fixture = CleanupFixture::new(policy).await?;
        assert_eq!(fixture.cleanup().await?, 0);
        fixture.assert_present()?;
        for artifact in &fixture.artifacts {
            assert_eq!(
                fixture
                    .family
                    .database
                    .artifacts()
                    .get(artifact.id)
                    .await?
                    .ok_or("artifact")?
                    .lifecycle,
                if retained {
                    igor_core::ArtifactLifecycleState::Retained
                } else {
                    igor_core::ArtifactLifecycleState::Superseded
                }
            );
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM superseded_cleanup_intents")
            .fetch_one(fixture.family.database.pool())
            .await?;
        assert_eq!(count, 0);
        for artifact in &fixture.artifacts {
            let subject = artifact.id.to_string();
            let evidence = fixture
                .family
                .database
                .cleanup_tombstone("superseded_artifact_retention", &subject)
                .await?;
            assert_eq!(evidence.is_some(), retained);
            if let Some(evidence) = evidence {
                assert_eq!(evidence["outcome"], "retained");
                fixture.cleanup().await?;
                assert_eq!(
                    fixture
                        .family
                        .database
                        .cleanup_tombstone("superseded_artifact_retention", &subject)
                        .await?,
                    Some(evidence)
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_matching_keep_pattern_retains_before_replacement() -> TestResult {
    let fixture = CleanupFixture::new(
        json!({"policy":"delete-after-replacement","keep_on_success":["nested/**"]}),
    )
    .await?;
    assert_eq!(fixture.cleanup().await?, 0);
    fixture.assert_present()?;
    let database = &fixture.family.database;
    assert_eq!(
        database
            .artifacts()
            .get(fixture.artifacts[1].id)
            .await?
            .ok_or("kept")?
            .lifecycle,
        igor_core::ArtifactLifecycleState::Retained
    );
    fixture.succeed().await?;
    assert_eq!(fixture.cleanup().await?, 1);
    for path in &fixture.copies[0] {
        assert!(!path.exists());
    }
    for path in &fixture.copies[1] {
        assert_eq!(fs::read(path)?, b"preserved model");
    }
    assert_eq!(fixture.cleanup().await?, 0);
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_active_old_extraction_blocks() -> TestResult {
    for state in ["pending", "running"] {
        let fixture = CleanupFixture::new(delete_policy()).await?;
        fixture.succeed().await?;
        sqlx::query("INSERT INTO actions (id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at, claim_id, claim_owner, claim_expires_at) VALUES (?, ?, ?, ?, 'extract_metrics', ?, '{}', ?, '2000-01-01', ?, ?, ?)")
            .bind(ActionId::new().to_string()).bind(fixture.family.prepared.family.project_id.to_string())
            .bind(fixture.family.prepared.submissions[0].job.id.to_string()).bind(fixture.attempt().to_string()).bind(state)
            .bind(format!("extract:{}", fixture.attempt()))
            .bind((state == "running").then_some("claim"))
            .bind((state == "running").then_some("owner"))
            .bind((state == "running").then_some("2999-01-01"))
            .execute(fixture.family.database.pool()).await?;
        assert_eq!(fixture.cleanup().await?, 0);
        fixture.assert_present()?;
    }
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_event_rollback_replays_durable_intents_once() -> TestResult {
    for third in [false, true] {
        let fixture = CleanupFixture::new(delete_policy()).await?;
        fixture.succeed().await?;
        fixture.reject_events().await?;
        assert!(fixture.cleanup().await.is_err());
        for copies in &fixture.copies {
            for path in copies {
                assert!(!path.exists());
            }
        }
        let database = &fixture.family.database;
        let evidence: Vec<(String, String)> = sqlx::query_as("SELECT identity_json, replacement_generation_id FROM superseded_cleanup_intents WHERE state = 'prepared' ORDER BY artifact_id")
            .fetch_all(database.pool()).await?;
        assert_eq!(evidence.len(), 2);
        if third {
            fixture.third_pending().await?;
        }
        fixture.allow_events().await?;
        assert_eq!(fixture.cleanup().await?, 2);
        let mut tombstones = Vec::new();
        for artifact in &fixture.artifacts {
            let subject = artifact.id.to_string();
            let value = database
                .cleanup_tombstone("superseded_artifact_retention", &subject)
                .await?
                .ok_or("superseded tombstone")?;
            assert_eq!(value["outcome"], "removed");
            tombstones.push((subject, value));
        }
        assert_eq!(fixture.cleanup().await?, 0);
        for (subject, value) in tombstones {
            assert_eq!(
                database
                    .cleanup_tombstone("superseded_artifact_retention", &subject)
                    .await?,
                Some(value)
            );
        }
        let after: Vec<(String, String)> = sqlx::query_as("SELECT identity_json, replacement_generation_id FROM superseded_cleanup_intents WHERE state = 'removed' AND completion_only = 0 ORDER BY artifact_id")
            .fetch_all(database.pool()).await?;
        assert_eq!(after, evidence);
        assert!(
            sqlx::query("UPDATE superseded_cleanup_intents SET identity_json = '{}' ")
                .execute(database.pool())
                .await
                .is_err()
        );
        assert!(
            sqlx::query("DELETE FROM superseded_cleanup_intents")
                .execute(database.pool())
                .await
                .is_err()
        );
        let events: i64 =
            sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'cleanup_state_changed'")
                .fetch_one(database.pool())
                .await?;
        assert_eq!(events, 2);
    }
    Ok(())
}

// Full-row projection of the durable superseded-cleanup decisions.
async fn reopen_intent_rows_superseded(database: &Database) -> Result<Vec<String>, Box<dyn Error>> {
    Ok(sqlx::query_scalar(
        "SELECT json_object('artifact_id', artifact_id, 'identity_json', identity_json, \
         'replacement_generation_id', replacement_generation_id, 'state', state, \
         'completion_only', completion_only) \
         FROM superseded_cleanup_intents WHERE completion_only = 0 ORDER BY artifact_id",
    )
    .fetch_all(database.pool())
    .await?)
}

// Identity that must not change between preparation, reopen and completion.
async fn reopen_identity_superseded(
    database: &Database,
) -> Result<Vec<(String, String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT artifact_id, identity_json, replacement_generation_id \
         FROM superseded_cleanup_intents WHERE completion_only = 0 ORDER BY artifact_id",
    )
    .fetch_all(database.pool())
    .await
}

async fn reopen_counts_superseded(database: &Database) -> Result<(i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM superseded_cleanup_intents WHERE state = 'prepared'), \
                (SELECT COUNT(*) FROM cleanup_tombstones), \
                (SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed')",
    )
    .fetch_one(database.pool())
    .await
}

#[tokio::test]
async fn superseded_cleanup_event_rollback_replays_durable_intents_after_pool_reopen() -> TestResult
{
    let mut fixture = CleanupFixture::new(delete_policy()).await?;
    fixture.succeed().await?;
    fixture.reject_events().await?;
    assert!(fixture.cleanup().await.is_err());
    // Every registered copy was really unlinked before the completion rollback.
    for path in fixture.copies.iter().flatten() {
        assert!(!path.exists());
    }
    let prepared = reopen_identity_superseded(&fixture.family.database).await?;
    assert_eq!(prepared.len(), 2);
    let prepared_rows = reopen_intent_rows_superseded(&fixture.family.database).await?;
    assert_eq!(
        reopen_counts_superseded(&fixture.family.database).await?,
        (2, 0, 0)
    );
    let persisted = gc_support::sql_snapshot(&fixture.family.database).await?;

    // Close the pool, then reopen the same database file.
    fixture.family.database.pool().close().await;
    fixture.family.database = Database::open(&fixture.paths.database).await?;
    let database = &fixture.family.database;
    assert_eq!(
        gc_support::sql_snapshot(database).await?,
        persisted,
        "prepared intents must survive the pool reopen"
    );
    assert_eq!(
        reopen_intent_rows_superseded(database).await?,
        prepared_rows
    );
    assert_eq!(reopen_identity_superseded(database).await?, prepared);
    assert_eq!(reopen_counts_superseded(database).await?, (2, 0, 0));
    assert!(
        sqlx::query("UPDATE superseded_cleanup_intents SET identity_json = '{}'")
            .execute(database.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM superseded_cleanup_intents")
            .execute(database.pool())
            .await
            .is_err()
    );

    // Unselected outputs outside the removal targets survive, and a file
    // created after the fault has no durable intent authorizing its removal.
    let publication = fixture.copies[0][2].parent().ok_or("publication")?;
    let unrelated = publication.join("unrelated");
    let late = publication.join("created-after-fault.bin");
    fs::write(&late, b"created after fault")?;
    let logs = fixture.paths.log_dir.join(fixture.attempt().to_string());
    assert_eq!(fs::read(&unrelated)?, b"other");
    assert_eq!(fs::read(logs.join("stdout.log"))?, b"stdout");
    assert_eq!(fs::read(logs.join("stderr.log"))?, b"stderr");

    fixture.allow_events().await?;
    assert_eq!(fixture.cleanup().await?, 2);
    assert_eq!(reopen_counts_superseded(database).await?, (0, 2, 2));
    // Exactly one tombstone and one event per subject, and the durable
    // decision identity is unchanged by completion.
    assert_eq!(reopen_identity_superseded(database).await?, prepared);
    for path in fixture.copies.iter().flatten() {
        assert!(!path.exists());
    }
    assert_eq!(fs::read(&unrelated)?, b"other");
    assert_eq!(fs::read(&late)?, b"created after fault");
    assert_eq!(fs::read(logs.join("stdout.log"))?, b"stdout");
    assert_eq!(fs::read(logs.join("stderr.log"))?, b"stderr");

    // Replaying without SQL or file changes completes nothing new.
    let snapshot = gc_support::sql_snapshot(database).await?;
    let disk = gc_support::files(fixture.family._database.path(), &fixture.paths.database)?;
    assert_eq!(fixture.cleanup().await?, 0);
    assert_eq!(gc_support::sql_snapshot(database).await?, snapshot);
    assert_eq!(
        gc_support::files(fixture.family._database.path(), &fixture.paths.database)?,
        disk
    );
    assert_eq!(reopen_identity_superseded(database).await?, prepared);
    assert_eq!(fs::read(&late)?, b"created after fault");
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_partial_replay_with_new_pending_generation_preserves_remaining()
-> TestResult {
    let fixture = CleanupFixture::new(delete_policy()).await?;
    fixture.succeed().await?;
    fixture.reject_events().await?;
    assert!(fixture.cleanup().await.is_err());
    // Model a crash after only one unlink: restore the other registered copies.
    for copies in &fixture.copies {
        for path in copies {
            fs::write(path, b"preserved model")?;
        }
    }
    fs::remove_file(&fixture.copies[0][2])?;
    fixture.third_pending().await?;
    fixture.allow_events().await?;
    assert_eq!(fixture.cleanup().await?, 0);
    for (index, copies) in fixture.copies.iter().enumerate() {
        for (copy, path) in copies.iter().enumerate() {
            if index == 0 && copy == 2 {
                assert!(!path.exists());
            } else {
                assert_eq!(fs::read(path)?, b"preserved model");
            }
        }
    }
    let completed: i64 = sqlx::query_scalar("SELECT count(*) FROM superseded_cleanup_intents WHERE state != 'prepared' OR completion_only != 0")
        .fetch_one(fixture.family.database.pool()).await?;
    assert_eq!(completed, 0);
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_preflight_refuses_whole_batch() -> TestResult {
    for damage in [
        "digest",
        "size",
        "published",
        "staged",
        "traversal",
        "dot",
        "stream",
        "hidden",
        "missing",
        "symlink",
        "root_symlink",
        "ancestor_symlink",
        "publication_root_symlink",
        "state_root_symlink",
        "outside_root",
    ] {
        let fixture = CleanupFixture::new(delete_policy()).await?;
        fixture.succeed().await?;
        let database = &fixture.family.database;
        let artifact = &fixture.artifacts[1];
        match damage {
            "digest" => fs::write(&fixture.copies[1][2], b"tampered models")?,
            "size" => fs::write(&fixture.copies[1][0], b"short")?,
            "published" | "staged" => {
                let mut metadata = artifact.metadata.clone();
                metadata[if damage == "published" {
                    "published_path"
                } else {
                    "staged_path"
                }] = json!(fixture.copies[0][2]);
                sqlx::query("UPDATE artifacts SET metadata_json = ? WHERE id = ?")
                    .bind(metadata.to_string())
                    .bind(artifact.id.to_string())
                    .execute(database.pool())
                    .await?;
            }
            "traversal" | "dot" | "stream" => {
                let path = match damage {
                    "traversal" => "../outside",
                    "dot" => "nested/./evidence.bin",
                    _ => "stdout.log",
                };
                sqlx::query("UPDATE artifacts SET path = ? WHERE id = ?")
                    .bind(path)
                    .bind(artifact.id.to_string())
                    .execute(database.pool())
                    .await?;
            }
            "hidden" => {
                let root = fixture
                    .family
                    ._repository
                    .path()
                    .join("project/artifacts/.igor-publication-staging")
                    .join(fixture.attempt().to_string());
                fs::create_dir_all(&root)?;
                fs::write(root.join(artifact.id.to_string()), b"preserved model")?;
            }
            "missing" => fs::remove_file(&fixture.copies[1][2])?,
            "symlink" => {
                fs::remove_file(&fixture.copies[1][2])?;
                std::os::unix::fs::symlink(&fixture.copies[1][0], &fixture.copies[1][2])?;
            }
            "root_symlink"
            | "ancestor_symlink"
            | "publication_root_symlink"
            | "state_root_symlink" => {
                let root = match damage {
                    "root_symlink" => fixture.paths.log_dir.clone(),
                    "publication_root_symlink" => {
                        fixture.family._repository.path().join("project/artifacts")
                    }
                    "state_root_symlink" => fixture.paths.state_dir.clone(),
                    _ => fixture
                        .paths
                        .log_dir
                        .parent()
                        .ok_or("parent")?
                        .to_path_buf(),
                };
                let moved = root.with_extension("moved");
                fs::rename(&root, &moved)?;
                std::os::unix::fs::symlink(&moved, &root)?;
            }
            "outside_root" => {
                sqlx::query("UPDATE projects SET root_path = ? WHERE id = ?")
                    .bind(fixture.paths.state_dir.to_str())
                    .bind(fixture.family.prepared.family.project_id.to_string())
                    .execute(database.pool())
                    .await?;
            }
            _ => unreachable!(),
        }
        assert!(fixture.cleanup().await.is_err(), "{damage}");
        // Every undamaged copy of the OTHER artifact survives, independent of UUID sort order.
        for path in &fixture.copies[0] {
            assert_eq!(fs::read(path)?, b"preserved model", "{damage}");
        }
        let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM superseded_cleanup_intents")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(intents, 0, "{damage}");
    }
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_prepared_identity_cannot_be_replaced_on_retry() -> TestResult {
    let fixture = CleanupFixture::new(delete_policy()).await?;
    fixture.succeed().await?;
    fixture.reject_events().await?;
    assert!(fixture.cleanup().await.is_err());
    fixture.allow_events().await?;
    for copies in &fixture.copies {
        for path in copies {
            fs::write(path, b"preserved model")?;
        }
    }
    let database = &fixture.family.database;
    let before: Vec<String> = sqlx::query_scalar(
        "SELECT identity_json FROM superseded_cleanup_intents ORDER BY artifact_id",
    )
    .fetch_all(database.pool())
    .await?;
    sqlx::query("UPDATE artifacts SET metadata_json = json_set(metadata_json, '$.extra', 'tampered') WHERE id = ?")
        .bind(fixture.artifacts[1].id.to_string()).execute(database.pool()).await?;
    assert!(fixture.cleanup().await.is_err());
    fixture.assert_present()?;
    let after: Vec<String> = sqlx::query_scalar(
        "SELECT identity_json FROM superseded_cleanup_intents ORDER BY artifact_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(before, after);
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_newer_pending_generation_does_not_latch_success() -> TestResult {
    let fixture = CleanupFixture::new(delete_policy()).await?;
    fixture.succeed().await?;
    fixture.third_pending().await?;
    assert_eq!(fixture.cleanup().await?, 0);
    fixture.assert_present()?;
    let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM superseded_cleanup_intents")
        .fetch_one(fixture.family.database.pool())
        .await?;
    assert_eq!(intents, 0);
    Ok(())
}

#[tokio::test]
async fn superseded_cleanup_completion_only_repairs_absent_artifact_without_unlinking_others()
-> TestResult {
    let fixture = CleanupFixture::new(delete_policy()).await?;
    fixture.succeed().await?;
    fixture.reject_events().await?;
    assert!(fixture.cleanup().await.is_err());
    for path in &fixture.copies[1] {
        fs::write(path, b"preserved model")?;
    }
    fixture.third_pending().await?;
    fixture.allow_events().await?;
    assert_eq!(fixture.cleanup().await?, 1);
    for path in &fixture.copies[0] {
        assert!(!path.exists());
    }
    for path in &fixture.copies[1] {
        assert_eq!(fs::read(path)?, b"preserved model");
    }
    let database = &fixture.family.database;
    assert_replacement_blocks_retirement(database, fixture.artifacts[1].id).await?;
    assert_eq!(fixture.cleanup().await?, 0);
    Ok(())
}
