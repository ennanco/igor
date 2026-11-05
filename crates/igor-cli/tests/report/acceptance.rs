use super::*;
use igor_core::{FamilyId, GenerationId, GenerationStatus, ReportContext, ReportId, ReportState};
use sha2::{Digest, Sha256};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use tokio::runtime::Runtime;

struct ReleaseGates(PathBuf);
impl Drop for ReleaseGates {
    fn drop(&mut self) {
        for generation in [2, 3] {
            let _ = fs::write(self.0.join(format!("release-{generation}")), b"go");
        }
    }
}

fn write_runner(root: &Path, gates: &Path, generation: u32) -> TestResult {
    let gate = gates.join(format!("release-{generation}"));
    let script = format!(
        "#!/bin/sh\nset -eu\nif [ '{generation}' -gt 1 ] && [ \"$1\" = 5 ]; then\n i=0; while [ ! -f '{gate}' ] && [ \"$i\" -lt 1200 ]; do i=$((i+1)); /usr/bin/sleep 0.05; done\n test -f '{gate}'\nfi\nif [ '{generation}' = 2 ] && [ \"$1\" = 3 ]; then exit 17; fi\nprintf '%s\\n' \"$(({generation} * 100 + $1))\"\n",
        gate = gate.display()
    );
    fs::write(root.join("run.sh"), script)?;
    git(root, &["add", "run.sh"])?;
    git(
        root,
        &["commit", "-qm", &format!("generation {generation}")],
    )
}

fn configure(home: &Path, root: &Path, gates: &Path) -> TestResult {
    project(home, root, false)?;
    fs::write(
        root.join(".gitignore"),
        "RESULTS.md\nartifacts/\n.igor-report-*.tmp\n",
    )?;
    fs::write(
        root.join("science.json"),
        "{\"dataset\":\"synthetic-v1\",\"seeds\":[1,2,3,4,5]}\n",
    )?;
    fs::write(
        root.join("NOTES.md"),
        "# Handwritten research notes\nPreserve me.\n",
    )?;
    fs::write(
        root.join(".igor/project.toml"),
        "schema_version = 1\n[resources]\ngpu_count = 0\n[results]\nextractor = ['./extract.sh']\nworking_directory = 'attempt'\n[report]\nenabled = true\n",
    )?;
    fs::write(
        root.join("extract.sh"),
        r#"#!/bin/sh
set -eu
/bin/cat > extractor-input.json
read -r score < stdout.log
printf '%s' "$score" > model.bin
printf '{"schema_version":1,"status":"success","metrics":{"validation.score":%s},"selection":{"split":"validation","metric":"score","value":%s},"classes":{"class-a":{"f1":0.75}},"artifacts":[{"path":"model.bin","role":"model"}]}\n' "$score" "$score"
"#,
    )?;
    fs::set_permissions(root.join("extract.sh"), fs::Permissions::from_mode(0o700))?;
    let members = (1..=5)
        .map(|seed| format!("\n[[members]]\nseed = {seed}\nargs = ['{seed}']\n"))
        .collect::<String>();
    let script = toml::Value::String(root.join("run.sh").to_str().ok_or("script path")?.into());
    fs::write(
        root.join("family.toml"),
        format!(
            "schema_version = 1\nname = 'five-seed non-agent acceptance'\nimmutable_inputs = ['science.json']\n[execution]\nprogram = '/bin/sh'\nargs = [{script}]\n{members}"
        ),
    )?;
    // Add every configured fixture before the revision used for the first submission.
    fs::write(root.join("run.sh"), "#!/bin/sh\nexit 0\n")?;
    git(root, &["add", "."])?;
    git(root, &["commit", "-qm", "configure acceptance workflow"])?;
    write_runner(root, gates, 1)
}

fn generation(value: &Value) -> Result<GenerationId, Box<dyn Error>> {
    Ok(value["generation_id"]
        .as_str()
        .ok_or("generation id")?
        .parse()?)
}

fn job_ids(value: &Value) -> Result<Vec<String>, Box<dyn Error>> {
    value["jobs"]
        .as_array()
        .ok_or("jobs")?
        .iter()
        .map(|job| {
            job["spec"]["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "job id".into())
        })
        .collect()
}

fn await_jobs(home: &Path, root: &Path, jobs: &[String], failed_seed: Option<usize>) -> TestResult {
    for (index, job) in jobs.iter().enumerate() {
        let output = command(home, root).args(["wait", job, "--json"]).output()?;
        assert_eq!(
            output.status.code(),
            Some(if failed_seed == Some(index + 1) { 1 } else { 0 }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn context(
    runtime: &Runtime,
    database: &Database,
    family: FamilyId,
) -> Result<ReportContext, Box<dyn Error>> {
    Ok(runtime.block_on(database.families().report_context(family))?)
}

fn assert_not_ready(
    runtime: &Runtime,
    database: &Database,
    family: FamilyId,
    current: GenerationId,
    status: GenerationStatus,
    baseline: &[u8],
    root: &Path,
) -> TestResult {
    let context = context(runtime, database, family)?;
    assert_eq!(context.generation.identity.id, current);
    assert_eq!(context.generation.status, status);
    assert!(!context.ready);
    assert!(context.primary_metrics.is_none());
    assert_eq!(fs::read(root.join("RESULTS.md"))?, baseline);
    Ok(())
}

fn await_generation_reports(
    runtime: &Runtime,
    database: &Database,
    current: GenerationId,
    count: i64,
) -> TestResult {
    wait(|| {
        let states: Vec<(String, Option<String>)> = runtime.block_on(sqlx::query_as("SELECT state, last_error FROM actions WHERE kind = 'generate_report' AND json_extract(spec_json, '$.generation.id') = ?")
            .bind(current.to_string()).fetch_all(database.pool()))?;
        assert!(
            states
                .iter()
                .all(|(state, _)| state != "failed" && state != "cancelled"),
            "report actions: {states:?}"
        );
        Ok(states.len() == usize::try_from(count)?
            && states.iter().all(|(state, _)| state == "succeeded"))
    })
}

fn await_extractions(
    runtime: &Runtime,
    database: &Database,
    current: GenerationId,
    count: i64,
) -> TestResult {
    wait(|| {
        let complete: i64 = runtime.block_on(sqlx::query_scalar("SELECT count(*) FROM actions a JOIN attempts t ON t.id = a.attempt_id JOIN jobs j ON j.id = t.job_id WHERE j.generation_id = ? AND a.kind = 'extract_metrics' AND a.state = 'succeeded'")
            .bind(current.to_string()).fetch_one(database.pool()))?;
        Ok(complete == count)
    })
}

fn published(
    runtime: &Runtime,
    database: &Database,
    current: GenerationId,
) -> Result<igor_core::StoredReportRun, Box<dyn Error>> {
    let id: String = runtime.block_on(
        sqlx::query_scalar(
            "SELECT id FROM report_runs WHERE generation_id = ? AND state = 'published'",
        )
        .bind(current.to_string())
        .fetch_one(database.pool()),
    )?;
    Ok(runtime
        .block_on(database.actions().report_run(id.parse::<ReportId>()?))?
        .ok_or("published run")?)
}

#[test]
fn five_seed_non_agent_workflow_publishes_only_current_generation() -> TestResult {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    let root = temp.path().join("project");
    let gates = temp.path().join("gates");
    fs::create_dir(&gates)?;
    configure(&home, &root, &gates)?;
    let protected: Vec<_> = [
        "science.json",
        "NOTES.md",
        ".igor/report-prompt.md",
        ".igor/project.toml",
    ]
    .into_iter()
    .map(|path| Ok((path, fs::read(root.join(path))?)))
    .collect::<Result<_, std::io::Error>>()?;
    let mut worker = spawn(&home, temp.path(), "worker")?;
    let _release = ReleaseGates(gates.clone());
    wait(|| {
        assert!(worker.0.try_wait()?.is_none());
        Ok(command(&home, temp.path())
            .args(["project", "list", "--json"])
            .output()?
            .status
            .success())
    })?;
    successful(
        command(&home, temp.path())
            .args(["project", "add"])
            .arg(&root)
            .arg("--json")
            .output()?,
    )?;
    let first = successful(
        command(&home, &root)
            .args(["family", "submit", "--file", "family.toml", "--json"])
            .output()?,
    )?;
    let family: FamilyId = first["family_id"].as_str().ok_or("family id")?.parse()?;
    let family_text = family.to_string();
    let first_generation = generation(&first)?;
    let first_jobs = job_ids(&first)?;
    assert_eq!(first_jobs.len(), 5);
    await_jobs(&home, &root, &first_jobs, None)?;
    let runtime = Runtime::new()?;
    let database = runtime.block_on(Database::open(home.join("state/igor/igor.sqlite3")))?;
    let pending = context(&runtime, &database, family)?;
    assert_eq!(pending.generation.status, GenerationStatus::Comparable);
    assert!(!pending.ready && pending.primary_metrics.is_none());
    assert!(!root.join("RESULTS.md").exists());
    let request: ReportRequestReceipt = serde_json::from_value(successful(
        command(&home, &root)
            .args(["report", "generate", &family_text, "--json"])
            .output()?,
    )?)?;
    assert_eq!(request.state, igor_core::ActionState::Pending);
    assert!(!root.join("RESULTS.md").exists());
    let mut supervisor = spawn(&home, temp.path(), "supervisor")?;
    // One manual request plus the independent family-completed trigger.
    await_generation_reports(&runtime, &database, first_generation, 2)?;
    let original = fs::read(root.join("RESULTS.md"))?;
    let original_inode = fs::metadata(root.join("RESULTS.md"))?.ino();
    let first_run = published(&runtime, &database, first_generation)?;
    let old_context = first_run.context.as_ref().ok_or("first context")?;
    let old_series = old_context
        .primary_metrics
        .as_ref()
        .ok_or("first metrics")?;
    assert_eq!(old_series.len(), 1);
    assert_eq!(
        old_series[0]
            .values
            .iter()
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        [101.0, 102.0, 103.0, 104.0, 105.0]
    );
    let old_artifacts: Vec<_> = old_context
        .members
        .iter()
        .flat_map(|member| &member.attempts)
        .flat_map(|attempt| &attempt.artifacts)
        .map(|artifact| {
            let path = artifact.metadata["published_path"]
                .as_str()
                .ok_or("published artifact path")?;
            Ok((artifact.id, PathBuf::from(path), fs::read(path)?))
        })
        .collect::<Result<_, Box<dyn Error>>>()?;
    assert_eq!(old_artifacts.len(), 5);

    // A terminal failed replacement cannot fall back to the previously comparable generation.
    write_runner(&root, &gates, 2)?;
    let second = successful(
        command(&home, &root)
            .args([
                "family",
                "supersede",
                &family_text,
                "--file",
                "family.toml",
                "--json",
            ])
            .output()?,
    )?;
    let second_generation = generation(&second)?;
    let second_jobs = job_ids(&second)?;
    assert_not_ready(
        &runtime,
        &database,
        family,
        second_generation,
        GenerationStatus::Incomplete,
        &original,
        &root,
    )?;
    assert_eq!(
        command(&home, &root)
            .args(["report", "generate", &family_text, "--json"])
            .output()?
            .status
            .code(),
        Some(9)
    );
    await_jobs(&home, &root, &second_jobs[..4], Some(3))?;
    await_extractions(&runtime, &database, second_generation, 3)?;
    assert_not_ready(
        &runtime,
        &database,
        family,
        second_generation,
        GenerationStatus::Incomplete,
        &original,
        &root,
    )?;
    for (_, path, bytes) in &old_artifacts {
        assert_eq!(fs::read(path)?, *bytes);
    }
    fs::write(gates.join("release-2"), b"go")?;
    await_jobs(&home, &root, &second_jobs, Some(3))?;
    await_extractions(&runtime, &database, second_generation, 4)?;
    let failed_context = context(&runtime, &database, family)?;
    for member in &failed_context.members {
        if member.seed.0 != 3 {
            assert_eq!(
                member.attempts[0].metrics.metrics,
                [(
                    "validation.score".into(),
                    f64::from(u32::try_from(member.seed.0)? + 200)
                )]
            );
        }
    }
    assert_not_ready(
        &runtime,
        &database,
        family,
        second_generation,
        GenerationStatus::Complete,
        &original,
        &root,
    )?;
    assert_eq!(
        command(&home, &root)
            .args(["report", "generate", &family_text, "--json"])
            .output()?
            .status
            .code(),
        Some(9)
    );
    successful(
        command(&home, &root)
            .args(["retry", &second_jobs[2], "--json"])
            .output()?,
    )?;
    assert_eq!(
        command(&home, &root)
            .args(["wait", &second_jobs[2], "--json"])
            .output()?
            .status
            .code(),
        Some(1)
    );
    assert_not_ready(
        &runtime,
        &database,
        family,
        second_generation,
        GenerationStatus::Complete,
        &original,
        &root,
    )?;
    for (_, path, bytes) in &old_artifacts {
        assert_eq!(fs::read(path)?, *bytes);
    }
    assert_eq!(fs::metadata(root.join("RESULTS.md"))?.ino(), original_inode);
    let second_reports: i64 = runtime.block_on(
        sqlx::query_scalar("SELECT count(*) FROM report_runs WHERE generation_id = ?")
            .bind(second_generation.to_string())
            .fetch_one(database.pool()),
    )?;
    assert_eq!(second_reports, 0);
    supervisor.stop()?;

    // A repaired revision reruns all seeds. Worker-only completion leaves extraction pending.
    write_runner(&root, &gates, 3)?;
    let third = successful(
        command(&home, &root)
            .args([
                "family",
                "supersede",
                &family_text,
                "--file",
                "family.toml",
                "--json",
            ])
            .output()?,
    )?;
    let third_generation = generation(&third)?;
    let third_jobs = job_ids(&third)?;
    assert_eq!(third_jobs.len(), 5);
    assert_not_ready(
        &runtime,
        &database,
        family,
        third_generation,
        GenerationStatus::Incomplete,
        &original,
        &root,
    )?;
    await_jobs(&home, &root, &third_jobs[..4], None)?;
    assert_not_ready(
        &runtime,
        &database,
        family,
        third_generation,
        GenerationStatus::Incomplete,
        &original,
        &root,
    )?;
    fs::write(gates.join("release-3"), b"go")?;
    await_jobs(&home, &root, &third_jobs, None)?;
    assert_not_ready(
        &runtime,
        &database,
        family,
        third_generation,
        GenerationStatus::Comparable,
        &original,
        &root,
    )?;
    for (_, path, bytes) in &old_artifacts {
        assert_eq!(fs::read(path)?, *bytes);
    }
    let mut supervisor = spawn(&home, temp.path(), "supervisor")?;
    await_generation_reports(&runtime, &database, third_generation, 1)?;
    let final_run = published(&runtime, &database, third_generation)?;
    let final_context = final_run.context.as_ref().ok_or("final context")?;
    assert_eq!(final_context.generation.identity.id, third_generation);
    assert!(final_context.ready);
    assert_eq!(final_context.history.len(), 2);
    assert_eq!(
        final_context.generation.identity.protocol_digest,
        old_context.generation.identity.protocol_digest
    );
    assert_ne!(
        final_context.generation.identity.source_revision,
        old_context.generation.identity.source_revision
    );
    let series = final_context
        .primary_metrics
        .as_ref()
        .ok_or("primary series")?;
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].name, "validation.score");
    assert_eq!(series[0].values.len(), 5);
    for (index, value) in series[0].values.iter().enumerate() {
        assert_eq!(value.seed.0, u64::try_from(index + 1)?);
        assert_eq!(value.value, f64::from(u32::try_from(index)? + 301));
        let member = &final_context.members[index];
        assert_eq!(member.job_id.to_string(), third_jobs[index]);
        assert_eq!(member.attempts.len(), 1);
        assert_eq!(value.attempt_id, member.attempts[0].attempt_id);
        let metrics = &member.attempts[0].metrics;
        assert_eq!(metrics.metrics, [("validation.score".into(), value.value)]);
        assert_eq!(
            metrics.selection.as_ref().ok_or("indexed selection")?["value"].as_f64(),
            Some(value.value)
        );
        assert_eq!(
            metrics.document.as_ref().ok_or("metric document")?["classes"]["class-a"]["f1"]
                .as_f64(),
            Some(0.75)
        );
        let artifact = member.attempts[0]
            .artifacts
            .first()
            .ok_or("current model")?;
        assert_eq!(
            fs::read(
                artifact.metadata["published_path"]
                    .as_str()
                    .ok_or("current artifact path")?
            )?,
            (index + 301).to_string().as_bytes()
        );
    }
    let output = fs::read(root.join("RESULTS.md"))?;
    assert_eq!(final_run.provider, "igor");
    assert_eq!(final_run.model, "deterministic-format-v1");
    assert_eq!(
        final_run.output_hash,
        Some(format!("sha256:{:x}", Sha256::digest(&output)))
    );
    let markdown = std::str::from_utf8(&output)?;
    assert_eq!(
        markdown.split_once('\n').ok_or("ownership claim")?.1,
        igor_core::render_report_markdown(final_context)?
    );
    let primary = markdown
        .split("## Primary metric series\n\n")
        .nth(1)
        .ok_or("primary section")?
        .split("## Current-generation attempts")
        .next()
        .ok_or("attempt section")?;
    for old_job in first_jobs.iter().chain(&second_jobs) {
        let detail = runtime
            .block_on(database.jobs().detail(old_job.parse()?))?
            .ok_or("historical job")?;
        for attempt in detail.attempts {
            assert!(!primary.contains(&attempt.spec.id().to_string()));
        }
    }
    for old_value in [101, 102, 103, 104, 105, 201, 202, 204, 205] {
        assert!(!primary.contains(&format!("| {old_value} |")));
    }

    // M13 retires superseded copies only after a comparable replacement, preserving history.
    wait(|| {
        let removed: i64 = runtime.block_on(sqlx::query_scalar("SELECT count(*) FROM artifacts WHERE attempt_id IN (SELECT t.id FROM attempts t JOIN jobs j ON j.id = t.job_id WHERE j.generation_id = ?) AND state = 'removed'").bind(first_generation.to_string()).fetch_one(database.pool()))?;
        Ok(removed == 5)
    })?;
    for (id, path, _) in &old_artifacts {
        assert!(!path.exists());
        assert!(runtime.block_on(database.artifacts().get(*id))?.is_some());
    }
    let mut expected_old = first_run.clone();
    expected_old.state = ReportState::Superseded;
    assert_eq!(
        runtime.block_on(database.actions().report_run(first_run.id))?,
        Some(expected_old)
    );
    let storage = successful(
        command(&home, &root)
            .args(["storage", "status", "--json"])
            .output()?,
    )?;
    assert!(storage.is_object());
    successful(
        command(&home, &root)
            .args(["gc", "--dry-run", "--json"])
            .output()?,
    )?;
    assert_eq!(fs::read(root.join("RESULTS.md"))?, output);
    let logs = command(&home, &root)
        .args(["logs", &third_jobs[0]])
        .output()?;
    assert!(logs.status.success());
    assert!(String::from_utf8(logs.stdout)?.contains("301"));
    assert_eq!(
        runtime.block_on(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
                .fetch_one(database.pool())
        )?,
        16
    );
    assert_eq!(
        runtime.block_on(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM report_runs WHERE generation_id = ?"
            )
            .bind(second_generation.to_string())
            .fetch_one(database.pool())
        )?,
        0
    );
    for (path, bytes) in protected {
        assert_eq!(fs::read(root.join(path))?, bytes, "protected {path}");
    }
    supervisor.stop()?;
    worker.stop()?;
    runtime.block_on(database.pool().close());
    Ok(())
}
