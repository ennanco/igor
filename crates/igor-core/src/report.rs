use serde_json::Value;
use thiserror::Error;

use crate::{
    ArtifactLifecycleState, ArtifactPublicationState, ArtifactRole, GenerationStatus,
    REPORT_CONTEXT_VERSION, ReportAttempt, ReportContext, ReportMember, SourceIdentity,
    TransitionState,
};

#[derive(Debug, Error)]
pub enum ReportRenderError {
    #[error("unsupported report context version {found}; supported version is {supported}")]
    UnsupportedContextVersion { found: u32, supported: u32 },
    #[error("report context contains a nonfinite scalar observation")]
    NonFiniteMetric,
    #[error("cannot render report evidence: {0}")]
    Evidence(#[from] serde_json::Error),
}

/// Renders context v1 without I/O. Rendering provisional text does not authorize publication.
pub fn render_report_markdown(context: &ReportContext) -> Result<String, ReportRenderError> {
    if context.schema_version != REPORT_CONTEXT_VERSION {
        return Err(ReportRenderError::UnsupportedContextVersion {
            found: context.schema_version,
            supported: REPORT_CONTEXT_VERSION,
        });
    }
    let mut out = String::from("<!-- igor:generated-report:v1 -->\n# Experiment results\n\n");
    let ready = context_ready(context);
    out.push_str(if ready {
        "Status: **Ready**\n\n"
    } else {
        "Status: **Provisional**\n\n"
    });
    out.push_str("## Family and generation\n\n");
    table(&mut out, &["Field", "Value"]);
    let generation = &context.generation.identity;
    for (key, value) in [
        ("Family", context.family.name.clone()),
        ("Family ID", context.family.id.to_string()),
        ("Project ID", context.family.project_id.to_string()),
        ("Generation", generation.number.to_string()),
        ("Generation ID", generation.id.to_string()),
        ("Git revision", generation.source_revision.clone()),
        ("Protocol digest", generation.protocol_digest.clone()),
        (
            "Generation status",
            generation_status(context.generation.status).into(),
        ),
    ] {
        row(&mut out, &[key.to_owned(), value]);
    }
    out.push_str("\n## Seed status\n\n");
    table(
        &mut out,
        &[
            "Seed",
            "Job",
            "Job state",
            "Latest attempt",
            "Extraction",
            "Document status",
        ],
    );
    let mut seeds: Vec<_> = context.required_seeds.iter().map(|seed| seed.0).collect();
    seeds.sort_unstable();
    for seed in seeds {
        if let Some(member) = context.members.iter().find(|member| member.seed.0 == seed) {
            let latest = member
                .attempts
                .iter()
                .max_by_key(|attempt| attempt.sequence);
            row(
                &mut out,
                &[
                    seed.to_string(),
                    member.job_id.to_string(),
                    member.state.as_str().into(),
                    latest.map_or_else(|| "none".into(), |attempt| attempt.sequence.to_string()),
                    latest.map_or_else(
                        || "not available".into(),
                        |attempt| extraction_status(attempt).into(),
                    ),
                    latest
                        .and_then(|attempt| attempt.metrics.document.as_ref())
                        .and_then(|document| document["status"].as_str())
                        .unwrap_or("not available")
                        .into(),
                ],
            );
        } else {
            row(
                &mut out,
                &[
                    seed.to_string(),
                    "none".into(),
                    "missing".into(),
                    "none".into(),
                    "not available".into(),
                    "not available".into(),
                ],
            );
        }
    }
    out.push_str("\n## Primary metric series\n\n");
    if ready {
        render_primary(&mut out, context)?;
    } else {
        out.push_str(
            "Primary metric series are unavailable while the current context is provisional.\n\n",
        );
    }
    out.push_str("## Current-generation attempts\n\n");
    let mut members: Vec<_> = context.members.iter().collect();
    members.sort_by_key(|member| (member.seed.0, member.job_id));
    for member in members {
        render_member(&mut out, member)?;
    }
    render_history(&mut out, context);
    render_repairs(&mut out, context)?;
    out.push_str("## Tool provenance\n\n");
    table(&mut out, &["Tool", "Version"]);
    row(
        &mut out,
        &["Renderer".into(), "deterministic (format v1)".into()],
    );
    row(
        &mut out,
        &[
            "Igor context builder".into(),
            context.tools.igor_context_builder.clone(),
        ],
    );
    for (name, version) in [
        ("Extractor", &context.tools.extractor),
        ("Prompt", &context.tools.prompt),
        ("Provider", &context.tools.provider),
        ("Model", &context.tools.model),
    ] {
        row(
            &mut out,
            &[
                name.into(),
                version
                    .as_deref()
                    .unwrap_or("unknown (not recorded)")
                    .into(),
            ],
        );
    }
    Ok(out)
}

pub(crate) fn context_ready(context: &ReportContext) -> bool {
    context.ready
        && context.generation.status == GenerationStatus::Comparable
        && context.primary_metrics.is_some()
}

fn render_primary(out: &mut String, context: &ReportContext) -> Result<(), ReportRenderError> {
    let mut series: Vec<_> = context.primary_metrics.iter().flatten().collect();
    series.sort_by(|left, right| left.name.cmp(&right.name));
    if series.iter().all(|series| series.values.is_empty()) {
        out.push_str("No primary scalar metrics were supplied.\n\n");
        return Ok(());
    }
    out.push_str("Per-seed extractor observations; no cross-seed statistics are inferred.\n\n");
    table(out, &["Metric", "Seed", "Attempt ID", "Value"]);
    for series in series {
        let mut values: Vec<_> = series.values.iter().collect();
        values.sort_by_key(|value| (value.seed.0, value.attempt_id));
        for value in values {
            if !value.value.is_finite() {
                return Err(ReportRenderError::NonFiniteMetric);
            }
            row(
                out,
                &[
                    series.name.clone(),
                    value.seed.0.to_string(),
                    value.attempt_id.to_string(),
                    value.value.to_string(),
                ],
            );
        }
    }
    out.push('\n');
    Ok(())
}

fn render_member(out: &mut String, member: &ReportMember) -> Result<(), ReportRenderError> {
    out.push_str(&format!("### Seed {}\n\n", member.seed.0));
    table(out, &["Field", "Value"]);
    row(out, &["Job".into(), member.name.clone()]);
    row(out, &["Job ID".into(), member.job_id.to_string()]);
    out.push_str(&format!(
        "\nLatest-job logs: `igor logs {}`.\n\n",
        member.job_id
    ));
    let mut attempts: Vec<_> = member.attempts.iter().collect();
    attempts.sort_by_key(|attempt| (attempt.sequence, attempt.attempt_id));
    let latest = attempts.last().map(|attempt| attempt.attempt_id);
    if attempts.is_empty() {
        out.push_str("No attempts recorded.\n\n");
    }
    for attempt in attempts {
        let scope = if Some(attempt.attempt_id) == latest {
            "latest"
        } else {
            "historical retry; excluded from primary series"
        };
        out.push_str(&format!("#### Attempt {} ({scope})\n\n", attempt.sequence));
        table(out, &["Field", "Value"]);
        for (key, value) in [
            ("Attempt ID", attempt.attempt_id.to_string()),
            ("State", attempt.state.as_str().into()),
            (
                "Project configuration digest",
                attempt.configuration.project_digest.clone(),
            ),
            (
                "Job configuration digest",
                attempt.configuration.job_digest.clone(),
            ),
            (
                "Metric schema version",
                attempt.metric_schema_version.to_string(),
            ),
            ("Extraction", extraction_status(attempt).into()),
        ] {
            row(out, &[key.into(), value]);
        }
        if let Some(error) = &attempt.metrics.last_error {
            row(out, &["Extraction error".into(), error.clone()]);
        }
        if let Some(extractor) = &attempt.extractor {
            row(
                out,
                &["Extractor program".into(), extractor.program.clone()],
            );
            row(
                out,
                &[
                    "Extractor command digest".into(),
                    extractor.command_sha256.clone(),
                ],
            );
        }
        match &attempt.source {
            SourceIdentity::Git(git) => {
                for (key, value) in [
                    ("Git repository", git.repository.clone()),
                    ("Git root", git.root.to_string_lossy().into_owned()),
                    (
                        "Git branch",
                        git.branch
                            .as_deref()
                            .unwrap_or("unknown (not recorded)")
                            .into(),
                    ),
                    ("Git revision", git.revision.clone()),
                    ("Git dirty", git.dirty.to_string()),
                    (
                        "Git dirty digest",
                        git.dirty_digest.as_deref().unwrap_or("none").into(),
                    ),
                ] {
                    row(out, &[key.into(), value]);
                }
            }
            SourceIdentity::GitRevision(revision) => {
                row(out, &["Git revision".into(), revision.clone()])
            }
            SourceIdentity::SnapshotDigest(digest) => {
                row(out, &["Snapshot digest".into(), digest.clone()])
            }
        }
        out.push('\n');
        if !attempt.configuration.contents.is_empty() {
            out.push_str("Configuration and input identities:\n\n");
            table(out, &["Path", "Role", "Bytes", "SHA-256"]);
            let mut contents: Vec<_> = attempt.configuration.contents.iter().collect();
            contents.sort_by(|left, right| (&left.path, left.role).cmp(&(&right.path, right.role)));
            for content in contents {
                row(
                    out,
                    &[
                        content.path.to_string_lossy().into_owned(),
                        match content.role {
                            crate::ContentRole::ScientificConfiguration => {
                                "scientific_configuration"
                            }
                            crate::ContentRole::ImmutableInput => "immutable_input",
                        }
                        .into(),
                        content.size_bytes.to_string(),
                        content.sha256.clone(),
                    ],
                );
            }
            out.push('\n');
        }
        if let Some(document) = &attempt.metrics.document {
            json_evidence(
                out,
                "Extractor document (including selection and per-class metrics)",
                document,
            )?;
        } else {
            out.push_str("No structured metric document recorded.\n\n");
        }
        if let Some(selection) = &attempt.metrics.selection {
            json_evidence(out, "Indexed selection metadata", selection)?;
        }
        render_artifacts(out, attempt);
    }
    Ok(())
}

fn render_artifacts(out: &mut String, attempt: &ReportAttempt) {
    if attempt.artifacts.is_empty() {
        out.push_str("No registered artifacts.\n\n");
        return;
    }
    out.push_str("Artifacts:\n\n");
    table(
        out,
        &[
            "Path",
            "Role",
            "Physical state",
            "Lifecycle",
            "Bytes",
            "SHA-256",
            "Link",
        ],
    );
    let mut artifacts: Vec<_> = attempt.artifacts.iter().collect();
    artifacts.sort_by(|left, right| (&left.path, left.id).cmp(&(&right.path, right.id)));
    for artifact in artifacts {
        let role = match &artifact.role {
            ArtifactRole::Metrics => "metrics".into(),
            ArtifactRole::Model => "model".into(),
            ArtifactRole::Checkpoint => "checkpoint".into(),
            ArtifactRole::Figure => "figure".into(),
            ArtifactRole::Log => "log".into(),
            ArtifactRole::CrashDump => "crash_dump".into(),
            ArtifactRole::Report => "report".into(),
            ArtifactRole::Other(role) => format!("other: {role}"),
        };
        // Cells are escaped before appending the one deliberately generated link cell.
        let cells = [
            artifact.path.clone(),
            role,
            artifact.state.as_str().into(),
            artifact.lifecycle.as_str().into(),
            artifact
                .size_bytes
                .map_or_else(|| "unknown".into(), |size| size.to_string()),
            artifact.sha256.as_deref().unwrap_or("unknown").into(),
        ];
        out.push('|');
        for cell in cells {
            out.push_str(&format!(" {} |", escape(&cell)));
        }
        let target = (artifact.state == ArtifactPublicationState::Published
            && matches!(
                artifact.lifecycle,
                ArtifactLifecycleState::Published | ArtifactLifecycleState::Retained
            ))
        .then(|| {
            artifact.metadata["published_path"]
                .as_str()
                .unwrap_or(&artifact.path)
        })
        .and_then(local_file_uri);
        if let Some(uri) = target {
            out.push_str(&format!(" [Open artifact](<{uri}>) |\n"));
        } else {
            out.push_str(" unavailable |\n");
        }
    }
    out.push('\n');
}

fn render_history(out: &mut String, context: &ReportContext) {
    out.push_str("## Historical generations\n\n");
    if context.history.is_empty() {
        out.push_str("No earlier generations recorded.\n\n");
        return;
    }
    out.push_str("Excluded from primary metric series.\n\n");
    table(
        out,
        &[
            "Generation",
            "ID",
            "Git revision",
            "Protocol digest",
            "Status",
            "Job counts",
        ],
    );
    let mut history: Vec<_> = context.history.iter().collect();
    history.sort_by_key(|generation| (generation.identity.number, generation.identity.id));
    for generation in history {
        let counts = generation
            .counts
            .iter()
            .map(|(state, count)| format!("{state}: {count}"))
            .collect::<Vec<_>>()
            .join("; ");
        row(
            out,
            &[
                generation.identity.number.to_string(),
                generation.identity.id.to_string(),
                generation.identity.source_revision.clone(),
                generation.identity.protocol_digest.clone(),
                generation_status(generation.status).into(),
                counts,
            ],
        );
    }
    out.push('\n');
}

fn render_repairs(out: &mut String, context: &ReportContext) -> Result<(), ReportRenderError> {
    out.push_str("## Repairs\n\n");
    let mut repairs: Vec<_> = context.repairs.iter().collect();
    repairs.sort_by_key(|repair| (repair.generation_id, repair.recovery_id));
    if repairs.is_empty() {
        out.push_str("No repairs recorded.\n\n");
    }
    for repair in repairs {
        out.push_str(&format!("### Recovery {}\n\n", repair.recovery_id));
        table(out, &["Field", "Value"]);
        row(
            out,
            &["Generation ID".into(), repair.generation_id.to_string()],
        );
        row(
            out,
            &[
                "Scope".into(),
                if repair.generation_id == context.generation.identity.id {
                    "current generation"
                } else {
                    "historical generation"
                }
                .into(),
            ],
        );
        row(out, &["Attempt ID".into(), repair.attempt_id.to_string()]);
        row(out, &["State".into(), repair.state.clone()]);
        out.push('\n');
        for (label, evidence) in [
            ("Diagnosis", &repair.diagnosis),
            ("Patch metadata", &repair.patch),
            ("Validation", &repair.validation),
            ("Decision", &repair.decision),
            ("Worktree evidence", &repair.worktree),
        ] {
            if let Some(evidence) = evidence {
                json_evidence(out, label, evidence)?;
            }
        }
    }
    Ok(())
}

fn generation_status(status: GenerationStatus) -> &'static str {
    match status {
        GenerationStatus::Incomplete => "incomplete",
        GenerationStatus::Complete => "complete (not comparable)",
        GenerationStatus::Comparable => "comparable",
        GenerationStatus::Invalid => "invalid",
    }
}

fn extraction_status(attempt: &ReportAttempt) -> &str {
    attempt
        .metrics
        .action_state
        .as_deref()
        .unwrap_or(if attempt.extractor.is_some() {
            "pending (not yet enqueued)"
        } else {
            "not configured"
        })
}

fn table(out: &mut String, headers: &[&str]) {
    row(
        out,
        &headers
            .iter()
            .map(|header| (*header).into())
            .collect::<Vec<_>>(),
    );
    out.push('|');
    for _ in headers {
        out.push_str(" --- |");
    }
    out.push('\n');
}

fn row(out: &mut String, cells: &[String]) {
    out.push('|');
    for cell in cells {
        out.push_str(&format!(" {} |", escape(cell)));
    }
    out.push('\n');
}

fn escape(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '\n' => escaped.push_str("<br>"),
            '\r' => escaped.push_str("&#13;"),
            '\\' | '`' | '*' | '_' | '[' | ']' | '!' | '|' | '#' | '~' => {
                escaped.push_str(&format!("&#{};", u32::from(character)))
            }
            control if control.is_control() => {
                escaped.push_str(&format!("&#{};", u32::from(control)))
            }
            other => escaped.push(other),
        }
    }
    escaped
}

fn local_file_uri(path: &str) -> Option<String> {
    if !path.starts_with('/') || path.starts_with("//") {
        return None;
    }
    let mut uri = String::from("file://");
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || b"/.-_~".contains(&byte) {
            uri.push(char::from(byte));
        } else {
            uri.push_str(&format!("%{byte:02X}"));
        }
    }
    Some(uri)
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        Value::Array(array) => Value::Array(array.iter().map(canonical_json).collect()),
        other => other.clone(),
    }
}

fn json_evidence(out: &mut String, label: &str, value: &Value) -> Result<(), ReportRenderError> {
    let json = serde_json::to_string_pretty(&canonical_json(value))?;
    let run = json
        .split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
    let fence = "`".repeat(3.max(run + 1));
    out.push_str(&format!("{label}:\n\n{fence}json\n{json}\n{fence}\n\n"));
    Ok(())
}
