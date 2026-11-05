use std::error::Error;

use igor_core::{GenerationStatus, ReportContext, ReportRenderError, render_report_markdown};
use serde_json::{Value, json};

type TestResult = Result<(), Box<dyn Error>>;

#[path = "report/golden.rs"]
mod golden;

fn id(number: u32) -> String {
    format!("00000000-0000-0000-0000-{number:012}")
}

fn attempt(number: u32, sequence: u32, value: f64) -> Value {
    json!({
        "attempt_id": id(number), "sequence": sequence, "state": "succeeded",
        "source": {"kind":"git", "identity": {"repository":"https://example.invalid/research.git",
            "root":"/fixture/project", "branch":"main", "revision":"current-revision", "dirty":false}},
        "configuration": {"project_digest":"sha256:project", "job_digest":"sha256:job",
            "contents":[{"path":"config/model.toml", "role":"scientific_configuration", "sha256":"sha256:config", "size_bytes":42}]},
        "metric_schema_version":1, "extractor":{"program":"python3", "command_sha256":"sha256:extractor"},
        "metrics": {"attempt_id":id(number), "action_state":"succeeded", "last_error":null,
            "document":{"schema_version":1, "status":"success", "metrics":{"validation.score":value},
                "selection":{"split":"validation", "metric":"score", "value":value, "epoch":4},
                "classes":{"class-a":{"f1":0.75}}},
            "selection":{"split":"validation", "metric":"score", "value":value, "epoch":4},
            "metrics":[["validation.score",value]]},
        "artifacts":[]
    })
}

fn context() -> Result<ReportContext, Box<dyn Error>> {
    Ok(serde_json::from_value(json!({
        "schema_version":1,
        "family":{"id":id(1), "project_id":id(2), "name":"A scientific family"},
        "generation":{"identity":{"id":id(3), "number":2, "source_revision":"current-revision", "protocol_digest":"sha256:protocol"},
            "status":"comparable", "counts":{"succeeded":2}},
        "required_seeds":[10,2],
        "members":[
            {"job_id":id(10), "name":"seed ten", "seed":10, "state":"succeeded", "attempts":[attempt(11,1,0.625)]},
            {"job_id":id(20), "name":"seed two", "seed":2, "state":"succeeded", "attempts":[attempt(22,2,0.875),attempt(21,1,9999.0)]}
        ],
        "history":[{"identity":{"id":id(4), "number":1, "source_revision":"old-revision", "protocol_digest":"sha256:protocol"},
            "status":"complete", "counts":{"superseded":2}}],
        "repairs":[{"recovery_id":id(30), "generation_id":id(4), "attempt_id":id(21), "state":"succeeded",
            "diagnosis":{"reason":"old failure"}, "validation":{"passed":true}, "patch":{"sha256":"sha256:patch"},
            "decision":null, "worktree":{"state":"removed", "path":"/fixture/worktree"}}],
        "tools":{"igor_context_builder":"0.1.0", "extractor":null, "prompt":null, "provider":null, "model":null},
        "ready":true,
        "primary_metrics":[{"name":"validation.score", "values":[
            {"seed":10, "attempt_id":id(11), "value":0.625},
            {"seed":2, "attempt_id":id(22), "value":0.875}]}]
    }))?)
}

fn primary(markdown: &str) -> Result<&str, Box<dyn Error>> {
    Ok(markdown
        .split("## Primary metric series\n\n")
        .nth(1)
        .ok_or("missing primary section")?
        .split("## Current-generation attempts")
        .next()
        .ok_or("missing attempts section")?)
}

#[test]
fn report_is_byte_stable_and_keeps_primary_results_separate_from_history() -> TestResult {
    let context = context()?;
    let original = serde_json::to_vec(&context)?;
    let markdown = render_report_markdown(&context)?;
    assert_eq!(markdown, render_report_markdown(&context)?);
    let round_trip: ReportContext = serde_json::from_slice(&original)?;
    assert_eq!(markdown, render_report_markdown(&round_trip)?);
    assert_eq!(original, serde_json::to_vec(&context)?);
    assert!(markdown.starts_with(
        "<!-- igor:generated-report:v1 -->\n# Experiment results\n\nStatus: **Ready**\n"
    ));
    let expected = format!(
        "| validation.score | 2 | {} | 0.875 |\n| validation.score | 10 | {} | 0.625 |",
        id(22),
        id(11)
    );
    assert!(primary(&markdown)?.contains(&expected));
    assert!(!primary(&markdown)?.contains("9999"));
    assert!(!primary(&markdown)?.contains("old-revision"));
    assert!(markdown.contains("Attempt 1 (historical retry; excluded from primary series)"));
    assert!(markdown.contains("9999.0"));
    assert!(markdown.contains("## Historical generations\n\nExcluded from primary metric series."));
    assert!(markdown.contains("| Scope | historical generation |"));
    assert!(markdown.contains("| Project configuration digest | sha256:project |"));
    assert!(
        markdown
            .contains("| config/model.toml | scientific&#95;configuration | 42 | sha256:config |")
    );
    assert!(markdown.contains("\"class-a\""));
    assert!(markdown.contains("\"split\": \"validation\""));
    assert!(markdown.contains("| Extractor | unknown (not recorded) |"));
    assert!(markdown.contains(&format!("Latest-job logs: `igor logs {}`.", id(20))));
    assert!(markdown.ends_with('\n'));
    Ok(())
}

#[test]
fn equivalent_collection_order_produces_identical_markdown() -> TestResult {
    let mut context = context()?;
    let expected = render_report_markdown(&context)?;
    context.required_seeds.reverse();
    context.members.reverse();
    for member in &mut context.members {
        member.attempts.reverse();
    }
    for series in context.primary_metrics.as_mut().ok_or("primary")? {
        series.values.reverse();
    }
    assert_eq!(expected, render_report_markdown(&context)?);
    Ok(())
}

#[test]
fn provisional_or_noncomparable_context_never_exposes_primary_series() -> TestResult {
    let mut context = context()?;
    for status in [
        GenerationStatus::Comparable,
        GenerationStatus::Incomplete,
        GenerationStatus::Complete,
        GenerationStatus::Invalid,
    ] {
        context.generation.status = status;
        context.ready = status != GenerationStatus::Comparable;
        let markdown = render_report_markdown(&context)?;
        assert!(markdown.contains("Status: **Provisional**"));
        assert!(primary(&markdown)?.contains("unavailable"));
        assert!(!primary(&markdown)?.contains("0.875"));
    }
    context.ready = false;
    context.members.pop();
    let markdown = render_report_markdown(&context)?;
    assert!(markdown.contains("| 2 | none | missing | none | not available | not available |"));
    Ok(())
}

#[test]
fn no_extractor_and_failed_extraction_remain_distinct_from_job_failure() -> TestResult {
    let mut context = context()?;
    let member = &mut context.members[0];
    let attempt = &mut member.attempts[0];
    attempt.extractor = None;
    attempt.metrics.action_state = None;
    attempt.metrics.document = None;
    attempt.metrics.selection = None;
    attempt.metrics.metrics.clear();
    context.primary_metrics = Some(vec![]);
    let no_extractor = render_report_markdown(&context)?;
    assert!(no_extractor.contains("not configured"));
    assert!(no_extractor.contains("No primary scalar metrics were supplied."));
    let attempt = &mut context.members[0].attempts[0];
    attempt.metrics.action_state = Some("failed".into());
    attempt.metrics.last_error = Some("invalid <JSON> | retry\nrequested".into());
    let failed = render_report_markdown(&context)?;
    assert!(failed.contains("Status: **Ready**"));
    assert!(failed.contains("| Extraction | failed |"));
    assert!(failed.contains("| State | succeeded |"));
    assert!(failed.contains("invalid &lt;JSON&gt; &#124; retry<br>requested"));
    Ok(())
}

#[test]
fn markdown_text_and_json_fences_cannot_escape_their_sections() -> TestResult {
    let mut context = context()?;
    context.family.name = "[x](javascript:bad) | <script>\n# injected".into();
    context.members[0].name = "`code` *bold* \\ [link]".into();
    context.repairs[0].diagnosis = Some(json!({"reason":"````\n# injected\n<script>"}));
    let markdown = render_report_markdown(&context)?;
    assert!(
        markdown.contains("&#91;x&#93;(javascript:bad) &#124; &lt;script&gt;<br>&#35; injected")
    );
    assert!(!markdown.contains("\n# injected"));
    assert!(!markdown.contains("[x](javascript:"));
    assert!(markdown.contains("&#96;code&#96; &#42;bold&#42; &#92; &#91;link&#93;"));
    assert!(markdown.contains("`````json\n"));
    assert!(markdown.contains("\n`````\n"));
    Ok(())
}

#[test]
fn only_published_retained_artifacts_get_encoded_local_links() -> TestResult {
    let mut value = serde_json::to_value(context()?)?;
    value["members"][0]["attempts"][0]["artifacts"] = json!([
        {"id":id(40), "attempt_id":id(11), "path":"model|<x>.bin", "role":"model", "sha256":"sha256:model",
            "size_bytes":12, "state":"published", "lifecycle":"retained", "diagnosis_required":false,
            "metadata":{"published_path":"/fixture/result (x)[1]é#?.bin"}},
        {"id":id(41), "attempt_id":id(11), "path":"removed.bin", "role":"checkpoint", "sha256":null,
            "size_bytes":null, "state":"removed", "lifecycle":"removed", "diagnosis_required":false,
            "metadata":{"published_path":"/fixture/removed.bin"}},
        {"id":id(42), "attempt_id":id(11), "path":"staged.bin", "role":"model", "sha256":null,
            "size_bytes":null, "state":"staged", "lifecycle":"published", "diagnosis_required":false,
            "metadata":{"published_path":"/fixture/staged.bin"}},
        {"id":id(43), "attempt_id":id(11), "path":"bad-target.bin", "role":"model", "sha256":null,
            "size_bytes":null, "state":"published", "lifecycle":"retained", "diagnosis_required":false,
            "metadata":{"published_path":"javascript:alert(1)"}},
        {"id":id(44), "attempt_id":id(11), "path":"relative.bin", "role":"model", "sha256":null,
            "size_bytes":null, "state":"published", "lifecycle":"retained", "diagnosis_required":false,
            "metadata":{}},
        {"id":id(45), "attempt_id":id(11), "path":"/fixture/legacy.bin", "role":"model", "sha256":null,
            "size_bytes":null, "state":"published", "lifecycle":"published", "diagnosis_required":false,
            "metadata":{}}
    ]);
    let context: ReportContext = serde_json::from_value(value)?;
    let markdown = render_report_markdown(&context)?;
    assert_eq!(markdown.matches("[Open artifact]").count(), 2);
    assert!(markdown.contains("[Open artifact](<file:///fixture/legacy.bin>)"));
    assert!(markdown.contains(
        "| relative.bin | model | published | retained | unknown | unknown | unavailable |"
    ));
    assert!(
        markdown
            .contains("[Open artifact](<file:///fixture/result%20%28x%29%5B1%5D%C3%A9%23%3F.bin>)")
    );
    assert!(markdown.contains("model&#124;&lt;x&gt;.bin"));
    assert!(markdown.contains(
        "| removed.bin | checkpoint | removed | removed | unknown | unknown | unavailable |"
    ));
    assert!(!markdown.contains("file:///fixture/removed.bin"));
    assert!(!markdown.contains("file:///fixture/staged.bin"));
    assert!(!markdown.contains("javascript:alert"));
    Ok(())
}

#[test]
fn unsupported_context_versions_and_nonfinite_primary_values_are_errors() -> TestResult {
    let mut context = context()?;
    context.schema_version = 2;
    assert!(matches!(
        render_report_markdown(&context),
        Err(ReportRenderError::UnsupportedContextVersion {
            found: 2,
            supported: 1
        })
    ));
    context.schema_version = 1;
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        context.primary_metrics.as_mut().ok_or("primary")?[0].values[0].value = value;
        assert!(matches!(
            render_report_markdown(&context),
            Err(ReportRenderError::NonFiniteMetric)
        ));
    }
    Ok(())
}
