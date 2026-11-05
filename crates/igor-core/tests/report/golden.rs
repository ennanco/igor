use super::*;

fn complete() -> Result<ReportContext, Box<dyn Error>> {
    let mut value = serde_json::to_value(context()?)?;
    value["family"]["name"] = json!("Scientific | family <baseline>");
    value["generation"]["identity"]["number"] = json!(1);
    value["history"] = json!([]);
    value["repairs"] = json!([]);
    value["members"][1]["attempts"] = json!([attempt(22, 1, 0.875)]);
    value["members"][0]["attempts"][0]["artifacts"] = json!([
        {"id":id(40), "attempt_id":id(11), "path":"model [best].bin", "role":"model",
            "sha256":"sha256:model", "size_bytes":12, "state":"published", "lifecycle":"retained",
            "diagnosis_required":false, "metadata":{"published_path":"/fixture/artifacts/model [best].bin"}},
        {"id":id(41), "attempt_id":id(11), "path":"checkpoint.bin", "role":"checkpoint",
            "sha256":"sha256:checkpoint", "size_bytes":8, "state":"removed", "lifecycle":"removed",
            "diagnosis_required":false, "metadata":{"published_path":"/fixture/artifacts/checkpoint.bin"}}
    ]);
    Ok(serde_json::from_value(value)?)
}

fn failed_attempt() -> Value {
    let mut value = attempt(21, 1, 9999.0);
    value["state"] = json!("failed");
    value["metrics"] = json!({"attempt_id":id(21), "action_state":"failed",
        "last_error":"missing metrics | training <failed>\ninspect logs",
        "document":null, "selection":null, "metrics":[]});
    value
}

fn failed() -> Result<ReportContext, Box<dyn Error>> {
    let mut value = serde_json::to_value(complete()?)?;
    value["generation"]["status"] = json!("complete");
    value["generation"]["counts"] = json!({"succeeded":1,"failed":1});
    value["members"][1]["state"] = json!("failed");
    value["members"][1]["attempts"] = json!([failed_attempt()]);
    value["ready"] = json!(false);
    value["primary_metrics"] = Value::Null;
    Ok(serde_json::from_value(value)?)
}

fn repaired() -> Result<ReportContext, Box<dyn Error>> {
    let mut value = serde_json::to_value(complete()?)?;
    value["members"][1]["attempts"] = json!([attempt(22, 2, 0.875), failed_attempt()]);
    value["repairs"] = json!([{
        "recovery_id":id(30), "generation_id":id(3), "attempt_id":id(21), "state":"succeeded",
        "diagnosis":{"reason":"missing configuration\n````\n# evidence, not a heading"},
        "patch":{"sha256":"sha256:patch", "path":"repairs/fix.patch"},
        "validation":{"passed":true, "command":["cargo", "test"]},
        "decision":{"outcome":"retry", "attempt_id":id(22)},
        "worktree":{"state":"removed", "path":"/fixture/worktree"}
    }]);
    Ok(serde_json::from_value(value)?)
}

fn superseded() -> Result<ReportContext, Box<dyn Error>> {
    let mut value = serde_json::to_value(complete()?)?;
    value["generation"]["identity"]["number"] = json!(3);
    value["history"] = json!([
        {"identity":{"id":id(5), "number":2, "source_revision":"replaced-revision", "protocol_digest":"sha256:protocol"},
            "status":"complete", "counts":{"superseded":2}},
        {"identity":{"id":id(4), "number":1, "source_revision":"old-revision", "protocol_digest":"sha256:old-protocol"},
            "status":"complete", "counts":{"failed":1,"superseded":1}}
    ]);
    value["repairs"] = json!([{
        "recovery_id":id(30), "generation_id":id(4), "attempt_id":id(31), "state":"failed",
        "diagnosis":{"reason":"historical failure", "old_score":9999.0},
        "patch":{"sha256":"sha256:old-patch"}, "validation":{"passed":false},
        "decision":{"outcome":"supersede"}, "worktree":null
    }]);
    Ok(serde_json::from_value(value)?)
}

fn assert_golden(context: ReportContext, expected: &str) -> TestResult {
    let original = serde_json::to_vec(&context)?;
    assert_eq!(render_report_markdown(&context)?, expected);
    let mut reordered: ReportContext = serde_json::from_slice(&original)?;
    reordered.required_seeds.reverse();
    reordered.members.reverse();
    reordered.history.reverse();
    reordered.repairs.reverse();
    for member in &mut reordered.members {
        member.attempts.reverse();
        for attempt in &mut member.attempts {
            attempt.artifacts.reverse();
            attempt.configuration.contents.reverse();
        }
    }
    if let Some(series) = &mut reordered.primary_metrics {
        series.reverse();
        for series in series {
            series.values.reverse();
        }
    }
    assert_eq!(render_report_markdown(&reordered)?, expected);
    assert_eq!(serde_json::to_vec(&context)?, original);
    Ok(())
}

#[test]
fn complete_family_matches_golden() -> TestResult {
    let context = complete()?;
    let markdown = render_report_markdown(&context)?;
    assert!(markdown.contains("Status: **Ready**"));
    assert!(primary(&markdown)?.contains("| validation.score | 2 |"));
    assert!(primary(&markdown)?.contains("| validation.score | 10 |"));
    assert_eq!(markdown.matches("[Open artifact]").count(), 1);
    assert!(!markdown.contains("file:///fixture/artifacts/checkpoint.bin"));
    assert!(markdown.contains("Scientific &#124; family &lt;baseline&gt;"));
    assert_golden(context, include_str!("golden/complete.md"))
}

#[test]
fn failed_family_matches_provisional_golden() -> TestResult {
    let context = failed()?;
    let markdown = render_report_markdown(&context)?;
    assert!(markdown.contains("Status: **Provisional**"));
    assert!(markdown.contains("| Generation status | complete (not comparable) |"));
    assert!(primary(&markdown)?.contains("unavailable"));
    assert!(!primary(&markdown)?.contains("validation.score"));
    assert!(markdown.contains("| State | failed |"));
    assert!(markdown.contains("missing metrics &#124; training &lt;failed&gt;<br>inspect logs"));
    assert_golden(context, include_str!("golden/failed.md"))
}

#[test]
fn repaired_family_matches_golden_with_failed_attempt_history() -> TestResult {
    let context = repaired()?;
    let markdown = render_report_markdown(&context)?;
    assert!(markdown.contains("Status: **Ready**"));
    assert!(primary(&markdown)?.contains(&id(22)));
    assert!(!primary(&markdown)?.contains(&id(21)));
    assert!(markdown.contains("Attempt 1 (historical retry; excluded from primary series)"));
    assert!(markdown.contains("| Scope | current generation |"));
    assert!(markdown.contains("`````json\n"));
    assert!(!markdown.contains("\n# evidence, not a heading"));
    assert_golden(context, include_str!("golden/repaired.md"))
}

#[test]
fn superseded_family_matches_golden_with_separate_history() -> TestResult {
    let context = superseded()?;
    let markdown = render_report_markdown(&context)?;
    assert!(markdown.contains("Status: **Ready**"));
    for excluded in ["old-revision", "replaced-revision", "9999", "old-patch"] {
        assert!(!primary(&markdown)?.contains(excluded));
        assert!(markdown.contains(excluded));
    }
    assert!(markdown.contains("| Scope | historical generation |"));
    assert!(markdown.contains("Excluded from primary metric series."));
    assert_golden(context, include_str!("golden/superseded.md"))
}

#[test]
#[ignore = "explicitly regenerate snapshots, then review every Markdown diff"]
fn regenerate_report_goldens() -> TestResult {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/report/golden");
    for (name, context) in [
        ("complete", complete()?),
        ("failed", failed()?),
        ("repaired", repaired()?),
        ("superseded", superseded()?),
    ] {
        std::fs::write(
            directory.join(format!("{name}.md")),
            render_report_markdown(&context)?,
        )?;
    }
    Ok(())
}
