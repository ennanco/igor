use igor_core::{parse_metric_document_json, validate_metric_document};
use serde_json::{Value, json};

#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn validate(value: Value) -> Result<(), String> {
        validate_metric_document(&value).map_err(|error| error.to_string())
    }

    #[test]
    fn accepts_shipped_example() {
        let example: Value = serde_json::from_str(include_str!(
            "../../../config/example-metric-result-v1.json"
        ))
        .unwrap();
        validate_metric_document(&example).unwrap();
    }

    #[test]
    fn rejects_version_missing_and_unknown_fields_without_echoing_values() {
        let mut value =
            json!({"schema_version": 2, "status": "success", "metrics": {"secret-value": 1}});
        let error = validate(value.clone()).unwrap_err();
        assert!(error.contains("schema_version"));
        assert!(!error.contains("secret-value"));
        value["schema_version"] = json!(1);
        value["private_field"] = json!("sensitive");
        assert!(validate(value).unwrap_err().contains("unknown field"));
        assert!(
            validate(json!({"schema_version": 1, "metrics": {}}))
                .unwrap_err()
                .contains("status")
        );
    }

    #[test]
    fn rejects_invalid_metric_values_and_names() {
        for metrics in [
            json!({"x": "1"}),
            json!({"x": null}),
            json!({"": 1}),
            json!({"   ": 1}),
        ] {
            assert!(
                validate(json!({"schema_version": 1, "status": "success", "metrics": metrics}))
                    .is_err()
            );
        }
    }

    #[test]
    fn validates_selection_and_artifact_constraints() {
        let base = json!({"schema_version": 1, "status": "partial", "metrics": {}});
        for selection in [
            json!({"split": "v", "metric": "m"}),
            json!({"split": "", "metric": "m", "value": 0}),
            json!({"split": "   ", "metric": "m", "value": 0}),
            json!({"split": "v", "metric": "m", "value": 0, "epoch": -1}),
            json!({"split": "v", "metric": "m", "value": 0, "epoch": 9223372036854775808_u64}),
            json!({"split": "v", "metric": "m", "value": 0, "extra": 1}),
        ] {
            let mut value = base.clone();
            value["selection"] = selection;
            assert!(validate(value).is_err());
        }
        for artifact in [
            json!({"path": "x", "role": "unknown"}),
            json!({"path": "x", "role": "other"}),
            json!({"path": "x", "role": "log", "role_detail": "extra"}),
            json!({"path": "x", "role": "log", "sha256": "sha256:ABC"}),
        ] {
            let mut value = base.clone();
            value["artifacts"] = json!([artifact]);
            assert!(validate(value).is_err());
        }
    }

    #[test]
    fn accepts_empty_metrics_and_optional_fields_omitted() {
        validate(json!({"schema_version": 1, "status": "failed", "metrics": {}, "classes": {}, "artifacts": []})).unwrap();
        validate(json!({"schema_version": 1, "status": "success", "metrics": {}, "selection": {"split": "test", "metric": "score", "value": 0.5}, "artifacts": [{"path": "result.json", "role": "other", "role_detail": "data"}]})).unwrap();
    }

    #[test]
    fn parses_valid_json_documents() {
        let documents: &[&[u8]] = &[
            br#"{"schema_version": 1, "status": "success", "metrics": {}}"#,
            br#"{"schema_version":1,"status":"failed","metrics":{"score":0.5},"classes":{"ok":{"score":1.0}},"artifacts":[]}"#,
            br#"{"schema_version": 1, "status": "partial", "metrics": {"score": -1.5e-3},
                 "selection": {"split": "test", "metric": "score", "value": 0.5},
                 "artifacts": [{"path": "r.json", "role": "log"}]}"#,
        ];
        for raw in documents {
            validate_metric_document(&parse_metric_document_json(raw).unwrap()).unwrap();
        }
    }

    #[test]
    fn rejects_malformed_syntax() {
        let documents: &[&[u8]] = &[
            b"",
            b"{",
            br#"{"schema_version": 1,"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {}"#,
            br#"{"schema_version": 1,}"#,
            br#"{"schema_version": 1 "status": "success"}"#,
            br#"{"schema_version": 1, "status": success, "metrics": {}}"#,
            b"not json",
            br#"{"schema_version": 1, "status": "success", "metrics": {}} trailing"#,
        ];
        for raw in documents {
            assert!(parse_metric_document_json(raw).is_err(), "{raw:?}");
        }
        assert!(
            validate_metric_document(&parse_metric_document_json(b"[1, 2, 3]").unwrap()).is_err()
        );
    }

    #[test]
    fn rejects_duplicate_keys_at_every_object_level() {
        let documents: &[&[u8]] = &[
            // top level
            br#"{"schema_version": 1, "schema_version": 1, "status": "success", "metrics": {}}"#,
            // metrics
            br#"{"schema_version": 1, "status": "success", "metrics": {"score": 0.1, "score": 0.2}}"#,
            // selection
            br#"{"schema_version": 1, "status": "success", "metrics": {},
                 "selection": {"split": "test", "split": "train", "metric": "score", "value": 0.5}}"#,
            // classes
            br#"{"schema_version": 1, "status": "success", "metrics": {}, "classes": {"ok": 1.0, "ok": 0.0}}"#,
            // artifacts (array of objects: duplicate inside one element)
            br#"{"schema_version": 1, "status": "success", "metrics": {},
                 "artifacts": [{"path": "a", "role": "log", "role_detail": "d", "path": "b"}]}"#,
            // escaped spelling of an already used key: "sc\u006fre" == "score"
            br#"{"schema_version": 1, "status": "success", "metrics": {"sc\u006fre": 0.1, "score": 0.2}}"#,
            // escaped spelling at top level: "schema_versi\u006fn" == "schema_version"
            br#"{"schema_versi\u006fn": 1, "schema_version": 2, "status": "success", "metrics": {}}"#,
        ];
        for raw in documents {
            assert!(parse_metric_document_json(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn duplicate_key_errors_do_not_echo_secret_key_or_value() {
        let documents: &[&[u8]] = &[
            br#"{"secret_top": 7.5, "secret_top": 8.25, "schema_version": 1, "status": "success", "metrics": {}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"secret_metric": 7.5, "secret_metric": 8.25}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {},
                 "artifacts": [{"path": "p", "role": "log", "role_detail": "d", "path": "q"}]}"#,
        ];
        for raw in documents {
            let error = parse_metric_document_json(raw).unwrap_err().to_string();
            assert!(!error.contains("secret_top"), "{error}");
            assert!(!error.contains("secret_metric"), "{error}");
            assert!(!error.contains("7.5"), "{error}");
            assert!(!error.contains("8.25"), "{error}");
            assert!(!error.contains("role_detail"), "{error}");
        }
        let syntax_error =
            parse_metric_document_json(br#"{"secret-key":"private", "metrics": nonsense}"#)
                .unwrap_err()
                .to_string();
        assert!(!syntax_error.contains("secret-key"), "{syntax_error}");
        assert!(!syntax_error.contains("private"), "{syntax_error}");
    }

    #[test]
    fn rejects_nonfinite_numbers_at_parse_time() {
        let documents: &[&[u8]] = &[
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": NaN}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": Infinity}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": -Infinity}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": 1e999}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": -1e999}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": 1.7976931348623157e309}}"#,
            br#"{"schema_version": 1, "status": "success", "metrics": {"x": -1.7976931348623157e309}}"#,
        ];
        for raw in documents {
            assert!(parse_metric_document_json(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn validate_after_parse_rejects_missing_required_fields_and_unsupported_versions() {
        let cases: &[(&[u8], &str)] = &[
            (br#"{"status": "success", "metrics": {}}"#, "schema_version"),
            (br#"{"schema_version": 1, "metrics": {}}"#, "status"),
            (br#"{"schema_version": 1, "status": "success"}"#, "metrics"),
            (br#"{}"#, "schema_version"),
            (
                br#"{"schema_version": 2, "status": "success", "metrics": {}}"#,
                "schema_version",
            ),
            (
                br#"{"schema_version": 0, "status": "success", "metrics": {}}"#,
                "schema_version",
            ),
            (
                br#"{"schema_version": "1", "status": "success", "metrics": {}}"#,
                "schema_version",
            ),
        ];
        for (raw, expected) in cases {
            let value = parse_metric_document_json(raw).unwrap();
            let error = validate_metric_document(&value).unwrap_err().to_string();
            assert!(error.contains(expected), "{raw:?} -> {error}");
        }
        // Version written as a JSON float (1.0) must not be accepted as version 1.
        let value = parse_metric_document_json(
            br#"{"schema_version": 1.0, "status": "success", "metrics": {}}"#,
        )
        .unwrap();
        assert!(validate_metric_document(&value).is_err());
    }
}
