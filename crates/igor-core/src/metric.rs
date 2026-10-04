//! Validation for the public metric result v1 JSON document.

use std::fmt;

use serde::{
    Deserialize, Deserializer,
    de::{Error as _, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid metric document at {path}: {reason}")]
pub struct MetricDocumentError {
    path: String,
    reason: &'static str,
}

impl MetricDocumentError {
    fn new(path: impl Into<String>, reason: &'static str) -> Self {
        Self {
            path: path.into(),
            reason,
        }
    }
}

/// Parses one JSON document, rejecting duplicate keys at every nesting level.
///
/// Validate the returned value with [`validate_metric_document`] before storing
/// or publishing it. Parse errors do not include object keys or values.
pub fn parse_metric_document_json(bytes: &[u8]) -> serde_json::Result<Value> {
    serde_json::from_slice::<DistinctValue>(bytes).map(|value| value.0)
}

struct DistinctValue(Value);

impl<'de> Deserialize<'de> for DistinctValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(DistinctVisitor)
    }
}

struct DistinctVisitor;

impl<'de> Visitor<'de> for DistinctVisitor {
    type Value = DistinctValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value with unique object keys")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(DistinctValue(Value::Bool(value)))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(DistinctValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(DistinctValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        let number = Number::from_f64(value).ok_or_else(|| E::custom("non-finite JSON number"))?;
        Ok(DistinctValue(Value::Number(number)))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(DistinctValue(Value::String(value.into())))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(DistinctValue(Value::String(value)))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(DistinctValue(Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<DistinctValue>()? {
            values.push(value.0);
        }
        Ok(DistinctValue(Value::Array(values)))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut object: A) -> Result<Self::Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom("duplicate JSON object key"));
            }
            values.insert(key, object.next_value::<DistinctValue>()?.0);
        }
        Ok(DistinctValue(Value::Object(values)))
    }
}

/// Validates a metric result against the strict v1 document contract.
///
/// Diagnostics contain field paths and rules only; they never include document values.
pub fn validate_metric_document(value: &Value) -> Result<(), MetricDocumentError> {
    let root = object(value, "$")?;
    fields(
        root,
        "$",
        &[
            "schema_version",
            "status",
            "metrics",
            "selection",
            "classes",
            "artifacts",
        ],
    )?;

    let version = root
        .get("schema_version")
        .ok_or_else(|| missing("$.schema_version"))?;
    if !is_integer(version) || version.as_i64() != Some(1) {
        return Err(MetricDocumentError::new(
            "$.schema_version",
            "expected integer version 1",
        ));
    }
    let status = root.get("status").ok_or_else(|| missing("$.status"))?;
    if !matches!(status.as_str(), Some("success" | "partial" | "failed")) {
        return Err(MetricDocumentError::new(
            "$.status",
            "expected success, partial, or failed",
        ));
    }
    validate_metrics(
        root.get("metrics").ok_or_else(|| missing("$.metrics"))?,
        "$.metrics",
    )?;

    if let Some(selection) = root.get("selection") {
        validate_selection(selection)?;
    }
    if let Some(classes) = root.get("classes") {
        let classes = object(classes, "$.classes")?;
        for (name, metrics) in classes {
            nonempty_name(name, format!("$.classes.{name}"))?;
            validate_metrics(metrics, &format!("$.classes.{name}"))?;
        }
    }
    if let Some(artifacts) = root.get("artifacts") {
        let artifacts = artifacts
            .as_array()
            .ok_or_else(|| invalid("$.artifacts", "expected an array"))?;
        for (index, artifact) in artifacts.iter().enumerate() {
            validate_artifact(artifact, &format!("$.artifacts[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_selection(value: &Value) -> Result<(), MetricDocumentError> {
    let path = "$.selection";
    let selection = object(value, path)?;
    fields(
        selection,
        path,
        &[
            "split",
            "metric",
            "value",
            "epoch",
            "trained_epochs",
            "checkpoint",
        ],
    )?;
    for field in ["split", "metric"] {
        let value = selection
            .get(field)
            .ok_or_else(|| missing(format!("{path}.{field}")))?;
        nonempty_name(value.as_str().unwrap_or(""), format!("{path}.{field}"))?;
    }
    number(
        selection
            .get("value")
            .ok_or_else(|| missing(format!("{path}.value")))?,
        format!("{path}.value"),
    )?;
    for field in ["epoch", "trained_epochs"] {
        if let Some(value) = selection.get(field)
            && value.as_i64().is_none_or(|n| n < 0)
        {
            return Err(invalid(
                format!("{path}.{field}"),
                "expected a nonnegative integer",
            ));
        }
    }
    if let Some(checkpoint) = selection.get("checkpoint") {
        nonempty_name(
            checkpoint.as_str().unwrap_or(""),
            format!("{path}.checkpoint"),
        )?;
    }
    Ok(())
}

fn validate_artifact(value: &Value, path: &str) -> Result<(), MetricDocumentError> {
    let artifact = object(value, path)?;
    fields(artifact, path, &["path", "role", "role_detail", "sha256"])?;
    let file_path = artifact
        .get("path")
        .ok_or_else(|| missing(format!("{path}.path")))?;
    nonempty_name(file_path.as_str().unwrap_or(""), format!("{path}.path"))?;
    let role = artifact
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{path}.role"), "expected an artifact role"))?;
    let known_role = matches!(
        role,
        "metrics" | "model" | "checkpoint" | "figure" | "log" | "crash_dump" | "report" | "other"
    );
    if !known_role {
        return Err(invalid(format!("{path}.role"), "unknown artifact role"));
    }
    match (role == "other", artifact.get("role_detail")) {
        (true, None) => return Err(missing(format!("{path}.role_detail"))),
        (false, Some(_)) => {
            return Err(invalid(
                format!("{path}.role_detail"),
                "only allowed for role other",
            ));
        }
        (true, Some(detail)) => {
            nonempty_name(detail.as_str().unwrap_or(""), format!("{path}.role_detail"))?
        }
        _ => (),
    }
    if let Some(digest) = artifact.get("sha256") {
        let digest = digest
            .as_str()
            .ok_or_else(|| invalid(format!("{path}.sha256"), "expected a SHA-256 digest"))?;
        let hex = digest.strip_prefix("sha256:").unwrap_or("");
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid(
                format!("{path}.sha256"),
                "expected sha256: followed by 64 lowercase hexadecimal characters",
            ));
        }
    }
    Ok(())
}

fn validate_metrics(value: &Value, path: &str) -> Result<(), MetricDocumentError> {
    let metrics = object(value, path)?;
    for (name, value) in metrics {
        nonempty_name(name, format!("{path}.{name}"))?;
        number(value, format!("{path}.{name}"))?;
    }
    Ok(())
}

fn number(value: &Value, path: String) -> Result<(), MetricDocumentError> {
    if value.as_f64().is_some_and(f64::is_finite) {
        Ok(())
    } else {
        Err(invalid(path, "expected a finite number"))
    }
}

fn is_integer(value: &Value) -> bool {
    value.as_i64().is_some() || value.as_u64().is_some()
}

fn nonempty_name(value: &str, path: String) -> Result<(), MetricDocumentError> {
    if value.trim().is_empty() {
        Err(invalid(path, "must not be empty"))
    } else {
        Ok(())
    }
}

fn object<'a>(value: &'a Value, path: &str) -> Result<&'a Map<String, Value>, MetricDocumentError> {
    value
        .as_object()
        .ok_or_else(|| invalid(path, "expected an object"))
}

fn fields(
    map: &Map<String, Value>,
    path: &str,
    allowed: &[&str],
) -> Result<(), MetricDocumentError> {
    if let Some(key) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid(format!("{path}.{key}"), "unknown field"));
    }
    Ok(())
}

fn missing(path: impl Into<String>) -> MetricDocumentError {
    MetricDocumentError::new(path, "required field is missing")
}
fn invalid(path: impl Into<String>, reason: &'static str) -> MetricDocumentError {
    MetricDocumentError::new(path, reason)
}
