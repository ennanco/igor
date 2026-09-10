use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRole {
    Metrics,
    Model,
    Checkpoint,
    Figure,
    Log,
    CrashDump,
    Report,
    Other(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionDecision {
    Keep,
    DeleteAfterDiagnosis,
    DeleteAfterReplacement,
    DeleteNow,
}
