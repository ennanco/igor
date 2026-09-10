use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;

use crate::{DomainError, ErrorCategory, ErrorCode, EventId, Result};

const EVENT_PAYLOAD_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    JobSubmitted,
    JobStateChanged,
    AttemptCreated,
    AttemptStateChanged,
    ActionStateChanged,
    DeliveryStateChanged,
    RecoveryStateChanged,
    ReportStateChanged,
    CleanupStateChanged,
    ResourceReserved,
    ResourceReleased,
    ArtifactRegistered,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EventPayload {
    pub schema_version: u32,
    pub data: Value,
}

#[derive(Deserialize)]
struct EventPayloadDocument {
    schema_version: u32,
    data: Value,
}

impl EventPayload {
    pub fn new(kind: EventKind, schema_version: u32, data: Value) -> Result<Self> {
        Self::validate_version(kind, schema_version)?;
        Ok(Self {
            schema_version,
            data,
        })
    }

    pub fn from_json(kind: EventKind, json: &str) -> Result<Self> {
        let document: EventPayloadDocument =
            serde_json::from_str(json).map_err(|error| DomainError::InvalidJson {
                category: ErrorCategory::Serialization,
                code: ErrorCode::InvalidJson,
                contract: format!("{} event payload", kind.as_str()),
                reason: error.to_string(),
            })?;
        Self::new(kind, document.schema_version, document.data)
    }

    fn validate_version(kind: EventKind, version: u32) -> Result<()> {
        if version == EVENT_PAYLOAD_VERSION {
            Ok(())
        } else {
            Err(DomainError::unsupported(
                ErrorCode::UnsupportedEventPayloadVersion,
                &format!("{} event payload", kind.as_str()),
                version,
                EVENT_PAYLOAD_VERSION,
            ))
        }
    }
}

impl<'de> Deserialize<'de> for EventPayload {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let document = EventPayloadDocument::deserialize(deserializer)?;
        if document.schema_version != EVENT_PAYLOAD_VERSION {
            return Err(de::Error::custom(format_args!(
                "{}: unsupported event payload version {}; supported version is {}; migrate the document or use a compatible Igor version",
                ErrorCode::UnsupportedEventPayloadVersion.as_str(),
                document.schema_version,
                EVENT_PAYLOAD_VERSION,
            )));
        }
        Ok(Self {
            schema_version: document.schema_version,
            data: document.data,
        })
    }
}

impl EventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobSubmitted => "job_submitted",
            Self::JobStateChanged => "job_state_changed",
            Self::AttemptCreated => "attempt_created",
            Self::AttemptStateChanged => "attempt_state_changed",
            Self::ActionStateChanged => "action_state_changed",
            Self::DeliveryStateChanged => "delivery_state_changed",
            Self::RecoveryStateChanged => "recovery_state_changed",
            Self::ReportStateChanged => "report_state_changed",
            Self::CleanupStateChanged => "cleanup_state_changed",
            Self::ResourceReserved => "resource_reserved",
            Self::ResourceReleased => "resource_released",
            Self::ArtifactRegistered => "artifact_registered",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Event {
    pub id: EventId,
    pub kind: EventKind,
    pub payload: EventPayload,
}

impl Event {
    pub fn new(id: EventId, kind: EventKind, payload: EventPayload) -> Result<Self> {
        EventPayload::validate_version(kind, payload.schema_version)?;
        Ok(Self { id, kind, payload })
    }
}
