use std::{fmt, path::Path};

use igor_core::RuntimePaths;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonRole {
    Worker,
    Supervisor,
}

impl DaemonRole {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Supervisor => "supervisor",
        }
    }

    #[must_use]
    pub fn socket_path(self, paths: &RuntimePaths) -> &Path {
        match self {
            Self::Worker => &paths.worker_socket,
            Self::Supervisor => &paths.supervisor_socket,
        }
    }
}

impl fmt::Display for DaemonRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub protocol_version: u32,
    pub request: Request,
}

impl RequestEnvelope {
    #[must_use]
    pub fn new(request: Request) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Health,
    Version,
    DatabaseStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseEnvelope {
    pub protocol_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<Response>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

impl ResponseEnvelope {
    #[must_use]
    pub fn success(response: Response) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            response: Some(response),
            error: None,
        }
    }

    #[must_use]
    pub fn failure(error: ProtocolError) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            response: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Health(Health),
    Version(Version),
    DatabaseStatus(DatabaseStatus),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub role: DaemonRole,
    pub healthy: bool,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Version {
    pub role: DaemonRole,
    pub igor: String,
    pub protocol: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseStatus {
    pub role: DaemonRole,
    pub schema_version: i64,
    pub integrity: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorKind {
    IncompatibleProtocol,
    InvalidRequest,
    FrameTooLarge,
    DatabaseUnavailable,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    pub code: String,
    pub kind: ProtocolErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_version: Option<u32>,
}

impl ProtocolError {
    #[must_use]
    pub fn incompatible(found: u32) -> Self {
        Self {
            code: "IGOR-PROTO-001".into(),
            kind: ProtocolErrorKind::IncompatibleProtocol,
            message: format!(
                "protocol version {found} is incompatible with supported version {PROTOCOL_VERSION}"
            ),
            found_version: Some(found),
            supported_version: Some(PROTOCOL_VERSION),
        }
    }

    #[must_use]
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: "IGOR-PROTO-002".into(),
            kind: ProtocolErrorKind::InvalidRequest,
            message: message.into(),
            found_version: None,
            supported_version: None,
        }
    }

    #[must_use]
    pub fn frame_too_large() -> Self {
        Self {
            code: "IGOR-PROTO-003".into(),
            kind: ProtocolErrorKind::FrameTooLarge,
            message: "request frame exceeds 65536 bytes".into(),
            found_version: None,
            supported_version: None,
        }
    }

    #[must_use]
    pub fn database_unavailable() -> Self {
        Self {
            code: "IGOR-DAEMON-001".into(),
            kind: ProtocolErrorKind::DatabaseUnavailable,
            message: "database status is unavailable".into(),
            found_version: None,
            supported_version: None,
        }
    }

    #[must_use]
    pub fn internal() -> Self {
        Self {
            code: "IGOR-DAEMON-002".into(),
            kind: ProtocolErrorKind::Internal,
            message: "daemon request failed".into(),
            found_version: None,
            supported_version: None,
        }
    }
}
