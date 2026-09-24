use std::{fmt, path::Path};

use igor_core::{
    JobDetail, JobId, JobLogs, Project, ProjectId, ResourceStatus, RuntimePaths, StoredEvent,
    StoredJob, SubmissionInput,
};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 5;

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Health,
    Version,
    DatabaseStatus,
    Resources,
    ProjectRegister {
        project: Project,
    },
    ProjectList,
    ProjectRemove {
        root: std::path::PathBuf,
    },
    ProjectByRoot {
        root: std::path::PathBuf,
    },
    Submit {
        project_id: ProjectId,
        input: Box<SubmissionInput>,
    },
    JobList {
        project_id: Option<ProjectId>,
    },
    JobShow {
        job_id: JobId,
    },
    JobEvents {
        job_id: JobId,
    },
    JobCancel {
        job_id: JobId,
        grace_seconds: u32,
    },
    JobRetry {
        job_id: JobId,
    },
    JobLogs {
        job_id: JobId,
    },
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
    Resources { resources: Vec<ResourceStatus> },
    Project(Project),
    Projects { projects: Vec<Project> },
    OptionalProject { project: Option<Project> },
    Submitted(StoredJob),
    Jobs { jobs: Vec<StoredJob> },
    Job(JobDetail),
    Events { events: Vec<StoredEvent> },
    Cancelled(JobDetail),
    Retried(JobDetail),
    Logs(JobLogs),
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
    NotFound,
    Conflict,
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
            message: "request frame exceeds 1048576 bytes".into(),
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

    #[must_use]
    pub fn not_found(entity: &str) -> Self {
        Self {
            code: "IGOR-API-001".into(),
            kind: ProtocolErrorKind::NotFound,
            message: format!("{entity} was not found"),
            found_version: None,
            supported_version: None,
        }
    }

    #[must_use]
    pub fn conflict(entity: &str) -> Self {
        Self {
            code: "IGOR-API-002".into(),
            kind: ProtocolErrorKind::Conflict,
            message: format!("{entity} conflicts with existing state"),
            found_version: None,
            supported_version: None,
        }
    }
}
