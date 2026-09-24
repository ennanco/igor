use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DomainError>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Compatibility,
    Validation,
    StateTransition,
    Serialization,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    UnsupportedProjectConfigVersion,
    UnsupportedEventPayloadVersion,
    InvalidProject,
    InvalidCommand,
    InvalidResourceRequest,
    InvalidRetryPolicy,
    InvalidJob,
    InvalidAttempt,
    InvalidExecutor,
    InvalidStateTransition,
    InvalidJson,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedProjectConfigVersion => "IGOR-COMPAT-001",
            Self::UnsupportedEventPayloadVersion => "IGOR-COMPAT-002",
            Self::InvalidProject => "IGOR-VALID-001",
            Self::InvalidCommand => "IGOR-VALID-002",
            Self::InvalidResourceRequest => "IGOR-VALID-003",
            Self::InvalidRetryPolicy => "IGOR-VALID-004",
            Self::InvalidJob => "IGOR-VALID-005",
            Self::InvalidAttempt => "IGOR-VALID-006",
            Self::InvalidExecutor => "IGOR-VALID-007",
            Self::InvalidStateTransition => "IGOR-STATE-001",
            Self::InvalidJson => "IGOR-SERDE-001",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DomainError {
    #[error("{code}: unsupported {contract} version {found}; supported version is {supported}; migrate the document or use a compatible Igor version", code = .code.as_str())]
    UnsupportedVersion {
        category: ErrorCategory,
        code: ErrorCode,
        contract: String,
        found: u32,
        supported: u32,
    },
    #[error("{code}: invalid {field}: {reason}", code = .code.as_str())]
    Validation {
        category: ErrorCategory,
        code: ErrorCode,
        field: String,
        reason: String,
    },
    #[error("{code}: cannot transition {entity} from {from} to {to}", code = .code.as_str())]
    InvalidTransition {
        category: ErrorCategory,
        code: ErrorCode,
        entity: String,
        from: String,
        to: String,
    },
    #[error("{code}: invalid JSON for {contract}: {reason}", code = .code.as_str())]
    InvalidJson {
        category: ErrorCategory,
        code: ErrorCode,
        contract: String,
        reason: String,
    },
}

impl DomainError {
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        match self {
            Self::UnsupportedVersion { category, .. }
            | Self::Validation { category, .. }
            | Self::InvalidTransition { category, .. }
            | Self::InvalidJson { category, .. } => *category,
        }
    }

    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::UnsupportedVersion { code, .. }
            | Self::Validation { code, .. }
            | Self::InvalidTransition { code, .. }
            | Self::InvalidJson { code, .. } => *code,
        }
    }

    pub(crate) fn validation(code: ErrorCode, field: &str, reason: &str) -> Self {
        Self::Validation {
            category: ErrorCategory::Validation,
            code,
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn unsupported(code: ErrorCode, contract: &str, found: u32, supported: u32) -> Self {
        Self::UnsupportedVersion {
            category: ErrorCategory::Compatibility,
            code,
            contract: contract.into(),
            found,
            supported,
        }
    }
}
