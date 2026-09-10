use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{DomainError, ErrorCode, Result};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResourceMode {
    #[default]
    ExclusiveHost,
    Shared,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "selection", content = "device")]
pub enum GpuRequest {
    #[default]
    None,
    Any,
    Specific(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedResourceMode {
    Shared,
    Exclusive,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NamedResourceRequest {
    pub name: String,
    pub mode: NamedResourceMode,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceRequest {
    pub mode: ResourceMode,
    #[serde(
        default,
        deserialize_with = "deserialize_unlimited_u32",
        serialize_with = "serialize_unlimited"
    )]
    pub cpu_threads: Option<u32>,
    #[serde(
        default,
        rename = "memory",
        alias = "memory_bytes",
        deserialize_with = "deserialize_unlimited_u64",
        serialize_with = "serialize_unlimited"
    )]
    pub memory_bytes: Option<u64>,
    #[serde(
        default,
        rename = "timeout",
        alias = "timeout_seconds",
        deserialize_with = "deserialize_timeout",
        serialize_with = "serialize_timeout"
    )]
    pub timeout_seconds: Option<u64>,
    pub gpu: GpuRequest,
    pub gpu_count: u32,
    pub gpu_exclusive: bool,
    pub named: Vec<NamedResourceRequest>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OptionalNumber<T> {
    Number(T),
    Keyword(String),
}

fn deserialize_unlimited_u32<'de, D>(deserializer: D) -> std::result::Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_number(deserializer, "unlimited")
}

fn deserialize_unlimited_u64<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_number(deserializer, "unlimited")
}

fn deserialize_timeout<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_number(deserializer, "none")
}

fn deserialize_optional_number<'de, D, T>(
    deserializer: D,
    keyword: &'static str,
) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    match OptionalNumber::<T>::deserialize(deserializer)? {
        OptionalNumber::Number(value) => Ok(Some(value)),
        OptionalNumber::Keyword(value) if value == keyword => Ok(None),
        OptionalNumber::Keyword(_) => Err(de::Error::custom(format_args!(
            "expected a positive integer or {keyword:?}"
        ))),
    }
}

fn serialize_unlimited<S, T>(
    value: &Option<T>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    match value {
        Some(value) => value.serialize(serializer),
        None => serializer.serialize_str("unlimited"),
    }
}

fn serialize_timeout<S>(value: &Option<u64>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => value.serialize(serializer),
        None => serializer.serialize_str("none"),
    }
}

impl Default for ResourceRequest {
    fn default() -> Self {
        Self {
            mode: ResourceMode::ExclusiveHost,
            cpu_threads: None,
            memory_bytes: None,
            timeout_seconds: None,
            gpu: GpuRequest::None,
            gpu_count: 1,
            gpu_exclusive: true,
            named: Vec::new(),
        }
    }
}

impl ResourceRequest {
    pub fn validate(&self) -> Result<()> {
        if self.cpu_threads == Some(0) {
            return Err(DomainError::validation(
                ErrorCode::InvalidResourceRequest,
                "resources.cpu_threads",
                "must be positive when specified",
            ));
        }
        if self.memory_bytes == Some(0) {
            return Err(DomainError::validation(
                ErrorCode::InvalidResourceRequest,
                "resources.memory_bytes",
                "must be positive when specified",
            ));
        }
        if self.timeout_seconds == Some(0) {
            return Err(DomainError::validation(
                ErrorCode::InvalidResourceRequest,
                "resources.timeout_seconds",
                "must be positive when specified",
            ));
        }
        if self.gpu != GpuRequest::None && self.gpu_count == 0 {
            return Err(DomainError::validation(
                ErrorCode::InvalidResourceRequest,
                "resources.gpu_count",
                "must be positive when a GPU is requested",
            ));
        }
        if let Some(resource) = self.named.iter().find(|resource| resource.name.is_empty()) {
            return Err(DomainError::validation(
                ErrorCode::InvalidResourceRequest,
                "resources.named.name",
                &format!("must not be empty ({:?})", resource.mode),
            ));
        }
        Ok(())
    }
}
