use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt, fs,
    io::Write,
    os::unix::fs::MetadataExt,
    os::unix::fs::OpenOptionsExt,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{PROJECT_CONFIG_VERSION, ProjectConfig};

pub const GLOBAL_CONFIG_VERSION: u32 = 1;
pub const PROJECT_CONFIG_RELATIVE_PATH: &str = ".igor/project.toml";
pub const REPORT_PROMPT_RELATIVE_PATH: &str = ".igor/report-prompt.md";
pub const REDACTED: &str = "<redacted>";
pub const DEFAULT_MAX_CONCURRENT_JOBS: u32 = 1;
pub const MAX_CONCURRENT_JOBS: u32 = 1024;

#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString(<redacted>)")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(REDACTED)
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Debug)]
pub struct Environment {
    home: PathBuf,
    uid: u32,
    values: BTreeMap<OsString, OsString>,
}

impl Environment {
    pub fn new(
        home: impl Into<PathBuf>,
        uid: u32,
        values: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Self {
        Self {
            home: home.into(),
            uid,
            values: values.into_iter().collect(),
        }
    }

    pub fn from_process() -> Result<Self, ConfigError> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(ConfigError::Environment {
                variable: "HOME",
                reason: "is not set".into(),
            })?;
        let uid = fs::metadata("/proc/self")
            .map_err(|error| ConfigError::Environment {
                variable: "UID",
                reason: format!("cannot determine from /proc/self: {error}"),
            })?
            .uid();
        Ok(Self::new(home, uid, std::env::vars_os()))
    }

    fn path(&self, name: &'static str) -> Option<PathBuf> {
        self.values.get(OsStr::new(name)).map(PathBuf::from)
    }

    fn xdg_path(&self, name: &'static str, fallback: PathBuf) -> PathBuf {
        self.path(name)
            .filter(|path| path.is_absolute())
            .unwrap_or(fallback)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConfigOverrides {
    pub global_config: Option<PathBuf>,
    pub project: Option<PathBuf>,
    pub state_dir: Option<PathBuf>,
    pub runtime_dir: Option<PathBuf>,
    pub database: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
    pub worktree_dir: Option<PathBuf>,
    pub report_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalPathConfig {
    pub state_dir: Option<PathBuf>,
    pub runtime_dir: Option<PathBuf>,
    pub database: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
    pub worktree_dir: Option<PathBuf>,
    pub report_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelegramConfig {
    pub bot_token: Option<SecretString>,
    pub chat_id: Option<SecretString>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostConfig {
    pub cpu_threads: Option<u32>,
    pub memory_bytes: Option<u64>,
    pub gpus: Vec<String>,
    pub discover_gpus: bool,
    pub named_resources: Vec<String>,
    pub max_concurrent_jobs: u32,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            cpu_threads: None,
            memory_bytes: None,
            gpus: Vec::new(),
            discover_gpus: true,
            named_resources: Vec::new(),
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HostConfigUpdate {
    pub cpu_threads: Option<Option<u32>>,
    pub memory_bytes: Option<Option<u64>>,
    pub gpus: Option<Vec<String>>,
    pub discover_gpus: Option<bool>,
    pub max_concurrent_jobs: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub paths: GlobalPathConfig,
    #[serde(default)]
    pub host: HostConfig,
    #[serde(default)]
    pub telegram: TelegramConfig,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            schema_version: GLOBAL_CONFIG_VERSION,
            paths: GlobalPathConfig::default(),
            host: HostConfig::default(),
            telegram: TelegramConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuntimePaths {
    pub config_file: PathBuf,
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub database: PathBuf,
    pub log_dir: PathBuf,
    pub worktree_dir: PathBuf,
    pub report_dir: PathBuf,
    pub worker_socket: PathBuf,
    pub supervisor_socket: PathBuf,
    pub resource_dir: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LoadedProjectConfig {
    pub config_file: PathBuf,
    pub project_root: PathBuf,
    pub config: ProjectConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EffectiveConfig {
    pub paths: RuntimePaths,
    pub global: GlobalConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<LoadedProjectConfig>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("environment variable {variable} {reason}")]
    Environment {
        variable: &'static str,
        reason: String,
    },
    #[error("cannot read configuration file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid configuration file {path} at {field}: {reason}")]
    Invalid {
        path: PathBuf,
        field: String,
        reason: String,
    },
    #[error(
        "unsupported configuration version in {path} at schema_version: found {found}, supported version is {supported}; migrate the file or use a compatible Igor version"
    )]
    UnsupportedVersion {
        path: PathBuf,
        found: u32,
        supported: u32,
    },
    #[error(
        "project configuration was not found from {start}; run `igor init` or select a project explicitly"
    )]
    ProjectNotFound { start: PathBuf },
    #[error("refusing to overwrite existing file {path}; pass --force to replace it")]
    AlreadyExists { path: PathBuf },
    #[error("cannot write {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn load_effective_config(
    environment: &Environment,
    overrides: &ConfigOverrides,
    cwd: &Path,
) -> Result<EffectiveConfig, ConfigError> {
    let config_file = normalize_absolute(&select_global_config_path(environment, overrides), cwd);
    let explicitly_selected =
        overrides.global_config.is_some() || environment.path("IGOR_CONFIG").is_some();
    let global = if config_file.is_file() {
        load_global_config(&config_file)?
    } else if explicitly_selected {
        return Err(read_missing(config_file));
    } else {
        GlobalConfig::default()
    };
    let paths = resolve_runtime_paths(environment, overrides, &config_file, &global, cwd);
    let project_file = match overrides
        .project
        .clone()
        .or_else(|| environment.path("IGOR_PROJECT"))
    {
        Some(path) => Some(explicit_project_file(cwd, &path)),
        None => discover_project_config(cwd),
    };
    let project = project_file
        .map(|path| load_project_config(&path))
        .transpose()?;
    Ok(EffectiveConfig {
        paths,
        global,
        project,
    })
}

#[must_use]
pub fn select_global_config_path(
    environment: &Environment,
    overrides: &ConfigOverrides,
) -> PathBuf {
    overrides
        .global_config
        .clone()
        .or_else(|| environment.path("IGOR_CONFIG"))
        .unwrap_or_else(|| {
            environment
                .xdg_path("XDG_CONFIG_HOME", environment.home.join(".config"))
                .join("igor/config.toml")
        })
}

#[must_use]
pub fn discover_project_config(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .map(|directory| directory.join(PROJECT_CONFIG_RELATIVE_PATH))
        .find(|candidate| candidate.is_file())
}

pub fn load_global_config(path: &Path) -> Result<GlobalConfig, ConfigError> {
    let input = read_config(path)?;
    parse_global_config(path, &input)
}

fn parse_global_config(path: &Path, input: &str) -> Result<GlobalConfig, ConfigError> {
    check_version(path, input, GLOBAL_CONFIG_VERSION)?;
    let config: GlobalConfig = parse_toml(path, input)?;
    if config.host.cpu_threads == Some(0) {
        return Err(invalid_field(path, "host.cpu_threads", "must be positive"));
    }
    if config
        .host
        .memory_bytes
        .is_some_and(|value| !(1..=i64::MAX as u64).contains(&value))
    {
        return Err(invalid_field(
            path,
            "host.memory_bytes",
            &format!("must be between 1 and {}", i64::MAX),
        ));
    }
    validate_unique_names(path, "host.gpus", &config.host.gpus, "identities")?;
    if !(1..=MAX_CONCURRENT_JOBS).contains(&config.host.max_concurrent_jobs) {
        return Err(invalid_field(
            path,
            "host.max_concurrent_jobs",
            &format!("must be between 1 and {MAX_CONCURRENT_JOBS}"),
        ));
    }
    validate_unique_names(
        path,
        "host.named_resources",
        &config.host.named_resources,
        "names",
    )?;
    Ok(config)
}

fn validate_unique_names(
    path: &Path,
    field: &str,
    values: &[String],
    description: &str,
) -> Result<(), ConfigError> {
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(invalid_field(
            path,
            field,
            &format!("{description} must not be empty"),
        ));
    }
    let unique: BTreeSet<_> = values.iter().collect();
    if unique.len() != values.len() {
        return Err(invalid_field(
            path,
            field,
            &format!("{description} must be unique"),
        ));
    }
    Ok(())
}

pub fn update_global_host_config(
    path: &Path,
    update: &HostConfigUpdate,
) -> Result<GlobalConfig, ConfigError> {
    let input = if path.is_file() {
        read_config(path)?
    } else {
        format!("schema_version = {GLOBAL_CONFIG_VERSION}\n")
    };
    check_version(path, &input, GLOBAL_CONFIG_VERSION)?;
    // Edit the raw document so serializing SecretString never replaces credentials with REDACTED.
    let mut document: toml::Table = parse_toml(path, &input)?;
    let host = document
        .entry("host")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or_else(|| invalid_field(path, "host", "must be a table"))?;
    update_optional_integer(
        path,
        host,
        "cpu_threads",
        update.cpu_threads.map(|value| value.map(u64::from)),
    )?;
    update_optional_integer(path, host, "memory_bytes", update.memory_bytes)?;
    if let Some(gpus) = &update.gpus {
        host.insert(
            "gpus".into(),
            toml::Value::Array(gpus.iter().cloned().map(toml::Value::String).collect()),
        );
    }
    if let Some(discover_gpus) = update.discover_gpus {
        host.insert("discover_gpus".into(), toml::Value::Boolean(discover_gpus));
    }
    if let Some(max_concurrent_jobs) = update.max_concurrent_jobs {
        host.insert(
            "max_concurrent_jobs".into(),
            toml::Value::Integer(i64::from(max_concurrent_jobs)),
        );
    }
    let output = toml::to_string_pretty(&document).map_err(|source| ConfigError::Write {
        path: path.to_path_buf(),
        source: std::io::Error::other(source),
    })?;
    let config = parse_global_config(path, &output)?;
    write_atomic(path, &output)?;
    Ok(config)
}

fn update_optional_integer(
    path: &Path,
    table: &mut toml::Table,
    key: &str,
    update: Option<Option<u64>>,
) -> Result<(), ConfigError> {
    match update {
        Some(Some(value)) => {
            let value = i64::try_from(value)
                .map_err(|_| invalid_field(path, &format!("host.{key}"), "is too large"))?;
            table.insert(key.into(), toml::Value::Integer(value));
        }
        Some(None) => {
            table.remove(key);
        }
        None => {}
    }
    Ok(())
}

pub fn load_project_config(path: &Path) -> Result<LoadedProjectConfig, ConfigError> {
    let input = read_config(path)?;
    check_version(path, &input, PROJECT_CONFIG_VERSION)?;
    let mut config: ProjectConfig = parse_toml(path, &input)?;
    let project_root = path
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."));
    let project_root = normalize_absolute(project_root, Path::new("."));
    config.paths.artifact_root = resolve_project_path(
        path,
        "paths.artifact_root",
        &project_root,
        &config.paths.artifact_root,
    )?;
    for (index, cleanup_root) in config.paths.cleanup_roots.iter_mut().enumerate() {
        *cleanup_root = resolve_project_path(
            path,
            &format!("paths.cleanup_roots[{index}]"),
            &project_root,
            cleanup_root,
        )?;
    }
    config.report.output =
        resolve_project_path(path, "report.output", &project_root, &config.report.output)?;
    config.report.prompt_file = resolve_project_path(
        path,
        "report.prompt_file",
        &project_root,
        &config.report.prompt_file,
    )?;
    for (index, declared_path) in config
        .provenance
        .scientific_configurations
        .iter_mut()
        .enumerate()
    {
        *declared_path = resolve_project_path(
            path,
            &format!("provenance.scientific_configurations[{index}]"),
            &project_root,
            declared_path,
        )?;
    }
    for (index, declared_path) in config.provenance.immutable_inputs.iter_mut().enumerate() {
        *declared_path = resolve_project_path(
            path,
            &format!("provenance.immutable_inputs[{index}]"),
            &project_root,
            declared_path,
        )?;
    }
    Ok(LoadedProjectConfig {
        config_file: path.to_path_buf(),
        project_root,
        config,
    })
}

pub fn initialize_project(root: &Path, force: bool) -> Result<Vec<PathBuf>, ConfigError> {
    let config_path = root.join(PROJECT_CONFIG_RELATIVE_PATH);
    let prompt_path = root.join(REPORT_PROMPT_RELATIVE_PATH);
    if !force {
        for path in [&config_path, &prompt_path] {
            if path.exists() {
                return Err(ConfigError::AlreadyExists {
                    path: path.to_path_buf(),
                });
            }
        }
    }
    let config_directory = config_path.parent().unwrap_or(root);
    fs::create_dir_all(config_directory).map_err(|source| ConfigError::Write {
        path: config_directory.to_path_buf(),
        source,
    })?;
    write_file(
        &config_path,
        include_str!("../../../config/example-project.toml"),
    )?;
    write_file(&prompt_path, DEFAULT_REPORT_PROMPT)?;
    Ok(vec![config_path, prompt_path])
}

fn resolve_runtime_paths(
    environment: &Environment,
    overrides: &ConfigOverrides,
    config_file: &Path,
    global: &GlobalConfig,
    cwd: &Path,
) -> RuntimePaths {
    let state_base = environment.xdg_path("XDG_STATE_HOME", environment.home.join(".local/state"));
    let runtime_base = environment.xdg_path(
        "XDG_RUNTIME_DIR",
        PathBuf::from(format!("/run/user/{}", environment.uid)),
    );
    let config_parent = config_file.parent().unwrap_or_else(|| Path::new("."));
    let configured = |value: &Option<PathBuf>| {
        value
            .as_ref()
            .map(|path| normalize_absolute(path, config_parent))
    };
    let choose = |explicit: &Option<PathBuf>, variable, config: &Option<PathBuf>, fallback| {
        explicit
            .as_ref()
            .map(|path| normalize_absolute(path, cwd))
            .or_else(|| {
                environment
                    .path(variable)
                    .map(|path| normalize_absolute(&path, cwd))
            })
            .or_else(|| configured(config))
            .unwrap_or(fallback)
    };
    let state_dir = choose(
        &overrides.state_dir,
        "IGOR_STATE_DIR",
        &global.paths.state_dir,
        state_base.join("igor"),
    );
    let runtime_dir = choose(
        &overrides.runtime_dir,
        "IGOR_RUNTIME_DIR",
        &global.paths.runtime_dir,
        runtime_base.join("igor"),
    );
    let database = choose(
        &overrides.database,
        "IGOR_DATABASE",
        &global.paths.database,
        state_dir.join("igor.sqlite3"),
    );
    let log_dir = choose(
        &overrides.log_dir,
        "IGOR_LOG_DIR",
        &global.paths.log_dir,
        state_dir.join("logs"),
    );
    let worktree_dir = choose(
        &overrides.worktree_dir,
        "IGOR_WORKTREE_DIR",
        &global.paths.worktree_dir,
        state_dir.join("worktrees"),
    );
    let report_dir = choose(
        &overrides.report_dir,
        "IGOR_REPORT_DIR",
        &global.paths.report_dir,
        state_dir.join("reports"),
    );
    RuntimePaths {
        config_file: config_file.to_path_buf(),
        state_dir,
        runtime_dir: runtime_dir.clone(),
        database,
        log_dir,
        worktree_dir,
        report_dir,
        worker_socket: runtime_dir.join("worker.sock"),
        supervisor_socket: runtime_dir.join("supervisor.sock"),
        resource_dir: runtime_dir.join("resources"),
    }
}

fn explicit_project_file(cwd: &Path, selected: &Path) -> PathBuf {
    let selected = normalize_absolute(selected, cwd);
    if selected
        .extension()
        .is_some_and(|extension| extension == "toml")
    {
        selected
    } else {
        selected.join(PROJECT_CONFIG_RELATIVE_PATH)
    }
}

fn resolve_project_path(
    config_file: &Path,
    field: &str,
    project_root: &Path,
    value: &Path,
) -> Result<PathBuf, ConfigError> {
    let resolved = normalize_absolute(value, project_root);
    if !resolved.starts_with(project_root) {
        return Err(ConfigError::Invalid {
            path: config_file.to_path_buf(),
            field: field.into(),
            reason: format!(
                "path {} escapes project root {}",
                value.display(),
                project_root.display()
            ),
        });
    }
    Ok(resolved)
}

fn normalize_absolute(path: &Path, base: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

#[derive(Deserialize)]
struct Version {
    schema_version: u32,
}

fn check_version(path: &Path, input: &str, supported: u32) -> Result<(), ConfigError> {
    let version: Version = parse_toml(path, input)?;
    if version.schema_version != supported {
        return Err(ConfigError::UnsupportedVersion {
            path: path.to_path_buf(),
            found: version.schema_version,
            supported,
        });
    }
    Ok(())
}

fn parse_toml<T: DeserializeOwned>(path: &Path, input: &str) -> Result<T, ConfigError> {
    let deserializer = toml::Deserializer::parse(input)
        .map_err(|error| invalid_toml(path, input, "document".into(), &error))?;
    serde_path_to_error::deserialize(deserializer).map_err(|error| {
        let field = if error.path().to_string().is_empty() {
            "document".into()
        } else {
            error.path().to_string()
        };
        invalid_toml(path, input, field, error.inner())
    })
}

fn invalid_toml(path: &Path, input: &str, field: String, error: &toml::de::Error) -> ConfigError {
    let location = error
        .span()
        .map(|span| line_column(input, span.start))
        .map_or_else(String::new, |(line, column)| {
            format!(" at line {line}, column {column}")
        });
    ConfigError::Invalid {
        path: path.to_path_buf(),
        field,
        reason: format!("{}{location}", error.message()),
    }
}

fn invalid_field(path: &Path, field: &str, reason: &str) -> ConfigError {
    ConfigError::Invalid {
        path: path.to_path_buf(),
        field: field.into(),
        reason: reason.into(),
    }
}

fn line_column(input: &str, offset: usize) -> (usize, usize) {
    let prefix = &input[..offset.min(input.len())];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix.len(), |(_, tail)| tail.len())
        + 1;
    (line, column)
}

fn read_config(path: &Path) -> Result<String, ConfigError> {
    fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn read_missing(path: PathBuf) -> ConfigError {
    ConfigError::Read {
        path,
        source: std::io::Error::from(std::io::ErrorKind::NotFound),
    }
}

fn write_file(path: &Path, contents: &str) -> Result<(), ConfigError> {
    fs::write(path, contents).map_err(|source| ConfigError::Write {
        path: path.to_path_buf(),
        source,
    })
}

fn write_atomic(path: &Path, contents: &str) -> Result<(), ConfigError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| ConfigError::Write {
        path: parent.to_path_buf(),
        source,
    })?;
    let temporary = parent.join(format!(".igor-config-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok::<_, std::io::Error>(())
    })();
    if let Err(source) = result {
        let _ = fs::remove_file(&temporary);
        return Err(ConfigError::Write {
            path: path.to_path_buf(),
            source,
        });
    }
    Ok(())
}

const DEFAULT_REPORT_PROMPT: &str = "# Igor report prompt\n\nGenerate a concise scientific report. Do not mix superseded generations.\n";
