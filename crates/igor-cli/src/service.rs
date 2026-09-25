use std::path::{Path, PathBuf};

const STOP_TIMEOUT: u64 = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Worker,
    Supervisor,
}

impl Role {
    fn unit_name(self) -> &'static str {
        match self {
            Self::Worker => "igor-worker.service",
            Self::Supervisor => "igor-supervisor.service",
        }
    }

    fn argument(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Supervisor => "supervisor",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct XdgDirectories {
    pub config: Option<PathBuf>,
    pub state: Option<PathBuf>,
    pub runtime: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedUnit {
    pub name: &'static str,
    pub contents: String,
}

/// Render both service units from one installed executable and optional XDG paths.
pub fn render_units(binary: &Path, xdg: &XdgDirectories) -> Result<[RenderedUnit; 2], String> {
    let binary = validate_path(binary, "binary")?;
    let dirs = [
        ("XDG_CONFIG_HOME", xdg.config.as_deref()),
        ("XDG_STATE_HOME", xdg.state.as_deref()),
        ("XDG_RUNTIME_DIR", xdg.runtime.as_deref()),
    ];
    let mut environment = Vec::new();
    for (key, path) in dirs {
        if let Some(path) = path {
            environment.push(systemd_quote(
                &format!("{key}={}", validate_path(path, key)?),
                false,
            ));
        }
    }
    let environment = if environment.is_empty() {
        String::new()
    } else {
        format!("Environment={}\n", environment.join(" "))
    };
    Ok([Role::Worker, Role::Supervisor].map(|role| {
        let mut contents = format!(
            "[Unit]\nDescription=Igor {}\n\n[Service]\nType=simple\nExecStart={} {}\nRestart=on-failure\nTimeoutStopSec={STOP_TIMEOUT}s\n{}",
            match role { Role::Worker => "worker", Role::Supervisor => "supervisor" },
            systemd_quote(&binary, true), role.argument(), environment,
        );
        if role == Role::Supervisor {
            contents.push_str("Nice=10\nIOSchedulingClass=idle\n");
        } else {
            // Process jobs must survive a worker restart for recovery to reconcile them.
            contents.push_str("KillMode=process\n");
        }
        contents.push_str("\n[Install]\nWantedBy=default.target\n");
        RenderedUnit { name: role.unit_name(), contents }
    }))
}

fn validate_path(path: &Path, label: &str) -> Result<String, String> {
    if !path.is_absolute() {
        return Err(format!("{label} path must be absolute"));
    }
    let value = path
        .to_str()
        .ok_or_else(|| format!("{label} path is not UTF-8"))?;
    if value.chars().any(char::is_control) {
        return Err(format!("{label} path contains control characters"));
    }
    Ok(value.to_owned())
}

// systemd uses C-style quoting; doubling '%' prevents specifier expansion.
fn systemd_quote(value: &str, escape_dollar: bool) -> String {
    let mut quoted = String::from("\"");
    for character in value.chars() {
        match character {
            '%' => quoted.push_str("%%"),
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '\n' => quoted.push_str("\\n"),
            '\t' => quoted.push_str("\\t"),
            '$' if escape_dollar => quoted.push_str("$$"),
            _ => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_units_share_binary_and_differ_by_role() -> Result<(), String> {
        let path = Path::new("/opt/igor/bin/igor");
        let first = render_units(path, &XdgDirectories::default())?;
        assert_eq!(first, render_units(path, &XdgDirectories::default())?);
        assert_eq!(first[0].name, "igor-worker.service");
        assert_eq!(first[1].name, "igor-supervisor.service");
        for unit in &first {
            assert!(unit.contents.contains("ExecStart=\"/opt/igor/bin/igor\" "));
            assert!(unit.contents.contains("Restart=on-failure"));
            assert!(unit.contents.contains("TimeoutStopSec=10s"));
            assert!(!unit.contents.contains("WorkingDirectory"));
        }
        assert!(!first[0].contents.contains("Nice="));
        assert!(!first[0].contents.contains("CPUQuota"));
        assert!(!first[0].contents.contains("MemoryMax"));
        assert!(first[1].contents.contains("Nice=10"));
        assert!(first[1].contents.contains("IOSchedulingClass=idle"));
        assert!(first[0].contents.contains("KillMode=process"));
        assert!(!first[1].contents.contains("KillMode="));
        Ok(())
    }

    #[test]
    fn rendered_units_match_complete_stable_contents() -> Result<(), String> {
        let xdg = XdgDirectories {
            config: Some(PathBuf::from("/home/test/.config")),
            state: Some(PathBuf::from("/home/test/.local/state")),
            runtime: Some(PathBuf::from("/run/user/1000")),
        };
        let units = render_units(Path::new("/opt/igor/bin/igor"), &xdg)?;
        assert_eq!(
            units.map(|unit| unit.contents),
            [
                "[Unit]\nDescription=Igor worker\n\n[Service]\nType=simple\nExecStart=\"/opt/igor/bin/igor\" worker\nRestart=on-failure\nTimeoutStopSec=10s\nEnvironment=\"XDG_CONFIG_HOME=/home/test/.config\" \"XDG_STATE_HOME=/home/test/.local/state\" \"XDG_RUNTIME_DIR=/run/user/1000\"\nKillMode=process\n\n[Install]\nWantedBy=default.target\n".to_owned(),
                "[Unit]\nDescription=Igor supervisor\n\n[Service]\nType=simple\nExecStart=\"/opt/igor/bin/igor\" supervisor\nRestart=on-failure\nTimeoutStopSec=10s\nEnvironment=\"XDG_CONFIG_HOME=/home/test/.config\" \"XDG_STATE_HOME=/home/test/.local/state\" \"XDG_RUNTIME_DIR=/run/user/1000\"\nNice=10\nIOSchedulingClass=idle\n\n[Install]\nWantedBy=default.target\n".to_owned(),
            ]
        );
        Ok(())
    }

    #[test]
    fn quotes_tricky_binary_and_environment_paths() -> Result<(), String> {
        let dirs = XdgDirectories {
            config: Some(PathBuf::from("/home/user/a b/%x\"q")),
            ..Default::default()
        };
        let units = render_units(Path::new("/opt/Igor's bin/igor%worker"), &dirs)?;
        assert!(
            units[0]
                .contents
                .contains("ExecStart=\"/opt/Igor's bin/igor%%worker\" worker")
        );
        assert!(
            units[0]
                .contents
                .contains("Environment=\"XDG_CONFIG_HOME=/home/user/a b/%%x\\\"q\"")
        );
        Ok(())
    }

    #[test]
    fn escapes_dollar_only_in_executable() -> Result<(), String> {
        let dirs = XdgDirectories {
            config: Some(PathBuf::from("/home/user/$literal/config")),
            ..Default::default()
        };
        let units = render_units(Path::new("/opt/$IGNORED/igor"), &dirs)?;
        assert!(
            units[0]
                .contents
                .contains("ExecStart=\"/opt/$$IGNORED/igor\" worker")
        );
        assert!(
            units[0]
                .contents
                .contains("Environment=\"XDG_CONFIG_HOME=/home/user/$literal/config\"")
        );
        Ok(())
    }

    #[test]
    fn rejects_relative_non_utf8_and_control_paths() {
        assert!(render_units(Path::new("igor"), &XdgDirectories::default()).is_err());
        assert!(render_units(Path::new("/bad\npath"), &XdgDirectories::default()).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            assert!(
                render_units(
                    Path::new(std::ffi::OsStr::from_bytes(b"/bad\xff")),
                    &XdgDirectories::default()
                )
                .is_err()
            );
            let dirs = XdgDirectories {
                state: Some(PathBuf::from("relative")),
                ..Default::default()
            };
            assert!(render_units(Path::new("/igor"), &dirs).is_err());
        }
    }
}
