//! Argument-vector planning and strict status parsing for systemd user units.

use std::{collections::BTreeMap, path::Path};

const SYSTEMD_RUN: &str = "systemd-run";
const SYSTEMCTL: &str = "systemctl";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlannedCommand {
    pub(crate) program: &'static str,
    pub(crate) args: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UnitOutcome {
    Running { invocation_id: String },
    Exited { invocation_id: String, status: i32 },
    Signaled { invocation_id: String, signal: i32 },
    Missing,
    Indeterminate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnitStatus {
    pub(crate) outcome: UnitOutcome,
    pub(crate) invocation_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PlanError {
    UnitName,
    Path,
    Environment,
    Program,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid systemd command input: {self:?}")
    }
}

impl std::error::Error for PlanError {}

pub(crate) fn unit_name(attempt_uuid: &str) -> Result<String, PlanError> {
    let valid = attempt_uuid.len() == 36
        && attempt_uuid.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    if !valid {
        return Err(PlanError::UnitName);
    }
    Ok(format!("igor-job-{attempt_uuid}.service"))
}

pub(crate) fn run(
    unit: &str,
    cwd: &Path,
    stdout: &Path,
    stderr: &Path,
    environment: &BTreeMap<String, String>,
    program: &str,
    args: &[String],
) -> Result<PlannedCommand, PlanError> {
    validate_unit(unit)?;
    for path in [cwd, stdout, stderr] {
        validate_path(path)?;
    }
    if program.is_empty() || has_linebreak(program) {
        return Err(PlanError::Program);
    }
    let mut planned = vec![
        "--user".to_owned(),
        format!("--unit={unit}"),
        "--remain-after-exit".to_owned(),
        "--service-type=exec".to_owned(),
        "--expand-environment=no".to_owned(),
        format!("--working-directory={}", cwd.display()),
        format!("--property=StandardOutput=append:{}", stdout.display()),
        format!("--property=StandardError=append:{}", stderr.display()),
        "--".to_owned(),
        "/usr/bin/env".to_owned(),
        "-i".to_owned(),
        "--".to_owned(),
    ];
    for (key, value) in environment {
        if !valid_environment_key(key) || has_linebreak(value) {
            return Err(PlanError::Environment);
        }
        planned.push(format!("{key}={value}"));
    }
    planned.push(program.to_owned());
    planned.extend(args.iter().cloned());
    Ok(PlannedCommand {
        program: SYSTEMD_RUN,
        args: planned,
    })
}

pub(crate) fn show(unit: &str) -> Result<PlannedCommand, PlanError> {
    validate_unit(unit)?;
    Ok(PlannedCommand {
        program: SYSTEMCTL,
        args: vec![
            "--user".into(),
            "show".into(),
            "--property=LoadState,ActiveState,SubState,Result,ExecMainStatus,ExecMainCode,InvocationID".into(),
            "--".into(),
            unit.into(),
        ],
    })
}

pub(crate) fn stop(unit: &str) -> Result<PlannedCommand, PlanError> {
    unit_operation("stop", unit)
}

pub(crate) fn signal(unit: &str, signal: &str) -> Result<PlannedCommand, PlanError> {
    validate_unit(unit)?;
    if !matches!(signal, "SIGTERM" | "SIGKILL") {
        return Err(PlanError::Program);
    }
    Ok(PlannedCommand {
        program: SYSTEMCTL,
        args: vec![
            "--user".into(),
            "kill".into(),
            format!("--signal={signal}"),
            "--".into(),
            unit.into(),
        ],
    })
}

pub(crate) fn cleanup(unit: &str) -> Result<PlannedCommand, PlanError> {
    unit_operation("reset-failed", unit)
}

fn unit_operation(operation: &str, unit: &str) -> Result<PlannedCommand, PlanError> {
    validate_unit(unit)?;
    Ok(PlannedCommand {
        program: SYSTEMCTL,
        args: vec!["--user".into(), operation.into(), "--".into(), unit.into()],
    })
}

pub(crate) fn parse_status(output: &[u8], expected_invocation: &str) -> UnitStatus {
    parse_unit_status(output, Some(expected_invocation))
}

pub(crate) fn parse_unclaimed_status(output: &[u8]) -> UnitStatus {
    parse_unit_status(output, None)
}

pub(crate) fn parse_launch(output: &[u8], expected_unit: &str) -> Option<String> {
    if validate_unit(expected_unit).is_err() {
        return None;
    }
    let text = std::str::from_utf8(output).ok()?;
    let line = text.strip_suffix('\n').unwrap_or(text);
    let line = line.strip_suffix('\r').unwrap_or(line);
    let (unit, invocation) = line
        .strip_prefix("Running as unit: ")?
        .split_once("; invocation ID: ")?;
    if unit != expected_unit || !valid_invocation_id(invocation) {
        return None;
    }
    Some(invocation.to_owned())
}

fn parse_unit_status(output: &[u8], expected_invocation: Option<&str>) -> UnitStatus {
    if expected_invocation.is_some_and(|id| !valid_invocation_id(id)) {
        return indeterminate();
    }
    let Ok(text) = std::str::from_utf8(output) else {
        return indeterminate();
    };
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            return indeterminate();
        };
        if fields.insert(key, value).is_some() {
            return indeterminate();
        }
    }
    let Some(load) = fields.get("LoadState") else {
        return indeterminate();
    };
    if *load == "not-found" {
        if fields.len() != 1 && fields.len() != 7 {
            return indeterminate();
        }
        if fields.len() == 7
            && [
                "ActiveState",
                "SubState",
                "Result",
                "ExecMainCode",
                "ExecMainStatus",
                "InvocationID",
            ]
            .iter()
            .any(|key| !fields.contains_key(key))
        {
            return indeterminate();
        }
        if fields.iter().any(|(key, value)| match *key {
            "LoadState" => false,
            "ActiveState" => *value != "inactive",
            "SubState" => *value != "dead",
            "Result" => *value != "success",
            "ExecMainCode" | "ExecMainStatus" => *value != "0",
            "InvocationID" => !value.is_empty(),
            _ => true,
        }) {
            return indeterminate();
        }
        return UnitStatus {
            outcome: UnitOutcome::Missing,
            invocation_id: None,
        };
    }
    if *load != "loaded" {
        return indeterminate();
    }
    if [
        "LoadState",
        "ActiveState",
        "SubState",
        "Result",
        "ExecMainStatus",
        "ExecMainCode",
        "InvocationID",
    ]
    .iter()
    .any(|key| !fields.contains_key(key))
    {
        return indeterminate();
    }
    let Some(invocation_id) = fields
        .get("InvocationID")
        .copied()
        .filter(|id| valid_invocation_id(id))
    else {
        return indeterminate();
    };
    if expected_invocation.is_some_and(|expected| invocation_id != expected) {
        return indeterminate();
    }
    let identity = Some(invocation_id.to_owned());
    let outcome = match (
        fields.get("ActiveState").copied(),
        fields.get("SubState").copied(),
    ) {
        (Some("active"), Some("running")) => UnitOutcome::Running {
            invocation_id: invocation_id.to_owned(),
        },
        (Some(active @ ("active" | "failed")), Some(substate))
            if (active == "active" && substate == "exited")
                || (active == "failed" && substate == "failed") =>
        {
            let (Some(result), Some(code), Some(status)) = (
                fields.get("Result"),
                fields.get("ExecMainCode"),
                fields.get("ExecMainStatus"),
            ) else {
                return indeterminate();
            };
            if status.is_empty() || !status.bytes().all(|byte| byte.is_ascii_digit()) {
                return indeterminate();
            }
            let Ok(status) = status.parse::<i32>() else {
                return indeterminate();
            };
            let valid = match (*result, active) {
                ("success", "active") => {
                    (*code == "1" && status == 0) || (matches!(*code, "2" | "3") && status > 0)
                }
                ("exit-code", "failed") => *code == "1" && status > 0,
                ("signal", "failed") => *code == "2" && status > 0,
                ("core-dump", "failed") => *code == "3" && status > 0,
                _ => false,
            };
            if !valid {
                return UnitStatus {
                    outcome: UnitOutcome::Indeterminate,
                    invocation_id: identity,
                };
            }
            if *code == "1" {
                UnitOutcome::Exited {
                    invocation_id: invocation_id.to_owned(),
                    status,
                }
            } else {
                UnitOutcome::Signaled {
                    invocation_id: invocation_id.to_owned(),
                    signal: status,
                }
            }
        }
        _ => {
            return UnitStatus {
                outcome: UnitOutcome::Indeterminate,
                invocation_id: identity,
            };
        }
    };
    UnitStatus {
        outcome,
        invocation_id: identity,
    }
}

fn valid_invocation_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn indeterminate() -> UnitStatus {
    UnitStatus {
        outcome: UnitOutcome::Indeterminate,
        invocation_id: None,
    }
}

fn validate_unit(unit: &str) -> Result<(), PlanError> {
    let Some(uuid) = unit
        .strip_prefix("igor-job-")
        .and_then(|name| name.strip_suffix(".service"))
    else {
        return Err(PlanError::UnitName);
    };
    if unit_name(uuid).as_deref() == Ok(unit) {
        Ok(())
    } else {
        Err(PlanError::UnitName)
    }
}

fn validate_path(path: &Path) -> Result<(), PlanError> {
    let text = path.to_str().ok_or(PlanError::Path)?;
    if !path.is_absolute()
        || text
            .bytes()
            .any(|byte| byte == 0 || byte == b'\n' || byte == b'\r')
    {
        return Err(PlanError::Path);
    }
    Ok(())
}

fn valid_environment_key(key: &str) -> bool {
    let mut bytes = key.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn has_linebreak(value: &str) -> bool {
    value.bytes().any(|byte| byte == b'\n' || byte == b'\r')
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::path::Path;

    const UNIT: &str = "igor-job-123e4567-e89b-12d3-a456-426614174000.service";
    const INVOCATION: &str = "abcdef0123456789abcdef0123456789";

    fn status(active: &str, sub: &str, result: &str, code: &str, exit: &str) -> String {
        format!(
            "LoadState=loaded\nActiveState={active}\nSubState={sub}\nResult={result}\nExecMainStatus={exit}\nExecMainCode={code}\nInvocationID={INVOCATION}\n"
        )
    }

    #[test]
    fn plans_unambiguous_vector_and_preserves_empty_and_space_arguments() {
        let env = BTreeMap::from([
            ("CUDA_VISIBLE_DEVICES".into(), "GPU 0".into()),
            ("EMPTY".into(), "".into()),
        ]);
        let command = run(
            UNIT,
            Path::new("/work dir"),
            Path::new("/logs/out file"),
            Path::new("/logs/err"),
            &env,
            "/opt/my program",
            &[
                "".into(),
                "argument with spaces".into(),
                "kill -TERM $$".into(),
            ],
        )
        .unwrap();
        assert_eq!(command.program, SYSTEMD_RUN);
        assert!(command.args.contains(&"--expand-environment=no".into()));
        assert!(command.args.contains(&"CUDA_VISIBLE_DEVICES=GPU 0".into()));
        assert!(command.args.contains(&"EMPTY=".into()));
        assert_eq!(
            command.args[8..14],
            [
                "--",
                "/usr/bin/env",
                "-i",
                "--",
                "CUDA_VISIBLE_DEVICES=GPU 0",
                "EMPTY="
            ]
        );
        assert_eq!(
            &command.args[14..],
            [
                "/opt/my program",
                "",
                "argument with spaces",
                "kill -TERM $$"
            ]
        );
        assert_eq!(
            unit_name("123e4567-e89b-12d3-a456-426614174000").unwrap(),
            UNIT
        );
    }

    #[test]
    fn plans_status_stop_and_cleanup_with_end_of_options() {
        assert_eq!(show(UNIT).unwrap().args.last().unwrap(), UNIT);
        assert_eq!(stop(UNIT).unwrap().args, ["--user", "stop", "--", UNIT]);
        assert_eq!(
            cleanup(UNIT).unwrap().args,
            ["--user", "reset-failed", "--", UNIT]
        );
        assert!(stop("--user.service").is_err());
    }

    #[test]
    fn parses_running_and_real_systemd_retained_success_and_failure() {
        let success = format!(
            "Result=success\nExecMainCode=1\nExecMainStatus=0\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID={INVOCATION}\n"
        );
        let failure = format!(
            "Result=exit-code\nExecMainCode=1\nExecMainStatus=1\nLoadState=loaded\nActiveState=failed\nSubState=failed\nInvocationID={INVOCATION}\n"
        );
        assert!(matches!(
            parse_status(
                status("active", "running", "", "", "").as_bytes(),
                INVOCATION
            )
            .outcome,
            UnitOutcome::Running { .. }
        ));
        assert_eq!(
            parse_status(success.as_bytes(), INVOCATION).outcome,
            UnitOutcome::Exited {
                invocation_id: INVOCATION.into(),
                status: 0
            }
        );
        assert_eq!(
            parse_status(failure.as_bytes(), INVOCATION).outcome,
            UnitOutcome::Exited {
                invocation_id: INVOCATION.into(),
                status: 1
            }
        );
        for (result, code) in [("signal", "2"), ("core-dump", "3")] {
            assert_eq!(
                parse_status(
                    status("failed", "failed", result, code, "15").as_bytes(),
                    INVOCATION
                )
                .outcome,
                UnitOutcome::Signaled {
                    invocation_id: INVOCATION.into(),
                    signal: 15
                }
            );
        }
        assert_eq!(
            parse_status(
                status("active", "exited", "success", "2", "15").as_bytes(),
                INVOCATION
            )
            .outcome,
            UnitOutcome::Signaled {
                invocation_id: INVOCATION.into(),
                signal: 15
            }
        );
        assert_eq!(
            parse_status(b"LoadState=not-found\n", INVOCATION).outcome,
            UnitOutcome::Missing
        );
        assert_eq!(parse_status(b"Result=success\nExecMainCode=0\nExecMainStatus=0\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nInvocationID=\n", INVOCATION).outcome, UnitOutcome::Missing);
        let mismatch = success.replace(INVOCATION, "wrong");
        assert_eq!(
            parse_status(mismatch.as_bytes(), INVOCATION).outcome,
            UnitOutcome::Indeterminate
        );
        assert_eq!(
            parse_status(b"LoadState=loaded\nActiveState=inactive\n", INVOCATION).outcome,
            UnitOutcome::Indeterminate
        );
        assert_eq!(
            parse_status(
                b"Result=success\nExecMainCode=1\nExecMainStatus=nope\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID=abcdef0123456789abcdef0123456789\n",
                INVOCATION
            )
            .outcome,
            UnitOutcome::Indeterminate
        );
    }

    #[test]
    fn parses_launch_output_and_rejects_non_exact_responses() {
        let id = "068e9623bae04dd59be4c837ff058a3f";
        let response = format!("Running as unit: {UNIT}; invocation ID: {id}\n");
        assert_eq!(parse_launch(response.as_bytes(), UNIT).as_deref(), Some(id));
        for invalid in [
            response.replace(UNIT, "igor-job-other.service"),
            format!("{}\nextra\n", response.trim_end()),
            response.replace(id, "068E9623bae04dd59be4c837ff058a3f"),
            response.replace(id, "short"),
        ] {
            assert_eq!(parse_launch(invalid.as_bytes(), UNIT), None);
        }
    }

    #[test]
    fn parses_unclaimed_full_status_and_rejects_ambiguous_fields() {
        assert!(matches!(
            parse_unclaimed_status(status("active", "running", "", "", "").as_bytes()).outcome,
            UnitOutcome::Running { invocation_id } if invocation_id == INVOCATION
        ));
        assert_eq!(
            parse_unclaimed_status(b"LoadState=not-found\n").outcome,
            UnitOutcome::Missing
        );
        let exit = format!(
            "Result=success\nExecMainCode=1\nExecMainStatus=0\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID={INVOCATION}\n"
        );
        assert_eq!(
            parse_unclaimed_status(exit.as_bytes()).outcome,
            UnitOutcome::Exited {
                invocation_id: INVOCATION.into(),
                status: 0
            }
        );
        assert_eq!(
            parse_unclaimed_status(
                b"LoadState=loaded\nInvocationID=abcdef0123456789abcdef0123456789\n"
            )
            .outcome,
            UnitOutcome::Indeterminate
        );
        assert_eq!(
            parse_unclaimed_status(b"LoadState=not-found\nActiveState=inactive\n").outcome,
            UnitOutcome::Indeterminate
        );
    }

    #[test]
    fn rejects_invalid_ids_duplicate_fields_and_inconsistent_status() {
        let valid = format!(
            "Result=success\nExecMainCode=1\nExecMainStatus=0\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID={INVOCATION}\n"
        );
        assert_eq!(
            parse_status(valid.as_bytes(), "ABCDEF0123456789abcdef0123456789").outcome,
            UnitOutcome::Indeterminate
        );
        assert_eq!(
            parse_status(
                valid
                    .replace(INVOCATION, "abcdef0123456789abcdef012345678g")
                    .as_bytes(),
                INVOCATION
            )
            .outcome,
            UnitOutcome::Indeterminate
        );
        assert_eq!(
            parse_status(
                format!("{valid}ActiveState=failed\n").as_bytes(),
                INVOCATION
            )
            .outcome,
            UnitOutcome::Indeterminate
        );
        for bogus in [
            format!(
                "Result=success\nExecMainCode=1\nExecMainStatus=1\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID={INVOCATION}\n"
            ),
            format!(
                "Result=exit-code\nExecMainCode=1\nExecMainStatus=0\nLoadState=loaded\nActiveState=failed\nSubState=failed\nInvocationID={INVOCATION}\n"
            ),
            format!(
                "Result=success\nExecMainCode=exited\nExecMainStatus=0\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID={INVOCATION}\n"
            ),
        ] {
            assert_eq!(
                parse_status(bogus.as_bytes(), INVOCATION).outcome,
                UnitOutcome::Indeterminate
            );
        }
    }

    #[test]
    fn rejects_unsafe_paths_environment_and_unit_names() {
        let env = BTreeMap::new();
        assert!(
            run(
                UNIT,
                Path::new("relative"),
                Path::new("/out"),
                Path::new("/err"),
                &env,
                "/bin/true",
                &[]
            )
            .is_err()
        );
        assert!(
            run(
                UNIT,
                Path::new("/bad\npath"),
                Path::new("/out"),
                Path::new("/err"),
                &env,
                "/bin/true",
                &[]
            )
            .is_err()
        );
        assert!(
            run(
                "igor-job-x.service",
                Path::new("/"),
                Path::new("/out"),
                Path::new("/err"),
                &env,
                "/bin/true",
                &[]
            )
            .is_err()
        );
        assert!(
            run(
                UNIT,
                Path::new("/"),
                Path::new("/out"),
                Path::new("/err"),
                &BTreeMap::from([("bad=key".into(), "v".into())]),
                "/bin/true",
                &[]
            )
            .is_err()
        );
    }
}
