use std::{collections::BTreeMap, error::Error, path::PathBuf};

use igor_core::{
    ActionId, ActionRetryPolicy, ActionState, AgentSessionId, ArtifactRole, AttemptId,
    AttemptRetryPolicy, AttemptSpec, AttemptState, CleanupState, CommandSpec,
    ConfigurationIdentity, DeliveryId, DeliveryRetryPolicy, DeliveryState, DockerExecutorSpec,
    DockerMount, DomainError, EnvironmentInheritance, EnvironmentPolicy, ErrorCategory, ErrorCode,
    Event, EventId, EventKind, EventPayload, ExecutorSpec, FamilyId, FamilyMembership,
    GenerationId, GenerationIdentity, GpuRequest, JobId, JobSpec, JobState, MountAccess,
    NamedResourceMode, NamedResourceRequest, ProcessExecutorSpec, Project, ProjectConfig,
    ProjectId, RecoveryId, RecoveryState, ReportId, ReportState, ResourceId, ResourceMode,
    ResourceRequest, ResultContract, RetentionDecision, RetryPolicy, Seed, ShellPolicy,
    SourceIdentity, TransitionState,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;

fn round_trip<T>(value: &T) -> Result<(), Box<dyn Error>>
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(value)?;
    let restored = serde_json::from_str::<T>(&json)?;
    assert_eq!(&restored, value);
    Ok(())
}

#[test]
fn every_identifier_round_trips() -> Result<(), Box<dyn Error>> {
    round_trip(&ProjectId::new())?;
    round_trip(&FamilyId::new())?;
    round_trip(&GenerationId::new())?;
    round_trip(&JobId::new())?;
    round_trip(&AttemptId::new())?;
    round_trip(&EventId::new())?;
    round_trip(&ActionId::new())?;
    round_trip(&DeliveryId::new())?;
    round_trip(&RecoveryId::new())?;
    round_trip(&ReportId::new())?;
    round_trip(&ResourceId::new())?;
    round_trip(&AgentSessionId::new())
}

fn command() -> CommandSpec {
    CommandSpec {
        program: "python".into(),
        args: vec![
            "-m".into(),
            "experiment runner".into(),
            String::new(),
            "--flag=value with spaces".into(),
            "--".into(),
        ],
        cwd: PathBuf::from("/tmp/a working directory"),
        shell: ShellPolicy::Direct,
        environment: EnvironmentPolicy {
            set: BTreeMap::from([("OMP_NUM_THREADS".into(), "8".into())]),
            remove: vec!["TELEGRAM_BOT_TOKEN".into()],
            inherit: EnvironmentInheritance::Minimal,
        },
    }
}

fn resources() -> ResourceRequest {
    ResourceRequest {
        mode: ResourceMode::Shared,
        cpu_threads: Some(8),
        memory_bytes: Some(16 * 1024 * 1024),
        timeout_seconds: Some(3600),
        gpu: GpuRequest::Specific("GPU-1".into()),
        gpu_count: 1,
        gpu_exclusive: true,
        named: vec![NamedResourceRequest {
            name: "scratch-disk".into(),
            mode: NamedResourceMode::Exclusive,
        }],
    }
}

fn family() -> FamilyMembership {
    FamilyMembership {
        family_id: FamilyId::new(),
        generation: GenerationIdentity {
            id: GenerationId::new(),
            number: 2,
            source_revision: "0123456789abcdef".into(),
            protocol_digest: "sha256:protocol".into(),
        },
        seed: Seed(42),
    }
}

fn executor() -> ExecutorSpec {
    ExecutorSpec::Docker(DockerExecutorSpec {
        image: "example/image:tag".into(),
        digest: Some("sha256:container".into()),
        mounts: vec![DockerMount {
            source: PathBuf::from("/tmp/input data"),
            target: PathBuf::from("/data/input"),
            access: MountAccess::ReadOnly,
        }],
        remove_container: true,
    })
}

#[test]
fn persisted_models_round_trip_without_losing_arguments() -> Result<(), Box<dyn Error>> {
    let project = Project {
        id: ProjectId::new(),
        name: "research".into(),
        root: PathBuf::from("/tmp/research"),
        config_path: PathBuf::from("/tmp/research/igor.toml"),
    };
    project.validate()?;
    round_trip(&project)?;

    let job = JobSpec {
        id: JobId::new(),
        project_id: project.id,
        name: "seed 42".into(),
        command: command(),
        executor: executor(),
        resources: resources(),
        retry: AttemptRetryPolicy::default(),
        family: Some(family()),
    };
    job.validate()?;
    round_trip(&job)?;

    let attempt = AttemptSpec::from_job(
        AttemptId::new(),
        1,
        &job,
        SourceIdentity::SnapshotDigest("0123456789abcdef".into()),
        ConfigurationIdentity {
            project_digest: "sha256:project".into(),
            job_digest: "sha256:job".into(),
            contents: Vec::new(),
        },
        ResultContract {
            schema_version: 1,
            extractor: Some(command()),
        },
    )?;
    round_trip(&attempt)?;
    assert_eq!(attempt.command().args, command().args);
    Ok(())
}

#[test]
fn omitted_execution_fields_have_safe_defaults() -> Result<(), Box<dyn Error>> {
    let value = json!({
        "id": JobId::new(),
        "project_id": ProjectId::new(),
        "name": "minimal",
        "command": { "program": "true", "cwd": "/tmp" }
    });
    let job: JobSpec = serde_json::from_value(value)?;
    assert!(job.command.args.is_empty());
    assert_eq!(job.command.shell, ShellPolicy::Direct);
    assert_eq!(job.command.environment, EnvironmentPolicy::default());
    assert_eq!(
        job.executor,
        ExecutorSpec::Process(ProcessExecutorSpec::default())
    );
    assert_eq!(job.resources, ResourceRequest::default());
    assert_eq!(job.resources.cpu_threads, None);
    assert_eq!(job.resources.memory_bytes, None);
    assert_eq!(job.resources.timeout_seconds, None);
    assert_eq!(job.resources.gpu_count, 1);
    assert!(job.resources.gpu_exclusive);
    Ok(())
}

#[test]
fn legacy_git_revision_identity_remains_readable() -> Result<(), Box<dyn Error>> {
    let source: SourceIdentity = serde_json::from_value(json!({
        "kind": "git_revision",
        "identity": "0123456789abcdef"
    }))?;
    assert_eq!(
        source,
        SourceIdentity::GitRevision("0123456789abcdef".into())
    );
    Ok(())
}

#[test]
fn remaining_persisted_contracts_round_trip() -> Result<(), Box<dyn Error>> {
    let config = ProjectConfig::from_json(r#"{"schema_version":1}"#)?;
    assert_eq!(config.resources, ResourceRequest::default());
    round_trip(&config)?;
    round_trip(&command())?;
    round_trip(&EnvironmentPolicy::default())?;
    round_trip(&executor())?;
    round_trip(&resources())?;
    round_trip(&family())?;
    round_trip(&RetryPolicy {
        max_attempts: 4,
        initial_delay_seconds: 2,
        max_delay_seconds: 60,
        multiplier: 2,
    })?;
    round_trip(&AttemptRetryPolicy::default())?;
    round_trip(&ActionRetryPolicy::default())?;
    round_trip(&DeliveryRetryPolicy::default())?;
    round_trip(&ArtifactRole::Checkpoint)?;
    round_trip(&RetentionDecision::DeleteAfterDiagnosis)?;

    let payload = EventPayload::new(EventKind::JobSubmitted, 1, json!({"priority": 10}))?;
    round_trip(&payload)?;
    round_trip(&Event::new(
        EventId::new(),
        EventKind::JobSubmitted,
        payload,
    )?)?;

    let error = DomainError::InvalidTransition {
        category: ErrorCategory::StateTransition,
        code: ErrorCode::InvalidStateTransition,
        entity: "job".into(),
        from: "queued".into(),
        to: "succeeded".into(),
    };
    round_trip(&error)?;
    round_trip(&ErrorCategory::Compatibility)?;
    round_trip(&ErrorCode::UnsupportedProjectConfigVersion)
}

macro_rules! transition_matrix_test {
    ($test:ident, $state:ty, [$($from:ident => [$($to:ident),*]),+ $(,)?]) => {
        #[test]
        fn $test() {
            let legal = [$( $( (<$state>::$from, <$state>::$to), )* )+];
            for &from in <$state>::ALL {
                for &to in <$state>::ALL {
                    let expected = legal.contains(&(from, to));
                    assert_eq!(
                        from.can_transition_to(to),
                        expected,
                        "can_transition_to mismatch for {from:?} -> {to:?}",
                    );
                    let result = from.transition_to(to);
                    assert_eq!(
                        result.is_ok(),
                        expected,
                        "transition_to mismatch for {from:?} -> {to:?}: {result:?}",
                    );
                    if let Err(error) = result {
                        assert_eq!(error.code(), ErrorCode::InvalidStateTransition);
                        assert_eq!(error.category(), ErrorCategory::StateTransition);
                    }
                }
            }
        }
    };
}

transition_matrix_test!(job_transition_matrix, JobState, [
    Queued => [Running, Cancelled, Superseded], Running => [Succeeded, Failed, Cancelled, Lost],
    Succeeded => [Superseded], Failed => [Queued, Superseded],
    Cancelled => [Queued, Superseded], Lost => [Queued, Superseded],
    Superseded => []
]);
transition_matrix_test!(attempt_transition_matrix, AttemptState, [
    Pending => [Starting, Cancelled], Starting => [Running, Failed, Cancelled, Lost],
    Running => [Succeeded, Failed, Cancelled, Lost], Succeeded => [], Failed => [],
    Cancelled => [], Lost => []
]);
transition_matrix_test!(action_transition_matrix, ActionState, [
    Pending => [Running, Cancelled], Running => [Succeeded, Failed, Pending, Cancelled],
    Succeeded => [], Failed => [Pending], Cancelled => []
]);
transition_matrix_test!(delivery_transition_matrix, DeliveryState, [
    Pending => [Delivering, Cancelled], Delivering => [Delivered, Failed, Pending, Cancelled],
    Delivered => [], Failed => [Pending], Cancelled => []
]);
transition_matrix_test!(recovery_transition_matrix, RecoveryState, [
    Pending => [Diagnosing, Cancelled], Diagnosing => [Proposed, Failed, Cancelled],
    Proposed => [Approved, Rejected, Cancelled], Approved => [Applying, Cancelled], Rejected => [],
    Applying => [Succeeded, Failed, Cancelled], Succeeded => [], Failed => [Pending], Cancelled => []
]);
transition_matrix_test!(report_transition_matrix, ReportState, [
    Pending => [Generating, Cancelled], Generating => [Published, Failed, Pending, Cancelled],
    Published => [Superseded], Failed => [Pending], Cancelled => [], Superseded => []
]);
transition_matrix_test!(cleanup_transition_matrix, CleanupState, [
    Pending => [Running, Cancelled], Running => [Succeeded, Failed, Pending, Cancelled],
    Succeeded => [], Failed => [Pending], Cancelled => []
]);

#[test]
fn every_state_contract_round_trips() -> Result<(), Box<dyn Error>> {
    for state in JobState::ALL {
        round_trip(state)?;
    }
    for state in AttemptState::ALL {
        round_trip(state)?;
    }
    for state in ActionState::ALL {
        round_trip(state)?;
    }
    for state in DeliveryState::ALL {
        round_trip(state)?;
    }
    for state in RecoveryState::ALL {
        round_trip(state)?;
    }
    for state in ReportState::ALL {
        round_trip(state)?;
    }
    for state in CleanupState::ALL {
        round_trip(state)?;
    }
    Ok(())
}

#[test]
fn unsupported_project_config_version_is_actionable() {
    let json = r#"{"schema_version":99}"#;
    let result = ProjectConfig::from_json(json);
    let Err(error) = result else {
        panic!("unsupported project config version was accepted");
    };
    assert_eq!(error.category(), ErrorCategory::Compatibility);
    assert_eq!(error.code(), ErrorCode::UnsupportedProjectConfigVersion);
    let DomainError::UnsupportedVersion {
        contract,
        found,
        supported,
        ..
    } = &error
    else {
        panic!("unsupported version returned the wrong error shape");
    };
    assert_eq!(contract, "project config");
    assert_eq!((*found, *supported), (99, 1));
    assert!(error.to_string().contains("version 99"));
    assert!(error.to_string().contains("supported version is 1"));
    assert!(error.to_string().contains("migrate"));
    assert!(serde_json::from_str::<ProjectConfig>(json).is_err());
}

#[test]
fn unsupported_event_payload_version_is_actionable() {
    let json = r#"{"schema_version":8,"data":{}}"#;
    let result = EventPayload::from_json(EventKind::AttemptStateChanged, json);
    let Err(error) = result else {
        panic!("unsupported event payload version was accepted");
    };
    assert_eq!(error.category(), ErrorCategory::Compatibility);
    assert_eq!(error.code(), ErrorCode::UnsupportedEventPayloadVersion);
    let DomainError::UnsupportedVersion {
        contract,
        found,
        supported,
        ..
    } = &error
    else {
        panic!("unsupported version returned the wrong error shape");
    };
    assert_eq!(contract, "attempt_state_changed event payload");
    assert_eq!((*found, *supported), (8, 1));
    assert!(
        error
            .to_string()
            .contains("attempt_state_changed event payload")
    );
    assert!(error.to_string().contains("version 8"));
    assert!(error.to_string().contains("supported version is 1"));
    assert!(error.to_string().contains("migrate"));
    assert!(serde_json::from_str::<EventPayload>(json).is_err());
}
