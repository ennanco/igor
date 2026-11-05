use std::error::Error;

use igor_core::{AttemptId, AttemptMetrics, JobId};
use igor_daemon::{
    DaemonRole, PROTOCOL_VERSION, ProtocolError, ProtocolErrorKind, Request, RequestEnvelope,
    Response, ResponseEnvelope, Version,
};

#[test]
fn report_retry_wire_receipt_binds_new_nonce_to_existing_action() -> Result<(), Box<dyn Error>> {
    let family_id = igor_core::FamilyId::new();
    let project_id = igor_core::ProjectId::new();
    let request_id = igor_core::ActionId::new();
    let action_id = igor_core::ActionId::new();
    let request = RequestEnvelope::new(Request::ReportRetry {
        family_id,
        project_id: Some(project_id),
        request_id,
    });
    assert_eq!(
        serde_json::from_value::<RequestEnvelope>(serde_json::to_value(&request)?)?,
        request
    );
    let receipt = igor_core::ReportRetryReceipt {
        request_id,
        report: igor_core::ReportRequestReceipt {
            action_id,
            project_id,
            family_id,
            generation: igor_core::GenerationIdentity {
                id: igor_core::GenerationId::new(),
                number: 1,
                source_revision: "revision".into(),
                protocol_digest: "sha256:protocol".into(),
            },
            output: "/fixture/RESULTS.md".into(),
            state: igor_core::ActionState::Pending,
        },
    };
    let response = ResponseEnvelope::success(Response::ReportRetried(receipt));
    let json = serde_json::to_value(&response)?;
    assert_eq!(json["protocol_version"], 22);
    assert_eq!(json["response"]["request_id"], request_id.to_string());
    assert_eq!(json["response"]["action_id"], action_id.to_string());
    assert_eq!(serde_json::from_value::<ResponseEnvelope>(json)?, response);
    Ok(())
}

#[test]
fn manual_report_request_and_receipt_round_trip_with_current_protocol() -> Result<(), Box<dyn Error>>
{
    let family_id = igor_core::FamilyId::new();
    let request_id = igor_core::ActionId::new();
    let request = RequestEnvelope::new(Request::ReportGenerate {
        family_id,
        project_id: None,
        request_id,
    });
    let value = serde_json::to_value(&request)?;
    assert_eq!(value["protocol_version"], 22);
    assert_eq!(value["request"]["type"], "report_generate");
    assert_eq!(value["request"]["request_id"], request_id.to_string());
    assert_eq!(serde_json::from_value::<RequestEnvelope>(value)?, request);
    let receipt = igor_core::ReportRequestReceipt {
        action_id: request_id,
        project_id: igor_core::ProjectId::new(),
        family_id,
        generation: igor_core::GenerationIdentity {
            id: igor_core::GenerationId::new(),
            number: 2,
            source_revision: "frozen-revision".into(),
            protocol_digest: "sha256:protocol".into(),
        },
        output: "/fixture/RESULTS.md".into(),
        state: igor_core::ActionState::Pending,
    };
    let response = ResponseEnvelope::success(Response::ReportRequested(receipt));
    let value = serde_json::to_value(&response)?;
    assert_eq!(value["response"]["type"], "report_requested");
    assert_eq!(value["response"]["state"], "pending");
    assert_eq!(serde_json::from_value::<ResponseEnvelope>(value)?, response);
    Ok(())
}

#[tokio::test]
async fn storage_inventory_round_trips_with_explicit_unmeasured_bytes() -> Result<(), Box<dyn Error>>
{
    for project_id in [None, Some(igor_core::ProjectId::new())] {
        let gc = RequestEnvelope::new(Request::GcPlan {
            project_id,
            failed_only: false,
        });
        assert_eq!(
            serde_json::from_value::<RequestEnvelope>(serde_json::to_value(&gc)?)?,
            gc
        );
        let request = RequestEnvelope::new(Request::StorageStatus { project_id });
        assert_eq!(
            serde_json::from_value::<RequestEnvelope>(serde_json::to_value(&request)?)?,
            request
        );
    }
    let temporary = tempfile::tempdir()?;
    let database = igor_core::Database::open(temporary.path().join("storage.db")).await?;
    let root = temporary.path();
    let paths = igor_core::RuntimePaths {
        config_file: root.join("config"),
        state_dir: root.join("state"),
        runtime_dir: root.join("runtime"),
        database: root.join("storage.db"),
        log_dir: root.join("logs"),
        worktree_dir: root.join("worktrees"),
        report_dir: root.join("reports"),
        worker_socket: root.join("worker.sock"),
        supervisor_socket: root.join("supervisor.sock"),
        resource_dir: root.join("resources"),
    };
    let gc = ResponseEnvelope::success(Response::GcPlan(database.gc_plan(None, &paths).await?));
    let encoded = serde_json::to_value(&gc)?;
    assert_eq!(encoded["response"]["type"], "gc_plan");
    assert_eq!(encoded["protocol_version"], 22);
    assert_eq!(serde_json::from_value::<ResponseEnvelope>(encoded)?, gc);
    let response = ResponseEnvelope::success(Response::StorageStatus(
        database.storage_status(None).await?,
    ));
    let encoded = serde_json::to_value(&response)?;
    assert_eq!(encoded["protocol_version"], 22);
    assert_eq!(encoded["response"]["type"], "storage_status");
    assert!(encoded["response"]["actual_disk_footprint"].is_null());
    assert_eq!(encoded["response"]["pending_log_preparations"]["count"], 0);
    assert_eq!(
        encoded["response"]["pending_log_preparations"]["temporary_bytes"],
        serde_json::Value::Null
    );
    assert_eq!(
        serde_json::from_value::<ResponseEnvelope>(encoded)?,
        response
    );
    Ok(())
}

#[test]
fn gc_execution_wire_contract_round_trips_with_structured_outcomes() -> Result<(), Box<dyn Error>> {
    assert_eq!(PROTOCOL_VERSION, 22);
    let request = RequestEnvelope::new(Request::GcExecute {
        project_id: None,
        failed_only: true,
        worklist: igor_core::GcWorklist::default(),
    });
    assert_eq!(
        serde_json::from_value::<RequestEnvelope>(serde_json::to_value(&request)?)?,
        request
    );
    for status in [
        igor_core::GcExecutionStatus::Completed,
        igor_core::GcExecutionStatus::AlreadyDone,
        igor_core::GcExecutionStatus::Deferred,
        igor_core::GcExecutionStatus::Unsafe,
        igor_core::GcExecutionStatus::Error,
    ] {
        let response = ResponseEnvelope::success(Response::GcExecution(igor_core::GcExecution {
            confirmed: true,
            entries: vec![igor_core::GcExecutionEntry {
                operation: igor_core::GcOperation::FailedArtifacts,
                subject_id: "fixture".into(),
                status,
                cause: Some(igor_core::GcExecutionCause::new(
                    "barrier",
                    "current eligibility",
                )),
            }],
            ..Default::default()
        }));
        let value = serde_json::to_value(&response)?;
        assert_eq!(value["response"]["type"], "gc_execution");
        assert_eq!(serde_json::from_value::<ResponseEnvelope>(value)?, response);
    }
    Ok(())
}

#[test]
fn requests_have_stable_versioned_json() -> Result<(), Box<dyn Error>> {
    let encoded = serde_json::to_string(&RequestEnvelope::new(Request::DatabaseStatus))?;
    assert_eq!(
        encoded,
        format!(
            "{{\"protocol_version\":{PROTOCOL_VERSION},\"request\":{{\"type\":\"database_status\"}}}}"
        )
    );
    let decoded: RequestEnvelope = serde_json::from_str(&encoded)?;
    assert_eq!(decoded, RequestEnvelope::new(Request::DatabaseStatus));
    Ok(())
}

#[test]
fn responses_contain_exactly_the_public_contract() -> Result<(), Box<dyn Error>> {
    let response = ResponseEnvelope::success(Response::Version(Version {
        role: DaemonRole::Worker,
        igor: "0.1.0".into(),
        protocol: PROTOCOL_VERSION,
    }));
    let encoded = serde_json::to_value(&response)?;
    assert_eq!(encoded["protocol_version"], PROTOCOL_VERSION);
    assert_eq!(encoded["response"]["type"], "version");
    assert_eq!(encoded["response"]["role"], "worker");
    assert!(encoded.get("error").is_none());
    assert_eq!(
        serde_json::from_value::<ResponseEnvelope>(encoded)?,
        response
    );
    Ok(())
}

#[test]
fn protocol_errors_have_stable_codes() {
    let mismatch = ProtocolError::incompatible(99);
    assert_eq!(mismatch.code, "IGOR-PROTO-001");
    assert_eq!(mismatch.kind, ProtocolErrorKind::IncompatibleProtocol);
    assert_eq!(mismatch.found_version, Some(99));
    assert_eq!(mismatch.supported_version, Some(PROTOCOL_VERSION));
    assert_eq!(
        ProtocolError::invalid_request("invalid").code,
        "IGOR-PROTO-002"
    );
    assert_eq!(ProtocolError::frame_too_large().code, "IGOR-PROTO-003");
    assert_eq!(
        ProtocolError::database_unavailable().code,
        "IGOR-DAEMON-001"
    );
    assert_eq!(ProtocolError::internal().code, "IGOR-DAEMON-002");
}

#[test]
fn milestone_five_requests_round_trip() -> Result<(), Box<dyn Error>> {
    for request in [
        Request::ProjectList,
        Request::ProjectRemove {
            root: "/tmp/project".into(),
        },
        Request::JobList { project_id: None },
    ] {
        let envelope = RequestEnvelope::new(request);
        let encoded = serde_json::to_vec(&envelope)?;
        assert_eq!(
            serde_json::from_slice::<RequestEnvelope>(&encoded)?,
            envelope
        );
    }
    assert_eq!(ProtocolError::not_found("job").code, "IGOR-API-001");
    assert_eq!(ProtocolError::conflict("project").code, "IGOR-API-002");
    Ok(())
}

#[test]
fn milestone_six_operator_requests_round_trip() -> Result<(), Box<dyn Error>> {
    let job_id = JobId::new();
    for request in [
        Request::JobCancel {
            job_id,
            grace_seconds: 5,
        },
        Request::JobRetry { job_id },
        Request::JobLogs { job_id },
    ] {
        let envelope = RequestEnvelope::new(request);
        let encoded = serde_json::to_vec(&envelope)?;
        assert_eq!(
            serde_json::from_slice::<RequestEnvelope>(&encoded)?,
            envelope
        );
    }
    Ok(())
}

#[test]
fn milestone_seven_resource_request_round_trips() -> Result<(), Box<dyn Error>> {
    let envelope = RequestEnvelope::new(Request::Resources);
    let encoded = serde_json::to_vec(&envelope)?;
    assert_eq!(
        serde_json::from_slice::<RequestEnvelope>(&encoded)?,
        envelope
    );
    Ok(())
}

#[test]
fn metrics_protocol_carries_attempt_identity_and_extraction_state() -> Result<(), Box<dyn Error>> {
    let job_id = JobId::new();
    let attempt_id = AttemptId::new();
    let request = RequestEnvelope::new(Request::JobMetrics { job_id });
    let encoded = serde_json::to_value(&request)?;
    assert_eq!(encoded["protocol_version"], PROTOCOL_VERSION);
    assert_eq!(encoded["request"]["type"], "job_metrics");
    assert_eq!(serde_json::from_value::<RequestEnvelope>(encoded)?, request);

    let response = ResponseEnvelope::success(Response::Metrics {
        job_id,
        attempts: vec![AttemptMetrics {
            attempt_id,
            action_state: Some("failed".into()),
            last_error: Some("invalid_extractor_json".into()),
            document: None,
            selection: None,
            metrics: Vec::new(),
        }],
    });
    let encoded = serde_json::to_value(&response)?;
    assert_eq!(encoded["response"]["type"], "metrics");
    assert_eq!(encoded["response"]["job_id"], job_id.to_string());
    assert_eq!(
        encoded["response"]["attempts"][0]["attempt_id"],
        attempt_id.to_string()
    );
    assert_eq!(encoded["response"]["attempts"][0]["action_state"], "failed");
    assert_eq!(
        serde_json::from_value::<ResponseEnvelope>(encoded)?,
        response
    );
    Ok(())
}
