use std::error::Error;

use igor_core::{AttemptId, AttemptMetrics, JobId};
use igor_daemon::{
    DaemonRole, PROTOCOL_VERSION, ProtocolError, ProtocolErrorKind, Request, RequestEnvelope,
    Response, ResponseEnvelope, Version,
};

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
