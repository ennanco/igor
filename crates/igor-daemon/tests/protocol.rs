use std::error::Error;

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
