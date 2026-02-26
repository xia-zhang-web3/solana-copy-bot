use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedUpstreamReject {
    pub(crate) retryable: bool,
    pub(crate) code: String,
    pub(crate) detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UpstreamOutcome {
    Success,
    Reject(ParsedUpstreamReject),
}

pub(crate) fn parse_upstream_outcome(body: &Value, default_reject_code: &str) -> UpstreamOutcome {
    let status = match parse_optional_status_field(body) {
        Ok(Some(value)) => value,
        Ok(None) => String::new(),
        Err(detail) => {
            return UpstreamOutcome::Reject(ParsedUpstreamReject {
                retryable: false,
                code: "upstream_invalid_response".to_string(),
                detail,
            });
        }
    };
    let ok_flag = match parse_optional_bool_field(
        body,
        "ok",
        "upstream ok must be boolean when present",
    ) {
        Ok(value) => value,
        Err(detail) => {
            return UpstreamOutcome::Reject(ParsedUpstreamReject {
                retryable: false,
                code: "upstream_invalid_response".to_string(),
                detail,
            });
        }
    };
    let accepted_flag = match parse_optional_bool_field(
        body,
        "accepted",
        "upstream accepted must be boolean when present",
    ) {
        Ok(value) => value,
        Err(detail) => {
            return UpstreamOutcome::Reject(ParsedUpstreamReject {
                retryable: false,
                code: "upstream_invalid_response".to_string(),
                detail,
            });
        }
    };
    let is_reject = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    ) || ok_flag == Some(false)
        || accepted_flag == Some(false);
    if matches!(status.as_str(), "ok" | "accepted" | "success")
        && (ok_flag == Some(false) || accepted_flag == Some(false))
    {
        return UpstreamOutcome::Reject(ParsedUpstreamReject {
            retryable: false,
            code: "upstream_invalid_response".to_string(),
            detail: "upstream status=ok conflicts with reject flags".to_string(),
        });
    }
    if matches!(status.as_str(), "reject" | "rejected" | "error" | "failed" | "failure")
        && (ok_flag == Some(true) || accepted_flag == Some(true))
    {
        return UpstreamOutcome::Reject(ParsedUpstreamReject {
            retryable: false,
            code: "upstream_invalid_response".to_string(),
            detail: "upstream status=reject conflicts with success flags".to_string(),
        });
    }
    if is_reject {
        let retryable = match parse_optional_bool_field(
            body,
            "retryable",
            "upstream reject retryable must be boolean when present",
        ) {
            Ok(Some(value)) => value,
            Ok(None) => false,
            Err(detail) => {
                return UpstreamOutcome::Reject(ParsedUpstreamReject {
                    retryable: false,
                    code: "upstream_invalid_response".to_string(),
                    detail,
                });
            }
        };
        let code = match parse_optional_non_empty_string_field(body, "code") {
            Ok(Some(value)) => value,
            Ok(None) => default_reject_code.to_string(),
            Err(detail) => {
                return UpstreamOutcome::Reject(ParsedUpstreamReject {
                    retryable: false,
                    code: "upstream_invalid_response".to_string(),
                    detail,
                });
            }
        };
        let detail = match parse_optional_non_empty_string_field(body, "detail") {
            Ok(Some(value)) => value,
            Ok(None) => "upstream rejected request".to_string(),
            Err(detail) => {
                return UpstreamOutcome::Reject(ParsedUpstreamReject {
                    retryable: false,
                    code: "upstream_invalid_response".to_string(),
                    detail,
                });
            }
        };
        return UpstreamOutcome::Reject(ParsedUpstreamReject {
            retryable,
            code,
            detail,
        });
    }

    let is_known_success_status = matches!(status.as_str(), "ok" | "accepted" | "success");
    let is_known_status = is_known_success_status
        || matches!(
            status.as_str(),
            "reject" | "rejected" | "error" | "failed" | "failure"
        );

    if !status.is_empty() && !is_known_status {
        return UpstreamOutcome::Reject(ParsedUpstreamReject {
            retryable: false,
            code: "upstream_invalid_status".to_string(),
            detail: format!("unknown upstream status={}", status),
        });
    }

    let success = accepted_flag.or(ok_flag).unwrap_or(is_known_success_status);
    if !success {
        return UpstreamOutcome::Reject(ParsedUpstreamReject {
            retryable: false,
            code: "upstream_invalid_response".to_string(),
            detail: "upstream did not explicitly confirm success".to_string(),
        });
    }

    UpstreamOutcome::Success
}

fn parse_optional_non_empty_string_field(
    payload: &Value,
    field_name: &str,
) -> Result<Option<String>, String> {
    let Some(field_value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(format!(
            "upstream reject {} must be non-empty string when present",
            field_name
        ));
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(format!(
            "upstream reject {} must be non-empty string when present",
            field_name
        ));
    }
    Ok(Some(normalized.to_string()))
}

fn parse_optional_bool_field(
    payload: &Value,
    field_name: &str,
    invalid_detail: &str,
) -> Result<Option<bool>, String> {
    let Some(field_value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Some(normalized) = field_value.as_bool() else {
        return Err(invalid_detail.to_string());
    };
    Ok(Some(normalized))
}

fn parse_optional_status_field(payload: &Value) -> Result<Option<String>, String> {
    let Some(field_value) = payload.get("status") else {
        return Ok(None);
    };
    let Some(raw_status) = field_value.as_str() else {
        return Err("upstream status must be non-empty string when present".to_string());
    };
    let normalized = raw_status.trim();
    if normalized.is_empty() {
        return Err("upstream status must be non-empty string when present".to_string());
    }
    Ok(Some(normalized.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::{parse_upstream_outcome, UpstreamOutcome};
    use serde_json::json;

    #[test]
    fn upstream_outcome_rejects_unknown_status() {
        let payload = json!({
            "status": "pending",
            "ok": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => assert_eq!(reject.code, "upstream_invalid_status"),
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_explicit_reject() {
        let payload = json!({
            "status": "reject",
            "retryable": true,
            "code": "busy",
            "detail": "backpressure"
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(reject.retryable);
                assert_eq!(reject.code, "busy");
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_non_string_reject_code_when_present() {
        let payload = json!({
            "status": "reject",
            "ok": false,
            "code": 123,
            "detail": "busy"
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream reject code must be non-empty string when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_null_reject_detail_when_present() {
        let payload = json!({
            "status": "reject",
            "ok": false,
            "code": "busy",
            "detail": null
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream reject detail must be non-empty string when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_non_bool_retryable_when_present() {
        let payload = json!({
            "status": "reject",
            "ok": false,
            "retryable": "true",
            "code": "busy",
            "detail": "backpressure"
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream reject retryable must be boolean when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_null_retryable_when_present() {
        let payload = json!({
            "status": "reject",
            "ok": false,
            "retryable": null,
            "code": "busy",
            "detail": "backpressure"
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream reject retryable must be boolean when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_non_string_status_when_present() {
        let payload = json!({
            "status": 123,
            "ok": true,
            "accepted": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream status must be non-empty string when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_empty_status_when_present() {
        let payload = json!({
            "status": " ",
            "ok": true,
            "accepted": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream status must be non-empty string when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_non_bool_ok_when_present() {
        let payload = json!({
            "status": "ok",
            "ok": "true",
            "accepted": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream ok must be boolean when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_null_accepted_when_present() {
        let payload = json!({
            "status": "ok",
            "ok": true,
            "accepted": null
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(
                    reject
                        .detail
                        .contains("upstream accepted must be boolean when present")
                );
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_conflicting_success_status_with_reject_flags() {
        let payload = json!({
            "status": "ok",
            "ok": false,
            "accepted": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(reject
                    .detail
                    .contains("upstream status=ok conflicts with reject flags"));
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn upstream_outcome_rejects_conflicting_reject_status_with_success_flags() {
        let payload = json!({
            "status": "reject",
            "ok": true,
            "accepted": false
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(reject
                    .detail
                    .contains("upstream status=reject conflicts with success flags"));
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }
}
