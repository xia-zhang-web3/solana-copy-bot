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
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let ok_flag = body.get("ok").and_then(Value::as_bool);
    let accepted_flag = body.get("accepted").and_then(Value::as_bool);
    let retryable = body
        .get("retryable")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let is_reject = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    ) || ok_flag == Some(false)
        || accepted_flag == Some(false);
    if is_reject {
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
}
