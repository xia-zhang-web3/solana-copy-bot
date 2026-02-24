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
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_reject_code)
            .to_string();
        let detail = body
            .get("detail")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("upstream rejected request")
            .to_string();
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
}
