use axum::{http::StatusCode, Json};
use serde_json::Value;

use crate::{reject_mapping::reject_to_json, Reject};

pub(crate) fn success_or_reject_to_http(
    result: Result<Value, Reject>,
    client_order_id: Option<&str>,
    contract_version: &str,
) -> (StatusCode, Json<Value>) {
    match result {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(reject) => (
            StatusCode::OK,
            Json(reject_to_json(&reject, client_order_id, contract_version)),
        ),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::success_or_reject_to_http;
    use crate::Reject;

    #[test]
    fn response_envelope_wraps_success_payload_with_http_200() {
        let payload = json!({"status":"ok","accepted":true});
        let response = success_or_reject_to_http(Ok(payload.clone()), None, "v1");
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(response.1.0, payload);
    }

    #[test]
    fn response_envelope_reject_includes_client_order_id_when_provided() {
        let reject = Reject::terminal("code-1", "detail-1");
        let response =
            success_or_reject_to_http(Err(reject), Some("client-1"), "v1");
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(
            response
                .1
                .0
                .get("client_order_id")
                .and_then(|value| value.as_str()),
            Some("client-1")
        );
    }

    #[test]
    fn response_envelope_reject_omits_client_order_id_when_absent() {
        let reject = Reject::terminal("code-1", "detail-1");
        let response = success_or_reject_to_http(Err(reject), None, "v1");
        assert_eq!(response.0, StatusCode::OK);
        assert!(
            response
                .1
                .0
                .get("client_order_id")
                .is_none()
        );
    }
}
