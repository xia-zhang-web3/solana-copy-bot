use axum::{
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{auth_verifier::AuthVerifier, reject_mapping::reject_to_json, Reject};

pub(crate) async fn verify_auth_or_reject(
    auth: &AuthVerifier,
    headers: &HeaderMap,
    raw_body: &[u8],
    contract_version: &str,
) -> Option<(StatusCode, Json<Value>)> {
    if let Err(reject) = auth.verify(headers, raw_body).await {
        return Some((
            StatusCode::OK,
            Json(reject_to_json(&reject, None, contract_version)),
        ));
    }
    None
}

pub(crate) fn parse_json_or_reject<T: DeserializeOwned>(
    raw_body: &[u8],
    contract_version: &str,
) -> Result<T, (StatusCode, Json<Value>)> {
    serde_json::from_slice(raw_body).map_err(|error| {
        (
            StatusCode::OK,
            Json(reject_to_json(
                &Reject::terminal(
                    "invalid_json",
                    format!("request body is not valid JSON: {}", error),
                ),
                None,
                contract_version,
            )),
        )
    })
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, StatusCode};
    use serde_json::{json, Value};

    use super::{parse_json_or_reject, verify_auth_or_reject};
    use crate::auth_verifier::AuthVerifier;

    #[test]
    fn request_ingress_parse_json_or_reject_returns_invalid_json_payload() {
        let result: Result<Value, _> = parse_json_or_reject(b"{", "v1");
        let response = result.expect_err("invalid json must reject");
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(
            response.1.0.get("code").and_then(Value::as_str),
            Some("invalid_json")
        );
    }

    #[tokio::test]
    async fn request_ingress_verify_auth_or_reject_returns_auth_reject_payload() {
        let auth = AuthVerifier::new(Some("expected-token".to_string()), None, None, 30);
        let response = verify_auth_or_reject(&auth, &HeaderMap::new(), b"{}", "v1")
            .await
            .expect("missing auth must reject");
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(
            response.1.0.get("code").and_then(Value::as_str),
            Some("auth_missing")
        );
    }

    #[tokio::test]
    async fn request_ingress_verify_auth_or_reject_returns_none_on_valid_auth() {
        let auth = AuthVerifier::new(Some("expected-token".to_string()), None, None, 30);
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer expected-token".parse().expect("valid header"),
        );
        let response = verify_auth_or_reject(&auth, &headers, json!({"ok": true}).to_string().as_bytes(), "v1").await;
        assert!(response.is_none());
    }
}
