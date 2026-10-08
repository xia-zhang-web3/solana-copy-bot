//! Minimal envelope checks preserve accepted refusal semantics before queuing bytes.
use super::{envelope, response};
use serde_json::json;

#[test]
fn raw_validation_keeps_null_distinct_from_missing_result_and_preserves_failures() {
    for (status, body) in [
        (200, json!({"jsonrpc":"2.0","id":1,"result":null})),
        (200, json!({"jsonrpc":"2.0","id":1})),
        (200, json!({"jsonrpc":"1.0","id":1,"result":[]})),
        (200, json!({"jsonrpc":"2.0","id":2,"result":[]})),
        (
            200,
            json!({"jsonrpc":"2.0","id":1,"result":[],"error":null}),
        ),
        (
            200,
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"private"}}),
        ),
        (200, json!({"broker_error":null})),
        (503, json!({"jsonrpc":"2.0","id":1,"error":{"code":-32004}})),
        (503, json!({"unexpected":"private"})),
    ] {
        let raw = serde_json::to_vec(&body).unwrap();
        let checked = envelope::validate(status, 1, "getBlock", &raw);
        let accepted = response::interpret(status, 1, "getBlock", &raw);
        assert_eq!(checked.is_ok(), accepted.is_ok());
        if let (Err(checked), Err(accepted)) = (checked, accepted) {
            assert_eq!(checked.to_string(), accepted.to_string());
        }
    }
    assert!(envelope::validate(
        200,
        1,
        "getBlock",
        br#"{"jsonrpc":"2.0","id":1,"result":null}"#
    )
    .is_ok());
    assert!(envelope::validate(200, 1, "getBlock", br#"{"jsonrpc":"2.0","id":1}"#).is_err());
}

#[test]
fn raw_validation_reuses_broker_identity_and_retry_cause_without_logging_body() {
    let failure = json!({"broker_error":{"schema":"http_recovery_broker_v1",
        "kind":"failed","method":"getBlock","stage":"upstream_body",
        "reason":"http_protocol","cause_type":"IncompleteRead","reservation_id":7,
        "request_id":1,"slot":10,"http_status":null,"verify_code":null}});
    let raw = serde_json::to_vec(&failure).unwrap();
    let error = envelope::validate(502, 1, "getBlock", &raw).unwrap_err();
    assert!(super::delivery_error::retryable(&error));
    let broker = error
        .downcast_ref::<super::delivery_error::BrokerFailure>()
        .unwrap();
    assert_eq!(broker.reservation, Some(7));
    assert_eq!(broker.slot, Some(10));
    assert!(envelope::validate(502, 2, "getBlock", &raw)
        .unwrap_err()
        .to_string()
        .contains("broker_request_identity"));
}
