use super::*;
use crate::execution_submit_adapter::ExecutionSubmitTransport;

#[test]
fn no_send_submit_transport_preserves_idempotency_and_timeout() -> Result<()> {
    let now = Utc::now();
    let intent = submit_transport_test_intent();

    let attempt: crate::execution_submit_adapter::ExecutionSubmitTransportAttempt =
        crate::execution_submit_adapter::build_submit_transport_attempt(&intent, 2_500, now)?;
    let outcome =
        crate::execution_submit_adapter::NoSendExecutionSubmitTransport.submit(&attempt)?;

    assert_eq!(attempt.idempotency_key, intent.idempotency_key);
    assert_eq!(attempt.submit_route, "rpc_dry_run");
    assert_eq!(attempt.timeout_ms, 2_500);
    assert_eq!(attempt.requested_at, now);
    assert_eq!(outcome.idempotency_key(), attempt.idempotency_key);
    assert_eq!(outcome.reason_label(), "submit_transport_no_send");
    assert!(matches!(
        outcome,
        crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::NotSent { .. }
    ));
    Ok(())
}

#[test]
fn no_send_submit_transport_rejects_zero_timeout() {
    let error = crate::execution_submit_adapter::build_submit_transport_attempt(
        &submit_transport_test_intent(),
        0,
        Utc::now(),
    )
    .expect_err("zero timeout must be rejected");

    assert!(error.to_string().contains("timeout must be positive"));
}

#[test]
fn no_send_submit_transport_rejects_empty_route() {
    let mut intent = submit_transport_test_intent();
    intent.submit_route.clear();

    let error =
        crate::execution_submit_adapter::build_submit_transport_attempt(&intent, 2_500, Utc::now())
            .expect_err("empty route must be rejected");

    assert!(error.to_string().contains("route must be non-empty"));
}

fn submit_transport_test_intent() -> crate::execution_submit_adapter::ExecutionSubmitIntent {
    crate::execution_submit_adapter::ExecutionSubmitIntent {
        idempotency_key: "copybot:submit:test-client-order:attempt:1".to_string(),
        submit_route: "rpc_dry_run".to_string(),
        signed_transaction_base64: "AQIDBA==".to_string(),
        tx_signature_hint: Some("test_tx_signature_hint".to_string()),
    }
}
