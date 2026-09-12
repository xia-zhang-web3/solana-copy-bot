use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use anyhow::Result;
use serde_json::json;

async fn witness(retry: bool, valid: bool) -> Result<()> {
    let mut f = RuntimeFixture::new(
        &format!("b17-runtime-{retry}-{valid}"),
        20_000_000,
        200,
        10_000_000,
        100,
        retry,
    )
    .await?;
    *f.simulation_result.lock().unwrap() = Some(if valid {
        json!({"context":{"slot":0},"value":{"err":null}})
    } else {
        json!({"context":{"slot":0},"value":{}})
    });
    let (hot, sweep) = if retry {
        (None, Some(f.sweep().await))
    } else {
        (Some(f.hot().await), None)
    };
    f.finish().await?;
    let (envelopes, failures) = if let Some(s) = sweep {
        let s = s?;
        (Some(s.signing_envelope_built), s.failed)
    } else {
        let s = hot.unwrap()?;
        (None, s.state_machine_failed)
    };
    let order = f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .unwrap();
    let metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .unwrap();
    let fee: serde_json::Value =
        serde_json::from_str(metadata.priority_fee_json.as_deref().unwrap())?;
    assert_eq!(
        fee.get("fee_proof").is_some(),
        valid,
        "no new signed envelope proof on malformed simulation"
    );
    eprintln!("B17 retry={retry} valid={valid} envelopes={envelopes:?} failures={failures} order={order:?} calls={:?}", f.calls());
    assert!(f.calls().iter().any(|s| s == "simulateTransaction"));
    if let Some(envelopes) = envelopes {
        assert_eq!(
            envelopes,
            usize::from(valid),
            "retry stops before signing envelope"
        );
    }
    assert_eq!(failures, usize::from(!valid));
    assert_eq!(
        f.calls().iter().filter(|s| *s == "sendTransaction").count(),
        usize::from(valid)
    );
    if valid {
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
        );
        assert_eq!(order.tx_signature.as_deref(), Some("tx-fresh-size"));
    } else {
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
        );
        assert_eq!(order.err_code.as_deref(), Some("simulation_failed"));
        assert!(order
            .simulation_error
            .as_deref()
            .unwrap_or_default()
            .contains("result.value.err"));
        assert!(order.tx_signature.is_none());
        assert!(!f.store.execution_canary_fill_exists(&order.order_id)?);
    }
    Ok(())
}

#[tokio::test]
async fn rpc_simulation_hot_malformed_never_signs_or_sends() -> Result<()> {
    witness(false, false).await
}
#[tokio::test]
async fn rpc_simulation_retry_malformed_never_signs_or_sends() -> Result<()> {
    witness(true, false).await
}
#[tokio::test]
async fn rpc_simulation_hot_valid_reaches_signed_send_and_receipt() -> Result<()> {
    witness(false, true).await
}
#[tokio::test]
async fn rpc_simulation_retry_valid_reaches_signed_send_and_receipt() -> Result<()> {
    witness(true, true).await
}
