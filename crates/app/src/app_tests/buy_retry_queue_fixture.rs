use super::buy_retry_safety_fixture::fixture;
use super::execution_state_machine_tiny_submit_timeout_route::record_tiny_timeout_build_metadata;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::receipt_reconciliation_fixture::{add_order, SIGNATURE};
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::ExecutionCanaryOrder;
use rusqlite::{params, Connection};
use serde_json::json;

pub(super) const UNKNOWN: &str = "retry_after_unknown_submit_timeout";
pub(super) const NOT_SENT: &str = "retry_after_rpc_submit_not_sent:rpc_send_transaction_error";
pub(super) const SELL_TOKEN: &str = "SellMint";
pub(super) const PENDING_TOKEN: &str = "PendingMint";

pub(super) async fn queue_fixture(name: &str, unknown: bool) -> Result<RuntimeFixture> {
    let mut f = fixture(name).await?;
    f.finish().await?; // replace the BUY-only fixture server with an awaited queue server
    if unknown {
        let order = buy_order(&f)?;
        f.store.mark_execution_canary_submitted_unknown(
            &order.order_id,
            f.now + Duration::seconds(4),
            "submitted_without_signature",
        )?;
        f.store.mark_execution_canary_retry_after_submit_timeout(
            &order.order_id,
            f.now + Duration::seconds(7),
            Duration::seconds(2),
            UNKNOWN,
        )?;
        f.config.max_submit_attempts = 3;
    }
    f.now += Duration::seconds(10);
    Ok(f)
}

pub(super) fn buy_order(f: &RuntimeFixture) -> Result<ExecutionCanaryOrder> {
    Ok(f.store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .unwrap())
}

pub(super) fn add_sell(f: &RuntimeFixture, unknown: bool) -> Result<String> {
    let now = f.now - Duration::seconds(2);
    let id = add_order(&f.store, "b12-sell", "sell", SELL_TOKEN, now, false)?;
    let signal = f.store.load_copy_signal_by_signal_id("b12-sell")?.unwrap();
    record_tiny_timeout_build_metadata(&f.store, &id, &signal, now)?;
    let body = json!({"inputMint":SELL_TOKEN,"outputMint":crate::execution_quote_canary_helpers::SOL_MINT,
        "inAmount":"100","outAmount":"10000000","otherAmountThreshold":"9500000",
        "swapMode":"ExactIn","slippageBps":crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps(&f.config, "sell"),"meta":{"inDecimals":0,"outDecimals":9},
        "routePlan":[{"swapInfo":{"label":"Metis"}}]});
    Connection::open(&f.db_path)?.execute("UPDATE execution_canary_build_plan_metadata SET quote_in_amount_raw='100', quote_out_amount_raw='10000000', quote_response_json=?2, route_plan_json=?3 WHERE order_id=?1", params![id,body.to_string(),body["routePlan"].to_string()])?;
    f.store.record_execution_canary_open_position(
        "b12-owned-sell",
        SELL_TOKEN,
        100.0,
        Some(TokenQuantity::new(100, 0)),
        0.009,
        now,
    )?;
    Connection::open(&f.db_path)?.execute(
        "UPDATE positions SET pnl_lamports=0 WHERE token=?1",
        [SELL_TOKEN],
    )?;
    if unknown {
        f.store
            .mark_execution_canary_submitted_unknown(&id, now, "submitted_without_signature")?;
        f.store.mark_execution_canary_retry_after_submit_timeout(
            &id,
            now + Duration::seconds(3),
            Duration::seconds(2),
            UNKNOWN,
        )?;
    } else {
        f.store.mark_execution_canary_retry_after_submit_not_sent(
            &id,
            now + Duration::seconds(1),
            NOT_SENT,
        )?;
    }
    // The local wallet fixture owns the same 100 raw as the current position.
    let order = f.store.load_execution_canary_order(&id)?.unwrap();
    let mut request = crate::execution_submit_adapter::build_tiny_submit_reconciliation_request(
        &f.store, &f.config, &order,
    )?;
    super::owned_sell_fixture::bind(&f.store, &mut request, 100, 0)?;
    let metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&id)?
        .unwrap();
    let proof = serde_json::to_string(request.metadata.owned_sell_amount.as_ref().unwrap())?;
    f.store
        .record_execution_canary_build_plan_metadata_with_sell_amount(&metadata, Some(&proof))?;
    Ok(id)
}

pub(super) fn add_pending(f: &RuntimeFixture, early: bool) -> Result<String> {
    add_order(
        &f.store,
        "b12-known",
        "buy",
        PENDING_TOKEN,
        f.now + Duration::seconds(if early { -20 } else { 2 }),
        true,
    )
}

pub(super) fn confirmed(f: &RuntimeFixture, id: &str) -> Result<()> {
    let order = f.store.load_execution_canary_order(id)?.unwrap();
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert!(f.store.execution_canary_fill_exists(id)?);
    if order.signal_id == "b12-known" {
        assert_eq!(order.tx_signature.as_deref(), Some(SIGNATURE));
    } else {
        let dispatch = f
            .store
            .load_execution_canary_dispatch(id)?
            .expect("SELL dispatch identity");
        assert_eq!(
            order.tx_signature.as_deref(),
            Some(dispatch.tx_signature.as_str())
        );
    }
    Ok(())
}
