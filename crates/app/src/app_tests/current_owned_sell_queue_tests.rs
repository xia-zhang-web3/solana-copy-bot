use super::{
    buy_retry_queue_fixture::*, buy_retry_queue_http_fixture::QueueRpc,
    buy_retry_safety_fixture::reopen,
};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::{json, Value};

#[tokio::test]
async fn owned_sell_progresses_after_restart_with_each_pending_buy_state() -> Result<()> {
    for state in ["unknown", "submitted", "accounting_pending"] {
        pending_consumer(state).await?;
    }
    Ok(())
}

pub(super) async fn pending_consumer(state: &str) -> Result<Value> {
    let mut f = queue_fixture("owned-sell-current-pending", false).await?;
    f.config.canary_batch_limit = 1;
    f.config.canary_entry_submit_enabled = true;
    let buy = buy_order(&f)?;
    let pending = if state == "unknown" {
        let id = super::receipt_reconciliation_fixture::add_order(
            &f.store,
            "unknown-pending-buy",
            "buy",
            PENDING_TOKEN,
            f.now - chrono::Duration::seconds(20),
            false,
        )?;
        f.store
            .mark_execution_canary_submitted_unknown(&id, f.now, "synthetic_unknown_submit")?;
        id
    } else {
        add_pending(&f, true)?
    };
    let sell = add_sell(&f, false)?;
    let mut rpc = QueueRpc::new(&mut f, state == "accounting_pending").await?;
    *rpc.ordinary_pending.lock().unwrap() = state == "submitted";
    let mut visits = Vec::new();
    for n in 0..4 {
        reopen(&mut f)?;
        let out = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(8 + n),
            super::ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n)),
        )
        .await?;
        assert_eq!(out.source_sell_refusals.count(), 0, "{out:?}");
        let valid = out.state_machine_existing - out.source_sell_refusals.count();
        assert!(valid <= 1, "{out:?}");
        visits.push(valid);
        assert_eq!(buy_order(&f)?, buy);
    }
    confirmed(&f, &sell)?;
    let held = f.store.load_execution_canary_order(&pending)?.unwrap();
    assert_eq!(
        held.status,
        if state == "accounting_pending" {
            EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
        } else {
            EXECUTION_STATUS_CANARY_SUBMITTED
        }
    );
    assert_eq!(held.attempt, 1);
    assert_eq!(held.tx_signature.is_none(), state == "unknown");
    let trace = rpc.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|r| *r == "sendTransaction:sell")
            .count(),
        1
    );
    assert!(trace.iter().any(|r| r == "simulateTransaction:sell"));
    assert!(!trace
        .iter()
        .any(|r| r == "buy-quote" || r == "sendTransaction:buy"));
    assert_eq!(f.store.execution_canary_open_position_count()?, 0);
    let receipt = f.store.load_execution_canary_receipt_facts(&sell)?.unwrap();
    assert_eq!(receipt.token_delta.unwrap().raw, -100);
    let dispatch = f.store.load_execution_canary_dispatch(&sell)?.unwrap();
    rpc.finish().await?;
    Ok(
        json!({"pending_kind":state,"pending_order":held.order_id,"pending_signature":held.tx_signature,
        "pending_status":held.status,"selected_raw":100,"remaining_raw":0,"sell_order":sell,
        "sell_signature":dispatch.tx_signature,"valid_handlers_per_tick":visits,"trace":trace,
        "boundary":"actual runner after database reopen; synthetic signing and loopback receipt"}),
    )
}
