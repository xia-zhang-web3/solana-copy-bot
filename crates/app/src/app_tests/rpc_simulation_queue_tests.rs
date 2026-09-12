use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use anyhow::Result;

#[tokio::test]
async fn rpc_simulation_malformed_buy_does_not_stop_sell_or_submitted_reconciliation() -> Result<()>
{
    let mut f = queue_fixture("b17-queue-malformed-buy", false).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 3;
    let sell = add_sell(&f, false)?;
    let pending = add_pending(&f, false)?;
    let original = buy_order(&f)?;
    let mut rpc = QueueRpc::with_simulation(&mut f, false, true, false).await?;
    let first = f.sweep().await?;
    assert_eq!(first.existing, 2, "{first:?}");
    assert_eq!(first.entry_gate_blocked, 0);
    assert_eq!(first.safety_blocked, 1);
    assert_eq!(first.skipped_reason, Some("unresolved_buy_dispatch"));
    assert_eq!(buy_order(&f)?, original);
    // Receipt-only work clears the existing BUY hold before this BUY may simulate.
    let second = f.sweep().await?;
    rpc.finish().await?;
    assert_eq!(second.existing, 1, "{second:?}");
    assert_eq!(
        second.entry_gate_blocked, 0,
        "BUY actually reached simulation"
    );
    assert_eq!(
        first.signing_envelope_built + second.signing_envelope_built,
        1
    );
    let trace = rpc.trace();
    assert!(trace.iter().any(|s| s == "simulateTransaction:buy"));
    assert!(trace.iter().any(|s| s == "simulateTransaction:sell"));
    assert_eq!(
        trace
            .iter()
            .filter(|s| *s == "sendTransaction:sell")
            .count(),
        1
    );
    assert!(!trace.iter().any(|s| s == "sendTransaction:buy"));
    let buy = buy_order(&f)?;
    assert_eq!(buy.order_id, original.order_id);
    assert_eq!(buy.attempt, original.attempt);
    assert_eq!(buy.err_code.as_deref(), Some("simulation_failed"));
    assert!(buy
        .simulation_error
        .as_deref()
        .unwrap()
        .contains("result.value.err"));
    assert!(buy.tx_signature.is_none());
    confirmed(&f, &sell)?;
    confirmed(&f, &pending)?;
    reopen(&mut f)?;
    assert_eq!(buy_order(&f)?, buy);
    confirmed(&f, &sell)?;
    confirmed(&f, &pending)?;
    eprintln!("B17 candidate-local: {trace:?}");
    Ok(())
}

#[tokio::test]
async fn rpc_simulation_sell_bypasses_buy_guard_but_never_bypasses_simulation() -> Result<()> {
    let mut f = queue_fixture("b17-malformed-sell", false).await?;
    f.config.canary_entry_submit_enabled = false;
    f.config.canary_batch_limit = 3;
    let sell = add_sell(&f, false)?;
    let pending = add_pending(&f, false)?;
    let original = buy_order(&f)?;
    let mut rpc = QueueRpc::with_simulation(&mut f, false, false, true).await?;
    let mut summaries = Vec::new();
    for _ in 0..3 {
        summaries.push(f.sweep().await);
    }
    rpc.finish().await?;
    for summary in summaries {
        let s = summary?;
        assert_eq!(s.signing_envelope_built, 0);
    }
    assert_eq!(buy_order(&f)?, original);
    let sell = f.store.load_execution_canary_order(&sell)?.unwrap();
    assert_eq!(sell.err_code.as_deref(), Some("simulation_failed"));
    assert!(sell
        .simulation_error
        .as_deref()
        .unwrap()
        .contains("result.value.err"));
    assert!(sell.tx_signature.is_none());
    assert!(rpc.trace().iter().any(|s| s == "simulateTransaction:sell"));
    assert!(!rpc.trace().iter().any(|s| s.starts_with("sendTransaction")));
    confirmed(&f, &pending)?;
    Ok(())
}
