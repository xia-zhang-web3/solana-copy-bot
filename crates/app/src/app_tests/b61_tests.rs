use super::{
    b61_fixture::Fixture,
    b61_receipt_fixture::*,
    source_sell_event_capture::capture,
    source_sell_handoff_fixture::{event_reason, recover},
};
use crate::source_sell_staging::StageNotice;
use anyhow::{Context, Result};
use copybot_core_types::{SwapEvent, TokenQuantity};
use copybot_storage_core::{ExecutionSourceSellReject as Reject, ReceiptFeeCoverage};

// The only delivery of B: real ingress -> actual writer ACK with A still held.
// Return only after checked A completion, but before B ever acquires a worker.
async fn ack_before_worker(f: &mut Fixture) -> Result<(SwapEvent, String)> {
    assert!(f.f.store.list_active_follow_wallets()?.is_empty());
    assert!(f.f.follow.active.is_empty() && f.f.lots.is_empty());
    let p = f.position()?;
    let a = f.sell("b61-busy-a", [67; 32]);
    let b = f.sell("b61-sell-b", LEADER);
    let release = f.f.pause_worker();
    f.f.send(&a, true).await?;
    let (result, events) = capture(f.f.send(&b, true)).await;
    release.send(())?;
    let completion = f.f.stage_completion().await?;
    result?;
    assert_eq!(completion.signature, a.signature);
    assert_eq!(
        completion.notice,
        StageNotice::Rejected(Reject::SourceNotProven)
    );
    event_reason(&events, &b.signature, "worker_capacity");
    assert!(f.f.scheduler.source_sells.is_empty());
    let h =
        f.f.store
            .load_source_sell_handoff(&b.signature)?
            .context("durable ACK handoff")?;
    assert_eq!(h.original_position_id.as_deref(), Some(p.as_str()));
    assert_eq!(h.disposition, "pending");
    assert_eq!(h.event.ts_utc, b.ts_utc);
    assert_eq!(h.event.wallet, b.wallet);
    assert_eq!(
        f.f.conn()?.query_row(
            "SELECT count(*) FROM observed_swaps WHERE signature=?1",
            [&b.signature],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    assert!(f.f.staged(&b.signature)?.is_none());
    assert!(f
        .f
        .store
        .load_copy_signal_by_signal_id(&f.signal_id(&b))?
        .is_none());
    assert!(f
        .f
        .store
        .load_execution_canary_order_by_signal(&f.signal_id(&b))?
        .is_none());
    Ok((b, p))
}

#[tokio::test]
async fn b61_restart_reaches_receipt_and_accounts_full_sell_exactly_once() -> Result<()> {
    run_case(true).await
}
async fn positive(f: &mut Fixture) -> Result<()> {
    let (b, p) = ack_before_worker(f).await?;
    let before =
        f.f.store
            .load_execution_canary_open_position(&key(MINT))?
            .unwrap();
    assert_eq!(before.qty_exact, Some(TokenQuantity::new(RAW, 3)));
    let before_exact: (String, i64, i64, String) = f.f.conn()?.query_row(
        "SELECT qty_raw,cost_lamports,pnl_lamports,state FROM positions WHERE position_id=?1",
        [&p],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    assert_eq!(before_exact, (RAW.to_string(), 1000, 0, "open".into()));
    // Controlled pre-worker restart, not a process-kill/power-loss claim.
    f.f.reopen_without_delivery_memory().await?;
    assert!(f.f.recent.is_empty() && f.f.scheduler.source_sells.is_empty());
    // Sensitivity control: ticks alone cannot consume an unstaged durable handoff.
    let without = f.tick().await?;
    assert_eq!(without.source_sell_production.inserted, 0);
    assert!(f.f.staged(&b.signature)?.is_none());
    assert_eq!(f.rpc.count("sendTransaction"), 0);
    assert!(f
        .f
        .store
        .load_execution_canary_order_by_signal(&f.signal_id(&b))?
        .is_none());
    let completed = recover(&mut f.f, &b.signature).await?;
    assert!(completed
        .iter()
        .any(|c| c.signature == b.signature && c.notice == StageNotice::Staged));
    let stage = f.f.staged(&b.signature)?.unwrap();
    assert_eq!(stage.position_id, p);
    assert_eq!(stage.event.ts_utc, b.ts_utc);
    assert_eq!(stage.buy_witness.source_wallet, b.wallet);
    assert!(f
        .f
        .store
        .load_copy_signal_by_signal_id(&f.signal_id(&b))?
        .is_none());
    let submitted = f.tick().await?;
    eprintln!("B61_SUBMIT {submitted:?}");
    assert_eq!(
        submitted.source_sell_production.inserted, 1,
        "{submitted:?}"
    );
    assert_eq!(submitted.quote_close_inserted, 1, "{submitted:?}");
    let signal =
        f.f.store
            .load_copy_signal_by_signal_id(&f.signal_id(&b))?
            .unwrap();
    assert_eq!(signal.ts, b.ts_utc);
    assert_eq!(signal.wallet_id, b.wallet);
    let order =
        f.f.store
            .load_execution_canary_order_by_signal(&signal.signal_id)?
            .context("SELL order")?;
    assert_eq!(
        f.rpc.count("quote_sell"),
        2,
        "stored quote plus mandatory wallet-balance refresh"
    );
    assert_eq!(f.rpc.count("simulateTransaction"), 1);
    assert_eq!(f.rpc.count("sendTransaction"), 1);
    let signature = f.rpc.state.lock().unwrap().sent.as_ref().unwrap().0.clone();
    assert_eq!(order.tx_signature.as_deref(), Some(signature.as_str()));
    let dispatch =
        f.f.store
            .load_execution_canary_dispatch(&order.order_id)?
            .unwrap();
    assert_eq!(dispatch.tx_signature, signature);
    assert_eq!(dispatch.signal_id, signal.signal_id);
    assert!(f
        .f
        .store
        .load_execution_canary_cash_settlement(&order.order_id)?
        .is_none());
    // A confirmed status with a missing full receipt must not become cash completion.
    let pending = f.tick().await?;
    assert!(f.rpc.count("getTransaction") > 0, "{pending:?}");
    assert!(f
        .f
        .store
        .load_execution_canary_cash_settlement(&order.order_id)?
        .is_none());
    assert_eq!(
        f.f.store.load_execution_canary_open_position(&key(MINT))?,
        Some(before.clone())
    );
    assert_eq!(f.rpc.count("sendTransaction"), 1);
    f.rpc.state.lock().unwrap().receipt_enabled = true;
    let settled = f.tick().await?;
    let cash =
        f.f.store
            .load_execution_canary_cash_settlement(&order.order_id)?
            .with_context(|| format!("full receipt must settle: {settled:?}"))?;
    assert_eq!(cash.position_id, p);
    assert_eq!(cash.sold_quantity, TokenQuantity::new(RAW, 3));
    assert_eq!(cash.remaining_quantity, TokenQuantity::new(0, 3));
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), i128::from(CASH));
    assert_eq!(cash.allocated_entry_basis.as_u64(), 1000);
    assert_eq!(cash.remaining_entry_basis.as_u64(), 0);
    assert_eq!(cash.cash_result_delta.as_i128(), i128::from(CASH) - 1000);
    assert_eq!(cash.accumulated_cash_result, cash.cash_result_delta);
    let facts =
        f.f.store
            .load_execution_canary_receipt_facts(&order.order_id)?
            .unwrap();
    assert_eq!(facts.tx_signature, signature);
    assert_eq!(facts.wallet_pubkey, f.config.canary_wallet_pubkey);
    assert_eq!(facts.token, key(MINT));
    assert_eq!(facts.transaction_fee.unwrap().as_u64(), FEE);
    assert_eq!(facts.fee_coverage, ReceiptFeeCoverage::Known);
    assert_eq!(facts.token_delta.unwrap().raw, -i128::from(RAW));
    assert_eq!(facts.token_delta.unwrap().decimals, 3);
    assert_eq!(facts.wallet_native_delta.as_i128(), i128::from(CASH));
    assert!(f
        .f
        .store
        .load_execution_canary_open_position(&key(MINT))?
        .is_none());
    assert_eq!(
        f.f.store
            .load_execution_canary_order(&order.order_id)?
            .unwrap()
            .status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert!(!f.f.store.execution_canary_accounting_pending()?);
    let conn = f.f.conn()?;
    let exact: (String, i64, i64, String) = conn.query_row(
        "SELECT qty_raw,cost_lamports,pnl_lamports,state FROM positions WHERE position_id=?1",
        [&p],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    assert_eq!(exact, ("0".into(), 0, CASH as i64 - 1000, "closed".into()));
    for (table, column, id) in [
        (
            "execution_source_sell_intents",
            "event_signature",
            &b.signature,
        ),
        (
            "execution_source_sell_promotions",
            "intent_id",
            &stage.intent_id,
        ),
        ("orders", "signal_id", &signal.signal_id),
        ("fills", "order_id", &order.order_id),
        ("execution_canary_dispatch", "order_id", &order.order_id),
    ] {
        assert_eq!(
            conn.query_row(
                &format!("SELECT count(*) FROM {table} WHERE {column}=?1"),
                [id],
                |r| r.get::<_, i64>(0)
            )?,
            1,
            "{table}"
        );
    }
    let exact_after = f.economics()?;
    let rpc_after = (
        f.rpc.count("quote_sell"),
        f.rpc.count("simulateTransaction"),
        f.rpc.count("sendTransaction"),
        f.rpc.count("getTransaction"),
    );
    f.f.reopen_without_delivery_memory().await?;
    for _ in 0..3 {
        f.f.scheduler
            .source_sells
            .recover(&f.f.store, &f.f.path.to_string_lossy())?;
        f.tick().await?;
    }
    assert_eq!(
        f.economics()?,
        exact_after,
        "all money/receipt/dispatch/intent rows unchanged after reopen and ticks"
    );
    assert_eq!(
        f.f.store
            .load_execution_canary_cash_settlement(&order.order_id)?,
        Some(cash.clone())
    );
    assert_eq!(
        (
            f.rpc.count("quote_sell"),
            f.rpc.count("simulateTransaction"),
            f.rpc.count("sendTransaction"),
            f.rpc.count("getTransaction")
        ),
        rpc_after
    );
    assert_eq!(
        format!("{:?}", f.f.staged(&b.signature)?.unwrap()),
        format!("{stage:?}")
    );
    eprintln!(
        "B61_EXACT {}",
        serde_json::json!({"before_raw":RAW,"before_basis":1000,"before_cash_result":0,
        "sold_raw":RAW,"remaining_raw":0,"allocated_basis":1000,"remaining_basis":0,"gross":GROSS,"fee":FEE,
        "native_cash":CASH,"cash_result":CASH-1000,"send_count":1,"settlement_count":1,"snapshot_replay_equal":true})
    );
    Ok(())
}

#[tokio::test]
async fn b61_restart_refuses_old_p_sell_after_q_replaces_position() -> Result<()> {
    run_case(false).await
}
async fn negative(f: &mut Fixture) -> Result<()> {
    let (b, p) = ack_before_worker(f).await?;
    f.f.reopen_without_delivery_memory().await?;
    f.replace_position(f.f.now + chrono::Duration::seconds(2))?;
    let q = f.position()?;
    assert_ne!(p, q);
    let before = f.economics()?;
    let (result, events) = capture(recover(&mut f.f, &b.signature)).await;
    let completed = result?;
    assert!(completed.iter().any(|c| c.signature == b.signature
        && c.notice == StageNotice::Rejected(Reject::GenerationMismatch)));
    event_reason(&events, &b.signature, "generation_mismatch");
    let h = f.f.store.load_source_sell_handoff(&b.signature)?.unwrap();
    assert_eq!(h.original_position_id.as_deref(), Some(p.as_str()));
    assert_eq!(h.disposition, "refused");
    for _ in 0..2 {
        eprintln!("B61_NEGATIVE_TICK {:?}", f.tick().await?);
    }
    assert!(f.f.staged(&b.signature)?.is_none());
    assert!(f
        .f
        .store
        .load_copy_signal_by_signal_id(&f.signal_id(&b))?
        .is_none());
    assert!(f
        .f
        .store
        .load_execution_canary_order_by_signal(&f.signal_id(&b))?
        .is_none());
    for method in [
        "quote_sell",
        "swap",
        "simulateTransaction",
        "sendTransaction",
    ] {
        assert_eq!(f.rpc.count(method), 0, "{method}");
    }
    assert_eq!(f.economics()?, before);
    assert_eq!(f.position()?, q);
    Ok(())
}

// Even an assertion panic must reach the checked shutdown of this fixture's tasks.
async fn run_case(is_positive: bool) -> Result<()> {
    use std::{
        future::{poll_fn, Future},
        panic::{catch_unwind, AssertUnwindSafe},
        pin::Pin,
        task::Poll,
    };
    let mut f = Fixture::new().await?;
    let result = {
        let mut body: Pin<Box<dyn Future<Output = Result<()>> + '_>> = if is_positive {
            Box::pin(positive(&mut f))
        } else {
            Box::pin(negative(&mut f))
        };
        poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
                Ok(out) => out,
                Err(_) => Poll::Ready(Err(anyhow::anyhow!(
                    "B61 assertion panicked; checked cleanup follows"
                ))),
            },
        )
        .await
    };
    if result.is_err() {
        eprintln!("B61_CALLS {:?}", f.rpc.state.lock().unwrap().calls);
    }
    let cleanup = f.finish().await;
    if let Err(error) = &cleanup {
        eprintln!("B61_CLEANUP {error:#}");
    }
    result?;
    cleanup
}
