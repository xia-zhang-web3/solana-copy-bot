use super::receipt_reconciliation_fixture::*;
use anyhow::Result;

#[tokio::test]
async fn actual_buy_then_signed_sell_completes_once_on_receipt_boundary() -> Result<()> {
    for cash in [-300_000_000, 0, 1_200_000_000] {
        let mut f = Fixture::new("buy")?;
        let rpc = Rpc::new(receipt("buy", -900_000_000)).await?;
        rpc.context(format!(
            "actual_buy_then_signed_sell_completes_once_on_receipt_boundary cash={cash:?}"
        ));
        assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
        f.order_id = add_distinct_order(&f.store, "signed-sell", "sell", TOKEN, f.now, true)?;
        set_order_receipt(&f, &rpc, receipt("sell", cash))?;
        let out = f.reconcile(&rpc, 2).await?;
        assert_eq!(
            out.confirmation_confirmed, 1,
            "supported signed SELL {cash}"
        );
        assert_eq!(out.confirmation_pending, 0);
        let cash_out = out
            .cash_settlement
            .as_ref()
            .expect("typed signed outcome reaches caller");
        assert_eq!(
            cash_out.wallet_native_cash_delta.as_i128(),
            i128::from(cash)
        );
        assert!(cash_out.swap_price.is_none());
        assert_eq!(f.fills()?, 1);
        let state: (String, String, i64, i64) = f.conn()?.query_row(
            "SELECT state,qty_raw,cost_lamports,pnl_lamports FROM positions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        assert_eq!(state, ("closed".into(), "0".into(), 0, cash - 900_000_000));
        f.reopen()?;
        assert_eq!(f.reconcile(&rpc, 3).await?.confirmation_confirmed, 1);
        assert_eq!(f.fills()?, 1);
        assert!(!f.store.execution_canary_accounting_pending()?);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_signed_partials_and_actual_buy_merge_preserve_accumulated_result() -> Result<()> {
    use serde_json::json;
    for cash in [-3, 0, 5] {
        let mut f = Fixture::new("buy")?;
        let rpc = Rpc::new(receipt("buy", -23)).await?;
        rpc.context(format!("receipt_signed_partials_and_actual_buy_merge_preserve_accumulated_result cash={cash:?}"));
        assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
        assert_eq!(
            f.conn()?
                .query_row("SELECT pnl_lamports FROM positions", [], |r| r
                    .get::<_, i64>(0))?,
            0
        );
        let mut allocated = 0;
        let mut total_cash = 0;
        for (i, sold) in [2000, 4999, 1].into_iter().enumerate() {
            f.order_id = add_distinct_order(
                &f.store,
                &format!("partial-{i}"),
                "sell",
                TOKEN,
                f.now,
                true,
            )?;
            let mut value = receipt("sell", cash);
            value["result"]["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] =
                json!(sold.to_string());
            value["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("0");
            set_order_receipt(&f, &rpc, value)?;
            assert_eq!(
                f.reconcile(&rpc, 2 + i as i64)
                    .await?
                    .confirmation_confirmed,
                1
            );
            let s = f
                .store
                .load_execution_canary_cash_settlement(&f.order_id)?
                .unwrap();
            allocated += s.allocated_entry_basis.as_u64();
            total_cash += cash;
            assert_eq!(s.remaining_entry_basis.as_u64() + allocated, 23);
            assert_eq!(
                s.accumulated_cash_result.as_i128(),
                i128::from(total_cash) - i128::from(allocated)
            );
            if i == 1 {
                assert_eq!(s.remaining_quantity.raw(), 1);
            }
            f.reopen()?;
        }
        assert_eq!(allocated, 23);
        rpc.finish().await?;
    }
    // Another actual BUY merges exact inventory but must retain previous realized cash.
    let mut f = Fixture::new("buy")?;
    let rpc = Rpc::new(receipt("buy", -900_000_000)).await?;
    rpc.context(format!(
        "receipt_signed_partials_and_actual_buy_merge_preserve_accumulated_result"
    ));
    f.reconcile(&rpc, 1).await?;
    f.order_id = add_distinct_order(&f.store, "partial-before-merge", "sell", TOKEN, f.now, true)?;
    let mut value = receipt("sell", 0);
    value["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("7000");
    set_order_receipt(&f, &rpc, value)?;
    f.reconcile(&rpc, 2).await?;
    let cash: f64 = f
        .conn()?
        .query_row("SELECT pnl_lamports FROM positions", [], |r| r.get(0))?;
    assert!(cash < 0.0);
    f.order_id = add_distinct_order(&f.store, "merge-buy", "buy", TOKEN, f.now, true)?;
    set_order_receipt(&f, &rpc, receipt("buy", -11))?;
    assert_eq!(f.reconcile(&rpc, 3).await?.confirmation_confirmed, 1);
    assert_eq!(
        f.conn()?
            .query_row("SELECT pnl_lamports FROM positions", [], |r| r
                .get::<_, f64>(0))?,
        cash
    );
    f.reopen()?;
    f.reconcile(&rpc, 4).await?;
    assert_eq!(
        f.conn()?
            .query_row("SELECT pnl_lamports FROM positions", [], |r| r
                .get::<_, f64>(0))?,
        cash
    );
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_signed_saved_known_then_fresh_partial_never_borrows_operands() -> Result<()> {
    use super::receipt_cash_facts_fixture::{
        assert_pending, facts, money_snapshot, stop_accounting,
    };
    use serde_json::json;
    for side in ["buy", "sell"] {
        let mut f = Fixture::new(side)?;
        let full = receipt(side, if side == "buy" { -23 } else { 0 });
        let rpc = Rpc::new(full.clone()).await?;
        rpc.context(format!(
            "receipt_signed_saved_known_then_fresh_partial_never_borrows_operands side={side:?}"
        ));
        stop_accounting(&f)?;
        let before = money_snapshot(&f)?;
        let out = f.reconcile(&rpc, 1).await;
        if side == "buy" {
            assert!(out.is_err(), "legacy BUY DB failure still propagates");
        } else {
            let out = out?;
            assert_eq!(
                (out.confirmation_pending, out.confirmation_confirmed),
                (1, 0)
            );
            assert_eq!(
                out.error.as_deref(),
                Some("receipt_accounting_write_failed")
            );
        }
        assert!(facts(&f)?.token_delta.is_some());
        assert_pending(&f)?;
        f.conn()?.execute_batch("DROP TRIGGER stop_accounting")?;
        f.reopen()?;
        let mut partial = full.clone();
        partial["result"]["meta"]["postTokenBalances"] = json!([]);
        set_order_receipt(&f, &rpc, partial)?;
        assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_pending, 1);
        assert_pending(&f)?;
        assert_eq!(money_snapshot(&f)?, before);
        assert!(
            facts(&f)?.token_delta.is_some(),
            "durable facts retain earlier known quantity"
        );
        let mut conflicting = full.clone();
        conflicting["result"]["meta"]["postBalances"][0] = json!(123);
        set_order_receipt(&f, &rpc, conflicting)?;
        assert_eq!(f.reconcile(&rpc, 3).await?.confirmation_pending, 1);
        assert_pending(&f)?;
        assert_eq!(money_snapshot(&f)?, before);
        set_order_receipt(&f, &rpc, full)?;
        assert_eq!(f.reconcile(&rpc, 4).await?.confirmation_confirmed, 1);
        assert_eq!(f.fills()?, 1);
        f.reopen()?;
        f.reconcile(&rpc, 5).await?;
        assert_eq!(f.fills()?, 1);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_signed_historical_null_and_unsupported_inventory_stay_blocked() -> Result<()> {
    use super::receipt_cash_facts_fixture::{assert_pending, money_snapshot};
    for change in [
        "pnl_lamports=NULL",
        "cost_lamports=NULL",
        "qty_decimals=NULL",
        "qty_raw='6000'",
    ] {
        let f = Fixture::new("sell")?;
        f.conn()?
            .execute(&format!("UPDATE positions SET {change}"), [])?;
        let before = money_snapshot(&f)?;
        let rpc = Rpc::new(receipt("sell", 1_200_000_000)).await?;
        rpc.context(format!("receipt_signed_historical_null_and_unsupported_inventory_stay_blocked change={change:?}"));
        assert_eq!(
            f.reconcile(&rpc, 1).await?.confirmation_pending,
            1,
            "{change}"
        );
        assert_pending(&f)?;
        assert_eq!(money_snapshot(&f)?, before);
        assert!(f
            .store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap()
            .reason
            .starts_with("receipt_sell_unsupported:"));
        rpc.finish().await?;
    }
    // A new BUY merged into imported inventory cannot prove an initial zero.
    let mut f = Fixture::new("sell")?;
    f.conn()?
        .execute("UPDATE positions SET pnl_lamports=NULL", [])?;
    f.order_id = add_distinct_order(&f.store, "buy-on-history", "buy", TOKEN, f.now, true)?;
    let rpc = Rpc::new(receipt("buy", -23)).await?;
    rpc.context(format!(
        "receipt_signed_historical_null_and_unsupported_inventory_stay_blocked"
    ));
    set_order_receipt(&f, &rpc, receipt("buy", -23))?;
    f.reconcile(&rpc, 1).await?;
    assert!(f
        .conn()?
        .query_row("SELECT pnl_lamports FROM positions", [], |r| r
            .get::<_, Option<i64>>(0))?
        .is_none());
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_signed_closed_loss_trips_existing_entry_cap() -> Result<()> {
    let mut f = Fixture::new("buy")?;
    let rpc = Rpc::new(receipt("buy", -900_000_000)).await?;
    rpc.context(format!(
        "receipt_signed_closed_loss_trips_existing_entry_cap"
    ));
    f.reconcile(&rpc, 1).await?;
    f.order_id = add_distinct_order(&f.store, "loss-sell", "sell", TOKEN, f.now, true)?;
    set_order_receipt(&f, &rpc, receipt("sell", -300_000_000))?;
    f.reconcile(&rpc, 2).await?;
    let mut cfg = config(&rpc.url);
    cfg.canary_max_daily_loss_sol = 1.0;
    let safety = crate::execution_canary_safety::pre_submit_safety_snapshot(
        &cfg,
        &f.store,
        f.now + chrono::Duration::seconds(3),
    )?;
    assert_eq!(safety.blocked_reason, Some("max_daily_loss"));
    assert!((safety.daily_loss_sol - 1.2).abs() < 1e-12);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_signed_pending_proof_cannot_follow_a_changed_configured_wallet() -> Result<()> {
    use super::receipt_cash_facts_fixture::{assert_pending, money_snapshot};
    let f = Fixture::new("sell")?;
    let rpc = Rpc::new(serde_json::json!({"result":null})).await?;
    rpc.context(format!(
        "receipt_signed_pending_proof_cannot_follow_a_changed_configured_wallet"
    ));
    f.reconcile(&rpc, 1).await?;
    let before = money_snapshot(&f)?;
    set_order_receipt(&f, &rpc, receipt("sell", 0))?;
    let mut cfg = config(&rpc.url);
    cfg.canary_wallet_pubkey = "ChangedWallet".into();
    assert!(
        crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
            &f.store,
            &cfg,
            &f.order_id,
            &reqwest::Client::new(),
            &rpc.url,
            f.now,
            80,
        )
        .await
        .is_err()
    );
    assert_pending(&f)?;
    assert_eq!(money_snapshot(&f)?, before);
    assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_confirmed, 1);
    rpc.finish().await?;
    Ok(())
}

// Each submitted transaction has its own signature. Earlier multi-order fixtures
// reused one signature for BUY and several SELLs, contradicting receipt uniqueness.
fn add_distinct_order(
    store: &copybot_storage_core::SqliteStore,
    name: &str,
    side: &str,
    token: &str,
    now: chrono::DateTime<chrono::Utc>,
    submitted: bool,
) -> Result<String> {
    let id = add_order(store, name, side, token, now, false)?;
    if submitted {
        store.mark_execution_canary_submitted(&id, now, &format!("{SIGNATURE}-{name}"))?;
    }
    Ok(id)
}
fn set_order_receipt(f: &Fixture, rpc: &Rpc, mut value: serde_json::Value) -> Result<()> {
    value["result"]["transaction"]["signatures"][0] = serde_json::json!(f
        .store
        .load_execution_canary_order(&f.order_id)?
        .unwrap()
        .tx_signature
        .unwrap());
    rpc.set(value);
    Ok(())
}
