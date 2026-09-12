use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use chrono::Duration;
use copybot_config::ExecutionConfig;
use copybot_core_types::{Lamports, TokenQuantity};
use copybot_storage_core::{
    EXECUTION_ACCOUNTING_PENDING_REASON, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use serde_json::{json, Value};

fn tick_config(rpc: &Rpc) -> ExecutionConfig {
    let mut cfg = config(&rpc.url);
    cfg.canary_enabled = true;
    cfg.canary_dry_run = true;
    cfg.canary_tiny_submit_enabled = true;
    cfg.execution_signer_pubkey = WALLET.into();
    cfg.execution_signer_keypair_path = "/nonexistent/receipt-no-signing".into();
    cfg.priority_fee_canary_rpc_url = rpc.url.clone();
    cfg.swap_transaction_dry_run_enabled = true;
    cfg.quote_canary_enabled = false;
    cfg.canary_buy_size_sol = 0.01;
    cfg.canary_max_signal_age_seconds = 10;
    cfg.max_confirm_seconds = 1;
    cfg
}

fn closed_history(f: &Fixture, token: &str) -> Result<()> {
    let qty = TokenQuantity::new(7_000, 3);
    f.store.record_execution_canary_open_position(
        &format!("old-{token}"),
        token,
        7.0,
        Some(qty),
        0.7,
        f.now - Duration::hours(2),
    )?;
    f.store.close_execution_canary_open_position(
        token,
        7.0,
        Some(qty),
        0.1,
        0.0,
        f.now - Duration::hours(1),
    )?;
    Ok(())
}

fn recovery_position(f: &Fixture) -> Result<()> {
    // Replace only the synthetic fixture's initial inventory, before any reconciliation.
    f.conn()?.execute("DELETE FROM positions", [])?;
    closed_history(f, TOKEN)?;
    f.store.record_execution_canary_open_position(
        "recovery-orphan:pending",
        TOKEN,
        7.0,
        Some(TokenQuantity::new(7_000, 3)),
        0.9,
        f.now,
    )?;
    Ok(())
}

fn wallet(rpc: &Rpc, balances: &[(&str, u64)]) {
    *rpc.accounts.lock().unwrap() = json!({"result":{"value":balances.iter().map(|(mint, raw)| {
        json!({"account":{"data":{"parsed":{"info":{"mint":mint,
            "tokenAmount":{"amount":raw.to_string(),"decimals":3}}}}}})
    }).collect::<Vec<_>>()}});
}

fn position_snapshot(f: &Fixture) -> Result<Vec<Value>> {
    let conn = f.conn()?;
    let mut stmt = conn.prepare("SELECT position_id, qty, cost_sol, cost_lamports, qty_raw, pnl_sol, pnl_lamports, opened_ts, state FROM positions ORDER BY position_id")?;
    let values = stmt
        .query_map([], |r| {
            Ok(json!({
                "id":r.get::<_,String>(0)?, "qty":r.get::<_,f64>(1)?, "cost":r.get::<_,f64>(2)?,
                "cost_lamports":r.get::<_,Option<i64>>(3)?, "raw":r.get::<_,Option<String>>(4)?,
                "pnl":r.get::<_,Option<f64>>(5)?, "pnl_lamports":r.get::<_,Option<i64>>(6)?,
                "opened":r.get::<_,String>(7)?, "state":r.get::<_,String>(8)?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(values)
}

fn assert_pending(f: &Fixture, cfg: &ExecutionConfig, rpc: &Rpc) -> Result<()> {
    assert_eq!(f.fills()?, 0);
    let order = f.store.load_execution_canary_order(&f.order_id)?.unwrap();
    assert_eq!(order.status, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED);
    assert_eq!(order.tx_signature.as_deref(), Some(SIGNATURE));
    assert_eq!(order.attempt, 1);
    assert_eq!(
        crate::execution_canary_safety::pre_submit_safety_snapshot(
            cfg,
            &f.store,
            f.now + Duration::seconds(2_000)
        )?
        .blocked_reason,
        Some(EXECUTION_ACCOUNTING_PENDING_REASON)
    );
    assert_eq!(
        f.store
            .execution_canary_receipt_submit_block_reason("next-sell", TOKEN, "sell")?,
        Some("sell_token_accounting_pending")
    );
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "sendTransaction"));
    Ok(())
}

#[tokio::test]
async fn receipt_orphan_buy_tick_waits_then_accounts_only_actual_inventory() -> Result<()> {
    for legacy in [false, true] {
        let mut f = Fixture::new("buy")?;
        closed_history(&f, TOKEN)?;
        closed_history(&f, "OtherMint")?;
        if legacy {
            f.store
                .mark_execution_canary_confirmed(&f.order_id, f.now)?;
        }
        let rpc = Rpc::new(json!({"result":null})).await?;
        rpc.context(format!(
            "receipt_orphan_buy_tick_waits_then_accounts_only_actual_inventory legacy={legacy:?}"
        ));
        wallet(&rpc, &[(TOKEN, 7_000), ("OtherMint", 2_000)]);
        let cfg = tick_config(&rpc);
        let runner = crate::execution_canary::ExecutionCanaryRunner::new(cfg.clone());
        f.reopen()?;
        let first = runner
            .process_tick(&f.store, f.now + Duration::seconds(1_000))
            .await?;
        assert_pending(&f, &cfg, &rpc)?;
        assert!(
            f.store
                .load_execution_canary_open_position(TOKEN)?
                .is_none(),
            "pending BUY must not be recovered from wallet balance"
        );
        assert_eq!(first.orphan_recovery_recovered, 1);
        let other = f
            .store
            .load_execution_canary_open_position("OtherMint")?
            .unwrap();
        assert_eq!(other.qty_exact, Some(TokenQuantity::new(2_000, 3)));
        assert_eq!(other.cost_lamports, Some(Lamports::new(10_000_000)));
        f.reopen()?;
        *rpc.status.lock().unwrap() = json!({"result":{"value":[null]}});
        runner
            .process_tick(&f.store, f.now + Duration::seconds(2_000))
            .await?;
        assert_pending(&f, &cfg, &rpc)?;
        rpc.set(receipt("buy", -900_000_000));
        runner
            .process_tick(&f.store, f.now + Duration::seconds(3_000))
            .await?;
        let position = f.store.load_execution_canary_open_position(TOKEN)?.unwrap();
        assert_eq!(position.qty_exact, Some(TokenQuantity::new(7_000, 3)));
        assert_eq!(position.cost_lamports, Some(Lamports::new(900_000_000)));
        assert_eq!(position.cost_sol, 0.9);
        assert_completed_tick_is_idempotent(&f, &rpc, &runner).await?;
        rpc.finish().await?;
        let methods = rpc.calls.lock().unwrap();
        assert_eq!(
            methods
                .iter()
                .filter(|m| *m == "getSignatureStatuses")
                .count(),
            usize::from(!legacy)
        );
        let start = usize::from(!legacy);
        assert_eq!(
            &methods[start..start + 3],
            [
                "getTransaction",
                "getTokenAccountsByOwner",
                "getTokenAccountsByOwner"
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn receipt_orphan_sell_tick_preserves_cost_until_actual_receipt() -> Result<()> {
    for legacy in [false, true] {
        for remainder in [0_u64, 3_000] {
            let mut f = Fixture::new("sell")?;
            recovery_position(&f)?;
            if legacy {
                f.store
                    .mark_execution_canary_confirmed(&f.order_id, f.now)?;
            }
            let before = position_snapshot(&f)?;
            let rpc = Rpc::new(json!({"result":null})).await?;
            rpc.context(format!("receipt_orphan_sell_tick_preserves_cost_until_actual_receipt legacy={legacy:?} remainder={remainder:?}"));
            wallet(&rpc, &[(TOKEN, remainder)]);
            let cfg = tick_config(&rpc);
            let runner = crate::execution_canary::ExecutionCanaryRunner::new(cfg.clone());
            f.reopen()?;
            runner
                .process_tick(&f.store, f.now + Duration::seconds(1_000))
                .await?;
            assert_pending(&f, &cfg, &rpc)?;
            assert_eq!(
                position_snapshot(&f)?,
                before,
                "pending SELL must not reduce, close or retimestamp recovery position"
            );
            f.reopen()?;
            runner
                .process_tick(&f.store, f.now + Duration::seconds(2_000))
                .await?;
            assert_eq!(position_snapshot(&f)?, before);
            let mut valid = receipt("sell", 1_200_000_000);
            valid["result"]["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] =
                json!("7000");
            valid["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] =
                json!(remainder.to_string());
            rpc.set(valid);
            runner
                .process_tick(&f.store, f.now + Duration::seconds(3_000))
                .await?;
            // Imported recovery inventory has no proven initial result. Even a full
            // positive receipt must not route around the signed planner's unknown guard.
            assert_eq!(position_snapshot(&f)?, before);
            assert_pending(&f, &cfg, &rpc)?;
            assert!(f
                .store
                .load_execution_canary_receipt_facts(&f.order_id)?
                .is_some());
            f.reopen()?;
            runner
                .process_tick(&f.store, f.now + Duration::seconds(4_000))
                .await?;
            assert_eq!(position_snapshot(&f)?, before);
            assert_pending(&f, &cfg, &rpc)?;
            rpc.finish().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn receipt_orphan_blocks_retimestamp_and_terminal_writeoff_in_both_pending_states(
) -> Result<()> {
    for legacy in [false, true] {
        for terminal in [false, true] {
            let mut f = Fixture::new("sell")?;
            recovery_position(&f)?;
            closed_history(&f, "OtherMint")?;
            f.store.record_execution_canary_open_position(
                "recovery-orphan:other",
                "OtherMint",
                2.0,
                Some(TokenQuantity::new(2_000, 3)),
                0.01,
                f.now,
            )?;
            let rpc = Rpc::new(json!({"result":null})).await?;
            rpc.context(format!("receipt_orphan_blocks_retimestamp_and_terminal_writeoff_in_both_pending_states legacy={legacy:?} terminal={terminal:?}"));
            // Same balance normally triggers retimestamp; terminal flag closes even
            // when wallet balance has not shrunk. Neither may preempt pending receipt.
            wallet(&rpc, &[(TOKEN, 7_000)]);
            if terminal {
                let terminal_order = add_order(
                    &f.store,
                    "old-terminal",
                    "sell",
                    TOKEN,
                    f.now - Duration::hours(26),
                    false,
                )?;
                f.store.mark_execution_canary_failed(
                    &terminal_order,
                    f.now - Duration::hours(25),
                    copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED,
                    "NO_ROUTES_FOUND",
                )?;
                f.store
                    .mark_execution_canary_terminal_sell_no_route_blocked(
                        &terminal_order,
                        "terminal_failed_sell_no_route_written_off",
                    )?;
            }
            if legacy {
                f.store
                    .mark_execution_canary_confirmed(&f.order_id, f.now)?;
            } else {
                f.reconcile(&rpc, 10).await?;
            }
            let before = f.store.load_execution_canary_open_position(TOKEN)?;
            f.reopen()?;
            let state =
                crate::execution_canary_route::process_tiny_submit_orphan_position_recovery_sweep(
                    &tick_config(&rpc),
                    &f.store,
                    f.now + Duration::seconds(4_000),
                )
                .await?
                .unwrap();
            assert_eq!(f.store.load_execution_canary_open_position(TOKEN)?, before);
            assert_eq!(f.conn()?.query_row("SELECT pnl_lamports FROM positions WHERE position_id = 'exec-canary-pos:recovery-orphan:pending'", [], |r| r.get::<_,Option<i64>>(0))?, None);
            assert_eq!(f.fills()?, 0);
            assert!(f.store.execution_canary_token_accounting_pending(TOKEN)?);
            assert!(!f
                .store
                .execution_canary_token_accounting_pending("OtherMint")?);
            // Non-pending mint still gets its existing zero-balance reconciliation.
            assert!(f
                .store
                .load_execution_canary_open_position("OtherMint")?
                .is_none());
            assert_eq!(state.orphan_recovery_reconciled, 1);
            assert_eq!(f.conn()?.query_row("SELECT pnl_lamports FROM positions WHERE position_id = 'exec-canary-pos:recovery-orphan:other'", [], |r| r.get::<_,i64>(0))?, -10_000_000);
            let order = f.store.load_execution_canary_order(&f.order_id)?.unwrap();
            assert_eq!(
                order.status,
                if legacy {
                    copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
                } else {
                    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
                }
            );
            assert!(!rpc
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|m| m == "sendTransaction"));
            rpc.finish().await?;
        }
    }
    Ok(())
}

async fn assert_completed_tick_is_idempotent(
    f: &Fixture,
    rpc: &Rpc,
    runner: &crate::execution_canary::ExecutionCanaryRunner,
) -> Result<()> {
    assert_eq!(f.fills()?, 1);
    assert!(!f.store.execution_canary_accounting_pending()?);
    assert_eq!(
        f.store
            .load_execution_canary_order(&f.order_id)?
            .unwrap()
            .status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    let snapshot = position_snapshot(f)?;
    let receipts = rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|m| *m == "getTransaction")
        .count();
    runner
        .process_tick(&f.store, f.now + Duration::seconds(4_000))
        .await?;
    assert_eq!(position_snapshot(f)?, snapshot);
    assert_eq!(f.fills()?, 1);
    let methods = rpc.calls.lock().unwrap();
    assert_eq!(
        methods.iter().filter(|m| *m == "getTransaction").count(),
        receipts
    );
    assert!(!methods.iter().any(|m| m == "sendTransaction"));
    Ok(())
}
