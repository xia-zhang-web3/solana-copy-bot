use super::buy_retry_safety_fixture::{fixture, reopen, rows};
use super::entry_cost_runtime_fixture::{as_of, closed_loss};
use super::failed_expense_runtime_tests::failure;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::receipt_reconciliation_fixture::add_order;
use anyhow::Result;
use copybot_storage_core::{FailedExpenseCoverage as Coverage, FailedTransactionFacts};
use rusqlite::Connection;

pub(super) fn expense(
    f: &RuntimeFixture,
    name: &str,
    fee: Option<u64>,
    foreign: bool,
    native: bool,
) -> Result<String> {
    let id = add_order(&f.store, name, "sell", "ExpenseMint", f.now, false)?;
    f.store
        .mark_execution_canary_submitted(&id, f.now, &format!("fee-{name}"))?;
    // Different historical route keeps ordinary pending enrichment out of this sweep.
    Connection::open(&f.db_path)?.execute(
        "UPDATE orders SET route='historical-route' WHERE order_id=?1",
        [&id],
    )?;
    f.store.detect_failed_expense(
        &id,
        &f.config.canary_wallet_pubkey,
        "signature_status",
        "confirmed",
        Some(42),
        &failure(),
        f.now,
    )?;
    if let Some(fee) = fee {
        let facts = FailedTransactionFacts {
            tx_signature: format!("fee-{name}"),
            wallet: f.config.canary_wallet_pubkey.clone(),
            slot: 42,
            commitment: "confirmed".into(),
            transaction_error: failure(),
            transaction_fee_lamports: Some(fee.to_string()),
            fee_coverage: Coverage::Known,
            payer: Some(if foreign {
                "other-payer".into()
            } else {
                f.config.canary_wallet_pubkey.clone()
            }),
            payer_coverage: Coverage::Known,
            wallet_native_pre_lamports: native.then(|| fee.to_string()),
            wallet_native_post_lamports: native.then(|| "0".into()),
            native_coverage: if native {
                Coverage::Known
            } else {
                Coverage::Missing
            },
        };
        f.store.apply_failed_expense(&id, &facts, f.now)?;
    }
    Ok(id)
}

#[tokio::test]
async fn entry_cost_below_cap_keeps_runtime_policy_for_known_zero_foreign_pending_and_unknown(
) -> Result<()> {
    for mode in [
        "zero",
        "foreign",
        "all_unknown",
        "mixed",
        "known_native_missing",
        "one_below",
    ] {
        let mut f = fixture(&format!("b13-policy-{mode}")).await?;
        f.config.canary_max_daily_loss_sol = 0.02;
        let fee = match mode {
            "zero" => Some(0),
            "all_unknown" => None,
            _ => Some(3),
        };
        expense(
            &f,
            "known",
            fee,
            mode == "foreign",
            mode != "known_native_missing",
        )?;
        if mode == "mixed" {
            expense(&f, "unknown", None, false, false)?;
        }
        if mode == "one_below" {
            closed_loss(&f, 19_999_996)?;
        }
        let cost = f.store.execution_canary_entry_cost(as_of(&f))?;
        assert!(!cost.check_cap(0.02)?.exhausted);
        if matches!(mode, "all_unknown" | "mixed" | "known_native_missing") {
            assert!(!cost.selected_cost_complete());
            assert!(cost.failed_expenses.economic_pnl_lamports.is_none());
        }
        reopen(&mut f)?;
        let result = f.sweep().await?;
        f.finish().await?;
        assert_eq!(result.safety_blocked, 0, "{mode}: {result:?}");
        assert!(f.store.execution_canary_fill_exists(
            &f.store
                .load_execution_canary_order_by_signal(&f.signal.signal_id)?
                .unwrap()
                .order_id
        )?);
        super::initial_sol_rpc_fixture::assert_funded_buy_trace(
            &f.calls(),
            &[
                "quote",
                "build-instructions",
                "simulateTransaction",
                "sendTransaction",
                "getSignatureStatuses",
                "getTransaction",
            ],
        );
        eprintln!("B13 below-cap {mode}: {:?}", f.calls());
    }
    Ok(())
}

#[tokio::test]
async fn entry_cost_known_fee_only_pending_native_and_changed_wallet_still_block() -> Result<()> {
    for mode in [
        "fee_only",
        "native_missing",
        "wallet_switch",
        "closed_only",
        "one_above",
    ] {
        let mut f = fixture(&format!("b13-block-{mode}")).await?;
        f.config.canary_max_daily_loss_sol = 0.02;
        if mode == "closed_only" {
            closed_loss(&f, 20_000_000)?;
        } else {
            expense(&f, "fee", Some(20_000_000), false, mode != "native_missing")?;
        }
        if mode == "one_above" {
            closed_loss(&f, 1)?;
        }
        if mode == "wallet_switch" {
            f.config.canary_wallet_pubkey = "changed-config-wallet".into();
        }
        let before = rows(&f)?;
        reopen(&mut f)?;
        let out = f.sweep().await?;
        f.finish().await?;
        assert_eq!(
            out.skipped_reason,
            Some("max_daily_loss"),
            "{mode}: {out:?}"
        );
        assert_eq!(rows(&f)?, before);
        assert!(f.calls().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn entry_cost_read_errors_fail_closed_without_execution_or_writes() -> Result<()> {
    for sql in [
        "DROP TABLE execution_failed_expense_tasks",
        "DROP TABLE execution_failed_expense_ledger",
        "UPDATE orders SET submit_ts='invalid' WHERE route='historical-route'",
    ] {
        let mut f = fixture("b13-read-failure").await?;
        expense(&f, "fee", Some(1), false, true)?;
        let conn = Connection::open(&f.db_path)?;
        conn.execute_batch("PRAGMA foreign_keys=OFF")?;
        conn.execute_batch(sql)?;
        drop(conn);
        let before = rows(&f)?;
        reopen(&mut f)?;
        assert!(f.sweep().await.is_err());
        f.finish().await?;
        assert!(f.calls().is_empty());
        assert_eq!(rows(&f)?, before);
    }
    Ok(())
}
