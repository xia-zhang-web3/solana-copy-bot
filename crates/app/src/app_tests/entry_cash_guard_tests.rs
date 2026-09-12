#[path = "../../../storage-core/tests/common/entry_cash_fixture.rs"]
pub(super) mod cash;
use super::buy_retry_safety_fixture::{reopen, rows};
use super::entry_cost_runtime_fixture::as_of;
use super::entry_risk_clock_fixture::at;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use anyhow::Result;
use chrono::Duration;
use rusqlite::Connection;

pub(super) fn partial(f: &RuntimeFixture) -> Result<()> {
    let conn = Connection::open(&f.db_path)?;
    cash::inventory(&conn, "partial-lot", "PartialMint", 3, 9, f.now)?;
    let v = cash::settle(
        &f.store,
        &conn,
        "exec-canary:partial-loss",
        "partial-signature",
        &f.config.canary_wallet_pubkey,
        "PartialMint",
        1,
        -4,
        as_of(f) - Duration::nanoseconds(1),
    )?;
    assert_eq!(v.cash_result_delta.as_i128(), -7);
    assert_eq!(v.remaining_quantity.raw(), 2);
    Ok(())
}

#[tokio::test]
async fn entry_cash_actual_partial_loss_blocks_hot_buy_and_retry_at_exact_cap() -> Result<()> {
    for retry in [false, true] {
        let mut f = RuntimeFixture::new(
            &format!("b16-partial-{retry}"),
            20_000_000,
            200,
            10_000_000,
            100,
            retry,
        )
        .await?;
        f.config.canary_max_open_positions = 10;
        f.config.canary_max_daily_loss_sol = 7e-9;
        partial(&f)?;
        assert!(!f.store.execution_canary_accounting_pending()?);
        assert_eq!(f.store.execution_canary_open_position_count()?, 1);
        let before = rows(&f)?;
        reopen(&mut f)?;
        let result = at(as_of(&f) + Duration::nanoseconds(1), async {
            if retry {
                super::ExecutionCanaryRunner::new(f.config.clone())
                    .process_tick(&f.store, as_of(&f))
                    .await
            } else {
                f.hot().await
            }
        })
        .await;
        f.finish().await?;
        let result = result?;
        eprintln!(
            "B16 actual partial -7 OPEN, cap 7, retry={retry}: reason={:?}, calls={:?}",
            result.state_machine_skipped_reason,
            f.calls()
        );
        assert_eq!(result.state_machine_skipped_reason, Some("max_daily_loss"));
        assert_eq!(result.state_machine_safety_blocked, 1);
        assert!(f.calls().is_empty());
        assert_eq!(rows(&f)?, before);
    }
    Ok(())
}

pub(super) fn historical_duplicate(f: &RuntimeFixture) -> Result<()> {
    let mut conn = Connection::open(&f.db_path)?;
    let id = "exec-canary:duplicate";
    cash::settle(
        &f.store,
        &conn,
        id,
        "independent-partial-signature",
        &f.config.canary_wallet_pubkey,
        "PartialMint",
        1,
        -4,
        as_of(f),
    )?;
    let tx = conn.transaction()?;
    for table in [
        "orders",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        assert_eq!(
            tx.execute(
                &format!("UPDATE {table} SET tx_signature='partial-signature' WHERE order_id=?1"),
                [id]
            )?,
            1
        );
    }
    tx.commit()?;
    Ok(())
}
