use super::buy_retry_safety_fixture::*;
use super::ExecutionCanaryRunner;
use anyhow::Result;
use copybot_storage_core::{EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_EXPIRED};

#[tokio::test]
async fn buy_retry_safety_block_reopen_unblock_keeps_same_attempt_and_sends_once() -> Result<()> {
    for block in [Block::Disabled, Block::Loss, Block::Open] {
        let mut f = fixture(&format!("b12-resume-{block:?}")).await?;
        let original = f
            .store
            .load_execution_canary_order_by_signal(&f.signal.signal_id)?
            .unwrap();
        block.apply(&mut f)?;
        let before = rows(&f)?;
        for _ in 0..2 {
            reopen(&mut f)?;
            let s = f.sweep().await?;
            assert_eq!(s.safety_blocked, 1);
            assert_eq!(s.skipped_reason, Some(block.reason()));
            assert_eq!(rows(&f)?, before);
            assert!(f.calls().is_empty());
        }
        block.remove(&mut f)?;
        reopen(&mut f)?;
        assert_eq!(f.store.execution_canary_open_position_count()?, 0);
        let s = f.sweep().await?;
        assert_eq!(s.signing_envelope_built, 1, "{s:?}");
        let order = f
            .store
            .load_execution_canary_order(&original.order_id)?
            .unwrap();
        assert_eq!(order.status, EXECUTION_STATUS_CANARY_CONFIRMED);
        assert_eq!(order.attempt, original.attempt);
        assert_eq!(order.client_order_id, original.client_order_id);
        assert_eq!(f.store.execution_canary_open_position_count()?, 1);
        let m = f
            .store
            .load_execution_canary_build_plan_metadata(&order.order_id)?
            .unwrap();
        assert_eq!(m.quote_in_amount_raw.as_deref(), Some("10000000"));
        assert_eq!(m.quote_out_amount_raw.as_deref(), Some("100"));
        let after = rows(&f)?;
        reopen(&mut f)?;
        f.sweep().await?;
        assert_eq!(rows(&f)?, after);
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
        f.finish().await?;
        eprintln!("B12 preserved and resumed {block:?}: {:?}", f.calls());
    }
    Ok(())
}

#[tokio::test]
async fn buy_retry_safety_safe_own_reserve_does_not_consume_an_open_slot() -> Result<()> {
    let mut f = fixture("b12-own-reserve").await?;
    assert_eq!(f.config.canary_max_open_positions, 1);
    assert_eq!(f.store.execution_canary_open_position_count()?, 0);
    assert!(!f.store.execution_canary_accounting_pending()?);
    reopen(&mut f)?;
    let s = ExecutionCanaryRunner::new(f.config.clone())
        .process_tick(&f.store, f.now + chrono::Duration::seconds(4))
        .await?;
    assert_eq!(s.state_machine_simulated, 1);
    assert_eq!(s.state_machine_safety_blocked, 0);
    assert_eq!(
        f.calls()
            .iter()
            .filter(|s| s.as_str() == "sendTransaction")
            .count(),
        1
    );
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn buy_retry_safety_zero_loss_and_budget_expiry_keep_previous_policy() -> Result<()> {
    for exhausted in [false, true] {
        let mut f = fixture(&format!("b12-expiry-{exhausted}")).await?;
        f.config.canary_max_daily_loss_sol = 0.0;
        if exhausted {
            f.config.max_submit_attempts = 1;
        }
        let before = rows(&f)?;
        let s = f.sweep().await?;
        assert!(f.calls().is_empty());
        if exhausted {
            assert_eq!(s.expired, 1);
            assert_eq!(s.skipped_reason, Some("submit_retry_budget_exhausted"));
            let order = f
                .store
                .load_execution_canary_order_by_signal(&f.signal.signal_id)?
                .unwrap();
            assert_eq!(order.status, EXECUTION_STATUS_CANARY_EXPIRED);
            assert_eq!(order.attempt, 2);
        } else {
            assert_eq!(s.safety_blocked, 1);
            assert_eq!(s.skipped_reason, Some("daily_loss_cap_zero"));
            assert_eq!(rows(&f)?, before);
        }
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn buy_retry_safety_read_failure_is_error_without_send_or_accounting_writes() -> Result<()> {
    let mut f = fixture("b12-sql-error").await?;
    let conn = rusqlite::Connection::open(&f.db_path)?;
    conn.execute_batch("ALTER TABLE positions RENAME TO positions_unavailable;")?;
    let before = rows(&f)?;
    let error = f
        .sweep()
        .await
        .expect_err("safety SQL read must fail closed");
    assert!(!error.to_string().is_empty());
    assert!(f.calls().is_empty());
    assert_eq!(rows(&f)?, before);
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn buy_retry_safety_own_known_signature_only_reconciles_when_entry_disabled() -> Result<()> {
    let mut f = fixture("b12-own-signature").await?;
    let order = f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .unwrap();
    f.store.mark_execution_canary_submitted(
        &order.order_id,
        f.now + chrono::Duration::seconds(3),
        "tx-fresh-size",
    )?;
    f.expect_existing_signature("tx-fresh-size");
    f.config.canary_entry_submit_enabled = false;
    reopen(&mut f)?;
    f.sweep().await?;
    let confirmed = f
        .store
        .load_execution_canary_order(&order.order_id)?
        .unwrap();
    assert_eq!(confirmed.status, EXECUTION_STATUS_CANARY_CONFIRMED);
    assert_eq!(confirmed.attempt, order.attempt);
    let after = rows(&f)?;
    reopen(&mut f)?;
    f.sweep().await?;
    assert_eq!(rows(&f)?, after);
    assert_eq!(f.calls(), ["getSignatureStatuses", "getTransaction"]);
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn buy_retry_safety_other_pending_or_failed_evidence_conflict_blocks_buy() -> Result<()> {
    for conflict in [false, true] {
        let mut f = fixture(&format!("b12-other-pending-{conflict}")).await?;
        let other = super::receipt_reconciliation_fixture::add_order(
            &f.store,
            "b12-other",
            "buy",
            "OtherMint",
            f.now,
            false,
        )?;
        let conn = rusqlite::Connection::open(&f.db_path)?;
        if conflict {
            f.store.mark_execution_canary_failed(
                &other,
                f.now,
                "transaction_failed",
                "synthetic conflict",
            )?;
            conn.execute("INSERT INTO execution_failed_expense_tasks(order_id,tx_signature,attempt,route,wallet,token,side,operation_at,detected_at,failure_source,failure_error_json,commitment,status,reason) VALUES(?1,'other-tx',1,'other-route',?2,'OtherMint','buy',?3,?3,'receipt_meta','{}','confirmed','conflict','synthetic conflict')",rusqlite::params![other,f.config.canary_wallet_pubkey,f.now.to_rfc3339()])?;
        } else {
            f.store
                .mark_execution_canary_submitted(&other, f.now, "other-tx")?;
            f.store.mark_execution_canary_confirmed(&other, f.now)?;
        }
        conn.execute(
            "UPDATE orders SET route='other-route' WHERE order_id=?1",
            [other],
        )?;
        assert!(f.store.execution_canary_accounting_pending()?);
        let before = rows(&f)?;
        reopen(&mut f)?;
        let s = f.sweep().await?;
        assert_eq!(s.safety_blocked, 1);
        assert_eq!(s.skipped_reason, Some("confirmed_accounting_pending"));
        assert!(f.calls().is_empty());
        assert_eq!(rows(&f)?, before);
        f.finish().await?;
    }
    Ok(())
}
