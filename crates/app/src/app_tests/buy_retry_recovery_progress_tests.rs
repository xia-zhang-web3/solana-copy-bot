use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::{reopen, rows};
use super::ExecutionCanaryRunner;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED;
use rusqlite::{params, Connection};

#[tokio::test]
async fn buy_retry_recovery_progress_missing_then_available_receipt_matrix() -> Result<()> {
    for reason in ["retry_after_rpc_submit_not_sent", NOT_SENT, UNKNOWN] {
        for remove_buy in [false, true] {
            let mut f = queue_fixture(&format!("b12-r1-{reason}-{remove_buy}"), false).await?;
            f.config.canary_entry_submit_enabled = false;
            f.config.canary_batch_limit = 1;
            let buy = buy_order(&f)?;
            let metadata = f
                .store
                .load_execution_canary_build_plan_metadata(&buy.order_id)?;
            if remove_buy {
                let conn = Connection::open(&f.db_path)?;
                conn.execute(
                    "DELETE FROM execution_canary_build_plan_metadata WHERE order_id=?1",
                    [&buy.order_id],
                )?;
                conn.execute("DELETE FROM orders WHERE order_id=?1", [&buy.order_id])?;
            }
            let sell = add_sell(&f, reason == UNKNOWN)?;
            // Existing fixture creates the supported suffix; test exact NotSent too.
            Connection::open(&f.db_path)?.execute(
                "UPDATE orders SET simulation_error=?2 WHERE order_id=?1",
                params![sell, reason],
            )?;
            let sell_before = f.store.load_execution_canary_order(&sell)?.unwrap();
            // Older than SELL: tick one must attempt receipt, then its attempt age advances.
            let pending = add_pending(&f, true)?;
            let known_before = f.store.load_execution_canary_order(&pending)?.unwrap();
            let mut rpc = QueueRpc::new(&mut f, true).await?;
            for n in 0..3 {
                reopen(&mut f)?;
                let s = ExecutionCanaryRunner::new(f.config.clone())
                    .process_tick(&f.store, f.now + Duration::seconds(4 + n))
                    .await?;
                assert!(s.state_machine_existing <= 1, "{s:?}");
                let current = f.store.load_execution_canary_order(&buy.order_id)?;
                assert_eq!(current, if remove_buy { None } else { Some(buy.clone()) });
                assert_eq!(
                    f.store
                        .load_execution_canary_build_plan_metadata(&buy.order_id)?,
                    if remove_buy { None } else { metadata.clone() }
                );
            }
            confirmed(&f, &sell)?;
            let known = f.store.load_execution_canary_order(&pending)?.unwrap();
            assert_eq!(known.status, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED);
            assert_eq!(known.tx_signature, known_before.tx_signature);
            assert_eq!(known.attempt, known_before.attempt);
            assert!(!f.store.execution_canary_fill_exists(&pending)?);
            assert_eq!(
                f.store.load_execution_canary_order(&sell)?.unwrap().attempt,
                sell_before.attempt
            );
            assert_eq!(
                rpc.trace()[..2],
                [
                    "getSignatureStatuses:receipt-signature",
                    "getTransaction:receipt-signature"
                ]
            );
            let mut expected = vec![
                "getSignatureStatuses:receipt-signature",
                "getTransaction:receipt-signature",
                "sell-build-instructions",
                "sell-build-transaction",
                "simulateTransaction:sell",
                "sendTransaction:sell",
                "getSignatureStatuses:b12-sell-signature",
                "getTransaction:b12-sell-signature",
                "getTransaction:receipt-signature",
            ];
            assert_eq!(rpc.trace(), expected);
            *rpc.pending_receipt.lock().unwrap() = false;
            reopen(&mut f)?;
            let s = ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + Duration::seconds(8))
                .await?;
            assert_eq!(s.state_machine_existing, 1);
            confirmed(&f, &pending)?;
            expected.push("getTransaction:receipt-signature");
            let accounted = rows(&f)?;
            for n in 9..11 {
                reopen(&mut f)?;
                ExecutionCanaryRunner::new(f.config.clone())
                    .process_tick(&f.store, f.now + Duration::seconds(n))
                    .await?;
                assert_eq!(rows(&f)?, accounted);
            }
            rpc.finish().await?;
            assert_eq!(rpc.trace(), expected);
            let fills: i64 =
                Connection::open(&f.db_path)?
                    .query_row("SELECT COUNT(*) FROM fills", [], |r| r.get(0))?;
            assert_eq!(fills, 2);
            eprintln!(
                "B12/R1 reason={reason}, remove_buy={remove_buy}: {:?}",
                rpc.trace()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn buy_retry_recovery_progress_allow_entry_keeps_previous_group_priority() -> Result<()> {
    let mut f = queue_fixture("b12-r1-allow-order", false).await?;
    f.config.canary_entry_submit_enabled = true;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 1;
    let sell = add_sell(&f, false)?;
    let pending = add_pending(&f, false)?; // younger than NotSent SELL
    assert!(!f.store.execution_canary_accounting_pending()?);
    let before = f.store.load_execution_canary_order(&sell)?;
    let mut rpc = QueueRpc::new(&mut f, true).await?;
    let s = f.sweep().await?;
    rpc.finish().await?;
    assert_eq!(s.existing, 1);
    assert_eq!(f.store.load_execution_canary_order(&sell)?, before);
    assert_eq!(
        f.store
            .load_execution_canary_order(&pending)?
            .unwrap()
            .status,
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    );
    assert_eq!(
        rpc.trace(),
        [
            "getSignatureStatuses:receipt-signature",
            "getTransaction:receipt-signature"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn buy_retry_recovery_progress_same_mint_receipt_still_blocks_sell_send() -> Result<()> {
    let mut f = queue_fixture("b12-r1-same-mint", false).await?;
    f.config.canary_entry_submit_enabled = false;
    f.config.canary_batch_limit = 1;
    let sell = add_sell(&f, false)?;
    let pending = add_pending(&f, true)?;
    Connection::open(&f.db_path)?.execute("UPDATE copy_signals SET token=?2 WHERE signal_id=(SELECT signal_id FROM orders WHERE order_id=?1)", params![pending, SELL_TOKEN])?;
    let positions_before = rows(&f)?["positions"].clone();
    let mut rpc = QueueRpc::new(&mut f, true).await?;
    for n in 0..3 {
        reopen(&mut f)?;
        super::super::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &f.config,
            &f.store,
            f.now + Duration::seconds(4 + n),
        )
        .await?;
    }
    rpc.finish().await?;
    assert!(f
        .store
        .execution_canary_token_accounting_pending(SELL_TOKEN)?);
    assert!(!rpc.trace().iter().any(|v| v.starts_with("sendTransaction")));
    assert!(!f.store.execution_canary_fill_exists(&sell)?);
    assert_eq!(rows(&f)?["positions"], positions_before);
    assert_eq!(
        f.store
            .load_execution_canary_order(&pending)?
            .unwrap()
            .status,
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    );
    Ok(())
}
