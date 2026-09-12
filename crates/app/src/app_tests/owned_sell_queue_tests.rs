use super::open_risk_sell_fixture::TOKEN;
use super::owned_sell_queue_fixture::Queue;
use anyhow::Result;
use copybot_storage_core::*;

#[tokio::test]
async fn owned_sell_queue_pending_receipt_does_not_starve_other_token_after_restart() -> Result<()>
{
    for (old_count, limit) in [(1, 1), (3, 2)] {
        let mut q = Queue::new(old_count, limit).await?;
        for tick in 0..old_count + 2 {
            q.intake.f.reopen()?;
            let before = q.quote_count();
            let summary = q.intake.tick().await?;
            if tick == 0 {
                assert_eq!(
                    summary.state_machine_skipped_reason,
                    Some("sell_token_in_flight")
                );
                assert!(q
                    .intake
                    .f
                    .store
                    .load_execution_quote_canary_event_by_id(&Queue::quote_id(&q.b))?
                    .is_none());
            }
            assert!(
                q.quote_count() - before <= limit as usize + 2,
                "bounded quote work per tick"
            );
            q.assert_a_pending()?;
        }
        assert!(q
            .intake
            .f
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, b)| b["method"] == "getTransaction"));
        q.assert_b_submitted()?;
        for a in &q.a {
            assert!(q
                .intake
                .f
                .store
                .load_execution_quote_canary_event_by_id(&Queue::quote_id(a))?
                .is_some());
        }
        q.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn owned_sell_queue_failure_before_quote_commit_preserves_bounded_progress() -> Result<()> {
    let mut q = Queue::new(3, 2).await?;
    q.intake.conn()?.execute_batch(&format!("CREATE TRIGGER fail_A_quote BEFORE INSERT ON execution_quote_canary_events WHEN NEW.token = '{TOKEN}' BEGIN SELECT RAISE(ABORT, 'synthetic_A_quote_failure'); END;"))?;
    for _ in 0..5 {
        q.intake.f.reopen()?;
        let before = q.quote_count();
        let summary = q.intake.tick().await?;
        assert!(summary.quote_close_errors > 0, "{summary:?}");
        assert!(
            summary
                .last_error
                .as_deref()
                .unwrap_or("")
                .contains("synthetic_A_quote_failure"),
            "{summary:?}"
        );
        assert!(summary.quote_close_candidates <= 2);
        if let Some(id) = summary.last_quote_event_id.as_deref() {
            assert!(q
                .intake
                .f
                .store
                .load_execution_quote_canary_event_by_id(id)?
                .is_some());
        }
        assert!(
            summary.quote_would_execute
                <= summary.quote_close_inserted + summary.quote_close_existing
        );
        assert!(q.quote_count() - before <= 4);
        q.assert_a_pending()?;
        for a in &q.a {
            assert!(q
                .intake
                .f
                .store
                .load_execution_quote_canary_event_by_id(&Queue::quote_id(a))?
                .is_none());
        }
    }
    q.assert_b_submitted()?;
    q.intake
        .conn()?
        .execute_batch("DROP TRIGGER fail_A_quote")?;
    q.intake.f.reopen()?;
    q.intake.tick().await?;
    assert!(q.a.iter().any(|a| q
        .intake
        .f
        .store
        .load_execution_quote_canary_event_by_id(&Queue::quote_id(a))
        .unwrap()
        .is_some()));
    q.assert_a_pending()?;
    q.finish().await?;
    Ok(())
}

#[tokio::test]
async fn owned_sell_queue_receipt_reconciliation_retries_remaining_inventory_with_guards(
) -> Result<()> {
    for case in ["remaining", "closed", "new_buy"] {
        eprintln!("receipt case={case} start");
        let mut q = Queue::new(1, 1).await?;
        for _ in 0..3 {
            q.intake.tick().await?;
        }
        q.assert_a_pending()?;
        q.assert_b_submitted()?;
        if case == "new_buy" {
            q.intake.f.prior_order("buy", q.intake.f.now, false)?;
        }
        // This queue test supplies prepared inventory with a known initial result.
        // Unknown imported inventory is independently required to remain blocked.
        q.intake.conn()?.execute(
            "UPDATE positions SET pnl_lamports=0 WHERE token=?1",
            [TOKEN],
        )?;
        q.allow_blocker_receipt(if case == "closed" { "0" } else { "6000" });
        for _ in 0..3 {
            q.intake.f.reopen()?;
            q.intake.tick().await?;
            let fills: u64 = q.intake.conn()?.query_row(
                "SELECT COUNT(*) FROM fills WHERE order_id=?1",
                [&q.blocker],
                |r| r.get(0),
            )?;
            assert_eq!(fills, 1, "{case}: receipt accounted once across reopen");
        }
        let store = &q.intake.f.store;
        assert_eq!(
            store
                .load_execution_canary_order(&q.blocker)?
                .unwrap()
                .status,
            EXECUTION_STATUS_CANARY_CONFIRMED
        );
        assert!(store.execution_canary_fill_exists(&q.blocker)?);
        let a = store
            .load_copy_signal_by_signal_id(&q.a[0].signal_id)?
            .unwrap();
        assert_eq!(a.ts, q.intake.f.now - chrono::Duration::seconds(120));
        assert_eq!(a.status, EXECUTION_SELL_INTENT_STATUS);
        let order = store.load_execution_canary_order_by_signal(&a.signal_id)?;
        if case == "remaining" {
            let order = order.expect("unblocked A recovers without raw replay");
            assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
            assert_eq!(
                store
                    .load_execution_canary_build_plan_metadata(&order.order_id)?
                    .unwrap()
                    .quote_in_amount_raw
                    .as_deref(),
                Some("6000")
            );
            assert_eq!(q.intake.f.sends(), 2);
        } else {
            assert!(order.is_none(), "{case}");
            assert_eq!(q.intake.f.sends(), 1);
        }
        let position = store.load_execution_canary_open_position(TOKEN)?;
        if case == "closed" {
            assert!(position.is_none());
        } else {
            assert_eq!(position.unwrap().qty_exact.unwrap().raw(), 6000);
        }
        assert_eq!((q.intake.counts()?.0, q.intake.counts()?.1), (0, 0));
        assert_rpc_trace(&q, case)?;
        eprintln!("receipt case={case} checking server");
        q.finish().await?;
        eprintln!("receipt case={case} finish=ok sends={}", q.intake.f.sends());
    }
    Ok(())
}

#[tokio::test]
async fn owned_sell_queue_hot_tick_overlap_after_restart_submits_b_once() -> Result<()> {
    let mut q = Queue::new(1, 1).await?;
    let (hot, tick) = tokio::join!(q.intake.hot(&q.b), q.intake.tick());
    hot?;
    tick?;
    for _ in 0..3 {
        q.intake.f.reopen()?;
        q.intake.tick().await?;
    }
    q.assert_a_pending()?;
    q.assert_b_submitted()?;
    q.finish().await?;
    Ok(())
}

#[tokio::test]
async fn owned_sell_queue_cursor_write_failure_stops_before_quote_or_order() -> Result<()> {
    let mut q = Queue::new(1, 1).await?;
    q.intake.conn()?.execute_batch("CREATE TRIGGER fail_cursor BEFORE INSERT ON execution_owned_sell_cursor BEGIN SELECT RAISE(ABORT, 'synthetic_cursor_failure'); END;")?;
    assert!(q.intake.tick().await.is_err());
    assert_eq!(q.quote_count(), 0);
    assert_eq!(q.intake.f.sends(), 0);
    q.assert_a_pending()?;
    q.intake.conn()?.execute_batch("DROP TRIGGER fail_cursor")?;
    for _ in 0..3 {
        q.intake.f.reopen()?;
        q.intake.tick().await?;
    }
    q.assert_b_submitted()?;
    q.finish().await?;
    Ok(())
}

fn assert_rpc_trace(q: &Queue, case: &str) -> Result<()> {
    use super::open_risk_sell_fixture::SOL;
    let calls = q.intake.f.calls.lock().unwrap();
    let responses = q.intake.f.responses.lock().unwrap();
    assert_eq!(
        calls.len(),
        responses.len(),
        "{case}: every request answered"
    );
    let supplies: Vec<_> = calls
        .iter()
        .enumerate()
        .filter(|(_, (_, body))| body["method"] == "getTokenSupply")
        .collect();
    if case != "new_buy" {
        assert!(supplies.is_empty(), "{case}");
        return Ok(());
    }
    assert!(
        !supplies.is_empty(),
        "new BUY must resolve decimals via RPC"
    );
    for (index, (_, body)) in &supplies {
        assert_eq!(body["params"], serde_json::json!([TOKEN]));
        let (path, quote) = &responses[index - 1];
        assert!(path.starts_with("GET /quote?"));
        assert_eq!(quote["inputMint"], SOL);
        assert_eq!(quote["outputMint"], TOKEN);
        assert_eq!(quote["inAmount"], "200000000");
        assert_eq!(quote["outAmount"], "20000");
        assert_eq!(responses[*index].1["result"]["value"]["decimals"], 3);
        eprintln!(
            "receipt case={case} quote={quote} supply_mint={} decimals=3 response=ok",
            body["params"][0]
        );
    }
    let first = supplies[0].0;
    assert!(
        calls[first + 1..]
            .iter()
            .any(|(_, body)| body["method"] == "getTokenAccountsByOwner"),
        "server continues real runner requests after token supply"
    );
    eprintln!(
        "receipt case={case} answered_after_first_supply={}",
        responses.len() - first - 1
    );
    Ok(())
}
