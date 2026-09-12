use super::open_risk_sell_fixture::*;
use anyhow::Result;
use chrono::Duration;
use copybot_shadow::{ShadowDropReason, ShadowProcessOutcome};

#[tokio::test]
async fn open_risk_sell_raw_event_reaches_fresh_owned_quote_and_one_submit_after_reopen(
) -> Result<()> {
    let mut f = Fixture::new(100_000).await?;
    let (signal, event_id) = f.quote_raw_sell().await?;
    let summary = f.submit(&event_id).await?;
    assert_eq!(summary.reserved, 1, "{summary:?}");
    assert_eq!(summary.failed, 0, "{summary:?}");
    assert_eq!(f.sends(), 1);
    let order = f
        .store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .unwrap();
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    let metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .unwrap();
    assert_eq!(metadata.quote_in_amount_raw.as_deref(), Some("7000"));
    assert_eq!(metadata.quote_out_amount_raw.as_deref(), Some("70000000"));
    {
        let calls = f.calls.lock().unwrap();
        let quantities: Vec<_> = calls
            .iter()
            .filter(|(p, _)| p.starts_with("GET /quote?"))
            .map(|(p, _)| {
                reqwest::Url::parse(&format!(
                    "http://localhost{}",
                    p.split_whitespace().nth(1).unwrap()
                ))
                .unwrap()
                .query_pairs()
                .find(|(k, _)| k == "amount")
                .unwrap()
                .1
                .into_owned()
            })
            .collect();
        assert_eq!(quantities, ["10000", "7000"]);
        let swap = calls
            .iter()
            .find(|(p, _)| p.contains("/swap-instructions "))
            .expect("fresh transaction build");
        assert_eq!(swap.1["quoteResponse"]["inAmount"], "7000");
        let balance = calls
            .iter()
            .position(|(_, b)| b["method"] == "getTokenAccountsByOwner")
            .unwrap();
        let build = calls
            .iter()
            .position(|(p, _)| p.contains("/swap-instructions "))
            .unwrap();
        let send = calls
            .iter()
            .position(|(_, b)| b["method"] == "sendTransaction")
            .unwrap();
        assert!(balance < build && build < send);
    }
    f.reopen()?;
    assert!(matches!(
        f.service
            .process_swap(&f.store, &f.swap, &f.follow, f.now)?,
        ShadowProcessOutcome::Dropped(ShadowDropReason::DuplicateSignal)
    ));
    let recovery = f
        .service
        .process_restart_recovery_sell(&f.store, &f.swap, f.now)?;
    if let ShadowProcessOutcome::Recorded(replay) = recovery {
        assert_eq!(replay.closed_qty, 0.0);
    }
    let replay = f.submit(&event_id).await?;
    assert_eq!(replay.reserved, 0, "{replay:?}");
    assert_eq!(f.sends(), 1);
    let lots = f.store.list_shadow_lots("leader", TOKEN)?;
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].qty_exact.unwrap().raw(), 10_000);
    assert_eq!(lots[0].cost_lamports.unwrap().as_u64(), 100_000_000);
    assert_eq!(f.close_count(&signal.signal_id)?, 1);
    let after = f
        .store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .unwrap();
    assert_eq!(after.order_id, order.order_id);
    assert_eq!(after.attempt, 1);
    assert_eq!(
        f.store
            .load_copy_signal_by_signal_id(&signal.signal_id)?
            .unwrap()
            .ts,
        f.swap.ts_utc
    );
    // No receipt was supplied: a successful transport submit does not book a fill.
    assert_eq!(
        f.store
            .load_execution_canary_open_position(TOKEN)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        7000
    );
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn open_risk_sell_keeps_latest_buy_pending_receipt_and_in_flight_guards() -> Result<()> {
    for guard in ["latest_buy", "pending_receipt", "in_flight"] {
        let mut f = Fixture::new(600_000).await?;
        let (signal, event_id) = f.quote_raw_sell().await?;
        let prior = match guard {
            "latest_buy" => f.prior_order("buy", f.now - Duration::seconds(1), true)?,
            "pending_receipt" => f.prior_order("sell", f.now - Duration::hours(1), true)?,
            _ => f.prior_order("sell", f.now - Duration::seconds(1), false)?,
        };
        f.reopen()?;
        if guard == "pending_receipt" {
            assert!(f.store.execution_canary_accounting_pending()?);
        }
        let summary = f.submit(&event_id).await?;
        let expected = if guard == "latest_buy" {
            "sell_before_latest_buy"
        } else {
            "sell_token_in_flight"
        };
        assert_eq!(
            summary.skipped_reason,
            Some(expected),
            "{guard}: {summary:?}"
        );
        assert_eq!(summary.reserved, 0);
        if guard != "latest_buy" {
            assert_eq!(summary.last_order_id.as_deref(), Some(prior.as_str()));
        }
        assert!(f
            .store
            .load_execution_canary_order_by_signal(&signal.signal_id)?
            .is_none());
        assert_eq!(f.sends(), 0);
        assert!(!f
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, b)| b["method"] == "getTokenAccountsByOwner"));
        assert_eq!(
            f.store
                .load_copy_signal_by_signal_id(&signal.signal_id)?
                .unwrap()
                .ts,
            f.swap.ts_utc
        );
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn open_risk_sell_still_rejects_priority_fee_over_cap_in_built_bytes() -> Result<()> {
    let mut f = Fixture::new(2_500_001).await?; // ceil(200k CU * price / 1e6) = 500001 > cap.
    let (_, event_id) = f.quote_raw_sell().await?;
    let summary = f.submit(&event_id).await?;
    assert_eq!(f.sends(), 0);
    assert!(
        summary
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("priority_fee"),
        "{summary:?}"
    );
    assert!(f
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(p, _)| p.contains("/swap-instructions ")));
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn open_risk_sell_does_not_bypass_invalid_execution_or_emergency_stop() -> Result<()> {
    for guard in ["invalid_execution", "kill_switch", "tiny_disabled"] {
        let mut f = Fixture::new(600_000).await?;
        let (_, event_id) = f.quote_raw_sell().await?;
        match guard {
            "invalid_execution" => f.config.execution_signer_pubkey.clear(),
            "kill_switch" => std::fs::write(&f.config.canary_kill_switch_path, b"stop")?,
            _ => f.config.canary_tiny_submit_enabled = false,
        }
        let summary =
            crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
                &f.config, &f.store, &event_id, f.now,
            )
            .await?;
        if guard == "tiny_disabled" {
            assert!(summary.is_none());
        } else {
            let summary = summary.unwrap();
            assert_eq!(summary.safety_blocked, 1);
            assert_eq!(
                summary.skipped_reason,
                Some(if guard == "kill_switch" {
                    "kill_switch_active"
                } else {
                    "missing_execution_signer_pubkey"
                })
            );
            assert_eq!(summary.reserved, 0);
        }
        assert_eq!(f.sends(), 0);
        f.finish().await?;
    }
    Ok(())
}
