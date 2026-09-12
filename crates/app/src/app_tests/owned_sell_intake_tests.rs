use super::open_risk_sell_fixture::TOKEN;
use super::owned_sell_intake_fixture::Intake;
use anyhow::Result;

#[tokio::test]
async fn owned_sell_intake_raw_exit_uses_existing_pipeline_without_shadow_accounting() -> Result<()>
{
    for (notional, lag) in [(0.1, 1), (0.5, 120), (0.1, 120)] {
        let mut f = Intake::new(notional, lag, 600_000).await?;
        assert_eq!(f.counts()?, (0, 0, 0, 0));
        f.dispatch(false, true).await?;
        assert_eq!(f.scheduler.held_shadow_sell_count(), 1);
        assert!(f.drain().await?.is_none());
        assert_eq!(f.counts()?, (0, 0, 0, 0));
        f.release();
        let signal = f
            .drain()
            .await?
            .expect("raw owned SELL must survive entry minimum/lag");
        assert_eq!(signal.signal_id, f.id());
        assert_eq!(signal.closed_qty, 0.0);
        let saved = f.f.store.load_copy_signal_by_signal_id(&f.id())?.unwrap();
        assert_eq!(saved.ts, f.f.swap.ts_utc);
        assert_eq!(saved.status, "execution_sell_intent");
        assert_eq!(
            (&saved.wallet_id, saved.side.as_str(), saved.token.as_str()),
            (&f.f.swap.wallet, "sell", TOKEN)
        );
        assert_eq!(saved.notional_sol, notional);
        let gate_rows: u64 = f.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_quote_canary_shadow_gate_events WHERE signal_id = ?1",
            [f.id()],
            |row| row.get(0),
        )?;
        assert_eq!(gate_rows, 0, "intent is not a recorded shadow fill");
        assert_eq!(f.counts()?, (0, 0, 1, 0));
        let summary = f.hot(&signal).await?;
        assert_eq!(f.f.sends(), 1, "{summary:?}");
        let quote =
            f.f.store
                .load_execution_quote_canary_event_by_id(&f.quote_id())?
                .unwrap();
        assert_eq!(quote.quote_in_amount_raw.as_deref(), Some("7000"));
        assert_eq!(quote.shadow_closed_trade_id, None);
        assert_eq!(quote.signal_ts, Some(f.f.swap.ts_utc));
        let order =
            f.f.store
                .load_execution_canary_order_by_signal(&f.id())?
                .unwrap();
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
        );
        let plan =
            f.f.store
                .load_execution_canary_build_plan_metadata(&order.order_id)?
                .unwrap();
        assert_eq!(plan.quote_in_amount_raw.as_deref(), Some("7000"));
        assert_eq!(plan.signal_id, saved.signal_id);
        assert_eq!(plan.order_id, order.order_id);
        assert_eq!(plan.quote_event_id.as_deref(), Some(f.quote_id().as_str()));
        let raw_quote: serde_json::Value =
            serde_json::from_str(plan.quote_response_json.as_deref().unwrap())?;
        assert_eq!(raw_quote["inputMint"], TOKEN);
        assert_eq!(raw_quote["inAmount"], "7000");
        let fee_json: serde_json::Value =
            serde_json::from_str(plan.priority_fee_json.as_deref().unwrap())?;
        let sent =
            f.f.calls
                .lock()
                .unwrap()
                .iter()
                .find(|(_, body)| body["method"] == "sendTransaction")
                .unwrap()
                .1["params"][0]
                .as_str()
                .unwrap()
                .to_string();
        use base64::Engine;
        use sha2::{Digest, Sha256};
        let bytes = base64::engine::general_purpose::STANDARD.decode(sent)?;
        assert_eq!(
            fee_json["fee_proof"]["transaction_sha256"],
            format!("{:x}", Sha256::digest(bytes))
        );
        assert_eq!(
            f.f.store
                .load_execution_canary_open_position(TOKEN)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            7000
        );
        f.dispatch(false, false).await?;
        assert!(f.drain().await?.is_none());
        f.f.reopen()?;
        f.tick().await?;
        assert_eq!(f.counts()?, (0, 0, 1, 1));
        assert_eq!(f.f.sends(), 1);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_restart_tick_finds_intent_before_quote_or_order() -> Result<()> {
    let mut f = Intake::new(0.1, 120, 600_000).await?;
    f.dispatch(false, false).await?;
    f.drain().await?.expect("persisted exit intent");
    assert_eq!(f.counts()?, (0, 0, 1, 0));
    assert_eq!(f.f.sends(), 0);
    f.f.reopen()?;
    let summary = f.tick().await?;
    assert_eq!(f.f.sends(), 1, "{summary:?}");
    assert_eq!(f.counts()?, (0, 0, 1, 1));
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_hot_tick_overlap_has_one_order_and_submit() -> Result<()> {
    let mut f = Intake::new(0.1, 120, 600_000).await?;
    f.dispatch(false, false).await?;
    let signal = f.drain().await?.unwrap();
    let (hot, tick) = tokio::join!(f.hot(&signal), f.tick());
    hot?;
    tick?;
    assert_eq!(f.counts()?, (0, 0, 1, 1));
    assert_eq!(f.f.sends(), 1);
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_restart_after_quote_refreshes_durable_unordered_intent() -> Result<()> {
    let mut f = Intake::new(0.1, 120, 600_000).await?;
    f.dispatch(false, false).await?;
    let signal = f.drain().await?.unwrap();
    crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.f.config.clone())
        .process_recorded_shadow_signal(&f.f.store, &signal, f.f.now)
        .await?;
    assert_eq!(f.counts()?, (0, 0, 1, 0));
    f.f.reopen()?;
    f.f.now += chrono::Duration::days(2);
    let summary = f.tick().await?;
    assert_eq!(f.f.sends(), 1, "{summary:?}");
    let quote =
        f.f.store
            .load_execution_quote_canary_event_by_id(&f.quote_id())?
            .unwrap();
    assert_eq!(quote.request_ts, f.f.now);
    let provider =
        f.f.store
            .load_execution_quote_canary_provider_sample(
                &f.quote_id(),
                copybot_storage_core::PROVIDER_GENERIC_METIS,
            )?
            .unwrap();
    assert_eq!(provider.request_ts, f.f.now);
    assert_eq!(quote.signal_ts, Some(f.f.swap.ts_utc));
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_position_replaced_after_quote_remains_protected() -> Result<()> {
    let mut f = Intake::new(0.1, 120, 600_000).await?;
    f.dispatch(false, false).await?;
    let signal = f.drain().await?.unwrap();
    crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.f.config.clone())
        .process_recorded_shadow_signal(&f.f.store, &signal, f.f.now)
        .await?;
    f.conn()?.execute(
        "UPDATE positions SET opened_ts = ?1",
        [f.f.now.to_rfc3339()],
    )?;
    let summary = f.f.submit(&f.quote_id()).await?;
    assert_eq!(f.f.sends(), 0, "{summary:?}");
    assert_eq!(summary.skipped_reason, Some("sell_before_position"));
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_preserves_approximate_source_and_wallet_bounded_raw_amount() -> Result<()>
{
    for approximate in [false, true] {
        let mut f = Intake::new(0.1, 120, 600_000).await?;
        if approximate {
            f.f.swap.exact_amounts = None;
        }
        f.conn()?.execute(
            "UPDATE positions SET qty=9.0, qty_raw='9000', cost_sol=0.09, cost_lamports=90000000",
            [],
        )?;
        f.dispatch(false, false).await?;
        let signal = f.drain().await?.unwrap();
        f.hot(&signal).await?;
        assert_eq!(f.f.sends(), 1);
        let saved = f.f.store.load_copy_signal_by_signal_id(&f.id())?.unwrap();
        assert_eq!(saved.notional_lamports.is_none(), approximate);
        let quote =
            f.f.store
                .load_execution_quote_canary_event_by_id(&f.quote_id())?
                .unwrap();
        assert_eq!(quote.quote_in_amount_raw.as_deref(), Some("9000"));
        let order =
            f.f.store
                .load_execution_canary_order_by_signal(&f.id())?
                .unwrap();
        let plan =
            f.f.store
                .load_execution_canary_build_plan_metadata(&order.order_id)?
                .unwrap();
        assert_eq!(plan.quote_in_amount_raw.as_deref(), Some("7000"));
        assert_eq!(
            f.f.store
                .load_execution_canary_open_position(TOKEN)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            9000
        );
        f.finish().await?;
    }
    Ok(())
}
