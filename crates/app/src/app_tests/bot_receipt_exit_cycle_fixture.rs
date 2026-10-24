//! LOCAL daemon/consumer operands only. No copied financial rows or historic edits.
use crate::app_tests::{
    controlled_copy_pipeline_fixture as frames,
    copy_cycle_fixture::{Cycle, BOT_RAW},
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{ExecutionConfig, OwnerExitConfig, PriorBotReceiptExitConfig};
use copybot_storage_core::{owner_exit_order_id, SqliteStore};

pub(super) async fn bot_buy(c: &Cycle) -> Result<()> {
    let runner = c.runner()?;
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            c.check_peers()?;
            let s = Box::pin(runner.process_tick(&c.case.bot, Utc::now())).await?;
            if c.case
                .bot
                .load_execution_canary_open_position(frames::USDC)?
                .is_some()
            {
                break;
            }
            ensure!(
                s.last_error.is_none() && s.state_machine_failed == 0,
                "LOCAL BOT BUY: {s:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("LOCAL actual BOT BUY timeout")??;
    ensure!(
        c.bot.sends() == 1 && c.count("orders")? == 1 && c.count("fills")? == 1,
        "LOCAL actual BOT BUY must have one dispatch/fill"
    );
    Ok(())
}

pub(super) fn old_exit(
    c: &Cycle,
    source: &str,
    at: DateTime<Utc>,
) -> Result<(ExecutionConfig, PriorBotReceiptExitConfig)> {
    let buy = format!("exec-canary:native-buy-v1:{source}");
    let facts = c
        .case
        .bot
        .load_execution_canary_receipt_facts(&buy)?
        .context("LOCAL old native receipt")?;
    let mut config = c.app.execution.clone();
    config.technical_cohort.as_mut().unwrap().activate = false;
    config.tiny_experiment.activate = false;
    config.native_fresh_buy = None;
    config.canary_entry_submit_enabled = false;
    let prior = PriorBotReceiptExitConfig {
        policy: copybot_config::PRIOR_BOT_RECEIPT_EXIT_V1.into(),
        state_db_path: c.case.path.to_string_lossy().into_owned(),
        exit_intent_id: format!("bot-receipt-exit:{source}"),
        buy_order_id: buy.clone(),
        buy_receipt_signature: facts.tx_signature,
        position_id: format!("exec-canary-pos:{buy}"),
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        mint: frames::USDC.into(),
        amount_raw: BOT_RAW,
        decimals: 6,
    };
    config.owner_exit = Some(OwnerExitConfig {
        source_recovery_parent_intent_id: None,
        policy: copybot_config::OWNER_BOT_RECEIPT_EXIT_V1.into(),
        activate: true,
        run_id: config.tiny_experiment.id.clone().unwrap(),
        intent_id: prior.exit_intent_id.clone(),
        buy_order_id: buy,
        buy_receipt_signature: prior.buy_receipt_signature.clone(),
        position_id: prior.position_id.clone(),
        wallet_pubkey: prior.wallet_pubkey.clone(),
        signer_pubkey: prior.wallet_pubkey.clone(),
        genesis_hash: crate::app_tests::owner_buy_fixture::GENESIS.into(),
        mint: prior.mint.clone(),
        amount_raw: BOT_RAW,
        decimals: 6,
        route: config.canary_route.clone(),
        activated_at: at.to_rfc3339(),
        expires_at: (at + Duration::seconds(120)).to_rfc3339(),
        max_priority_fee_lamports: 22_000,
        min_reserve_lamports: 0,
        max_slippage_bps: 50,
        max_daily_loss_lamports: 20_000_000,
    });
    copybot_config::validate_owner_exit(&config)?;
    Ok((config, prior))
}

pub(super) async fn phase_b_held(b: &Cycle) -> Result<()> {
    let s = Box::pin(source_tick(b)).await?;
    ensure!(
        s.state_machine_skipped_reason == Some("prior_bot_receipt_exit_pending"),
        "LOCAL B SOURCE hold: {s:?}"
    );
    let native = crate::execution_canary_safety::pre_submit_safety_snapshot(
        &b.app.execution,
        &b.case.bot,
        Utc::now(),
    )?;
    ensure!(
        native.blocked_reason == Some("prior_bot_receipt_exit_pending"),
        "LOCAL B native BUY shares the prior-exit safety gate"
    );
    // Recreate the runner and open the BOT store anew: restarts cannot release B.
    let restarted = SqliteStore::open(&b.case.path)?;
    Box::pin(b.runner()?.process_tick(&restarted, Utc::now())).await?;
    ensure!(
        !crate::execution_canary_safety::prior_bot_receipt_exit::ready(&b.app.execution)?
            && b.source.sends() == 0
            && b.bot.sends() == 0
            && b.count("orders")? == 0,
        "LOCAL B BUY must remain zero before positive old closure"
    );
    let orders: i64 = b
        .case
        .source
        .sql()?
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))?;
    ensure!(orders == 0, "LOCAL B SOURCE order must remain absent");
    Ok(())
}

pub(super) async fn full_b_cycle(b: &mut Cycle) -> Result<(String, String, String)> {
    Box::pin(source_tick(b)).await?;
    ensure!(b.source.sends() == 1, "LOCAL B SOURCE automatic BUY");
    let source_buy = Box::pin(b.event(true, 0, true)).await?;
    Box::pin(bot_buy(b)).await?;
    ensure!(
        b.case.ready()?,
        "LOCAL B actual BOT receipt must release SOURCE SELL"
    );
    let bot_buy = Box::pin(b.event(false, 0, true)).await?;
    Box::pin(source_tick(b)).await?;
    ensure!(b.source.sends() == 2, "LOCAL B SOURCE automatic SELL");
    let source_sell = Box::pin(b.event(true, 1, true)).await?;
    let runner = b.runner()?;
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            b.check_peers()?;
            let s = Box::pin(runner.process_tick(&b.case.bot, Utc::now())).await?;
            if b.bot.sends() == 2
                && b.case
                    .bot
                    .load_execution_canary_open_position(frames::USDC)?
                    .is_none()
            {
                break;
            }
            ensure!(s.last_error.is_none(), "LOCAL B automatic SELL: {s:?}");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("LOCAL B full close timeout")??;
    ensure!(
        (
            b.source.sends(),
            b.bot.sends(),
            b.count("orders")?,
            b.count("fills")?
        ) == (2, 2, 2, 2),
        "LOCAL B must complete exactly four automatic trades"
    );
    Ok((source_buy, bot_buy, source_sell))
}

pub(super) async fn old_tick(
    c: &ExecutionConfig,
    store: &SqliteStore,
    at: DateTime<Utc>,
) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    crate::app_tests::entry_risk_clock_fixture::at(
        at,
        Box::pin(
            crate::execution_canary::ExecutionCanaryRunner::new(c.clone())
                .process_tick(store, at - Duration::milliseconds(1)),
        ),
    )
    .await
}

pub(super) async fn source_tick(
    c: &Cycle,
) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    Box::pin(
        crate::execution_canary::ExecutionCanaryRunner::new(c.case.source.config.clone())
            .process_tick(&c.case.source.store, Utc::now()),
    )
    .await
}

pub(super) fn old_closed(c: &Cycle, p: &PriorBotReceiptExitConfig) -> Result<()> {
    let id = owner_exit_order_id(&p.exit_intent_id);
    let cash = c
        .case
        .bot
        .load_execution_canary_cash_settlement(&id)?
        .context("LOCAL old cash fill")?;
    ensure!(
        (
            cash.sold_quantity.raw(),
            cash.remaining_quantity.raw(),
            cash.remaining_entry_basis.as_u64()
        ) == (BOT_RAW, 0, 0),
        "LOCAL old lot must close in full"
    );
    let fee: (String, u64) = c.case.sql()?.query_row(
        "SELECT state,actual_fee FROM owner_exit_fee_reservations WHERE order_id=?1",
        [&id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        fee == (
            "successful".into(),
            crate::app_tests::wsol_lifecycle_oracle::FEE
        ),
        "LOCAL actual old SELL fee"
    );
    ensure!(
        !c.case.bot.execution_canary_accounting_pending()?,
        "LOCAL old accounting pending"
    );
    let state = c.bot.state.lock().unwrap();
    let personal: u64 = state
        .bank
        .accounts
        .values()
        .filter_map(|a| a.token)
        .filter(|(_, owner, _)| owner == &state.bank.wallet)
        .map(|(_, _, raw)| raw)
        .sum();
    ensure!(
        personal == 100_000_000,
        "LOCAL original personal inventory must remain untouched"
    );
    Ok(())
}
