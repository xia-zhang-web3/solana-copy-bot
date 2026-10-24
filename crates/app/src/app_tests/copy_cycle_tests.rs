//! Actual four-role daemon/consumer orchestration; all providers and chain effects LOCAL.
//! This is not a live trade, a Solana VM execution, or profitability evidence.
use super::{
    controlled_copy_pipeline_fixture as frames,
    copy_cycle_fixture::{Cycle, BOT_RAW},
};
use anyhow::{ensure, Context, Result};
use copybot_storage_core::SqliteStore;
use serde_json::json;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_cycle_controlled_usdc_daemon_buy_gate_sell_closes_lots_once() -> Result<()> {
    let mut c = Cycle::new().await?;
    assert_eq!(
        (c.source.sends(), c.bot.sends(), c.count("orders")?),
        (0, 0, 0)
    );
    c.case.source.tick().await?;
    assert_eq!(c.source.sends(), 1);
    assert!(c
        .case
        .source
        .store
        .load_execution_canary_open_position(frames::USDC)?
        .is_some());
    // GateCase's original peer is unused: inspect the real gate/state and this peer.
    assert!(!c.case.ready()?);
    assert!(crate::execution_owner_roundtrip::exit_config(
        &c.case.source.config,
        &c.case.source.store,
    )?
    .is_none());
    assert!(c
        .case
        .source
        .store
        .load_owner_exit_intent(super::owner_source_gate_fixture::EXIT)?
        .is_none());
    assert_eq!(
        (c.source.sends(), c.bot.sends(), c.count("orders")?),
        (1, 0, 0)
    );
    let source_buy = c.event(true, 0, true).await?;
    assert_eq!(c.case.bot.list_native_buy_pending(8)?.len(), 1);
    assert_eq!(
        c.count("orders")?,
        0,
        "ingress cannot manufacture BOT order"
    );
    let runner = c.runner()?;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            c.check_peers()?;
            let s = runner.process_tick(&c.case.bot, chrono::Utc::now()).await?;
            if c.case
                .bot
                .load_execution_canary_open_position(frames::USDC)?
                .is_some()
            {
                break;
            }
            ensure!(
                s.last_error.is_none() && s.state_machine_failed == 0,
                "LOCAL BOT entry refused: {s:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("automatic BOT BUY timeout")??;
    assert_eq!(
        (c.bot.sends(), c.count("orders")?, c.count("fills")?),
        (1, 1, 1)
    );
    assert_eq!(
        c.case
            .bot
            .load_execution_canary_open_position(frames::USDC)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        BOT_RAW
    );
    assert!(
        c.case.ready()?,
        "actual native BOT receipt must release SOURCE exit gate"
    );
    let bot_buy = c.event(false, 0, true).await?;
    c.case.source.tick().await?;
    c.check_peers()?;
    assert_eq!(c.source.sends(), 2);
    assert!(c
        .case
        .source
        .store
        .load_execution_canary_open_position(frames::USDC)?
        .is_none());
    let source_sell = c.event(true, 1, true).await?;
    // Hold only the external SELL outcome. Its actual daemon dispatch is UNKNOWN.
    c.bot.state.lock().unwrap().unknown_sell = true;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            c.check_peers()?;
            let s = runner.process_tick(&c.case.bot, chrono::Utc::now()).await?;
            if c.bot.sends() == 2 {
                break;
            }
            ensure!(s.last_error.is_none(), "LOCAL BOT exit refused: {s:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("automatic BOT SELL timeout")??;
    assert!(c
        .case
        .bot
        .load_execution_canary_open_position(frames::USDC)?
        .is_some());
    let sell_id: String = c.case.sql()?.query_row(
        "SELECT order_id FROM rpc_owned_sell_dispatches LIMIT 1",
        [],
        |r| r.get(0),
    )?;
    assert!(c
        .case
        .bot
        .load_execution_canary_cash_settlement(&sell_id)?
        .is_none());
    drop(runner);
    let reopened = SqliteStore::open(&c.case.path)?;
    let restarted = c.runner()?;
    for _ in 0..5 {
        restarted
            .process_tick(&reopened, chrono::Utc::now())
            .await?;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        c.bot.sends(),
        2,
        "UNKNOWN restart cannot resend either trade"
    );
    assert!(reopened
        .load_execution_canary_open_position(frames::USDC)?
        .is_some());
    c.bot.state.lock().unwrap().unknown_sell = false;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            c.check_peers()?;
            restarted
                .process_tick(&reopened, chrono::Utc::now())
                .await?;
            if reopened
                .load_execution_canary_cash_settlement(&sell_id)?
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("canonical BOT SELL reconciliation timeout")??;
    assert!(reopened
        .load_execution_canary_open_position(frames::USDC)?
        .is_none());
    let cash = reopened
        .load_execution_canary_cash_settlement(&sell_id)?
        .unwrap();
    assert_eq!(
        (
            cash.sold_quantity.raw(),
            cash.remaining_quantity.raw(),
            cash.remaining_entry_basis.as_u64()
        ),
        (BOT_RAW, 0, 0)
    );
    for _ in 0..5 {
        restarted
            .process_tick(&reopened, chrono::Utc::now())
            .await?;
        c.case.source.tick().await?;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        (
            c.source.sends(),
            c.bot.sends(),
            c.count("orders")?,
            c.count("fills")?
        ),
        (2, 2, 2, 2)
    );
    let unresolved: i64 = c.case.sql()?.query_row(
        "SELECT count(*) FROM execution_tiny_reservations WHERE outcome IS NULL",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(unresolved, 0);
    if let Ok(root) = std::env::var("COPYBOT_COPY_CYCLE_EVIDENCE_DIR") {
        let root = std::path::Path::new(&root);
        ensure!(root.is_dir(), "LOCAL evidence root must already exist");
        c.export(root)?;
        std::fs::write(
            root.join("COPY_CYCLE_RESULT.json"),
            serde_json::to_vec_pretty(&json!({
                "kind":"LOCAL_ACTUAL_DAEMON_AND_CONSUMER_MODEL_CHAIN",
                "source_buy_signature":source_buy,"bot_buy_signature":bot_buy,"source_sell_signature":source_sell,
                "stages":["SOURCE_OWNER_BUY_RECEIPT","SOURCE_NATIVE_ADMISSION","BOT_RUNNER_BUY_RECEIPT","SOURCE_RECEIPT_GATE_RELEASE","SOURCE_OWNER_SELL_CLOSED","BOT_RUNNER_SELL_UNKNOWN_RESTART_NO_RESEND","BOT_RECEIPT_CLOSED"],
                "source_sends":2,"bot_sends":2,"bot_owned_raw":BOT_RAW,"bot_remaining_raw":0,"unresolved":unresolved,
                "mock_boundary":"loopback quote/RPC/simulation and accepted System/SPL instruction model with modeled AMM; synthetic local signer keys",
                "live_trades":false,"external_requests":0,"http_recovery_prefix_in_this_run":false
            }))?,
        )?;
    }
    Ok(())
}

#[path = "copy12_sell_wake_tests.rs"]
mod copy12_sell_wake_tests;
