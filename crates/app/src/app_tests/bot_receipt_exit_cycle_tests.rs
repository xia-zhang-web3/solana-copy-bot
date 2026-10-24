//! LOCAL causal old native BOT exit → separate new four-trade cycle.
//! All quote/chain effects and signer keys are synthetic loopback operands.
use super::{
    controlled_copy_pipeline_fixture as frames,
    copy_cycle_fixture::{Cycle, CycleOptions},
};
#[path = "bot_receipt_exit_cycle_fixture.rs"]
mod f;
use anyhow::{ensure, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{owner_exit_order_id, SqliteStore};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bot_receipt_exit_unknown_restart_holds_new_buys_then_full_cycle_once() -> Result<()> {
    let body = run_cycle();
    eprintln!(
        "LOCAL boxed phase orchestration future bytes={}",
        std::mem::size_of_val(&body)
    );
    Box::pin(body).await
}

async fn run_cycle() -> Result<()> {
    let now = Utc::now();
    let mut a = Cycle::with_options(CycleOptions {
        phase: Some("phase-A"),
        source_window: Some((now - Duration::seconds(101), now + Duration::seconds(20))),
        prior_exit: None,
        source_intent: None,
    })
    .await?;
    // Original A authority is registered with this window before any event.
    // The peer must report actual modeled finality before the BUY, never SQL-retag it.
    a.bot.state.lock().unwrap().confirmation_status = "finalized";
    Box::pin(f::source_tick(&a)).await?;
    let source_a = Box::pin(a.event(true, 0, true)).await?;
    Box::pin(f::bot_buy(&a)).await?;
    ensure!(
        a.case.ready()?,
        "LOCAL A BOT receipt releases only A SOURCE SELL"
    );
    Box::pin(f::source_tick(&a)).await?;
    ensure!(
        (a.source.sends(), a.bot.sends()) == (2, 1),
        "LOCAL A setup is three actual daemon sends"
    );
    // Keep the ordinary BOT source-SELL path inactive; the new native exit owns A.
    let deadline =
        DateTime::parse_from_rfc3339(&a.app.execution.technical_cohort.as_ref().unwrap().deadline)?
            .with_timezone(&Utc);
    let at = deadline + Duration::seconds(1);
    let (exit, prior) = f::old_exit(&a, &source_a, deadline)?;
    let parent = a.case.sql()?.query_row(
        "SELECT deadline FROM native_buy_technical_cohort",
        [],
        |r| r.get::<_, String>(0),
    )?;
    let mut b = Cycle::with_options(CycleOptions {
        phase: Some("phase-B"),
        prior_exit: Some(prior.clone()),
        ..Default::default()
    })
    .await?;
    Box::pin(f::phase_b_held(&b)).await?;
    a.bot.state.lock().unwrap().unknown_sell = true;
    let sent = Box::pin(f::old_tick(&exit, &a.case.bot, at)).await?;
    a.check_peers()?;
    ensure!(
        a.bot.sends() == 2,
        "LOCAL one actual old SELL must submit: {sent:?}"
    );
    let order = owner_exit_order_id(&prior.exit_intent_id);
    ensure!(
        a.case
            .bot
            .load_execution_canary_cash_settlement(&order)?
            .is_none()
            && a.case
                .bot
                .load_execution_canary_open_position(frames::USDC)?
                .is_some(),
        "LOCAL UNKNOWN is not closure"
    );
    Box::pin(f::phase_b_held(&b)).await?;
    let restarted = SqliteStore::open(&a.case.path)?;
    Box::pin(f::old_tick(&exit, &restarted, at)).await?;
    Box::pin(f::phase_b_held(&b)).await?;
    ensure!(a.bot.sends() == 2, "LOCAL UNKNOWN recovery must not resend");
    a.bot.state.lock().unwrap().unknown_sell = false;
    Box::pin(f::old_tick(&exit, &restarted, at)).await?;
    f::old_closed(&a, &prior)?;
    ensure!(
        crate::execution_canary_safety::prior_bot_receipt_exit::ready(&b.app.execution)?,
        "LOCAL positive receipt/cash/closed/fees witness releases B"
    );
    let old_scope = a.app.execution.controlled_receipt_lot.as_ref().unwrap();
    let old_namespace = Cycle::with_options(CycleOptions {
        phase: Some("phase-B-reused-intent"),
        prior_exit: Some(prior.clone()),
        source_intent: Some(old_scope.source_buy_intent_id.clone()),
        ..Default::default()
    })
    .await?;
    // New DBs and new runs cannot release an old SOURCE order/client namespace.
    Box::pin(f::phase_b_held(&old_namespace)).await?;
    Box::pin(f::phase_b_held(&old_namespace)).await?;
    ensure!(
        !old_namespace
            .source
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .any(|method| method == "quote" || method == "simulateTransaction"),
        "LOCAL old SOURCE intent cannot quote, simulate, sign or send"
    );
    for old in [&old_scope.source_run_id, &old_scope.bot_run_id] {
        let mut native = b.app.execution.clone();
        native.tiny_experiment.id = Some(old.clone());
        native.technical_cohort.as_mut().unwrap().run_id = old.clone();
        ensure!(
            !crate::execution_canary_safety::prior_bot_receipt_exit::ready(&native)?,
            "LOCAL new BOT DB must not rearm an old run ID"
        );
        let mut source = b.case.source.config.clone();
        source.owner_technical_buy.as_mut().unwrap().run_id = old.clone();
        ensure!(
            !crate::execution_canary_safety::prior_bot_receipt_exit::ready(&source)?,
            "LOCAL new SOURCE DB must not rearm an old run ID"
        );
    }
    ensure!(
        (b.source.sends(), b.bot.sends(), b.count("orders")?) == (0, 0, 0),
        "LOCAL old identity clones cannot quote, reserve or send"
    );
    let b_signatures = Box::pin(f::full_b_cycle(&mut b)).await?;
    ensure!(
        b_signatures.0 != source_a,
        "LOCAL B native event has a fresh SOURCE BUY signature"
    );
    // Repeated/restarted A and B ticks cannot create any new dispatch.
    Box::pin(f::old_tick(&exit, &restarted, at)).await?;
    Box::pin(b.runner()?.process_tick(&b.case.bot, Utc::now())).await?;
    Box::pin(f::source_tick(&b)).await?;
    ensure!(
        (
            a.source.sends(),
            a.bot.sends(),
            b.source.sends(),
            b.bot.sends()
        ) == (2, 2, 2, 2),
        "LOCAL A setup3+exit1 is separate from B cycle4, with no second send"
    );
    ensure!(
        a.case.sql()?.query_row(
            "SELECT deadline FROM native_buy_technical_cohort",
            [],
            |r| r.get::<_, String>(0)
        )? == parent,
        "LOCAL old parent clock remains unchanged"
    );
    if let Ok(root) = std::env::var("COPYBOT_BOT_EXIT_CYCLE_EVIDENCE_DIR") {
        let root = std::path::Path::new(&root);
        ensure!(root.is_dir(), "LOCAL evidence root must exist");
        for (role, cycle) in [("phase-A", &a), ("phase-B", &b)] {
            let path = root.join(role);
            std::fs::create_dir(&path)?;
            cycle.export(&path)?;
        }
        std::fs::write(
            root.join("RESULT.json"),
            serde_json::to_vec_pretty(&json!({
                "kind":"LOCAL_ACTUAL_DAEMON_CONSUMER_A_EXIT_THEN_B_CYCLE",
                "a_source_buy":source_a,"a_exit_order":order,"b_signatures":b_signatures,
                "a_setup_sends":3,"a_exit_sends":1,"b_cycle_sends":4,
                "new_buys_before_a_confirmed_closed":0,"unknown_restart_resends":0,
                "old_source_intent_reuse_buys":0,"old_source_intent_repeated_restart_buys":0,
                "local_personal_baseline_raw":100000000,"actual_saved_personal_baseline_not_used":100,
                "clock":"task-local future after original A deadline, no SQL edit or sleep",
                "live_trades":false,"external_requests":0
            }))?,
        )?;
    }
    Ok(())
}
