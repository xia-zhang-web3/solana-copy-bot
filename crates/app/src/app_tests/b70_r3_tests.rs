use super::{b70_r2_fixture::Ready, b70_r3_fixture::Boundary, *};
use anyhow::{ensure, Result};
use copybot_storage_core::execution_quote_entry_is_closed;
use std::time::Duration;

async fn closed_consumers(f: &Ready) -> Result<()> {
    for reopen in [false, true] {
        let opened = SqliteStore::open(&f.f.f.path)?;
        let fresh = ExecutionCanaryRunner::new(f.f.execution.clone());
        let (store, runner) = if reopen {
            (&opened, &fresh)
        } else {
            (&f.f.f.store, &f.runner)
        };
        let pending = store
            .load_latest_execution_quote_canary_entry_event(&f.signal.signal_id)?
            .unwrap();
        ensure!(execution_quote_entry_is_closed(&pending));
        // Provider would_execute must not overwrite the owner's closed decision in build metadata.
        let metadata = crate::execution_build_plan_metadata::load_execution_build_plan_metadata(
            store,
            &f.signal.signal_id,
        )?;
        ensure!(metadata.decision_status == pending.decision_status);
        ensure!(
            crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata(
                &f.f.execution,
                &metadata
            )
            .is_some()
        );
        ensure!(
            runner
                .process_recorded_shadow_signal(store, &f.signal, Utc::now())
                .await?
                .skipped_reason
                == Some("hot_buy_owner_pending")
        );
        ensure!(
            crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
                store,
                &f.f.execution,
                "shadow_recorded",
                f.swap.ts_utc
            )?
            .is_empty()
        );
        runner.process_tick(store, Utc::now()).await?;
        let quote =
            crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.f.execution.clone());
        ensure!(quote
            .prepare_hot_quote(store, &f.swap, Utc::now())?
            .is_none());
        store.record_execution_quote_canary_event(&f.quote)?;
        ensure!(!f.orders()?);
    }
    f.unchanged_quote_facts()
}

#[tokio::test]
async fn b70_r3_busy_permission_commit_remains_closed_for_all_consumers() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let mut f = Ready::new(0.067, true).await?;
    f.f.f.store.set_busy_timeout(Duration::from_millis(1))?;
    let lock = f.f.f.conn()?;
    lock.execute_batch("BEGIN IMMEDIATE")?;
    let result = f
        .runner
        .resume_hot_buy(
            &f.f.f.store,
            f.origin.take().unwrap(),
            &f.f.f.follow,
            false,
            &mut f.risk,
            &OperatorEmergencyStop::from_env(),
            true,
            Utc::now(),
        )
        .await;
    lock.execute_batch("ROLLBACK")?;
    let error = result.expect_err("permission commit must encounter the real WAL lock");
    ensure!(format!("{error:#}").contains("database is locked"));
    closed_consumers(&f).await?;
    f.f.save("r3-busy-allow", serde_json::json!({"error":format!("{error:#}"),"orders":0,"same_and_reopen":true,"decision":"owner_pending"}))?;
    Ok(())
}

#[tokio::test]
async fn b70_r3_decision_compare_and_swap_never_reopens_a_winner() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let mut f = Ready::new(0.067, false).await?;
    drop(f.origin.take());
    let mut wrong = f.quote.clone();
    wrong.wallet_id = "other-owner".into();
    ensure!(f
        .f
        .f
        .store
        .complete_execution_quote_entry_owner(&wrong)
        .is_err());
    ensure!(f
        .f
        .f
        .store
        .execution_quote_entry_blocked(&f.signal.signal_id, true)?);
    f.f.f.store.complete_execution_quote_entry_owner(&f.quote)?;
    ensure!(f
        .f
        .f
        .store
        .complete_execution_quote_entry_owner(&f.quote)
        .is_err());
    f.f.f
        .store
        .mark_execution_quote_entry_refused(&f.quote, "first")?;
    f.f.f
        .store
        .mark_execution_quote_entry_refused(&f.quote, "second")?;
    ensure!(f
        .f
        .f
        .store
        .complete_execution_quote_entry_owner(&f.quote)
        .is_err());
    let opened = SqliteStore::open(&f.f.f.path)?;
    ensure!(opened
        .complete_execution_quote_entry_owner(&f.quote)
        .is_err());
    ensure!(
        opened
            .load_latest_execution_quote_canary_entry_event(&f.signal.signal_id)?
            .unwrap()
            .decision_reason
            .as_deref()
            == Some("hot_buy_refused:first")
    );
    f.unchanged_quote_facts()?;
    Ok(())
}

#[tokio::test]
async fn b70_r3_busy_completion_fresh_and_priority_stay_closed_after_restart() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    for existing in [false, true] {
        let f = Boundary::new().await?;
        let swap = f.f.buy();
        let (completion, signal) = f.completed(&swap, existing, true, true).await?;
        let signal = signal.unwrap();
        let before =
            f.f.f
                .store
                .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?;
        let samples = Ready::samples(&f.f)?;
        f.f.f.store.set_busy_timeout(Duration::from_millis(1))?;
        let lock = f.f.f.conn()?;
        lock.execute_batch("BEGIN IMMEDIATE")?;
        let mut stop = OperatorEmergencyStop::from_env();
        stop.active = true;
        let mut risk = ShadowRiskGuard::new(RiskConfig::default());
        let ready = f.runner.complete_hot_observed_buy_quote(
            &f.f.f.store,
            completion,
            &f.f.f.follow,
            false,
            &mut risk,
            &stop,
            true,
            Utc::now(),
            0,
            0,
        );
        lock.execute_batch("ROLLBACK")?;
        ensure!(ready.is_none());
        ensure!(
            f.f.f
                .store
                .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
                == before
        );
        ensure!(Ready::samples(&f.f)? == samples);
        for reopen in [false, true] {
            let opened = SqliteStore::open(&f.f.f.path)?;
            let fresh = ExecutionCanaryRunner::new(f.f.execution.clone());
            let (store, runner) = if reopen {
                (&opened, &fresh)
            } else {
                (&f.f.f.store, &f.runner)
            };
            ensure!(store.execution_quote_entry_blocked(&signal.signal_id, true)?);
            ensure!(
                runner
                    .process_recorded_shadow_signal(store, &signal, Utc::now())
                    .await?
                    .skipped_reason
                    == Some("hot_buy_owner_pending")
            );
            ensure!(
                crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
                    store,
                    &f.f.execution,
                    "shadow_recorded",
                    swap.ts_utc
                )?
                .is_empty()
            );
            if !existing && !reopen {
                // A retry may collect new network facts. It cannot manufacture owner permission.
                let (tick, server) = tokio::join!(
                    runner.process_tick(store, Utc::now()),
                    f.respond(true, false)
                );
                server?;
                ensure!(tick?.inserted == 0);
            } else {
                ensure!(runner.process_tick(store, Utc::now()).await?.inserted == 0);
            }
            ensure!(f.count(&signal.signal_id)? == 0);
            ensure!(store.execution_quote_entry_blocked(&signal.signal_id, true)?);
        }
        f.f.save(&format!("r3-busy-completion-{existing}"), serde_json::json!({"priority":existing,"same_and_reopen":true,"orders":0,"decision":"owner_pending"}))?;
    }
    Ok(())
}

#[tokio::test]
async fn b70_r3_pending_a_does_not_starve_real_healthy_c_or_unstarted_shadow() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let f = Boundary::new().await?;
    let a = f.f.buy();
    let (completion, signal_a) = f.completed(&a, false, true, true).await?;
    let signal_a = signal_a.unwrap();
    let mut risk = ShadowRiskGuard::new(RiskConfig::default());
    let stop = OperatorEmergencyStop::from_env();
    let ready = f
        .runner
        .complete_hot_observed_buy_quote(
            &f.f.f.store,
            completion,
            &f.f.f.follow,
            false,
            &mut risk,
            &stop,
            true,
            Utc::now(),
            0,
            0,
        )
        .unwrap();
    f.f.f.store.set_busy_timeout(Duration::from_millis(1))?;
    let lock = f.f.f.conn()?;
    lock.execute_batch("BEGIN IMMEDIATE")?;
    let mut active = OperatorEmergencyStop::from_env();
    active.active = true;
    let result = f
        .runner
        .resume_hot_buy(
            &f.f.f.store,
            ready,
            &f.f.f.follow,
            false,
            &mut risk,
            &active,
            true,
            Utc::now(),
        )
        .await;
    lock.execute_batch("ROLLBACK")?;
    ensure!(format!("{:#}", result.unwrap_err()).contains("database is locked"));
    let mut c = f.f.buy();
    c.signature = "b70-real-c".into();
    c.token_out = "TokenC".into();
    c.slot += 1;
    // No Shadow worker has started for C when HTTP finishes. Permission is still a separate owner step.
    let (completion, none) = f.completed(&c, false, false, false).await?;
    ensure!(none.is_none());
    let id_c = completion.origin.signal_id();
    let ready = f
        .runner
        .complete_hot_observed_buy_quote(
            &f.f.f.store,
            completion,
            &f.f.f.follow,
            false,
            &mut risk,
            &stop,
            true,
            Utc::now(),
            0,
            0,
        )
        .unwrap();
    ensure!(f.f.f.store.execution_quote_entry_blocked(&id_c, true)?);
    f.runner
        .resume_hot_buy(
            &f.f.f.store,
            ready,
            &f.f.f.follow,
            false,
            &mut risk,
            &stop,
            true,
            Utc::now(),
        )
        .await?;
    let output = f.shadow(&c)?;
    let Ok(copybot_shadow::ShadowProcessOutcome::Recorded(signal_c)) = output.outcome else {
        unreachable!()
    };
    let selected = crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
        &f.f.f.store,
        &f.f.execution,
        "shadow_recorded",
        a.ts_utc,
    )?;
    ensure!(selected.len() == 1 && selected[0].signal_id == id_c);
    for _ in 0..2 {
        f.runner
            .process_recorded_shadow_signal(&f.f.f.store, &signal_c, Utc::now())
            .await?;
    }
    ensure!(f.count(&signal_a.signal_id)? == 0 && f.count(&id_c)? == 1);
    ensure!(f
        .f
        .f
        .store
        .execution_quote_entry_blocked(&signal_a.signal_id, true)?);
    f.sell_after_closed_buy().await?;
    ensure!(f.count(&signal_a.signal_id)? == 0 && f.count(&id_c)? == 1);
    f.f.save("r3-independent-c",serde_json::json!({"orders_a":0,"orders_c":1,"batch_limit":1,"shadow_started_after_http":true,"periodic_ticks":0}))?;
    Ok(())
}
