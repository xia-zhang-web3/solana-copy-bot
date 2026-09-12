use super::{b70_r2_fixture::Ready, *};
use anyhow::{ensure, Result};

async fn refusal_case(case: &str, http_first: bool) -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let mut f = Ready::new(0.067, http_first).await?;
    let mut follow = (*f.f.f.follow).clone();
    let mut stop = OperatorEmergencyStop::from_env();
    match case {
        "stop" => stop.active = true,
        "follow" => follow.active.clear(),
        "risk" => f.risk.config.shadow_max_open_notional_per_token_sol = 0.000000001,
        "identity" => {
            f.f.f.conn()?.execute(
                "UPDATE copy_signals SET notional_lamports=notional_lamports+1 WHERE signal_id=?1",
                [&f.signal.signal_id],
            )?;
        }
        _ => unreachable!(),
    }
    f.runner
        .resume_hot_buy(
            &f.f.f.store,
            f.origin.take().unwrap(),
            &follow,
            false,
            &mut f.risk,
            &stop,
            true,
            Utc::now(),
        )
        .await?;
    let event =
        f.f.f
            .store
            .load_latest_execution_quote_canary_entry_event(&f.signal.signal_id)?
            .unwrap();
    ensure!(event.decision_status.as_deref() == Some("would_skip"));
    let expected_reason = match case {
        "stop" => "hot_quote_operator_stop",
        "follow" => "hot_quote_source_changed",
        "risk" => "risk_exposure_hard_cap",
        "identity" => "risk_fail_closed",
        _ => unreachable!(),
    };
    ensure!(
        event.decision_reason.as_deref() == Some(&format!("hot_buy_refused:{expected_reason}")),
        "{case}: {:?}",
        event.decision_reason
    );
    ensure!(!f.orders()?);
    f.unchanged_quote_facts()?;
    // Re-delivery, several ordinary ticks and a fresh runner/store cannot reuse the old quote.
    // Clearing the external trigger is not an implicit new permission for A either.
    for reopen in [false, true] {
        let reopened = SqliteStore::open(&f.f.f.path)?;
        let fresh = ExecutionCanaryRunner::new(f.f.execution.clone());
        let (store, runner) = if reopen {
            (&reopened, &fresh)
        } else {
            (&f.f.f.store, &f.runner)
        };
        for _ in 0..2 {
            let recorded = runner
                .process_recorded_shadow_signal(store, &f.signal, Utc::now())
                .await?;
            ensure!(recorded.skipped_reason == Some("hot_buy_refused"));
            runner.process_tick(store, Utc::now()).await?;
            ensure!(!f.orders()?);
            ensure!(
                crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
                    store,
                    &f.f.execution,
                    "shadow_recorded",
                    f.swap.ts_utc
                )?
                .is_empty()
            );
        }
        let quotes =
            crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.f.execution.clone());
        ensure!(!quotes.entry_pending(&f.signal.signal_id));
        ensure!(quotes
            .prepare_hot_quote(store, &f.swap, Utc::now())?
            .is_none());
        quotes
            .process_recorded_shadow_signal(store, &f.signal, Utc::now())
            .await?;
        // Old successful payload replay must be idempotent, not an allow transition.
        store.record_execution_quote_canary_event(&f.quote)?;
        ensure!(store.execution_quote_entry_refused(&f.signal.signal_id)?);
    }
    // A terminal A does not consume the sole candidate slot in front of healthy C.
    let mut c =
        f.f.f
            .store
            .load_copy_signal_by_signal_id(&f.signal.signal_id)?
            .unwrap();
    c.signal_id = "shadow:b70-healthy-c:leader-a:buy:TokenC".into();
    c.token = "TokenC".into();
    c.ts = Utc::now();
    f.f.f.store.insert_copy_signal(&c)?;
    let mut quote_c = f.quote.clone();
    quote_c.signal_id = Some(c.signal_id.clone());
    quote_c.event_id = format!("quote:entry:{}", c.signal_id);
    quote_c.token = c.token.clone();
    quote_c.signal_ts = Some(c.ts);
    f.f.f.store.record_execution_quote_canary_event(&quote_c)?;
    let stored_c =
        f.f.f
            .store
            .load_execution_quote_canary_event_by_id(&quote_c.event_id)?
            .unwrap();
    f.f.f
        .store
        .complete_execution_quote_entry_owner(&stored_c)?;
    let selected = crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
        &f.f.f.store,
        &f.f.execution,
        "shadow_recorded",
        f.swap.ts_utc,
    )?;
    ensure!(selected.len() == 1 && selected[0].signal_id == c.signal_id);
    let tick = f.runner.process_tick(&f.f.f.store, Utc::now()).await?;
    ensure!(tick.inserted == 1 && !f.orders()?);
    ensure!(
        f.f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [&c.signal_id],
            |r| r.get::<_, i64>(0)
        )? == 1
    );
    f.unchanged_quote_facts()?;
    f.f.save(&format!("r2-late-{case}-{http_first}"), serde_json::json!({"refused_a":f.signal.signal_id,"reason":event.decision_reason,"orders_a":f.orders()?,"candidate_c":c.signal_id,"reopen":true}))?;
    Ok(())
}

#[tokio::test]
async fn b70_r2_late_stop_follow_risk_identity_survive_replay_reopen() -> Result<()> {
    for case in ["stop", "follow", "risk", "identity"] {
        refusal_case(case, false).await?;
    }
    refusal_case("stop", true).await
}

#[tokio::test]
async fn b70_r2_rounding_both_completion_orders_and_once() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    for http_first in [false, true] {
        let mut f = Ready::new(0.067, http_first).await?;
        let signal =
            f.f.f
                .store
                .load_copy_signal_by_signal_id(&f.signal.signal_id)?
                .unwrap();
        let lot =
            f.f.f
                .store
                .list_shadow_lots(&f.swap.wallet, &f.swap.token_out)?
                .pop()
                .unwrap();
        ensure!(signal.notional_lamports.unwrap().as_u64() == 67_000_000);
        ensure!(lot.cost_lamports.unwrap().as_u64() == 67_000_001);
        let stop = OperatorEmergencyStop::from_env();
        f.runner
            .resume_hot_buy(
                &f.f.f.store,
                f.origin.take().unwrap(),
                &f.f.f.follow,
                false,
                &mut f.risk,
                &stop,
                true,
                Utc::now(),
            )
            .await?;
        for _ in 0..2 {
            f.runner
                .process_recorded_shadow_signal(&f.f.f.store, &f.signal, Utc::now())
                .await?;
            f.runner.process_tick(&f.f.f.store, Utc::now()).await?;
        }
        ensure!(f.orders()?);
        ensure!(
            f.f.f.conn()?.query_row(
                "SELECT count(*) FROM orders WHERE signal_id=?1",
                [&f.signal.signal_id],
                |r| r.get::<_, i64>(0)
            )? == 1
        );
        ensure!(!f
            .f
            .f
            .store
            .execution_quote_entry_refused(&f.signal.signal_id)?);
        ensure!(
            f.f.f
                .store
                .list_shadow_lots(&f.swap.wallet, &f.swap.token_out)?
                .pop()
                .unwrap()
                .cost_lamports
                == lot.cost_lamports
        );
        f.unchanged_quote_facts()?;
        f.f.save(&format!("r2-rounding-{http_first}"), serde_json::json!({"signal_cost":67000000,"lot_cost":67000001,"orders":1,"http_first":http_first}))?;
    }
    Ok(())
}

#[tokio::test]
async fn b70_r2_root_sqlite_busy_must_not_restore_permission() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let mut observations = Vec::new();
    for reopen in [false, true] {
        let mut f = Ready::new(0.067, false).await?;
        f.f.f
            .store
            .set_busy_timeout(std::time::Duration::from_millis(1))?;
        let lock = f.f.f.conn()?;
        ensure!(lock.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))? == "wal");
        lock.execute_batch("BEGIN IMMEDIATE")?;
        let mut stop = OperatorEmergencyStop::from_env();
        stop.active = true;
        let started = std::time::Instant::now();
        let outcome = f
            .runner
            .resume_hot_buy(
                &f.f.f.store,
                f.origin.take().unwrap(),
                &f.f.f.follow,
                false,
                &mut f.risk,
                &stop,
                true,
                Utc::now(),
            )
            .await;
        let elapsed_ms = started.elapsed().as_millis();
        lock.execute_batch("ROLLBACK")?;
        let error = outcome.expect_err("real held WAL write lock must refuse the UPDATE");
        ensure!(
            format!("{error:#}").contains("database is locked"),
            "unexpected error: {error:#}"
        );
        ensure!(!f.orders()?);
        let opened = SqliteStore::open(&f.f.f.path)?;
        let fresh = ExecutionCanaryRunner::new(f.f.execution.clone());
        let (store, runner) = if reopen {
            (&opened, &fresh)
        } else {
            (&f.f.f.store, &f.runner)
        };
        let selected = crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
            store,
            &f.f.execution,
            "shadow_recorded",
            f.swap.ts_utc,
        )?;
        let summary = runner.process_tick(store, Utc::now()).await?;
        f.unchanged_quote_facts()?;
        let observation = serde_json::json!({"reopen":reopen,"error":format!("{error:#}"),
            "busy_elapsed_ms":elapsed_ms,"stop_active":stop.is_active(),"selected":selected.len(),
            "has_order":f.orders()?,"tick_inserted":summary.inserted,
            "durable_refused":store.execution_quote_entry_refused(&f.signal.signal_id)?,
            "quote_decision":store.load_latest_execution_quote_canary_entry_event(&f.signal.signal_id)?.unwrap().decision_status});
        f.f.save(&format!("root-r2-busy-{reopen}"), observation.clone())?;
        observations.push(observation);
    }
    ensure!(
        observations
            .iter()
            .all(|v| v["selected"] == 0 && v["has_order"] == false),
        "failed durable refusal must stay closed through tick/reopen: {observations:?}"
    );
    Ok(())
}
