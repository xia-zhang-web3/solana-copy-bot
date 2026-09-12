use super::{b64_http_fixture as http, b70_fixture::Fixture, *};
use crate::shadow_scheduler::{ShadowSwapSide, ShadowTaskKey, ShadowTaskOutput};
use anyhow::{ensure, Result};
use copybot_shadow::ShadowProcessOutcome;

async fn owner_case(case: &str) -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut f = Fixture::new(&format!("http://{}", listener.local_addr()?), true).await?;
    if case == "root_late_stop" {
        f.execution.priority_fee_canary_enabled = true;
        f.execution.priority_fee_canary_rpc_url = format!("http://{}", listener.local_addr()?);
        f.execution.priority_fee_canary_timeout_ms = 2_000;
        f.execution.canary_buy_size_sol = f.execution.quote_canary_buy_size_sol;
    }
    let a = f.buy();
    f.f.store.insert_observed_swap(&a)?;
    let runner = ExecutionCanaryRunner::new(f.execution.clone());
    let mut scheduler = ShadowScheduler::new();
    runner.admit_hot_observed_buy_quote(&f.f.store, &a, Utc::now(), &mut scheduler)?;
    let requests = http::pair(&listener).await?;
    let mut risk = ShadowRiskGuard::new(RiskConfig::default());
    ensure!(matches!(
        risk.can_open_buy_for_signal(
            &f.f.store,
            Utc::now(),
            true,
            Some(&a.wallet),
            Some(&a.token_out)
        ),
        BuyRiskDecision::Allow
    ));
    let mut root_quality = permissive_shadow_quality();
    if case == "root_rounding" {
        root_quality.copy_notional_sol = 0.067;
    }
    let (outcome, receipt) = ShadowService::new(root_quality).process_swap_with_buy_receipt(
        &f.f.store,
        &a,
        &f.f.follow,
        Utc::now(),
    )?;
    let ShadowProcessOutcome::Recorded(signal) = &outcome else {
        anyhow::bail!("shadow not recorded")
    };
    let signal = signal.clone();
    ensure!(receipt.is_some());
    let key = ShadowTaskKey {
        wallet: a.wallet.clone(),
        token: a.token_out.clone(),
    };
    scheduler.inflight_shadow_keys.insert(key.clone());
    let mut output = ShadowTaskOutput {
        signature: a.signature.clone(),
        key: key.clone(),
        signal_id: Some(signal.signal_id.clone()),
        side: Some(ShadowSwapSide::Buy),
        buy_receipt: receipt,
        owned_sell_reject: None,
        outcome: Ok(outcome),
    };
    for request in requests {
        let body = request.quote();
        request.reply(200, body).await?;
    }
    if case == "root_late_stop" {
        let fee = http::accept(&listener).await?;
        ensure!(fee.body["method"] == "qn_estimatePriorityFees");
        fee.reply(
            200,
            serde_json::json!({"jsonrpc":"2.0","result":{"recommended":0}}),
        )
        .await?;
    }
    scheduler.hot_quotes.collect_next().await;
    // The insert may precede JoinSet delivery. Do not infer provenance in that gap.
    ensure!(!scheduler.hot_completion_ready());
    if case == "unproven" {
        output.buy_receipt = None;
        risk.config.shadow_max_open_lots_per_token = 10;
    }
    scheduler.hot_quotes.note_shadow_output(&output);
    scheduler.mark_task_complete(&key);
    ensure!(scheduler.hot_completion_ready());
    let mut follow = (*f.f.follow).clone();
    let mut stop = OperatorEmergencyStop::from_env();
    let mut now = Utc::now();
    match case {
        "foreign" | "same_wallet" => {
            let wallet = if case == "same_wallet" {
                &a.wallet
            } else {
                "other-leader"
            };
            f.f.store
                .insert_shadow_lot(wallet, &a.token_out, 1.0, 0.01, a.ts_utc)?;
        }
        "unknown" => {
            f.f.conn()?.execute(
                "UPDATE shadow_lots SET cost_lamports=NULL WHERE token='TokenA'",
                [],
            )?;
        }
        "changed_signal" => {
            f.f.conn()?.execute(
                "UPDATE copy_signals SET notional_lamports=notional_lamports+1 WHERE signal_id=?1",
                [&signal.signal_id],
            )?;
        }
        "limit" => {
            risk.config.shadow_max_open_notional_per_token_sol = 0.000000001;
        }
        "global_limit" => {
            risk.config.shadow_hard_exposure_cap_sol = 0.000000001;
        }
        "outage" => {
            risk.infra_block_reason = Some("fixture outage".into());
        }
        "unfollow" => {
            follow.active.clear();
        }
        "temporal_unfollow" => {
            f.f.store
                .deactivate_follow_wallet(&a.wallet, now, "fixture")?;
        }
        "stop" => {
            stop.active = true;
        }
        "expiry" => {
            now = a.ts_utc + chrono::Duration::seconds(6);
        }
        "drawdown" => {
            // Same refresh window: a cached healthy guard must see the new loss.
            f.f.store.insert_shadow_closed_trade_exact_with_context(
                "b70-loss",
                "foreign",
                "LossMint",
                1.0,
                None,
                100.0,
                0.0,
                -100.0,
                copybot_storage_core::SHADOW_CLOSE_CONTEXT_MARKET,
                now - chrono::Duration::seconds(10),
                now,
            )?;
        }
        _ => {}
    }
    let ready = runner.complete_hot_observed_buy_quote(
        &f.f.store,
        scheduler.hot_quotes.take_completion().unwrap(),
        &follow,
        case == "publication",
        &mut risk,
        &stop,
        true,
        now,
        0,
        0,
    );
    let event =
        f.f.store
            .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
            .unwrap();
    ensure!(event.request_ts <= Utc::now() && event.signal_ts == Some(a.ts_utc));
    if case == "root_rounding" {
        let signal_cost: i64 = f.f.conn()?.query_row(
            "SELECT notional_lamports FROM copy_signals WHERE signal_id=?1",
            [&signal.signal_id],
            |r| r.get(0),
        )?;
        let lot_cost: i64 = f.f.conn()?.query_row(
            "SELECT cost_lamports FROM shadow_lots WHERE token='TokenA'",
            [],
            |r| r.get(0),
        )?;
        f.save("root-r1-rounding", serde_json::json!({"signal_cost":signal_cost,"lot_cost":lot_cost,"quote_status":event.quote_status,"error":event.error,"ready":ready.is_some()}))?;
    }
    if ["healthy", "resume_stop", "root_rounding", "root_late_stop"].contains(&case) {
        ensure!(
            event.quote_status == "ok" && ready.is_some(),
            "{case}: {event:?}"
        );
        ensure!(
            runner
                .process_recorded_shadow_signal(&f.f.store, &signal, Utc::now())
                .await?
                .skipped_reason
                == Some("hot_quote_pending")
        );
        if case == "root_late_stop" {
            let eligible =
                crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
                    &f.f.store,
                    &f.execution,
                    "shadow_recorded",
                    a.ts_utc,
                )?;
            ensure!(
                eligible.is_empty() && event.decision_status.as_deref() == Some("owner_pending"),
                "successful network quote awaits the owner decision"
            );
        }
        if case == "resume_stop" || case == "root_late_stop" {
            stop.active = true;
        }
        runner
            .resume_hot_buy(
                &f.f.store,
                ready.unwrap(),
                &follow,
                false,
                &mut risk,
                &stop,
                true,
                Utc::now(),
            )
            .await?;
        let count: i64 = f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [&signal.signal_id],
            |r| r.get(0),
        )?;
        ensure!(
            count == i64::from(case == "healthy" || case == "root_rounding"),
            "{case}"
        );
        if case == "healthy" {
            runner
                .process_recorded_shadow_signal(&f.f.store, &signal, Utc::now())
                .await?;
            runner.process_tick(&f.f.store, Utc::now()).await?;
            let after: i64 = f.f.conn()?.query_row(
                "SELECT count(*) FROM orders WHERE signal_id=?1",
                [&signal.signal_id],
                |r| r.get(0),
            )?;
            ensure!(after == 1, "replay/tick must remain once");
        }
    } else {
        ensure!(
            ready.is_none()
                && event.quote_status == "ok"
                && event.error.is_none()
                && copybot_storage_core::execution_quote_entry_is_refused(&event),
            "{case}: {event:?}"
        );
        let orders: i64 = f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [&signal.signal_id],
            |r| r.get(0),
        )?;
        ensure!(orders == 0);
        // Releasing the claim does not revoke the durable terminal refusal.
        runner.admit_hot_observed_buy_quote(&f.f.store, &a, Utc::now(), &mut scheduler)?;
        // The refusal is final at this admission seam (no additional network).
        ensure!(scheduler.active_task_count() == 0);
    }
    http::no_more(&listener).await?;
    scheduler.hot_quotes.shutdown().await;
    if case == "root_late_stop" {
        let before: i64 = f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [&signal.signal_id],
            |r| r.get(0),
        )?;
        let selected = crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
            &f.f.store,
            &f.execution,
            "shadow_recorded",
            a.ts_utc,
        )?;
        let tick = runner.process_tick(&f.f.store, Utc::now()).await?;
        let after: i64 = f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [&signal.signal_id],
            |r| r.get(0),
        )?;
        f.save("root-r1-late-stop", serde_json::json!({"before":before,"after_tick":after,"blueprint_selected_after_refusal":selected.len(),"operator_stop_active":stop.is_active(),"tick_inserted":tick.inserted,"tick_reason":tick.skipped_reason}))?;
        ensure!(
            after == 0 && selected.is_empty(),
            "late owner refusal must survive ordinary tick and blueprint selection; before={before}, after={after}, selected={}", selected.len()
        );
    }
    Ok(())
}

#[tokio::test]
async fn b70_r1_owner_own_lot_and_once() -> Result<()> {
    owner_case("healthy").await
}
#[tokio::test]
async fn b70_r1_owner_foreign_exposure_and_no_inferred_proof() -> Result<()> {
    for case in [
        "foreign",
        "same_wallet",
        "unknown",
        "unproven",
        "changed_signal",
    ] {
        owner_case(case).await?;
    }
    Ok(())
}
#[tokio::test]
async fn b70_r1_owner_current_limits_drawdown_outage_and_authority() -> Result<()> {
    for case in [
        "limit",
        "global_limit",
        "drawdown",
        "outage",
        "unfollow",
        "temporal_unfollow",
        "stop",
        "publication",
        "expiry",
        "resume_stop",
    ] {
        owner_case(case).await?;
    }
    Ok(())
}

#[tokio::test]
async fn b70_r1_root_rounding_does_not_refuse_own_insert() -> Result<()> {
    owner_case("root_rounding").await
}
#[tokio::test]
async fn b70_r1_root_late_owner_refusal_survives_tick() -> Result<()> {
    owner_case("root_late_stop").await
}
#[tokio::test]
async fn b70_r1_root_healthy_owner_control() -> Result<()> {
    owner_case("healthy").await
}
