use super::initial_sol_rpc_fixture::FundingRpc;
use super::native_rpc_fixture::{Fixture as Rpc, Reply};
use super::priority_fee_route_fixture::{Fixture, Route};
use super::submit_refusal_fixture::{capture, check_event, seed_hot};
use anyhow::Result;
use serde_json::json;
use std::sync::{Arc, Mutex};

async fn scenario(retry: bool, case: &'static str) -> Result<()> {
    let mut f = Fixture::new(Route::Metis, 200_000, 200_000).await?;
    f.wire.lock().unwrap().guard = Some(50_000_001);
    f.config.canary_max_open_positions = 10;
    f.config.max_confirm_seconds = 1;
    if case == "loss" {
        f.store.record_execution_canary_open_position(
            "loss-source",
            "OtherLossMint",
            1.0,
            Some(copybot_core_types::TokenQuantity::new(1, 0)),
            0.02,
            f.now,
        )?;
    }
    let signal =
        if retry {
            f.build().await?.envelope.unwrap();
            f.store.mark_execution_canary_retry_after_submit_not_sent(&f.request.order_id, f.now,
            crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON)?;
            f.store
                .load_copy_signal_by_signal_id(&f.request.signal_id)?
                .unwrap()
        } else {
            seed_hot(&f).await?
        };
    let conn = Arc::new(Mutex::new(f.conn()?));
    let id = signal.signal_id.clone();
    let stamp = (f.now + chrono::Duration::seconds(1)).to_rfc3339();
    let saved = Arc::new(Mutex::new(None));
    let before = saved.clone();
    let server = Rpc::start(false, move |r| {
        if r["method"] == "simulateTransaction" {
            return Reply::json(json!({"jsonrpc":"2.0","id":r["id"],
                "result":{"context":{"slot":42},"value":{"err":null,"logs":[]}}}));
        }
        if r["method"] == "getFeeForMessage" {
            let c = conn.lock().unwrap();
            *before.lock().unwrap() = Some(c.query_row(
                "SELECT status,tx_signature,err_code,simulation_error,attempt FROM orders WHERE signal_id=?1",
                [&id], |row| Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?,
                    row.get::<_,Option<String>>(2)?,row.get::<_,Option<String>>(3)?,row.get::<_,u32>(4)?))).unwrap());
            match case {
                "changed" | "changed_error" => { c.execute("UPDATE copy_signals SET status='r1-current-state' WHERE signal_id=?1", [&id]).unwrap(); }
                "loss" => { c.execute("UPDATE positions SET state='closed',closed_ts=?1,pnl_lamports=-20000000,pnl_sol=-0.02 WHERE token='OtherLossMint'", [&stamp]).unwrap(); }
                "sql" => { c.execute("ALTER TABLE orders RENAME TO r1_delayed_orders", []).unwrap(); }
                "safety_sql" => { c.execute("ALTER TABLE positions RENAME TO r1_delayed_positions", []).unwrap(); }
                _ => {}
            }
        }
        let mut reply = Reply::json(FundingRpc::default().reply(r));
        if r["id"].as_str().is_some_and(|s| s.starts_with("native-funding-")) {
            reply.delay = std::time::Duration::from_millis(20);
        }
        if case == "changed_error" && r["method"] == "getFeeForMessage" {
            reply.body = b"{".to_vec();
        }
        reply
    }).await?;
    f.config.submit_adapter_http_url = server.endpoint.clone();
    let later = f.now + chrono::Duration::seconds(2);
    let mut times = vec![later; if retry { 2 } else { 1 }];
    times.push(if case == "clock" { f.now } else { later });
    let state = super::entry_risk_clock_fixture::sequence(times, async {
        if retry {
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &f.config, &f.store, f.now,
            )
            .await
            .map(Option::unwrap)
        } else {
            crate::execution_canary_route::process_canary_state_machine_for_route(
                &f.config, &f.store, &signal, f.now,
            )
            .await
        }
    })
    .await;
    f.finish().await?;
    let trace = server.finish().await?;
    if case == "sql" {
        f.conn()?
            .execute("ALTER TABLE r1_delayed_orders RENAME TO orders", [])?;
    }
    if case == "safety_sql" {
        f.conn()?
            .execute("ALTER TABLE r1_delayed_positions RENAME TO positions", [])?;
    }
    let state = state?;
    assert_eq!(state.failed, 0, "{case}/{retry}: {state:?}");
    assert_eq!(
        state.safety_blocked, 0,
        "refusal diagnostic must not invent a safety block"
    );
    assert_eq!(state.signing_envelope_built, 1);
    for method in [
        "getFeeForMessage",
        "getMultipleAccounts",
        "getMinimumBalanceForRentExemption",
    ] {
        assert_eq!(
            trace
                .iter()
                .filter(|r| r.request["method"] == method)
                .count(),
            1,
            "{case}/{retry}"
        );
    }
    let success = case == "success";
    assert_eq!(
        trace
            .iter()
            .filter(|r| r.request["method"] == "sendTransaction")
            .count(),
        usize::from(success)
    );
    let order = f
        .store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .unwrap();
    if !success {
        assert_eq!(
            (
                order.status.clone(),
                order.tx_signature.clone(),
                order.err_code.clone(),
                order.simulation_error.clone(),
                order.attempt
            ),
            saved.lock().unwrap().clone().unwrap(),
            "no destructive rewrite of A"
        );
    } else {
        assert!(order.tx_signature.is_some());
    }
    assert_eq!(
        f.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    let expected = match case {
        "changed" | "changed_error" => "initial_sol_order_changed",
        "loss" => "max_daily_loss",
        "clock" => "risk_decision_clock_unordered",
        "sql" => "tiny_submit_state_unavailable",
        "safety_sql" => "initial_sol_safety_unavailable",
        _ => "none",
    };
    let mut tick = crate::execution_canary::ExecutionCanaryTickSummary::default();
    crate::execution_canary_summary::apply_state_machine_summary(&mut tick, state);
    assert!(tick.has_status_change());
    // Unrelated generic error is never promoted to the typed event fields.
    tick.last_error = Some("unrelated SYNTHETIC_PRIVATE_PAYLOAD".into());
    let event = capture(|| crate::telemetry::record_execution_canary_tick(&tick));
    check_event(
        &event,
        if success { "none" } else { &order.order_id },
        expected,
        usize::from(!success),
    );
    let shadow = copybot_shadow::ShadowSignalResult {
        signal_id: signal.signal_id,
        wallet_id: signal.wallet_id,
        side: signal.side,
        token: signal.token,
        notional_sol: signal.notional_sol,
        latency_ms: 0,
        closed_qty: 0.0,
        realized_pnl_sol: 0.0,
        has_open_lots_after_signal: None,
    };
    let event = capture(|| crate::telemetry::record_execution_canary_shadow_signal(&tick, &shadow));
    check_event(
        &event,
        if success { "none" } else { &order.order_id },
        expected,
        usize::from(!success),
    );
    // Isolate the actual refusal from unrelated counters: it alone must cause emission.
    let only = crate::execution_canary::ExecutionCanaryTickSummary {
        pre_submit_refusals: tick.pre_submit_refusals.clone(),
        ..Default::default()
    };
    assert_eq!(only.has_status_change(), !success);
    let clean = crate::execution_canary::ExecutionCanaryTickSummary::default();
    assert!(!clean.has_status_change());
    check_event(
        &capture(|| crate::telemetry::record_execution_canary_tick(&clean)),
        "none",
        "none",
        0,
    );
    Ok(())
}

#[tokio::test]
async fn submit_refusal_actual_hot_changed_and_error_reach_both_events() -> Result<()> {
    for case in ["success", "changed", "changed_error"] {
        scenario(false, case).await?;
    }
    Ok(())
}
#[tokio::test]
async fn submit_refusal_actual_retry_changed_and_error_reach_both_events() -> Result<()> {
    for case in ["success", "changed", "changed_error"] {
        scenario(true, case).await?;
    }
    Ok(())
}
#[tokio::test]
async fn submit_refusal_actual_hot_and_retry_safety_sql_clock_labels() -> Result<()> {
    for retry in [false, true] {
        for case in ["loss", "clock", "sql", "safety_sql"] {
            scenario(retry, case).await?;
        }
    }
    Ok(())
}
