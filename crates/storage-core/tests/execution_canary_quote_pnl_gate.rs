use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

#[test]
fn quote_readiness_gate_ignores_skipped_priority_fee_statuses() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-gate-skipped-priority")?;
    let opened = ts("2026-06-02T13:00:00Z");

    for index in 0..34 {
        let token = format!("GateToken{index}");
        let signal_id = format!("sell-gate-{index}");
        let buy_decision = if index < 4 {
            "would_skip"
        } else {
            "would_execute"
        };
        let priority_status = if index < 4 { "skipped" } else { "ok" };
        let trade_opened = opened + Duration::seconds(index);
        let closed = trade_opened + Duration::seconds(30);

        store.insert_shadow_closed_trade_exact(
            &signal_id,
            "leader-wallet",
            &token,
            50.0,
            Some(TokenQuantity::new(50, 0)),
            0.10,
            0.13,
            0.03,
            trade_opened,
            closed,
        )?;
        let close_id = store
            .list_execution_quote_canary_close_candidates_for_signal(&signal_id, 10)?
            .into_iter()
            .next()
            .expect("close candidate")
            .id;
        store.record_execution_quote_canary_event(&quote_event(
            format!("quote:buy:gate:{index}"),
            Some(format!("buy-gate-{index}")),
            None,
            &token,
            "buy",
            trade_opened,
            buy_decision,
            priority_status,
        ))?;
        store.record_execution_quote_canary_event(&quote_event(
            format!("quote:sell:gate:{index}"),
            Some(format!("sell:{close_id}")),
            Some(close_id),
            &token,
            "sell",
            trade_opened + Duration::seconds(30),
            "would_execute",
            priority_status,
        ))?;
    }

    let summary = store.execution_canary_quote_pnl_summary(
        opened + Duration::seconds(120),
        opened - Duration::seconds(1),
        100,
    )?;

    assert_eq!(summary.total_closed_trades, 34);
    assert_eq!(summary.pnl_counted_trades, 30);
    assert_eq!(summary.skipped_trades, 4);
    assert_eq!(summary.readiness_gate.status, "ready");
    assert!(summary.readiness_gate.can_start_tiny_execution);
    assert_eq!(summary.readiness_gate.blocker_count, 0);
    assert_close(summary.readiness_gate.non_ok_priority_fee_rate_pct, 0.0);
    Ok(())
}

#[test]
fn quote_readiness_gate_reports_candidate_threshold_without_unblocking() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-gate-candidate")?;
    let opened = ts("2026-06-02T14:00:00Z");

    for index in 0..38 {
        let token = format!("CandidateToken{index}");
        let signal_id = format!("sell-candidate-{index}");
        let skipped = index < 8;
        let trade_opened = opened + Duration::seconds(index);
        let closed = trade_opened + Duration::seconds(30);

        store.insert_shadow_closed_trade_exact(
            &signal_id,
            "leader-wallet",
            &token,
            50.0,
            Some(TokenQuantity::new(50, 0)),
            0.10,
            0.13,
            0.03,
            trade_opened,
            closed,
        )?;
        let close_id = store
            .list_execution_quote_canary_close_candidates_for_signal(&signal_id, 10)?
            .into_iter()
            .next()
            .expect("close candidate")
            .id;
        store.record_execution_quote_canary_event(&quote_event_with_slippage(
            format!("quote:buy:candidate:{index}"),
            Some(format!("buy-candidate-{index}")),
            None,
            &token,
            "buy",
            trade_opened,
            if skipped {
                "would_skip"
            } else {
                "would_execute"
            },
            "ok",
            if skipped { 700.0 } else { 15.0 },
        ))?;
        store.record_execution_quote_canary_event(&quote_event(
            format!("quote:sell:candidate:{index}"),
            Some(format!("sell:{close_id}")),
            Some(close_id),
            &token,
            "sell",
            trade_opened + Duration::seconds(30),
            "would_execute",
            "ok",
        ))?;
    }

    let summary = store.execution_canary_quote_pnl_summary(
        opened + Duration::seconds(120),
        opened - Duration::seconds(1),
        100,
    )?;

    assert_eq!(summary.readiness_gate.status, "blocked");
    assert!(!summary.readiness_gate.can_start_tiny_execution);
    let candidate = summary
        .readiness_gate
        .threshold_candidate
        .expect("threshold candidate");
    assert_eq!(candidate.threshold_bps, 1000);
    assert_eq!(candidate.counted_trades, 38);
    assert_eq!(candidate.skipped_trades, 0);
    assert!(candidate.clears_skip_rate_blocker);
    assert!(candidate.pnl_positive);
    assert!(summary
        .readiness_gate
        .checks
        .iter()
        .any(|check| check.name == "candidate_1000_bps" && check.status == "warn"));
    Ok(())
}

fn quote_event(
    event_id: String,
    signal_id: Option<String>,
    shadow_closed_trade_id: Option<i64>,
    token: &str,
    side: &str,
    signal_ts: DateTime<Utc>,
    decision_status: &str,
    priority_fee_status: &str,
) -> ExecutionQuoteCanaryEventInsert {
    quote_event_with_slippage(
        event_id,
        signal_id,
        shadow_closed_trade_id,
        token,
        side,
        signal_ts,
        decision_status,
        priority_fee_status,
        15.0,
    )
}

fn quote_event_with_slippage(
    event_id: String,
    signal_id: Option<String>,
    shadow_closed_trade_id: Option<i64>,
    token: &str,
    side: &str,
    signal_ts: DateTime<Utc>,
    decision_status: &str,
    priority_fee_status: &str,
    slippage_bps: f64,
) -> ExecutionQuoteCanaryEventInsert {
    let (quote_in_amount_raw, quote_out_amount_raw, leader_notional_sol) = if side == "sell" {
        ("50", "125000000", 0.125)
    } else {
        ("200000000", "100", 0.2)
    };

    ExecutionQuoteCanaryEventInsert {
        event_id,
        signal_id,
        shadow_closed_trade_id,
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: side.to_string(),
        quote_status: "ok".to_string(),
        request_ts: signal_ts + Duration::milliseconds(10),
        signal_ts: Some(signal_ts),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(leader_notional_sol),
        quote_in_amount_raw: Some(quote_in_amount_raw.to_string()),
        quote_out_amount_raw: Some(quote_out_amount_raw.to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.002),
        shadow_price_sol: Some(0.002),
        slippage_bps: Some(slippage_bps),
        price_impact_pct: Some(0.02),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some(priority_fee_status.to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some("{\"recommended\":10000}".to_string()),
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some("inside_test_limit".to_string()),
        error: None,
    }
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
}
