use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionQuoteCanaryEventInsert, SqliteStore,
};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};

struct Fixture {
    dir: TempDir,
    store: SqliteStore,
    now: DateTime<Utc>,
}

impl Fixture {
    fn new() -> Result<Self> {
        let dir = tempdir()?;
        let mut store = SqliteStore::open(dir.path().join("fees.db"))?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        Ok(Self {
            dir,
            store,
            now: "2026-09-04T12:00:00Z".parse()?,
        })
    }

    fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(self.dir.path().join("fees.db"))?;
        Ok(())
    }

    fn report(&self) -> Result<ExecutionCanaryQuotePnlSummary> {
        self.store
            .execution_canary_quote_pnl_summary(self.now, self.now - Duration::hours(2), 100)
    }

    fn round_trip(
        &self,
        index: i64,
        buy_fee: Option<u64>,
        sell_fee: Option<u64>,
        skipped: bool,
    ) -> Result<()> {
        let opened = self.now - Duration::hours(1) + Duration::seconds(index);
        let closed = opened + Duration::seconds(30);
        let signal = format!("shadow-{index}");
        let token = format!("Token{index}");
        self.store.insert_shadow_closed_trade(
            &signal, "wallet", &token, 1000.0, 0.2, 0.21, 0.01, opened, closed,
        )?;
        let close_id = self
            .store
            .list_execution_quote_canary_close_candidates_for_signal(&signal, 1)?[0]
            .id;
        let buy_signal = format!("buy-{index}");
        for (side, fee) in [("buy", buy_fee), ("sell", sell_fee)] {
            let buy = side == "buy";
            let request_ts = if buy { opened } else { closed };
            self.store.record_execution_quote_canary_event(&ExecutionQuoteCanaryEventInsert {
                http_request_started_ts: Some(request_ts + Duration::milliseconds(10)),
                quote_response_available_ts: None,
                event_id: if buy { format!("quote:entry:{buy_signal}") } else { format!("sell-{index}") },
                signal_id: Some(if buy { buy_signal.clone() } else { signal.clone() }),
                shadow_closed_trade_id: (!buy).then_some(close_id),
                wallet_id: "wallet".into(), token: token.clone(), side: side.into(),
                quote_status: "ok".into(), request_ts, signal_ts: Some(request_ts),
                decision_delay_ms: Some(10), quote_latency_ms: Some(20), leader_notional_sol: Some(0.2),
                quote_in_amount_raw: Some(if buy { "10000000" } else { "1000" }.into()),
                quote_out_amount_raw: Some(if buy { "1000" } else { "10100000" }.into()),
                quote_response_json: None, quote_price_sol: None, shadow_price_sol: None,
                slippage_bps: Some(if buy && skipped { 700.0 } else { 15.0 }), price_impact_pct: None,
                route_plan_json: Some(r#"[{"swapInfo":{"label":"Metis"}}]"#.into()),
                priority_fee_status: Some("ok".into()), priority_fee_lamports: fee,
                priority_fee_json: Some(r#"{"version":1,"source":"qn_estimatePriorityFees","api_version":2,"field":"recommended","unit":"micro_lamports_per_compute_unit","value":600000}"#.into()),
                decision_status: Some(if buy && skipped { "would_skip" } else { "would_execute" }.into()),
                decision_reason: Some(if skipped { "slippage" } else { "test" }.into()), error: None,
            })?;
        }
        self.store.record_execution_quote_canary_shadow_gate_event(
            &buy_signal,
            "wallet",
            &token,
            "buy",
            "shadow_recorded",
            None,
            opened + Duration::milliseconds(15),
        )?;
        Ok(())
    }
}

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-10,
        "actual={actual} expected={expected}"
    );
}

fn assert_incomplete(report: &ExecutionCanaryQuotePnlSummary) -> Result<Value> {
    let json = serde_json::to_value(report)?;
    assert!(
        json["quote_adjusted_pnl_after_priority_fee_sol"].is_null(),
        "net must be unknown: {json}"
    );
    assert!(json["priority_fee_lamports_sum"].is_null());
    assert!(!report.readiness_gate.can_start_tiny_execution);
    assert_eq!(report.readiness_gate.status, "blocked");
    assert!(json["readiness_gate"]["quote_after_fee_pnl_sol"].is_null());
    Ok(json)
}

#[test]
fn null_fee_round_trips_remain_unknown_after_reopen() -> Result<()> {
    let mut fixture = Fixture::new()?;
    for i in 0..30 {
        fixture.round_trip(i, None, None, false)?;
    }
    for _ in 0..2 {
        let report = fixture.report()?;
        assert_eq!(
            report.pnl_counted_trades, 0,
            "unknown fees cannot count as net samples"
        );
        assert_eq!(report.unknown_trades, 30);
        assert_eq!(report.unknown_priority_fee_trades, 30);
        assert_eq!(report.gross_pnl_counted_trades, 30);
        close(report.quote_adjusted_pnl_sol, 0.003);
        assert_incomplete(&report)?;
        for trade in &report.trades {
            close(
                trade.quote_adjusted_pnl_sol.expect("gross survives"),
                0.0001,
            );
            assert!(trade.quote_adjusted_pnl_after_priority_fee_sol.is_none());
            assert!(trade.quote_after_fee_vs_shadow_delta_sol.is_none());
            assert!(trade.priority_fee_lamports_total.is_none());
        }
        let feedback = fixture
            .store
            .executable_wallet_feedback_since(fixture.now - Duration::hours(2))?;
        assert_eq!(feedback.get("wallet").map_or(0, |f| f.samples), 0);
        assert_eq!(feedback["wallet"].unknown_samples, 30);
        assert!(feedback["wallet"]
            .complete_pnl_after_priority_fee_sol()
            .is_none());
        assert!(feedback["wallet"].flip_rate().is_none());
        fixture.reopen()?;
    }
    Ok(())
}

#[test]
fn either_unknown_side_prevents_a_known_net() -> Result<()> {
    for (buy, sell) in [
        (None, Some(0)),
        (Some(0), None),
        (None, Some(120000)),
        (Some(120000), None),
    ] {
        let fixture = Fixture::new()?;
        fixture.round_trip(0, buy, sell, false)?;
        let report = fixture.report()?;
        assert_eq!(report.pnl_counted_trades, 0);
        assert_incomplete(&report)?;
        let feedback = fixture
            .store
            .executable_wallet_feedback_since(fixture.now - Duration::hours(2))?;
        assert_eq!(feedback.get("wallet").map_or(0, |f| f.samples), 0);
    }
    Ok(())
}

#[test]
fn explicit_zero_and_known_paid_fees_keep_exact_results() -> Result<()> {
    for (fee, expected, ready) in [(0, 0.003, true), (120000, -0.0042, false)] {
        let fixture = Fixture::new()?;
        for i in 0..30 {
            fixture.round_trip(i, Some(fee), Some(fee), false)?;
        }
        let report = fixture.report()?;
        let value = serde_json::to_value(&report)?;
        assert_eq!(report.pnl_counted_trades, 30);
        assert_eq!(report.unknown_trades, 0);
        close(
            value["quote_adjusted_pnl_after_priority_fee_sol"]
                .as_f64()
                .expect("known net"),
            expected,
        );
        assert_eq!(value["priority_fee_lamports_sum"], json!(fee * 60));
        assert_eq!(report.readiness_gate.can_start_tiny_execution, ready);
        let feedback = fixture
            .store
            .executable_wallet_feedback_since(fixture.now - Duration::hours(2))?;
        let feedback = &feedback["wallet"];
        assert_eq!(feedback.samples, 30);
        assert_eq!(feedback.unknown_samples, 0);
        close(
            feedback
                .complete_pnl_after_priority_fee_sol()
                .expect("complete feedback"),
            expected,
        );
        close(feedback.known_sample_pnl_after_priority_fee_sol, expected);
        assert_eq!(
            feedback.shadow_positive_executable_negative,
            if fee == 0 { 0 } else { 30 }
        );
    }
    Ok(())
}

#[test]
fn mixed_cohort_does_not_hide_unknown_rows_in_aggregates() -> Result<()> {
    let mut fixture = Fixture::new()?;
    for i in 0..30 {
        fixture.round_trip(i, Some(0), Some(0), false)?;
    }
    // The query reads newest closes first: exercise unknown before AND after known rows.
    fixture.round_trip(-1, None, None, false)?;
    fixture.round_trip(30, None, None, false)?;
    fixture.reopen()?;
    let report = fixture.report()?;
    let value = assert_incomplete(&report)?;
    assert_eq!(report.pnl_counted_trades, 30);
    assert_eq!(report.unknown_trades, 2);
    for threshold in value["threshold_summaries"].as_array().unwrap() {
        assert_eq!(threshold["unknown_trades"], 2);
        assert!(threshold["quote_adjusted_pnl_after_priority_fee_sol"].is_null());
    }
    for key in [
        "buy_slippage_buckets",
        "entry_decision_delay_buckets",
        "buy_leader_notional_buckets",
    ] {
        let bucket = &value[key][0];
        assert_eq!(bucket["trades"], 32);
        assert_eq!(bucket["after_fee_known_trades"], 30);
        assert_eq!(bucket["after_fee_unknown_trades"], 2);
        assert!(bucket["quote_adjusted_pnl_after_priority_fee_sol"].is_null());
    }
    assert!(
        !report
            .readiness_gate
            .threshold_candidate
            .as_ref()
            .unwrap()
            .pnl_positive
    );
    let feedback = fixture
        .store
        .executable_wallet_feedback_since(fixture.now - Duration::hours(2))?;
    assert_eq!(feedback["wallet"].samples, 30);
    assert_eq!(feedback["wallet"].unknown_samples, 2);
    close(
        feedback["wallet"].known_sample_pnl_after_priority_fee_sol,
        0.003,
    );
    assert!(feedback["wallet"]
        .complete_pnl_after_priority_fee_sol()
        .is_none());
    Ok(())
}

#[test]
fn skipped_unknown_fee_keeps_counterfactual_and_thresholds_incomplete() -> Result<()> {
    let fixture = Fixture::new()?;
    for i in 0..30 {
        fixture.round_trip(i, Some(0), Some(0), false)?;
    }
    fixture.round_trip(30, None, None, true)?;
    let report = fixture.report()?;
    let value = assert_incomplete(&report)?;
    let skipped = report
        .trades
        .iter()
        .find(|t| t.entry_decision_status.as_deref() == Some("would_skip"))
        .unwrap();
    close(
        skipped
            .skipped_counterfactual_pnl_sol
            .expect("gross counterfactual"),
        0.0001,
    );
    assert!(skipped
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_none());
    assert!(skipped
        .skipped_counterfactual_after_fee_vs_shadow_delta_sol
        .is_none());
    assert!(value["skipped_counterfactual_pnl_after_priority_fee_sol"].is_null());
    assert!(value["skipped_counterfactual_after_fee_vs_shadow_delta_sol"].is_null());
    for threshold in value["threshold_summaries"].as_array().unwrap() {
        assert_eq!(threshold["unknown_trades"], 1);
        assert!(threshold["quote_adjusted_pnl_after_priority_fee_sol"].is_null());
        assert!(threshold["skipped_counterfactual_pnl_after_priority_fee_sol"].is_null());
    }
    let bucket = value["buy_slippage_buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == "500-1000")
        .unwrap();
    assert!(bucket["quote_adjusted_pnl_after_priority_fee_sol"].is_null());
    assert!(
        !report
            .readiness_gate
            .threshold_candidate
            .as_ref()
            .unwrap()
            .pnl_positive
    );
    Ok(())
}
