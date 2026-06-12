use chrono::{TimeZone, Utc};
use copybot_operators::execution_ata_sweep::parse_args_from as parse_sweep_args;
use copybot_operators::execution_canary_manual_writeoff::parse_args_from as parse_writeoff_args;
use copybot_operators::execution_tiny_economics::parse_args_from as parse_economics_args;
use copybot_operators::execution_tiny_economics_gap::follower_gap_from_trades;
use copybot_storage_core::ExecutionCanaryQuotePnlTrade;

#[test]
fn ata_sweep_cli_defaults_to_dry_run() {
    let cli = parse_sweep_args(["--config", "/tmp/live.toml", "--json"]).unwrap();

    assert_eq!(cli.config_path.to_string_lossy(), "/tmp/live.toml");
    assert!(!cli.commit);
    assert_eq!(cli.max_accounts, 40);
    assert_eq!(cli.batch_size, 8);
}

#[test]
fn ata_sweep_cli_rejects_unbounded_batch_size() {
    let error = parse_sweep_args(["--config", "/tmp/live.toml", "--json", "--batch-size", "17"])
        .unwrap_err()
        .to_string();

    assert!(error.contains("--batch-size must be between 1 and 16"));
}

#[test]
fn tiny_economics_cli_defaults_to_five_hour_live_wallet_report() {
    let cli = parse_economics_args(["--config", "/tmp/live.toml", "--json"]).unwrap();

    assert_eq!(cli.config_path.unwrap().to_string_lossy(), "/tmp/live.toml");
    assert_eq!(cli.since_hours, 5);
    assert_eq!(cli.limit, 200);
    assert!(cli.live_wallet);
}

#[test]
fn tiny_writeoff_cli_defaults_to_dry_run_with_guards() {
    let cli = parse_writeoff_args(["--config", "/tmp/live.toml", "--json"]).unwrap();

    assert_eq!(cli.config_path.to_string_lossy(), "/tmp/live.toml");
    assert!(!cli.commit);
    assert_eq!(cli.max_positions, 20);
    assert_eq!(cli.min_age_minutes, 60);
    assert_eq!(cli.max_position_quote_sol, 0.001);
    assert_eq!(cli.max_total_quote_sol, 0.002);
}

#[test]
fn tiny_writeoff_cli_rejects_unbounded_position_count() {
    let error = parse_writeoff_args([
        "--config",
        "/tmp/live.toml",
        "--json",
        "--max-positions",
        "101",
    ])
    .unwrap_err()
    .to_string();

    assert!(error.contains("--max-positions must be between 1 and 100"));
}

#[test]
fn follower_gap_tracks_tail_and_sign_flips() {
    let trades = vec![
        trade(0.01, Some(-0.001), Some(1.0), Some(0.5)),
        trade(-0.01, Some(-0.002), Some(1.0), Some(1.0)),
        trade(0.02, Some(0.003), Some(1.0), Some(1.5)),
    ];

    let report = follower_gap_from_trades(&trades).unwrap();

    assert_eq!(report.samples, 3);
    assert_eq!(report.min_exit_quote_to_shadow_price, 0.5);
    assert_eq!(report.p50_exit_quote_to_shadow_price, 1.0);
    assert_eq!(report.shadow_positive_quote_after_fee_negative, 1);
    assert_eq!(report.quote_after_fee_negative, 2);
}

fn trade(
    shadow_pnl_sol: f64,
    quote_pnl_after_fee: Option<f64>,
    shadow_exit_price: Option<f64>,
    quote_exit_price: Option<f64>,
) -> ExecutionCanaryQuotePnlTrade {
    let ts = Utc.with_ymd_and_hms(2026, 6, 12, 0, 0, 0).unwrap();
    ExecutionCanaryQuotePnlTrade {
        shadow_closed_trade_id: 1,
        signal_id: "signal".to_string(),
        wallet_id: "wallet".to_string(),
        token: "token".to_string(),
        opened_ts: ts,
        closed_ts: ts,
        status: "pnl_counted".to_string(),
        reason: "ok".to_string(),
        shadow_pnl_sol,
        quote_adjusted_pnl_sol: quote_pnl_after_fee,
        quote_adjusted_pnl_after_priority_fee_sol: quote_pnl_after_fee,
        quote_vs_shadow_delta_sol: None,
        quote_after_fee_vs_shadow_delta_sol: None,
        skipped_counterfactual_pnl_sol: None,
        skipped_counterfactual_pnl_after_priority_fee_sol: None,
        skipped_counterfactual_after_fee_vs_shadow_delta_sol: None,
        entry_quote_event_id: None,
        exit_quote_event_id: None,
        entry_decision_status: None,
        exit_decision_status: None,
        entry_quote_status: None,
        exit_quote_status: None,
        entry_priority_fee_status: None,
        exit_priority_fee_status: None,
        entry_cost_sol: None,
        exit_quote_sol: None,
        closed_qty_ratio: None,
        buy_leader_notional_sol: None,
        sell_leader_notional_sol: None,
        buy_slippage_bps: None,
        sell_slippage_bps: None,
        buy_price_impact_pct: None,
        sell_price_impact_pct: None,
        entry_decision_delay_ms: None,
        exit_decision_delay_ms: None,
        entry_quote_latency_ms: None,
        exit_quote_latency_ms: None,
        entry_quote_price_sol: None,
        exit_quote_price_sol: quote_exit_price,
        entry_shadow_price_sol: None,
        exit_shadow_price_sol: shadow_exit_price,
        entry_route_labels: Vec::new(),
        exit_route_labels: Vec::new(),
        priority_fee_lamports_total: None,
    }
}
