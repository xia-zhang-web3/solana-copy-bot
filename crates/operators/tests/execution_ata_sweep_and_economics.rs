use chrono::{TimeZone, Utc};
use copybot_operators::execution_ata_sweep::parse_args_from as parse_sweep_args;
use copybot_operators::execution_canary_manual_writeoff::parse_args_from as parse_writeoff_args;
use copybot_operators::execution_canary_quote_pnl_wallet::{
    WalletReconciliationReport, WalletSellQuoteProof, WalletTokenReconciliation,
};
use copybot_operators::execution_tiny_economics::parse_args_from as parse_economics_args;
use copybot_operators::execution_tiny_economics_gap::follower_gap_from_trades;
use copybot_operators::execution_tiny_equity::build_equity_view;
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
    assert!(!cli.include_stale_decay);
}

#[test]
fn tiny_economics_cli_enables_stale_decay_only_by_flag() {
    let cli = parse_economics_args([
        "--config",
        "/tmp/live.toml",
        "--json",
        "--include-stale-decay",
        "--stale-decay-window-minutes",
        "15",
        "--stale-decay-min-sol-notional",
        "0.001",
        "--stale-decay-min-samples",
        "2",
        "--stale-decay-max-samples",
        "20",
    ])
    .unwrap();

    assert!(cli.include_stale_decay);
    assert_eq!(cli.stale_decay_window_minutes, 15);
    assert_eq!(cli.stale_decay_min_sol_notional, 0.001);
    assert_eq!(cli.stale_decay_min_samples, 2);
    assert_eq!(cli.stale_decay_max_samples, 20);
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
fn tiny_writeoff_cli_accepts_explicit_no_route_tokens() {
    let cli = parse_writeoff_args([
        "--config",
        "/tmp/live.toml",
        "--json",
        "--allow-no-route-token",
        "NoRouteMint",
    ])
    .unwrap();

    assert!(cli.no_route_tokens.contains("NoRouteMint"));
}

#[test]
fn tiny_writeoff_cli_accepts_explicit_threshold_error_tokens() {
    let cli = parse_writeoff_args([
        "--config",
        "/tmp/live.toml",
        "--json",
        "--allow-threshold-error-token",
        "ThresholdMint",
    ])
    .unwrap();

    assert!(cli.threshold_error_tokens.contains("ThresholdMint"));
}

#[test]
fn tiny_writeoff_cli_rejects_large_quote_guards() {
    let error = parse_writeoff_args([
        "--config",
        "/tmp/live.toml",
        "--json",
        "--max-position-quote-sol",
        "0.02",
    ])
    .unwrap_err()
    .to_string();

    assert!(error.contains("--max-position-quote-sol must be <= 0.01"));
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

#[test]
fn equity_view_combines_liquid_open_dust_and_recoverable_rent() {
    let wallet = WalletReconciliationReport {
        as_of: Utc.with_ymd_and_hms(2026, 6, 12, 0, 0, 0).unwrap(),
        owner_pubkey: "wallet".to_string(),
        source_status: "ok".to_string(),
        errors: Vec::new(),
        sol_balance_lamports: Some(1_500_000_000),
        sol_balance_sol: Some(1.5),
        token_account_count: 5,
        zero_token_account_count: 2,
        nonzero_token_account_count: 3,
        bot_open_position_count: 1,
        matched_open_position_count: 1,
        unmatched_open_position_count: 0,
        unmatched_open_position_tokens: Vec::new(),
        terminal_no_route_leftover_count: 1,
        failed_sell_leftover_count: 0,
        untracked_nonzero_count: 1,
        quote_ok_count: 3,
        quote_no_route_count: 0,
        near_zero_quote_count: 0,
        near_zero_lamports_threshold: 10_000,
        balances: vec![
            balance("bot_open_position", Some(0.03)),
            balance("untracked_wallet_balance", Some(0.02)),
            balance("terminal_no_route_leftover", Some(0.01)),
        ],
    };

    let report = build_equity_view(1, 0.05, Some(0.03), Some(&wallet));

    assert_eq!(report.liquid_sol, Some(1.5));
    assert_eq!(report.open_unrealized_pnl_sol, Some(-0.020000000000000004));
    assert_eq!(report.untracked_quote_value_sol, Some(0.02));
    assert_eq!(report.terminal_leftover_quote_value_sol, Some(0.01));
    assert_eq!(report.recoverable_zero_ata_rent_sol, Some(0.00407856));
    assert_eq!(report.wallet_mark_value_sol, Some(1.56407856));
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

fn balance(classification: &str, out_sol: Option<f64>) -> WalletTokenReconciliation {
    WalletTokenReconciliation {
        mint: format!("{classification}-mint"),
        token_account: format!("{classification}-ata"),
        amount_raw: "1".to_string(),
        ui_amount_string: "1".to_string(),
        decimals: 6,
        classification: classification.to_string(),
        bot_open_position: None,
        sell_failure: None,
        sell_quote: Some(WalletSellQuoteProof {
            status: "ok".to_string(),
            http_status: Some(200),
            error: None,
            error_code: None,
            out_amount_raw: None,
            out_sol,
            price_impact_pct: None,
            route_labels: Vec::new(),
        }),
    }
}
