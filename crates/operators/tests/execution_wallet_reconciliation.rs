use chrono::{DateTime, Utc};
use copybot_operators::execution_canary_quote_pnl::parse_args_from;
use copybot_operators::execution_canary_quote_pnl_sell_side::{
    SellSideDiagnosticsReport, SellSideTokenFailure,
};
use copybot_operators::execution_canary_quote_pnl_wallet::{
    build_wallet_reconciliation_from_parts, WalletSellQuoteProof, WalletTokenBalance,
};
use copybot_storage_core::ExecutionTinyProofOpenPosition;
use std::collections::BTreeMap;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn wallet_reconciliation_classifies_live_leftovers_and_untracked_balances() {
    let as_of = ts("2026-06-09T20:45:00Z");
    let report = build_wallet_reconciliation_from_parts(
        as_of,
        "Aegb8cCuQVZJEcRfd4AbE3TE5MecJE5UBtkCdQ2764vs".to_string(),
        Some(1_516_958_865),
        10,
        vec![
            balance("OpenMint", "26435909646"),
            balance("TerminalMint", "476443270406"),
            balance("FailedMint", "30041062390"),
            balance("UntrackedMint", "7"),
        ],
        &[ExecutionTinyProofOpenPosition {
            position_id: "open-position".to_string(),
            token: "OpenMint".to_string(),
            qty: 26_435.909646,
            cost_sol: 0.0176063,
            opened_ts: ts("2026-06-09T19:03:04Z"),
        }],
        &sell_side(),
        quote_proofs(),
        Vec::new(),
    );

    assert_eq!(report.source_status, "ok");
    assert_eq!(report.sol_balance_sol, Some(1.516958865));
    assert_eq!(report.token_account_count, 10);
    assert_eq!(report.zero_token_account_count, 6);
    assert_eq!(report.nonzero_token_account_count, 4);
    assert_eq!(report.bot_open_position_count, 1);
    assert_eq!(report.matched_open_position_count, 1);
    assert_eq!(report.terminal_no_route_leftover_count, 1);
    assert_eq!(report.failed_sell_leftover_count, 1);
    assert_eq!(report.untracked_nonzero_count, 1);
    assert_eq!(report.quote_ok_count, 3);
    assert_eq!(report.quote_no_route_count, 1);
    assert_eq!(report.near_zero_quote_count, 2);
    assert_classification(&report, "OpenMint", "bot_open_position");
    assert_classification(&report, "TerminalMint", "terminal_no_route_leftover");
    assert_classification(&report, "FailedMint", "failed_sell_leftover");
    assert_classification(&report, "UntrackedMint", "untracked_wallet_balance");
}

#[test]
fn wallet_reconciliation_cli_flag_is_opt_in() {
    let plain = parse_args_from(["--config", "live.toml", "--json"]).expect("plain cli");
    let with_wallet =
        parse_args_from(["--config", "live.toml", "--json", "--wallet-reconciliation"])
            .expect("wallet cli");

    assert!(!plain.wallet_reconciliation);
    assert!(with_wallet.wallet_reconciliation);
}

fn balance(mint: &str, amount_raw: &str) -> WalletTokenBalance {
    WalletTokenBalance {
        token_account: format!("ata-{mint}"),
        mint: mint.to_string(),
        amount_raw: amount_raw.to_string(),
        ui_amount_string: amount_raw.to_string(),
        decimals: 6,
    }
}

fn sell_side() -> SellSideDiagnosticsReport {
    SellSideDiagnosticsReport {
        recent_sell_orders: 3,
        failed_sell_orders: 2,
        simulation_failed_orders: 1,
        build_failed_orders: 0,
        terminal_no_route_orders: 1,
        terminal_simulation_orders: 0,
        tx_signature_present_failed_orders: 0,
        open_position_count: 1,
        open_position_tokens: vec!["OpenMint".to_string()],
        failure_token_count: 2,
        failures_by_token: vec![
            failure("TerminalMint", 1, 0, "inspect_terminal_no_route_proof"),
            failure(
                "FailedMint",
                0,
                0,
                "inspect_simulation_error_and_route_source",
            ),
        ],
    }
}

fn failure(
    token: &str,
    terminal_no_route_orders: u64,
    terminal_simulation_orders: u64,
    next_action: &str,
) -> SellSideTokenFailure {
    SellSideTokenFailure {
        token: token.to_string(),
        orders: 1,
        latest_submit_ts: ts("2026-06-09T20:00:00Z"),
        statuses: vec!["execution_canary_failed".to_string()],
        err_codes: vec!["simulation_failed".to_string()],
        simulation_error_classes: vec!["custom_program_error:0x1788".to_string()],
        quote_sources: vec!["execution_quote_canary_provider:generic_metis".to_string()],
        routes: vec!["metis-swap-instructions-dry-run".to_string()],
        terminal_no_route_orders,
        terminal_simulation_orders,
        tx_signature_present_orders: 0,
        latest_error: Some("proof".to_string()),
        next_action: next_action.to_string(),
    }
}

fn quote_proofs() -> BTreeMap<String, WalletSellQuoteProof> {
    [
        ("OpenMint", ok_quote("5055")),
        ("TerminalMint", no_route_quote()),
        ("FailedMint", ok_quote("50000")),
        ("UntrackedMint", ok_quote("7")),
    ]
    .into_iter()
    .map(|(mint, quote)| (mint.to_string(), quote))
    .collect()
}

fn ok_quote(out_amount_raw: &str) -> WalletSellQuoteProof {
    WalletSellQuoteProof {
        status: "ok".to_string(),
        http_status: Some(200),
        error: None,
        error_code: None,
        out_amount_raw: Some(out_amount_raw.to_string()),
        out_sol: out_amount_raw
            .parse::<u64>()
            .ok()
            .map(|lamports| lamports as f64 / 1_000_000_000.0),
        price_impact_pct: Some("0.999".to_string()),
        route_labels: vec!["Pump.fun Amm".to_string()],
    }
}

fn no_route_quote() -> WalletSellQuoteProof {
    WalletSellQuoteProof {
        status: "no_route".to_string(),
        http_status: Some(400),
        error: Some("No routes found".to_string()),
        error_code: Some("NO_ROUTES_FOUND".to_string()),
        out_amount_raw: None,
        out_sol: None,
        price_impact_pct: None,
        route_labels: Vec::new(),
    }
}

fn assert_classification(
    report: &copybot_operators::execution_canary_quote_pnl_wallet::WalletReconciliationReport,
    mint: &str,
    classification: &str,
) {
    assert!(
        report
            .balances
            .iter()
            .any(|balance| balance.mint == mint && balance.classification == classification),
        "missing {mint} classification {classification}: {:?}",
        report.balances
    );
}
