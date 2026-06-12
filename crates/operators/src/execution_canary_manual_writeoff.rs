use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_storage_core::{
    ExecutionCanaryManualWriteOffResult, ExecutionCanaryOwnedPosition, SqliteStore,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

pub use crate::execution_canary_manual_writeoff_cli::parse_args_from;
use crate::execution_canary_manual_writeoff_cli::{
    Cli, DEFAULT_MAX_POSITION_QUOTE_SOL, DEFAULT_MAX_TOTAL_QUOTE_SOL, DEFAULT_MIN_AGE_MINUTES,
};
use crate::execution_canary_quote_pnl_sell_side::build_sell_side_diagnostics;
use crate::execution_canary_quote_pnl_wallet::{
    WalletReconciliationReport, WalletTokenReconciliation,
};
use crate::execution_canary_quote_pnl_wallet_live::build_live_wallet_reconciliation;

const REASON_OK: &str = "execution_tiny_writeoff_loaded";
const REASON_NOOP: &str = "execution_tiny_writeoff_noop";
const REASON_BLOCKED: &str = "execution_tiny_writeoff_blocked";
const REASON_PARTIAL: &str = "execution_tiny_writeoff_partial";
const REASON_ERROR: &str = "execution_tiny_writeoff_error";

#[derive(Debug, Serialize)]
pub struct TinyWriteOffReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub commit: bool,
    pub wallet_source_status: Option<String>,
    pub wallet_errors: Vec<String>,
    pub open_positions: u64,
    pub candidate_positions: u64,
    pub selected_positions: u64,
    pub selected_cost_sol: f64,
    pub selected_quote_sol: f64,
    pub max_position_quote_sol: f64,
    pub max_total_quote_sol: f64,
    pub min_age_minutes: i64,
    pub candidates: Vec<TinyWriteOffCandidate>,
    pub write_offs: Vec<TinyWriteOffResult>,
}

#[derive(Debug, Serialize)]
pub struct TinyWriteOffCandidate {
    pub token: String,
    pub position_id: String,
    pub opened_ts: DateTime<Utc>,
    pub age_minutes: i64,
    pub qty: f64,
    pub cost_sol: f64,
    pub quote_status: String,
    pub quote_out_sol: Option<f64>,
    pub selected: bool,
    pub decision_reason: String,
}

#[derive(Debug, Serialize)]
pub struct TinyWriteOffResult {
    pub token: String,
    pub order_id: String,
    pub signal_id: String,
    pub closed_positions: u64,
    pub closed_qty: f64,
    pub entry_cost_sol: f64,
    pub exit_value_sol: f64,
    pub pnl_sol: f64,
    pub error: Option<String>,
}

impl TinyWriteOffReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            commit: false,
            wallet_source_status: None,
            wallet_errors: Vec::new(),
            open_positions: 0,
            candidate_positions: 0,
            selected_positions: 0,
            selected_cost_sol: 0.0,
            selected_quote_sol: 0.0,
            max_position_quote_sol: DEFAULT_MAX_POSITION_QUOTE_SOL,
            max_total_quote_sol: DEFAULT_MAX_TOTAL_QUOTE_SOL,
            min_age_minutes: DEFAULT_MIN_AGE_MINUTES,
            candidates: Vec::new(),
            write_offs: Vec::new(),
        }
    }

    fn exit_code(&self) -> i32 {
        match self.reason_class.as_str() {
            REASON_OK | REASON_NOOP => 0,
            _ => 1,
        }
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => TinyWriteOffReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => TinyWriteOffReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("tiny write-off report must serialize")
    );
    report.exit_code()
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> TinyWriteOffReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => TinyWriteOffReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<TinyWriteOffReport> {
    let config = load_from_path(&cli.config_path)
        .with_context(|| format!("failed to load config {}", cli.config_path.display()))?;
    let db_path = PathBuf::from(config.sqlite.path.clone());
    let read_store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed to open db {}", db_path.display()))?;
    let open_positions = read_store.list_execution_canary_open_positions()?;
    let proof_limit = open_positions
        .len()
        .max(cli.max_positions)
        .min(u32::MAX as usize) as u32;
    let proof = read_store.execution_tiny_proof_report(
        as_of,
        as_of - Duration::hours(24),
        proof_limit.max(1),
    )?;
    let sell_side = build_sell_side_diagnostics(&proof);
    let wallet = build_live_wallet_reconciliation(&config.execution, &proof, &sell_side, as_of);
    let candidates = candidates_from_positions(&cli, as_of, &open_positions, &wallet);
    let selected_quote_sol = candidates
        .iter()
        .filter(|candidate| candidate.selected)
        .filter_map(|candidate| candidate.quote_out_sol)
        .sum::<f64>();
    let selected_cost_sol = candidates
        .iter()
        .filter(|candidate| candidate.selected)
        .map(|candidate| candidate.cost_sol)
        .sum::<f64>();
    let selected_count = candidates
        .iter()
        .filter(|candidate| candidate.selected)
        .count() as u64;
    let blocked = selected_quote_sol > cli.max_total_quote_sol;
    let write_offs = if cli.commit && selected_count > 0 && !blocked {
        write_off_selected(&config, &db_path, &candidates, as_of)?
    } else {
        Vec::new()
    };
    let reason_class = reason_class(cli.commit, selected_count, blocked, &write_offs);
    Ok(TinyWriteOffReport {
        as_of,
        reason_class: reason_class.to_string(),
        error: None,
        commit: cli.commit,
        wallet_source_status: Some(wallet.source_status),
        wallet_errors: wallet.errors,
        open_positions: open_positions.len() as u64,
        candidate_positions: candidates.len() as u64,
        selected_positions: selected_count,
        selected_cost_sol,
        selected_quote_sol,
        max_position_quote_sol: cli.max_position_quote_sol,
        max_total_quote_sol: cli.max_total_quote_sol,
        min_age_minutes: cli.min_age_minutes,
        candidates,
        write_offs,
    })
}

fn candidates_from_positions(
    cli: &Cli,
    as_of: DateTime<Utc>,
    positions: &[ExecutionCanaryOwnedPosition],
    wallet: &WalletReconciliationReport,
) -> Vec<TinyWriteOffCandidate> {
    let wallet_by_token: BTreeMap<_, _> = wallet
        .balances
        .iter()
        .map(|balance| (balance.mint.as_str(), balance))
        .collect();
    positions
        .iter()
        .take(cli.max_positions)
        .map(|position| candidate_from_position(cli, as_of, position, &wallet_by_token))
        .collect()
}

fn candidate_from_position(
    cli: &Cli,
    as_of: DateTime<Utc>,
    position: &ExecutionCanaryOwnedPosition,
    wallet_by_token: &BTreeMap<&str, &WalletTokenReconciliation>,
) -> TinyWriteOffCandidate {
    let age_minutes = (as_of - position.opened_ts).num_minutes();
    let wallet = wallet_by_token.get(position.token.as_str()).copied();
    let (quote_status, quote_out_sol) = wallet
        .and_then(|row| row.sell_quote.as_ref())
        .map(|quote| (quote.status.clone(), quote.out_sol))
        .unwrap_or_else(|| ("missing".to_string(), None));
    let decision_reason =
        write_off_decision_reason(cli, position, age_minutes, &quote_status, quote_out_sol);
    TinyWriteOffCandidate {
        token: position.token.clone(),
        position_id: position.position_id.clone(),
        opened_ts: position.opened_ts,
        age_minutes,
        qty: position.qty,
        cost_sol: position.cost_sol,
        quote_status,
        quote_out_sol,
        selected: decision_reason == "selected",
        decision_reason,
    }
}

fn write_off_decision_reason(
    cli: &Cli,
    position: &ExecutionCanaryOwnedPosition,
    age_minutes: i64,
    quote_status: &str,
    quote_out_sol: Option<f64>,
) -> String {
    if !cli.tokens.is_empty() && !cli.tokens.contains(&position.token) {
        return "token_not_requested".to_string();
    }
    if age_minutes < cli.min_age_minutes {
        return "position_too_fresh".to_string();
    }
    if quote_status != "ok" {
        return format!("quote_status_{quote_status}");
    }
    let Some(quote_out_sol) = quote_out_sol else {
        return "quote_missing_out_sol".to_string();
    };
    if quote_out_sol > cli.max_position_quote_sol {
        return "quote_value_above_position_guard".to_string();
    }
    "selected".to_string()
}

fn write_off_selected(
    config: &AppConfig,
    db_path: &PathBuf,
    candidates: &[TinyWriteOffCandidate],
    as_of: DateTime<Utc>,
) -> Result<Vec<TinyWriteOffResult>> {
    let store = SqliteStore::open(db_path)
        .with_context(|| format!("failed to open writable db {}", db_path.display()))?;
    let route = config.execution.canary_route.trim();
    if route.is_empty() {
        anyhow::bail!("execution.canary_route is required for write-off marker");
    }
    let mut results = Vec::new();
    for candidate in candidates.iter().filter(|candidate| candidate.selected) {
        results.push(
            match store.record_execution_canary_manual_terminal_write_off(
                &candidate.token,
                route,
                "operator_dead_book_cleanup",
                as_of,
            ) {
                Ok(result) => write_off_result(&candidate.token, result, None),
                Err(error) => TinyWriteOffResult {
                    token: candidate.token.clone(),
                    order_id: String::new(),
                    signal_id: String::new(),
                    closed_positions: 0,
                    closed_qty: 0.0,
                    entry_cost_sol: 0.0,
                    exit_value_sol: 0.0,
                    pnl_sol: 0.0,
                    error: Some(error.to_string()),
                },
            },
        );
    }
    Ok(results)
}

fn write_off_result(
    token: &str,
    result: ExecutionCanaryManualWriteOffResult,
    error: Option<String>,
) -> TinyWriteOffResult {
    TinyWriteOffResult {
        token: token.to_string(),
        order_id: result.order_id,
        signal_id: result.signal_id,
        closed_positions: result.close_results.len() as u64,
        closed_qty: result
            .close_results
            .iter()
            .map(|result| result.closed_qty)
            .sum(),
        entry_cost_sol: result
            .close_results
            .iter()
            .map(|result| result.entry_cost_sol)
            .sum(),
        exit_value_sol: result
            .close_results
            .iter()
            .map(|result| result.exit_value_sol)
            .sum(),
        pnl_sol: result
            .close_results
            .iter()
            .map(|result| result.pnl_sol)
            .sum(),
        error,
    }
}

fn reason_class(
    commit: bool,
    selected_count: u64,
    blocked: bool,
    write_offs: &[TinyWriteOffResult],
) -> &'static str {
    if selected_count == 0 {
        return REASON_NOOP;
    }
    if blocked {
        return REASON_BLOCKED;
    }
    if commit && write_offs.iter().any(|result| result.error.is_some()) {
        return REASON_PARTIAL;
    }
    REASON_OK
}
