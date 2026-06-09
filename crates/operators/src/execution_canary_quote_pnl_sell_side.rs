use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionTinyProofOrder, ExecutionTinyProofReport, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
    EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED, EXECUTION_STATUS_CANARY_CONFIRMED,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const MAX_TOKEN_FAILURES: usize = 20;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SellSideDiagnosticsReport {
    pub recent_sell_orders: u64,
    pub failed_sell_orders: u64,
    pub simulation_failed_orders: u64,
    pub build_failed_orders: u64,
    pub terminal_no_route_orders: u64,
    pub terminal_simulation_orders: u64,
    pub tx_signature_present_failed_orders: u64,
    pub open_position_count: usize,
    pub open_position_tokens: Vec<String>,
    pub failure_token_count: usize,
    pub failures_by_token: Vec<SellSideTokenFailure>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SellSideTokenFailure {
    pub token: String,
    pub orders: u64,
    pub latest_submit_ts: DateTime<Utc>,
    pub statuses: Vec<String>,
    pub err_codes: Vec<String>,
    pub simulation_error_classes: Vec<String>,
    pub quote_sources: Vec<String>,
    pub routes: Vec<String>,
    pub terminal_no_route_orders: u64,
    pub terminal_simulation_orders: u64,
    pub tx_signature_present_orders: u64,
    pub latest_error: Option<String>,
    pub next_action: String,
}

#[derive(Debug)]
struct TokenFailureAccumulator {
    token: String,
    orders: u64,
    latest_submit_ts: DateTime<Utc>,
    statuses: BTreeSet<String>,
    err_codes: BTreeSet<String>,
    simulation_error_classes: BTreeSet<String>,
    quote_sources: BTreeSet<String>,
    routes: BTreeSet<String>,
    terminal_no_route_orders: u64,
    terminal_simulation_orders: u64,
    tx_signature_present_orders: u64,
    latest_error: Option<String>,
}

pub fn build_sell_side_diagnostics(proof: &ExecutionTinyProofReport) -> SellSideDiagnosticsReport {
    let mut recent_sell_orders = 0_u64;
    let mut failed_sell_orders = 0_u64;
    let mut simulation_failed_orders = 0_u64;
    let mut build_failed_orders = 0_u64;
    let mut terminal_no_route_orders = 0_u64;
    let mut terminal_simulation_orders = 0_u64;
    let mut tx_signature_present_failed_orders = 0_u64;
    let mut by_token = BTreeMap::<String, TokenFailureAccumulator>::new();

    for order in proof.recent_orders.iter().filter(|order| {
        order
            .side
            .as_deref()
            .is_some_and(|side| side.eq_ignore_ascii_case("sell"))
    }) {
        recent_sell_orders += 1;
        if order.status == EXECUTION_STATUS_CANARY_CONFIRMED {
            continue;
        }
        failed_sell_orders += 1;
        let err_code = value_or_missing(order.err_code.as_deref());
        match err_code.as_str() {
            EXECUTION_ERROR_SIMULATION_FAILED => simulation_failed_orders += 1,
            EXECUTION_ERROR_BUILD_FAILED => build_failed_orders += 1,
            EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE => terminal_no_route_orders += 1,
            EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED => terminal_simulation_orders += 1,
            _ => {}
        }
        if order.tx_signature_present {
            tx_signature_present_failed_orders += 1;
        }
        record_token_failure(&mut by_token, order, err_code);
    }

    let mut failures_by_token: Vec<_> = by_token
        .into_values()
        .map(TokenFailureAccumulator::into_report)
        .collect();
    failures_by_token.sort_by(|left, right| {
        right
            .latest_submit_ts
            .cmp(&left.latest_submit_ts)
            .then_with(|| left.token.cmp(&right.token))
    });
    let failure_token_count = failures_by_token.len();
    failures_by_token.truncate(MAX_TOKEN_FAILURES);

    let mut open_position_tokens: Vec<_> = proof
        .open_positions
        .iter()
        .map(|position| position.token.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    open_position_tokens.truncate(MAX_TOKEN_FAILURES);

    SellSideDiagnosticsReport {
        recent_sell_orders,
        failed_sell_orders,
        simulation_failed_orders,
        build_failed_orders,
        terminal_no_route_orders,
        terminal_simulation_orders,
        tx_signature_present_failed_orders,
        open_position_count: proof.open_positions.len(),
        open_position_tokens,
        failure_token_count,
        failures_by_token,
    }
}

fn record_token_failure(
    by_token: &mut BTreeMap<String, TokenFailureAccumulator>,
    order: &ExecutionTinyProofOrder,
    err_code: String,
) {
    let token = value_or_missing(order.token.as_deref());
    let error_class = simulation_error_class(order.simulation_error.as_deref());
    let entry = by_token
        .entry(token.clone())
        .or_insert_with(|| TokenFailureAccumulator {
            token,
            orders: 0,
            latest_submit_ts: order.submit_ts,
            statuses: BTreeSet::new(),
            err_codes: BTreeSet::new(),
            simulation_error_classes: BTreeSet::new(),
            quote_sources: BTreeSet::new(),
            routes: BTreeSet::new(),
            terminal_no_route_orders: 0,
            terminal_simulation_orders: 0,
            tx_signature_present_orders: 0,
            latest_error: None,
        });
    entry.orders += 1;
    entry.statuses.insert(order.status.clone());
    entry.err_codes.insert(err_code.clone());
    entry.simulation_error_classes.insert(error_class);
    entry.routes.insert(order.route.clone());
    entry
        .quote_sources
        .insert(value_or_missing(order.quote_source.as_deref()));
    if err_code == EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE {
        entry.terminal_no_route_orders += 1;
    }
    if err_code == EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED {
        entry.terminal_simulation_orders += 1;
    }
    if order.tx_signature_present {
        entry.tx_signature_present_orders += 1;
    }
    if order.submit_ts >= entry.latest_submit_ts {
        entry.latest_submit_ts = order.submit_ts;
        entry.latest_error = order.simulation_error.clone();
    }
}

impl TokenFailureAccumulator {
    fn into_report(self) -> SellSideTokenFailure {
        let next_action = next_action(
            &self.err_codes,
            &self.simulation_error_classes,
            self.tx_signature_present_orders,
        );
        SellSideTokenFailure {
            token: self.token,
            orders: self.orders,
            latest_submit_ts: self.latest_submit_ts,
            statuses: self.statuses.into_iter().collect(),
            err_codes: self.err_codes.into_iter().collect(),
            simulation_error_classes: self.simulation_error_classes.into_iter().collect(),
            quote_sources: self.quote_sources.into_iter().collect(),
            routes: self.routes.into_iter().collect(),
            terminal_no_route_orders: self.terminal_no_route_orders,
            terminal_simulation_orders: self.terminal_simulation_orders,
            tx_signature_present_orders: self.tx_signature_present_orders,
            latest_error: self.latest_error,
            next_action,
        }
    }
}

fn next_action(
    err_codes: &BTreeSet<String>,
    error_classes: &BTreeSet<String>,
    tx_signature_present_orders: u64,
) -> String {
    if tx_signature_present_orders > 0 {
        return "reconcile_submitted_sell_before_retry".to_string();
    }
    if err_codes.contains(EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED) {
        return "inspect_terminal_simulation_proof".to_string();
    }
    if err_codes.contains(EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE) {
        return "inspect_terminal_no_route_proof".to_string();
    }
    if err_codes.contains(EXECUTION_ERROR_SIMULATION_FAILED) {
        return "inspect_simulation_error_and_route_source".to_string();
    }
    if error_classes.contains("terminal_no_route")
        || error_classes.contains("pump_fun_bonding_curve_not_found")
    {
        return "reprobe_paid_routes_or_terminal_no_route".to_string();
    }
    "inspect_sell_failure".to_string()
}

fn value_or_missing(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("missing")
        .to_string()
}

fn simulation_error_class(value: Option<&str>) -> String {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return "missing".to_string();
    };
    let lower = raw.to_ascii_lowercase();
    if lower.contains("no_routes_found")
        || lower.contains("no routes found")
        || lower.contains("terminal_failed_sell_no_route")
        || lower.contains("terminal_sell_no_route")
    {
        return "terminal_no_route".to_string();
    }
    if lower.contains("bonding curve for mint not found") {
        return "pump_fun_bonding_curve_not_found".to_string();
    }
    if let Some(hex) = custom_program_error_code(&lower) {
        return format!("custom_program_error:{hex}");
    }
    if lower.contains("insufficient funds") {
        return "insufficient_funds".to_string();
    }
    if lower.contains("account not found") || lower.contains("could not find account") {
        return "account_not_found".to_string();
    }
    if lower.contains("blockhash") {
        return "blockhash".to_string();
    }
    if lower.contains("slippage") {
        return "slippage".to_string();
    }
    "other".to_string()
}

fn custom_program_error_code(lower: &str) -> Option<String> {
    let marker = "custom program error:";
    let (_, tail) = lower.split_once(marker)?;
    let code = tail
        .trim_start()
        .split(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';' || ch == '}')
        .next()?
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '.' || ch == ':');
    code.strip_prefix("0x")
        .filter(|hex| !hex.is_empty() && hex.chars().all(|ch| ch.is_ascii_hexdigit()))
        .map(|hex| format!("0x{hex}"))
}
