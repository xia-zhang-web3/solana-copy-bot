use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_storage_core::{ExecutionCanaryQuotePnlSummary, ExecutionTinyProofReport, SqliteStore};
use serde::Serialize;
use std::env;
use std::path::PathBuf;

use crate::execution_canary_quote_pnl_sell_side::build_sell_side_diagnostics;
use crate::execution_canary_quote_pnl_wallet::WalletReconciliationReport;
use crate::execution_canary_quote_pnl_wallet_live::build_live_wallet_reconciliation;
use crate::execution_tiny_economics_gap::{follower_gap_from_trades, FollowerGapReport};
use crate::execution_tiny_equity::{build_equity_view, EquityViewReport};

const REASON_OK: &str = "execution_tiny_economics_loaded";
const REASON_ERROR: &str = "execution_tiny_economics_error";
const DEFAULT_SINCE_HOURS: i64 = 5;
const MAX_SINCE_HOURS: i64 = 168;
const DEFAULT_LIMIT: u32 = 200;
const MAX_LIMIT: u32 = 1000;
const ZERO_TOKEN_ACCOUNT_RENT_LAMPORTS: u64 = 2_039_280;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub limit: u32,
    pub live_wallet: bool,
}

#[derive(Debug, Serialize)]
pub struct TinyEconomicsReport {
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub configured_notional: Option<ConfiguredNotionalReport>,
    pub shadow: Option<ShadowEconomics>,
    pub canary: Option<CanaryEconomics>,
    pub tiny: Option<TinyRealizedEconomics>,
    pub open_mark_to_quote: Option<OpenMarkToQuoteReport>,
    pub equity_view: Option<EquityViewReport>,
    pub follower_gap: Option<FollowerGapReport>,
}

#[derive(Debug, Serialize)]
pub struct ConfiguredNotionalReport {
    pub shadow_copy_notional_sol: f64,
    pub quote_canary_buy_size_sol: f64,
    pub tiny_buy_size_sol: f64,
}

#[derive(Debug, Serialize)]
pub struct ShadowEconomics {
    pub market_trades: u64,
    pub market_pnl_sol: f64,
    pub market_pnl_scaled_to_tiny_sol: Option<f64>,
    pub all_context_trades: u64,
    pub all_context_pnl_sol: f64,
    pub all_context_pnl_scaled_to_tiny_sol: Option<f64>,
    pub stale_rug_like_pnl_sol: f64,
}

#[derive(Debug, Serialize)]
pub struct CanaryEconomics {
    pub counted_trades: u64,
    pub quote_adjusted_pnl_sol: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub quote_adjusted_pnl_scaled_to_tiny_sol: Option<f64>,
    pub quote_adjusted_pnl_after_priority_fee_scaled_to_tiny_sol: Option<f64>,
    pub quote_win_count: u64,
    pub quote_loss_count: u64,
}

#[derive(Debug, Serialize)]
pub struct TinyRealizedEconomics {
    pub closed_positions: u64,
    pub realized_pnl_sol: f64,
    pub zero_token_account_count: Option<u64>,
    pub zero_token_account_rent_estimate_sol: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct OpenMarkToQuoteReport {
    pub open_positions: u64,
    pub open_cost_sol: f64,
    pub quoted_value_sol: Option<f64>,
    pub unrealized_pnl_sol: Option<f64>,
    pub quote_errors: Vec<String>,
}

impl TinyEconomicsReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            since: as_of - Duration::hours(DEFAULT_SINCE_HOURS),
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            configured_notional: None,
            shadow: None,
            canary: None,
            tiny: None,
            open_mark_to_quote: None,
            equity_view: None,
            follower_gap: None,
        }
    }

    fn exit_code(&self) -> i32 {
        i32::from(self.reason_class != REASON_OK)
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => TinyEconomicsReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => TinyEconomicsReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("economics report must serialize")
    );
    report.exit_code()
}

pub fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut config_path = None;
    let mut db_path = None;
    let mut json = false;
    let mut since = None;
    let mut since_hours = DEFAULT_SINCE_HOURS;
    let mut limit = DEFAULT_LIMIT;
    let mut live_wallet = true;
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(next_value(&mut iter, "--config")?)),
            "--db-path" => db_path = Some(PathBuf::from(next_value(&mut iter, "--db-path")?)),
            "--since" => since = Some(parse_since(&next_value(&mut iter, "--since")?)?),
            "--since-hours" => {
                since_hours = parse_since_hours(&next_value(&mut iter, "--since-hours")?)?
            }
            "--limit" => limit = parse_limit(&next_value(&mut iter, "--limit")?)?,
            "--no-live-wallet" => live_wallet = false,
            "--json" => json = true,
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if config_path.is_none() && db_path.is_none() {
        anyhow::bail!("either --config or --db-path is required");
    }
    Ok(Cli {
        config_path,
        db_path,
        json,
        since,
        since_hours,
        limit,
        live_wallet,
    })
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> TinyEconomicsReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => TinyEconomicsReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<TinyEconomicsReport> {
    let since = cli
        .since
        .unwrap_or(as_of - Duration::hours(cli.since_hours));
    let (config, db_path) = load_context(&cli)?;
    let store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed to open db {}", db_path.display()))?;
    let summary = store.execution_canary_quote_pnl_summary(as_of, since, cli.limit)?;
    let proof = store.execution_tiny_proof_report(as_of, since, cli.limit)?;
    let wallet = config.as_ref().and_then(|config| {
        cli.live_wallet.then(|| {
            let sell_side = build_sell_side_diagnostics(&proof);
            build_live_wallet_reconciliation(&config.execution, &proof, &sell_side, as_of)
        })
    });
    let open_mark = open_mark_to_quote(&proof, wallet.as_ref());
    let equity_view = build_equity_view(
        proof.summary.tiny_open_positions,
        open_mark.open_cost_sol,
        open_mark.quoted_value_sol,
        wallet.as_ref(),
    );
    Ok(TinyEconomicsReport {
        as_of,
        since,
        reason_class: REASON_OK.to_string(),
        error: None,
        configured_notional: config.as_ref().map(configured_notional),
        shadow: Some(shadow_economics(&summary, config.as_ref())),
        canary: Some(canary_economics(&summary, config.as_ref())),
        tiny: Some(tiny_economics(&proof, wallet.as_ref())),
        open_mark_to_quote: Some(open_mark),
        equity_view: Some(equity_view),
        follower_gap: follower_gap_from_trades(&summary.trades),
    })
}

fn load_context(cli: &Cli) -> Result<(Option<AppConfig>, PathBuf)> {
    if let Some(path) = &cli.config_path {
        let config = load_from_path(path)
            .with_context(|| format!("failed to load config {}", path.display()))?;
        let db_path = cli
            .db_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(config.sqlite.path.clone()));
        return Ok((Some(config), db_path));
    }
    Ok((
        None,
        cli.db_path
            .clone()
            .ok_or_else(|| anyhow!("either --config or --db-path is required"))?,
    ))
}

fn configured_notional(config: &AppConfig) -> ConfiguredNotionalReport {
    ConfiguredNotionalReport {
        shadow_copy_notional_sol: config.shadow.copy_notional_sol,
        quote_canary_buy_size_sol: config.execution.quote_canary_buy_size_sol,
        tiny_buy_size_sol: config.execution.canary_buy_size_sol,
    }
}

fn shadow_economics(
    summary: &ExecutionCanaryQuotePnlSummary,
    config: Option<&AppConfig>,
) -> ShadowEconomics {
    let scale = config.and_then(|config| {
        tiny_scale(
            config.execution.canary_buy_size_sol,
            config.shadow.copy_notional_sol,
        )
    });
    ShadowEconomics {
        market_trades: summary.total_closed_trades,
        market_pnl_sol: summary.shadow_pnl_sol,
        market_pnl_scaled_to_tiny_sol: scale.map(|scale| summary.shadow_pnl_sol * scale),
        all_context_trades: summary.shadow_close_breakdown.total_closed_trades,
        all_context_pnl_sol: summary.shadow_close_breakdown.total_pnl_sol,
        all_context_pnl_scaled_to_tiny_sol: scale
            .map(|scale| summary.shadow_close_breakdown.total_pnl_sol * scale),
        stale_rug_like_pnl_sol: summary.shadow_close_breakdown.stale_rug_like_pnl_sol,
    }
}

fn canary_economics(
    summary: &ExecutionCanaryQuotePnlSummary,
    config: Option<&AppConfig>,
) -> CanaryEconomics {
    let scale = config.and_then(|config| {
        tiny_scale(
            config.execution.canary_buy_size_sol,
            config.execution.quote_canary_buy_size_sol,
        )
    });
    CanaryEconomics {
        counted_trades: summary.pnl_counted_trades,
        quote_adjusted_pnl_sol: summary.quote_adjusted_pnl_sol,
        quote_adjusted_pnl_after_priority_fee_sol: summary
            .quote_adjusted_pnl_after_priority_fee_sol,
        quote_adjusted_pnl_scaled_to_tiny_sol: scale
            .map(|scale| summary.quote_adjusted_pnl_sol * scale),
        quote_adjusted_pnl_after_priority_fee_scaled_to_tiny_sol: scale
            .map(|scale| summary.quote_adjusted_pnl_after_priority_fee_sol * scale),
        quote_win_count: summary.quote_win_count,
        quote_loss_count: summary.quote_loss_count,
    }
}

fn tiny_economics(
    proof: &ExecutionTinyProofReport,
    wallet: Option<&WalletReconciliationReport>,
) -> TinyRealizedEconomics {
    let zero_count = wallet.map(|wallet| wallet.zero_token_account_count);
    TinyRealizedEconomics {
        closed_positions: proof.summary.tiny_unique_closed_positions,
        realized_pnl_sol: proof.summary.tiny_realized_pnl_sol,
        zero_token_account_count: zero_count,
        zero_token_account_rent_estimate_sol: zero_count.map(estimated_zero_rent_sol),
    }
}

fn open_mark_to_quote(
    proof: &ExecutionTinyProofReport,
    wallet: Option<&WalletReconciliationReport>,
) -> OpenMarkToQuoteReport {
    let open_cost_sol: f64 = proof
        .open_positions
        .iter()
        .map(|position| position.cost_sol)
        .sum();
    let quoted_value_sol = wallet.map(|wallet| {
        wallet
            .balances
            .iter()
            .filter_map(|balance| {
                balance
                    .bot_open_position
                    .as_ref()
                    .and_then(|_| balance.sell_quote.as_ref())
                    .and_then(|quote| quote.out_sol)
            })
            .sum::<f64>()
    });
    let quote_errors = wallet
        .map(|wallet| wallet.errors.clone())
        .unwrap_or_default();
    OpenMarkToQuoteReport {
        open_positions: proof.summary.tiny_open_positions,
        open_cost_sol,
        quoted_value_sol,
        unrealized_pnl_sol: quoted_value_sol.map(|value| value - open_cost_sol),
        quote_errors,
    }
}

fn tiny_scale(tiny_size_sol: f64, source_size_sol: f64) -> Option<f64> {
    if tiny_size_sol.is_finite() && source_size_sol.is_finite() && source_size_sol > 0.0 {
        Some(tiny_size_sol / source_size_sol)
    } else {
        None
    }
}

fn estimated_zero_rent_sol(count: u64) -> f64 {
    count as f64 * ZERO_TOKEN_ACCOUNT_RENT_LAMPORTS as f64 / 1_000_000_000.0
}

fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_limit(value: &str) -> Result<u32> {
    let parsed = value
        .parse::<u32>()
        .with_context(|| format!("invalid --limit: {value}"))?;
    if parsed == 0 || parsed > MAX_LIMIT {
        anyhow::bail!("--limit must be between 1 and {MAX_LIMIT}");
    }
    Ok(parsed)
}

fn parse_since_hours(value: &str) -> Result<i64> {
    let parsed = value
        .parse::<i64>()
        .with_context(|| format!("invalid --since-hours: {value}"))?;
    if parsed <= 0 || parsed > MAX_SINCE_HOURS {
        anyhow::bail!("--since-hours must be between 1 and {MAX_SINCE_HOURS}");
    }
    Ok(parsed)
}

fn parse_since(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid --since RFC3339 timestamp: {value}"))
}
