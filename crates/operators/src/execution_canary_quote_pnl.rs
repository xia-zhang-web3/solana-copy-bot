use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionQuoteCanaryProviderComparisonSummary,
    ExecutionQuoteCanaryProviderSelectionSummary, ExecutionQuoteCanaryPublicPaidComparisonSummary,
    ExecutionTinyProofReport, SqliteStore,
};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

use crate::execution_canary_quote_pnl_gate::build_tiny_execution_gate;
pub use crate::execution_canary_quote_pnl_gate::{TinyExecutionGate, TinyExecutionGateCheck};
use crate::execution_canary_quote_pnl_metis::{build_metis_diagnostics, MetisDiagnosticsReport};
use crate::execution_canary_quote_pnl_sell_side::{
    build_sell_side_diagnostics, SellSideDiagnosticsReport,
};

#[path = "execution_canary_quote_pnl_cli.rs"]
mod quote_pnl_cli;
use quote_pnl_cli::{next_value, parse_limit, parse_since, parse_since_hours};

const REASON_OK: &str = "execution_canary_quote_pnl_loaded";
const REASON_CLI_ERROR: &str = "execution_canary_quote_pnl_cli_error";
const REASON_CONFIG_UNREADABLE: &str = "execution_canary_quote_pnl_config_unreadable";
const REASON_DB_UNREADABLE: &str = "execution_canary_quote_pnl_db_unreadable";
const REASON_JSON_REQUIRED: &str = "execution_canary_quote_pnl_json_required";
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 200;
const DEFAULT_SINCE_HOURS: i64 = 24;
const MAX_SINCE_HOURS: i64 = 168;
const TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON: &str =
    "retry_after_unknown_submit_timeout";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub limit: u32,
    pub since: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub metis_diagnostics: bool,
}

#[derive(Debug, Serialize)]
pub struct CanaryQuotePnlOperatorReport {
    pub config_loaded: bool,
    pub db_opened: bool,
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub summary: Option<ExecutionCanaryQuotePnlSummary>,
    pub provider_comparison: Option<ExecutionQuoteCanaryProviderComparisonSummary>,
    pub public_paid_comparison: Option<ExecutionQuoteCanaryPublicPaidComparisonSummary>,
    pub provider_selection: Option<ExecutionQuoteCanaryProviderSelectionSummary>,
    pub tiny_execution_proof: Option<ExecutionTinyProofReport>,
    pub sell_side_diagnostics: Option<SellSideDiagnosticsReport>,
    pub tiny_execution_gate: Option<TinyExecutionGate>,
    pub metis_diagnostics: Option<MetisDiagnosticsReport>,
}

impl CanaryQuotePnlOperatorReport {
    fn failed(reason_class: &str, error: Option<String>, as_of: DateTime<Utc>) -> Self {
        Self {
            config_loaded: false,
            db_opened: false,
            as_of,
            since: as_of - Duration::hours(DEFAULT_SINCE_HOURS),
            reason_class: reason_class.to_string(),
            error,
            summary: None,
            provider_comparison: None,
            public_paid_comparison: None,
            provider_selection: None,
            tiny_execution_proof: None,
            sell_side_diagnostics: None,
            tiny_execution_gate: None,
            metis_diagnostics: None,
        }
    }

    fn exit_code(&self) -> i32 {
        match self.reason_class.as_str() {
            REASON_OK => 0,
            _ => 1,
        }
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => CanaryQuotePnlOperatorReport::failed(
            REASON_JSON_REQUIRED,
            Some("--json is required for operator output".to_string()),
            as_of,
        ),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => {
            CanaryQuotePnlOperatorReport::failed(REASON_CLI_ERROR, Some(error.to_string()), as_of)
        }
    };

    println!(
        "{}",
        serde_json::to_string(&report).expect("operator report must serialize")
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
    let mut limit = DEFAULT_LIMIT;
    let mut since = None;
    let mut since_hours = DEFAULT_SINCE_HOURS;
    let mut metis_diagnostics = false;
    let mut iter = args.into_iter().map(Into::into);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(next_value(&mut iter, "--config")?)),
            "--db-path" => db_path = Some(PathBuf::from(next_value(&mut iter, "--db-path")?)),
            "--limit" => limit = parse_limit(&next_value(&mut iter, "--limit")?)?,
            "--since" => since = Some(parse_since(&next_value(&mut iter, "--since")?)?),
            "--since-hours" => {
                since_hours = parse_since_hours(&next_value(&mut iter, "--since-hours")?)?;
            }
            "--metis-diagnostics" => metis_diagnostics = true,
            "--json" => json = true,
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    if config_path.is_none() && db_path.is_none() {
        return Err(anyhow!("either --config or --db-path is required"));
    }

    Ok(Cli {
        config_path,
        db_path,
        json,
        limit,
        since,
        since_hours,
        metis_diagnostics,
    })
}

pub fn build_report_from_db_path(
    db_path: &Path,
    as_of: DateTime<Utc>,
) -> CanaryQuotePnlOperatorReport {
    build_report(
        Cli {
            config_path: None,
            db_path: Some(db_path.to_path_buf()),
            json: true,
            limit: DEFAULT_LIMIT,
            since: None,
            since_hours: DEFAULT_SINCE_HOURS,
            metis_diagnostics: false,
        },
        as_of,
    )
}

pub fn build_report_from_config_path(
    config_path: &Path,
    as_of: DateTime<Utc>,
) -> CanaryQuotePnlOperatorReport {
    build_report(
        Cli {
            config_path: Some(config_path.to_path_buf()),
            db_path: None,
            json: true,
            limit: DEFAULT_LIMIT,
            since: None,
            since_hours: DEFAULT_SINCE_HOURS,
            metis_diagnostics: false,
        },
        as_of,
    )
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> CanaryQuotePnlOperatorReport {
    let since = cli
        .since
        .unwrap_or(as_of - Duration::hours(cli.since_hours));
    let context = match resolve_report_context(&cli) {
        Ok(context) => context,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_CONFIG_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };

    let store = match SqliteStore::open_read_only(&context.db_path) {
        Ok(store) => store,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let summary = match store.execution_canary_quote_pnl_summary(as_of, since, cli.limit) {
        Ok(summary) => summary,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let canary_status = match store.execution_canary_status_report(as_of) {
        Ok(report) => report,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let canary_readiness = match store.execution_canary_readiness_summary(as_of) {
        Ok(report) => report,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let provider_comparison =
        match store.execution_quote_canary_provider_comparison_summary(as_of, since, cli.limit) {
            Ok(summary) => summary,
            Err(error) => {
                return CanaryQuotePnlOperatorReport::failed(
                    REASON_DB_UNREADABLE,
                    Some(error.to_string()),
                    as_of,
                );
            }
        };
    let public_paid_comparison = match store
        .execution_quote_canary_public_paid_comparison_summary(as_of, since, cli.limit)
    {
        Ok(summary) => summary,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let provider_selection =
        match store.execution_quote_canary_provider_selection_summary(as_of, since, cli.limit) {
            Ok(summary) => summary,
            Err(error) => {
                return CanaryQuotePnlOperatorReport::failed(
                    REASON_DB_UNREADABLE,
                    Some(error.to_string()),
                    as_of,
                );
            }
        };
    let tiny_execution_proof = match store.execution_tiny_proof_report(as_of, since, cli.limit) {
        Ok(report) => report,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let sell_side_diagnostics = build_sell_side_diagnostics(&tiny_execution_proof);
    let recent_loss =
        match store.execution_canary_realized_loss_sol_since(as_of - Duration::hours(24)) {
            Ok(loss) => loss,
            Err(error) => {
                return CanaryQuotePnlOperatorReport::failed(
                    REASON_DB_UNREADABLE,
                    Some(error.to_string()),
                    as_of,
                );
            }
        };
    let max_submit_attempts = context
        .config
        .as_ref()
        .map(|config| config.execution.max_submit_attempts)
        .unwrap_or(1);
    let submit_risk = match store.execution_canary_submit_risk_summary(
        as_of,
        TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON,
        max_submit_attempts,
    ) {
        Ok(summary) => summary,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let runtime_root = runtime_root_from_db_path(&context.db_path);
    let tiny_execution_gate = build_tiny_execution_gate(
        &summary,
        &canary_status,
        &canary_readiness,
        &submit_risk,
        recent_loss,
        as_of,
        context.config.as_ref().map(|config| &config.execution),
        runtime_root.as_deref(),
    );
    let metis_diagnostics = context.config.as_ref().and_then(|config| {
        cli.metis_diagnostics.then(|| {
            build_metis_diagnostics(
                &config.execution,
                &tiny_execution_proof,
                &provider_selection,
                as_of,
            )
        })
    });

    CanaryQuotePnlOperatorReport {
        config_loaded: cli.config_path.is_some(),
        db_opened: true,
        as_of,
        since,
        reason_class: REASON_OK.to_string(),
        error: None,
        summary: Some(summary),
        provider_comparison: Some(provider_comparison),
        public_paid_comparison: Some(public_paid_comparison),
        provider_selection: Some(provider_selection),
        tiny_execution_proof: Some(tiny_execution_proof),
        sell_side_diagnostics: Some(sell_side_diagnostics),
        tiny_execution_gate: Some(tiny_execution_gate),
        metis_diagnostics,
    }
}

struct ReportContext {
    config: Option<AppConfig>,
    db_path: PathBuf,
}

fn resolve_report_context(cli: &Cli) -> Result<ReportContext> {
    if let Some(config_path) = &cli.config_path {
        let config = load_from_path(config_path)
            .with_context(|| format!("failed to load config: {}", config_path.display()))?;
        let db_path = cli
            .db_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(config.sqlite.path.clone()));
        return Ok(ReportContext {
            config: Some(config),
            db_path,
        });
    }

    let db_path = cli
        .db_path
        .clone()
        .ok_or_else(|| anyhow!("either --config or --db-path is required"))?;
    Ok(ReportContext {
        config: None,
        db_path,
    })
}

fn runtime_root_from_db_path(db_path: &Path) -> Option<PathBuf> {
    let parent = db_path.parent()?;
    if parent.file_name().and_then(|name| name.to_str()) == Some("state") {
        return parent.parent().map(Path::to_path_buf);
    }
    Some(parent.to_path_buf())
}
