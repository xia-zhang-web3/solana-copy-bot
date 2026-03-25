use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig, ExecutionConfig};
use copybot_execution::auth::compute_hmac_signature_hex;
use copybot_storage::{ExecutionDryRunRehearsalWrite, SqliteStore};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

#[path = "../config_contract.rs"]
mod config_contract;
#[path = "../secrets.rs"]
mod secrets;

const USAGE: &str = "usage: copybot_execution_dry_run_rehearsal --config <path> [--route <route>] [--token <mint>] [--notional-sol <value>] [--history] [--limit <n>] [--json]";
const DEFAULT_REHEARSAL_TOKEN: &str = "So11111111111111111111111111111111111111112";
const DEFAULT_REHEARSAL_NOTIONAL_SOL: f64 = 0.01;
const MIN_PROBE_TIMEOUT_MS: u64 = 1_000;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for execution dry-run rehearsal")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    route_override: Option<String>,
    token: String,
    notional_sol: f64,
    history: bool,
    limit: usize,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExecutionDryRunRehearsalVerdict {
    RehearsalGreen,
    RehearsalGreenWithBusinessReject,
    RehearsalBlockedByAdapterContract,
    RehearsalBlockedByConnectivity,
    RehearsalBlockedByPolicyEcho,
    RehearsalBlockedByStaticContract,
    RehearsalInputInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RpcPreflightClassification {
    Reachable,
    ConnectivityBlocked,
    Skipped,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AdapterRehearsalClassification {
    Accepted,
    BusinessReject,
    ContractReject,
    PolicyEchoMissing,
    ConnectivityBlocked,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
struct RehearsalIntentSummary {
    route: String,
    token: String,
    notional_sol: f64,
    side: String,
}

#[derive(Debug, Clone, Serialize)]
struct RoutePolicyEnvelope {
    route: String,
    slippage_bps_cap: Option<f64>,
    tip_lamports: Option<u64>,
    compute_unit_limit: Option<u64>,
    compute_unit_price_micro_lamports: Option<u64>,
    policy_echo_required: bool,
    simulate_before_submit: bool,
    dynamic_cu_price_enabled: bool,
    dynamic_tip_lamports_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RpcPreflightReport {
    required: bool,
    attempted: bool,
    reachable: bool,
    classification: RpcPreflightClassification,
    timeout_ms: u64,
    successful_endpoint_role: Option<String>,
    detail: String,
    slot: Option<u64>,
    blockhash: Option<String>,
    signer_balance_lamports: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct AdapterRehearsalReport {
    required: bool,
    attempted: bool,
    reachable: bool,
    contract_valid: bool,
    policy_echo_present: bool,
    route_echo_present: bool,
    contract_version_echo_present: bool,
    accepted: Option<bool>,
    classification: AdapterRehearsalClassification,
    timeout_ms: u64,
    endpoint_role: Option<String>,
    endpoint_label: Option<String>,
    http_status: Option<u16>,
    echoed_route: Option<String>,
    echoed_contract_version: Option<String>,
    response_status: Option<String>,
    response_code: Option<String>,
    response_detail: Option<String>,
    response_slippage_bps: Option<f64>,
    response_tip_lamports: Option<u64>,
    response_compute_unit_limit: Option<u64>,
    response_compute_unit_price_micro_lamports: Option<u64>,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutionDryRunRehearsalReport {
    generated_at: DateTime<Utc>,
    rehearsal_id: Option<i64>,
    config_path: String,
    db_path: String,
    execution_enabled: bool,
    execution_dry_run_only: bool,
    stage3_gate_not_evaluated: bool,
    mode: String,
    signer_pubkey_configured: bool,
    config_valid: bool,
    connectivity_valid: bool,
    adapter_contract_valid: bool,
    signer_contract_valid: bool,
    policy_contract_valid: bool,
    route_contract_valid: bool,
    ready_for_dry_run: bool,
    would_be_admissible_for_later_tiny_live: bool,
    verdict: ExecutionDryRunRehearsalVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    intent: RehearsalIntentSummary,
    route_policy: RoutePolicyEnvelope,
    rpc_preflight: RpcPreflightReport,
    adapter_rehearsal: AdapterRehearsalReport,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutionDryRunRehearsalHistoryEntry {
    rehearsal_id: i64,
    rehearsed_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    execution_mode: String,
    route: String,
    token: String,
    notional_sol: f64,
    ready_for_dry_run: bool,
    would_be_admissible_for_later_tiny_live: bool,
    adapter_result_classification: String,
    adapter_accepted: Option<bool>,
    connectivity_valid: bool,
    policy_echo_present: bool,
    blockers: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutionDryRunRehearsalHistoryReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    db_path: String,
    records_loaded: usize,
    latest_rehearsed_at: Option<DateTime<Utc>>,
    verdict_counts: BTreeMap<String, usize>,
    rehearsals: Vec<ExecutionDryRunRehearsalHistoryEntry>,
}

#[derive(Debug, Clone)]
struct SelectedIntent {
    route: String,
    token: String,
    notional_sol: f64,
}

#[derive(Debug, Clone)]
struct AdapterEndpoint {
    role: String,
    url: String,
}

#[derive(Debug, Clone)]
struct AdapterResponseSummary {
    accepted: Option<bool>,
    classification: AdapterRehearsalClassification,
    detail: String,
    route_echo_present: bool,
    contract_version_echo_present: bool,
    policy_echo_present: bool,
    echoed_route: Option<String>,
    echoed_contract_version: Option<String>,
    response_status: Option<String>,
    response_code: Option<String>,
    response_detail: Option<String>,
    response_slippage_bps: Option<f64>,
    response_tip_lamports: Option<u64>,
    response_compute_unit_limit: Option<u64>,
    response_compute_unit_price_micro_lamports: Option<u64>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut route_override: Option<String> = None;
    let mut token = DEFAULT_REHEARSAL_TOKEN.to_string();
    let mut notional_sol = DEFAULT_REHEARSAL_NOTIONAL_SOL;
    let mut history = false;
    let mut limit = 10usize;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--route" => route_override = Some(parse_string_arg("--route", args.next())?),
            "--token" => token = parse_string_arg("--token", args.next())?,
            "--notional-sol" => notional_sol = parse_f64_arg("--notional-sol", args.next())?,
            "--history" => history = true,
            "--limit" => limit = parse_usize_arg("--limit", args.next())?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if !notional_sol.is_finite() || notional_sol <= 0.0 {
        bail!("--notional-sol must be finite and > 0");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        route_override,
        token,
        notional_sol,
        history,
        limit,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid usize value for {flag}: {raw}"))
}

fn parse_f64_arg(flag: &str, value: Option<String>) -> Result<f64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<f64>()
        .with_context(|| format!("invalid f64 value for {flag}: {raw}"))
}

async fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(&config.config_path, &loaded_config.sqlite.path);
    let store = SqliteStore::open(&db_path).with_context(|| {
        format!(
            "failed opening rehearsal history store {}",
            db_path.display()
        )
    })?;

    if config.history {
        let report = build_history_report(&store, &config.config_path, &db_path, config.limit)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing execution dry-run rehearsal history json")
        } else {
            Ok(render_history_human(&report))
        }
    } else {
        let report = evaluate_execution_dry_run_rehearsal(
            &config.config_path,
            &db_path,
            &loaded_config,
            config.route_override.as_deref(),
            config.token.as_str(),
            config.notional_sol,
        )
        .await?;
        let row = store
            .append_execution_dry_run_rehearsal(&persisted_row_for_report(&report)?)
            .context("failed persisting execution dry-run rehearsal history")?;
        let mut persisted = report.clone();
        persisted.rehearsal_id = Some(row.rehearsal_id);
        if config.json {
            serde_json::to_string_pretty(&persisted)
                .context("failed serializing execution dry-run rehearsal json")
        } else {
            Ok(render_human(&persisted))
        }
    }
}

async fn evaluate_execution_dry_run_rehearsal(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    route_override: Option<&str>,
    token: &str,
    notional_sol: f64,
) -> Result<ExecutionDryRunRehearsalReport> {
    let _runtime_contract_validator: fn(&ExecutionConfig, &str) -> Result<()> =
        config_contract::validate_execution_runtime_contract;
    let mut execution = loaded_config.execution.clone();
    let secret_resolution_error = resolve_execution_secrets_for_audit(&mut execution, config_path)
        .err()
        .map(|error| error.to_string());
    let mut assessment =
        config_contract::assess_execution_static_contract(&execution, &loaded_config.system.env);
    if let Some(error) = secret_resolution_error.as_deref() {
        apply_secret_resolution_error(&mut assessment, error);
    }

    let mode = normalize_mode(
        assessment
            .mode
            .as_deref()
            .unwrap_or(execution.mode.as_str()),
    );
    let selected_intent =
        select_intent(&execution, &assessment, route_override, token, notional_sol)?;
    let route_policy = build_route_policy(&execution, selected_intent.route.as_str());
    let probe_timeout_ms = execution_probe_timeout_ms(&execution);

    let rpc_preflight = probe_rpc_preflight(&execution, mode.as_str(), probe_timeout_ms).await;
    let adapter_rehearsal = rehearse_adapter(
        &execution,
        mode.as_str(),
        &assessment,
        selected_intent.clone(),
        probe_timeout_ms,
    )
    .await;

    let connectivity_valid = (!rpc_preflight.required || rpc_preflight.reachable)
        && (!adapter_rehearsal.required || adapter_rehearsal.reachable);
    let final_adapter_contract_valid = assessment.adapter_contract_valid
        && adapter_rehearsal.contract_valid
        && adapter_rehearsal.classification != AdapterRehearsalClassification::PolicyEchoMissing;
    let ready_for_dry_run = mode == "adapter_submit_confirm"
        && assessment.mode_compatible
        && assessment.signer_contract_valid
        && assessment.policy_contract_valid
        && assessment.route_contract_valid
        && final_adapter_contract_valid
        && connectivity_valid
        && rpc_preflight.classification != RpcPreflightClassification::Skipped
        && adapter_rehearsal.classification != AdapterRehearsalClassification::Skipped;
    let would_be_admissible_for_later_tiny_live = ready_for_dry_run
        && matches!(
            adapter_rehearsal.classification,
            AdapterRehearsalClassification::Accepted
                | AdapterRehearsalClassification::BusinessReject
        );

    let verdict = derive_verdict(
        mode.as_str(),
        &assessment,
        &rpc_preflight,
        &adapter_rehearsal,
        selected_intent.route.as_str(),
    );
    let reason = derive_reason(&assessment, &rpc_preflight, &adapter_rehearsal, verdict);
    let blockers = collect_blockers(
        &assessment,
        &rpc_preflight,
        &adapter_rehearsal,
        verdict,
        &reason,
    );
    let warnings = collect_warnings(
        loaded_config.execution.enabled,
        ready_for_dry_run,
        would_be_admissible_for_later_tiny_live,
        &adapter_rehearsal,
    );

    Ok(ExecutionDryRunRehearsalReport {
        generated_at: Utc::now(),
        rehearsal_id: None,
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        execution_enabled: loaded_config.execution.enabled,
        execution_dry_run_only: true,
        stage3_gate_not_evaluated: true,
        mode,
        signer_pubkey_configured: !execution.execution_signer_pubkey.trim().is_empty(),
        config_valid: assessment.config_valid,
        connectivity_valid,
        adapter_contract_valid: final_adapter_contract_valid,
        signer_contract_valid: assessment.signer_contract_valid,
        policy_contract_valid: assessment.policy_contract_valid,
        route_contract_valid: assessment.route_contract_valid,
        ready_for_dry_run,
        would_be_admissible_for_later_tiny_live,
        verdict,
        reason,
        blockers,
        warnings,
        intent: RehearsalIntentSummary {
            route: selected_intent.route,
            token: selected_intent.token,
            notional_sol: selected_intent.notional_sol,
            side: "buy".to_string(),
        },
        route_policy,
        rpc_preflight,
        adapter_rehearsal,
    })
}

fn persisted_row_for_report(
    report: &ExecutionDryRunRehearsalReport,
) -> Result<ExecutionDryRunRehearsalWrite> {
    Ok(ExecutionDryRunRehearsalWrite {
        rehearsed_at: report.generated_at,
        execution_mode: report.mode.clone(),
        execution_enabled: report.execution_enabled,
        route: report.intent.route.clone(),
        token: report.intent.token.clone(),
        notional_sol: report.intent.notional_sol,
        signer_pubkey_configured: report.signer_pubkey_configured,
        config_valid: report.config_valid,
        connectivity_valid: report.connectivity_valid,
        adapter_contract_valid: report.adapter_contract_valid,
        policy_contract_valid: report.policy_contract_valid,
        route_contract_valid: report.route_contract_valid,
        ready_for_dry_run: report.ready_for_dry_run,
        would_be_admissible_for_later_tiny_live: report.would_be_admissible_for_later_tiny_live,
        rpc_preconditions_valid: report.rpc_preflight.reachable,
        rpc_slot: report.rpc_preflight.slot,
        rpc_blockhash: report.rpc_preflight.blockhash.clone(),
        rpc_signer_balance_lamports: report.rpc_preflight.signer_balance_lamports,
        adapter_result_classification: serde_json::to_string(
            &report.adapter_rehearsal.classification,
        )
        .unwrap_or_default()
        .trim_matches('"')
        .to_string(),
        adapter_accepted: report.adapter_rehearsal.accepted,
        adapter_detail: report.adapter_rehearsal.detail.clone(),
        policy_echo_present: report.adapter_rehearsal.policy_echo_present,
        route_echo_present: report.adapter_rehearsal.route_echo_present,
        contract_version_echo_present: report.adapter_rehearsal.contract_version_echo_present,
        response_slippage_bps: report.adapter_rehearsal.response_slippage_bps,
        response_tip_lamports: report.adapter_rehearsal.response_tip_lamports,
        response_compute_unit_limit: report.adapter_rehearsal.response_compute_unit_limit,
        response_compute_unit_price_micro_lamports: report
            .adapter_rehearsal
            .response_compute_unit_price_micro_lamports,
        verdict: serde_json::to_string(&report.verdict)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        reason: report.reason.clone(),
        blockers: report.blockers.clone(),
        warnings: report.warnings.clone(),
        rehearsal_json: serde_json::to_string(report)
            .context("failed serializing execution dry-run rehearsal report")?,
    })
}

fn build_history_report(
    store: &SqliteStore,
    config_path: &Path,
    db_path: &Path,
    limit: usize,
) -> Result<ExecutionDryRunRehearsalHistoryReport> {
    let rehearsals = store
        .list_execution_dry_run_rehearsals(limit)
        .context("failed loading execution dry-run rehearsal history")?;
    let latest_rehearsed_at = rehearsals.first().map(|row| row.rehearsed_at);
    let mut verdict_counts = BTreeMap::new();
    let entries = rehearsals
        .into_iter()
        .map(|row| {
            *verdict_counts.entry(row.verdict.clone()).or_insert(0) += 1;
            ExecutionDryRunRehearsalHistoryEntry {
                rehearsal_id: row.rehearsal_id,
                rehearsed_at: row.rehearsed_at,
                verdict: row.verdict,
                reason: row.reason,
                execution_mode: row.execution_mode,
                route: row.route,
                token: row.token,
                notional_sol: row.notional_sol,
                ready_for_dry_run: row.ready_for_dry_run,
                would_be_admissible_for_later_tiny_live: row
                    .would_be_admissible_for_later_tiny_live,
                adapter_result_classification: row.adapter_result_classification,
                adapter_accepted: row.adapter_accepted,
                connectivity_valid: row.connectivity_valid,
                policy_echo_present: row.policy_echo_present,
                blockers: row.blockers,
                warnings: row.warnings,
            }
        })
        .collect::<Vec<_>>();
    Ok(ExecutionDryRunRehearsalHistoryReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        records_loaded: entries.len(),
        latest_rehearsed_at,
        verdict_counts,
        rehearsals: entries,
    })
}

fn resolve_execution_secrets_for_audit(
    execution: &mut ExecutionConfig,
    config_path: &Path,
) -> Result<()> {
    let original_enabled = execution.enabled;
    execution.enabled = true;
    let result = secrets::resolve_execution_adapter_secrets(execution, config_path);
    execution.enabled = original_enabled;
    result
}

fn apply_secret_resolution_error(
    assessment: &mut config_contract::ExecutionStaticContractAssessment,
    error: &str,
) {
    if error.contains("submit_dynamic_cu_price_api_auth_token") {
        assessment.policy_contract_valid = false;
        assessment.config_valid = false;
        if assessment.policy_error.is_none() {
            assessment.policy_error = Some(error.to_string());
        }
        return;
    }

    assessment.adapter_contract_valid = false;
    assessment.config_valid = false;
    if assessment.adapter_error.is_none() {
        assessment.adapter_error = Some(error.to_string());
    }
}

fn normalize_mode(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "<empty>".to_string()
    } else {
        normalized
    }
}

fn normalize_route(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "paper".to_string()
    } else {
        normalized
    }
}

fn execution_probe_timeout_ms(config: &ExecutionConfig) -> u64 {
    config
        .submit_timeout_ms
        .max(config.poll_interval_ms)
        .max(MIN_PROBE_TIMEOUT_MS)
}

fn select_intent(
    execution: &ExecutionConfig,
    assessment: &config_contract::ExecutionStaticContractAssessment,
    route_override: Option<&str>,
    token: &str,
    notional_sol: f64,
) -> Result<SelectedIntent> {
    let route = route_override
        .map(normalize_route)
        .unwrap_or_else(|| normalize_route(execution.default_route.as_str()));
    if !token.trim().is_ascii() || token.trim().is_empty() {
        bail!("rehearsal token must be non-empty ascii");
    }
    if !assessment.route_contract_valid {
        return Ok(SelectedIntent {
            route,
            token: token.trim().to_string(),
            notional_sol,
        });
    }
    let allowed_routes = execution
        .submit_allowed_routes
        .iter()
        .map(|value| normalize_route(value))
        .collect::<Vec<_>>();
    if !allowed_routes.iter().any(|allowed| allowed == &route) {
        bail!(
            "rehearsal route={} is not in execution.submit_allowed_routes={}",
            route,
            allowed_routes.join(",")
        );
    }
    Ok(SelectedIntent {
        route,
        token: token.trim().to_string(),
        notional_sol,
    })
}

fn build_route_policy(config: &ExecutionConfig, route: &str) -> RoutePolicyEnvelope {
    RoutePolicyEnvelope {
        route: route.to_string(),
        slippage_bps_cap: config.submit_route_max_slippage_bps.get(route).copied(),
        tip_lamports: config.submit_route_tip_lamports.get(route).copied(),
        compute_unit_limit: config
            .submit_route_compute_unit_limit
            .get(route)
            .copied()
            .map(u64::from),
        compute_unit_price_micro_lamports: config
            .submit_route_compute_unit_price_micro_lamports
            .get(route)
            .copied(),
        policy_echo_required: config.submit_adapter_require_policy_echo,
        simulate_before_submit: config.simulate_before_submit,
        dynamic_cu_price_enabled: config.submit_dynamic_cu_price_enabled,
        dynamic_tip_lamports_enabled: config.submit_dynamic_tip_lamports_enabled,
    }
}

fn build_endpoints(primary: &str, fallback: &str) -> Vec<AdapterEndpoint> {
    let mut endpoints = Vec::new();
    let primary = primary.trim();
    if !primary.is_empty() {
        endpoints.push(AdapterEndpoint {
            role: "primary".to_string(),
            url: primary.to_string(),
        });
    }
    let fallback = fallback.trim();
    if !fallback.is_empty() && fallback != primary {
        endpoints.push(AdapterEndpoint {
            role: "fallback".to_string(),
            url: fallback.to_string(),
        });
    }
    endpoints
}

async fn probe_rpc_preflight(
    execution: &ExecutionConfig,
    mode: &str,
    timeout_ms: u64,
) -> RpcPreflightReport {
    if mode != "adapter_submit_confirm" {
        return RpcPreflightReport {
            required: false,
            attempted: false,
            reachable: false,
            classification: RpcPreflightClassification::Skipped,
            timeout_ms,
            successful_endpoint_role: None,
            detail: "rpc_preflight_not_required_for_mode".to_string(),
            slot: None,
            blockhash: None,
            signer_balance_lamports: None,
        };
    }
    let client = match Client::builder()
        .timeout(StdDuration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return RpcPreflightReport {
                required: true,
                attempted: false,
                reachable: false,
                classification: RpcPreflightClassification::ConnectivityBlocked,
                timeout_ms,
                successful_endpoint_role: None,
                detail: format!("failed_building_rpc_preflight_client: {error}"),
                slot: None,
                blockhash: None,
                signer_balance_lamports: None,
            };
        }
    };

    let mut failures = Vec::new();
    for (role, endpoint) in [
        ("primary", execution.rpc_http_url.trim()),
        ("fallback", execution.rpc_fallback_http_url.trim()),
    ] {
        if endpoint.is_empty() {
            continue;
        }
        match probe_single_rpc_preflight(
            &client,
            endpoint,
            execution.execution_signer_pubkey.trim(),
        )
        .await
        {
            Ok((slot, blockhash, balance)) => {
                return RpcPreflightReport {
                    required: true,
                    attempted: true,
                    reachable: true,
                    classification: RpcPreflightClassification::Reachable,
                    timeout_ms,
                    successful_endpoint_role: Some(role.to_string()),
                    detail: format!("rpc_get_latest_blockhash_ok via {role}"),
                    slot: Some(slot),
                    blockhash: Some(blockhash),
                    signer_balance_lamports: balance,
                };
            }
            Err(error) => failures.push(format!("{role}: {error}")),
        }
    }

    RpcPreflightReport {
        required: true,
        attempted: true,
        reachable: false,
        classification: RpcPreflightClassification::ConnectivityBlocked,
        timeout_ms,
        successful_endpoint_role: None,
        detail: if failures.is_empty() {
            "no_rpc_endpoints_configured".to_string()
        } else {
            failures.join(" | ")
        },
        slot: None,
        blockhash: None,
        signer_balance_lamports: None,
    }
}

async fn probe_single_rpc_preflight(
    client: &Client,
    endpoint: &str,
    signer_pubkey: &str,
) -> Result<(u64, String, Option<u64>)> {
    let latest_response = client
        .post(endpoint)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "processed"}]
        }))
        .send()
        .await
        .with_context(|| format!("request_failed endpoint={endpoint} method=getLatestBlockhash"))?;
    let latest_status = latest_response.status();
    let latest_body: Value = latest_response
        .json()
        .await
        .with_context(|| format!("invalid_json endpoint={endpoint} method=getLatestBlockhash"))?;
    if !latest_status.is_success() {
        bail!(
            "http_status={} endpoint={} method=getLatestBlockhash",
            latest_status.as_u16(),
            endpoint
        );
    }
    if let Some(error) = latest_body.get("error") {
        bail!(
            "rpc_error endpoint={} method=getLatestBlockhash error={}",
            endpoint,
            error
        );
    }
    let context = latest_body
        .get("result")
        .and_then(|value| value.get("context"))
        .and_then(|value| value.get("slot"))
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing_slot endpoint={endpoint} method=getLatestBlockhash"))?;
    let blockhash = latest_body
        .get("result")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.get("blockhash"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing_blockhash endpoint={endpoint} method=getLatestBlockhash"))?
        .to_string();

    let signer_balance_lamports = if signer_pubkey.is_empty() {
        None
    } else {
        let balance_response = client
            .post(endpoint)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [signer_pubkey, {"commitment": "processed"}]
            }))
            .send()
            .await
            .with_context(|| format!("request_failed endpoint={endpoint} method=getBalance"))?;
        let balance_status = balance_response.status();
        let balance_body: Value = balance_response
            .json()
            .await
            .with_context(|| format!("invalid_json endpoint={endpoint} method=getBalance"))?;
        if !balance_status.is_success() {
            bail!(
                "http_status={} endpoint={} method=getBalance",
                balance_status.as_u16(),
                endpoint
            );
        }
        if let Some(error) = balance_body.get("error") {
            bail!(
                "rpc_error endpoint={} method=getBalance error={}",
                endpoint,
                error
            );
        }
        Some(
            balance_body
                .get("result")
                .and_then(|value| value.get("value"))
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    anyhow!("missing_u64_result endpoint={endpoint} method=getBalance")
                })?,
        )
    };

    Ok((context, blockhash, signer_balance_lamports))
}

async fn rehearse_adapter(
    execution: &ExecutionConfig,
    mode: &str,
    assessment: &config_contract::ExecutionStaticContractAssessment,
    intent: SelectedIntent,
    timeout_ms: u64,
) -> AdapterRehearsalReport {
    if mode != "adapter_submit_confirm" {
        return skipped_adapter_rehearsal(timeout_ms, "adapter_rehearsal_not_required_for_mode");
    }
    if !assessment.mode_compatible {
        return skipped_adapter_rehearsal(
            timeout_ms,
            "adapter_rehearsal_skipped_due_to_mode_contract",
        );
    }
    if !assessment.adapter_contract_valid {
        return skipped_adapter_rehearsal(
            timeout_ms,
            "adapter_rehearsal_skipped_due_to_static_adapter_contract",
        );
    }
    if !assessment.policy_contract_valid || !assessment.route_contract_valid {
        return skipped_adapter_rehearsal(
            timeout_ms,
            "adapter_rehearsal_skipped_due_to_policy_or_route_contract",
        );
    }

    let client = match Client::builder()
        .timeout(StdDuration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return AdapterRehearsalReport {
                required: true,
                attempted: false,
                reachable: false,
                contract_valid: false,
                policy_echo_present: false,
                route_echo_present: false,
                contract_version_echo_present: false,
                accepted: None,
                classification: AdapterRehearsalClassification::ConnectivityBlocked,
                timeout_ms,
                endpoint_role: None,
                endpoint_label: None,
                http_status: None,
                echoed_route: None,
                echoed_contract_version: None,
                response_status: None,
                response_code: None,
                response_detail: None,
                response_slippage_bps: None,
                response_tip_lamports: None,
                response_compute_unit_limit: None,
                response_compute_unit_price_micro_lamports: None,
                detail: format!("failed_building_adapter_rehearsal_client: {error}"),
            };
        }
    };

    let signal_ts = Utc::now();
    let signal_id = "execution_dry_run_rehearsal";
    let payload = json!({
        "action": "simulate",
        "contract_version": execution.submit_adapter_contract_version.trim(),
        "request_id": format!("execution-dry-run-rehearsal-{}", Utc::now().timestamp_millis()),
        "signal_id": signal_id,
        "side": "buy",
        "token": intent.token,
        "notional_sol": intent.notional_sol,
        "signal_ts": signal_ts.to_rfc3339(),
        "route": intent.route,
        "dry_run": true
    });
    let endpoints = build_endpoints(
        execution.submit_adapter_http_url.as_str(),
        execution.submit_adapter_fallback_http_url.as_str(),
    );
    let payload_json = match serde_json::to_string(&payload) {
        Ok(value) => value,
        Err(error) => {
            return AdapterRehearsalReport {
                required: true,
                attempted: false,
                reachable: false,
                contract_valid: false,
                policy_echo_present: false,
                route_echo_present: false,
                contract_version_echo_present: false,
                accepted: None,
                classification: AdapterRehearsalClassification::ContractReject,
                timeout_ms,
                endpoint_role: None,
                endpoint_label: None,
                http_status: None,
                echoed_route: None,
                echoed_contract_version: None,
                response_status: None,
                response_code: None,
                response_detail: None,
                response_slippage_bps: None,
                response_tip_lamports: None,
                response_compute_unit_limit: None,
                response_compute_unit_price_micro_lamports: None,
                detail: format!("failed_serializing_rehearsal_payload: {error}"),
            };
        }
    };

    let mut failures = Vec::new();
    for endpoint in endpoints {
        match rehearse_single_endpoint(
            &client,
            &endpoint,
            &payload_json,
            execution,
            intent.route.as_str(),
            timeout_ms,
        )
        .await
        {
            EndpointOutcome::Terminal(report) => return report,
            EndpointOutcome::Retryable(reason) => {
                failures.push(format!("{}: {}", endpoint.role, reason))
            }
        }
    }

    AdapterRehearsalReport {
        required: true,
        attempted: true,
        reachable: false,
        contract_valid: false,
        policy_echo_present: false,
        route_echo_present: false,
        contract_version_echo_present: false,
        accepted: None,
        classification: AdapterRehearsalClassification::ConnectivityBlocked,
        timeout_ms,
        endpoint_role: None,
        endpoint_label: None,
        http_status: None,
        echoed_route: None,
        echoed_contract_version: None,
        response_status: None,
        response_code: None,
        response_detail: None,
        response_slippage_bps: None,
        response_tip_lamports: None,
        response_compute_unit_limit: None,
        response_compute_unit_price_micro_lamports: None,
        detail: if failures.is_empty() {
            "no_adapter_endpoints_configured".to_string()
        } else {
            failures.join(" | ")
        },
    }
}

enum EndpointOutcome {
    Terminal(AdapterRehearsalReport),
    Retryable(String),
}

async fn rehearse_single_endpoint(
    client: &Client,
    endpoint: &AdapterEndpoint,
    payload_json: &str,
    execution: &ExecutionConfig,
    expected_route: &str,
    timeout_ms: u64,
) -> EndpointOutcome {
    let endpoint_label = redacted_endpoint_label(endpoint.url.as_str());
    let mut request = client
        .post(endpoint.url.as_str())
        .header("content-type", "application/json")
        .body(payload_json.to_string());
    if !execution.submit_adapter_auth_token.trim().is_empty() {
        request = request.bearer_auth(execution.submit_adapter_auth_token.trim());
    }
    if !execution.submit_adapter_hmac_key_id.trim().is_empty()
        && !execution.submit_adapter_hmac_secret.trim().is_empty()
    {
        let timestamp_sec = Utc::now().timestamp();
        let nonce = format!("rehearsal-{}", timestamp_sec);
        let signature_payload = format!(
            "{}\n{}\n{}\n{}",
            timestamp_sec, execution.submit_adapter_hmac_ttl_sec, nonce, payload_json
        );
        let signature = match compute_hmac_signature_hex(
            execution.submit_adapter_hmac_secret.as_bytes(),
            signature_payload.as_bytes(),
        ) {
            Ok(value) => value,
            Err(error) => {
                return EndpointOutcome::Terminal(AdapterRehearsalReport {
                    required: true,
                    attempted: true,
                    reachable: false,
                    contract_valid: false,
                    policy_echo_present: false,
                    route_echo_present: false,
                    contract_version_echo_present: false,
                    accepted: None,
                    classification: AdapterRehearsalClassification::ContractReject,
                    timeout_ms,
                    endpoint_role: Some(endpoint.role.clone()),
                    endpoint_label: Some(endpoint_label),
                    http_status: None,
                    echoed_route: None,
                    echoed_contract_version: None,
                    response_status: None,
                    response_code: None,
                    response_detail: None,
                    response_slippage_bps: None,
                    response_tip_lamports: None,
                    response_compute_unit_limit: None,
                    response_compute_unit_price_micro_lamports: None,
                    detail: format!("failed_generating_hmac_signature: {error}"),
                });
            }
        };
        request = request
            .header(
                "x-copybot-key-id",
                execution.submit_adapter_hmac_key_id.trim(),
            )
            .header("x-copybot-timestamp", timestamp_sec.to_string())
            .header(
                "x-copybot-auth-ttl-sec",
                execution.submit_adapter_hmac_ttl_sec.to_string(),
            )
            .header("x-copybot-nonce", nonce)
            .header("x-copybot-signature", signature)
            .header("x-copybot-signature-alg", "hmac-sha256-v1");
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return EndpointOutcome::Retryable(format!(
                "endpoint={} request_error_class={}",
                endpoint_label,
                classify_request_error(&error)
            ));
        }
    };
    let status = response.status();
    if status.as_u16() == 429 || status.is_server_error() {
        let body_text = response.text().await.unwrap_or_default();
        return EndpointOutcome::Retryable(format!(
            "endpoint={} simulate_http_unavailable status={} body={}",
            endpoint_label,
            status.as_u16(),
            body_text
        ));
    }
    let http_status = status.as_u16();
    let body = match response.json::<Value>().await {
        Ok(body) => body,
        Err(error) => {
            return EndpointOutcome::Terminal(AdapterRehearsalReport {
                required: true,
                attempted: true,
                reachable: true,
                contract_valid: false,
                policy_echo_present: false,
                route_echo_present: false,
                contract_version_echo_present: false,
                accepted: None,
                classification: AdapterRehearsalClassification::ContractReject,
                timeout_ms,
                endpoint_role: Some(endpoint.role.clone()),
                endpoint_label: Some(endpoint_label),
                http_status: Some(http_status),
                echoed_route: None,
                echoed_contract_version: None,
                response_status: None,
                response_code: None,
                response_detail: None,
                response_slippage_bps: None,
                response_tip_lamports: None,
                response_compute_unit_limit: None,
                response_compute_unit_price_micro_lamports: None,
                detail: format!("simulation_invalid_json: {}", error),
            });
        }
    };
    if !status.is_success() {
        let detail = body
            .get("detail")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("adapter rejected simulate request");
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("simulation_http_rejected");
        return EndpointOutcome::Terminal(AdapterRehearsalReport {
            required: true,
            attempted: true,
            reachable: true,
            contract_valid: false,
            policy_echo_present: false,
            route_echo_present: false,
            contract_version_echo_present: false,
            accepted: Some(false),
            classification: AdapterRehearsalClassification::ContractReject,
            timeout_ms,
            endpoint_role: Some(endpoint.role.clone()),
            endpoint_label: Some(endpoint_label),
            http_status: Some(http_status),
            echoed_route: None,
            echoed_contract_version: None,
            response_status: body
                .get("status")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase()),
            response_code: Some(code.to_string()),
            response_detail: Some(detail.to_string()),
            response_slippage_bps: parse_optional_f64_field(
                &body,
                &["slippage_bps", "route_slippage_cap_bps"],
            ),
            response_tip_lamports: parse_optional_u64_field(&body, &["tip_lamports"]),
            response_compute_unit_limit: parse_optional_u64_field(&body, &["compute_unit_limit"])
                .or_else(|| parse_nested_compute_budget_u64(&body, "cu_limit")),
            response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                &body,
                &["compute_unit_price_micro_lamports"],
            )
            .or_else(|| parse_nested_compute_budget_u64(&body, "cu_price_micro_lamports")),
            detail: format!("{}: {}", code, detail),
        });
    }

    let summary = parse_adapter_response_summary(
        &body,
        expected_route,
        execution.submit_adapter_contract_version.trim(),
        execution.submit_adapter_require_policy_echo,
    );
    EndpointOutcome::Terminal(AdapterRehearsalReport {
        required: true,
        attempted: true,
        reachable: true,
        contract_valid: matches!(
            summary.classification,
            AdapterRehearsalClassification::Accepted
                | AdapterRehearsalClassification::BusinessReject
        ),
        policy_echo_present: summary.policy_echo_present,
        route_echo_present: summary.route_echo_present,
        contract_version_echo_present: summary.contract_version_echo_present,
        accepted: summary.accepted,
        classification: summary.classification,
        timeout_ms,
        endpoint_role: Some(endpoint.role.clone()),
        endpoint_label: Some(endpoint_label),
        http_status: Some(http_status),
        echoed_route: summary.echoed_route,
        echoed_contract_version: summary.echoed_contract_version,
        response_status: summary.response_status,
        response_code: summary.response_code,
        response_detail: summary.response_detail,
        response_slippage_bps: summary.response_slippage_bps,
        response_tip_lamports: summary.response_tip_lamports,
        response_compute_unit_limit: summary.response_compute_unit_limit,
        response_compute_unit_price_micro_lamports: summary
            .response_compute_unit_price_micro_lamports,
        detail: summary.detail,
    })
}

fn parse_adapter_response_summary(
    body: &Value,
    expected_route: &str,
    expected_contract_version: &str,
    require_policy_echo: bool,
) -> AdapterResponseSummary {
    let route = body
        .get("route")
        .and_then(Value::as_str)
        .map(|value| normalize_route(value));
    let contract_version = body
        .get("contract_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let route_echo_present = route.is_some();
    let contract_version_echo_present = contract_version.is_some();
    let policy_echo_present = route_echo_present && contract_version_echo_present;

    if require_policy_echo && !route_echo_present {
        return AdapterResponseSummary {
            accepted: Some(false),
            classification: AdapterRehearsalClassification::PolicyEchoMissing,
            detail: "simulation_policy_echo_missing: route".to_string(),
            route_echo_present,
            contract_version_echo_present,
            policy_echo_present,
            echoed_route: None,
            echoed_contract_version: contract_version,
            response_status: body
                .get("status")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase()),
            response_code: body
                .get("code")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            response_detail: body
                .get("detail")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            response_slippage_bps: parse_optional_f64_field(
                body,
                &["slippage_bps", "route_slippage_cap_bps"],
            ),
            response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
            response_compute_unit_limit: parse_optional_u64_field(body, &["compute_unit_limit"])
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
            response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                body,
                &["compute_unit_price_micro_lamports"],
            )
            .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
        };
    }
    if let Some(route) = route.as_deref() {
        if route != expected_route {
            return AdapterResponseSummary {
                accepted: Some(false),
                classification: AdapterRehearsalClassification::ContractReject,
                detail: format!(
                    "simulation_route_mismatch: response route={} expected={}",
                    route, expected_route
                ),
                route_echo_present,
                contract_version_echo_present,
                policy_echo_present,
                echoed_route: Some(route.to_string()),
                echoed_contract_version: contract_version,
                response_status: body
                    .get("status")
                    .and_then(Value::as_str)
                    .map(|value| value.trim().to_ascii_lowercase()),
                response_code: body
                    .get("code")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                response_detail: body
                    .get("detail")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                response_slippage_bps: parse_optional_f64_field(
                    body,
                    &["slippage_bps", "route_slippage_cap_bps"],
                ),
                response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
                response_compute_unit_limit: parse_optional_u64_field(
                    body,
                    &["compute_unit_limit"],
                )
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
                response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                    body,
                    &["compute_unit_price_micro_lamports"],
                )
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
            };
        }
    }

    if require_policy_echo && !contract_version_echo_present {
        return AdapterResponseSummary {
            accepted: Some(false),
            classification: AdapterRehearsalClassification::PolicyEchoMissing,
            detail: "simulation_policy_echo_missing: contract_version".to_string(),
            route_echo_present,
            contract_version_echo_present,
            policy_echo_present,
            echoed_route: route,
            echoed_contract_version: None,
            response_status: body
                .get("status")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase()),
            response_code: body
                .get("code")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            response_detail: body
                .get("detail")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            response_slippage_bps: parse_optional_f64_field(
                body,
                &["slippage_bps", "route_slippage_cap_bps"],
            ),
            response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
            response_compute_unit_limit: parse_optional_u64_field(body, &["compute_unit_limit"])
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
            response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                body,
                &["compute_unit_price_micro_lamports"],
            )
            .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
        };
    }
    if let Some(version) = contract_version.as_deref() {
        if version != expected_contract_version {
            return AdapterResponseSummary {
                accepted: Some(false),
                classification: AdapterRehearsalClassification::ContractReject,
                detail: format!(
                    "simulation_contract_version_mismatch: response={} expected={}",
                    version, expected_contract_version
                ),
                route_echo_present,
                contract_version_echo_present,
                policy_echo_present,
                echoed_route: route,
                echoed_contract_version: Some(version.to_string()),
                response_status: body
                    .get("status")
                    .and_then(Value::as_str)
                    .map(|value| value.trim().to_ascii_lowercase()),
                response_code: body
                    .get("code")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                response_detail: body
                    .get("detail")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                response_slippage_bps: parse_optional_f64_field(
                    body,
                    &["slippage_bps", "route_slippage_cap_bps"],
                ),
                response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
                response_compute_unit_limit: parse_optional_u64_field(
                    body,
                    &["compute_unit_limit"],
                )
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
                response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                    body,
                    &["compute_unit_price_micro_lamports"],
                )
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
            };
        }
    }

    let response_status = body
        .get("status")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase());
    let accepted_flag = body.get("accepted").and_then(Value::as_bool);
    let ok_flag = body.get("ok").and_then(Value::as_bool);
    let is_reject = matches!(
        response_status.as_deref(),
        Some("reject" | "rejected" | "error" | "failed" | "failure")
    ) || accepted_flag == Some(false)
        || ok_flag == Some(false);
    let response_code = body
        .get("code")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let response_detail = body
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if is_reject {
        let code = response_code
            .clone()
            .unwrap_or_else(|| "simulation_rejected".to_string());
        let detail = response_detail
            .clone()
            .unwrap_or_else(|| "adapter simulation rejected order".to_string());
        return AdapterResponseSummary {
            accepted: Some(false),
            classification: AdapterRehearsalClassification::BusinessReject,
            detail: format!("{}: {}", code, detail),
            route_echo_present,
            contract_version_echo_present,
            policy_echo_present,
            echoed_route: route,
            echoed_contract_version: contract_version,
            response_status,
            response_code,
            response_detail,
            response_slippage_bps: parse_optional_f64_field(
                body,
                &["slippage_bps", "route_slippage_cap_bps"],
            ),
            response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
            response_compute_unit_limit: parse_optional_u64_field(body, &["compute_unit_limit"])
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
            response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                body,
                &["compute_unit_price_micro_lamports"],
            )
            .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
        };
    }

    let is_known_success_status = matches!(
        response_status.as_deref(),
        Some("ok" | "accepted" | "success")
    );
    let is_known_status = is_known_success_status
        || matches!(
            response_status.as_deref(),
            Some("reject" | "rejected" | "error" | "failed" | "failure")
        );
    if response_status.is_some() && !is_known_status {
        return AdapterResponseSummary {
            accepted: Some(false),
            classification: AdapterRehearsalClassification::ContractReject,
            detail: format!(
                "simulation_invalid_status: {}",
                response_status.clone().unwrap_or_default()
            ),
            route_echo_present,
            contract_version_echo_present,
            policy_echo_present,
            echoed_route: route,
            echoed_contract_version: contract_version,
            response_status,
            response_code,
            response_detail,
            response_slippage_bps: parse_optional_f64_field(
                body,
                &["slippage_bps", "route_slippage_cap_bps"],
            ),
            response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
            response_compute_unit_limit: parse_optional_u64_field(body, &["compute_unit_limit"])
                .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
            response_compute_unit_price_micro_lamports: parse_optional_u64_field(
                body,
                &["compute_unit_price_micro_lamports"],
            )
            .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
        };
    }

    AdapterResponseSummary {
        accepted: Some(true),
        classification: AdapterRehearsalClassification::Accepted,
        detail: response_detail
            .clone()
            .unwrap_or_else(|| "adapter_simulation_ok".to_string()),
        route_echo_present,
        contract_version_echo_present,
        policy_echo_present,
        echoed_route: route,
        echoed_contract_version: contract_version,
        response_status,
        response_code,
        response_detail,
        response_slippage_bps: parse_optional_f64_field(
            body,
            &["slippage_bps", "route_slippage_cap_bps"],
        ),
        response_tip_lamports: parse_optional_u64_field(body, &["tip_lamports"]),
        response_compute_unit_limit: parse_optional_u64_field(body, &["compute_unit_limit"])
            .or_else(|| parse_nested_compute_budget_u64(body, "cu_limit")),
        response_compute_unit_price_micro_lamports: parse_optional_u64_field(
            body,
            &["compute_unit_price_micro_lamports"],
        )
        .or_else(|| parse_nested_compute_budget_u64(body, "cu_price_micro_lamports")),
    }
}

fn parse_optional_u64_field(body: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| body.get(*key))
        .and_then(Value::as_u64)
}

fn parse_optional_f64_field(body: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| body.get(*key))
        .and_then(Value::as_f64)
}

fn parse_nested_compute_budget_u64(body: &Value, key: &str) -> Option<u64> {
    body.get("compute_budget")
        .and_then(|value| value.get(key))
        .and_then(Value::as_u64)
}

fn skipped_adapter_rehearsal(timeout_ms: u64, detail: &str) -> AdapterRehearsalReport {
    AdapterRehearsalReport {
        required: true,
        attempted: false,
        reachable: false,
        contract_valid: false,
        policy_echo_present: false,
        route_echo_present: false,
        contract_version_echo_present: false,
        accepted: None,
        classification: AdapterRehearsalClassification::Skipped,
        timeout_ms,
        endpoint_role: None,
        endpoint_label: None,
        http_status: None,
        echoed_route: None,
        echoed_contract_version: None,
        response_status: None,
        response_code: None,
        response_detail: None,
        response_slippage_bps: None,
        response_tip_lamports: None,
        response_compute_unit_limit: None,
        response_compute_unit_price_micro_lamports: None,
        detail: detail.to_string(),
    }
}

fn classify_request_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_request() {
        "request"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_redirect() {
        "redirect"
    } else if error.is_status() {
        "status"
    } else {
        "other"
    }
}

fn redacted_endpoint_label(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return "unknown".to_string();
    }
    match reqwest::Url::parse(endpoint) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("unknown");
            match url.port() {
                Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
                None => format!("{}://{}", url.scheme(), host),
            }
        }
        Err(_) => "invalid_endpoint".to_string(),
    }
}

fn derive_verdict(
    mode: &str,
    assessment: &config_contract::ExecutionStaticContractAssessment,
    rpc_preflight: &RpcPreflightReport,
    adapter_rehearsal: &AdapterRehearsalReport,
    route: &str,
) -> ExecutionDryRunRehearsalVerdict {
    if route.trim().is_empty() {
        return ExecutionDryRunRehearsalVerdict::RehearsalInputInvalid;
    }
    if mode != "adapter_submit_confirm" {
        return ExecutionDryRunRehearsalVerdict::RehearsalBlockedByStaticContract;
    }
    if !assessment.mode_compatible
        || !assessment.signer_contract_valid
        || !assessment.policy_contract_valid
        || !assessment.route_contract_valid
    {
        return ExecutionDryRunRehearsalVerdict::RehearsalBlockedByStaticContract;
    }
    if !assessment.adapter_contract_valid {
        return ExecutionDryRunRehearsalVerdict::RehearsalBlockedByAdapterContract;
    }
    if rpc_preflight.required && !rpc_preflight.reachable {
        return ExecutionDryRunRehearsalVerdict::RehearsalBlockedByConnectivity;
    }
    match adapter_rehearsal.classification {
        AdapterRehearsalClassification::Accepted => ExecutionDryRunRehearsalVerdict::RehearsalGreen,
        AdapterRehearsalClassification::BusinessReject => {
            ExecutionDryRunRehearsalVerdict::RehearsalGreenWithBusinessReject
        }
        AdapterRehearsalClassification::PolicyEchoMissing => {
            ExecutionDryRunRehearsalVerdict::RehearsalBlockedByPolicyEcho
        }
        AdapterRehearsalClassification::ConnectivityBlocked => {
            ExecutionDryRunRehearsalVerdict::RehearsalBlockedByConnectivity
        }
        AdapterRehearsalClassification::ContractReject
        | AdapterRehearsalClassification::Skipped => {
            ExecutionDryRunRehearsalVerdict::RehearsalBlockedByAdapterContract
        }
    }
}

fn derive_reason(
    assessment: &config_contract::ExecutionStaticContractAssessment,
    rpc_preflight: &RpcPreflightReport,
    adapter_rehearsal: &AdapterRehearsalReport,
    verdict: ExecutionDryRunRehearsalVerdict,
) -> String {
    match verdict {
        ExecutionDryRunRehearsalVerdict::RehearsalGreen => {
            "execution dry-run rehearsal succeeded through safe simulate path; execution remains disabled".to_string()
        }
        ExecutionDryRunRehearsalVerdict::RehearsalGreenWithBusinessReject => format!(
            "execution wiring looks valid for dry-run, but the deterministic rehearsal intent was rejected business-wise: {}",
            adapter_rehearsal.detail
        ),
        ExecutionDryRunRehearsalVerdict::RehearsalBlockedByAdapterContract => assessment
            .adapter_error
            .clone()
            .unwrap_or_else(|| adapter_rehearsal.detail.clone()),
        ExecutionDryRunRehearsalVerdict::RehearsalBlockedByConnectivity => {
            if rpc_preflight.required && !rpc_preflight.reachable {
                rpc_preflight.detail.clone()
            } else {
                adapter_rehearsal.detail.clone()
            }
        }
        ExecutionDryRunRehearsalVerdict::RehearsalBlockedByPolicyEcho => {
            adapter_rehearsal.detail.clone()
        }
        ExecutionDryRunRehearsalVerdict::RehearsalBlockedByStaticContract => assessment
            .mode_error
            .clone()
            .or_else(|| assessment.signer_error.clone())
            .or_else(|| assessment.policy_error.clone())
            .or_else(|| assessment.route_error.clone())
            .unwrap_or_else(|| "execution dry-run rehearsal blocked by static contract".to_string()),
        ExecutionDryRunRehearsalVerdict::RehearsalInputInvalid => {
            "execution dry-run rehearsal input invalid".to_string()
        }
    }
}

fn collect_blockers(
    assessment: &config_contract::ExecutionStaticContractAssessment,
    rpc_preflight: &RpcPreflightReport,
    adapter_rehearsal: &AdapterRehearsalReport,
    verdict: ExecutionDryRunRehearsalVerdict,
    reason: &str,
) -> Vec<String> {
    let mut blockers = Vec::new();
    blockers.extend(
        [
            assessment.mode_error.clone(),
            assessment.signer_error.clone(),
            assessment.adapter_error.clone(),
            assessment.policy_error.clone(),
            assessment.route_error.clone(),
        ]
        .into_iter()
        .flatten(),
    );
    if matches!(
        verdict,
        ExecutionDryRunRehearsalVerdict::RehearsalBlockedByConnectivity
            | ExecutionDryRunRehearsalVerdict::RehearsalBlockedByAdapterContract
            | ExecutionDryRunRehearsalVerdict::RehearsalBlockedByPolicyEcho
            | ExecutionDryRunRehearsalVerdict::RehearsalBlockedByStaticContract
            | ExecutionDryRunRehearsalVerdict::RehearsalInputInvalid
    ) {
        blockers.push(reason.to_string());
    }
    if rpc_preflight.required && !rpc_preflight.reachable {
        blockers.push(rpc_preflight.detail.clone());
    }
    if adapter_rehearsal.required
        && matches!(
            adapter_rehearsal.classification,
            AdapterRehearsalClassification::ConnectivityBlocked
                | AdapterRehearsalClassification::ContractReject
                | AdapterRehearsalClassification::PolicyEchoMissing
        )
    {
        blockers.push(adapter_rehearsal.detail.clone());
    }
    blockers.sort();
    blockers.dedup();
    blockers
}

fn collect_warnings(
    execution_enabled: bool,
    ready_for_dry_run: bool,
    would_be_admissible_for_later_tiny_live: bool,
    adapter_rehearsal: &AdapterRehearsalReport,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if !execution_enabled {
        warnings.push(
            "execution.enabled=false; dry-run rehearsal used only safe simulate/read checks"
                .to_string(),
        );
    }
    warnings.push(
        "Stage 3 discovery gate is not evaluated by this rehearsal; activation remains blocked until Stage 3 turns green".to_string(),
    );
    if ready_for_dry_run && !would_be_admissible_for_later_tiny_live {
        warnings.push(
            "dry-run wiring is not yet admissible for later tiny-live because the adapter result is still a hard blocker".to_string(),
        );
    }
    if adapter_rehearsal.classification == AdapterRehearsalClassification::BusinessReject {
        warnings.push(format!(
            "adapter simulate endpoint is reachable and contract-valid, but the deterministic rehearsal intent was rejected business-wise: {}",
            adapter_rehearsal.detail
        ));
    }
    warnings
}

fn resolve_db_path(config_path: &Path, sqlite_path: &str) -> PathBuf {
    let configured = Path::new(sqlite_path.trim());
    if configured.is_absolute() {
        return configured.to_path_buf();
    }
    match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(configured),
        _ => configured.to_path_buf(),
    }
}

fn render_human(report: &ExecutionDryRunRehearsalReport) -> String {
    [
        "event=copybot_execution_dry_run_rehearsal".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!(
            "rehearsal_id={}",
            report
                .rehearsal_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
        format!("execution_enabled={}", report.execution_enabled),
        format!("execution_dry_run_only={}", report.execution_dry_run_only),
        format!(
            "stage3_gate_not_evaluated={}",
            report.stage3_gate_not_evaluated
        ),
        format!("mode={}", report.mode),
        format!(
            "signer_pubkey_configured={}",
            report.signer_pubkey_configured
        ),
        format!(
            "verdict={}",
            serde_json::to_string(&report.verdict)
                .unwrap_or_default()
                .trim_matches('"')
        ),
        format!("reason={}", report.reason),
        format!("config_valid={}", report.config_valid),
        format!("connectivity_valid={}", report.connectivity_valid),
        format!("adapter_contract_valid={}", report.adapter_contract_valid),
        format!("signer_contract_valid={}", report.signer_contract_valid),
        format!("policy_contract_valid={}", report.policy_contract_valid),
        format!("route_contract_valid={}", report.route_contract_valid),
        format!("ready_for_dry_run={}", report.ready_for_dry_run),
        format!(
            "would_be_admissible_for_later_tiny_live={}",
            report.would_be_admissible_for_later_tiny_live
        ),
        format!("route={}", report.intent.route),
        format!("token={}", report.intent.token),
        format!("notional_sol={}", report.intent.notional_sol),
        format!(
            "policy_echo_required={}",
            report.route_policy.policy_echo_required
        ),
        format!(
            "rpc_preflight_classification={:?}",
            report.rpc_preflight.classification
        )
        .to_ascii_lowercase(),
        format!("rpc_preflight_detail={}", report.rpc_preflight.detail),
        format!(
            "rpc_preflight_slot={}",
            report
                .rpc_preflight
                .slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "rpc_preflight_blockhash={}",
            report
                .rpc_preflight
                .blockhash
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "adapter_rehearsal_classification={:?}",
            report.adapter_rehearsal.classification
        )
        .to_ascii_lowercase(),
        format!(
            "adapter_rehearsal_detail={}",
            report.adapter_rehearsal.detail
        ),
        format!(
            "adapter_rehearsal_accepted={}",
            report
                .adapter_rehearsal
                .accepted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "policy_echo_present={}",
            report.adapter_rehearsal.policy_echo_present
        ),
        format!("blockers={}", report.blockers.join(" | ")),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

fn render_history_human(report: &ExecutionDryRunRehearsalHistoryReport) -> String {
    let latest = report
        .latest_rehearsed_at
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "null".to_string());
    let counts = report
        .verdict_counts
        .iter()
        .map(|(verdict, count)| format!("{verdict}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    let latest_entry = report.rehearsals.first().map(|entry| {
        format!(
            "latest_verdict={} latest_reason={} latest_route={}",
            entry.verdict, entry.reason, entry.route
        )
    });
    [
        "event=copybot_execution_dry_run_rehearsal_history".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
        format!("records_loaded={}", report.records_loaded),
        format!("latest_rehearsed_at={latest}"),
        format!("verdict_counts={counts}"),
        latest_entry.unwrap_or_else(|| "latest_verdict=null".to_string()),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn green_dry_run_with_mock_rpc_and_adapter_persists_history() {
        let temp = temp_dir("execution_rehearsal_green");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":123456u64},"value":{"blockhash":"abc123","lastValidBlockHeight":999u64}}
                })),
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":123456u64},"value":42u64}
                })),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![MockResponse::json(json!({
                "status":"ok",
                "ok":true,
                "accepted":true,
                "route":"jito",
                "contract_version":"v1",
                "detail":"simulated"
            }))],
            Some("authorization"),
        );

        let mut config = base_adapter_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config);
        assert_eq!(
            report.verdict,
            ExecutionDryRunRehearsalVerdict::RehearsalGreen
        );
        assert!(report.ready_for_dry_run);
        assert!(!report.execution_enabled);

        let store = SqliteStore::open(&temp.join("runtime.db")).expect("open store");
        let row = store
            .append_execution_dry_run_rehearsal(
                &persisted_row_for_report(&report).expect("persisted row"),
            )
            .expect("append rehearsal");
        assert_eq!(row.verdict, "rehearsal_green");

        let history = build_history_report(&store, &config_path, &temp.join("runtime.db"), 5)
            .expect("history report");
        assert_eq!(history.records_loaded, 1);
        assert_eq!(history.rehearsals[0].verdict, "rehearsal_green");

        let captured = adapter_server.take_requests();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].body.contains("\"action\":\"simulate\""));
        assert!(captured[0].body.contains("\"dry_run\":true"));
    }

    #[test]
    fn business_reject_is_distinct_from_hard_contract_failure() {
        let temp = temp_dir("execution_rehearsal_business_reject");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);
        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":123u64},"value":{"blockhash":"abc","lastValidBlockHeight":99u64}}
                })),
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":123u64},"value":0u64}
                })),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![MockResponse::json(json!({
                "status":"reject",
                "route":"jito",
                "contract_version":"v1",
                "code":"simulation_rejected",
                "detail":"probe intent rejected"
            }))],
            Some("authorization"),
        );
        let mut config = base_adapter_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config);
        assert_eq!(
            report.verdict,
            ExecutionDryRunRehearsalVerdict::RehearsalGreenWithBusinessReject
        );
        assert!(report.ready_for_dry_run);
        assert!(report.would_be_admissible_for_later_tiny_live);
        assert_eq!(
            report.adapter_rehearsal.classification,
            AdapterRehearsalClassification::BusinessReject
        );
    }

    #[test]
    fn missing_policy_echo_is_classified_correctly() {
        let temp = temp_dir("execution_rehearsal_policy_echo_missing");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);
        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":123u64},"value":{"blockhash":"abc","lastValidBlockHeight":99u64}}
                })),
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":123u64},"value":10u64}
                })),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![MockResponse::json(json!({
                "status":"ok",
                "ok":true,
                "accepted":true,
                "detail":"simulated"
            }))],
            Some("authorization"),
        );
        let mut config = base_adapter_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config);
        assert_eq!(
            report.verdict,
            ExecutionDryRunRehearsalVerdict::RehearsalBlockedByPolicyEcho
        );
        assert!(!report.adapter_contract_valid);
        assert!(
            report.reason.contains("simulation_policy_echo_missing"),
            "unexpected reason: {}",
            report.reason
        );
    }

    #[test]
    fn connectivity_failure_is_classified_correctly() {
        let temp = temp_dir("execution_rehearsal_connectivity");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_adapter_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_http_url = "http://127.0.0.1:1".to_string();
        config.execution.submit_adapter_http_url = "http://127.0.0.1:1/submit".to_string();

        let report = run_report(&config_path, &config);
        assert_eq!(
            report.verdict,
            ExecutionDryRunRehearsalVerdict::RehearsalBlockedByConnectivity
        );
        assert!(!report.connectivity_valid);
    }

    #[test]
    fn history_render_and_json_work_for_persisted_rows() {
        let temp = temp_dir("execution_rehearsal_history_render");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);
        let store = SqliteStore::open(&temp.join("runtime.db")).expect("open store");
        let report = ExecutionDryRunRehearsalReport {
            generated_at: Utc::now(),
            rehearsal_id: None,
            config_path: config_path.display().to_string(),
            db_path: temp.join("runtime.db").display().to_string(),
            execution_enabled: false,
            execution_dry_run_only: true,
            stage3_gate_not_evaluated: true,
            mode: "adapter_submit_confirm".to_string(),
            signer_pubkey_configured: true,
            config_valid: true,
            connectivity_valid: true,
            adapter_contract_valid: true,
            signer_contract_valid: true,
            policy_contract_valid: true,
            route_contract_valid: true,
            ready_for_dry_run: true,
            would_be_admissible_for_later_tiny_live: true,
            verdict: ExecutionDryRunRehearsalVerdict::RehearsalGreen,
            reason: "ready".to_string(),
            blockers: Vec::new(),
            warnings: vec!["execution.enabled=false".to_string()],
            intent: RehearsalIntentSummary {
                route: "jito".to_string(),
                token: DEFAULT_REHEARSAL_TOKEN.to_string(),
                notional_sol: 0.01,
                side: "buy".to_string(),
            },
            route_policy: RoutePolicyEnvelope {
                route: "jito".to_string(),
                slippage_bps_cap: Some(50.0),
                tip_lamports: Some(10_000),
                compute_unit_limit: Some(300_000),
                compute_unit_price_micro_lamports: Some(1_500),
                policy_echo_required: true,
                simulate_before_submit: true,
                dynamic_cu_price_enabled: false,
                dynamic_tip_lamports_enabled: false,
            },
            rpc_preflight: RpcPreflightReport {
                required: true,
                attempted: true,
                reachable: true,
                classification: RpcPreflightClassification::Reachable,
                timeout_ms: 3_000,
                successful_endpoint_role: Some("primary".to_string()),
                detail: "ok".to_string(),
                slot: Some(123),
                blockhash: Some("abc".to_string()),
                signer_balance_lamports: Some(1),
            },
            adapter_rehearsal: AdapterRehearsalReport {
                required: true,
                attempted: true,
                reachable: true,
                contract_valid: true,
                policy_echo_present: true,
                route_echo_present: true,
                contract_version_echo_present: true,
                accepted: Some(true),
                classification: AdapterRehearsalClassification::Accepted,
                timeout_ms: 3_000,
                endpoint_role: Some("primary".to_string()),
                endpoint_label: Some("http://127.0.0.1:8080".to_string()),
                http_status: Some(200),
                echoed_route: Some("jito".to_string()),
                echoed_contract_version: Some("v1".to_string()),
                response_status: Some("ok".to_string()),
                response_code: None,
                response_detail: Some("simulated".to_string()),
                response_slippage_bps: None,
                response_tip_lamports: None,
                response_compute_unit_limit: None,
                response_compute_unit_price_micro_lamports: None,
                detail: "simulated".to_string(),
            },
        };
        store
            .append_execution_dry_run_rehearsal(&persisted_row_for_report(&report).expect("row"))
            .expect("append row");
        let history = build_history_report(&store, &config_path, &temp.join("runtime.db"), 10)
            .expect("build history");
        let human = render_history_human(&history);
        assert!(human.contains("records_loaded=1"));
        let json = serde_json::to_string_pretty(&history).expect("history json");
        assert!(json.contains("\"rehearsal_green\""));
    }

    #[test]
    fn command_does_not_depend_on_discovery_truth() {
        let temp = temp_dir("execution_rehearsal_no_discovery_truth");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":999u64},"value":{"blockhash":"ready","lastValidBlockHeight":77u64}}
                })),
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":999u64},"value":123u64}
                })),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![MockResponse::json(json!({
                "status":"ok",
                "ok":true,
                "accepted":true,
                "route":"jito",
                "contract_version":"v1",
                "detail":"simulated"
            }))],
            Some("authorization"),
        );

        let mut config = base_adapter_config();
        config.sqlite.path = temp.join("brand-new.db").display().to_string();
        config.execution.rpc_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config);
        assert_eq!(
            report.verdict,
            ExecutionDryRunRehearsalVerdict::RehearsalGreen
        );
    }

    fn base_adapter_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "paper".to_string();
        config.execution.enabled = false;
        config.execution.mode = "adapter_submit_confirm".to_string();
        config.execution.default_route = "jito".to_string();
        config.execution.rpc_http_url = "http://127.0.0.1:8899".to_string();
        config.execution.submit_adapter_http_url = "http://127.0.0.1:8080/submit".to_string();
        config.execution.submit_adapter_contract_version = "v1".to_string();
        config.execution.submit_adapter_require_policy_echo = true;
        config.execution.submit_allowed_routes = vec!["jito".to_string(), "rpc".to_string()];
        config.execution.submit_route_order = vec!["jito".to_string(), "rpc".to_string()];
        config.execution.submit_route_max_slippage_bps =
            [("jito".to_string(), 50.0), ("rpc".to_string(), 50.0)]
                .into_iter()
                .collect();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 10_000), ("rpc".to_string(), 0)]
                .into_iter()
                .collect();
        config.execution.submit_route_compute_unit_limit =
            [("jito".to_string(), 300_000), ("rpc".to_string(), 300_000)]
                .into_iter()
                .collect();
        config
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500), ("rpc".to_string(), 1_000)]
                .into_iter()
                .collect();
        config.execution.execution_signer_pubkey = "11111111111111111111111111111111".to_string();
        config.execution.submit_adapter_auth_token = "adapter-token".to_string();
        config
    }

    fn run_report(config_path: &Path, config: &AppConfig) -> ExecutionDryRunRehearsalReport {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");
        runtime
            .block_on(evaluate_execution_dry_run_rehearsal(
                config_path,
                &resolve_db_path(config_path, &config.sqlite.path),
                config,
                None,
                DEFAULT_REHEARSAL_TOKEN,
                DEFAULT_REHEARSAL_NOTIONAL_SOL,
            ))
            .expect("execution rehearsal report")
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_execution_dry_run_rehearsal_{}_{}_{}",
            label,
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_placeholder_config(path: &Path) {
        fs::write(path, "[system]\nenv = \"paper\"\n").expect("write placeholder config file");
    }

    #[derive(Debug, Clone)]
    struct CapturedRequest {
        body: String,
    }

    #[derive(Debug, Clone)]
    struct MockResponse {
        status: u16,
        body: String,
        content_type: String,
    }

    impl MockResponse {
        fn json(body: Value) -> Self {
            Self {
                status: 200,
                body: body.to_string(),
                content_type: "application/json".to_string(),
            }
        }
    }

    struct MockHttpServer {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl MockHttpServer {
        fn spawn(responses: Vec<MockResponse>, required_header: Option<&str>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            listener.set_nonblocking(false).expect("blocking listener");
            let addr = listener.local_addr().expect("mock addr");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_clone = Arc::clone(&requests);
            let required_header = required_header.map(str::to_string);
            let handle = thread::spawn(move || {
                for response in responses {
                    let (mut stream, _) = listener.accept().expect("accept request");
                    let Some(request) = read_http_request(&mut stream) else {
                        break;
                    };
                    if let Some(required_header) = required_header.as_deref() {
                        assert!(
                            request.headers.iter().any(|header| header
                                .to_ascii_lowercase()
                                .starts_with(required_header)),
                            "missing required header {required_header} in {:?}",
                            request.headers
                        );
                    }
                    requests_clone
                        .lock()
                        .expect("lock requests")
                        .push(CapturedRequest {
                            body: request.body.clone(),
                        });
                    write_http_response(
                        &mut stream,
                        response.status,
                        response.content_type.as_str(),
                        response.body.as_str(),
                    );
                }
            });
            Self {
                addr,
                requests,
                handle: Some(handle),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.addr, path)
        }

        fn take_requests(mut self) -> Vec<CapturedRequest> {
            if let Some(handle) = self.handle.take() {
                handle.join().expect("join mock server");
            }
            self.requests.lock().expect("lock requests").clone()
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            let _ = TcpStream::connect(self.addr);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    struct ParsedHttpRequest {
        headers: Vec<String>,
        body: String,
    }

    fn read_http_request(stream: &mut TcpStream) -> Option<ParsedHttpRequest> {
        let mut header_bytes = Vec::new();
        let mut buf = [0_u8; 1024];
        let header_end;
        loop {
            let bytes = stream.read(&mut buf).expect("read request");
            if bytes == 0 {
                return None;
            }
            header_bytes.extend_from_slice(&buf[..bytes]);
            if let Some(pos) = header_bytes
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
            {
                header_end = pos + 4;
                break;
            }
        }

        let header_text = String::from_utf8_lossy(&header_bytes[..header_end]).to_string();
        let headers: Vec<String> = header_text.lines().map(|line| line.to_string()).collect();
        let content_length = headers
            .iter()
            .find_map(|line| {
                let lower = line.to_ascii_lowercase();
                lower
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut body_bytes = header_bytes[header_end..].to_vec();
        while body_bytes.len() < content_length {
            let bytes = stream.read(&mut buf).expect("read request body");
            assert!(bytes > 0, "unexpected eof reading body");
            body_bytes.extend_from_slice(&buf[..bytes]);
        }
        Some(ParsedHttpRequest {
            headers,
            body: String::from_utf8_lossy(&body_bytes[..content_length]).to_string(),
        })
    }

    fn write_http_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
        let reason = if status == 200 { "OK" } else { "ERR" };
        let response = format!(
            "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            status,
            reason,
            content_type,
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write mock response");
        stream.flush().expect("flush mock response");
    }
}
