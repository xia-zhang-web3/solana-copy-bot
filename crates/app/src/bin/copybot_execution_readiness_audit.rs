use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig, ExecutionConfig};
use copybot_core_types::Lamports;
use copybot_execution::intent::{ExecutionIntent, ExecutionSide};
use copybot_execution::simulator::{AdapterIntentSimulator, IntentSimulator};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

#[path = "../config_contract.rs"]
mod config_contract;
#[path = "../secrets.rs"]
mod secrets;

const USAGE: &str = "usage: copybot_execution_readiness_audit --config <path> [--json]";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const READINESS_PROBE_NOTIONAL_SOL: f64 = 0.01;
const READINESS_PROBE_NOTIONAL_LAMPORTS: u64 = 10_000_000;
const MIN_PROBE_TIMEOUT_MS: u64 = 1_000;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for execution readiness audit")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionReadinessVerdict {
    ReadyForExecutionDryRun,
    ConfigValidButConnectivityBlocked,
    AdapterContractIncomplete,
    SignerContractIncomplete,
    PolicyContractIncomplete,
    ModeContractIncompatible,
    NotApplicableExecutionDisabledContractOnly,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RpcProbeClassification {
    Reachable,
    ConnectivityBlocked,
    Skipped,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdapterProbeClassification {
    Accepted,
    BusinessReject,
    ContractReject,
    ConnectivityBlocked,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RouteSummary {
    pub(crate) default_route: String,
    pub(crate) allowed_routes: Vec<String>,
    pub(crate) submit_route_order: Vec<String>,
    pub(crate) policy_echo_required: bool,
    pub(crate) simulate_before_submit: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RpcProbeReport {
    pub(crate) required: bool,
    pub(crate) attempted: bool,
    pub(crate) reachable: bool,
    pub(crate) classification: RpcProbeClassification,
    pub(crate) timeout_ms: u64,
    pub(crate) successful_endpoint_role: Option<String>,
    pub(crate) detail: String,
    pub(crate) slot: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AdapterProbeReport {
    pub(crate) required: bool,
    pub(crate) attempted: bool,
    pub(crate) reachable: bool,
    pub(crate) contract_valid: bool,
    pub(crate) accepted: Option<bool>,
    pub(crate) classification: AdapterProbeClassification,
    pub(crate) timeout_ms: u64,
    pub(crate) route: Option<String>,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExecutionReadinessAuditReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) config_path: String,
    pub(crate) execution_enabled: bool,
    pub(crate) mode: String,
    pub(crate) mode_compatible: bool,
    pub(crate) config_valid: bool,
    pub(crate) connectivity_valid: bool,
    pub(crate) adapter_contract_valid: bool,
    pub(crate) signer_contract_valid: bool,
    pub(crate) policy_contract_valid: bool,
    pub(crate) route_contract_valid: bool,
    pub(crate) ready_for_dry_run: bool,
    pub(crate) blocked_for_activation: bool,
    pub(crate) verdict: ExecutionReadinessVerdict,
    pub(crate) reason: String,
    pub(crate) activation_blockers: Vec<String>,
    pub(crate) static_blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) signer_pubkey_configured: bool,
    pub(crate) adapter_auth_token_configured: bool,
    pub(crate) adapter_auth_token_source: String,
    pub(crate) adapter_hmac_configured: bool,
    pub(crate) adapter_hmac_source: String,
    pub(crate) route_summary: RouteSummary,
    pub(crate) rpc_probe: RpcProbeReport,
    pub(crate) adapter_probe: AdapterProbeReport,
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
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
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

async fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let report = evaluate_execution_readiness(&config.config_path, &loaded_config).await?;
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing execution readiness audit json")
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) async fn evaluate_execution_readiness(
    config_path: &Path,
    loaded_config: &AppConfig,
) -> Result<ExecutionReadinessAuditReport> {
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

    let mode = assessment
        .mode
        .clone()
        .unwrap_or_else(|| normalize_mode(execution.mode.as_str()));
    let probe_timeout_ms = execution_probe_timeout_ms(&execution);
    let route_summary = build_route_summary(&execution);
    let rpc_probe = probe_rpc_readiness(&execution, mode.as_str(), probe_timeout_ms).await;
    let adapter_probe =
        probe_adapter_readiness(&execution, mode.as_str(), &assessment, probe_timeout_ms).await;

    let rpc_connectivity_valid = !rpc_probe.required || rpc_probe.reachable;
    let adapter_connectivity_valid = !adapter_probe.required || adapter_probe.reachable;
    let connectivity_valid = rpc_connectivity_valid && adapter_connectivity_valid;
    let adapter_endpoint_contract_valid = !adapter_probe.required
        || adapter_probe.classification != AdapterProbeClassification::ContractReject;
    let final_adapter_contract_valid =
        assessment.adapter_contract_valid && adapter_endpoint_contract_valid;
    let ready_for_dry_run = is_dry_run_applicable(mode.as_str())
        && assessment.config_valid
        && final_adapter_contract_valid
        && connectivity_valid;

    let verdict = derive_verdict(
        mode.as_str(),
        loaded_config.execution.enabled,
        &assessment,
        final_adapter_contract_valid,
        connectivity_valid,
    );
    let static_blockers = collect_static_blockers(&assessment);
    let reason = derive_reason(
        verdict,
        &assessment,
        &rpc_probe,
        &adapter_probe,
        loaded_config.execution.enabled,
    );
    let warnings = collect_warnings(
        loaded_config.execution.enabled,
        mode.as_str(),
        ready_for_dry_run,
        &adapter_probe,
    );
    let activation_blockers =
        collect_activation_blockers(loaded_config.execution.enabled, ready_for_dry_run, &reason);

    Ok(ExecutionReadinessAuditReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        execution_enabled: loaded_config.execution.enabled,
        mode,
        mode_compatible: assessment.mode_compatible,
        config_valid: assessment.config_valid,
        connectivity_valid,
        adapter_contract_valid: final_adapter_contract_valid,
        signer_contract_valid: assessment.signer_contract_valid,
        policy_contract_valid: assessment.policy_contract_valid,
        route_contract_valid: assessment.route_contract_valid,
        ready_for_dry_run,
        blocked_for_activation: !activation_blockers.is_empty(),
        verdict,
        reason,
        activation_blockers,
        static_blockers,
        warnings,
        signer_pubkey_configured: !execution.execution_signer_pubkey.trim().is_empty(),
        adapter_auth_token_configured: !execution.submit_adapter_auth_token.trim().is_empty(),
        adapter_auth_token_source: secret_source(
            execution.submit_adapter_auth_token.as_str(),
            execution.submit_adapter_auth_token_file.as_str(),
        ),
        adapter_hmac_configured: !execution.submit_adapter_hmac_key_id.trim().is_empty()
            && !execution.submit_adapter_hmac_secret.trim().is_empty(),
        adapter_hmac_source: secret_source(
            execution.submit_adapter_hmac_secret.as_str(),
            execution.submit_adapter_hmac_secret_file.as_str(),
        ),
        route_summary,
        rpc_probe,
        adapter_probe,
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

fn normalize_routes(values: &[String]) -> Vec<String> {
    values.iter().map(|value| normalize_route(value)).collect()
}

fn execution_probe_timeout_ms(config: &ExecutionConfig) -> u64 {
    config
        .submit_timeout_ms
        .max(config.poll_interval_ms)
        .max(MIN_PROBE_TIMEOUT_MS)
}

fn build_route_summary(config: &ExecutionConfig) -> RouteSummary {
    RouteSummary {
        default_route: normalize_route(config.default_route.as_str()),
        allowed_routes: normalize_routes(&config.submit_allowed_routes),
        submit_route_order: normalize_routes(&config.submit_route_order),
        policy_echo_required: config.submit_adapter_require_policy_echo,
        simulate_before_submit: config.simulate_before_submit,
    }
}

fn rpc_probe_required(mode: &str) -> bool {
    matches!(
        mode,
        "paper_rpc_confirm" | "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    )
}

async fn probe_rpc_readiness(
    execution: &ExecutionConfig,
    mode: &str,
    timeout_ms: u64,
) -> RpcProbeReport {
    if !rpc_probe_required(mode) {
        return RpcProbeReport {
            required: false,
            attempted: false,
            reachable: false,
            classification: RpcProbeClassification::Skipped,
            timeout_ms,
            successful_endpoint_role: None,
            detail: "rpc_probe_not_required_for_mode".to_string(),
            slot: None,
        };
    }

    let client = match Client::builder()
        .timeout(StdDuration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return RpcProbeReport {
                required: true,
                attempted: false,
                reachable: false,
                classification: RpcProbeClassification::ConnectivityBlocked,
                timeout_ms,
                successful_endpoint_role: None,
                detail: format!("failed_building_rpc_probe_client: {error}"),
                slot: None,
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
        match probe_single_rpc_endpoint(&client, endpoint).await {
            Ok(slot) => {
                return RpcProbeReport {
                    required: true,
                    attempted: true,
                    reachable: true,
                    classification: RpcProbeClassification::Reachable,
                    timeout_ms,
                    successful_endpoint_role: Some(role.to_string()),
                    detail: format!("rpc_get_slot_ok via {role}"),
                    slot: Some(slot),
                };
            }
            Err(error) => failures.push(format!("{role}: {error}")),
        }
    }

    RpcProbeReport {
        required: true,
        attempted: true,
        reachable: false,
        classification: RpcProbeClassification::ConnectivityBlocked,
        timeout_ms,
        successful_endpoint_role: None,
        detail: if failures.is_empty() {
            "no_rpc_endpoints_configured".to_string()
        } else {
            failures.join(" | ")
        },
        slot: None,
    }
}

async fn probe_single_rpc_endpoint(client: &Client, endpoint: &str) -> Result<u64> {
    let response = client
        .post(endpoint)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{"commitment": "processed"}]
        }))
        .send()
        .await
        .with_context(|| format!("request_failed endpoint={endpoint}"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("invalid_json endpoint={endpoint}"))?;
    if !status.is_success() {
        bail!("http_status={} endpoint={endpoint}", status.as_u16());
    }
    if let Some(error) = body.get("error") {
        bail!("rpc_error endpoint={endpoint} error={error}");
    }
    let slot = body
        .get("result")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing_u64_result endpoint={endpoint}"))?;
    Ok(slot)
}

fn adapter_probe_required(mode: &str) -> bool {
    mode == "adapter_submit_confirm"
}

async fn probe_adapter_readiness(
    execution: &ExecutionConfig,
    mode: &str,
    assessment: &config_contract::ExecutionStaticContractAssessment,
    timeout_ms: u64,
) -> AdapterProbeReport {
    if !adapter_probe_required(mode) {
        return AdapterProbeReport {
            required: false,
            attempted: false,
            reachable: false,
            contract_valid: false,
            accepted: None,
            classification: AdapterProbeClassification::Skipped,
            timeout_ms,
            route: None,
            detail: "adapter_probe_not_required_for_mode".to_string(),
        };
    }
    if !assessment.mode_compatible {
        return skipped_adapter_probe(timeout_ms, "adapter_probe_skipped_due_to_mode_contract");
    }
    if !assessment.adapter_contract_valid {
        return skipped_adapter_probe(
            timeout_ms,
            "adapter_probe_skipped_due_to_static_adapter_contract",
        );
    }
    if !assessment.policy_contract_valid || !assessment.route_contract_valid {
        return skipped_adapter_probe(
            timeout_ms,
            "adapter_probe_skipped_due_to_policy_or_route_contract",
        );
    }

    let execution = execution.clone();
    let probe_route = normalize_route(execution.default_route.as_str());
    let join_probe_route = probe_route.clone();
    match tokio::task::spawn_blocking(move || {
        let Some(simulator) = AdapterIntentSimulator::new(
            &execution.submit_adapter_http_url,
            &execution.submit_adapter_fallback_http_url,
            &execution.submit_adapter_auth_token,
            &execution.submit_adapter_hmac_key_id,
            &execution.submit_adapter_hmac_secret,
            execution.submit_adapter_hmac_ttl_sec,
            &execution.submit_adapter_contract_version,
            execution.submit_adapter_require_policy_echo,
            timeout_ms,
        ) else {
            return AdapterProbeReport {
                required: true,
                attempted: false,
                reachable: false,
                contract_valid: false,
                accepted: None,
                classification: AdapterProbeClassification::ContractReject,
                timeout_ms,
                route: Some(probe_route.clone()),
                detail: "adapter_probe_constructor_rejected_current_config".to_string(),
            };
        };

        let route = probe_route.clone();
        let intent = execution_readiness_probe_intent();
        match simulator.simulate(&intent, route.as_str()) {
            Ok(result) if result.accepted => AdapterProbeReport {
                required: true,
                attempted: true,
                reachable: true,
                contract_valid: true,
                accepted: Some(true),
                classification: AdapterProbeClassification::Accepted,
                timeout_ms,
                route: Some(route),
                detail: result.detail,
            },
            Ok(result) => {
                let classification = classify_adapter_probe_detail(result.detail.as_str());
                AdapterProbeReport {
                    required: true,
                    attempted: true,
                    reachable: true,
                    contract_valid: classification == AdapterProbeClassification::BusinessReject,
                    accepted: Some(false),
                    classification,
                    timeout_ms,
                    route: Some(route),
                    detail: result.detail,
                }
            }
            Err(error) => AdapterProbeReport {
                required: true,
                attempted: true,
                reachable: false,
                contract_valid: false,
                accepted: None,
                classification: AdapterProbeClassification::ConnectivityBlocked,
                timeout_ms,
                route: Some(route),
                detail: error.to_string(),
            },
        }
    })
    .await
    {
        Ok(report) => report,
        Err(error) => AdapterProbeReport {
            required: true,
            attempted: true,
            reachable: false,
            contract_valid: false,
            accepted: None,
            classification: AdapterProbeClassification::ConnectivityBlocked,
            timeout_ms,
            route: Some(join_probe_route),
            detail: format!("adapter_probe_join_failed: {error}"),
        },
    }
}

fn skipped_adapter_probe(timeout_ms: u64, detail: &str) -> AdapterProbeReport {
    AdapterProbeReport {
        required: true,
        attempted: false,
        reachable: false,
        contract_valid: false,
        accepted: None,
        classification: AdapterProbeClassification::Skipped,
        timeout_ms,
        route: None,
        detail: detail.to_string(),
    }
}

fn execution_readiness_probe_intent() -> ExecutionIntent {
    ExecutionIntent {
        signal_id: "execution_readiness_probe".to_string(),
        leader_wallet: "execution_readiness_probe".to_string(),
        side: ExecutionSide::Buy,
        token: WSOL_MINT.to_string(),
        notional_sol: READINESS_PROBE_NOTIONAL_SOL,
        notional_lamports: Lamports::new(READINESS_PROBE_NOTIONAL_LAMPORTS),
        signal_ts: Utc::now(),
    }
}

fn classify_adapter_probe_detail(detail: &str) -> AdapterProbeClassification {
    if detail.starts_with("simulation_policy_echo_missing")
        || detail.starts_with("simulation_contract_version_mismatch")
        || detail.starts_with("simulation_route_mismatch")
        || detail.starts_with("simulation_invalid_status")
        || detail.starts_with("simulation_invalid_json")
        || detail.starts_with("simulation_http_rejected")
        || detail.starts_with("simulation_route_missing")
    {
        AdapterProbeClassification::ContractReject
    } else {
        AdapterProbeClassification::BusinessReject
    }
}

fn derive_verdict(
    mode: &str,
    execution_enabled: bool,
    assessment: &config_contract::ExecutionStaticContractAssessment,
    adapter_contract_valid: bool,
    connectivity_valid: bool,
) -> ExecutionReadinessVerdict {
    if !assessment.mode_compatible {
        return ExecutionReadinessVerdict::ModeContractIncompatible;
    }
    if !assessment.signer_contract_valid {
        return ExecutionReadinessVerdict::SignerContractIncomplete;
    }
    if !assessment.policy_contract_valid || !assessment.route_contract_valid {
        return ExecutionReadinessVerdict::PolicyContractIncomplete;
    }
    if !adapter_contract_valid {
        return ExecutionReadinessVerdict::AdapterContractIncomplete;
    }
    if !connectivity_valid {
        return ExecutionReadinessVerdict::ConfigValidButConnectivityBlocked;
    }
    if !is_dry_run_applicable(mode) && !execution_enabled {
        return ExecutionReadinessVerdict::NotApplicableExecutionDisabledContractOnly;
    }
    ExecutionReadinessVerdict::ReadyForExecutionDryRun
}

fn derive_reason(
    verdict: ExecutionReadinessVerdict,
    assessment: &config_contract::ExecutionStaticContractAssessment,
    rpc_probe: &RpcProbeReport,
    adapter_probe: &AdapterProbeReport,
    execution_enabled: bool,
) -> String {
    match verdict {
        ExecutionReadinessVerdict::ModeContractIncompatible => assessment
            .mode_error
            .clone()
            .unwrap_or_else(|| "execution_mode_contract_incompatible".to_string()),
        ExecutionReadinessVerdict::SignerContractIncomplete => assessment
            .signer_error
            .clone()
            .unwrap_or_else(|| "execution_signer_pubkey contract incomplete".to_string()),
        ExecutionReadinessVerdict::PolicyContractIncomplete => assessment
            .policy_error
            .clone()
            .or_else(|| assessment.route_error.clone())
            .unwrap_or_else(|| "execution policy contract incomplete".to_string()),
        ExecutionReadinessVerdict::AdapterContractIncomplete => assessment
            .adapter_error
            .clone()
            .unwrap_or_else(|| adapter_probe.detail.clone()),
        ExecutionReadinessVerdict::ConfigValidButConnectivityBlocked => {
            if rpc_probe.required && !rpc_probe.reachable {
                rpc_probe.detail.clone()
            } else {
                adapter_probe.detail.clone()
            }
        }
        ExecutionReadinessVerdict::NotApplicableExecutionDisabledContractOnly => {
            "execution mode is paper-only and execution.enabled=false; contract check is informational only".to_string()
        }
        ExecutionReadinessVerdict::ReadyForExecutionDryRun => {
            if execution_enabled {
                "execution-side wiring is ready for a later dry-run rehearsal".to_string()
            } else {
                "execution-side wiring is ready for a later dry-run rehearsal; execution.enabled remains false".to_string()
            }
        }
    }
}

fn collect_static_blockers(
    assessment: &config_contract::ExecutionStaticContractAssessment,
) -> Vec<String> {
    [
        assessment.mode_error.clone(),
        assessment.signer_error.clone(),
        assessment.adapter_error.clone(),
        assessment.policy_error.clone(),
        assessment.route_error.clone(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn collect_warnings(
    execution_enabled: bool,
    mode: &str,
    ready_for_dry_run: bool,
    adapter_probe: &AdapterProbeReport,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if !execution_enabled {
        warnings
            .push("execution.enabled=false; this audit does not activate live trading".to_string());
    }
    if mode == "adapter_submit_confirm"
        && ready_for_dry_run
        && adapter_probe.classification == AdapterProbeClassification::BusinessReject
    {
        warnings.push(format!(
            "adapter simulate endpoint is reachable and contract-valid, but the probe payload was rejected business-wise: {}",
            adapter_probe.detail
        ));
    }
    warnings
}

fn collect_activation_blockers(
    execution_enabled: bool,
    ready_for_dry_run: bool,
    reason: &str,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if !ready_for_dry_run {
        blockers.push(reason.to_string());
    }
    if !execution_enabled {
        blockers.push("execution.enabled=false (Stage 4 preparation only)".to_string());
    }
    blockers
}

fn is_dry_run_applicable(mode: &str) -> bool {
    mode != "paper"
}

fn secret_source(value: &str, file: &str) -> String {
    if !file.trim().is_empty() {
        "file".to_string()
    } else if !value.trim().is_empty() {
        "inline".to_string()
    } else {
        "none".to_string()
    }
}

pub(crate) fn render_human(report: &ExecutionReadinessAuditReport) -> String {
    [
        "event=copybot_execution_readiness_audit".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("execution_enabled={}", report.execution_enabled),
        format!("mode={}", report.mode),
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
        format!("blocked_for_activation={}", report.blocked_for_activation),
        format!("default_route={}", report.route_summary.default_route),
        format!(
            "allowed_routes={}",
            report.route_summary.allowed_routes.join(",")
        ),
        format!(
            "policy_echo_required={}",
            report.route_summary.policy_echo_required
        ),
        format!(
            "signer_pubkey_configured={}",
            report.signer_pubkey_configured
        ),
        format!(
            "adapter_auth_token_configured={}",
            report.adapter_auth_token_configured
        ),
        format!(
            "adapter_auth_token_source={}",
            report.adapter_auth_token_source
        ),
        format!("adapter_hmac_configured={}", report.adapter_hmac_configured),
        format!("adapter_hmac_source={}", report.adapter_hmac_source),
        format!(
            "rpc_probe_classification={}",
            serde_json::to_string(&report.rpc_probe.classification)
                .unwrap_or_default()
                .trim_matches('"')
        ),
        format!("rpc_probe_detail={}", report.rpc_probe.detail),
        format!(
            "rpc_probe_slot={}",
            report
                .rpc_probe
                .slot
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "adapter_probe_classification={}",
            serde_json::to_string(&report.adapter_probe.classification)
                .unwrap_or_default()
                .trim_matches('"')
        ),
        format!("adapter_probe_detail={}", report.adapter_probe.detail),
        format!(
            "adapter_probe_route={}",
            report
                .adapter_probe
                .route
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "adapter_probe_accepted={}",
            report
                .adapter_probe
                .accepted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("static_blockers={}", report.static_blockers.join(" | ")),
        format!(
            "activation_blockers={}",
            report.activation_blockers.join(" | ")
        ),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::AppConfig;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn valid_adapter_submit_confirm_config_yields_green_static_contract() {
        let temp = temp_dir("execution_readiness_static_green");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_adapter_config("paper");
        config.execution.submit_adapter_http_url = "http://127.0.0.1:1/submit".to_string();
        config.execution.rpc_http_url = "http://127.0.0.1:1".to_string();

        let report = run_report(&config_path, &config);

        assert!(report.config_valid, "static contract should be green");
        assert_eq!(
            report.verdict,
            ExecutionReadinessVerdict::ConfigValidButConnectivityBlocked
        );
        assert!(!report.execution_enabled);
    }

    #[test]
    fn missing_adapter_auth_token_file_is_reported_as_adapter_contract_incomplete() {
        let temp = temp_dir("execution_readiness_missing_auth_file");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_adapter_config("paper");
        config.execution.submit_adapter_http_url = "http://127.0.0.1:1/submit".to_string();
        config.execution.rpc_http_url = "http://127.0.0.1:1".to_string();
        config.execution.submit_adapter_auth_token_file = "missing.token".to_string();

        let report = run_report(&config_path, &config);

        assert_eq!(
            report.verdict,
            ExecutionReadinessVerdict::AdapterContractIncomplete
        );
        assert!(
            report.reason.contains("submit_adapter_auth_token_file"),
            "unexpected reason: {}",
            report.reason
        );
    }

    #[test]
    fn route_policy_incompleteness_is_reported_correctly() {
        let temp = temp_dir("execution_readiness_route_policy");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_adapter_config("paper");
        config.execution.submit_allowed_routes = vec!["jito".to_string(), "rpc".to_string()];
        config.execution.submit_route_max_slippage_bps.remove("rpc");

        let report = run_report(&config_path, &config);

        assert_eq!(
            report.verdict,
            ExecutionReadinessVerdict::PolicyContractIncomplete
        );
        assert!(
            report.reason.contains("missing cap for allowed route=rpc"),
            "unexpected reason: {}",
            report.reason
        );
    }

    #[test]
    fn prod_policy_echo_requirement_is_reflected_in_verdict() {
        let temp = temp_dir("execution_readiness_policy_echo");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_adapter_config("prod");
        config.execution.submit_adapter_require_policy_echo = false;

        let report = run_report(&config_path, &config);

        assert_eq!(
            report.verdict,
            ExecutionReadinessVerdict::PolicyContractIncomplete
        );
        assert!(
            report
                .reason
                .contains("submit_adapter_require_policy_echo must be true"),
            "unexpected reason: {}",
            report.reason
        );
    }

    #[test]
    fn mock_rpc_and_adapter_yield_ready_for_execution_dry_run_while_execution_stays_disabled() {
        let temp = temp_dir("execution_readiness_green");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            1,
            json!({"jsonrpc":"2.0","id":1,"result":123456u64}).to_string(),
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            1,
            json!({
                "status": "ok",
                "route": "jito",
                "contract_version": "v1",
                "detail": "simulated"
            })
            .to_string(),
            Some("authorization"),
        );

        let mut config = base_adapter_config("paper");
        config.sqlite.path = "state/nonexistent.db".to_string();
        config.execution.rpc_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config);

        assert_eq!(
            report.verdict,
            ExecutionReadinessVerdict::ReadyForExecutionDryRun
        );
        assert!(report.ready_for_dry_run);
        assert!(report.connectivity_valid);
        assert!(report.blocked_for_activation);
        assert!(!report.execution_enabled);

        let captured = adapter_server.take_requests();
        assert_eq!(
            captured.len(),
            1,
            "simulate probe should hit adapter exactly once"
        );
        assert!(
            captured[0].body.contains("\"action\":\"simulate\""),
            "unexpected adapter request body: {}",
            captured[0].body
        );
        assert!(
            captured[0].body.contains("\"dry_run\":true"),
            "unexpected adapter request body: {}",
            captured[0].body
        );
    }

    #[test]
    fn report_does_not_depend_on_discovery_or_sqlite_truth() {
        let temp = temp_dir("execution_readiness_no_discovery_dependency");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            1,
            json!({"jsonrpc":"2.0","id":1,"result":777u64}).to_string(),
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            1,
            json!({
                "status": "reject",
                "route": "jito",
                "contract_version": "v1",
                "code": "simulation_rejected",
                "detail": "business reject for probe"
            })
            .to_string(),
            Some("authorization"),
        );

        let mut config = base_adapter_config("paper");
        config.sqlite.path = "/definitely/missing/runtime.db".to_string();
        config.execution.rpc_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config);

        assert!(report.connectivity_valid);
        assert!(report.adapter_contract_valid);
        assert_eq!(
            report.adapter_probe.classification,
            AdapterProbeClassification::BusinessReject
        );
    }

    fn base_adapter_config(env: &str) -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = env.to_string();
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

    fn run_report(config_path: &Path, config: &AppConfig) -> ExecutionReadinessAuditReport {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test tokio runtime");
        runtime
            .block_on(evaluate_execution_readiness(config_path, config))
            .expect("report should build")
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_execution_readiness_{}_{}_{}",
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

    struct MockHttpServer {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl MockHttpServer {
        fn spawn(
            expected_requests: usize,
            response_body: String,
            required_header: Option<&str>,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            listener
                .set_nonblocking(false)
                .expect("mock server blocking listener");
            let addr = listener.local_addr().expect("mock server addr");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_clone = Arc::clone(&requests);
            let required_header = required_header.map(str::to_string);
            let handle = thread::spawn(move || {
                for _ in 0..expected_requests {
                    let (mut stream, _) = listener.accept().expect("accept mock request");
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
                    write_http_response(&mut stream, &response_body);
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

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write mock response");
        stream.flush().expect("flush mock response");
    }

    #[test]
    fn human_output_contains_readiness_summary_fields() {
        let report = ExecutionReadinessAuditReport {
            generated_at: Utc::now(),
            config_path: "/tmp/config.toml".to_string(),
            execution_enabled: false,
            mode: "adapter_submit_confirm".to_string(),
            mode_compatible: true,
            config_valid: true,
            connectivity_valid: true,
            adapter_contract_valid: true,
            signer_contract_valid: true,
            policy_contract_valid: true,
            route_contract_valid: true,
            ready_for_dry_run: true,
            blocked_for_activation: true,
            verdict: ExecutionReadinessVerdict::ReadyForExecutionDryRun,
            reason: "ready".to_string(),
            activation_blockers: vec![
                "execution.enabled=false (Stage 4 preparation only)".to_string()
            ],
            static_blockers: Vec::new(),
            warnings: Vec::new(),
            signer_pubkey_configured: true,
            adapter_auth_token_configured: true,
            adapter_auth_token_source: "inline".to_string(),
            adapter_hmac_configured: false,
            adapter_hmac_source: "none".to_string(),
            route_summary: RouteSummary {
                default_route: "jito".to_string(),
                allowed_routes: vec!["jito".to_string(), "rpc".to_string()],
                submit_route_order: vec!["jito".to_string(), "rpc".to_string()],
                policy_echo_required: true,
                simulate_before_submit: true,
            },
            rpc_probe: RpcProbeReport {
                required: true,
                attempted: true,
                reachable: true,
                classification: RpcProbeClassification::Reachable,
                timeout_ms: 3_000,
                successful_endpoint_role: Some("primary".to_string()),
                detail: "ok".to_string(),
                slot: Some(123),
            },
            adapter_probe: AdapterProbeReport {
                required: true,
                attempted: true,
                reachable: true,
                contract_valid: true,
                accepted: Some(true),
                classification: AdapterProbeClassification::Accepted,
                timeout_ms: 3_000,
                route: Some("jito".to_string()),
                detail: "simulated".to_string(),
            },
        };

        let output = render_human(&report);
        assert!(output.contains("verdict=ready_for_execution_dry_run"));
        assert!(output.contains("ready_for_dry_run=true"));
        assert!(output.contains("adapter_probe_classification=accepted"));
    }

    #[test]
    fn resolve_execution_secrets_for_audit_does_not_require_execution_enabled() {
        let temp = temp_dir("execution_readiness_secret_resolution");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);
        let secret_path = temp.join("adapter.token");
        fs::write(&secret_path, "secret-token\n").expect("write secret file");
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600))
            .expect("set secret file mode");

        let mut execution = ExecutionConfig::default();
        execution.enabled = false;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = "adapter.token".to_string();

        resolve_execution_secrets_for_audit(&mut execution, &config_path)
            .expect("audit secret resolution should succeed");
        assert_eq!(execution.submit_adapter_auth_token, "secret-token");
        assert!(
            !execution.enabled,
            "audit helper must restore original enabled flag"
        );
    }
}
