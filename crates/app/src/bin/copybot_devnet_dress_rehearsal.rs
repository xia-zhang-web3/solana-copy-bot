use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_storage::{ExecutionDevnetDressRehearsalWrite, SqliteStore};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use url::Url;

#[cfg(test)]
use serde_json::json;

#[allow(dead_code)]
#[path = "copybot_execution_dry_run_rehearsal.rs"]
mod execution_dry_run_rehearsal;
#[allow(dead_code)]
#[path = "copybot_execution_readiness_audit.rs"]
mod execution_readiness_audit;
#[allow(dead_code)]
#[path = "copybot_tiny_live_policy_audit.rs"]
mod tiny_live_policy_audit;

const USAGE: &str = "usage: copybot_devnet_dress_rehearsal --config <path> [--route <route> --token <mint> --notional-sol <value>] [--history] [--limit <n>] [--json]";
const TARGET_ENVIRONMENT: &str = "devnet";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for devnet dress rehearsal")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    route: Option<String>,
    token: Option<String>,
    notional_sol: Option<f64>,
    history: bool,
    limit: usize,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DevnetDressRehearsalVerdict {
    DevnetRehearsalGreen,
    DevnetRehearsalGreenWithBusinessReject,
    DevnetRehearsalBlockedByConnectivity,
    DevnetRehearsalBlockedByAdapterContract,
    DevnetRehearsalBlockedByPolicyContract,
    DevnetRehearsalBlockedByStaticContract,
    DevnetRehearsalInputInvalid,
    DevnetRehearsalRefusedForProdProfile,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RehearsalIntent {
    pub(crate) route: String,
    pub(crate) token: String,
    pub(crate) notional_sol: f64,
    pub(crate) side: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExecutionReadinessSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) config_valid: bool,
    pub(crate) connectivity_valid: bool,
    pub(crate) adapter_contract_valid: bool,
    pub(crate) signer_contract_valid: bool,
    pub(crate) policy_contract_valid: bool,
    pub(crate) route_contract_valid: bool,
    pub(crate) ready_for_dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TinyLivePolicySummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) bounded: bool,
    pub(crate) enabled: bool,
    pub(crate) blocker_count: usize,
    pub(crate) first_blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DryRunSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) ready_for_dry_run: bool,
    pub(crate) would_be_admissible_for_later_tiny_live: bool,
    pub(crate) connectivity_valid: bool,
    pub(crate) adapter_contract_valid: bool,
    pub(crate) policy_echo_present: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DevnetDressRehearsalReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) rehearsal_id: Option<i64>,
    pub(crate) config_path: String,
    pub(crate) db_path: String,
    pub(crate) target_environment: String,
    pub(crate) config_env: String,
    pub(crate) prod_profile_refused: bool,
    pub(crate) production_unchanged: bool,
    pub(crate) planning_safe_for_production: bool,
    pub(crate) stage3_gate_not_evaluated: bool,
    pub(crate) execution_enabled: bool,
    pub(crate) verdict: DevnetDressRehearsalVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) intent: RehearsalIntent,
    pub(crate) readiness: Option<ExecutionReadinessSummary>,
    pub(crate) tiny_live_policy: Option<TinyLivePolicySummary>,
    pub(crate) dry_run: Option<DryRunSummary>,
    pub(crate) readiness_audit: Option<execution_readiness_audit::ExecutionReadinessAuditReport>,
    pub(crate) tiny_live_policy_audit: Option<tiny_live_policy_audit::TinyLivePolicyAuditReport>,
    pub(crate) dry_run_rehearsal:
        Option<execution_dry_run_rehearsal::ExecutionDryRunRehearsalReport>,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetDressRehearsalHistoryEntry {
    rehearsal_id: i64,
    rehearsed_at: DateTime<Utc>,
    target_environment: String,
    config_env: String,
    verdict: String,
    reason: String,
    route: String,
    token: String,
    side: String,
    notional_sol: f64,
    readiness_verdict: String,
    dry_run_verdict: Option<String>,
    tiny_live_policy_verdict: String,
    tiny_live_policy_bounded: bool,
    ready_for_dry_run: bool,
    would_be_admissible_for_later_tiny_live: bool,
    blockers: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetDressRehearsalHistoryReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    db_path: String,
    target_environment: String,
    records_loaded: usize,
    latest_rehearsed_at: Option<DateTime<Utc>>,
    verdict_counts: BTreeMap<String, usize>,
    rehearsals: Vec<DevnetDressRehearsalHistoryEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetDressRehearsalHistoryRefusalReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    db_path: String,
    target_environment: String,
    config_env: String,
    history_mode: bool,
    prod_profile_refused: bool,
    production_unchanged: bool,
    planning_safe_for_production: bool,
    verdict: DevnetDressRehearsalVerdict,
    reason: String,
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
    let mut route: Option<String> = None;
    let mut token: Option<String> = None;
    let mut notional_sol: Option<f64> = None;
    let mut history = false;
    let mut limit = 10usize;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--route" => route = Some(parse_string_arg("--route", args.next())?),
            "--token" => token = Some(parse_string_arg("--token", args.next())?),
            "--notional-sol" => notional_sol = Some(parse_f64_arg("--notional-sol", args.next())?),
            "--history" => history = true,
            "--limit" => limit = parse_usize_arg("--limit", args.next())?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if !history {
        let notional = notional_sol.ok_or_else(|| anyhow!("missing required --notional-sol"))?;
        if !notional.is_finite() || notional <= 0.0 {
            bail!("--notional-sol must be finite and > 0");
        }
        if route.is_none() {
            bail!("missing required --route");
        }
        if token.is_none() {
            bail!("missing required --token");
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        route,
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
    let db_path = execution_dry_run_rehearsal::resolve_db_path(
        &config.config_path,
        &loaded_config.sqlite.path,
    );

    if config.history {
        render_history_mode_output(
            &config.config_path,
            &db_path,
            &loaded_config,
            config.limit,
            config.json,
        )
    } else {
        let route = config.route.as_deref().expect("route validated");
        let token = config.token.as_deref().expect("token validated");
        let notional_sol = config.notional_sol.expect("notional validated");
        let mut report = evaluate_devnet_dress_rehearsal(
            &config.config_path,
            &db_path,
            &loaded_config,
            route,
            token,
            notional_sol,
        )
        .await?;
        if !report.prod_profile_refused {
            let store = SqliteStore::open(&db_path).with_context(|| {
                format!(
                    "failed opening devnet dress rehearsal history store {}",
                    db_path.display()
                )
            })?;
            let persisted = store
                .append_execution_devnet_dress_rehearsal(&persisted_row_for_report(&report)?)
                .context("failed persisting devnet dress rehearsal history")?;
            report.rehearsal_id = Some(persisted.rehearsal_id);
        }
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing devnet dress rehearsal json")
        } else {
            Ok(render_human(&report))
        }
    }
}

fn render_history_mode_output(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    limit: usize,
    json: bool,
) -> Result<String> {
    if is_production_like_env(loaded_config.system.env.as_str()) {
        let report = history_prod_refusal_report(config_path, db_path, loaded_config);
        return if json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing devnet dress rehearsal history refusal json")
        } else {
            Ok(render_history_refusal_human(&report))
        };
    }

    let store = SqliteStore::open(db_path).with_context(|| {
        format!(
            "failed opening devnet dress rehearsal history store {}",
            db_path.display()
        )
    })?;
    let report = build_history_report(&store, config_path, db_path, limit)?;
    if json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing devnet dress rehearsal history json")
    } else {
        Ok(render_history_human(&report))
    }
}

pub(crate) async fn evaluate_devnet_dress_rehearsal(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    route: &str,
    token: &str,
    notional_sol: f64,
) -> Result<DevnetDressRehearsalReport> {
    let intent = RehearsalIntent {
        route: route.trim().to_ascii_lowercase(),
        token: token.trim().to_string(),
        notional_sol,
        side: "buy".to_string(),
    };

    if is_production_like_env(loaded_config.system.env.as_str()) {
        return Ok(prod_refusal_report(
            config_path,
            db_path,
            loaded_config,
            intent,
        ));
    }

    let tiny_live_policy_report =
        tiny_live_policy_audit::evaluate_tiny_live_policy(config_path, loaded_config)?;
    let tiny_live_policy_summary = TinyLivePolicySummary {
        verdict: json_enum_string(&tiny_live_policy_report.verdict),
        reason: tiny_live_policy_report.reason.clone(),
        bounded: tiny_live_policy_report.current_config_bounded_for_later_tiny_live_discussion,
        enabled: tiny_live_policy_report.tiny_live_policy_enabled,
        blocker_count: tiny_live_policy_report.blockers.len(),
        first_blocker: tiny_live_policy_report.blockers.first().cloned(),
    };

    let devnet_config = match build_devnet_target_config(loaded_config) {
        Ok(config) => config,
        Err(error) => {
            let reason = format!(
                "devnet adapter/rpc contract incomplete for non-production dress rehearsal: {error}"
            );
            let warnings = collect_warnings(
                loaded_config.execution.enabled,
                loaded_config.system.env.as_str(),
                &tiny_live_policy_report,
                None,
            );
            return Ok(DevnetDressRehearsalReport {
                generated_at: Utc::now(),
                rehearsal_id: None,
                config_path: config_path.display().to_string(),
                db_path: db_path.display().to_string(),
                target_environment: TARGET_ENVIRONMENT.to_string(),
                config_env: loaded_config.system.env.clone(),
                prod_profile_refused: false,
                production_unchanged: true,
                planning_safe_for_production: true,
                stage3_gate_not_evaluated: true,
                execution_enabled: loaded_config.execution.enabled,
                verdict: DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract,
                reason: reason.clone(),
                blockers: vec![reason],
                warnings,
                intent,
                readiness: None,
                tiny_live_policy: Some(tiny_live_policy_summary),
                dry_run: None,
                readiness_audit: None,
                tiny_live_policy_audit: Some(tiny_live_policy_report),
                dry_run_rehearsal: None,
            });
        }
    };
    let readiness_audit =
        execution_readiness_audit::evaluate_execution_readiness(config_path, &devnet_config)
            .await?;
    let readiness_summary = ExecutionReadinessSummary {
        verdict: json_enum_string(&readiness_audit.verdict),
        reason: readiness_audit.reason.clone(),
        config_valid: readiness_audit.config_valid,
        connectivity_valid: readiness_audit.connectivity_valid,
        adapter_contract_valid: readiness_audit.adapter_contract_valid,
        signer_contract_valid: readiness_audit.signer_contract_valid,
        policy_contract_valid: readiness_audit.policy_contract_valid,
        route_contract_valid: readiness_audit.route_contract_valid,
        ready_for_dry_run: readiness_audit.ready_for_dry_run,
    };

    let dry_run_rehearsal = execution_dry_run_rehearsal::evaluate_execution_dry_run_rehearsal(
        config_path,
        db_path,
        &devnet_config,
        Some(route),
        token,
        notional_sol,
    )
    .await;

    let (dry_run_rehearsal, input_invalid_reason) = match dry_run_rehearsal {
        Ok(report) => (Some(report), None),
        Err(error) => {
            let raw = error.to_string();
            if is_input_contract_error(raw.as_str()) {
                (None, Some(raw))
            } else {
                return Err(error);
            }
        }
    };

    let dry_run_summary = dry_run_rehearsal.as_ref().map(|report| DryRunSummary {
        verdict: json_enum_string(&report.verdict),
        reason: report.reason.clone(),
        ready_for_dry_run: report.ready_for_dry_run,
        would_be_admissible_for_later_tiny_live: report.would_be_admissible_for_later_tiny_live,
        connectivity_valid: report.connectivity_valid,
        adapter_contract_valid: report.adapter_contract_valid,
        policy_echo_present: report.adapter_rehearsal.policy_echo_present,
    });

    let verdict = derive_verdict(
        input_invalid_reason.as_deref(),
        &readiness_audit,
        &tiny_live_policy_report,
        dry_run_rehearsal.as_ref(),
    );
    let reason = derive_reason(
        verdict,
        input_invalid_reason.as_deref(),
        &readiness_audit,
        &tiny_live_policy_report,
        dry_run_rehearsal.as_ref(),
    );
    let blockers = collect_blockers(
        verdict,
        input_invalid_reason.as_deref(),
        &readiness_audit,
        &tiny_live_policy_report,
        dry_run_rehearsal.as_ref(),
    );
    let warnings = collect_warnings(
        loaded_config.execution.enabled,
        loaded_config.system.env.as_str(),
        &tiny_live_policy_report,
        dry_run_rehearsal.as_ref(),
    );

    Ok(DevnetDressRehearsalReport {
        generated_at: Utc::now(),
        rehearsal_id: None,
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        prod_profile_refused: false,
        production_unchanged: true,
        planning_safe_for_production: true,
        stage3_gate_not_evaluated: true,
        execution_enabled: loaded_config.execution.enabled,
        verdict,
        reason,
        blockers,
        warnings,
        intent,
        readiness: Some(readiness_summary),
        tiny_live_policy: Some(tiny_live_policy_summary),
        dry_run: dry_run_summary,
        readiness_audit: Some(readiness_audit),
        tiny_live_policy_audit: Some(tiny_live_policy_report),
        dry_run_rehearsal,
    })
}

fn prod_refusal_report(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    intent: RehearsalIntent,
) -> DevnetDressRehearsalReport {
    let reason = format!(
        "copybot_devnet_dress_rehearsal refuses production-like system.env={}",
        loaded_config.system.env
    );
    DevnetDressRehearsalReport {
        generated_at: Utc::now(),
        rehearsal_id: None,
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        prod_profile_refused: true,
        production_unchanged: true,
        planning_safe_for_production: true,
        stage3_gate_not_evaluated: true,
        execution_enabled: loaded_config.execution.enabled,
        verdict: DevnetDressRehearsalVerdict::DevnetRehearsalRefusedForProdProfile,
        reason: reason.clone(),
        blockers: vec![reason],
        warnings: vec![
            "production execution remains untouched; use a non-production config profile for devnet dress rehearsal".to_string(),
        ],
        intent,
        readiness: None,
        tiny_live_policy: None,
        dry_run: None,
        readiness_audit: None,
        tiny_live_policy_audit: None,
        dry_run_rehearsal: None,
    }
}

fn build_devnet_target_config(loaded_config: &AppConfig) -> Result<AppConfig> {
    let mut config = loaded_config.clone();
    let devnet_rpc = config.execution.rpc_devnet_http_url.trim().to_string();
    if devnet_rpc.is_empty() {
        bail!("execution.rpc_devnet_http_url must be configured for devnet dress rehearsal");
    }
    let devnet_adapter_primary = config
        .execution
        .submit_adapter_devnet_http_url
        .trim()
        .to_string();
    let devnet_adapter_fallback = config
        .execution
        .submit_adapter_devnet_fallback_http_url
        .trim()
        .to_string();
    if devnet_adapter_primary.is_empty() && devnet_adapter_fallback.is_empty() {
        bail!(
            "execution.submit_adapter_devnet_http_url or execution.submit_adapter_devnet_fallback_http_url must be configured for devnet dress rehearsal"
        );
    }
    validate_devnet_adapter_contract(
        config.execution.submit_adapter_http_url.as_str(),
        config.execution.submit_adapter_fallback_http_url.as_str(),
        devnet_adapter_primary.as_str(),
        devnet_adapter_fallback.as_str(),
    )?;
    config.execution.rpc_http_url = devnet_rpc;
    config.execution.rpc_fallback_http_url.clear();
    config.execution.submit_adapter_http_url = devnet_adapter_primary;
    config.execution.submit_adapter_fallback_http_url = devnet_adapter_fallback;
    Ok(config)
}

fn validate_devnet_adapter_contract(
    primary_prod_endpoint: &str,
    fallback_prod_endpoint: &str,
    primary_devnet_endpoint: &str,
    fallback_devnet_endpoint: &str,
) -> Result<()> {
    let mut prod_identities = Vec::new();
    for (field_name, endpoint) in [
        ("execution.submit_adapter_http_url", primary_prod_endpoint),
        (
            "execution.submit_adapter_fallback_http_url",
            fallback_prod_endpoint,
        ),
    ] {
        let trimmed = endpoint.trim();
        if trimmed.is_empty() {
            continue;
        }
        prod_identities.push((
            field_name,
            adapter_endpoint_identity(trimmed)
                .with_context(|| format!("failed normalizing {field_name}"))?,
        ));
    }

    let mut devnet_identities = Vec::new();
    for (field_name, endpoint) in [
        (
            "execution.submit_adapter_devnet_http_url",
            primary_devnet_endpoint,
        ),
        (
            "execution.submit_adapter_devnet_fallback_http_url",
            fallback_devnet_endpoint,
        ),
    ] {
        let trimmed = endpoint.trim();
        if trimmed.is_empty() {
            continue;
        }
        let identity = adapter_endpoint_identity(trimmed)
            .with_context(|| format!("failed normalizing {field_name}"))?;
        for (prod_field_name, prod_identity) in &prod_identities {
            if &identity == prod_identity {
                bail!(
                    "{field_name} must not reuse {prod_field_name}; configure an explicit non-production adapter endpoint for devnet dress rehearsal"
                );
            }
        }
        if devnet_identities
            .iter()
            .any(|(_, existing_identity): &(String, String)| existing_identity == &identity)
        {
            bail!(
                "{field_name} must resolve to a distinct endpoint when both devnet adapter endpoints are set"
            );
        }
        devnet_identities.push((field_name.to_string(), identity));
    }
    Ok(())
}

fn adapter_endpoint_identity(endpoint: &str) -> Result<String> {
    let parsed = Url::parse(endpoint.trim())
        .map_err(|error| anyhow!("invalid adapter endpoint URL: {error}"))?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https") {
        bail!("adapter endpoint must use http:// or https:// scheme");
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("adapter endpoint missing host"))?
        .to_ascii_lowercase();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("adapter endpoint missing known default port"))?;
    let path = if parsed.path().is_empty() {
        "/".to_string()
    } else {
        parsed.path().to_string()
    };
    Ok(format!("{scheme}://{host}:{port}{path}"))
}

pub(crate) fn is_production_like_env(env: &str) -> bool {
    let env_norm = env.trim().to_ascii_lowercase();
    matches!(env_norm.as_str(), "prod" | "production")
        || env_norm.starts_with("prod-")
        || env_norm.starts_with("prod_")
        || env_norm.starts_with("production-")
        || env_norm.starts_with("production_")
}

fn is_input_contract_error(reason: &str) -> bool {
    reason.contains("rehearsal route=")
        || reason.contains("rehearsal token")
        || reason.contains("ascii")
        || reason.contains("not in execution.submit_allowed_routes")
}

fn derive_verdict(
    input_invalid_reason: Option<&str>,
    readiness: &execution_readiness_audit::ExecutionReadinessAuditReport,
    policy: &tiny_live_policy_audit::TinyLivePolicyAuditReport,
    dry_run: Option<&execution_dry_run_rehearsal::ExecutionDryRunRehearsalReport>,
) -> DevnetDressRehearsalVerdict {
    if input_invalid_reason.is_some() {
        return DevnetDressRehearsalVerdict::DevnetRehearsalInputInvalid;
    }

    if matches!(
        readiness.verdict,
        execution_readiness_audit::ExecutionReadinessVerdict::ConfigValidButConnectivityBlocked
    ) {
        return DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByConnectivity;
    }
    if matches!(
        readiness.verdict,
        execution_readiness_audit::ExecutionReadinessVerdict::AdapterContractIncomplete
    ) {
        return DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByAdapterContract;
    }
    if matches!(
        readiness.verdict,
        execution_readiness_audit::ExecutionReadinessVerdict::SignerContractIncomplete
            | execution_readiness_audit::ExecutionReadinessVerdict::ModeContractIncompatible
            | execution_readiness_audit::ExecutionReadinessVerdict::NotApplicableExecutionDisabledContractOnly
    ) {
        return DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract;
    }

    if matches!(
        readiness.verdict,
        execution_readiness_audit::ExecutionReadinessVerdict::PolicyContractIncomplete
    ) || !policy.current_config_bounded_for_later_tiny_live_discussion
    {
        return DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByPolicyContract;
    }

    let Some(dry_run) = dry_run else {
        return DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract;
    };

    match dry_run.verdict {
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalGreen => {
            DevnetDressRehearsalVerdict::DevnetRehearsalGreen
        }
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalGreenWithBusinessReject => {
            DevnetDressRehearsalVerdict::DevnetRehearsalGreenWithBusinessReject
        }
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalBlockedByConnectivity => {
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByConnectivity
        }
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalBlockedByAdapterContract => {
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByAdapterContract
        }
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalBlockedByPolicyEcho => {
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByPolicyContract
        }
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalBlockedByStaticContract => {
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract
        }
        execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalInputInvalid => {
            DevnetDressRehearsalVerdict::DevnetRehearsalInputInvalid
        }
    }
}

fn derive_reason(
    verdict: DevnetDressRehearsalVerdict,
    input_invalid_reason: Option<&str>,
    readiness: &execution_readiness_audit::ExecutionReadinessAuditReport,
    policy: &tiny_live_policy_audit::TinyLivePolicyAuditReport,
    dry_run: Option<&execution_dry_run_rehearsal::ExecutionDryRunRehearsalReport>,
) -> String {
    match verdict {
        DevnetDressRehearsalVerdict::DevnetRehearsalGreen => {
            "non-production devnet dress rehearsal completed green; production execution remains unchanged".to_string()
        }
        DevnetDressRehearsalVerdict::DevnetRehearsalGreenWithBusinessReject => dry_run
            .map(|report| {
                format!(
                    "non-production devnet dress rehearsal reached adapter simulate end-to-end; business reject remained inside safe non-prod contract: {}",
                    report.reason
                )
            })
            .unwrap_or_else(|| "non-production devnet dress rehearsal reached adapter simulate end-to-end with a business reject".to_string()),
        DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByConnectivity => dry_run
            .map(|report| report.reason.clone())
            .unwrap_or_else(|| readiness.reason.clone()),
        DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByAdapterContract => dry_run
            .map(|report| report.reason.clone())
            .unwrap_or_else(|| readiness.reason.clone()),
        DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByPolicyContract => dry_run
            .filter(|report| {
                report.verdict
                    == execution_dry_run_rehearsal::ExecutionDryRunRehearsalVerdict::RehearsalBlockedByPolicyEcho
            })
            .map(|report| report.reason.clone())
            .unwrap_or_else(|| policy.reason.clone()),
        DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract => readiness.reason.clone(),
        DevnetDressRehearsalVerdict::DevnetRehearsalInputInvalid => input_invalid_reason
            .unwrap_or("devnet dress rehearsal input invalid")
            .to_string(),
        DevnetDressRehearsalVerdict::DevnetRehearsalRefusedForProdProfile => {
            "devnet dress rehearsal refused because the supplied config profile is production-like"
                .to_string()
        }
    }
}

fn collect_blockers(
    verdict: DevnetDressRehearsalVerdict,
    input_invalid_reason: Option<&str>,
    readiness: &execution_readiness_audit::ExecutionReadinessAuditReport,
    policy: &tiny_live_policy_audit::TinyLivePolicyAuditReport,
    dry_run: Option<&execution_dry_run_rehearsal::ExecutionDryRunRehearsalReport>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if let Some(reason) = input_invalid_reason {
        blockers.push(reason.to_string());
    }
    blockers.extend(readiness.static_blockers.iter().cloned());
    blockers.extend(readiness.activation_blockers.iter().cloned());
    blockers.extend(policy.blockers.iter().cloned());
    if let Some(report) = dry_run {
        blockers.extend(report.blockers.iter().cloned());
    }
    if matches!(
        verdict,
        DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByConnectivity
            | DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByAdapterContract
            | DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByPolicyContract
            | DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract
            | DevnetDressRehearsalVerdict::DevnetRehearsalInputInvalid
    ) {
        blockers.push(derive_reason(
            verdict,
            input_invalid_reason,
            readiness,
            policy,
            dry_run,
        ));
    }
    blockers.sort();
    blockers.dedup();
    blockers
}

fn collect_warnings(
    execution_enabled: bool,
    config_env: &str,
    policy: &tiny_live_policy_audit::TinyLivePolicyAuditReport,
    dry_run: Option<&execution_dry_run_rehearsal::ExecutionDryRunRehearsalReport>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if !execution_enabled {
        warnings.push(
            "execution.enabled=false; devnet dress rehearsal used only non-production simulate/read checks"
                .to_string(),
        );
    }
    warnings.push(format!(
        "system.env={} is treated as non-production for this command; production activation state is unchanged",
        config_env
    ));
    warnings.push(
        "Stage 3 discovery evidence remains the production gate; devnet dress rehearsal does not authorize prod activation".to_string(),
    );
    warnings.extend(policy.warnings.iter().cloned());
    if let Some(report) = dry_run {
        warnings.extend(report.warnings.iter().cloned());
    }
    warnings.sort();
    warnings.dedup();
    warnings
}

fn persisted_row_for_report(
    report: &DevnetDressRehearsalReport,
) -> Result<ExecutionDevnetDressRehearsalWrite> {
    let readiness = report
        .readiness
        .as_ref()
        .ok_or_else(|| anyhow!("missing readiness summary for persisted devnet dress rehearsal"))?;
    let policy = report.tiny_live_policy.as_ref().ok_or_else(|| {
        anyhow!("missing tiny-live policy summary for persisted devnet dress rehearsal")
    })?;

    Ok(ExecutionDevnetDressRehearsalWrite {
        rehearsed_at: report.generated_at,
        target_environment: report.target_environment.clone(),
        config_env: report.config_env.clone(),
        execution_mode: report
            .readiness_audit
            .as_ref()
            .map(|value| value.mode.clone())
            .unwrap_or_else(|| "<unknown>".to_string()),
        execution_enabled: report.execution_enabled,
        route: report.intent.route.clone(),
        token: report.intent.token.clone(),
        side: report.intent.side.clone(),
        notional_sol: report.intent.notional_sol,
        readiness_verdict: readiness.verdict.clone(),
        readiness_reason: readiness.reason.clone(),
        dry_run_verdict: report.dry_run.as_ref().map(|value| value.verdict.clone()),
        dry_run_reason: report.dry_run.as_ref().map(|value| value.reason.clone()),
        tiny_live_policy_verdict: policy.verdict.clone(),
        tiny_live_policy_reason: policy.reason.clone(),
        tiny_live_policy_bounded: policy.bounded,
        signer_pubkey_configured: report
            .readiness_audit
            .as_ref()
            .map(|value| value.signer_pubkey_configured)
            .unwrap_or(false),
        config_valid: readiness.config_valid,
        connectivity_valid: readiness.connectivity_valid,
        adapter_contract_valid: readiness.adapter_contract_valid,
        policy_contract_valid: readiness.policy_contract_valid,
        route_contract_valid: readiness.route_contract_valid,
        ready_for_dry_run: readiness.ready_for_dry_run,
        would_be_admissible_for_later_tiny_live: report
            .dry_run_rehearsal
            .as_ref()
            .map(|value| value.would_be_admissible_for_later_tiny_live)
            .unwrap_or(false),
        rpc_preconditions_valid: report
            .dry_run_rehearsal
            .as_ref()
            .map(|value| value.rpc_preflight.reachable)
            .unwrap_or(false),
        adapter_result_classification: report
            .dry_run_rehearsal
            .as_ref()
            .map(|value| json_enum_string(&value.adapter_rehearsal.classification)),
        adapter_accepted: report
            .dry_run_rehearsal
            .as_ref()
            .and_then(|value| value.adapter_rehearsal.accepted),
        policy_echo_present: report
            .dry_run_rehearsal
            .as_ref()
            .map(|value| value.adapter_rehearsal.policy_echo_present)
            .unwrap_or(false),
        route_echo_present: report
            .dry_run_rehearsal
            .as_ref()
            .map(|value| value.adapter_rehearsal.route_echo_present)
            .unwrap_or(false),
        contract_version_echo_present: report
            .dry_run_rehearsal
            .as_ref()
            .map(|value| value.adapter_rehearsal.contract_version_echo_present)
            .unwrap_or(false),
        verdict: json_enum_string(&report.verdict),
        reason: report.reason.clone(),
        blockers: report.blockers.clone(),
        warnings: report.warnings.clone(),
        rehearsal_json: serde_json::to_string(report)
            .context("failed serializing devnet dress rehearsal report")?,
    })
}

fn build_history_report(
    store: &SqliteStore,
    config_path: &Path,
    db_path: &Path,
    limit: usize,
) -> Result<DevnetDressRehearsalHistoryReport> {
    let rehearsals = store
        .list_execution_devnet_dress_rehearsals(limit)
        .context("failed loading execution devnet dress rehearsal history")?;
    let latest_rehearsed_at = rehearsals.first().map(|row| row.rehearsed_at);
    let mut verdict_counts = BTreeMap::new();
    let entries = rehearsals
        .into_iter()
        .map(|row| {
            *verdict_counts.entry(row.verdict.clone()).or_insert(0) += 1;
            DevnetDressRehearsalHistoryEntry {
                rehearsal_id: row.rehearsal_id,
                rehearsed_at: row.rehearsed_at,
                target_environment: row.target_environment,
                config_env: row.config_env,
                verdict: row.verdict,
                reason: row.reason,
                route: row.route,
                token: row.token,
                side: row.side,
                notional_sol: row.notional_sol,
                readiness_verdict: row.readiness_verdict,
                dry_run_verdict: row.dry_run_verdict,
                tiny_live_policy_verdict: row.tiny_live_policy_verdict,
                tiny_live_policy_bounded: row.tiny_live_policy_bounded,
                ready_for_dry_run: row.ready_for_dry_run,
                would_be_admissible_for_later_tiny_live: row
                    .would_be_admissible_for_later_tiny_live,
                blockers: row.blockers,
                warnings: row.warnings,
            }
        })
        .collect::<Vec<_>>();

    Ok(DevnetDressRehearsalHistoryReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        records_loaded: entries.len(),
        latest_rehearsed_at,
        verdict_counts,
        rehearsals: entries,
    })
}

fn history_prod_refusal_report(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
) -> DevnetDressRehearsalHistoryRefusalReport {
    DevnetDressRehearsalHistoryRefusalReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        history_mode: true,
        prod_profile_refused: true,
        production_unchanged: true,
        planning_safe_for_production: true,
        verdict: DevnetDressRehearsalVerdict::DevnetRehearsalRefusedForProdProfile,
        reason: format!(
            "copybot_devnet_dress_rehearsal --history refuses production-like system.env={}",
            loaded_config.system.env
        ),
    }
}

fn render_human(report: &DevnetDressRehearsalReport) -> String {
    let readiness_verdict = report
        .readiness
        .as_ref()
        .map(|value| value.verdict.clone())
        .unwrap_or_else(|| "null".to_string());
    let dry_run_verdict = report
        .dry_run
        .as_ref()
        .map(|value| value.verdict.clone())
        .unwrap_or_else(|| "null".to_string());
    let policy_verdict = report
        .tiny_live_policy
        .as_ref()
        .map(|value| value.verdict.clone())
        .unwrap_or_else(|| "null".to_string());
    [
        "event=copybot_devnet_dress_rehearsal".to_string(),
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
        format!("target_environment={}", report.target_environment),
        format!("config_env={}", report.config_env),
        format!("prod_profile_refused={}", report.prod_profile_refused),
        format!("production_unchanged={}", report.production_unchanged),
        format!(
            "planning_safe_for_production={}",
            report.planning_safe_for_production
        ),
        format!(
            "stage3_gate_not_evaluated={}",
            report.stage3_gate_not_evaluated
        ),
        format!("execution_enabled={}", report.execution_enabled),
        format!("verdict={}", json_enum_string(&report.verdict)),
        format!("reason={}", report.reason),
        format!("route={}", report.intent.route),
        format!("token={}", report.intent.token),
        format!("side={}", report.intent.side),
        format!("notional_sol={}", report.intent.notional_sol),
        format!("readiness_verdict={readiness_verdict}"),
        format!("dry_run_verdict={dry_run_verdict}"),
        format!("tiny_live_policy_verdict={policy_verdict}"),
        format!("blockers={}", report.blockers.join(" | ")),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

fn render_history_human(report: &DevnetDressRehearsalHistoryReport) -> String {
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
            "latest_verdict={} latest_reason={} latest_target_environment={}",
            entry.verdict, entry.reason, entry.target_environment
        )
    });
    [
        "event=copybot_devnet_dress_rehearsal_history".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
        format!("target_environment={}", report.target_environment),
        format!("records_loaded={}", report.records_loaded),
        format!("latest_rehearsed_at={latest}"),
        format!("verdict_counts={counts}"),
        latest_entry.unwrap_or_else(|| "latest_verdict=null".to_string()),
    ]
    .join("\n")
}

fn render_history_refusal_human(report: &DevnetDressRehearsalHistoryRefusalReport) -> String {
    [
        "event=copybot_devnet_dress_rehearsal_history".to_string(),
        format!("target_environment={}", report.target_environment),
        format!("config_env={}", report.config_env),
        format!("history_mode={}", report.history_mode),
        format!("prod_profile_refused={}", report.prod_profile_refused),
        format!("production_unchanged={}", report.production_unchanged),
        format!(
            "planning_safe_for_production={}",
            report.planning_safe_for_production
        ),
        format!("verdict={}", json_enum_string(&report.verdict)),
        format!("reason={}", report.reason),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
    ]
    .join("\n")
}

fn json_enum_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prod_profile_is_refused() {
        let temp = temp_dir("devnet_dress_prod_refused");
        let config_path = temp.join("prod.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.system.env = "prod-live".to_string();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_devnet_http_url = "http://127.0.0.1:8899".to_string();
        config.execution.submit_adapter_devnet_http_url =
            "http://127.0.0.1:18080/submit".to_string();

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(
            report.verdict,
            DevnetDressRehearsalVerdict::DevnetRehearsalRefusedForProdProfile
        );
        assert!(report.prod_profile_refused);
        assert!(report.reason.contains("production-like"));
    }

    #[test]
    fn prod_profile_is_refused_in_history_mode() {
        let temp = temp_dir("devnet_dress_history_prod_refused");
        let config_path = temp.join("prod.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.system.env = "production_canary".to_string();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.submit_adapter_devnet_http_url =
            "http://127.0.0.1:18080/submit".to_string();

        let output = run_history_output(&config_path, &config);
        let report: Value = serde_json::from_str(&output).expect("history refusal json");
        assert_eq!(
            report["verdict"],
            "devnet_rehearsal_refused_for_prod_profile"
        );
        assert_eq!(report["prod_profile_refused"], true);
        assert_eq!(report["history_mode"], true);
    }

    #[test]
    fn valid_non_prod_config_runs_and_persists_environment_labeled_history() {
        let temp = temp_dir("devnet_dress_green");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":123456u64
                })),
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
            vec![
                MockResponse::json(json!({
                    "status":"ok",
                    "ok":true,
                    "accepted":true,
                    "route":"jito",
                    "contract_version":"v1",
                    "detail":"simulated"
                })),
                MockResponse::json(json!({
                    "status":"ok",
                    "ok":true,
                    "accepted":true,
                    "route":"jito",
                    "contract_version":"v1",
                    "detail":"simulated"
                })),
            ],
            Some("authorization"),
        );

        let mut config = base_devnet_config();
        let db_path = temp.join("runtime.db");
        config.sqlite.path = db_path.display().to_string();
        config.execution.rpc_devnet_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url =
            "https://prod.adapter.example.com/submit".to_string();
        config.execution.submit_adapter_devnet_http_url = adapter_server.url("/submit");

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(
            report.verdict,
            DevnetDressRehearsalVerdict::DevnetRehearsalGreen,
            "{}",
            serde_json::to_string_pretty(&report).expect("report json")
        );
        assert!(report.production_unchanged);
        assert!(!report.execution_enabled);

        let store = SqliteStore::open(&db_path).expect("open store");
        let row = store
            .append_execution_devnet_dress_rehearsal(
                &persisted_row_for_report(&report).expect("persisted row"),
            )
            .expect("append row");
        assert_eq!(row.target_environment, TARGET_ENVIRONMENT);
        assert_eq!(row.config_env, "paper-devnet");

        let history =
            build_history_report(&store, &config_path, &db_path, 5).expect("history report");
        assert_eq!(history.records_loaded, 1);
        assert_eq!(history.rehearsals[0].target_environment, TARGET_ENVIRONMENT);
        let captured = adapter_server.take_requests();
        assert_eq!(captured.len(), 2);
        assert!(captured
            .iter()
            .all(|request| request.body.contains("\"action\":\"simulate\"")));
    }

    #[test]
    fn missing_devnet_adapter_endpoint_is_refused() {
        let temp = temp_dir("devnet_dress_missing_adapter");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.submit_adapter_devnet_http_url.clear();
        config
            .execution
            .submit_adapter_devnet_fallback_http_url
            .clear();

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(
            report.verdict,
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract
        );
        assert!(report.reason.contains("submit_adapter_devnet_http_url"));
    }

    #[test]
    fn reusing_normal_adapter_endpoint_for_devnet_is_refused() {
        let temp = temp_dir("devnet_dress_adapter_reuse_refused");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.submit_adapter_http_url =
            "https://prod.adapter.example.com/submit".to_string();
        config.execution.submit_adapter_devnet_http_url =
            "https://prod.adapter.example.com/submit".to_string();

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(
            report.verdict,
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract
        );
        assert!(report.reason.contains("must not reuse"));
    }

    #[test]
    fn connectivity_failures_are_classified_correctly() {
        let temp = temp_dir("devnet_dress_connectivity");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_devnet_http_url = "http://127.0.0.1:1".to_string();
        config.execution.submit_adapter_devnet_http_url = "http://127.0.0.1:1/submit".to_string();

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(
            report.verdict,
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByConnectivity
        );
    }

    #[test]
    fn policy_route_mismatch_is_classified_correctly() {
        let temp = temp_dir("devnet_dress_policy_route_mismatch");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":1u64
                })),
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":1u64},"value":{"blockhash":"abc","lastValidBlockHeight":2u64}}
                })),
                MockResponse::json(json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "result":{"context":{"slot":1u64},"value":42u64}
                })),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "status":"ok",
                    "ok":true,
                    "accepted":true,
                    "route":"jito",
                    "contract_version":"v1",
                    "detail":"simulated"
                })),
                MockResponse::json(json!({
                    "status":"ok",
                    "ok":true,
                    "accepted":true,
                    "route":"jito",
                    "contract_version":"v1",
                    "detail":"simulated"
                })),
            ],
            Some("authorization"),
        );

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_devnet_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url =
            "https://prod.adapter.example.com/submit".to_string();
        config.execution.submit_adapter_devnet_http_url = adapter_server.url("/submit");
        config.tiny_live_policy.allowed_routes = vec!["rpc".to_string()];
        config.tiny_live_policy.max_route_slippage_bps =
            [("rpc".to_string(), 50.0)].into_iter().collect();
        config.tiny_live_policy.max_route_tip_lamports =
            [("rpc".to_string(), 0u64)].into_iter().collect();
        config
            .tiny_live_policy
            .max_route_compute_unit_price_micro_lamports =
            [("rpc".to_string(), 1000u64)].into_iter().collect();

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(
            report.verdict,
            DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByPolicyContract,
            "{}",
            serde_json::to_string_pretty(&report).expect("report json")
        );
        assert!(report.reason.contains("tiny-live") || report.reason.contains("policy"));
    }

    #[test]
    fn no_prod_activation_flags_are_changed() {
        let temp = temp_dir("devnet_dress_no_prod_flags");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.enabled = false;
        let before = config.execution.enabled;

        let report = run_report(&config_path, &config, "jito", WSOL, 0.01);
        assert_eq!(config.execution.enabled, before);
        assert!(!report.execution_enabled);
    }

    const WSOL: &str = "So11111111111111111111111111111111111111112";

    fn base_devnet_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "paper-devnet".to_string();
        config.execution.enabled = false;
        config.execution.mode = "adapter_submit_confirm".to_string();
        config.execution.default_route = "jito".to_string();
        config.execution.rpc_http_url = "https://api.mainnet-beta.solana.com".to_string();
        config.execution.rpc_devnet_http_url = "http://127.0.0.1:8899".to_string();
        config.execution.submit_adapter_http_url = "http://127.0.0.1:8080/submit".to_string();
        config.execution.submit_adapter_devnet_http_url =
            "http://127.0.0.1:18080/submit".to_string();
        config.execution.submit_adapter_contract_version = "v1".to_string();
        config.execution.submit_adapter_require_policy_echo = true;
        config.execution.submit_allowed_routes = vec!["jito".to_string()];
        config.execution.submit_route_order = vec!["jito".to_string()];
        config.execution.submit_route_max_slippage_bps =
            [("jito".to_string(), 50.0)].into_iter().collect();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 10_000)].into_iter().collect();
        config.execution.submit_route_compute_unit_limit =
            [("jito".to_string(), 300_000)].into_iter().collect();
        config
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500)].into_iter().collect();
        config.execution.execution_signer_pubkey = "11111111111111111111111111111111".to_string();
        config.execution.submit_adapter_auth_token = "adapter-token".to_string();

        config.tiny_live_policy.enabled = true;
        config.tiny_live_policy.max_trade_notional_sol = 0.05;
        config.tiny_live_policy.max_batch_size = 1;
        config.tiny_live_policy.max_concurrent_positions = 1;
        config.tiny_live_policy.max_daily_loss_limit_pct = 1.0;
        config.tiny_live_policy.allowed_routes = vec!["jito".to_string()];
        config.tiny_live_policy.require_policy_echo = true;
        config.tiny_live_policy.max_pretrade_fee_overhead_bps = 1_000;
        config.tiny_live_policy.max_pretrade_priority_fee_lamports = 2_000;
        config.tiny_live_policy.max_route_slippage_bps =
            [("jito".to_string(), 50.0)].into_iter().collect();
        config.tiny_live_policy.max_route_tip_lamports =
            [("jito".to_string(), 10_000u64)].into_iter().collect();
        config
            .tiny_live_policy
            .max_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500u64)].into_iter().collect();

        config.shadow.copy_notional_sol = 0.05;
        config.risk.max_position_sol = 0.05;
        config.risk.max_concurrent_positions = 1;
        config.risk.daily_loss_limit_pct = 1.0;
        config.execution.batch_size = 1;
        config.execution.pretrade_max_fee_overhead_bps = 1_000;
        config.execution.pretrade_max_priority_fee_lamports = 2_000;
        config
    }

    fn run_report(
        config_path: &Path,
        config: &AppConfig,
        route: &str,
        token: &str,
        notional_sol: f64,
    ) -> DevnetDressRehearsalReport {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");
        runtime
            .block_on(evaluate_devnet_dress_rehearsal(
                config_path,
                &execution_dry_run_rehearsal::resolve_db_path(config_path, &config.sqlite.path),
                config,
                route,
                token,
                notional_sol,
            ))
            .expect("devnet dress rehearsal report")
    }

    fn run_history_output(config_path: &Path, config: &AppConfig) -> String {
        let db_path =
            execution_dry_run_rehearsal::resolve_db_path(config_path, &config.sqlite.path);
        render_history_mode_output(config_path, &db_path, config, 10, true)
            .expect("devnet dress rehearsal history output")
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_devnet_dress_rehearsal_{}_{}_{}",
            label,
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_placeholder_config(path: &Path) {
        fs::write(path, "[system]\nenv = \"paper-devnet\"\n").expect("write placeholder config");
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
        fn json(body: serde_json::Value) -> Self {
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
            let required_header = required_header.map(|value| value.to_ascii_lowercase());
            let handle = thread::spawn(move || {
                for response in responses {
                    let (mut stream, _) = listener.accept().expect("accept request");
                    let request = read_request(&mut stream);
                    if let Some(header) = required_header.as_deref() {
                        assert!(
                            request.to_ascii_lowercase().contains(header),
                            "required header {header} missing in request: {request}"
                        );
                    }
                    let body = request
                        .split("\r\n\r\n")
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    requests_clone
                        .lock()
                        .expect("lock captured requests")
                        .push(CapturedRequest { body });
                    write_response(&mut stream, &response);
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

        #[allow(dead_code)]
        fn take_requests(&self) -> Vec<CapturedRequest> {
            self.requests
                .lock()
                .expect("lock captured requests")
                .clone()
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                handle.join().expect("join mock server");
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut buffer = vec![0u8; 16 * 1024];
        let mut request = Vec::new();
        let mut content_length = None;
        loop {
            let read = stream.read(&mut buffer).expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if content_length.is_none() {
                if let Some(headers_end) = find_headers_end(&request) {
                    let headers = String::from_utf8_lossy(&request[..headers_end]).to_string();
                    content_length = headers
                        .lines()
                        .find_map(|line| {
                            let lower = line.to_ascii_lowercase();
                            lower
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .or(Some(0));
                }
            }
            if let Some(headers_end) = find_headers_end(&request) {
                let body_len = request.len() - headers_end;
                if body_len >= content_length.unwrap_or(0) {
                    break;
                }
            }
        }
        String::from_utf8(request).expect("utf8 request")
    }

    fn find_headers_end(buffer: &[u8]) -> Option<usize> {
        buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
    }

    fn write_response(stream: &mut TcpStream, response: &MockResponse) {
        let payload = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.status,
            response.content_type,
            response.body.len(),
            response.body
        );
        stream
            .write_all(payload.as_bytes())
            .expect("write response");
    }
}
