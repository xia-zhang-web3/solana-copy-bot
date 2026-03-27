use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use url::Url;

#[allow(dead_code)]
#[path = "copybot_activation_checklist_report.rs"]
pub(crate) mod activation_checklist_report;

const USAGE: &str = "usage: copybot_activation_decision_packet --config <prod-path> --non-prod-config <path> [--json] [--output <path>] [--note <text>] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>] [--non-prod-limit <count>] [--non-prod-dress-recent-horizon-seconds <seconds>] [--non-prod-activation-recent-horizon-seconds <seconds>] [--non-prod-min-recent-green-dress <count>] [--non-prod-min-recent-green-activation <count>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for activation decision packet")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) config_path: PathBuf,
    pub(crate) non_prod_config_path: PathBuf,
    pub(crate) json: bool,
    pub(crate) output_path: Option<PathBuf>,
    pub(crate) note: Option<String>,
    pub(crate) now: DateTime<Utc>,
    pub(crate) stage3_limit: usize,
    pub(crate) stage3_recent_horizon_seconds: Option<u64>,
    pub(crate) rehearsal_limit: usize,
    pub(crate) rehearsal_recent_horizon_seconds: u64,
    pub(crate) min_recent_acceptable_rehearsals: usize,
    pub(crate) non_prod_limit: usize,
    pub(crate) non_prod_dress_recent_horizon_seconds: u64,
    pub(crate) non_prod_activation_recent_horizon_seconds: u64,
    pub(crate) non_prod_min_recent_green_dress: usize,
    pub(crate) non_prod_min_recent_green_activation: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActivationDecisionPacketVerdict {
    DecisionPacketBlocked,
    DecisionPacketDiscussionReadyButNotAuthorized,
    DecisionPacketRefusedForProfileMismatch,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConfigFingerprintSummary {
    pub(crate) scope: String,
    pub(crate) sha256: String,
    pub(crate) secrets_excluded: bool,
    pub(crate) sensitive_urls_redacted_before_hashing: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActivationDecisionPacket {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) packet_version: String,
    pub(crate) build_version: String,
    pub(crate) git_commit: Option<String>,
    pub(crate) prod_config_path: String,
    pub(crate) non_prod_config_path: String,
    pub(crate) operator_note: Option<String>,
    pub(crate) execution_enabled: bool,
    pub(crate) read_only_packet: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) discussion_ready_only: bool,
    pub(crate) prod_stage3_remains_hard_gate: bool,
    pub(crate) non_prod_evidence_is_secondary: bool,
    pub(crate) verdict: ActivationDecisionPacketVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) checklist_verdict: String,
    pub(crate) checklist_reason: String,
    pub(crate) prod_config_fingerprint: ConfigFingerprintSummary,
    pub(crate) non_prod_config_fingerprint: ConfigFingerprintSummary,
    pub(crate) prod_pre_activation_gate: activation_checklist_report::ProdPreActivationGateSummary,
    pub(crate) launch_dossier: activation_checklist_report::LaunchDossierSummary,
    pub(crate) tiny_live_guardrails: activation_checklist_report::GuardrailSummary,
    pub(crate) non_prod_readiness: activation_checklist_report::NonProdReadinessSummary,
}

#[derive(Debug, Clone, Serialize)]
struct FingerprintProjection {
    system_env: String,
    execution_enabled: bool,
    execution_mode: String,
    execution_default_route: String,
    execution_submit_allowed_routes: Vec<String>,
    execution_submit_route_order: Vec<String>,
    execution_submit_adapter_require_policy_echo: bool,
    execution_batch_size: u32,
    execution_pretrade_max_fee_overhead_bps: u32,
    execution_pretrade_max_priority_fee_lamports: u64,
    execution_rpc_http_url_identity: String,
    execution_rpc_fallback_http_url_identity: String,
    execution_rpc_devnet_http_url_identity: String,
    execution_submit_adapter_http_url_identity: String,
    execution_submit_adapter_fallback_http_url_identity: String,
    execution_submit_adapter_devnet_http_url_identity: String,
    execution_submit_adapter_devnet_fallback_http_url_identity: String,
    execution_contract_version: String,
    shadow_copy_notional_sol: f64,
    risk_max_position_sol: f64,
    risk_max_concurrent_positions: u32,
    risk_daily_loss_limit_pct: f64,
    tiny_live_policy_enabled: bool,
    tiny_live_policy_allowed_routes: Vec<String>,
    tiny_live_policy_max_trade_notional_sol: f64,
    tiny_live_policy_max_batch_size: u32,
    tiny_live_policy_max_concurrent_positions: u32,
    tiny_live_policy_max_daily_loss_limit_pct: f64,
    tiny_live_policy_require_policy_echo: bool,
    tiny_live_policy_max_pretrade_fee_overhead_bps: u32,
    tiny_live_policy_max_pretrade_priority_fee_lamports: u64,
    tiny_live_policy_max_route_slippage_bps: BTreeMap<String, f64>,
    tiny_live_policy_max_route_tip_lamports: BTreeMap<String, u64>,
    tiny_live_policy_max_route_compute_unit_price_micro_lamports: BTreeMap<String, u64>,
    tiny_live_guardrails_enabled: bool,
    tiny_live_guardrails_evaluation_window_seconds: u64,
    tiny_live_guardrails_max_execution_error_rate_pct: f64,
    tiny_live_guardrails_max_adapter_contract_failure_rate_pct: f64,
    tiny_live_guardrails_max_policy_echo_mismatch_rate_pct: f64,
    tiny_live_guardrails_max_fee_or_slippage_breach_rate_pct: f64,
    tiny_live_guardrails_max_connectivity_degraded_window_seconds: u64,
    tiny_live_guardrails_max_daily_realized_loss_sol: f64,
    tiny_live_guardrails_max_consecutive_hard_failures: u32,
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
    let mut non_prod_config_path: Option<PathBuf> = None;
    let mut json = false;
    let mut output_path: Option<PathBuf> = None;
    let mut note: Option<String> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit = activation_checklist_report::DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds =
        activation_checklist_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals =
        activation_checklist_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
    let mut non_prod_limit = activation_checklist_report::DEFAULT_NON_PROD_HISTORY_LIMIT;
    let mut non_prod_dress_recent_horizon_seconds =
        activation_checklist_report::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS;
    let mut non_prod_activation_recent_horizon_seconds =
        activation_checklist_report::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS;
    let mut non_prod_min_recent_green_dress =
        activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS;
    let mut non_prod_min_recent_green_activation =
        activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--non-prod-config" => {
                non_prod_config_path = Some(PathBuf::from(parse_string_arg(
                    "--non-prod-config",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--note" => note = Some(parse_string_arg("--note", args.next())?),
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--stage3-limit" => stage3_limit = parse_usize_arg("--stage3-limit", args.next())?,
            "--stage3-recent-horizon-seconds" => {
                stage3_recent_horizon_seconds = Some(parse_u64_arg(
                    "--stage3-recent-horizon-seconds",
                    args.next(),
                )?)
            }
            "--rehearsal-limit" => {
                rehearsal_limit = parse_usize_arg("--rehearsal-limit", args.next())?
            }
            "--rehearsal-recent-horizon-seconds" => {
                rehearsal_recent_horizon_seconds =
                    parse_u64_arg("--rehearsal-recent-horizon-seconds", args.next())?
            }
            "--min-recent-acceptable-rehearsals" => {
                min_recent_acceptable_rehearsals =
                    parse_usize_arg("--min-recent-acceptable-rehearsals", args.next())?
            }
            "--non-prod-limit" => {
                non_prod_limit = parse_usize_arg("--non-prod-limit", args.next())?
            }
            "--non-prod-dress-recent-horizon-seconds" => {
                non_prod_dress_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-dress-recent-horizon-seconds", args.next())?
            }
            "--non-prod-activation-recent-horizon-seconds" => {
                non_prod_activation_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-activation-recent-horizon-seconds", args.next())?
            }
            "--non-prod-min-recent-green-dress" => {
                non_prod_min_recent_green_dress =
                    parse_usize_arg("--non-prod-min-recent-green-dress", args.next())?
            }
            "--non-prod-min-recent-green-activation" => {
                non_prod_min_recent_green_activation =
                    parse_usize_arg("--non-prod-min-recent-green-activation", args.next())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        non_prod_config_path: non_prod_config_path
            .ok_or_else(|| anyhow!("missing required --non-prod-config"))?,
        json,
        output_path,
        note,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
        non_prod_limit: non_prod_limit.max(1),
        non_prod_dress_recent_horizon_seconds: non_prod_dress_recent_horizon_seconds.max(1),
        non_prod_activation_recent_horizon_seconds: non_prod_activation_recent_horizon_seconds
            .max(1),
        non_prod_min_recent_green_dress: non_prod_min_recent_green_dress.max(1),
        non_prod_min_recent_green_activation: non_prod_min_recent_green_activation.max(1),
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

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

async fn run(config: Config) -> Result<String> {
    let packet = evaluate_activation_decision_packet(&config).await?;
    let json_output = serde_json::to_string_pretty(&packet)
        .context("failed serializing activation decision packet json")?;
    if let Some(output_path) = &config.output_path {
        write_output(output_path, &json_output)?;
    }
    if config.json {
        Ok(json_output)
    } else {
        Ok(render_human(&packet))
    }
}

pub(crate) async fn evaluate_activation_decision_packet(
    config: &Config,
) -> Result<ActivationDecisionPacket> {
    let checklist_report = activation_checklist_report::evaluate_activation_checklist_report(
        &activation_checklist_report::Config {
            config_path: config.config_path.clone(),
            non_prod_config_path: config.non_prod_config_path.clone(),
            json: false,
            now: config.now,
            stage3_limit: config.stage3_limit,
            stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
            rehearsal_limit: config.rehearsal_limit,
            rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
            min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
            non_prod_limit: config.non_prod_limit,
            non_prod_dress_recent_horizon_seconds: config.non_prod_dress_recent_horizon_seconds,
            non_prod_activation_recent_horizon_seconds: config
                .non_prod_activation_recent_horizon_seconds,
            non_prod_min_recent_green_dress: config.non_prod_min_recent_green_dress,
            non_prod_min_recent_green_activation: config.non_prod_min_recent_green_activation,
        },
    )
    .await?;

    let prod_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let non_prod_config = load_from_path(&config.non_prod_config_path).with_context(|| {
        format!(
            "failed loading non-prod config {}",
            config.non_prod_config_path.display()
        )
    })?;

    Ok(build_decision_packet(
        config,
        checklist_report,
        build_config_fingerprint("prod_execution_policy_guardrails", &prod_config)?,
        build_config_fingerprint("non_prod_execution_policy_guardrails", &non_prod_config)?,
        resolve_git_commit(),
    ))
}

fn build_decision_packet(
    config: &Config,
    checklist: activation_checklist_report::ActivationChecklistReport,
    prod_config_fingerprint: ConfigFingerprintSummary,
    non_prod_config_fingerprint: ConfigFingerprintSummary,
    git_commit: Option<String>,
) -> ActivationDecisionPacket {
    let verdict = match checklist.verdict {
        activation_checklist_report::ActivationChecklistVerdict::ActivationChecklistDiscussionReadyButNotAuthorized => {
            ActivationDecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized
        }
        activation_checklist_report::ActivationChecklistVerdict::ActivationChecklistRefusedForProdProfileMismatch => {
            ActivationDecisionPacketVerdict::DecisionPacketRefusedForProfileMismatch
        }
        _ => ActivationDecisionPacketVerdict::DecisionPacketBlocked,
    };

    ActivationDecisionPacket {
        generated_at: config.now,
        packet_version: "1".to_string(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit,
        prod_config_path: checklist.prod_config_path.clone(),
        non_prod_config_path: checklist.non_prod_config_path.clone(),
        operator_note: config.note.clone(),
        execution_enabled: checklist.execution_enabled,
        read_only_packet: true,
        activation_authorized: false,
        discussion_ready_only: verdict
            == ActivationDecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
        prod_stage3_remains_hard_gate: checklist.prod_stage3_remains_hard_gate,
        non_prod_evidence_is_secondary: checklist.non_prod_evidence_is_secondary,
        verdict,
        reason: checklist.reason.clone(),
        blockers: checklist.blockers.clone(),
        warnings: checklist.warnings.clone(),
        checklist_verdict: serialize_enum(&checklist.verdict),
        checklist_reason: checklist.reason,
        prod_config_fingerprint,
        non_prod_config_fingerprint,
        prod_pre_activation_gate: checklist.prod_pre_activation_gate,
        launch_dossier: checklist.launch_dossier,
        tiny_live_guardrails: checklist.tiny_live_guardrails,
        non_prod_readiness: checklist.non_prod_readiness,
    }
}

pub(crate) fn build_config_fingerprint(
    scope: &str,
    config: &AppConfig,
) -> Result<ConfigFingerprintSummary> {
    let projection = FingerprintProjection {
        system_env: config.system.env.clone(),
        execution_enabled: config.execution.enabled,
        execution_mode: config.execution.mode.clone(),
        execution_default_route: config.execution.default_route.clone(),
        execution_submit_allowed_routes: config.execution.submit_allowed_routes.clone(),
        execution_submit_route_order: config.execution.submit_route_order.clone(),
        execution_submit_adapter_require_policy_echo: config
            .execution
            .submit_adapter_require_policy_echo,
        execution_batch_size: config.execution.batch_size,
        execution_pretrade_max_fee_overhead_bps: config.execution.pretrade_max_fee_overhead_bps,
        execution_pretrade_max_priority_fee_lamports: config
            .execution
            .pretrade_max_priority_fee_lamports,
        execution_rpc_http_url_identity: sanitize_url_identity(&config.execution.rpc_http_url),
        execution_rpc_fallback_http_url_identity: sanitize_url_identity(
            &config.execution.rpc_fallback_http_url,
        ),
        execution_rpc_devnet_http_url_identity: sanitize_url_identity(
            &config.execution.rpc_devnet_http_url,
        ),
        execution_submit_adapter_http_url_identity: sanitize_url_identity(
            &config.execution.submit_adapter_http_url,
        ),
        execution_submit_adapter_fallback_http_url_identity: sanitize_url_identity(
            &config.execution.submit_adapter_fallback_http_url,
        ),
        execution_submit_adapter_devnet_http_url_identity: sanitize_url_identity(
            &config.execution.submit_adapter_devnet_http_url,
        ),
        execution_submit_adapter_devnet_fallback_http_url_identity: sanitize_url_identity(
            &config.execution.submit_adapter_devnet_fallback_http_url,
        ),
        execution_contract_version: config.execution.submit_adapter_contract_version.clone(),
        shadow_copy_notional_sol: config.shadow.copy_notional_sol,
        risk_max_position_sol: config.risk.max_position_sol,
        risk_max_concurrent_positions: config.risk.max_concurrent_positions,
        risk_daily_loss_limit_pct: config.risk.daily_loss_limit_pct,
        tiny_live_policy_enabled: config.tiny_live_policy.enabled,
        tiny_live_policy_allowed_routes: config.tiny_live_policy.allowed_routes.clone(),
        tiny_live_policy_max_trade_notional_sol: config.tiny_live_policy.max_trade_notional_sol,
        tiny_live_policy_max_batch_size: config.tiny_live_policy.max_batch_size,
        tiny_live_policy_max_concurrent_positions: config.tiny_live_policy.max_concurrent_positions,
        tiny_live_policy_max_daily_loss_limit_pct: config.tiny_live_policy.max_daily_loss_limit_pct,
        tiny_live_policy_require_policy_echo: config.tiny_live_policy.require_policy_echo,
        tiny_live_policy_max_pretrade_fee_overhead_bps: config
            .tiny_live_policy
            .max_pretrade_fee_overhead_bps,
        tiny_live_policy_max_pretrade_priority_fee_lamports: config
            .tiny_live_policy
            .max_pretrade_priority_fee_lamports,
        tiny_live_policy_max_route_slippage_bps: config
            .tiny_live_policy
            .max_route_slippage_bps
            .clone(),
        tiny_live_policy_max_route_tip_lamports: config
            .tiny_live_policy
            .max_route_tip_lamports
            .clone(),
        tiny_live_policy_max_route_compute_unit_price_micro_lamports: config
            .tiny_live_policy
            .max_route_compute_unit_price_micro_lamports
            .clone(),
        tiny_live_guardrails_enabled: config.tiny_live_guardrails.enabled,
        tiny_live_guardrails_evaluation_window_seconds: config
            .tiny_live_guardrails
            .evaluation_window_seconds,
        tiny_live_guardrails_max_execution_error_rate_pct: config
            .tiny_live_guardrails
            .max_execution_error_rate_pct,
        tiny_live_guardrails_max_adapter_contract_failure_rate_pct: config
            .tiny_live_guardrails
            .max_adapter_contract_failure_rate_pct,
        tiny_live_guardrails_max_policy_echo_mismatch_rate_pct: config
            .tiny_live_guardrails
            .max_policy_echo_mismatch_rate_pct,
        tiny_live_guardrails_max_fee_or_slippage_breach_rate_pct: config
            .tiny_live_guardrails
            .max_fee_or_slippage_breach_rate_pct,
        tiny_live_guardrails_max_connectivity_degraded_window_seconds: config
            .tiny_live_guardrails
            .max_connectivity_degraded_window_seconds,
        tiny_live_guardrails_max_daily_realized_loss_sol: config
            .tiny_live_guardrails
            .max_daily_realized_loss_sol,
        tiny_live_guardrails_max_consecutive_hard_failures: config
            .tiny_live_guardrails
            .max_consecutive_hard_failures,
    };
    let serialized = serde_json::to_vec(&projection)
        .context("failed serializing config fingerprint projection")?;
    let digest = Sha256::digest(serialized);
    Ok(ConfigFingerprintSummary {
        scope: scope.to_string(),
        sha256: format!("{digest:x}"),
        secrets_excluded: true,
        sensitive_urls_redacted_before_hashing: true,
    })
}

fn sanitize_url_identity(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(url) = Url::parse(trimmed) {
        let mut path = url.path().to_string();
        if path.is_empty() {
            path = "/".to_string();
        }
        let port_suffix = url
            .port()
            .map(|port| format!(":{port}"))
            .unwrap_or_default();
        return format!(
            "{}://{}{}{}",
            url.scheme(),
            url.host_str().unwrap_or("unknown"),
            port_suffix,
            path
        );
    }
    trimmed
        .split_once('?')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

fn resolve_git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn write_output(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed creating parent dir for activation decision packet {}",
                    path.display()
                )
            })?;
        }
    }
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing activation decision packet {}",
            path.display()
        )
    })
}

pub(crate) fn render_human(packet: &ActivationDecisionPacket) -> String {
    [
        "event=copybot_activation_decision_packet".to_string(),
        format!("generated_at={}", packet.generated_at.to_rfc3339()),
        format!("packet_version={}", packet.packet_version),
        format!("build_version={}", packet.build_version),
        format!(
            "git_commit={}",
            packet
                .git_commit
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("prod_config_path={}", packet.prod_config_path),
        format!("non_prod_config_path={}", packet.non_prod_config_path),
        format!("execution_enabled={}", packet.execution_enabled),
        format!("read_only_packet={}", packet.read_only_packet),
        format!("activation_authorized={}", packet.activation_authorized),
        format!("discussion_ready_only={}", packet.discussion_ready_only),
        format!(
            "prod_stage3_remains_hard_gate={}",
            packet.prod_stage3_remains_hard_gate
        ),
        format!(
            "non_prod_evidence_is_secondary={}",
            packet.non_prod_evidence_is_secondary
        ),
        format!("verdict={}", serialize_enum(&packet.verdict)),
        format!("reason={}", packet.reason),
        format!("checklist_verdict={}", packet.checklist_verdict),
        format!("blockers={}", packet.blockers.join(" | ")),
        format!("warnings={}", packet.warnings.join(" | ")),
        format!(
            "prod_config_fingerprint_sha256={}",
            packet.prod_config_fingerprint.sha256
        ),
        format!(
            "non_prod_config_fingerprint_sha256={}",
            packet.non_prod_config_fingerprint.sha256
        ),
        format!(
            "prod_pre_activation_gate_verdict={}",
            packet.prod_pre_activation_gate.verdict
        ),
        format!("launch_dossier_verdict={}", packet.launch_dossier.verdict),
        format!(
            "tiny_live_guardrail_verdict={}",
            packet.tiny_live_guardrails.verdict
        ),
        format!(
            "non_prod_readiness_verdict={}",
            packet.non_prod_readiness.verdict
        ),
        format!(
            "operator_note={}",
            packet
                .operator_note
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
    ]
    .join("\n")
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_checklist_yields_blocked_packet() {
        let packet = build_decision_packet(
            &test_config(),
            blocked_checklist(),
            fingerprint("prod"),
            fingerprint("non-prod"),
            Some("deadbeef".to_string()),
        );

        assert_eq!(
            packet.verdict,
            ActivationDecisionPacketVerdict::DecisionPacketBlocked
        );
    }

    #[test]
    fn discussion_ready_checklist_yields_discussion_ready_packet() {
        let packet = build_decision_packet(
            &test_config(),
            discussion_ready_checklist(),
            fingerprint("prod"),
            fingerprint("non-prod"),
            Some("deadbeef".to_string()),
        );

        assert_eq!(
            packet.verdict,
            ActivationDecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized
        );
        assert!(packet.discussion_ready_only);
        assert!(!packet.activation_authorized);
    }

    #[test]
    fn profile_mismatch_is_preserved() {
        let mut checklist = blocked_checklist();
        checklist.verdict = activation_checklist_report::ActivationChecklistVerdict::ActivationChecklistRefusedForProdProfileMismatch;
        checklist.reason = "prod profile mismatch".to_string();
        let packet = build_decision_packet(
            &test_config(),
            checklist,
            fingerprint("prod"),
            fingerprint("non-prod"),
            None,
        );

        assert_eq!(
            packet.verdict,
            ActivationDecisionPacketVerdict::DecisionPacketRefusedForProfileMismatch
        );
    }

    #[test]
    fn artifact_export_writes_expected_structure() {
        let packet = build_decision_packet(
            &test_config(),
            discussion_ready_checklist(),
            fingerprint("prod"),
            fingerprint("non-prod"),
            Some("deadbeef".to_string()),
        );
        let temp = temp_dir("decision_packet_export");
        let output_path = temp.join("packet.json");
        let json = serde_json::to_string_pretty(&packet).expect("serialize");
        write_output(&output_path, &json).expect("write output");

        let written = fs::read_to_string(&output_path).expect("read packet");
        let parsed: serde_json::Value = serde_json::from_str(&written).expect("parse json");
        assert_eq!(
            parsed["verdict"],
            "decision_packet_discussion_ready_but_not_authorized"
        );
        assert_eq!(
            parsed["checklist_verdict"],
            "activation_checklist_discussion_ready_but_not_authorized"
        );
        assert_eq!(parsed["activation_authorized"], false);
    }

    #[test]
    fn config_fingerprint_excludes_secret_material() {
        let mut config = AppConfig::default();
        config.system.env = "prod-live".to_string();
        config.execution.rpc_http_url =
            "https://rpc.example.com/?token=super-secret-rpc".to_string();
        config.execution.submit_adapter_http_url =
            "https://adapter.example.com/submit?api_key=super-secret-adapter".to_string();
        config.execution.submit_adapter_auth_token = "super-secret-token".to_string();
        config.execution.submit_adapter_hmac_secret = "super-secret-hmac".to_string();
        config.execution.submit_dynamic_cu_price_api_auth_token = "super-secret-cu-api".to_string();

        let fingerprint_a =
            build_config_fingerprint("prod_execution_policy_guardrails", &config).expect("fp");
        config.execution.submit_adapter_auth_token = "different-secret-token".to_string();
        config.execution.submit_adapter_hmac_secret = "different-secret-hmac".to_string();
        config.execution.submit_dynamic_cu_price_api_auth_token =
            "different-secret-cu-api".to_string();
        let fingerprint_b =
            build_config_fingerprint("prod_execution_policy_guardrails", &config).expect("fp");

        assert_eq!(fingerprint_a.sha256, fingerprint_b.sha256);
        let serialized = serde_json::to_string(&fingerprint_a).expect("json");
        assert!(!serialized.contains("super-secret"));
        assert!(!serialized.contains("adapter.example.com/submit?api_key"));
    }

    #[test]
    fn execution_remains_disabled_and_no_live_mutation_occurs() {
        let source_path = temp_dir("decision_packet_no_mutation").join("live.server.toml");
        fs::write(&source_path, "execution_enabled=false\n").expect("write source");
        let before = fs::read_to_string(&source_path).expect("read before");

        let packet = build_decision_packet(
            &test_config(),
            discussion_ready_checklist(),
            fingerprint("prod"),
            fingerprint("non-prod"),
            Some("deadbeef".to_string()),
        );

        let after = fs::read_to_string(&source_path).expect("read after");
        assert_eq!(before, after);
        assert!(!packet.execution_enabled);
        assert!(!packet.activation_authorized);
    }

    fn test_config() -> Config {
        Config {
            config_path: PathBuf::from("/etc/solana-copy-bot/live.server.toml"),
            non_prod_config_path: PathBuf::from("/etc/solana-copy-bot/devnet.server.toml"),
            json: false,
            output_path: None,
            note: Some("manual activation discussion packet".to_string()),
            now: ts("2026-03-26T12:00:00Z"),
            stage3_limit: copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit: activation_checklist_report::DEFAULT_REHEARSAL_HISTORY_LIMIT,
            rehearsal_recent_horizon_seconds:
                activation_checklist_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS,
            min_recent_acceptable_rehearsals:
                activation_checklist_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS,
            non_prod_limit: activation_checklist_report::DEFAULT_NON_PROD_HISTORY_LIMIT,
            non_prod_dress_recent_horizon_seconds:
                activation_checklist_report::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS,
            non_prod_activation_recent_horizon_seconds:
                activation_checklist_report::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS,
            non_prod_min_recent_green_dress:
                activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS,
            non_prod_min_recent_green_activation:
                activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION,
        }
    }

    fn blocked_checklist() -> activation_checklist_report::ActivationChecklistReport {
        activation_checklist_report::ActivationChecklistReport {
            generated_at: ts("2026-03-26T12:00:00Z"),
            prod_config_path: "/etc/solana-copy-bot/live.server.toml".to_string(),
            non_prod_config_path: "/etc/solana-copy-bot/devnet.server.toml".to_string(),
            prod_config_env: "prod-live".to_string(),
            non_prod_config_env: "paper-devnet".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            activation_authorized: false,
            discussion_ready_only: false,
            prod_stage3_remains_hard_gate: true,
            non_prod_evidence_is_secondary: true,
            verdict: activation_checklist_report::ActivationChecklistVerdict::ActivationChecklistBlockedByProdStage3,
            reason: "stage3 blocked".to_string(),
            blockers: vec!["stage3 blocked".to_string()],
            warnings: vec!["not authorized".to_string()],
            prod_pre_activation_gate: activation_checklist_report::ProdPreActivationGateSummary {
                verdict: "blocked_by_stage3".to_string(),
                reason: "stage3 blocked".to_string(),
                planning_green: false,
                blocked_by_stage3: true,
                stage3_verdict: "publication_drifting".to_string(),
                stage3_reason: "stage3 blocked".to_string(),
                stage3_captures_within_recent_horizon: 1,
                stage3_latest_capture_age_seconds: Some(60),
                stage4_readiness_verdict: "ready_for_execution_dry_run".to_string(),
                stage4_rehearsal_history_verdict: "sufficient_recent_rehearsal_evidence".to_string(),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                blockers: vec!["stage3 blocked".to_string()],
            },
            launch_dossier: activation_checklist_report::LaunchDossierSummary {
                verdict: "blocked_by_pre_activation_gate".to_string(),
                reason: "stage3 blocked".to_string(),
                ready_when_stage_gate_allows: false,
                activation_overlay_complete: true,
                rollback_plan_complete: true,
                service_restart_contract_complete: true,
                activation_overlay_change_count: 4,
                rollback_overlay_change_count: 4,
                drift_finding_count: 0,
                blocker_count: 1,
                first_blocker: Some("stage3 blocked".to_string()),
            },
            tiny_live_guardrails: activation_checklist_report::GuardrailSummary {
                verdict: "tiny_live_guardrails_bounded".to_string(),
                reason: "bounded".to_string(),
                bounded: true,
                enabled: true,
                blocker_count: 0,
                first_blocker: None,
                rollback_trigger_count: 0,
                rollback_triggers: Vec::new(),
            },
            non_prod_readiness: activation_checklist_report::NonProdReadinessSummary {
                verdict: "devnet_readiness_green".to_string(),
                reason: "green".to_string(),
                green: true,
                prod_profile_refused: false,
                config_env: "paper-devnet".to_string(),
                blockers: Vec::new(),
                warnings: vec!["non-prod evidence only".to_string()],
                dress_latest_record_age_seconds: Some(120),
                dress_recent_green_count: 2,
                activation_latest_record_age_seconds: Some(180),
                activation_recent_green_count: 2,
                activation_recent_rollback_success_count: 2,
                activation_recent_internal_consistency_count: 2,
                stale_evidence_excluded: false,
            },
        }
    }

    fn discussion_ready_checklist() -> activation_checklist_report::ActivationChecklistReport {
        let mut checklist = blocked_checklist();
        checklist.verdict = activation_checklist_report::ActivationChecklistVerdict::ActivationChecklistDiscussionReadyButNotAuthorized;
        checklist.reason = "discussion ready".to_string();
        checklist.blockers = Vec::new();
        checklist.discussion_ready_only = true;
        checklist.prod_pre_activation_gate.verdict = "pre_activation_gates_green".to_string();
        checklist.prod_pre_activation_gate.reason = "green".to_string();
        checklist.prod_pre_activation_gate.planning_green = true;
        checklist.prod_pre_activation_gate.blocked_by_stage3 = false;
        checklist.launch_dossier.verdict =
            "activation_plan_ready_when_stage_gate_allows".to_string();
        checklist.launch_dossier.reason = "ready".to_string();
        checklist.launch_dossier.ready_when_stage_gate_allows = true;
        checklist
    }

    fn fingerprint(scope: &str) -> ConfigFingerprintSummary {
        ConfigFingerprintSummary {
            scope: scope.to_string(),
            sha256: "abc123".to_string(),
            secrets_excluded: true,
            sensitive_urls_redacted_before_hashing: true,
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = format!(
            "{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let dir = env::temp_dir().join(format!(
            "copybot_activation_decision_packet_{}_{}",
            label, unique
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }
}
