use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    wallet_freshness_audit::{
        WalletFreshnessHistoryReport, WalletFreshnessHistoryVerdict, DEFAULT_HISTORY_CAPTURE_LIMIT,
    },
    DiscoveryService,
};
use copybot_storage::{ExecutionDryRunRehearsalRow, ExecutionDryRunRehearsalWrite, SqliteStore};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_execution_readiness_audit.rs"]
mod execution_readiness_audit;
#[allow(dead_code)]
#[path = "copybot_tiny_live_policy_audit.rs"]
mod tiny_live_policy_audit;

const USAGE: &str = "usage: copybot_pre_activation_gate_report --config <path> [--json] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>]";
const DEFAULT_REHEARSAL_HISTORY_LIMIT: usize = 10;
const DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS: u64 = 86_400;
const DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS: usize = 2;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for pre-activation gate report")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    json: bool,
    now: DateTime<Utc>,
    stage3_limit: usize,
    stage3_recent_horizon_seconds: Option<u64>,
    rehearsal_limit: usize,
    rehearsal_recent_horizon_seconds: u64,
    min_recent_acceptable_rehearsals: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PreActivationGateVerdict {
    PreActivationGatesGreen,
    BlockedByStage3,
    BlockedByStage4Readiness,
    BlockedByDryRunHistory,
    BlockedByTinyLivePolicy,
    InsufficientRecentEvidence,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DryRunHistoryVerdict {
    SufficientRecentRehearsalEvidence,
    NoPersistedRehearsals,
    StaleRehearsalsOnly,
    InsufficientRecentSuccessfulRehearsals,
    RecentHardBlockersPresent,
}

#[derive(Debug, Clone, Serialize)]
struct Stage3GateSummary {
    verdict: String,
    reason: String,
    stage3_green: bool,
    captures_loaded: usize,
    captures_within_recent_horizon: usize,
    recent_horizon_seconds: u64,
    latest_capture_age_seconds: Option<u64>,
    stale_captures_excluded_from_verdict: bool,
    exact_published_current_match_count: usize,
    exact_active_current_match_count: usize,
    rotation_evidence_capture_count: usize,
    shadow_signal_present_capture_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct Stage4ReadinessSummary {
    verdict: String,
    reason: String,
    config_valid: bool,
    connectivity_valid: bool,
    adapter_contract_valid: bool,
    signer_contract_valid: bool,
    policy_contract_valid: bool,
    route_contract_valid: bool,
    ready_for_dry_run: bool,
    blocked_for_activation: bool,
}

#[derive(Debug, Clone, Serialize)]
struct DryRunHistorySummary {
    verdict: DryRunHistoryVerdict,
    reason: String,
    records_loaded: usize,
    recent_rehearsals_within_horizon: usize,
    recent_horizon_seconds: u64,
    latest_rehearsal_age_seconds: Option<u64>,
    stale_rehearsals_excluded_from_verdict: bool,
    stale_rehearsals_excluded_count: usize,
    acceptable_recent_rehearsal_count: usize,
    recent_hard_blocker_count: usize,
    latest_recent_verdict: Option<String>,
    verdict_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
struct TinyLivePolicySummary {
    verdict: String,
    reason: String,
    tiny_live_policy_bounded: bool,
    tiny_live_policy_enabled: bool,
    blocker_count: usize,
    first_blocker: Option<String>,
    warnings_count: usize,
    mode_compatible: bool,
    execution_policy_contract_valid: bool,
    execution_route_contract_valid: bool,
}

#[derive(Debug, Clone, Serialize)]
struct PreActivationGateReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    db_path: String,
    execution_enabled: bool,
    planning_safe_only: bool,
    activation_permission_granted: bool,
    stage3_is_primary_gate: bool,
    verdict: PreActivationGateVerdict,
    reason: String,
    blockers: Vec<String>,
    stage3: Stage3GateSummary,
    stage4_readiness: Stage4ReadinessSummary,
    stage4_dry_run_history: DryRunHistorySummary,
    tiny_live_policy: TinyLivePolicySummary,
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
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit = DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds = DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals = DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--json" => json = true,
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
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        json,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
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
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(&config.config_path, &loaded_config.sqlite.path);
    let store = SqliteStore::open(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let report =
        build_pre_activation_gate_report(&config, &loaded_config, &store, &db_path).await?;
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing pre-activation gate report json")
    } else {
        Ok(render_human(&report))
    }
}

async fn build_pre_activation_gate_report(
    config: &Config,
    loaded_config: &copybot_config::AppConfig,
    store: &SqliteStore,
    db_path: &Path,
) -> Result<PreActivationGateReport> {
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let stage3_recent_horizon_seconds = config.stage3_recent_horizon_seconds.unwrap_or_else(|| {
        discovery.default_wallet_freshness_history_recent_horizon_seconds(config.stage3_limit)
    });
    let stage3_report = discovery.wallet_freshness_history_report_with_horizon(
        store,
        config.now,
        config.stage3_limit,
        stage3_recent_horizon_seconds,
    )?;
    let readiness_report =
        execution_readiness_audit::evaluate_execution_readiness(&config.config_path, loaded_config)
            .await?;
    let dry_run_history_summary = summarize_dry_run_history(
        config.now,
        store.list_execution_dry_run_rehearsals(config.rehearsal_limit)?,
        config.rehearsal_recent_horizon_seconds,
        config.min_recent_acceptable_rehearsals,
    );
    let tiny_live_policy_report =
        tiny_live_policy_audit::evaluate_tiny_live_policy(&config.config_path, loaded_config)?;
    Ok(compose_gate_report(
        config,
        loaded_config.execution.enabled,
        db_path,
        stage3_report,
        readiness_report,
        dry_run_history_summary,
        tiny_live_policy_report,
    ))
}

fn compose_gate_report(
    config: &Config,
    execution_enabled: bool,
    db_path: &Path,
    stage3_report: WalletFreshnessHistoryReport,
    readiness_report: execution_readiness_audit::ExecutionReadinessAuditReport,
    dry_run_history_summary: DryRunHistorySummary,
    tiny_live_policy_report: tiny_live_policy_audit::TinyLivePolicyAuditReport,
) -> PreActivationGateReport {
    let stage3_summary = Stage3GateSummary {
        verdict: stage3_report.verdict.as_str().to_string(),
        reason: stage3_report.reason.clone(),
        stage3_green: stage3_report.verdict == WalletFreshnessHistoryVerdict::ValidatedCurrent,
        captures_loaded: stage3_report.captures_loaded,
        captures_within_recent_horizon: stage3_report.captures_within_recent_horizon,
        recent_horizon_seconds: stage3_report.recent_horizon_seconds,
        latest_capture_age_seconds: stage3_report.latest_capture_age_seconds,
        stale_captures_excluded_from_verdict: stage3_report.stale_captures_excluded_from_verdict,
        exact_published_current_match_count: stage3_report.exact_published_current_match_count,
        exact_active_current_match_count: stage3_report.exact_active_current_match_count,
        rotation_evidence_capture_count: stage3_report.rotation_evidence_capture_count,
        shadow_signal_present_capture_count: stage3_report.shadow_signal_present_capture_count,
    };
    let stage4_readiness_summary = Stage4ReadinessSummary {
        verdict: serde_json::to_string(&readiness_report.verdict)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        reason: readiness_report.reason.clone(),
        config_valid: readiness_report.config_valid,
        connectivity_valid: readiness_report.connectivity_valid,
        adapter_contract_valid: readiness_report.adapter_contract_valid,
        signer_contract_valid: readiness_report.signer_contract_valid,
        policy_contract_valid: readiness_report.policy_contract_valid,
        route_contract_valid: readiness_report.route_contract_valid,
        ready_for_dry_run: readiness_report.ready_for_dry_run,
        blocked_for_activation: readiness_report.blocked_for_activation,
    };
    let tiny_live_policy_summary = TinyLivePolicySummary {
        verdict: serde_json::to_string(&tiny_live_policy_report.verdict)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        reason: tiny_live_policy_report.reason.clone(),
        tiny_live_policy_bounded: tiny_live_policy_report
            .current_config_bounded_for_later_tiny_live_discussion,
        tiny_live_policy_enabled: tiny_live_policy_report.tiny_live_policy_enabled,
        blocker_count: tiny_live_policy_report.blockers.len(),
        first_blocker: tiny_live_policy_report.blockers.first().cloned(),
        warnings_count: tiny_live_policy_report.warnings.len(),
        mode_compatible: tiny_live_policy_report.mode_compatible,
        execution_policy_contract_valid: tiny_live_policy_report.execution_policy_contract_valid,
        execution_route_contract_valid: tiny_live_policy_report.execution_route_contract_valid,
    };

    let (verdict, reason, mut blockers) = derive_top_level_gate(
        &stage3_summary,
        &stage4_readiness_summary,
        &dry_run_history_summary,
        &tiny_live_policy_summary,
    );
    if !execution_enabled {
        blockers.push(
            "execution.enabled=false remains in force; this report is planning-safe only"
                .to_string(),
        );
    }
    blockers.sort();
    blockers.dedup();

    PreActivationGateReport {
        generated_at: config.now,
        config_path: config.config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        execution_enabled,
        planning_safe_only: true,
        activation_permission_granted: false,
        stage3_is_primary_gate: true,
        verdict,
        reason,
        blockers,
        stage3: stage3_summary,
        stage4_readiness: stage4_readiness_summary,
        stage4_dry_run_history: dry_run_history_summary,
        tiny_live_policy: tiny_live_policy_summary,
    }
}

fn derive_top_level_gate(
    stage3: &Stage3GateSummary,
    readiness: &Stage4ReadinessSummary,
    dry_run_history: &DryRunHistorySummary,
    tiny_live_policy: &TinyLivePolicySummary,
) -> (PreActivationGateVerdict, String, Vec<String>) {
    if stage3.captures_within_recent_horizon == 0
        || stage3.verdict == WalletFreshnessHistoryVerdict::InsufficientEvidence.as_str()
        || stage3.verdict == WalletFreshnessHistoryVerdict::RawTruthInsufficient.as_str()
    {
        return (
            PreActivationGateVerdict::InsufficientRecentEvidence,
            format!(
                "Stage 3 recent-cycle evidence is not sufficient yet: {}",
                stage3.reason
            ),
            vec![format!("stage3: {}", stage3.reason)],
        );
    }
    if !stage3.stage3_green {
        return (
            PreActivationGateVerdict::BlockedByStage3,
            format!(
                "Stage 3 remains the primary gate and is not green: {}",
                stage3.reason
            ),
            vec![format!("stage3: {}", stage3.reason)],
        );
    }
    if readiness.verdict
        != serde_json::to_string(
            &execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
        )
        .unwrap_or_default()
        .trim_matches('"')
    {
        return (
            PreActivationGateVerdict::BlockedByStage4Readiness,
            format!(
                "Stage 4 readiness/preflight is still blocked: {}",
                readiness.reason
            ),
            vec![format!("stage4_readiness: {}", readiness.reason)],
        );
    }

    match dry_run_history.verdict {
        DryRunHistoryVerdict::SufficientRecentRehearsalEvidence => {}
        DryRunHistoryVerdict::NoPersistedRehearsals
        | DryRunHistoryVerdict::StaleRehearsalsOnly
        | DryRunHistoryVerdict::InsufficientRecentSuccessfulRehearsals => {
            return (
                PreActivationGateVerdict::InsufficientRecentEvidence,
                format!(
                    "Stage 4 dry-run rehearsal history is not recent/sufficient enough yet: {}",
                    dry_run_history.reason
                ),
                vec![format!(
                    "stage4_dry_run_history: {}",
                    dry_run_history.reason
                )],
            )
        }
        DryRunHistoryVerdict::RecentHardBlockersPresent => {
            return (
                PreActivationGateVerdict::BlockedByDryRunHistory,
                format!(
                    "Recent dry-run rehearsal history still shows hard blockers: {}",
                    dry_run_history.reason
                ),
                vec![format!(
                    "stage4_dry_run_history: {}",
                    dry_run_history.reason
                )],
            )
        }
    }

    if !tiny_live_policy.tiny_live_policy_bounded {
        return (
            PreActivationGateVerdict::BlockedByTinyLivePolicy,
            format!(
                "Tiny-live policy envelope is not bounded enough yet: {}",
                tiny_live_policy.reason
            ),
            vec![format!("tiny_live_policy: {}", tiny_live_policy.reason)],
        );
    }

    (
        PreActivationGateVerdict::PreActivationGatesGreen,
        "Stage 3 is green, Stage 4 readiness is green, recent dry-run rehearsal history is sufficient, and the tiny-live policy envelope is explicitly bounded for planning-safe tiny-live discussion".to_string(),
        Vec::new(),
    )
}

fn summarize_dry_run_history(
    now: DateTime<Utc>,
    rehearsals: Vec<ExecutionDryRunRehearsalRow>,
    recent_horizon_seconds: u64,
    min_recent_acceptable_rehearsals: usize,
) -> DryRunHistorySummary {
    let latest_rehearsal_age_seconds = rehearsals
        .first()
        .map(|row| age_seconds(now, row.rehearsed_at));
    let mut verdict_counts = BTreeMap::new();
    for rehearsal in &rehearsals {
        *verdict_counts.entry(rehearsal.verdict.clone()).or_insert(0) += 1;
    }

    if rehearsals.is_empty() {
        return DryRunHistorySummary {
            verdict: DryRunHistoryVerdict::NoPersistedRehearsals,
            reason: "no persisted dry-run rehearsals found".to_string(),
            records_loaded: 0,
            recent_rehearsals_within_horizon: 0,
            recent_horizon_seconds,
            latest_rehearsal_age_seconds: None,
            stale_rehearsals_excluded_from_verdict: false,
            stale_rehearsals_excluded_count: 0,
            acceptable_recent_rehearsal_count: 0,
            recent_hard_blocker_count: 0,
            latest_recent_verdict: None,
            verdict_counts,
        };
    }

    let recent = rehearsals
        .iter()
        .filter(|row| age_seconds(now, row.rehearsed_at) <= recent_horizon_seconds)
        .cloned()
        .collect::<Vec<_>>();
    let stale_rehearsals_excluded_count = rehearsals.len().saturating_sub(recent.len());
    if recent.is_empty() {
        return DryRunHistorySummary {
            verdict: DryRunHistoryVerdict::StaleRehearsalsOnly,
            reason: "persisted dry-run rehearsals exist, but none are within the recent horizon"
                .to_string(),
            records_loaded: rehearsals.len(),
            recent_rehearsals_within_horizon: 0,
            recent_horizon_seconds,
            latest_rehearsal_age_seconds,
            stale_rehearsals_excluded_from_verdict: stale_rehearsals_excluded_count > 0,
            stale_rehearsals_excluded_count,
            acceptable_recent_rehearsal_count: 0,
            recent_hard_blocker_count: 0,
            latest_recent_verdict: None,
            verdict_counts,
        };
    }

    let acceptable_recent_rehearsal_count = recent
        .iter()
        .filter(|row| is_acceptable_rehearsal_verdict(row.verdict.as_str()))
        .count();
    let recent_hard_blocker_count = recent
        .iter()
        .filter(|row| !is_acceptable_rehearsal_verdict(row.verdict.as_str()))
        .count();
    let latest_recent_verdict = recent.first().map(|row| row.verdict.clone());

    let (verdict, reason) = if recent_hard_blocker_count > 0
        && recent
            .first()
            .map(|row| !is_acceptable_rehearsal_verdict(row.verdict.as_str()))
            .unwrap_or(false)
    {
        (
            DryRunHistoryVerdict::RecentHardBlockersPresent,
            recent
                .first()
                .map(|row| {
                    format!(
                        "latest recent rehearsal is a hard blocker: {} ({})",
                        row.verdict, row.reason
                    )
                })
                .unwrap_or_else(|| "recent hard blockers present".to_string()),
        )
    } else if acceptable_recent_rehearsal_count >= min_recent_acceptable_rehearsals {
        (
            DryRunHistoryVerdict::SufficientRecentRehearsalEvidence,
            format!(
                "{} recent rehearsal(s) are acceptable within the current horizon",
                acceptable_recent_rehearsal_count
            ),
        )
    } else {
        (
            DryRunHistoryVerdict::InsufficientRecentSuccessfulRehearsals,
            format!(
                "only {} acceptable recent rehearsal(s); need at least {}",
                acceptable_recent_rehearsal_count, min_recent_acceptable_rehearsals
            ),
        )
    };

    DryRunHistorySummary {
        verdict,
        reason,
        records_loaded: rehearsals.len(),
        recent_rehearsals_within_horizon: recent.len(),
        recent_horizon_seconds,
        latest_rehearsal_age_seconds,
        stale_rehearsals_excluded_from_verdict: stale_rehearsals_excluded_count > 0,
        stale_rehearsals_excluded_count,
        acceptable_recent_rehearsal_count,
        recent_hard_blocker_count,
        latest_recent_verdict,
        verdict_counts,
    }
}

fn is_acceptable_rehearsal_verdict(verdict: &str) -> bool {
    matches!(
        verdict,
        "rehearsal_green" | "rehearsal_green_with_business_reject"
    )
}

fn age_seconds(now: DateTime<Utc>, ts: DateTime<Utc>) -> u64 {
    now.signed_duration_since(ts).num_seconds().max(0) as u64
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

fn render_human(report: &PreActivationGateReport) -> String {
    [
        "event=copybot_pre_activation_gate_report".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
        format!("execution_enabled={}", report.execution_enabled),
        format!("planning_safe_only={}", report.planning_safe_only),
        format!(
            "activation_permission_granted={}",
            report.activation_permission_granted
        ),
        format!("stage3_is_primary_gate={}", report.stage3_is_primary_gate),
        format!(
            "verdict={}",
            serde_json::to_string(&report.verdict)
                .unwrap_or_default()
                .trim_matches('"')
        ),
        format!("reason={}", report.reason),
        format!("blockers={}", report.blockers.join(" | ")),
        format!("stage3_verdict={}", report.stage3.verdict),
        format!("stage3_reason={}", report.stage3.reason),
        format!(
            "stage3_latest_capture_age_seconds={}",
            report
                .stage3
                .latest_capture_age_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_captures_within_recent_horizon={}",
            report.stage3.captures_within_recent_horizon
        ),
        format!(
            "stage4_readiness_verdict={}",
            report.stage4_readiness.verdict
        ),
        format!("stage4_readiness_reason={}", report.stage4_readiness.reason),
        format!(
            "stage4_rehearsal_history_verdict={}",
            serde_json::to_string(&report.stage4_dry_run_history.verdict)
                .unwrap_or_default()
                .trim_matches('"')
        ),
        format!(
            "stage4_rehearsal_history_reason={}",
            report.stage4_dry_run_history.reason
        ),
        format!(
            "stage4_latest_rehearsal_age_seconds={}",
            report
                .stage4_dry_run_history
                .latest_rehearsal_age_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage4_recent_rehearsals_within_horizon={}",
            report
                .stage4_dry_run_history
                .recent_rehearsals_within_horizon
        ),
        format!(
            "tiny_live_policy_verdict={}",
            report.tiny_live_policy.verdict
        ),
        format!("tiny_live_policy_reason={}", report.tiny_live_policy.reason),
        format!(
            "tiny_live_policy_bounded={}",
            report.tiny_live_policy.tiny_live_policy_bounded
        ),
        format!(
            "tiny_live_policy_enabled={}",
            report.tiny_live_policy.tiny_live_policy_enabled
        ),
        format!(
            "tiny_live_policy_first_blocker={}",
            report
                .tiny_live_policy
                .first_blocker
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use copybot_discovery::wallet_freshness_audit::WalletFreshnessCaptureSnapshot;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn stage3_blocked_always_blocks_top_level_gate() {
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(
                WalletFreshnessHistoryVerdict::PublicationDrifting,
                "drifting",
                3,
                Some(60),
            ),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
                "ready",
            ),
            sufficient_rehearsal_summary(),
            bounded_policy_report(),
        );

        assert_eq!(report.verdict, PreActivationGateVerdict::BlockedByStage3);
        assert!(report.reason.contains("Stage 3"));
    }

    #[test]
    fn stage4_readiness_blocked_with_stage3_green_blocks_top_level_gate() {
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(WalletFreshnessHistoryVerdict::ValidatedCurrent, "validated", 3, Some(60)),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ConfigValidButConnectivityBlocked,
                "rpc blocked",
            ),
            sufficient_rehearsal_summary(),
            bounded_policy_report(),
        );

        assert_eq!(
            report.verdict,
            PreActivationGateVerdict::BlockedByStage4Readiness
        );
    }

    #[test]
    fn stale_rehearsal_history_cannot_yield_green_top_level_gate() {
        let summary = summarize_dry_run_history(
            ts("2026-03-25T12:00:00Z"),
            vec![rehearsal_row(
                ts("2026-03-24T09:00:00Z"),
                "rehearsal_green",
                "green",
            )],
            3_600,
            1,
        );
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(
                WalletFreshnessHistoryVerdict::ValidatedCurrent,
                "validated",
                3,
                Some(60),
            ),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
                "ready",
            ),
            summary,
            bounded_policy_report(),
        );

        assert_eq!(
            report.verdict,
            PreActivationGateVerdict::InsufficientRecentEvidence
        );
    }

    #[test]
    fn fresh_stage3_plus_readiness_plus_recent_rehearsal_history_yields_green_gate() {
        let summary = summarize_dry_run_history(
            ts("2026-03-25T12:00:00Z"),
            vec![
                rehearsal_row(ts("2026-03-25T11:55:00Z"), "rehearsal_green", "green"),
                rehearsal_row(
                    ts("2026-03-25T11:40:00Z"),
                    "rehearsal_green_with_business_reject",
                    "business reject",
                ),
            ],
            3_600,
            2,
        );
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(
                WalletFreshnessHistoryVerdict::ValidatedCurrent,
                "validated",
                3,
                Some(60),
            ),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
                "ready",
            ),
            summary,
            bounded_policy_report(),
        );

        assert_eq!(
            report.verdict,
            PreActivationGateVerdict::PreActivationGatesGreen
        );
        assert!(!report.execution_enabled);
        assert!(report.planning_safe_only);
        assert!(!report.activation_permission_granted);
    }

    #[test]
    fn execution_disabled_stays_explicit_without_blocking_planning_green() {
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(
                WalletFreshnessHistoryVerdict::ValidatedCurrent,
                "validated",
                3,
                Some(60),
            ),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
                "ready",
            ),
            sufficient_rehearsal_summary(),
            bounded_policy_report(),
        );

        assert_eq!(
            report.verdict,
            PreActivationGateVerdict::PreActivationGatesGreen
        );
        assert!(!report.execution_enabled);
        assert!(report
            .blockers
            .iter()
            .any(|value| value.contains("execution.enabled=false")));
    }

    #[test]
    fn no_discovery_restore_truth_is_needed_for_rehearsal_summary() -> Result<()> {
        let temp = temp_dir("pre-activation-gate");
        let db_path = temp.join("pre-activation-gate.db");
        let store = SqliteStore::open(&db_path)?;
        store.append_execution_dry_run_rehearsal(&rehearsal_write(
            ts("2026-03-25T11:55:00Z"),
            "rehearsal_green",
        ))?;
        store.append_execution_dry_run_rehearsal(&rehearsal_write(
            ts("2026-03-25T11:45:00Z"),
            "rehearsal_green_with_business_reject",
        ))?;

        let summary = summarize_dry_run_history(
            ts("2026-03-25T12:00:00Z"),
            store.list_execution_dry_run_rehearsals(10)?,
            3_600,
            2,
        );
        assert_eq!(
            summary.verdict,
            DryRunHistoryVerdict::SufficientRecentRehearsalEvidence
        );
        Ok(())
    }

    #[test]
    fn policy_non_green_blocks_gate_when_stage3_and_stage4_are_green() {
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(
                WalletFreshnessHistoryVerdict::ValidatedCurrent,
                "validated",
                3,
                Some(60),
            ),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
                "ready",
            ),
            sufficient_rehearsal_summary(),
            policy_report(
                tiny_live_policy_audit::TinyLivePolicyVerdict::TinyLivePolicyTooOpen,
                "shadow.copy_notional_sol exceeds tiny-live cap",
                false,
            ),
        );

        assert_eq!(
            report.verdict,
            PreActivationGateVerdict::BlockedByTinyLivePolicy
        );
        assert!(report.reason.contains("Tiny-live policy envelope"));
        assert_eq!(
            report.tiny_live_policy.verdict,
            "tiny_live_policy_too_open".to_string()
        );
    }

    #[test]
    fn green_gate_now_requires_bounded_tiny_live_policy() {
        let report = compose_gate_report(
            &test_config(),
            false,
            Path::new("/tmp/runtime.db"),
            stage3_report(
                WalletFreshnessHistoryVerdict::ValidatedCurrent,
                "validated",
                3,
                Some(60),
            ),
            readiness_report(
                execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
                "ready",
            ),
            sufficient_rehearsal_summary(),
            bounded_policy_report(),
        );

        assert_eq!(
            report.verdict,
            PreActivationGateVerdict::PreActivationGatesGreen
        );
        assert!(report.tiny_live_policy.tiny_live_policy_bounded);
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_pre_activation_gate_{}_{}_{}",
            label,
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn test_config() -> Config {
        Config {
            config_path: PathBuf::from("/tmp/live.server.toml"),
            json: false,
            now: ts("2026-03-25T12:00:00Z"),
            stage3_limit: 5,
            stage3_recent_horizon_seconds: Some(3_600),
            rehearsal_limit: 10,
            rehearsal_recent_horizon_seconds: 86_400,
            min_recent_acceptable_rehearsals: 2,
        }
    }

    fn stage3_report(
        verdict: WalletFreshnessHistoryVerdict,
        reason: &str,
        captures_within_recent_horizon: usize,
        latest_capture_age_seconds: Option<u64>,
    ) -> WalletFreshnessHistoryReport {
        WalletFreshnessHistoryReport {
            generated_at: ts("2026-03-25T12:00:00Z"),
            captures_requested: 5,
            captures_loaded: captures_within_recent_horizon,
            captures_considered: captures_within_recent_horizon,
            captures_within_recent_horizon,
            recent_horizon_seconds: 3_600,
            latest_capture_age_seconds,
            stale_captures_excluded_from_verdict: false,
            stale_captures_excluded_count: 0,
            verdict,
            reason: reason.to_string(),
            fresh_capture_count: if verdict == WalletFreshnessHistoryVerdict::ValidatedCurrent {
                captures_within_recent_horizon
            } else {
                0
            },
            drifting_capture_count: 0,
            stale_capture_count: 0,
            insufficient_raw_capture_count: 0,
            no_publication_truth_capture_count: 0,
            exact_published_current_match_count: captures_within_recent_horizon,
            exact_active_current_match_count: captures_within_recent_horizon,
            active_follow_change_count: 1,
            current_raw_change_count: 1,
            rotation_evidence_capture_count: captures_within_recent_horizon,
            shadow_signal_present_capture_count: captures_within_recent_horizon,
            broad_shadow_signal_capture_count: captures_within_recent_horizon,
            captures: Vec::<WalletFreshnessCaptureSnapshot>::new(),
        }
    }

    fn readiness_report(
        verdict: execution_readiness_audit::ExecutionReadinessVerdict,
        reason: &str,
    ) -> execution_readiness_audit::ExecutionReadinessAuditReport {
        execution_readiness_audit::ExecutionReadinessAuditReport {
            generated_at: ts("2026-03-25T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            execution_enabled: false,
            mode: "adapter_submit_confirm".to_string(),
            mode_compatible: true,
            config_valid: true,
            connectivity_valid: verdict
                != execution_readiness_audit::ExecutionReadinessVerdict::ConfigValidButConnectivityBlocked,
            adapter_contract_valid: true,
            signer_contract_valid: true,
            policy_contract_valid: true,
            route_contract_valid: true,
            ready_for_dry_run: verdict
                == execution_readiness_audit::ExecutionReadinessVerdict::ReadyForExecutionDryRun,
            blocked_for_activation: true,
            verdict,
            reason: reason.to_string(),
            activation_blockers: vec!["execution.enabled=false".to_string()],
            static_blockers: Vec::new(),
            warnings: vec!["execution.enabled=false".to_string()],
            signer_pubkey_configured: true,
            adapter_auth_token_configured: true,
            adapter_auth_token_source: "inline".to_string(),
            adapter_hmac_configured: false,
            adapter_hmac_source: "none".to_string(),
            route_summary: execution_readiness_audit::RouteSummary {
                default_route: "jito".to_string(),
                allowed_routes: vec!["jito".to_string()],
                submit_route_order: vec!["jito".to_string()],
                policy_echo_required: true,
                simulate_before_submit: true,
            },
            rpc_probe: execution_readiness_audit::RpcProbeReport {
                required: true,
                attempted: true,
                reachable: true,
                classification: execution_readiness_audit::RpcProbeClassification::Reachable,
                timeout_ms: 3_000,
                successful_endpoint_role: Some("primary".to_string()),
                detail: "ok".to_string(),
                slot: Some(123),
            },
            adapter_probe: execution_readiness_audit::AdapterProbeReport {
                required: true,
                attempted: true,
                reachable: true,
                contract_valid: true,
                accepted: Some(true),
                classification: execution_readiness_audit::AdapterProbeClassification::Accepted,
                timeout_ms: 3_000,
                route: Some("jito".to_string()),
                detail: "simulated".to_string(),
            },
        }
    }

    fn sufficient_rehearsal_summary() -> DryRunHistorySummary {
        summarize_dry_run_history(
            ts("2026-03-25T12:00:00Z"),
            vec![
                rehearsal_row(ts("2026-03-25T11:55:00Z"), "rehearsal_green", "green"),
                rehearsal_row(
                    ts("2026-03-25T11:40:00Z"),
                    "rehearsal_green_with_business_reject",
                    "business reject",
                ),
            ],
            3_600,
            2,
        )
    }

    fn bounded_policy_report() -> tiny_live_policy_audit::TinyLivePolicyAuditReport {
        policy_report(
            tiny_live_policy_audit::TinyLivePolicyVerdict::TinyLivePolicyBounded,
            "bounded",
            true,
        )
    }

    fn policy_report(
        verdict: tiny_live_policy_audit::TinyLivePolicyVerdict,
        reason: &str,
        bounded: bool,
    ) -> tiny_live_policy_audit::TinyLivePolicyAuditReport {
        tiny_live_policy_audit::TinyLivePolicyAuditReport {
            generated_at: ts("2026-03-25T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            stage3_gate_not_evaluated: true,
            mode: "adapter_submit_confirm".to_string(),
            mode_compatible: true,
            execution_policy_contract_valid: true,
            execution_route_contract_valid: true,
            current_config_bounded_for_later_tiny_live_discussion: bounded,
            suitable_only_for_paper_or_dry_run: !bounded,
            verdict,
            reason: reason.to_string(),
            blockers: if bounded {
                Vec::new()
            } else {
                vec![reason.to_string()]
            },
            warnings: vec!["execution.enabled=false".to_string()],
            tiny_live_policy_enabled: true,
            current_default_route: "jito".to_string(),
            current_allowed_routes: vec!["jito".to_string()],
            current_route_order: vec!["jito".to_string()],
            policy_allowed_routes: vec!["jito".to_string()],
            current_shadow_copy_notional_sol: 0.05,
            current_risk_max_position_sol: 0.05,
            policy_max_trade_notional_sol: 0.05,
            current_execution_batch_size: 1,
            policy_max_batch_size: 1,
            current_risk_max_concurrent_positions: 1,
            policy_max_concurrent_positions: 1,
            current_risk_daily_loss_limit_pct: 1.0,
            policy_max_daily_loss_limit_pct: 1.0,
            current_pretrade_max_fee_overhead_bps: 1_000,
            policy_max_pretrade_fee_overhead_bps: 1_000,
            current_pretrade_max_priority_fee_lamports: 2_000,
            policy_max_pretrade_priority_fee_lamports: 2_000,
            current_policy_echo_required: true,
            policy_echo_required_for_tiny_live: true,
            route_policy_rows: Vec::new(),
        }
    }

    fn rehearsal_row(
        rehearsed_at: DateTime<Utc>,
        verdict: &str,
        reason: &str,
    ) -> ExecutionDryRunRehearsalRow {
        ExecutionDryRunRehearsalRow {
            rehearsal_id: 1,
            rehearsed_at,
            execution_mode: "adapter_submit_confirm".to_string(),
            execution_enabled: false,
            route: "jito".to_string(),
            token: "So11111111111111111111111111111111111111112".to_string(),
            notional_sol: 0.01,
            signer_pubkey_configured: true,
            config_valid: true,
            connectivity_valid: true,
            adapter_contract_valid: true,
            policy_contract_valid: true,
            route_contract_valid: true,
            ready_for_dry_run: true,
            would_be_admissible_for_later_tiny_live: true,
            rpc_preconditions_valid: true,
            rpc_slot: Some(123),
            rpc_blockhash: Some("abc".to_string()),
            rpc_signer_balance_lamports: Some(1),
            adapter_result_classification: verdict.to_string(),
            adapter_accepted: Some(true),
            adapter_detail: reason.to_string(),
            policy_echo_present: true,
            route_echo_present: true,
            contract_version_echo_present: true,
            response_slippage_bps: None,
            response_tip_lamports: None,
            response_compute_unit_limit: None,
            response_compute_unit_price_micro_lamports: None,
            verdict: verdict.to_string(),
            reason: reason.to_string(),
            blockers: Vec::new(),
            warnings: Vec::new(),
            rehearsal_json: "{}".to_string(),
        }
    }

    fn rehearsal_write(
        rehearsed_at: DateTime<Utc>,
        verdict: &str,
    ) -> ExecutionDryRunRehearsalWrite {
        ExecutionDryRunRehearsalWrite {
            rehearsed_at,
            execution_mode: "adapter_submit_confirm".to_string(),
            execution_enabled: false,
            route: "jito".to_string(),
            token: "So11111111111111111111111111111111111111112".to_string(),
            notional_sol: 0.01,
            signer_pubkey_configured: true,
            config_valid: true,
            connectivity_valid: true,
            adapter_contract_valid: true,
            policy_contract_valid: true,
            route_contract_valid: true,
            ready_for_dry_run: true,
            would_be_admissible_for_later_tiny_live: true,
            rpc_preconditions_valid: true,
            rpc_slot: Some(123),
            rpc_blockhash: Some("abc".to_string()),
            rpc_signer_balance_lamports: Some(1),
            adapter_result_classification: verdict.to_string(),
            adapter_accepted: Some(true),
            adapter_detail: verdict.to_string(),
            policy_echo_present: true,
            route_echo_present: true,
            contract_version_echo_present: true,
            response_slippage_bps: None,
            response_tip_lamports: None,
            response_compute_unit_limit: None,
            response_compute_unit_price_micro_lamports: None,
            verdict: verdict.to_string(),
            reason: verdict.to_string(),
            blockers: Vec::new(),
            warnings: Vec::new(),
            rehearsal_json: "{}".to_string(),
        }
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }
}
