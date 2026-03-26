use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_storage::{
    ExecutionDevnetActivationDrillRow, ExecutionDevnetActivationDrillWrite,
    ExecutionDevnetDressRehearsalRow, ExecutionDevnetDressRehearsalWrite, SqliteStore,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_devnet_dress_rehearsal.rs"]
mod devnet_dress_rehearsal;
#[allow(dead_code)]
#[path = "copybot_execution_dry_run_rehearsal.rs"]
mod execution_dry_run_rehearsal;

const USAGE: &str = "usage: copybot_devnet_readiness_report --config <path> [--limit <n>] [--dress-recent-horizon-seconds <seconds>] [--activation-recent-horizon-seconds <seconds>] [--min-recent-green-dress <count>] [--min-recent-green-activation <count>] [--json]";
const TARGET_ENVIRONMENT: &str = "devnet_readiness";
pub(crate) const DEFAULT_HISTORY_LIMIT: usize = 10;
pub(crate) const DEFAULT_DRESS_RECENT_HORIZON_SECONDS: u64 = 86_400;
pub(crate) const DEFAULT_ACTIVATION_RECENT_HORIZON_SECONDS: u64 = 86_400;
pub(crate) const DEFAULT_MIN_RECENT_GREEN_DRESS: usize = 1;
pub(crate) const DEFAULT_MIN_RECENT_GREEN_ACTIVATION: usize = 1;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) config_path: PathBuf,
    pub(crate) limit: usize,
    pub(crate) dress_recent_horizon_seconds: u64,
    pub(crate) activation_recent_horizon_seconds: u64,
    pub(crate) min_recent_green_dress: usize,
    pub(crate) min_recent_green_activation: usize,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DevnetReadinessVerdict {
    DevnetReadinessGreen,
    DevnetReadinessInsufficientRecentEvidence,
    DevnetReadinessBlockedByDressRehearsalHistory,
    DevnetReadinessBlockedByActivationDrillHistory,
    DevnetReadinessStaleHistory,
    DevnetReadinessRefusedForProdProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceEvidenceVerdict {
    Green,
    InsufficientRecentEvidence,
    StaleHistory,
    Blocked,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DressRehearsalHistorySummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) records_loaded: usize,
    pub(crate) recent_records_within_horizon: usize,
    pub(crate) recent_horizon_seconds: u64,
    pub(crate) latest_record_age_seconds: Option<u64>,
    pub(crate) stale_records_excluded_from_verdict: bool,
    pub(crate) stale_records_excluded_count: usize,
    pub(crate) recent_green_count: usize,
    pub(crate) recent_blocked_count: usize,
    pub(crate) latest_recent_verdict: Option<String>,
    pub(crate) verdict_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActivationDrillHistorySummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) records_loaded: usize,
    pub(crate) recent_records_within_horizon: usize,
    pub(crate) recent_horizon_seconds: u64,
    pub(crate) latest_record_age_seconds: Option<u64>,
    pub(crate) stale_records_excluded_from_verdict: bool,
    pub(crate) stale_records_excluded_count: usize,
    pub(crate) recent_green_count: usize,
    pub(crate) recent_blocked_count: usize,
    pub(crate) recent_rollback_success_count: usize,
    pub(crate) recent_rollback_failure_count: usize,
    pub(crate) recent_internal_consistency_count: usize,
    pub(crate) latest_recent_verdict: Option<String>,
    pub(crate) verdict_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DevnetReadinessReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) config_path: String,
    pub(crate) db_path: String,
    pub(crate) target_environment: String,
    pub(crate) config_env: String,
    pub(crate) prod_profile_refused: bool,
    pub(crate) non_prod_only: bool,
    pub(crate) production_unchanged: bool,
    pub(crate) planning_safe_for_production: bool,
    pub(crate) prod_stage3_not_overridden: bool,
    pub(crate) execution_enabled: bool,
    pub(crate) verdict: DevnetReadinessVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) dress_rehearsal_history: DressRehearsalHistorySummary,
    pub(crate) activation_drill_history: ActivationDrillHistorySummary,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DevnetReadinessRefusalReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) config_path: String,
    pub(crate) db_path: String,
    pub(crate) target_environment: String,
    pub(crate) config_env: String,
    pub(crate) prod_profile_refused: bool,
    pub(crate) non_prod_only: bool,
    pub(crate) production_unchanged: bool,
    pub(crate) planning_safe_for_production: bool,
    pub(crate) prod_stage3_not_overridden: bool,
    pub(crate) verdict: DevnetReadinessVerdict,
    pub(crate) reason: String,
}

#[derive(Debug, Clone)]
pub(crate) enum DevnetReadinessEvaluation {
    Report(DevnetReadinessReport),
    Refusal(DevnetReadinessRefusalReport),
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
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut dress_recent_horizon_seconds = DEFAULT_DRESS_RECENT_HORIZON_SECONDS;
    let mut activation_recent_horizon_seconds = DEFAULT_ACTIVATION_RECENT_HORIZON_SECONDS;
    let mut min_recent_green_dress = DEFAULT_MIN_RECENT_GREEN_DRESS;
    let mut min_recent_green_activation = DEFAULT_MIN_RECENT_GREEN_ACTIVATION;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--limit" => limit = parse_usize_arg("--limit", args.next())?,
            "--dress-recent-horizon-seconds" => {
                dress_recent_horizon_seconds =
                    parse_u64_arg("--dress-recent-horizon-seconds", args.next())?
            }
            "--activation-recent-horizon-seconds" => {
                activation_recent_horizon_seconds =
                    parse_u64_arg("--activation-recent-horizon-seconds", args.next())?
            }
            "--min-recent-green-dress" => {
                min_recent_green_dress = parse_usize_arg("--min-recent-green-dress", args.next())?
            }
            "--min-recent-green-activation" => {
                min_recent_green_activation =
                    parse_usize_arg("--min-recent-green-activation", args.next())?
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        limit: limit.max(1),
        dress_recent_horizon_seconds: dress_recent_horizon_seconds.max(1),
        activation_recent_horizon_seconds: activation_recent_horizon_seconds.max(1),
        min_recent_green_dress: min_recent_green_dress.max(1),
        min_recent_green_activation: min_recent_green_activation.max(1),
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

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid u64 value for {flag}: {raw}"))
}

fn run(config: Config) -> Result<String> {
    match evaluate_devnet_readiness_report(&config, Utc::now())? {
        DevnetReadinessEvaluation::Report(report) => {
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing devnet readiness json")
            } else {
                Ok(render_human(&report))
            }
        }
        DevnetReadinessEvaluation::Refusal(report) => {
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing devnet readiness refusal json")
            } else {
                Ok(render_refusal_human(&report))
            }
        }
    }
}

pub(crate) fn evaluate_devnet_readiness_report(
    config: &Config,
    now: DateTime<Utc>,
) -> Result<DevnetReadinessEvaluation> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = execution_dry_run_rehearsal::resolve_db_path(
        &config.config_path,
        &loaded_config.sqlite.path,
    );

    if devnet_dress_rehearsal::is_production_like_env(loaded_config.system.env.as_str()) {
        return Ok(DevnetReadinessEvaluation::Refusal(
            DevnetReadinessRefusalReport {
                generated_at: now,
                config_path: config.config_path.display().to_string(),
                db_path: db_path.display().to_string(),
                target_environment: TARGET_ENVIRONMENT.to_string(),
                config_env: loaded_config.system.env.clone(),
                prod_profile_refused: true,
                non_prod_only: true,
                production_unchanged: true,
                planning_safe_for_production: true,
                prod_stage3_not_overridden: true,
                verdict: DevnetReadinessVerdict::DevnetReadinessRefusedForProdProfile,
                reason: format!(
                    "copybot_devnet_readiness_report refuses production-like system.env={}",
                    loaded_config.system.env
                ),
            },
        ));
    }

    let store = SqliteStore::open(&db_path).with_context(|| {
        format!(
            "failed opening devnet readiness history store {}",
            db_path.display()
        )
    })?;
    let report = build_report(&store, config, &loaded_config, &db_path, now)?;
    Ok(DevnetReadinessEvaluation::Report(report))
}

fn build_report(
    store: &SqliteStore,
    config: &Config,
    loaded_config: &AppConfig,
    db_path: &Path,
    now: DateTime<Utc>,
) -> Result<DevnetReadinessReport> {
    let dress_rows = store
        .list_execution_devnet_dress_rehearsals(config.limit)
        .context("failed loading devnet dress rehearsal history")?;
    let activation_rows = store
        .list_execution_devnet_activation_drills(config.limit)
        .context("failed loading devnet activation drill history")?;

    let (dress_summary, dress_verdict) = summarize_dress_history(
        &dress_rows,
        now,
        config.dress_recent_horizon_seconds,
        config.min_recent_green_dress,
    );
    let (activation_summary, activation_verdict) = summarize_activation_history(
        &activation_rows,
        now,
        config.activation_recent_horizon_seconds,
        config.min_recent_green_activation,
    );

    let (verdict, reason, blockers) = derive_top_level_verdict(
        &dress_summary,
        dress_verdict,
        &activation_summary,
        activation_verdict,
    );
    let warnings = vec![
        "non-prod readiness evidence does not authorize production activation".to_string(),
        "Stage 3 discovery evidence on prod remains the hard gate before any prod activation discussion".to_string(),
    ];

    Ok(DevnetReadinessReport {
        generated_at: now,
        config_path: config.config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        prod_profile_refused: false,
        non_prod_only: true,
        production_unchanged: true,
        planning_safe_for_production: true,
        prod_stage3_not_overridden: true,
        execution_enabled: loaded_config.execution.enabled,
        verdict,
        reason,
        blockers,
        warnings,
        dress_rehearsal_history: dress_summary,
        activation_drill_history: activation_summary,
    })
}

fn summarize_dress_history(
    rows: &[ExecutionDevnetDressRehearsalRow],
    now: DateTime<Utc>,
    horizon_seconds: u64,
    min_recent_green: usize,
) -> (DressRehearsalHistorySummary, SourceEvidenceVerdict) {
    let latest_record_age_seconds = rows.first().map(|row| age_seconds(now, row.rehearsed_at));
    let recent_rows: Vec<&ExecutionDevnetDressRehearsalRow> = rows
        .iter()
        .filter(|row| age_seconds(now, row.rehearsed_at) <= horizon_seconds)
        .collect();
    let stale_excluded_count = rows.len().saturating_sub(recent_rows.len());
    let stale_excluded = stale_excluded_count > 0;
    let recent_green_count = recent_rows
        .iter()
        .filter(|row| is_green_dress_verdict(row.verdict.as_str()))
        .count();
    let recent_blocked_count = recent_rows
        .iter()
        .filter(|row| !is_green_dress_verdict(row.verdict.as_str()))
        .count();
    let latest_recent_verdict = recent_rows.first().map(|row| row.verdict.clone());
    let verdict_counts = count_verdicts(rows.iter().map(|row| row.verdict.as_str()));

    let (verdict, reason) = if rows.is_empty() {
        (
            SourceEvidenceVerdict::InsufficientRecentEvidence,
            "no persisted devnet dress rehearsal history".to_string(),
        )
    } else if recent_rows.is_empty() {
        (
            SourceEvidenceVerdict::StaleHistory,
            format!(
                "persisted devnet dress rehearsal history exists, but none is within the recent horizon of {}s",
                horizon_seconds
            ),
        )
    } else if recent_blocked_count > 0 {
        (
            SourceEvidenceVerdict::Blocked,
            format!(
                "recent devnet dress rehearsal history contains {} blocked outcome(s)",
                recent_blocked_count
            ),
        )
    } else if recent_green_count < min_recent_green {
        (
            SourceEvidenceVerdict::InsufficientRecentEvidence,
            format!(
                "recent devnet dress rehearsal history has only {} green outcome(s); need at least {}",
                recent_green_count, min_recent_green
            ),
        )
    } else {
        (
            SourceEvidenceVerdict::Green,
            format!(
                "recent devnet dress rehearsal history is green with {} acceptable rehearsal(s)",
                recent_green_count
            ),
        )
    };

    (
        DressRehearsalHistorySummary {
            verdict: source_verdict_str(verdict).to_string(),
            reason,
            records_loaded: rows.len(),
            recent_records_within_horizon: recent_rows.len(),
            recent_horizon_seconds: horizon_seconds,
            latest_record_age_seconds,
            stale_records_excluded_from_verdict: stale_excluded,
            stale_records_excluded_count: stale_excluded_count,
            recent_green_count,
            recent_blocked_count,
            latest_recent_verdict,
            verdict_counts,
        },
        verdict,
    )
}

fn summarize_activation_history(
    rows: &[ExecutionDevnetActivationDrillRow],
    now: DateTime<Utc>,
    horizon_seconds: u64,
    min_recent_green: usize,
) -> (ActivationDrillHistorySummary, SourceEvidenceVerdict) {
    let latest_record_age_seconds = rows.first().map(|row| age_seconds(now, row.drilled_at));
    let recent_rows: Vec<&ExecutionDevnetActivationDrillRow> = rows
        .iter()
        .filter(|row| age_seconds(now, row.drilled_at) <= horizon_seconds)
        .collect();
    let stale_excluded_count = rows.len().saturating_sub(recent_rows.len());
    let stale_excluded = stale_excluded_count > 0;
    let recent_green_count = recent_rows
        .iter()
        .filter(|row| row.activation_drill_verdict == "devnet_activation_drill_green")
        .count();
    let recent_blocked_count = recent_rows
        .iter()
        .filter(|row| row.activation_drill_verdict != "devnet_activation_drill_green")
        .count();
    let recent_rollback_success_count = recent_rows
        .iter()
        .filter(|row| row.rollback_drill_verdict == "rollback_drill_green")
        .count();
    let recent_rollback_failure_count = recent_rows
        .iter()
        .filter(|row| row.rollback_drill_verdict != "rollback_drill_green")
        .count();
    let recent_internal_consistency_count = recent_rows
        .iter()
        .filter(|row| {
            row.activated_config_policy_bounded
                && row.activated_config_guardrails_bounded
                && row.rollback_restores_safe_mode
        })
        .count();
    let latest_recent_verdict = recent_rows
        .first()
        .map(|row| row.activation_drill_verdict.clone());
    let verdict_counts = count_verdicts(
        recent_rows
            .iter()
            .map(|row| row.activation_drill_verdict.as_str()),
    );
    let all_verdict_counts =
        count_verdicts(rows.iter().map(|row| row.activation_drill_verdict.as_str()));
    let verdict_counts = if rows.is_empty() {
        verdict_counts
    } else {
        all_verdict_counts
    };

    let (verdict, reason) = if rows.is_empty() {
        (
            SourceEvidenceVerdict::InsufficientRecentEvidence,
            "no persisted devnet activation drill history".to_string(),
        )
    } else if recent_rows.is_empty() {
        (
            SourceEvidenceVerdict::StaleHistory,
            format!(
                "persisted devnet activation drill history exists, but none is within the recent horizon of {}s",
                horizon_seconds
            ),
        )
    } else if recent_blocked_count > 0 || recent_rollback_failure_count > 0 {
        (
            SourceEvidenceVerdict::Blocked,
            format!(
                "recent devnet activation drill history contains {} blocked activation outcome(s) and {} rollback failure outcome(s)",
                recent_blocked_count, recent_rollback_failure_count
            ),
        )
    } else if recent_internal_consistency_count < min_recent_green {
        (
            SourceEvidenceVerdict::InsufficientRecentEvidence,
            format!(
                "recent devnet activation drill history has only {} internally consistent green drill(s); need at least {}",
                recent_internal_consistency_count, min_recent_green
            ),
        )
    } else {
        (
            SourceEvidenceVerdict::Green,
            format!(
                "recent devnet activation drill history is green with {} internally consistent drill(s)",
                recent_internal_consistency_count
            ),
        )
    };

    (
        ActivationDrillHistorySummary {
            verdict: source_verdict_str(verdict).to_string(),
            reason,
            records_loaded: rows.len(),
            recent_records_within_horizon: recent_rows.len(),
            recent_horizon_seconds: horizon_seconds,
            latest_record_age_seconds,
            stale_records_excluded_from_verdict: stale_excluded,
            stale_records_excluded_count: stale_excluded_count,
            recent_green_count,
            recent_blocked_count,
            recent_rollback_success_count,
            recent_rollback_failure_count,
            recent_internal_consistency_count,
            latest_recent_verdict,
            verdict_counts,
        },
        verdict,
    )
}

fn derive_top_level_verdict(
    dress_summary: &DressRehearsalHistorySummary,
    dress_verdict: SourceEvidenceVerdict,
    activation_summary: &ActivationDrillHistorySummary,
    activation_verdict: SourceEvidenceVerdict,
) -> (DevnetReadinessVerdict, String, Vec<String>) {
    if dress_verdict == SourceEvidenceVerdict::Blocked {
        return (
            DevnetReadinessVerdict::DevnetReadinessBlockedByDressRehearsalHistory,
            dress_summary.reason.clone(),
            vec![format!("dress_rehearsal_history: {}", dress_summary.reason)],
        );
    }
    if activation_verdict == SourceEvidenceVerdict::Blocked {
        return (
            DevnetReadinessVerdict::DevnetReadinessBlockedByActivationDrillHistory,
            activation_summary.reason.clone(),
            vec![format!(
                "activation_drill_history: {}",
                activation_summary.reason
            )],
        );
    }
    if dress_verdict == SourceEvidenceVerdict::StaleHistory
        || activation_verdict == SourceEvidenceVerdict::StaleHistory
    {
        let mut blockers = Vec::new();
        if dress_verdict == SourceEvidenceVerdict::StaleHistory {
            blockers.push(format!("dress_rehearsal_history: {}", dress_summary.reason));
        }
        if activation_verdict == SourceEvidenceVerdict::StaleHistory {
            blockers.push(format!(
                "activation_drill_history: {}",
                activation_summary.reason
            ));
        }
        return (
            DevnetReadinessVerdict::DevnetReadinessStaleHistory,
            "non-prod evidence exists, but the recent horizons are stale".to_string(),
            blockers,
        );
    }
    if dress_verdict == SourceEvidenceVerdict::InsufficientRecentEvidence
        || activation_verdict == SourceEvidenceVerdict::InsufficientRecentEvidence
    {
        let mut blockers = Vec::new();
        if dress_verdict == SourceEvidenceVerdict::InsufficientRecentEvidence {
            blockers.push(format!("dress_rehearsal_history: {}", dress_summary.reason));
        }
        if activation_verdict == SourceEvidenceVerdict::InsufficientRecentEvidence {
            blockers.push(format!(
                "activation_drill_history: {}",
                activation_summary.reason
            ));
        }
        return (
            DevnetReadinessVerdict::DevnetReadinessInsufficientRecentEvidence,
            "recent non-prod evidence is still insufficient".to_string(),
            blockers,
        );
    }

    (
        DevnetReadinessVerdict::DevnetReadinessGreen,
        "recent devnet dress rehearsal and activation drill histories are both green enough for non-production execution readiness".to_string(),
        Vec::new(),
    )
}

fn is_green_dress_verdict(verdict: &str) -> bool {
    matches!(
        verdict,
        "devnet_rehearsal_green" | "devnet_rehearsal_green_with_business_reject"
    )
}

fn count_verdicts<'a, I>(values: I) -> BTreeMap<String, usize>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value.to_string()).or_insert(0usize) += 1;
    }
    counts
}

fn age_seconds(now: DateTime<Utc>, ts: DateTime<Utc>) -> u64 {
    now.signed_duration_since(ts).num_seconds().max(0) as u64
}

fn source_verdict_str(verdict: SourceEvidenceVerdict) -> &'static str {
    match verdict {
        SourceEvidenceVerdict::Green => "green",
        SourceEvidenceVerdict::InsufficientRecentEvidence => "insufficient_recent_evidence",
        SourceEvidenceVerdict::StaleHistory => "stale_history",
        SourceEvidenceVerdict::Blocked => "blocked",
    }
}

fn render_human(report: &DevnetReadinessReport) -> String {
    [
        "event=copybot_devnet_readiness_report".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("target_environment={}", report.target_environment),
        format!("config_env={}", report.config_env),
        format!("non_prod_only={}", report.non_prod_only),
        format!("production_unchanged={}", report.production_unchanged),
        format!(
            "prod_stage3_not_overridden={}",
            report.prod_stage3_not_overridden
        ),
        format!("execution_enabled={}", report.execution_enabled),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("blockers={}", report.blockers.join(" | ")),
        format!(
            "dress_rehearsal_history_verdict={}",
            report.dress_rehearsal_history.verdict
        ),
        format!(
            "dress_rehearsal_latest_age_seconds={}",
            report
                .dress_rehearsal_history
                .latest_record_age_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "dress_rehearsal_recent_green_count={}",
            report.dress_rehearsal_history.recent_green_count
        ),
        format!(
            "activation_drill_history_verdict={}",
            report.activation_drill_history.verdict
        ),
        format!(
            "activation_drill_latest_age_seconds={}",
            report
                .activation_drill_history
                .latest_record_age_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "activation_drill_recent_green_count={}",
            report.activation_drill_history.recent_green_count
        ),
        format!(
            "activation_drill_recent_rollback_success_count={}",
            report
                .activation_drill_history
                .recent_rollback_success_count
        ),
        format!(
            "activation_drill_recent_internal_consistency_count={}",
            report
                .activation_drill_history
                .recent_internal_consistency_count
        ),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

fn render_refusal_human(report: &DevnetReadinessRefusalReport) -> String {
    [
        "event=copybot_devnet_readiness_report".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("target_environment={}", report.target_environment),
        format!("config_env={}", report.config_env),
        format!("prod_profile_refused={}", report.prod_profile_refused),
        format!("non_prod_only={}", report.non_prod_only),
        format!("production_unchanged={}", report.production_unchanged),
        format!(
            "prod_stage3_not_overridden={}",
            report.prod_stage3_not_overridden
        ),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
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
    use serde_json::Value;
    use std::fs;

    const WSOL: &str = "So11111111111111111111111111111111111111112";

    #[test]
    fn prod_like_config_is_refused() {
        let temp = temp_dir("devnet_readiness_prod_refused");
        let config_path = temp.join("prod.server.toml");
        fs::write(&config_path, "[system]\nenv = \"prod-live\"\n").expect("write config");
        let output = run(Config {
            config_path: config_path.clone(),
            limit: 10,
            dress_recent_horizon_seconds: 86_400,
            activation_recent_horizon_seconds: 86_400,
            min_recent_green_dress: 1,
            min_recent_green_activation: 1,
            json: true,
        })
        .expect("run report");
        let report: Value = serde_json::from_str(&output).expect("json");
        assert_eq!(
            report["verdict"],
            "devnet_readiness_refused_for_prod_profile"
        );
    }

    #[test]
    fn stale_histories_cannot_yield_green() {
        let temp = temp_dir("devnet_readiness_stale");
        let db_path = temp.join("runtime.db");
        let store = SqliteStore::open(&db_path).expect("open store");
        append_dress_row(&store, ts("2026-03-20T12:00:00Z"), "devnet_rehearsal_green");
        append_activation_row(
            &store,
            ts("2026-03-20T12:00:00Z"),
            "devnet_activation_drill_green",
            "rollback_drill_green",
            true,
            true,
            true,
        );
        let config = base_non_prod_config(&db_path);
        let report = build_report(
            &store,
            &test_config(&temp.join("devnet.server.toml")),
            &config,
            &db_path,
            ts("2026-03-26T12:00:00Z"),
        )
        .expect("report");
        assert_eq!(
            report.verdict,
            DevnetReadinessVerdict::DevnetReadinessStaleHistory
        );
    }

    #[test]
    fn recent_green_histories_yield_green_readiness() {
        let temp = temp_dir("devnet_readiness_green");
        let db_path = temp.join("runtime.db");
        let store = SqliteStore::open(&db_path).expect("open store");
        append_dress_row(&store, ts("2026-03-26T11:55:00Z"), "devnet_rehearsal_green");
        append_activation_row(
            &store,
            ts("2026-03-26T11:54:00Z"),
            "devnet_activation_drill_green",
            "rollback_drill_green",
            true,
            true,
            true,
        );
        let config = base_non_prod_config(&db_path);
        let report = build_report(
            &store,
            &test_config(&temp.join("devnet.server.toml")),
            &config,
            &db_path,
            ts("2026-03-26T12:00:00Z"),
        )
        .expect("report");
        assert_eq!(report.verdict, DevnetReadinessVerdict::DevnetReadinessGreen);
    }

    #[test]
    fn recent_blocked_activation_drill_blocks_top_level_readiness() {
        let temp = temp_dir("devnet_readiness_activation_blocked");
        let db_path = temp.join("runtime.db");
        let store = SqliteStore::open(&db_path).expect("open store");
        append_dress_row(
            &store,
            ts("2026-03-26T11:55:00Z"),
            "devnet_rehearsal_green_with_business_reject",
        );
        append_activation_row(
            &store,
            ts("2026-03-26T11:54:00Z"),
            "devnet_activation_drill_blocked_by_non_prod_contract",
            "rollback_drill_failed",
            false,
            false,
            false,
        );
        let config = base_non_prod_config(&db_path);
        let report = build_report(
            &store,
            &test_config(&temp.join("devnet.server.toml")),
            &config,
            &db_path,
            ts("2026-03-26T12:00:00Z"),
        )
        .expect("report");
        assert_eq!(
            report.verdict,
            DevnetReadinessVerdict::DevnetReadinessBlockedByActivationDrillHistory
        );
    }

    #[test]
    fn missing_one_required_source_yields_insufficient_evidence() {
        let temp = temp_dir("devnet_readiness_missing_source");
        let db_path = temp.join("runtime.db");
        let store = SqliteStore::open(&db_path).expect("open store");
        append_dress_row(&store, ts("2026-03-26T11:55:00Z"), "devnet_rehearsal_green");
        let config = base_non_prod_config(&db_path);
        let report = build_report(
            &store,
            &test_config(&temp.join("devnet.server.toml")),
            &config,
            &db_path,
            ts("2026-03-26T12:00:00Z"),
        )
        .expect("report");
        assert_eq!(
            report.verdict,
            DevnetReadinessVerdict::DevnetReadinessInsufficientRecentEvidence
        );
    }

    #[test]
    fn output_remains_explicit_that_prod_activation_is_not_authorized() {
        let temp = temp_dir("devnet_readiness_human");
        let db_path = temp.join("runtime.db");
        let store = SqliteStore::open(&db_path).expect("open store");
        append_dress_row(&store, ts("2026-03-26T11:55:00Z"), "devnet_rehearsal_green");
        append_activation_row(
            &store,
            ts("2026-03-26T11:54:00Z"),
            "devnet_activation_drill_green",
            "rollback_drill_green",
            true,
            true,
            true,
        );
        let config = base_non_prod_config(&db_path);
        let report = build_report(
            &store,
            &test_config(&temp.join("devnet.server.toml")),
            &config,
            &db_path,
            ts("2026-03-26T12:00:00Z"),
        )
        .expect("report");
        let human = render_human(&report);
        assert!(human.contains("prod_stage3_not_overridden=true"));
        assert!(human.contains("non_prod_only=true"));
    }

    fn append_dress_row(store: &SqliteStore, rehearsed_at: DateTime<Utc>, verdict: &str) {
        store
            .append_execution_devnet_dress_rehearsal(&ExecutionDevnetDressRehearsalWrite {
                rehearsed_at,
                target_environment: "devnet".to_string(),
                config_env: "paper-devnet".to_string(),
                execution_mode: "adapter_submit_confirm".to_string(),
                execution_enabled: false,
                route: "jito".to_string(),
                token: WSOL.to_string(),
                side: "buy".to_string(),
                notional_sol: 0.01,
                readiness_verdict: "ready_for_execution_dry_run".to_string(),
                readiness_reason: "ready".to_string(),
                dry_run_verdict: Some("rehearsal_green".to_string()),
                dry_run_reason: Some("green".to_string()),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                tiny_live_policy_reason: "bounded".to_string(),
                tiny_live_policy_bounded: true,
                signer_pubkey_configured: true,
                config_valid: true,
                connectivity_valid: true,
                adapter_contract_valid: true,
                policy_contract_valid: true,
                route_contract_valid: true,
                ready_for_dry_run: true,
                would_be_admissible_for_later_tiny_live: true,
                rpc_preconditions_valid: true,
                adapter_result_classification: Some("accepted".to_string()),
                adapter_accepted: Some(true),
                policy_echo_present: true,
                route_echo_present: true,
                contract_version_echo_present: true,
                verdict: verdict.to_string(),
                reason: verdict.to_string(),
                blockers: if is_green_dress_verdict(verdict) {
                    Vec::new()
                } else {
                    vec![verdict.to_string()]
                },
                warnings: Vec::new(),
                rehearsal_json: "{}".to_string(),
            })
            .expect("append dress row");
    }

    fn append_activation_row(
        store: &SqliteStore,
        drilled_at: DateTime<Utc>,
        activation_verdict: &str,
        rollback_verdict: &str,
        policy_bounded: bool,
        guardrails_bounded: bool,
        rollback_restores: bool,
    ) {
        store
            .append_execution_devnet_activation_drill(&ExecutionDevnetActivationDrillWrite {
                drilled_at,
                target_environment: "devnet_activation_drill".to_string(),
                config_env: "paper-devnet".to_string(),
                source_config_path: "/tmp/devnet.server.toml".to_string(),
                execution_enabled_source: false,
                route: "jito".to_string(),
                token: WSOL.to_string(),
                side: "buy".to_string(),
                notional_sol: 0.01,
                launch_dossier_verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
                launch_dossier_reason: "ready".to_string(),
                pre_activation_gate_verdict: "blocked_by_stage3".to_string(),
                pre_activation_gate_reason: "stage3 not green".to_string(),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                tiny_live_guardrail_verdict: "tiny_live_guardrails_bounded".to_string(),
                tiny_live_policy_bounded: policy_bounded,
                tiny_live_guardrails_bounded: guardrails_bounded,
                activation_overlay_change_count: 10,
                rollback_overlay_change_count: 10,
                activation_drill_verdict: activation_verdict.to_string(),
                activation_drill_reason: activation_verdict.to_string(),
                activation_rehearsal_verdict: Some("devnet_rehearsal_green".to_string()),
                activation_rehearsal_reason: Some("green".to_string()),
                rollback_drill_verdict: rollback_verdict.to_string(),
                rollback_drill_reason: rollback_verdict.to_string(),
                activated_config_policy_bounded: policy_bounded,
                activated_config_guardrails_bounded: guardrails_bounded,
                rollback_restores_safe_mode: rollback_restores,
                blockers: if activation_verdict == "devnet_activation_drill_green" {
                    Vec::new()
                } else {
                    vec![activation_verdict.to_string()]
                },
                warnings: Vec::new(),
                drill_json: "{}".to_string(),
            })
            .expect("append activation row");
    }

    fn base_non_prod_config(db_path: &Path) -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "paper-devnet".to_string();
        config.sqlite.path = db_path.display().to_string();
        config.execution.enabled = false;
        config
    }

    fn test_config(path: &Path) -> Config {
        Config {
            config_path: path.to_path_buf(),
            limit: DEFAULT_HISTORY_LIMIT,
            dress_recent_horizon_seconds: DEFAULT_DRESS_RECENT_HORIZON_SECONDS,
            activation_recent_horizon_seconds: DEFAULT_ACTIVATION_RECENT_HORIZON_SECONDS,
            min_recent_green_dress: DEFAULT_MIN_RECENT_GREEN_DRESS,
            min_recent_green_activation: DEFAULT_MIN_RECENT_GREEN_ACTIVATION,
            json: false,
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = format!(
            "{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let dir = env::temp_dir().join(format!("copybot_devnet_readiness_{}_{}", label, unique));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }
}
