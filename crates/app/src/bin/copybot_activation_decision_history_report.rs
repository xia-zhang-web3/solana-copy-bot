#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const USAGE: &str = "usage: copybot_activation_decision_history_report (--history-dir <path> [--packet <path>]... [--latest] [--limit <count>] | --compare <older> <newer>) [--json]";
const DEFAULT_HISTORY_LIMIT: usize = 10;

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
struct Config {
    history_dir: Option<PathBuf>,
    packet_paths: Vec<PathBuf>,
    compare: Option<(PathBuf, PathBuf)>,
    latest: bool,
    limit: usize,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DecisionHistoryVerdict {
    DecisionHistoryLatestBlocked,
    DecisionHistoryLatestDiscussionReady,
    DecisionHistoryLatestRefusedForProfileMismatch,
    DecisionHistoryInsufficientPackets,
    DecisionHistoryCompareReady,
    DecisionHistoryInvalidArtifact,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BlockerTrend {
    Narrower,
    Wider,
    Unchanged,
    Mixed,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DecisionPacketVerdict {
    DecisionPacketBlocked,
    DecisionPacketDiscussionReadyButNotAuthorized,
    DecisionPacketRefusedForProfileMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigFingerprintSummary {
    scope: String,
    sha256: String,
    secrets_excluded: bool,
    sensitive_urls_redacted_before_hashing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProdPreActivationGateSummary {
    verdict: String,
    reason: String,
    planning_green: bool,
    blocked_by_stage3: bool,
    stage3_verdict: String,
    stage3_reason: String,
    stage3_captures_within_recent_horizon: usize,
    stage3_latest_capture_age_seconds: Option<u64>,
    stage4_readiness_verdict: String,
    stage4_rehearsal_history_verdict: String,
    tiny_live_policy_verdict: String,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LaunchDossierSummary {
    verdict: String,
    reason: String,
    ready_when_stage_gate_allows: bool,
    activation_overlay_complete: bool,
    rollback_plan_complete: bool,
    service_restart_contract_complete: bool,
    activation_overlay_change_count: usize,
    rollback_overlay_change_count: usize,
    drift_finding_count: usize,
    blocker_count: usize,
    first_blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RollbackTriggerSummary {
    trigger: String,
    threshold_kind: String,
    threshold_rate_pct: Option<f64>,
    threshold_seconds: Option<u64>,
    threshold_sol: Option<f64>,
    threshold_count: Option<u32>,
    evaluation_window_seconds: Option<u64>,
    action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GuardrailSummary {
    verdict: String,
    reason: String,
    bounded: bool,
    enabled: bool,
    blocker_count: usize,
    first_blocker: Option<String>,
    rollback_trigger_count: usize,
    rollback_triggers: Vec<RollbackTriggerSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NonProdReadinessSummary {
    verdict: String,
    reason: String,
    green: bool,
    prod_profile_refused: bool,
    config_env: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    dress_latest_record_age_seconds: Option<u64>,
    dress_recent_green_count: usize,
    activation_latest_record_age_seconds: Option<u64>,
    activation_recent_green_count: usize,
    activation_recent_rollback_success_count: usize,
    activation_recent_internal_consistency_count: usize,
    stale_evidence_excluded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionPacketArtifact {
    generated_at: DateTime<Utc>,
    packet_version: String,
    build_version: String,
    git_commit: Option<String>,
    prod_config_path: String,
    non_prod_config_path: String,
    operator_note: Option<String>,
    execution_enabled: bool,
    read_only_packet: bool,
    activation_authorized: bool,
    discussion_ready_only: bool,
    prod_stage3_remains_hard_gate: bool,
    non_prod_evidence_is_secondary: bool,
    verdict: DecisionPacketVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    checklist_verdict: String,
    checklist_reason: String,
    prod_config_fingerprint: ConfigFingerprintSummary,
    non_prod_config_fingerprint: ConfigFingerprintSummary,
    prod_pre_activation_gate: ProdPreActivationGateSummary,
    launch_dossier: LaunchDossierSummary,
    tiny_live_guardrails: GuardrailSummary,
    non_prod_readiness: NonProdReadinessSummary,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidArtifact {
    path: String,
    error: String,
}

#[derive(Debug, Clone)]
struct LoadedPacket {
    path: PathBuf,
    packet: DecisionPacketArtifact,
}

#[derive(Debug, Clone, Serialize)]
struct PacketSnapshotSummary {
    path: String,
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    blocker_count: usize,
    warning_count: usize,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionHistorySummaryReport {
    mode: String,
    verdict: DecisionHistoryVerdict,
    reason: String,
    history_dir: Option<String>,
    packet_paths_examined: Vec<String>,
    latest_only_requested: bool,
    limit: usize,
    packets_loaded: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    blocked_count: usize,
    discussion_ready_count: usize,
    refused_count: usize,
    latest_packet: Option<PacketSnapshotSummary>,
    latest_prod_pre_activation_gate: Option<ProdPreActivationGateSummary>,
    latest_launch_dossier: Option<LaunchDossierSummary>,
    latest_tiny_live_guardrails: Option<GuardrailSummary>,
    latest_non_prod_readiness: Option<NonProdReadinessSummary>,
    latest_execution_enabled: Option<bool>,
    prod_config_fingerprint_changed_over_history: bool,
    non_prod_config_fingerprint_changed_over_history: bool,
    blockers_trend: BlockerTrend,
    blockers_added_since_oldest: Vec<String>,
    blockers_removed_since_oldest: Vec<String>,
    recent_verdict_progression: Vec<PacketSnapshotSummary>,
    artifact_analysis_only: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionHistoryDiffReport {
    mode: String,
    verdict: DecisionHistoryVerdict,
    reason: String,
    older_path: String,
    newer_path: String,
    invalid_artifacts: Vec<InvalidArtifact>,
    older_generated_at: Option<DateTime<Utc>>,
    newer_generated_at: Option<DateTime<Utc>>,
    older_verdict: Option<String>,
    newer_verdict: Option<String>,
    older_reason: Option<String>,
    newer_reason: Option<String>,
    checklist_verdict_changed: bool,
    older_checklist_verdict: Option<String>,
    newer_checklist_verdict: Option<String>,
    blockers_added: Vec<String>,
    blockers_removed: Vec<String>,
    warnings_added: Vec<String>,
    warnings_removed: Vec<String>,
    prod_pre_activation_gate_verdict_changed: bool,
    older_prod_pre_activation_gate_verdict: Option<String>,
    newer_prod_pre_activation_gate_verdict: Option<String>,
    prod_pre_activation_gate_reason_changed: bool,
    older_prod_pre_activation_gate_reason: Option<String>,
    newer_prod_pre_activation_gate_reason: Option<String>,
    launch_dossier_verdict_changed: bool,
    older_launch_dossier_verdict: Option<String>,
    newer_launch_dossier_verdict: Option<String>,
    launch_dossier_reason_changed: bool,
    older_launch_dossier_reason: Option<String>,
    newer_launch_dossier_reason: Option<String>,
    tiny_live_guardrails_verdict_changed: bool,
    older_tiny_live_guardrails_verdict: Option<String>,
    newer_tiny_live_guardrails_verdict: Option<String>,
    tiny_live_guardrails_reason_changed: bool,
    older_tiny_live_guardrails_reason: Option<String>,
    newer_tiny_live_guardrails_reason: Option<String>,
    non_prod_readiness_verdict_changed: bool,
    older_non_prod_readiness_verdict: Option<String>,
    newer_non_prod_readiness_verdict: Option<String>,
    non_prod_readiness_reason_changed: bool,
    older_non_prod_readiness_reason: Option<String>,
    newer_non_prod_readiness_reason: Option<String>,
    prod_config_fingerprint_changed: bool,
    older_prod_config_fingerprint_sha256: Option<String>,
    newer_prod_config_fingerprint_sha256: Option<String>,
    non_prod_config_fingerprint_changed: bool,
    older_non_prod_config_fingerprint_sha256: Option<String>,
    newer_non_prod_config_fingerprint_sha256: Option<String>,
    build_version_changed: bool,
    older_build_version: Option<String>,
    newer_build_version: Option<String>,
    git_commit_changed: bool,
    older_git_commit: Option<String>,
    newer_git_commit: Option<String>,
    artifact_analysis_only: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut history_dir: Option<PathBuf> = None;
    let mut packet_paths = Vec::new();
    let mut compare: Option<(PathBuf, PathBuf)> = None;
    let mut latest = false;
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--history-dir" => {
                history_dir = Some(PathBuf::from(parse_string_arg(
                    "--history-dir",
                    args.next(),
                )?))
            }
            "--packet" => {
                packet_paths.push(PathBuf::from(parse_string_arg("--packet", args.next())?))
            }
            "--compare" => {
                let older = PathBuf::from(parse_string_arg("--compare", args.next())?);
                let newer = PathBuf::from(parse_string_arg("--compare", args.next())?);
                compare = Some((older, newer));
            }
            "--latest" => latest = true,
            "--limit" => limit = parse_usize_arg("--limit", args.next())?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if compare.is_some() && (history_dir.is_some() || !packet_paths.is_empty() || latest) {
        bail!("--compare cannot be combined with --history-dir, --packet, or --latest");
    }
    if compare.is_none() && history_dir.is_none() && packet_paths.is_empty() {
        bail!("either --history-dir/--packet or --compare is required");
    }

    Ok(Some(Config {
        history_dir,
        packet_paths,
        compare,
        latest,
        limit: limit.max(1),
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
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

fn run(config: Config) -> Result<String> {
    if let Some((older, newer)) = &config.compare {
        let report = build_compare_report(older, newer)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing decision history compare report json")
        } else {
            Ok(render_compare_human(&report))
        }
    } else {
        let report = build_history_report(&config)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing decision history report json")
        } else {
            Ok(render_history_human(&report))
        }
    }
}

fn build_history_report(config: &Config) -> Result<DecisionHistorySummaryReport> {
    let packet_paths = collect_history_paths(config)?;
    let (mut valid_packets, invalid_artifacts) = load_artifacts(&packet_paths);
    valid_packets.sort_by(|left, right| {
        left.packet
            .generated_at
            .cmp(&right.packet.generated_at)
            .then_with(|| left.path.cmp(&right.path))
    });

    let latest_packet = valid_packets.last().cloned();
    let blocked_count = valid_packets
        .iter()
        .filter(|record| record.packet.verdict == DecisionPacketVerdict::DecisionPacketBlocked)
        .count();
    let discussion_ready_count = valid_packets
        .iter()
        .filter(|record| {
            record.packet.verdict
                == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized
        })
        .count();
    let refused_count = valid_packets
        .iter()
        .filter(|record| {
            record.packet.verdict == DecisionPacketVerdict::DecisionPacketRefusedForProfileMismatch
        })
        .count();

    let prod_config_fingerprint_changed_over_history = unique_count(
        valid_packets
            .iter()
            .map(|record| record.packet.prod_config_fingerprint.sha256.as_str()),
    ) > 1;
    let non_prod_config_fingerprint_changed_over_history = unique_count(
        valid_packets
            .iter()
            .map(|record| record.packet.non_prod_config_fingerprint.sha256.as_str()),
    ) > 1;

    let (blockers_trend, blockers_added_since_oldest, blockers_removed_since_oldest) =
        compare_blocker_sets(valid_packets.first(), valid_packets.last());

    let recent_verdict_progression = if valid_packets.is_empty() {
        Vec::new()
    } else if config.latest {
        vec![packet_snapshot_summary(
            valid_packets.last().expect("latest packet"),
        )]
    } else {
        let start = valid_packets.len().saturating_sub(config.limit);
        valid_packets[start..]
            .iter()
            .map(packet_snapshot_summary)
            .collect()
    };

    let (verdict, reason) = if !invalid_artifacts.is_empty() {
        (
            DecisionHistoryVerdict::DecisionHistoryInvalidArtifact,
            format!(
                "history contains {} invalid artifact(s); review invalid_artifacts before trusting the latest packet trend",
                invalid_artifacts.len()
            ),
        )
    } else if latest_packet.is_none() {
        (
            DecisionHistoryVerdict::DecisionHistoryInsufficientPackets,
            "no valid activation decision packet artifacts were found".to_string(),
        )
    } else {
        let latest = latest_packet.as_ref().expect("latest packet");
        match latest.packet.verdict {
            DecisionPacketVerdict::DecisionPacketBlocked => (
                DecisionHistoryVerdict::DecisionHistoryLatestBlocked,
                format!(
                    "latest activation decision packet remains blocked: {}",
                    latest.packet.reason
                ),
            ),
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized => (
                DecisionHistoryVerdict::DecisionHistoryLatestDiscussionReady,
                "latest activation decision packet is discussion-ready but still not authorized"
                    .to_string(),
            ),
            DecisionPacketVerdict::DecisionPacketRefusedForProfileMismatch => (
                DecisionHistoryVerdict::DecisionHistoryLatestRefusedForProfileMismatch,
                format!(
                    "latest activation decision packet is refused for profile mismatch: {}",
                    latest.packet.reason
                ),
            ),
        }
    };

    Ok(DecisionHistorySummaryReport {
        mode: "history".to_string(),
        verdict,
        reason,
        history_dir: config
            .history_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        packet_paths_examined: packet_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        latest_only_requested: config.latest,
        limit: config.limit,
        packets_loaded: valid_packets.len(),
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        blocked_count,
        discussion_ready_count,
        refused_count,
        latest_packet: latest_packet.as_ref().map(packet_snapshot_summary),
        latest_prod_pre_activation_gate: latest_packet
            .as_ref()
            .map(|record| record.packet.prod_pre_activation_gate.clone()),
        latest_launch_dossier: latest_packet
            .as_ref()
            .map(|record| record.packet.launch_dossier.clone()),
        latest_tiny_live_guardrails: latest_packet
            .as_ref()
            .map(|record| record.packet.tiny_live_guardrails.clone()),
        latest_non_prod_readiness: latest_packet
            .as_ref()
            .map(|record| record.packet.non_prod_readiness.clone()),
        latest_execution_enabled: latest_packet
            .as_ref()
            .map(|record| record.packet.execution_enabled),
        prod_config_fingerprint_changed_over_history,
        non_prod_config_fingerprint_changed_over_history,
        blockers_trend,
        blockers_added_since_oldest,
        blockers_removed_since_oldest,
        recent_verdict_progression,
        artifact_analysis_only: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact analysis only. This history report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn build_compare_report(older: &Path, newer: &Path) -> Result<DecisionHistoryDiffReport> {
    let older_result = load_single_artifact(older);
    let newer_result = load_single_artifact(newer);
    let mut invalid_artifacts = Vec::new();
    let older_loaded = match older_result {
        Ok(packet) => Some(packet),
        Err(error) => {
            invalid_artifacts.push(InvalidArtifact {
                path: older.display().to_string(),
                error: format!("{error:#}"),
            });
            None
        }
    };
    let newer_loaded = match newer_result {
        Ok(packet) => Some(packet),
        Err(error) => {
            invalid_artifacts.push(InvalidArtifact {
                path: newer.display().to_string(),
                error: format!("{error:#}"),
            });
            None
        }
    };

    if !invalid_artifacts.is_empty() {
        return Ok(DecisionHistoryDiffReport {
            mode: "compare".to_string(),
            verdict: DecisionHistoryVerdict::DecisionHistoryInvalidArtifact,
            reason: "one or more compared artifacts are invalid activation decision packets".to_string(),
            older_path: older.display().to_string(),
            newer_path: newer.display().to_string(),
            invalid_artifacts,
            older_generated_at: None,
            newer_generated_at: None,
            older_verdict: None,
            newer_verdict: None,
            older_reason: None,
            newer_reason: None,
            checklist_verdict_changed: false,
            older_checklist_verdict: None,
            newer_checklist_verdict: None,
            blockers_added: Vec::new(),
            blockers_removed: Vec::new(),
            warnings_added: Vec::new(),
            warnings_removed: Vec::new(),
            prod_pre_activation_gate_verdict_changed: false,
            older_prod_pre_activation_gate_verdict: None,
            newer_prod_pre_activation_gate_verdict: None,
            prod_pre_activation_gate_reason_changed: false,
            older_prod_pre_activation_gate_reason: None,
            newer_prod_pre_activation_gate_reason: None,
            launch_dossier_verdict_changed: false,
            older_launch_dossier_verdict: None,
            newer_launch_dossier_verdict: None,
            launch_dossier_reason_changed: false,
            older_launch_dossier_reason: None,
            newer_launch_dossier_reason: None,
            tiny_live_guardrails_verdict_changed: false,
            older_tiny_live_guardrails_verdict: None,
            newer_tiny_live_guardrails_verdict: None,
            tiny_live_guardrails_reason_changed: false,
            older_tiny_live_guardrails_reason: None,
            newer_tiny_live_guardrails_reason: None,
            non_prod_readiness_verdict_changed: false,
            older_non_prod_readiness_verdict: None,
            newer_non_prod_readiness_verdict: None,
            non_prod_readiness_reason_changed: false,
            older_non_prod_readiness_reason: None,
            newer_non_prod_readiness_reason: None,
            prod_config_fingerprint_changed: false,
            older_prod_config_fingerprint_sha256: None,
            newer_prod_config_fingerprint_sha256: None,
            non_prod_config_fingerprint_changed: false,
            older_non_prod_config_fingerprint_sha256: None,
            newer_non_prod_config_fingerprint_sha256: None,
            build_version_changed: false,
            older_build_version: None,
            newer_build_version: None,
            git_commit_changed: false,
            older_git_commit: None,
            newer_git_commit: None,
            artifact_analysis_only: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact analysis only. This diff report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let older_loaded = older_loaded.expect("older loaded");
    let newer_loaded = newer_loaded.expect("newer loaded");
    let blockers_added =
        set_difference(&older_loaded.packet.blockers, &newer_loaded.packet.blockers);
    let blockers_removed =
        set_difference(&newer_loaded.packet.blockers, &older_loaded.packet.blockers);
    let warnings_added =
        set_difference(&older_loaded.packet.warnings, &newer_loaded.packet.warnings);
    let warnings_removed =
        set_difference(&newer_loaded.packet.warnings, &older_loaded.packet.warnings);

    Ok(DecisionHistoryDiffReport {
        mode: "compare".to_string(),
        verdict: DecisionHistoryVerdict::DecisionHistoryCompareReady,
        reason: "decision packet diff is ready for operator review".to_string(),
        older_path: older_loaded.path.display().to_string(),
        newer_path: newer_loaded.path.display().to_string(),
        invalid_artifacts,
        older_generated_at: Some(older_loaded.packet.generated_at),
        newer_generated_at: Some(newer_loaded.packet.generated_at),
        older_verdict: Some(serialize_enum(&older_loaded.packet.verdict)),
        newer_verdict: Some(serialize_enum(&newer_loaded.packet.verdict)),
        older_reason: Some(older_loaded.packet.reason.clone()),
        newer_reason: Some(newer_loaded.packet.reason.clone()),
        checklist_verdict_changed: older_loaded.packet.checklist_verdict
            != newer_loaded.packet.checklist_verdict,
        older_checklist_verdict: Some(older_loaded.packet.checklist_verdict.clone()),
        newer_checklist_verdict: Some(newer_loaded.packet.checklist_verdict.clone()),
        blockers_added,
        blockers_removed,
        warnings_added,
        warnings_removed,
        prod_pre_activation_gate_verdict_changed: older_loaded.packet.prod_pre_activation_gate.verdict
            != newer_loaded.packet.prod_pre_activation_gate.verdict,
        older_prod_pre_activation_gate_verdict: Some(
            older_loaded.packet.prod_pre_activation_gate.verdict.clone(),
        ),
        newer_prod_pre_activation_gate_verdict: Some(
            newer_loaded.packet.prod_pre_activation_gate.verdict.clone(),
        ),
        prod_pre_activation_gate_reason_changed: older_loaded.packet.prod_pre_activation_gate.reason
            != newer_loaded.packet.prod_pre_activation_gate.reason,
        older_prod_pre_activation_gate_reason: Some(
            older_loaded.packet.prod_pre_activation_gate.reason.clone(),
        ),
        newer_prod_pre_activation_gate_reason: Some(
            newer_loaded.packet.prod_pre_activation_gate.reason.clone(),
        ),
        launch_dossier_verdict_changed: older_loaded.packet.launch_dossier.verdict
            != newer_loaded.packet.launch_dossier.verdict,
        older_launch_dossier_verdict: Some(older_loaded.packet.launch_dossier.verdict.clone()),
        newer_launch_dossier_verdict: Some(newer_loaded.packet.launch_dossier.verdict.clone()),
        launch_dossier_reason_changed: older_loaded.packet.launch_dossier.reason
            != newer_loaded.packet.launch_dossier.reason,
        older_launch_dossier_reason: Some(older_loaded.packet.launch_dossier.reason.clone()),
        newer_launch_dossier_reason: Some(newer_loaded.packet.launch_dossier.reason.clone()),
        tiny_live_guardrails_verdict_changed: older_loaded.packet.tiny_live_guardrails.verdict
            != newer_loaded.packet.tiny_live_guardrails.verdict,
        older_tiny_live_guardrails_verdict: Some(
            older_loaded.packet.tiny_live_guardrails.verdict.clone(),
        ),
        newer_tiny_live_guardrails_verdict: Some(
            newer_loaded.packet.tiny_live_guardrails.verdict.clone(),
        ),
        tiny_live_guardrails_reason_changed: older_loaded.packet.tiny_live_guardrails.reason
            != newer_loaded.packet.tiny_live_guardrails.reason,
        older_tiny_live_guardrails_reason: Some(
            older_loaded.packet.tiny_live_guardrails.reason.clone(),
        ),
        newer_tiny_live_guardrails_reason: Some(
            newer_loaded.packet.tiny_live_guardrails.reason.clone(),
        ),
        non_prod_readiness_verdict_changed: older_loaded.packet.non_prod_readiness.verdict
            != newer_loaded.packet.non_prod_readiness.verdict,
        older_non_prod_readiness_verdict: Some(
            older_loaded.packet.non_prod_readiness.verdict.clone(),
        ),
        newer_non_prod_readiness_verdict: Some(
            newer_loaded.packet.non_prod_readiness.verdict.clone(),
        ),
        non_prod_readiness_reason_changed: older_loaded.packet.non_prod_readiness.reason
            != newer_loaded.packet.non_prod_readiness.reason,
        older_non_prod_readiness_reason: Some(
            older_loaded.packet.non_prod_readiness.reason.clone(),
        ),
        newer_non_prod_readiness_reason: Some(
            newer_loaded.packet.non_prod_readiness.reason.clone(),
        ),
        prod_config_fingerprint_changed: older_loaded.packet.prod_config_fingerprint.sha256
            != newer_loaded.packet.prod_config_fingerprint.sha256,
        older_prod_config_fingerprint_sha256: Some(
            older_loaded.packet.prod_config_fingerprint.sha256.clone(),
        ),
        newer_prod_config_fingerprint_sha256: Some(
            newer_loaded.packet.prod_config_fingerprint.sha256.clone(),
        ),
        non_prod_config_fingerprint_changed: older_loaded.packet.non_prod_config_fingerprint.sha256
            != newer_loaded.packet.non_prod_config_fingerprint.sha256,
        older_non_prod_config_fingerprint_sha256: Some(
            older_loaded.packet.non_prod_config_fingerprint.sha256.clone(),
        ),
        newer_non_prod_config_fingerprint_sha256: Some(
            newer_loaded.packet.non_prod_config_fingerprint.sha256.clone(),
        ),
        build_version_changed: older_loaded.packet.build_version != newer_loaded.packet.build_version,
        older_build_version: Some(older_loaded.packet.build_version.clone()),
        newer_build_version: Some(newer_loaded.packet.build_version.clone()),
        git_commit_changed: older_loaded.packet.git_commit != newer_loaded.packet.git_commit,
        older_git_commit: older_loaded.packet.git_commit.clone(),
        newer_git_commit: newer_loaded.packet.git_commit.clone(),
        artifact_analysis_only: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact analysis only. This diff report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn collect_history_paths(config: &Config) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if let Some(dir) = &config.history_dir {
        let entries = fs::read_dir(dir)
            .with_context(|| format!("failed reading history dir {}", dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("failed reading entry in history dir {}", dir.display())
            })?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "json") {
                paths.push(path);
            }
        }
    }
    paths.extend(config.packet_paths.iter().cloned());
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn load_artifacts(paths: &[PathBuf]) -> (Vec<LoadedPacket>, Vec<InvalidArtifact>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match load_single_artifact(path) {
            Ok(packet) => valid.push(packet),
            Err(error) => invalid.push(InvalidArtifact {
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn load_single_artifact(path: &Path) -> Result<LoadedPacket> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading decision packet artifact {}", path.display()))?;
    let packet: DecisionPacketArtifact = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing decision packet artifact {}", path.display()))?;
    validate_packet(path, &packet)?;
    Ok(LoadedPacket {
        path: path.to_path_buf(),
        packet,
    })
}

fn validate_packet(path: &Path, packet: &DecisionPacketArtifact) -> Result<()> {
    if packet.packet_version.trim().is_empty() {
        bail!("{} has empty packet_version", path.display());
    }
    if !packet.read_only_packet {
        bail!(
            "{} is not a planning-only activation decision packet (read_only_packet=false)",
            path.display()
        );
    }
    if packet.activation_authorized {
        bail!(
            "{} is not a planning-only activation decision packet (activation_authorized=true)",
            path.display()
        );
    }
    Ok(())
}

fn packet_snapshot_summary(record: &LoadedPacket) -> PacketSnapshotSummary {
    PacketSnapshotSummary {
        path: record.path.display().to_string(),
        generated_at: record.packet.generated_at,
        verdict: serialize_enum(&record.packet.verdict),
        reason: record.packet.reason.clone(),
        blocker_count: record.packet.blockers.len(),
        warning_count: record.packet.warnings.len(),
        prod_config_fingerprint_sha256: record.packet.prod_config_fingerprint.sha256.clone(),
        non_prod_config_fingerprint_sha256: record
            .packet
            .non_prod_config_fingerprint
            .sha256
            .clone(),
    }
}

fn compare_blocker_sets(
    oldest: Option<&LoadedPacket>,
    latest: Option<&LoadedPacket>,
) -> (BlockerTrend, Vec<String>, Vec<String>) {
    let Some(oldest) = oldest else {
        return (BlockerTrend::Unknown, Vec::new(), Vec::new());
    };
    let Some(latest) = latest else {
        return (BlockerTrend::Unknown, Vec::new(), Vec::new());
    };
    let added = set_difference(&oldest.packet.blockers, &latest.packet.blockers);
    let removed = set_difference(&latest.packet.blockers, &oldest.packet.blockers);
    let trend = if added.is_empty() && removed.is_empty() {
        BlockerTrend::Unchanged
    } else if !added.is_empty() && removed.is_empty() {
        BlockerTrend::Wider
    } else if added.is_empty() && !removed.is_empty() {
        BlockerTrend::Narrower
    } else {
        BlockerTrend::Mixed
    };
    (trend, added, removed)
}

fn set_difference(left: &[String], right: &[String]) -> Vec<String> {
    let left = left.iter().cloned().collect::<BTreeSet<_>>();
    let right = right.iter().cloned().collect::<BTreeSet<_>>();
    right.difference(&left).cloned().collect()
}

fn unique_count<'a, I>(values: I) -> usize
where
    I: Iterator<Item = &'a str>,
{
    values.collect::<BTreeSet<_>>().len()
}

fn render_history_human(report: &DecisionHistorySummaryReport) -> String {
    let progression = report
        .recent_verdict_progression
        .iter()
        .map(|entry| format!("{}:{}", entry.generated_at.to_rfc3339(), entry.verdict))
        .collect::<Vec<_>>()
        .join(" -> ");
    let invalid = report
        .invalid_artifacts
        .iter()
        .map(|artifact| format!("{}:{}", artifact.path, artifact.error))
        .collect::<Vec<_>>()
        .join(" | ");

    [
        "event=copybot_activation_decision_history_report".to_string(),
        "mode=history".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("packets_loaded={}", report.packets_loaded),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!("blocked_count={}", report.blocked_count),
        format!("discussion_ready_count={}", report.discussion_ready_count),
        format!("refused_count={}", report.refused_count),
        format!(
            "latest_packet_verdict={}",
            report
                .latest_packet
                .as_ref()
                .map(|packet| packet.verdict.clone())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_prod_pre_activation_gate_verdict={}",
            report
                .latest_prod_pre_activation_gate
                .as_ref()
                .map(|summary| summary.verdict.clone())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_non_prod_readiness_verdict={}",
            report
                .latest_non_prod_readiness
                .as_ref()
                .map(|summary| summary.verdict.clone())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "prod_config_fingerprint_changed_over_history={}",
            report.prod_config_fingerprint_changed_over_history
        ),
        format!(
            "non_prod_config_fingerprint_changed_over_history={}",
            report.non_prod_config_fingerprint_changed_over_history
        ),
        format!("blockers_trend={}", serialize_enum(&report.blockers_trend)),
        format!(
            "blockers_added_since_oldest={}",
            report.blockers_added_since_oldest.join(" | ")
        ),
        format!(
            "blockers_removed_since_oldest={}",
            report.blockers_removed_since_oldest.join(" | ")
        ),
        format!("recent_verdict_progression={progression}"),
        format!("invalid_artifacts={invalid}"),
        format!("artifact_analysis_only={}", report.artifact_analysis_only),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_compare_human(report: &DecisionHistoryDiffReport) -> String {
    let invalid = report
        .invalid_artifacts
        .iter()
        .map(|artifact| format!("{}:{}", artifact.path, artifact.error))
        .collect::<Vec<_>>()
        .join(" | ");

    [
        "event=copybot_activation_decision_history_report".to_string(),
        "mode=compare".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("older_path={}", report.older_path),
        format!("newer_path={}", report.newer_path),
        format!(
            "older_verdict={}",
            report
                .older_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "newer_verdict={}",
            report
                .newer_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("blockers_added={}", report.blockers_added.join(" | ")),
        format!("blockers_removed={}", report.blockers_removed.join(" | ")),
        format!("warnings_added={}", report.warnings_added.join(" | ")),
        format!("warnings_removed={}", report.warnings_removed.join(" | ")),
        format!(
            "prod_pre_activation_gate_reason_changed={}",
            report.prod_pre_activation_gate_reason_changed
        ),
        format!(
            "launch_dossier_verdict_changed={}",
            report.launch_dossier_verdict_changed
        ),
        format!(
            "tiny_live_guardrails_verdict_changed={}",
            report.tiny_live_guardrails_verdict_changed
        ),
        format!(
            "non_prod_readiness_verdict_changed={}",
            report.non_prod_readiness_verdict_changed
        ),
        format!(
            "prod_config_fingerprint_changed={}",
            report.prod_config_fingerprint_changed
        ),
        format!(
            "non_prod_config_fingerprint_changed={}",
            report.non_prod_config_fingerprint_changed
        ),
        format!("build_version_changed={}", report.build_version_changed),
        format!("git_commit_changed={}", report.git_commit_changed),
        format!("invalid_artifacts={invalid}"),
        format!("artifact_analysis_only={}", report.artifact_analysis_only),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
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
    fn empty_history_yields_insufficient_packets() {
        let dir = temp_dir("activation_history_empty");
        let report = build_history_report(&Config {
            history_dir: Some(dir.clone()),
            packet_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            json: false,
        })
        .expect("history");

        assert_eq!(
            report.verdict,
            DecisionHistoryVerdict::DecisionHistoryInsufficientPackets
        );
        assert_eq!(report.packets_loaded, 0);
    }

    #[test]
    fn valid_packet_series_yields_latest_verdict_summary() {
        let dir = temp_dir("activation_history_series");
        write_packet(
            &dir.join("20260326T100000Z.json"),
            sample_packet(
                "2026-03-26T10:00:00Z",
                DecisionPacketVerdict::DecisionPacketBlocked,
                vec!["stage3: insufficient recent evidence"],
                vec!["still planning-only"],
                "prod_a",
                "nonprod_a",
            ),
        );
        write_packet(
            &dir.join("20260326T120000Z.json"),
            sample_packet(
                "2026-03-26T12:00:00Z",
                DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
                vec![],
                vec!["still planning-only"],
                "prod_b",
                "nonprod_a",
            ),
        );

        let report = build_history_report(&Config {
            history_dir: Some(dir.clone()),
            packet_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            json: false,
        })
        .expect("history");

        assert_eq!(
            report.verdict,
            DecisionHistoryVerdict::DecisionHistoryLatestDiscussionReady
        );
        assert_eq!(report.blocked_count, 1);
        assert_eq!(report.discussion_ready_count, 1);
        assert!(report.prod_config_fingerprint_changed_over_history);
        assert_eq!(report.blockers_trend, BlockerTrend::Narrower);
        assert_eq!(
            report.latest_packet.as_ref().expect("latest").verdict,
            "decision_packet_discussion_ready_but_not_authorized"
        );
    }

    #[test]
    fn diff_mode_shows_blocker_additions_and_removals() {
        let dir = temp_dir("activation_history_diff");
        let older = dir.join("older.json");
        let newer = dir.join("newer.json");
        write_packet(
            &older,
            sample_packet(
                "2026-03-26T10:00:00Z",
                DecisionPacketVerdict::DecisionPacketBlocked,
                vec!["stage3: insufficient recent evidence", "non_prod: stale"],
                vec!["warn_a"],
                "prod_a",
                "nonprod_a",
            ),
        );
        write_packet(
            &newer,
            sample_packet(
                "2026-03-26T12:00:00Z",
                DecisionPacketVerdict::DecisionPacketBlocked,
                vec![
                    "stage3: insufficient recent evidence",
                    "launch: overlay drift",
                ],
                vec!["warn_b"],
                "prod_b",
                "nonprod_a",
            ),
        );

        let report = build_compare_report(&older, &newer).expect("diff");

        assert_eq!(
            report.verdict,
            DecisionHistoryVerdict::DecisionHistoryCompareReady
        );
        assert_eq!(
            report.blockers_added,
            vec!["launch: overlay drift".to_string()]
        );
        assert_eq!(report.blockers_removed, vec!["non_prod: stale".to_string()]);
        assert_eq!(report.warnings_added, vec!["warn_b".to_string()]);
        assert_eq!(report.warnings_removed, vec!["warn_a".to_string()]);
    }

    #[test]
    fn fingerprint_changes_are_detected() {
        let dir = temp_dir("activation_history_fingerprint");
        write_packet(
            &dir.join("older.json"),
            sample_packet(
                "2026-03-26T10:00:00Z",
                DecisionPacketVerdict::DecisionPacketBlocked,
                vec!["stage3: insufficient recent evidence"],
                vec![],
                "prod_a",
                "nonprod_a",
            ),
        );
        write_packet(
            &dir.join("newer.json"),
            sample_packet(
                "2026-03-26T12:00:00Z",
                DecisionPacketVerdict::DecisionPacketBlocked,
                vec!["stage3: insufficient recent evidence"],
                vec![],
                "prod_b",
                "nonprod_c",
            ),
        );

        let report = build_history_report(&Config {
            history_dir: Some(dir),
            packet_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            json: false,
        })
        .expect("history");

        assert!(report.prod_config_fingerprint_changed_over_history);
        assert!(report.non_prod_config_fingerprint_changed_over_history);
    }

    #[test]
    fn malformed_artifact_is_reported_as_invalid() {
        let dir = temp_dir("activation_history_invalid");
        fs::write(dir.join("broken.json"), "{not-json").expect("write invalid");

        let report = build_history_report(&Config {
            history_dir: Some(dir),
            packet_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            json: false,
        })
        .expect("history");

        assert_eq!(
            report.verdict,
            DecisionHistoryVerdict::DecisionHistoryInvalidArtifact
        );
        assert_eq!(report.invalid_artifact_count, 1);
    }

    #[test]
    fn execution_and_source_artifacts_are_not_mutated() {
        let dir = temp_dir("activation_history_no_mutation");
        let packet_path = dir.join("packet.json");
        write_packet(
            &packet_path,
            sample_packet(
                "2026-03-26T10:00:00Z",
                DecisionPacketVerdict::DecisionPacketBlocked,
                vec!["stage3: insufficient recent evidence"],
                vec![],
                "prod_a",
                "nonprod_a",
            ),
        );
        let before = fs::read_to_string(&packet_path).expect("before");

        let _ = build_history_report(&Config {
            history_dir: None,
            packet_paths: vec![packet_path.clone()],
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            json: false,
        })
        .expect("history");

        let after = fs::read_to_string(&packet_path).expect("after");
        assert_eq!(before, after);
    }

    fn sample_packet(
        generated_at: &str,
        verdict: DecisionPacketVerdict,
        blockers: Vec<&str>,
        warnings: Vec<&str>,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> serde_json::Value {
        let reason = match verdict {
            DecisionPacketVerdict::DecisionPacketBlocked => {
                "prod gate is still blocked".to_string()
            }
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized => {
                "discussion ready but not authorized".to_string()
            }
            DecisionPacketVerdict::DecisionPacketRefusedForProfileMismatch => {
                "prod profile mismatch".to_string()
            }
        };

        serde_json::json!({
            "generated_at": generated_at,
            "packet_version": "1",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "operator_note": null,
            "execution_enabled": false,
            "read_only_packet": true,
            "activation_authorized": false,
            "discussion_ready_only": verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": serialize_enum(&verdict),
            "reason": reason,
            "blockers": blockers,
            "warnings": warnings,
            "checklist_verdict": "activation_checklist_blocked_by_prod_stage3",
            "checklist_reason": "stage3 gate is blocked",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "prod_pre_activation_gate": {
                "verdict": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "pre_activation_gates_green" } else { "blocked_by_stage3" },
                "reason": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "prod gate green" } else { "stage3 blocked" },
                "planning_green": verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
                "blocked_by_stage3": verdict != DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
                "stage3_verdict": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "fresh_current" } else { "insufficient_raw_truth" },
                "stage3_reason": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "fresh current" } else { "recent captures missing" },
                "stage3_captures_within_recent_horizon": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { 3 } else { 0 },
                "stage3_latest_capture_age_seconds": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { 300 } else { 3600 },
                "stage4_readiness_verdict": "ready_for_execution_dry_run",
                "stage4_rehearsal_history_verdict": "sufficient_recent_rehearsal_evidence",
                "tiny_live_policy_verdict": "tiny_live_policy_bounded",
                "blockers": blockers
            },
            "launch_dossier": {
                "verdict": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "activation_plan_ready_when_stage_gate_allows" } else { "blocked_by_pre_activation_gate" },
                "reason": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "launch dossier is bounded" } else { "launch dossier blocked by prod gate" },
                "ready_when_stage_gate_allows": verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
                "activation_overlay_complete": true,
                "rollback_plan_complete": true,
                "service_restart_contract_complete": true,
                "activation_overlay_change_count": 3,
                "rollback_overlay_change_count": 3,
                "drift_finding_count": 0,
                "blocker_count": blockers.len(),
                "first_blocker": blockers.first().cloned()
            },
            "tiny_live_guardrails": {
                "verdict": "tiny_live_guardrails_bounded",
                "reason": "guardrails bounded",
                "bounded": true,
                "enabled": true,
                "blocker_count": 0,
                "first_blocker": null,
                "rollback_trigger_count": 1,
                "rollback_triggers": [{
                    "trigger": "consecutive_hard_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 600,
                    "action": "rollback_now"
                }]
            },
            "non_prod_readiness": {
                "verdict": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "devnet_readiness_green" } else { "devnet_readiness_insufficient_recent_evidence" },
                "reason": if verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized { "recent non-prod evidence is green" } else { "missing recent non-prod evidence" },
                "green": verdict == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
                "prod_profile_refused": false,
                "config_env": "devnet",
                "blockers": [],
                "warnings": [],
                "dress_latest_record_age_seconds": 120,
                "dress_recent_green_count": 3,
                "activation_latest_record_age_seconds": 120,
                "activation_recent_green_count": 3,
                "activation_recent_rollback_success_count": 3,
                "activation_recent_internal_consistency_count": 3,
                "stale_evidence_excluded": false
            }
        })
    }

    fn write_packet(path: &Path, value: serde_json::Value) {
        fs::write(
            path,
            serde_json::to_string_pretty(&value).expect("serialize"),
        )
        .expect("write packet");
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{}_{}", prefix, nanos));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
