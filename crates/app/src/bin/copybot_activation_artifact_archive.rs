use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_activation_artifact_archive --archive-dir <path> [--json] [--retention-plan --keep-latest <count> | --retention-apply --keep-latest <count>]";
const DEFAULT_KEEP_LATEST: usize = 10;
const RUNBOOK_MARKDOWN_TITLE: &str = "# Tiny-Live Activation Runbook";

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
    archive_dir: PathBuf,
    json: bool,
    retention_plan: bool,
    retention_apply: bool,
    keep_latest: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArchiveVerdict {
    ArchiveHealthOk,
    ArchiveHealthMissingPairings,
    ArchiveHealthInvalidArtifactsPresent,
    ArchiveRetentionPlanReady,
    ArchiveRetentionPlanInsufficientArtifacts,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArchiveCleanupVerdict {
    ArchiveCleanupApplied,
    ArchiveCleanupBlockedByInvalidArtifacts,
    ArchiveCleanupNothingToDo,
    ArchiveCleanupFailedPartial,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DecisionPacketVerdict {
    DecisionPacketBlocked,
    DecisionPacketDiscussionReadyButNotAuthorized,
    DecisionPacketRefusedForProfileMismatch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ActivationRunbookVerdict {
    RunbookBlocked,
    RunbookDiscussionReadyButNotAuthorized,
    RunbookRefusedForProfileMismatch,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivationRunbookArtifact {
    generated_at: DateTime<Utc>,
    runbook_version: String,
    prod_config_path: String,
    non_prod_config_path: String,
    json_output_path: Option<String>,
    markdown_output_path: Option<String>,
    execution_enabled: bool,
    read_only_runbook: bool,
    activation_authorized: bool,
    discussion_ready_only: bool,
    prod_stage3_remains_hard_gate: bool,
    non_prod_evidence_is_secondary: bool,
    verdict: ActivationRunbookVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    not_authorized_disclaimer: String,
    decision_packet_version: String,
    decision_packet_generated_at: DateTime<Utc>,
    decision_packet_verdict: String,
    decision_packet_reason: String,
    build_version: String,
    git_commit: Option<String>,
    prod_config_fingerprint: ConfigFingerprintSummary,
    non_prod_config_fingerprint: ConfigFingerprintSummary,
    prod_pre_activation_gate: ProdPreActivationGateSummary,
    launch_dossier: LaunchDossierSummary,
    tiny_live_guardrails: GuardrailSummary,
    non_prod_readiness: NonProdReadinessSummary,
    section_order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ArtifactGenerationKey {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone)]
struct LoadedPacketArtifact {
    path: PathBuf,
    packet: DecisionPacketArtifact,
}

#[derive(Debug, Clone)]
struct LoadedRunbookArtifact {
    path: PathBuf,
    runbook: ActivationRunbookArtifact,
}

#[derive(Debug, Clone)]
struct LoadedMarkdownArtifact {
    path: PathBuf,
    _title_line: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InvalidArtifact {
    pub(crate) path: String,
    pub(crate) error: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArchiveArtifactGenerationSummary {
    pub(crate) decision_packet_generated_at: DateTime<Utc>,
    pub(crate) prod_config_fingerprint_sha256: String,
    pub(crate) non_prod_config_fingerprint_sha256: String,
    pub(crate) decision_packet_paths: Vec<String>,
    pub(crate) runbook_json_paths: Vec<String>,
    pub(crate) runbook_markdown_paths: Vec<String>,
    pub(crate) latest_packet_verdict: Option<String>,
    pub(crate) latest_runbook_verdict: Option<String>,
    pub(crate) build_versions: Vec<String>,
    pub(crate) git_commits: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArchiveLatestArtifactSummary {
    path: String,
    generated_at: DateTime<Utc>,
    verdict: String,
    build_version: String,
    git_commit: Option<String>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArchiveIndexReport {
    mode: String,
    verdict: ArchiveVerdict,
    reason: String,
    archive_dir: String,
    packet_json_count: usize,
    runbook_json_count: usize,
    runbook_markdown_count: usize,
    paired_generation_count: usize,
    missing_runbook_for_packet_count: usize,
    missing_packet_for_runbook_count: usize,
    missing_markdown_for_runbook_count: usize,
    orphan_markdown_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    latest_packet: Option<ArchiveLatestArtifactSummary>,
    latest_runbook: Option<ArchiveLatestArtifactSummary>,
    unique_prod_config_fingerprints: usize,
    unique_non_prod_config_fingerprints: usize,
    unique_build_versions: usize,
    unique_git_commits: usize,
    generations: Vec<ArchiveArtifactGenerationSummary>,
    missing_runbook_for_packet: Vec<ArchiveArtifactGenerationSummary>,
    missing_packet_for_runbook: Vec<ArchiveArtifactGenerationSummary>,
    missing_markdown_for_runbook: Vec<ArchiveArtifactGenerationSummary>,
    orphan_markdown_paths: Vec<String>,
    read_only_archive_analysis: bool,
    deletes_performed: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArchiveRetentionGenerationPlan {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
    latest_packet_verdict: Option<String>,
    latest_runbook_verdict: Option<String>,
    would_keep: bool,
    reason: String,
    decision_packet_paths: Vec<String>,
    runbook_json_paths: Vec<String>,
    runbook_markdown_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArchiveRetentionPlanReport {
    mode: String,
    verdict: ArchiveVerdict,
    reason: String,
    archive_dir: String,
    keep_latest: usize,
    total_generation_count: usize,
    generations_to_keep: usize,
    generations_to_remove: usize,
    generations: Vec<ArchiveRetentionGenerationPlan>,
    orphan_runbook_generations: Vec<ArchiveArtifactGenerationSummary>,
    packet_generations_missing_runbook: Vec<ArchiveArtifactGenerationSummary>,
    orphan_markdown_paths: Vec<String>,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    read_only_archive_analysis: bool,
    deletes_performed: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactRemovalFailure {
    path: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArchiveCleanupApplyReport {
    mode: String,
    verdict: ArchiveCleanupVerdict,
    reason: String,
    archive_dir: String,
    keep_latest: usize,
    generations_to_keep: usize,
    generations_to_remove: usize,
    kept_generations: Vec<ArchiveRetentionGenerationPlan>,
    removed_generations: Vec<ArchiveRetentionGenerationPlan>,
    removed_file_paths: Vec<String>,
    failed_removals: Vec<ArtifactRemovalFailure>,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    orphan_runbook_generations: Vec<ArchiveArtifactGenerationSummary>,
    packet_generations_missing_runbook: Vec<ArchiveArtifactGenerationSummary>,
    orphan_markdown_paths: Vec<String>,
    read_only_archive_analysis: bool,
    deletes_performed: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Default, Clone)]
struct ArchiveScan {
    packets: Vec<LoadedPacketArtifact>,
    runbook_json: Vec<LoadedRunbookArtifact>,
    runbook_markdown: Vec<LoadedMarkdownArtifact>,
    invalid_artifacts: Vec<InvalidArtifact>,
}

#[derive(Debug, Default, Clone)]
struct GenerationRecord {
    packets: Vec<LoadedPacketArtifact>,
    runbook_json: Vec<LoadedRunbookArtifact>,
    runbook_markdown_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct RetentionSelection {
    scan: ArchiveScan,
    generations: Vec<ArchiveRetentionGenerationPlan>,
    orphan_runbook_generations: Vec<ArchiveArtifactGenerationSummary>,
    packet_generations_missing_runbook: Vec<ArchiveArtifactGenerationSummary>,
    orphan_markdown_paths: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ArchiveInventory {
    pub(crate) generation_summaries: Vec<ArchiveArtifactGenerationSummary>,
    pub(crate) invalid_artifacts: Vec<InvalidArtifact>,
    pub(crate) orphan_markdown_paths: Vec<String>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut archive_dir: Option<PathBuf> = None;
    let mut json = false;
    let mut retention_plan = false;
    let mut retention_apply = false;
    let mut keep_latest = DEFAULT_KEEP_LATEST;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--archive-dir" => {
                archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--archive-dir",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--retention-plan" => retention_plan = true,
            "--retention-apply" => retention_apply = true,
            "--keep-latest" => keep_latest = parse_usize_arg("--keep-latest", args.next())?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if retention_plan && retention_apply {
        bail!("--retention-plan and --retention-apply cannot be combined");
    }

    Ok(Some(Config {
        archive_dir: archive_dir.ok_or_else(|| anyhow!("missing required --archive-dir"))?,
        json,
        retention_plan,
        retention_apply,
        keep_latest: keep_latest.max(1),
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
    if config.retention_apply {
        let report = apply_retention_plan(&config)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing activation artifact cleanup apply json")
        } else {
            Ok(render_cleanup_apply_human(&report))
        }
    } else if config.retention_plan {
        let report = build_retention_plan_report(&config)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing activation artifact retention plan json")
        } else {
            Ok(render_retention_plan_human(&report))
        }
    } else {
        let report = build_index_report(&config)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing activation artifact archive report json")
        } else {
            Ok(render_index_human(&report))
        }
    }
}

fn build_index_report(config: &Config) -> Result<ArchiveIndexReport> {
    let scan = scan_archive(&config.archive_dir)?;
    let generations = build_generation_records(&scan);
    let mut generation_summaries = generation_summaries(&generations);
    generation_summaries.sort_by(|left, right| {
        left.decision_packet_generated_at
            .cmp(&right.decision_packet_generated_at)
            .reverse()
    });

    let missing_runbook_for_packet = generation_summaries
        .iter()
        .filter(|summary| {
            !summary.decision_packet_paths.is_empty() && summary.runbook_json_paths.is_empty()
        })
        .cloned()
        .collect::<Vec<_>>();
    let missing_packet_for_runbook = generation_summaries
        .iter()
        .filter(|summary| {
            summary.decision_packet_paths.is_empty() && !summary.runbook_json_paths.is_empty()
        })
        .cloned()
        .collect::<Vec<_>>();
    let missing_markdown_for_runbook = generation_summaries
        .iter()
        .filter(|summary| {
            !summary.runbook_json_paths.is_empty() && summary.runbook_markdown_paths.is_empty()
        })
        .cloned()
        .collect::<Vec<_>>();

    let latest_packet = scan
        .packets
        .iter()
        .max_by_key(|record| record.packet.generated_at)
        .map(packet_summary);
    let latest_runbook = scan
        .runbook_json
        .iter()
        .max_by_key(|record| record.runbook.generated_at)
        .map(runbook_summary);

    let unique_prod_config_fingerprints = scan
        .packets
        .iter()
        .map(|record| record.packet.prod_config_fingerprint.sha256.as_str())
        .chain(
            scan.runbook_json
                .iter()
                .map(|record| record.runbook.prod_config_fingerprint.sha256.as_str()),
        )
        .collect::<BTreeSet<_>>()
        .len();
    let unique_non_prod_config_fingerprints = scan
        .packets
        .iter()
        .map(|record| record.packet.non_prod_config_fingerprint.sha256.as_str())
        .chain(
            scan.runbook_json
                .iter()
                .map(|record| record.runbook.non_prod_config_fingerprint.sha256.as_str()),
        )
        .collect::<BTreeSet<_>>()
        .len();
    let unique_build_versions = scan
        .packets
        .iter()
        .map(|record| record.packet.build_version.as_str())
        .chain(
            scan.runbook_json
                .iter()
                .map(|record| record.runbook.build_version.as_str()),
        )
        .collect::<BTreeSet<_>>()
        .len();
    let unique_git_commits = scan
        .packets
        .iter()
        .filter_map(|record| record.packet.git_commit.as_deref())
        .chain(
            scan.runbook_json
                .iter()
                .filter_map(|record| record.runbook.git_commit.as_deref()),
        )
        .collect::<BTreeSet<_>>()
        .len();
    let orphan_markdown_paths_list = orphan_markdown_paths(&scan, &generations);

    let verdict = if !scan.invalid_artifacts.is_empty() {
        ArchiveVerdict::ArchiveHealthInvalidArtifactsPresent
    } else if scan.packets.is_empty()
        || !missing_runbook_for_packet.is_empty()
        || !missing_packet_for_runbook.is_empty()
    {
        ArchiveVerdict::ArchiveHealthMissingPairings
    } else {
        ArchiveVerdict::ArchiveHealthOk
    };

    let reason = match verdict {
        ArchiveVerdict::ArchiveHealthInvalidArtifactsPresent => format!(
            "archive contains {} invalid artifact(s); review invalid_artifacts before trusting archive health",
            scan.invalid_artifacts.len()
        ),
        ArchiveVerdict::ArchiveHealthMissingPairings => {
            if scan.packets.is_empty() && scan.runbook_json.is_empty() {
                "archive does not contain any valid activation packet/runbook artifacts yet".to_string()
            } else {
                "archive contains missing packet/runbook pairings or incomplete artifact sets".to_string()
            }
        }
        ArchiveVerdict::ArchiveHealthOk => {
            "archive contains valid packet/runbook generations and no invalid artifacts".to_string()
        }
        ArchiveVerdict::ArchiveRetentionPlanReady
        | ArchiveVerdict::ArchiveRetentionPlanInsufficientArtifacts => unreachable!(),
    };

    Ok(ArchiveIndexReport {
        mode: "index".to_string(),
        verdict,
        reason,
        archive_dir: config.archive_dir.display().to_string(),
        packet_json_count: scan.packets.len(),
        runbook_json_count: scan.runbook_json.len(),
        runbook_markdown_count: scan.runbook_markdown.len(),
        paired_generation_count: generation_summaries
            .iter()
            .filter(|summary| {
                !summary.decision_packet_paths.is_empty() && !summary.runbook_json_paths.is_empty()
            })
            .count(),
        missing_runbook_for_packet_count: missing_runbook_for_packet.len(),
        missing_packet_for_runbook_count: missing_packet_for_runbook.len(),
        missing_markdown_for_runbook_count: missing_markdown_for_runbook.len(),
        orphan_markdown_count: orphan_markdown_paths_list.len(),
        invalid_artifact_count: scan.invalid_artifacts.len(),
        invalid_artifacts: scan.invalid_artifacts,
        latest_packet,
        latest_runbook,
        unique_prod_config_fingerprints,
        unique_non_prod_config_fingerprints,
        unique_build_versions,
        unique_git_commits,
        generations: generation_summaries,
        missing_runbook_for_packet,
        missing_packet_for_runbook,
        missing_markdown_for_runbook,
        orphan_markdown_paths: orphan_markdown_paths_list,
        read_only_archive_analysis: true,
        deletes_performed: false,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact analysis only. This archive report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn build_retention_plan_report(config: &Config) -> Result<ArchiveRetentionPlanReport> {
    let selection = build_retention_selection(config)?;
    let packet_backed = selection
        .generations
        .iter()
        .filter(|summary| !summary.decision_packet_paths.is_empty())
        .collect::<Vec<_>>();

    let verdict = if !selection.scan.invalid_artifacts.is_empty() {
        ArchiveVerdict::ArchiveHealthInvalidArtifactsPresent
    } else if packet_backed.is_empty() {
        ArchiveVerdict::ArchiveRetentionPlanInsufficientArtifacts
    } else {
        ArchiveVerdict::ArchiveRetentionPlanReady
    };

    let reason = match verdict {
        ArchiveVerdict::ArchiveHealthInvalidArtifactsPresent => format!(
            "retention preview is blocked by {} invalid artifact(s); review invalid_artifacts first",
            selection.scan.invalid_artifacts.len()
        ),
        ArchiveVerdict::ArchiveRetentionPlanInsufficientArtifacts => {
            "retention preview has no packet-backed artifact generations to evaluate".to_string()
        }
        ArchiveVerdict::ArchiveRetentionPlanReady => format!(
            "retention preview is ready; keeping the latest {} packet-backed generation(s) would retain {} generation(s) and remove {} generation(s)",
            config.keep_latest,
            selection
                .generations
                .iter()
                .filter(|summary| summary.would_keep)
                .count(),
            selection
                .generations
                .iter()
                .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
                .count()
        ),
        ArchiveVerdict::ArchiveHealthOk | ArchiveVerdict::ArchiveHealthMissingPairings => unreachable!(),
    };

    Ok(ArchiveRetentionPlanReport {
        mode: "retention_plan".to_string(),
        verdict,
        reason,
        archive_dir: config.archive_dir.display().to_string(),
        keep_latest: config.keep_latest,
        total_generation_count: selection.generations.len(),
        generations_to_keep: selection
            .generations
            .iter()
            .filter(|summary| summary.would_keep)
            .count(),
        generations_to_remove: selection
            .generations
            .iter()
            .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
            .count(),
        generations: selection.generations,
        orphan_runbook_generations: selection.orphan_runbook_generations,
        packet_generations_missing_runbook: selection.packet_generations_missing_runbook,
        orphan_markdown_paths: selection.orphan_markdown_paths,
        invalid_artifact_count: selection.scan.invalid_artifacts.len(),
        invalid_artifacts: selection.scan.invalid_artifacts,
        read_only_archive_analysis: true,
        deletes_performed: false,
        activation_authorized: false,
        not_authorized_summary:
            "Retention preview only. This command does not delete artifacts, does not authorize activation, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn build_retention_selection(config: &Config) -> Result<RetentionSelection> {
    let scan = scan_archive(&config.archive_dir)?;
    let generation_records = build_generation_records(&scan);
    let mut generation_summaries = generation_summaries(&generation_records);
    generation_summaries.sort_by(|left, right| {
        left.decision_packet_generated_at
            .cmp(&right.decision_packet_generated_at)
            .reverse()
    });

    let packet_backed = generation_summaries
        .iter()
        .filter(|summary| !summary.decision_packet_paths.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    let keep_keys = packet_backed
        .iter()
        .take(config.keep_latest)
        .map(generation_key_from_summary)
        .collect::<BTreeSet<_>>();

    let generations = generation_summaries
        .iter()
        .map(|summary| {
            let key = generation_key_from_summary(summary);
            let has_packet = !summary.decision_packet_paths.is_empty();
            let would_keep = has_packet && keep_keys.contains(&key);
            let reason = if !has_packet {
                "orphan runbook generation is left untouched by retention apply".to_string()
            } else if would_keep {
                format!(
                    "kept because it is within the latest {} packet-backed generation(s)",
                    config.keep_latest
                )
            } else {
                format!(
                    "selected for removal because it is older than the latest {} packet-backed generation(s)",
                    config.keep_latest
                )
            };
            ArchiveRetentionGenerationPlan {
                decision_packet_generated_at: summary.decision_packet_generated_at,
                prod_config_fingerprint_sha256: summary.prod_config_fingerprint_sha256.clone(),
                non_prod_config_fingerprint_sha256: summary.non_prod_config_fingerprint_sha256.clone(),
                latest_packet_verdict: summary.latest_packet_verdict.clone(),
                latest_runbook_verdict: summary.latest_runbook_verdict.clone(),
                would_keep,
                reason,
                decision_packet_paths: summary.decision_packet_paths.clone(),
                runbook_json_paths: summary.runbook_json_paths.clone(),
                runbook_markdown_paths: summary.runbook_markdown_paths.clone(),
            }
        })
        .collect::<Vec<_>>();

    let orphan_runbook_generations = generation_summaries
        .iter()
        .filter(|summary| {
            summary.decision_packet_paths.is_empty() && !summary.runbook_json_paths.is_empty()
        })
        .cloned()
        .collect::<Vec<_>>();
    let packet_generations_missing_runbook = generation_summaries
        .iter()
        .filter(|summary| {
            !summary.decision_packet_paths.is_empty() && summary.runbook_json_paths.is_empty()
        })
        .cloned()
        .collect::<Vec<_>>();
    let orphan_markdown_paths = orphan_markdown_paths(&scan, &generation_records);

    Ok(RetentionSelection {
        scan,
        generations,
        orphan_runbook_generations,
        packet_generations_missing_runbook,
        orphan_markdown_paths,
    })
}

fn apply_retention_plan(config: &Config) -> Result<ArchiveCleanupApplyReport> {
    let selection = build_retention_selection(config)?;
    if !selection.scan.invalid_artifacts.is_empty() {
        return Ok(ArchiveCleanupApplyReport {
            mode: "retention_apply".to_string(),
            verdict: ArchiveCleanupVerdict::ArchiveCleanupBlockedByInvalidArtifacts,
            reason: format!(
                "cleanup is blocked because {} invalid artifact(s) are present; review or fix them before apply mode",
                selection.scan.invalid_artifacts.len()
            ),
            archive_dir: config.archive_dir.display().to_string(),
            keep_latest: config.keep_latest,
            generations_to_keep: selection
                .generations
                .iter()
                .filter(|summary| summary.would_keep)
                .count(),
            generations_to_remove: selection
                .generations
                .iter()
                .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
                .count(),
            kept_generations: selection
                .generations
                .iter()
                .filter(|summary| summary.would_keep)
                .cloned()
                .collect(),
            removed_generations: selection
                .generations
                .iter()
                .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
                .cloned()
                .collect(),
            removed_file_paths: Vec::new(),
            failed_removals: Vec::new(),
            invalid_artifact_count: selection.scan.invalid_artifacts.len(),
            invalid_artifacts: selection.scan.invalid_artifacts,
            orphan_runbook_generations: selection.orphan_runbook_generations,
            packet_generations_missing_runbook: selection.packet_generations_missing_runbook,
            orphan_markdown_paths: selection.orphan_markdown_paths,
            read_only_archive_analysis: false,
            deletes_performed: false,
            activation_authorized: false,
            not_authorized_summary:
                "Cleanup is blocked. This command still does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let kept_generations = selection
        .generations
        .iter()
        .filter(|summary| summary.would_keep)
        .cloned()
        .collect::<Vec<_>>();
    let removed_generations = selection
        .generations
        .iter()
        .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
        .cloned()
        .collect::<Vec<_>>();

    if removed_generations.is_empty() {
        return Ok(ArchiveCleanupApplyReport {
            mode: "retention_apply".to_string(),
            verdict: ArchiveCleanupVerdict::ArchiveCleanupNothingToDo,
            reason: "cleanup found nothing to remove under the requested keep_latest retention rule".to_string(),
            archive_dir: config.archive_dir.display().to_string(),
            keep_latest: config.keep_latest,
            generations_to_keep: kept_generations.len(),
            generations_to_remove: 0,
            kept_generations,
            removed_generations,
            removed_file_paths: Vec::new(),
            failed_removals: Vec::new(),
            invalid_artifact_count: 0,
            invalid_artifacts: Vec::new(),
            orphan_runbook_generations: selection.orphan_runbook_generations,
            packet_generations_missing_runbook: selection.packet_generations_missing_runbook,
            orphan_markdown_paths: selection.orphan_markdown_paths,
            read_only_archive_analysis: false,
            deletes_performed: false,
            activation_authorized: false,
            not_authorized_summary:
                "Cleanup apply mode touched only archive artifacts and still does not authorize activation.".to_string(),
        });
    }

    let archive_root = fs::canonicalize(&config.archive_dir).with_context(|| {
        format!(
            "failed canonicalizing archive dir {} before cleanup",
            config.archive_dir.display()
        )
    })?;

    let mut removed_file_paths = Vec::new();
    let mut failed_removals = Vec::new();
    for generation in &removed_generations {
        let removal_targets = generation
            .decision_packet_paths
            .iter()
            .chain(generation.runbook_json_paths.iter())
            .chain(generation.runbook_markdown_paths.iter())
            .cloned()
            .collect::<Vec<_>>();
        for path in removal_targets {
            let path_buf = PathBuf::from(&path);
            match remove_archive_artifact_file(&archive_root, &path_buf) {
                Ok(()) => removed_file_paths.push(path),
                Err(error) => failed_removals.push(ArtifactRemovalFailure {
                    path,
                    error: format!("{error:#}"),
                }),
            }
        }
    }

    let verdict = if failed_removals.is_empty() {
        ArchiveCleanupVerdict::ArchiveCleanupApplied
    } else {
        ArchiveCleanupVerdict::ArchiveCleanupFailedPartial
    };
    let reason = match verdict {
        ArchiveCleanupVerdict::ArchiveCleanupApplied => format!(
            "cleanup removed {} artifact file(s) using the same selection logic as retention preview",
            removed_file_paths.len()
        ),
        ArchiveCleanupVerdict::ArchiveCleanupFailedPartial => format!(
            "cleanup removed {} artifact file(s) but {} removal(s) failed; review failed_removals before re-running",
            removed_file_paths.len(),
            failed_removals.len()
        ),
        ArchiveCleanupVerdict::ArchiveCleanupBlockedByInvalidArtifacts
        | ArchiveCleanupVerdict::ArchiveCleanupNothingToDo => unreachable!(),
    };

    Ok(ArchiveCleanupApplyReport {
        mode: "retention_apply".to_string(),
        verdict,
        reason,
        archive_dir: config.archive_dir.display().to_string(),
        keep_latest: config.keep_latest,
        generations_to_keep: kept_generations.len(),
        generations_to_remove: removed_generations.len(),
        kept_generations,
        removed_generations,
        removed_file_paths,
        failed_removals,
        invalid_artifact_count: 0,
        invalid_artifacts: Vec::new(),
        orphan_runbook_generations: selection.orphan_runbook_generations,
        packet_generations_missing_runbook: selection.packet_generations_missing_runbook,
        orphan_markdown_paths: selection.orphan_markdown_paths,
        read_only_archive_analysis: false,
        deletes_performed: true,
        activation_authorized: false,
        not_authorized_summary:
            "Cleanup apply mode only prunes exported archive artifacts. It still does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

#[allow(dead_code)]
pub(crate) fn archive_inventory(archive_dir: &Path) -> Result<ArchiveInventory> {
    let scan = scan_archive(archive_dir)?;
    let generations = build_generation_records(&scan);
    Ok(ArchiveInventory {
        generation_summaries: generation_summaries(&generations),
        invalid_artifacts: scan.invalid_artifacts.clone(),
        orphan_markdown_paths: orphan_markdown_paths(&scan, &generations),
    })
}

#[allow(dead_code)]
pub(crate) fn generation_id_from_summary(summary: &ArchiveArtifactGenerationSummary) -> String {
    format!(
        "{}|{}|{}",
        summary.decision_packet_generated_at.to_rfc3339(),
        summary.prod_config_fingerprint_sha256,
        summary.non_prod_config_fingerprint_sha256
    )
}

fn scan_archive(archive_dir: &Path) -> Result<ArchiveScan> {
    let mut scan = ArchiveScan::default();
    for path in collect_archive_files(archive_dir)? {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => classify_json_artifact(&path, &mut scan),
            Some("md") => classify_markdown_artifact(&path, &mut scan),
            _ => {}
        }
    }
    Ok(scan)
}

fn collect_archive_files(archive_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![archive_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .with_context(|| format!("failed reading archive dir {}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn classify_json_artifact(path: &Path, scan: &mut ArchiveScan) {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) => {
            scan.invalid_artifacts.push(InvalidArtifact {
                path: path.display().to_string(),
                error: format!("failed reading json artifact: {error}"),
            });
            return;
        }
    };

    match serde_json::from_str::<DecisionPacketArtifact>(&raw) {
        Ok(packet) => match validate_packet(path, &packet) {
            Ok(()) => {
                scan.packets.push(LoadedPacketArtifact {
                    path: path.to_path_buf(),
                    packet,
                });
                return;
            }
            Err(error) => {
                scan.invalid_artifacts.push(InvalidArtifact {
                    path: path.display().to_string(),
                    error: format!("{error:#}"),
                });
                return;
            }
        },
        Err(packet_error) => match serde_json::from_str::<ActivationRunbookArtifact>(&raw) {
            Ok(runbook) => match validate_runbook(path, &runbook) {
                Ok(()) => {
                    scan.runbook_json.push(LoadedRunbookArtifact {
                        path: path.to_path_buf(),
                        runbook,
                    });
                }
                Err(error) => {
                    scan.invalid_artifacts.push(InvalidArtifact {
                        path: path.display().to_string(),
                        error: format!("{error:#}"),
                    });
                }
            },
            Err(runbook_error) => {
                scan.invalid_artifacts.push(InvalidArtifact {
                    path: path.display().to_string(),
                    error: format!(
                        "unrecognized activation artifact json; packet parse error: {packet_error}; runbook parse error: {runbook_error}"
                    ),
                });
            }
        },
    }
}

fn classify_markdown_artifact(path: &Path, scan: &mut ArchiveScan) {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) => {
            scan.invalid_artifacts.push(InvalidArtifact {
                path: path.display().to_string(),
                error: format!("failed reading markdown artifact: {error}"),
            });
            return;
        }
    };
    let first_line = raw.lines().next().unwrap_or_default().trim().to_string();
    if first_line == RUNBOOK_MARKDOWN_TITLE {
        scan.runbook_markdown.push(LoadedMarkdownArtifact {
            path: path.to_path_buf(),
            _title_line: first_line,
        });
    } else {
        scan.invalid_artifacts.push(InvalidArtifact {
            path: path.display().to_string(),
            error: format!(
                "markdown artifact is not a recognized activation runbook markdown export (expected first line `{RUNBOOK_MARKDOWN_TITLE}`)"
            ),
        });
    }
}

fn validate_packet(path: &Path, packet: &DecisionPacketArtifact) -> Result<()> {
    if packet.packet_version.trim().is_empty() {
        bail!("{} has empty packet_version", path.display());
    }
    if !packet.read_only_packet {
        bail!("{} has read_only_packet=false", path.display());
    }
    if packet.activation_authorized {
        bail!("{} has activation_authorized=true", path.display());
    }
    Ok(())
}

fn validate_runbook(path: &Path, runbook: &ActivationRunbookArtifact) -> Result<()> {
    if runbook.runbook_version.trim().is_empty() {
        bail!("{} has empty runbook_version", path.display());
    }
    if !runbook.read_only_runbook {
        bail!("{} has read_only_runbook=false", path.display());
    }
    if runbook.activation_authorized {
        bail!("{} has activation_authorized=true", path.display());
    }
    if runbook.section_order.is_empty() {
        bail!("{} has empty section_order", path.display());
    }
    Ok(())
}

fn build_generation_records(
    scan: &ArchiveScan,
) -> BTreeMap<ArtifactGenerationKey, GenerationRecord> {
    let mut records = BTreeMap::<ArtifactGenerationKey, GenerationRecord>::new();
    for packet in &scan.packets {
        let key = ArtifactGenerationKey {
            decision_packet_generated_at: packet.packet.generated_at,
            prod_config_fingerprint_sha256: packet.packet.prod_config_fingerprint.sha256.clone(),
            non_prod_config_fingerprint_sha256: packet
                .packet
                .non_prod_config_fingerprint
                .sha256
                .clone(),
        };
        records.entry(key).or_default().packets.push(packet.clone());
    }
    let mut runbook_keys_by_path = BTreeMap::<PathBuf, ArtifactGenerationKey>::new();
    for runbook in &scan.runbook_json {
        let key = ArtifactGenerationKey {
            decision_packet_generated_at: runbook.runbook.decision_packet_generated_at,
            prod_config_fingerprint_sha256: runbook.runbook.prod_config_fingerprint.sha256.clone(),
            non_prod_config_fingerprint_sha256: runbook
                .runbook
                .non_prod_config_fingerprint
                .sha256
                .clone(),
        };
        records
            .entry(key.clone())
            .or_default()
            .runbook_json
            .push(runbook.clone());
        runbook_keys_by_path.insert(runbook.path.clone(), key);
    }

    for markdown in &scan.runbook_markdown {
        if let Some(key) =
            resolve_markdown_generation_key(markdown, &scan.runbook_json, &runbook_keys_by_path)
        {
            records
                .entry(key)
                .or_default()
                .runbook_markdown_paths
                .push(markdown.path.clone());
        }
    }

    records
}

fn resolve_markdown_generation_key(
    markdown: &LoadedMarkdownArtifact,
    runbooks: &[LoadedRunbookArtifact],
    runbook_keys_by_path: &BTreeMap<PathBuf, ArtifactGenerationKey>,
) -> Option<ArtifactGenerationKey> {
    let markdown_stem = markdown.path.file_stem().and_then(|stem| stem.to_str())?;
    let markdown_parent = markdown.path.parent();

    let same_parent_same_stem = runbooks
        .iter()
        .filter(|runbook| {
            runbook.path.parent() == markdown_parent
                && runbook.path.file_stem().and_then(|stem| stem.to_str()) == Some(markdown_stem)
        })
        .collect::<Vec<_>>();
    if same_parent_same_stem.len() == 1 {
        return runbook_keys_by_path
            .get(&same_parent_same_stem[0].path)
            .cloned();
    }

    let same_parent = runbooks
        .iter()
        .filter(|runbook| runbook.path.parent() == markdown_parent)
        .collect::<Vec<_>>();
    if same_parent.len() == 1 {
        return runbook_keys_by_path.get(&same_parent[0].path).cloned();
    }

    let same_stem = runbooks
        .iter()
        .filter(|runbook| {
            runbook.path.file_stem().and_then(|stem| stem.to_str()) == Some(markdown_stem)
        })
        .collect::<Vec<_>>();
    if same_stem.len() == 1 {
        return runbook_keys_by_path.get(&same_stem[0].path).cloned();
    }

    None
}

fn generation_summaries(
    generations: &BTreeMap<ArtifactGenerationKey, GenerationRecord>,
) -> Vec<ArchiveArtifactGenerationSummary> {
    generations
        .iter()
        .map(|(key, record)| ArchiveArtifactGenerationSummary {
            decision_packet_generated_at: key.decision_packet_generated_at,
            prod_config_fingerprint_sha256: key.prod_config_fingerprint_sha256.clone(),
            non_prod_config_fingerprint_sha256: key.non_prod_config_fingerprint_sha256.clone(),
            decision_packet_paths: sorted_paths(record.packets.iter().map(|packet| &packet.path)),
            runbook_json_paths: sorted_paths(
                record.runbook_json.iter().map(|runbook| &runbook.path),
            ),
            runbook_markdown_paths: sorted_paths(record.runbook_markdown_paths.iter()),
            latest_packet_verdict: record
                .packets
                .iter()
                .max_by_key(|packet| packet.packet.generated_at)
                .map(|packet| serialize_enum(&packet.packet.verdict)),
            latest_runbook_verdict: record
                .runbook_json
                .iter()
                .max_by_key(|runbook| runbook.runbook.generated_at)
                .map(|runbook| serialize_enum(&runbook.runbook.verdict)),
            build_versions: record
                .packets
                .iter()
                .map(|packet| packet.packet.build_version.clone())
                .chain(
                    record
                        .runbook_json
                        .iter()
                        .map(|runbook| runbook.runbook.build_version.clone()),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            git_commits: record
                .packets
                .iter()
                .filter_map(|packet| packet.packet.git_commit.clone())
                .chain(
                    record
                        .runbook_json
                        .iter()
                        .filter_map(|runbook| runbook.runbook.git_commit.clone()),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        })
        .collect()
}

fn orphan_markdown_paths(
    scan: &ArchiveScan,
    generations: &BTreeMap<ArtifactGenerationKey, GenerationRecord>,
) -> Vec<String> {
    let attached = generations
        .values()
        .flat_map(|record| record.runbook_markdown_paths.iter().cloned())
        .collect::<BTreeSet<_>>();
    scan.runbook_markdown
        .iter()
        .filter(|artifact| !attached.contains(&artifact.path))
        .map(|artifact| artifact.path.display().to_string())
        .collect()
}

fn packet_summary(packet: &LoadedPacketArtifact) -> ArchiveLatestArtifactSummary {
    ArchiveLatestArtifactSummary {
        path: packet.path.display().to_string(),
        generated_at: packet.packet.generated_at,
        verdict: serialize_enum(&packet.packet.verdict),
        build_version: packet.packet.build_version.clone(),
        git_commit: packet.packet.git_commit.clone(),
        prod_config_fingerprint_sha256: packet.packet.prod_config_fingerprint.sha256.clone(),
        non_prod_config_fingerprint_sha256: packet
            .packet
            .non_prod_config_fingerprint
            .sha256
            .clone(),
    }
}

fn runbook_summary(runbook: &LoadedRunbookArtifact) -> ArchiveLatestArtifactSummary {
    ArchiveLatestArtifactSummary {
        path: runbook.path.display().to_string(),
        generated_at: runbook.runbook.generated_at,
        verdict: serialize_enum(&runbook.runbook.verdict),
        build_version: runbook.runbook.build_version.clone(),
        git_commit: runbook.runbook.git_commit.clone(),
        prod_config_fingerprint_sha256: runbook.runbook.prod_config_fingerprint.sha256.clone(),
        non_prod_config_fingerprint_sha256: runbook
            .runbook
            .non_prod_config_fingerprint
            .sha256
            .clone(),
    }
}

fn generation_key_from_summary(
    summary: &ArchiveArtifactGenerationSummary,
) -> ArtifactGenerationKey {
    ArtifactGenerationKey {
        decision_packet_generated_at: summary.decision_packet_generated_at,
        prod_config_fingerprint_sha256: summary.prod_config_fingerprint_sha256.clone(),
        non_prod_config_fingerprint_sha256: summary.non_prod_config_fingerprint_sha256.clone(),
    }
}

fn sorted_paths<'a, I>(paths: I) -> Vec<String>
where
    I: Iterator<Item = &'a PathBuf>,
{
    let mut values = paths
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    values.sort();
    values
}

fn remove_archive_artifact_file(archive_root: &Path, path: &Path) -> Result<()> {
    let canonical = fs::canonicalize(path).with_context(|| {
        format!(
            "failed canonicalizing archive artifact before deletion {}",
            path.display()
        )
    })?;
    if !canonical.starts_with(archive_root) {
        bail!(
            "refusing to delete {} because its canonical target {} is outside archive root {}",
            path.display(),
            canonical.display(),
            archive_root.display()
        );
    }
    if path.extension().and_then(|ext| ext.to_str()) != Some("json")
        && path.extension().and_then(|ext| ext.to_str()) != Some("md")
    {
        bail!(
            "refusing to delete {} because it is not a recognized artifact file type",
            path.display()
        );
    }
    fs::remove_file(path)
        .with_context(|| format!("failed deleting archive artifact {}", path.display()))
}

fn render_index_human(report: &ArchiveIndexReport) -> String {
    [
        "event=copybot_activation_artifact_archive".to_string(),
        "mode=index".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("packet_json_count={}", report.packet_json_count),
        format!("runbook_json_count={}", report.runbook_json_count),
        format!("runbook_markdown_count={}", report.runbook_markdown_count),
        format!("paired_generation_count={}", report.paired_generation_count),
        format!(
            "missing_runbook_for_packet_count={}",
            report.missing_runbook_for_packet_count
        ),
        format!(
            "missing_packet_for_runbook_count={}",
            report.missing_packet_for_runbook_count
        ),
        format!(
            "missing_markdown_for_runbook_count={}",
            report.missing_markdown_for_runbook_count
        ),
        format!("orphan_markdown_count={}", report.orphan_markdown_count),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "latest_packet_verdict={}",
            report
                .latest_packet
                .as_ref()
                .map(|summary| summary.verdict.clone())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_runbook_verdict={}",
            report
                .latest_runbook
                .as_ref()
                .map(|summary| summary.verdict.clone())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "unique_prod_config_fingerprints={}",
            report.unique_prod_config_fingerprints
        ),
        format!(
            "unique_non_prod_config_fingerprints={}",
            report.unique_non_prod_config_fingerprints
        ),
        format!("unique_build_versions={}", report.unique_build_versions),
        format!("unique_git_commits={}", report.unique_git_commits),
        format!(
            "orphan_markdown_paths={}",
            report.orphan_markdown_paths.join(" | ")
        ),
        format!(
            "read_only_archive_analysis={}",
            report.read_only_archive_analysis
        ),
        format!("deletes_performed={}", report.deletes_performed),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_retention_plan_human(report: &ArchiveRetentionPlanReport) -> String {
    let keep_paths = report
        .generations
        .iter()
        .filter(|summary| summary.would_keep)
        .flat_map(|summary| {
            summary
                .decision_packet_paths
                .iter()
                .chain(summary.runbook_json_paths.iter())
                .chain(summary.runbook_markdown_paths.iter())
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join(" | ");
    let remove_paths = report
        .generations
        .iter()
        .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
        .flat_map(|summary| {
            summary
                .decision_packet_paths
                .iter()
                .chain(summary.runbook_json_paths.iter())
                .chain(summary.runbook_markdown_paths.iter())
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join(" | ");

    [
        "event=copybot_activation_artifact_archive".to_string(),
        "mode=retention_plan".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("keep_latest={}", report.keep_latest),
        format!("total_generation_count={}", report.total_generation_count),
        format!("generations_to_keep={}", report.generations_to_keep),
        format!("generations_to_remove={}", report.generations_to_remove),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!("would_keep_paths={keep_paths}"),
        format!("would_remove_paths={remove_paths}"),
        format!(
            "orphan_markdown_paths={}",
            report.orphan_markdown_paths.join(" | ")
        ),
        format!(
            "read_only_archive_analysis={}",
            report.read_only_archive_analysis
        ),
        format!("deletes_performed={}", report.deletes_performed),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_cleanup_apply_human(report: &ArchiveCleanupApplyReport) -> String {
    [
        "event=copybot_activation_artifact_archive".to_string(),
        "mode=retention_apply".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("keep_latest={}", report.keep_latest),
        format!("generations_to_keep={}", report.generations_to_keep),
        format!("generations_to_remove={}", report.generations_to_remove),
        format!(
            "removed_file_paths={}",
            report.removed_file_paths.join(" | ")
        ),
        format!(
            "failed_removals={}",
            report
                .failed_removals
                .iter()
                .map(|failure| format!("{}:{}", failure.path, failure.error))
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "orphan_markdown_paths={}",
            report.orphan_markdown_paths.join(" | ")
        ),
        format!(
            "read_only_archive_analysis={}",
            report.read_only_archive_analysis
        ),
        format!("deletes_performed={}", report.deletes_performed),
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn empty_archive_is_reported() {
        let dir = temp_dir("activation_archive_empty");
        let report = build_index_report(&Config {
            archive_dir: dir,
            json: false,
            retention_plan: false,
            retention_apply: false,
            keep_latest: DEFAULT_KEEP_LATEST,
        })
        .expect("report");

        assert_eq!(report.verdict, ArchiveVerdict::ArchiveHealthMissingPairings);
        assert_eq!(report.packet_json_count, 0);
        assert_eq!(report.runbook_json_count, 0);
    }

    #[test]
    fn valid_archive_with_packet_and_runbook_pairs_is_ok() {
        let dir = temp_dir("activation_archive_valid");
        let packet = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
        );
        let runbook = sample_runbook(
            "2026-03-26T12:05:00Z",
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
        );
        write_json(&dir.join("packet-1.json"), &packet);
        write_json(&dir.join("runbook-1.json"), &runbook);
        fs::write(
            dir.join("runbook-1.md"),
            format!("{RUNBOOK_MARKDOWN_TITLE}\n\nok"),
        )
        .expect("write md");

        let report = build_index_report(&Config {
            archive_dir: dir,
            json: false,
            retention_plan: false,
            retention_apply: false,
            keep_latest: DEFAULT_KEEP_LATEST,
        })
        .expect("report");

        assert_eq!(report.verdict, ArchiveVerdict::ArchiveHealthOk);
        assert_eq!(report.packet_json_count, 1);
        assert_eq!(report.runbook_json_count, 1);
        assert_eq!(report.runbook_markdown_count, 1);
        assert_eq!(report.paired_generation_count, 1);
    }

    #[test]
    fn malformed_artifact_surfaces_invalid_verdict() {
        let dir = temp_dir("activation_archive_invalid");
        fs::write(dir.join("broken.json"), "{broken").expect("write invalid");

        let report = build_index_report(&Config {
            archive_dir: dir,
            json: false,
            retention_plan: false,
            retention_apply: false,
            keep_latest: DEFAULT_KEEP_LATEST,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArchiveVerdict::ArchiveHealthInvalidArtifactsPresent
        );
        assert_eq!(report.invalid_artifact_count, 1);
    }

    #[test]
    fn missing_pairings_are_detected() {
        let dir = temp_dir("activation_archive_missing_pair");
        let packet = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            DecisionPacketVerdict::DecisionPacketBlocked,
        );
        write_json(&dir.join("packet-1.json"), &packet);

        let report = build_index_report(&Config {
            archive_dir: dir,
            json: false,
            retention_plan: false,
            retention_apply: false,
            keep_latest: DEFAULT_KEEP_LATEST,
        })
        .expect("report");

        assert_eq!(report.verdict, ArchiveVerdict::ArchiveHealthMissingPairings);
        assert_eq!(report.missing_runbook_for_packet_count, 1);
    }

    #[test]
    fn retention_plan_keeps_latest_packet_backed_sets() {
        let dir = temp_dir("activation_archive_retention");
        let packet_old = sample_packet(
            "2026-03-26T10:00:00Z",
            "prod_fp_old",
            "non_prod_fp_old",
            DecisionPacketVerdict::DecisionPacketBlocked,
        );
        let runbook_old = sample_runbook(
            "2026-03-26T10:05:00Z",
            "2026-03-26T10:00:00Z",
            "prod_fp_old",
            "non_prod_fp_old",
            ActivationRunbookVerdict::RunbookBlocked,
        );
        let packet_new = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
        );
        let runbook_new = sample_runbook(
            "2026-03-26T12:05:00Z",
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
            ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
        );
        write_json(&dir.join("packet-old.json"), &packet_old);
        write_json(&dir.join("runbook-old.json"), &runbook_old);
        fs::write(
            dir.join("runbook-old.md"),
            format!("{RUNBOOK_MARKDOWN_TITLE}\n\nold"),
        )
        .expect("write old md");
        write_json(&dir.join("packet-new.json"), &packet_new);
        write_json(&dir.join("runbook-new.json"), &runbook_new);
        fs::write(
            dir.join("runbook-new.md"),
            format!("{RUNBOOK_MARKDOWN_TITLE}\n\nnew"),
        )
        .expect("write new md");

        let report = build_retention_plan_report(&Config {
            archive_dir: dir,
            json: false,
            retention_plan: true,
            retention_apply: false,
            keep_latest: 1,
        })
        .expect("report");

        assert_eq!(report.verdict, ArchiveVerdict::ArchiveRetentionPlanReady);
        assert_eq!(report.generations_to_keep, 1);
        assert_eq!(report.generations_to_remove, 1);
        assert!(report.generations.iter().any(|summary| summary.would_keep
            && summary
                .decision_packet_paths
                .iter()
                .any(|path| path.contains("packet-new.json"))));
        assert!(report.generations.iter().any(|summary| !summary.would_keep
            && summary
                .decision_packet_paths
                .iter()
                .any(|path| path.contains("packet-old.json"))));
    }

    #[test]
    fn no_deletions_or_mutations_occur() {
        let dir = temp_dir("activation_archive_no_mutation");
        let packet = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            DecisionPacketVerdict::DecisionPacketBlocked,
        );
        let path = dir.join("packet-1.json");
        write_json(&path, &packet);
        let before = fs::read_to_string(&path).expect("before");

        let _ = build_retention_plan_report(&Config {
            archive_dir: dir,
            json: false,
            retention_plan: true,
            retention_apply: false,
            keep_latest: 1,
        })
        .expect("report");

        let after = fs::read_to_string(&path).expect("after");
        assert_eq!(before, after);
    }

    #[test]
    fn apply_mode_removes_exact_preview_targets_and_keeps_latest() {
        let dir = temp_dir("activation_archive_apply");
        let packet_old = sample_packet(
            "2026-03-26T10:00:00Z",
            "prod_fp_old",
            "non_prod_fp_old",
            DecisionPacketVerdict::DecisionPacketBlocked,
        );
        let runbook_old = sample_runbook(
            "2026-03-26T10:05:00Z",
            "2026-03-26T10:00:00Z",
            "prod_fp_old",
            "non_prod_fp_old",
            ActivationRunbookVerdict::RunbookBlocked,
        );
        let packet_new = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
        );
        let runbook_new = sample_runbook(
            "2026-03-26T12:05:00Z",
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
            ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
        );
        let packet_old_path = dir.join("packet-old.json");
        let runbook_old_path = dir.join("runbook-old.json");
        let runbook_old_md_path = dir.join("runbook-old.md");
        let packet_new_path = dir.join("packet-new.json");
        let runbook_new_path = dir.join("runbook-new.json");
        let runbook_new_md_path = dir.join("runbook-new.md");
        write_json(&packet_old_path, &packet_old);
        write_json(&runbook_old_path, &runbook_old);
        fs::write(
            &runbook_old_md_path,
            format!("{RUNBOOK_MARKDOWN_TITLE}\n\nold"),
        )
        .expect("write old md");
        write_json(&packet_new_path, &packet_new);
        write_json(&runbook_new_path, &runbook_new);
        fs::write(
            &runbook_new_md_path,
            format!("{RUNBOOK_MARKDOWN_TITLE}\n\nnew"),
        )
        .expect("write new md");

        let preview = build_retention_plan_report(&Config {
            archive_dir: dir.clone(),
            json: false,
            retention_plan: true,
            retention_apply: false,
            keep_latest: 1,
        })
        .expect("preview");
        let preview_remove = preview
            .generations
            .iter()
            .filter(|summary| !summary.would_keep && !summary.decision_packet_paths.is_empty())
            .flat_map(|summary| {
                summary
                    .decision_packet_paths
                    .iter()
                    .chain(summary.runbook_json_paths.iter())
                    .chain(summary.runbook_markdown_paths.iter())
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect::<BTreeSet<_>>();

        let apply = apply_retention_plan(&Config {
            archive_dir: dir.clone(),
            json: false,
            retention_plan: false,
            retention_apply: true,
            keep_latest: 1,
        })
        .expect("apply");

        assert_eq!(apply.verdict, ArchiveCleanupVerdict::ArchiveCleanupApplied);
        assert_eq!(
            apply
                .removed_file_paths
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            preview_remove
        );
        assert!(!packet_old_path.exists());
        assert!(!runbook_old_path.exists());
        assert!(!runbook_old_md_path.exists());
        assert!(packet_new_path.exists());
        assert!(runbook_new_path.exists());
        assert!(runbook_new_md_path.exists());
    }

    #[test]
    fn invalid_artifacts_block_cleanup_by_default() {
        let dir = temp_dir("activation_archive_apply_invalid");
        let packet = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            DecisionPacketVerdict::DecisionPacketBlocked,
        );
        let packet_path = dir.join("packet-1.json");
        write_json(&packet_path, &packet);
        fs::write(dir.join("broken.json"), "{broken").expect("write invalid");

        let apply = apply_retention_plan(&Config {
            archive_dir: dir.clone(),
            json: false,
            retention_plan: false,
            retention_apply: true,
            keep_latest: 1,
        })
        .expect("apply");

        assert_eq!(
            apply.verdict,
            ArchiveCleanupVerdict::ArchiveCleanupBlockedByInvalidArtifacts
        );
        assert!(packet_path.exists());
        assert_eq!(apply.removed_file_paths.len(), 0);
    }

    #[test]
    fn apply_mode_handles_nothing_to_do_explicitly() {
        let dir = temp_dir("activation_archive_apply_nothing");
        let packet = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
        );
        let packet_path = dir.join("packet-1.json");
        write_json(&packet_path, &packet);

        let apply = apply_retention_plan(&Config {
            archive_dir: dir.clone(),
            json: false,
            retention_plan: false,
            retention_apply: true,
            keep_latest: 1,
        })
        .expect("apply");

        assert_eq!(
            apply.verdict,
            ArchiveCleanupVerdict::ArchiveCleanupNothingToDo
        );
        assert!(packet_path.exists());
        assert_eq!(apply.removed_file_paths.len(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn no_file_outside_archive_dir_can_be_deleted() {
        use std::os::unix::fs::symlink;

        let outside_dir = temp_dir("activation_archive_outside_target");
        let outside_packet_path = outside_dir.join("packet-outside.json");
        write_json(
            &outside_packet_path,
            &sample_packet(
                "2026-03-26T10:00:00Z",
                "prod_fp_old",
                "non_prod_fp_old",
                DecisionPacketVerdict::DecisionPacketBlocked,
            ),
        );

        let dir = temp_dir("activation_archive_apply_symlink");
        let packet_new = sample_packet(
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
            DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
        );
        let runbook_new = sample_runbook(
            "2026-03-26T12:05:00Z",
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
            ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
        );
        let symlink_path = dir.join("packet-old.json");
        symlink(&outside_packet_path, &symlink_path).expect("symlink");
        write_json(&dir.join("packet-new.json"), &packet_new);
        write_json(&dir.join("runbook-new.json"), &runbook_new);

        let apply = apply_retention_plan(&Config {
            archive_dir: dir.clone(),
            json: false,
            retention_plan: false,
            retention_apply: true,
            keep_latest: 1,
        })
        .expect("apply");

        assert_eq!(
            apply.verdict,
            ArchiveCleanupVerdict::ArchiveCleanupFailedPartial
        );
        assert!(outside_packet_path.exists());
        assert!(symlink_path.exists());
    }

    fn sample_packet(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
        verdict: DecisionPacketVerdict,
    ) -> DecisionPacketArtifact {
        DecisionPacketArtifact {
            generated_at: DateTime::parse_from_rfc3339(generated_at)
                .expect("ts")
                .with_timezone(&Utc),
            packet_version: "1".to_string(),
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
            prod_config_path: "/etc/solana-copy-bot/live.server.toml".to_string(),
            non_prod_config_path: "/etc/solana-copy-bot/devnet.server.toml".to_string(),
            operator_note: None,
            execution_enabled: false,
            read_only_packet: true,
            activation_authorized: false,
            discussion_ready_only: verdict
                == DecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
            prod_stage3_remains_hard_gate: true,
            non_prod_evidence_is_secondary: true,
            verdict,
            reason: "artifact sample".to_string(),
            blockers: if verdict == DecisionPacketVerdict::DecisionPacketBlocked {
                vec!["stage3: insufficient evidence".to_string()]
            } else {
                Vec::new()
            },
            warnings: vec!["still planning-only".to_string()],
            checklist_verdict: "activation_checklist_blocked_by_prod_stage3".to_string(),
            checklist_reason: "stage3 blocked".to_string(),
            prod_config_fingerprint: ConfigFingerprintSummary {
                scope: "prod_execution_policy_guardrails".to_string(),
                sha256: prod_fingerprint.to_string(),
                secrets_excluded: true,
                sensitive_urls_redacted_before_hashing: true,
            },
            non_prod_config_fingerprint: ConfigFingerprintSummary {
                scope: "non_prod_execution_policy_guardrails".to_string(),
                sha256: non_prod_fingerprint.to_string(),
                secrets_excluded: true,
                sensitive_urls_redacted_before_hashing: true,
            },
            prod_pre_activation_gate: ProdPreActivationGateSummary {
                verdict: "pre_activation_gates_green".to_string(),
                reason: "gate sample".to_string(),
                planning_green: true,
                blocked_by_stage3: false,
                stage3_verdict: "fresh_current".to_string(),
                stage3_reason: "fresh".to_string(),
                stage3_captures_within_recent_horizon: 3,
                stage3_latest_capture_age_seconds: Some(300),
                stage4_readiness_verdict: "ready_for_execution_dry_run".to_string(),
                stage4_rehearsal_history_verdict: "sufficient_recent_rehearsal_evidence"
                    .to_string(),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                blockers: Vec::new(),
            },
            launch_dossier: LaunchDossierSummary {
                verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
                reason: "launch sample".to_string(),
                ready_when_stage_gate_allows: true,
                activation_overlay_complete: true,
                rollback_plan_complete: true,
                service_restart_contract_complete: true,
                activation_overlay_change_count: 3,
                rollback_overlay_change_count: 3,
                drift_finding_count: 0,
                blocker_count: 0,
                first_blocker: None,
            },
            tiny_live_guardrails: GuardrailSummary {
                verdict: "tiny_live_guardrails_bounded".to_string(),
                reason: "guardrails bounded".to_string(),
                bounded: true,
                enabled: true,
                blocker_count: 0,
                first_blocker: None,
                rollback_trigger_count: 1,
                rollback_triggers: vec![RollbackTriggerSummary {
                    trigger: "consecutive_hard_failures".to_string(),
                    threshold_kind: "count".to_string(),
                    threshold_rate_pct: None,
                    threshold_seconds: None,
                    threshold_sol: None,
                    threshold_count: Some(3),
                    evaluation_window_seconds: Some(600),
                    action: "rollback_now".to_string(),
                }],
            },
            non_prod_readiness: NonProdReadinessSummary {
                verdict: "devnet_readiness_green".to_string(),
                reason: "ready".to_string(),
                green: true,
                prod_profile_refused: false,
                config_env: "devnet".to_string(),
                blockers: Vec::new(),
                warnings: Vec::new(),
                dress_latest_record_age_seconds: Some(120),
                dress_recent_green_count: 3,
                activation_latest_record_age_seconds: Some(120),
                activation_recent_green_count: 3,
                activation_recent_rollback_success_count: 3,
                activation_recent_internal_consistency_count: 3,
                stale_evidence_excluded: false,
            },
        }
    }

    fn sample_runbook(
        generated_at: &str,
        decision_packet_generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
        verdict: ActivationRunbookVerdict,
    ) -> ActivationRunbookArtifact {
        ActivationRunbookArtifact {
            generated_at: DateTime::parse_from_rfc3339(generated_at)
                .expect("ts")
                .with_timezone(&Utc),
            runbook_version: "1".to_string(),
            prod_config_path: "/etc/solana-copy-bot/live.server.toml".to_string(),
            non_prod_config_path: "/etc/solana-copy-bot/devnet.server.toml".to_string(),
            json_output_path: None,
            markdown_output_path: None,
            execution_enabled: false,
            read_only_runbook: true,
            activation_authorized: false,
            discussion_ready_only: verdict
                == ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
            prod_stage3_remains_hard_gate: true,
            non_prod_evidence_is_secondary: true,
            verdict,
            reason: "runbook sample".to_string(),
            blockers: Vec::new(),
            warnings: vec!["still planning-only".to_string()],
            not_authorized_disclaimer: "planning-only".to_string(),
            decision_packet_version: "1".to_string(),
            decision_packet_generated_at: DateTime::parse_from_rfc3339(
                decision_packet_generated_at,
            )
            .expect("ts")
            .with_timezone(&Utc),
            decision_packet_verdict: "decision_packet_discussion_ready_but_not_authorized"
                .to_string(),
            decision_packet_reason: "sample".to_string(),
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
            prod_config_fingerprint: ConfigFingerprintSummary {
                scope: "prod_execution_policy_guardrails".to_string(),
                sha256: prod_fingerprint.to_string(),
                secrets_excluded: true,
                sensitive_urls_redacted_before_hashing: true,
            },
            non_prod_config_fingerprint: ConfigFingerprintSummary {
                scope: "non_prod_execution_policy_guardrails".to_string(),
                sha256: non_prod_fingerprint.to_string(),
                secrets_excluded: true,
                sensitive_urls_redacted_before_hashing: true,
            },
            prod_pre_activation_gate: ProdPreActivationGateSummary {
                verdict: "pre_activation_gates_green".to_string(),
                reason: "gate sample".to_string(),
                planning_green: true,
                blocked_by_stage3: false,
                stage3_verdict: "fresh_current".to_string(),
                stage3_reason: "fresh".to_string(),
                stage3_captures_within_recent_horizon: 3,
                stage3_latest_capture_age_seconds: Some(300),
                stage4_readiness_verdict: "ready_for_execution_dry_run".to_string(),
                stage4_rehearsal_history_verdict: "sufficient_recent_rehearsal_evidence"
                    .to_string(),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                blockers: Vec::new(),
            },
            launch_dossier: LaunchDossierSummary {
                verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
                reason: "launch sample".to_string(),
                ready_when_stage_gate_allows: true,
                activation_overlay_complete: true,
                rollback_plan_complete: true,
                service_restart_contract_complete: true,
                activation_overlay_change_count: 3,
                rollback_overlay_change_count: 3,
                drift_finding_count: 0,
                blocker_count: 0,
                first_blocker: None,
            },
            tiny_live_guardrails: GuardrailSummary {
                verdict: "tiny_live_guardrails_bounded".to_string(),
                reason: "guardrails bounded".to_string(),
                bounded: true,
                enabled: true,
                blocker_count: 0,
                first_blocker: None,
                rollback_trigger_count: 1,
                rollback_triggers: vec![RollbackTriggerSummary {
                    trigger: "consecutive_hard_failures".to_string(),
                    threshold_kind: "count".to_string(),
                    threshold_rate_pct: None,
                    threshold_seconds: None,
                    threshold_sol: None,
                    threshold_count: Some(3),
                    evaluation_window_seconds: Some(600),
                    action: "rollback_now".to_string(),
                }],
            },
            non_prod_readiness: NonProdReadinessSummary {
                verdict: "devnet_readiness_green".to_string(),
                reason: "ready".to_string(),
                green: true,
                prod_profile_refused: false,
                config_env: "devnet".to_string(),
                blockers: Vec::new(),
                warnings: Vec::new(),
                dress_latest_record_age_seconds: Some(120),
                dress_recent_green_count: 3,
                activation_latest_record_age_seconds: Some(120),
                activation_recent_green_count: 3,
                activation_recent_rollback_success_count: 3,
                activation_recent_internal_consistency_count: 3,
                stale_evidence_excluded: false,
            },
            section_order: vec![
                "current_state".to_string(),
                "preflight_checks".to_string(),
                "stop_here_conditions".to_string(),
                "bounded_activation_candidate".to_string(),
                "post_change_checks".to_string(),
                "rollback_triggers".to_string(),
                "rollback_procedure".to_string(),
                "not_authorized_disclaimer".to_string(),
            ],
        }
    }

    fn write_json<T: Serialize>(path: &Path, value: &T) {
        fs::write(
            path,
            serde_json::to_string_pretty(value).expect("serialize"),
        )
        .expect("write");
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
