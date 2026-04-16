use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::runtime_restore_ops::{
    artifact_archive_path, artifact_latest_path, copy_atomic, journal_snapshot_latest_path,
    load_json, prune_rotated_archives, resolve_db_path, resolve_relative_to_config,
    write_json_atomic, ARTIFACT_ARCHIVE_PREFIX, ARTIFACT_ARCHIVE_SUFFIX,
};
use copybot_discovery::{
    DiscoveryService, RecentRawCatchUpDiagnostic,
    RecentRawPromotedRetentionContractDiagnostic, RecentRawPromotionBlockerDiagnostic,
    RecentRawReplacementArtifactHistoryContractDiagnostic,
    RecentRawReplacementAttemptTelemetryDiagnostic, RecentRawReplacementConvergenceDiagnostic,
    RecentRawReplacementProgressContractDiagnostic,
    RecentRawReplacementPromotionContractDiagnostic, RecentRawSourceWindowContractDiagnostic,
    RecentRawStagedBirthDiagnostic, RecentRawStagedLineageDiagnostic,
    RecentRawStagedRegressionDiagnostic, RecentRawStagedWindowSeedingDiagnostic,
    ReplaySolLegIncompleteDiagnostic, ReplaySolLegIncompleteReasonClass,
};
use copybot_storage::{
    DiscoveryPersistedRebuildPhase, DiscoveryRuntimeArtifact, SqliteStore,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, Instant};

const USAGE: &str = "usage:
  discovery_runtime_export --config <path> [--db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]
  discovery_runtime_export --explain-recent-raw-promotion-blocker --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-catch-up-status --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-source-window-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-promoted-retention-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-replacement-promotion-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-replacement-progress-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-replacement-artifact-history-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-replacement-attempt-telemetry --state-root <path> [--json] [--deep-attempt-telemetry-scan]
  discovery_runtime_export --explain-recent-raw-replacement-convergence --state-root <path> [--json]
  discovery_runtime_export --explain-publication-truth-export-blocker --config <path> [--json]
  discovery_runtime_export --explain-replay-sol-leg-blocker --config <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-lineage --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-regression --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-window-seeding --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-birth --state-root <path> [--json]";

const PUBLICATION_TRUTH_WITHHELD_PREFIX: &str = "publication_truth_withheld_while_";
const DEFAULT_PUBLICATION_TRUTH_CHECKPOINT_HEADLINE_BUDGET_MS: u64 = 1_000;
const DEFAULT_PUBLICATION_TRUTH_CHECKPOINT_HEADLINE_BUDGET_SOURCE: &str =
    "fixed_constant_default_primary_operator";
const DEFAULT_REPLAY_SOL_LEG_BLOCKER_BUDGET_MS: u64 = 30_000;
const DEFAULT_REPLAY_SOL_LEG_BLOCKER_BUDGET_SOURCE: &str =
    "copybot_discovery_default_replay_sol_leg_read_only_source_scan_budget";
const REPLAY_SOL_LEG_BLOCKER_BUDGET_EXHAUSTED_STAGE: &str =
    "deep_replay_sol_leg_reason_source_scan_budget_exhausted";
const CHECKPOINT_HEADLINE_ROW_META_TABLE_EXISTS_SQL: &str = "SELECT EXISTS(
    SELECT 1
    FROM sqlite_master
    WHERE type = 'table' AND name = 'discovery_persisted_rebuild_state'
)";
const CHECKPOINT_HEADLINE_ROW_META_ROW_COUNT_SQL: &str =
    "SELECT COUNT(1) FROM discovery_persisted_rebuild_state WHERE id = 1";
const CHECKPOINT_HEADLINE_ROW_META_SQL: &str =
    "SELECT phase, updated_at FROM discovery_persisted_rebuild_state WHERE id = 1";

fn main() -> Result<()> {
    let Some(command) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("{}", run_command(command)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    scheduled: bool,
    force: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawPromotionBlockerConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawCatchUpStatusConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawSourceWindowContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawPromotedRetentionContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementPromotionContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementProgressContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementArtifactHistoryContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementAttemptTelemetryConfig {
    state_root: PathBuf,
    json: bool,
    deep_attempt_telemetry_scan: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementConvergenceConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainPublicationTruthExportBlockerConfig {
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainReplaySolLegBlockerConfig {
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedLineageConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedRegressionConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedBirthConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedWindowSeedingConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
enum Command {
    Export(Config),
    ExplainRecentRawPromotionBlocker(ExplainRecentRawPromotionBlockerConfig),
    ExplainRecentRawCatchUpStatus(ExplainRecentRawCatchUpStatusConfig),
    ExplainRecentRawSourceWindowContract(ExplainRecentRawSourceWindowContractConfig),
    ExplainRecentRawPromotedRetentionContract(ExplainRecentRawPromotedRetentionContractConfig),
    ExplainRecentRawReplacementPromotionContract(
        ExplainRecentRawReplacementPromotionContractConfig,
    ),
    ExplainRecentRawReplacementProgressContract(ExplainRecentRawReplacementProgressContractConfig),
    ExplainRecentRawReplacementArtifactHistoryContract(
        ExplainRecentRawReplacementArtifactHistoryContractConfig,
    ),
    ExplainRecentRawReplacementAttemptTelemetry(ExplainRecentRawReplacementAttemptTelemetryConfig),
    ExplainRecentRawReplacementConvergence(ExplainRecentRawReplacementConvergenceConfig),
    ExplainPublicationTruthExportBlocker(ExplainPublicationTruthExportBlockerConfig),
    ExplainReplaySolLegBlocker(ExplainReplaySolLegBlockerConfig),
    ExplainRecentRawStagedLineage(ExplainRecentRawStagedLineageConfig),
    ExplainRecentRawStagedRegression(ExplainRecentRawStagedRegressionConfig),
    ExplainRecentRawStagedBirth(ExplainRecentRawStagedBirthConfig),
    ExplainRecentRawStagedWindowSeeding(ExplainRecentRawStagedWindowSeedingConfig),
}

#[derive(Debug, Clone, Serialize)]
struct ExportOutput {
    event: String,
    state: String,
    config_path: String,
    db_path: String,
    output_path: String,
    archive_path: Option<String>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_archive_paths: Vec<String>,
    exported_at: DateTime<Utc>,
    publication_runtime_mode: String,
    publication_reason: String,
    publication_truth_complete: bool,
    fresh_under_export_gate: bool,
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    published_wallet_count: usize,
    wallet_metrics_snapshot_rows: usize,
    fresh_under_current_gate: bool,
    runtime_cursor_ts: DateTime<Utc>,
    runtime_cursor_slot: u64,
    runtime_cursor_signature: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PublicationTruthExportBlockerReasonClass {
    PublicationTruthExportGateSatisfied,
    PublicationTruthExportBlockedOnReplaySolLegIncomplete,
    PublicationTruthExportBlockedOnOtherPublishableCheckpointReason,
    PublicationTruthExportBlockedOnIncompleteOrStaleTruthWithoutCheckpointExplanation,
    PublicationTruthExportBlockerUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct PublicationTruthExportBlockerDiagnostic {
    publication_truth_export_blocker_observed: bool,
    publication_truth_export_blocker_reason_class: PublicationTruthExportBlockerReasonClass,
    publication_truth_export_blocker_explanation: String,
    publication_truth_export_blocker_top_level_proven_from_publication_state: bool,
    publication_truth_export_checkpoint_headline_budget_ms: u64,
    publication_truth_export_checkpoint_headline_budget_source: String,
    // Unreached checkpoint-headline stages keep their elapsed/budget-remaining telemetry at 0.
    publication_truth_export_checkpoint_headline_total_elapsed_ms: u64,
    publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms: u64,
    publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms: u64,
    publication_truth_export_checkpoint_headline_row_count_elapsed_ms: u64,
    publication_truth_export_checkpoint_headline_prepare_elapsed_ms: u64,
    publication_truth_export_checkpoint_headline_step_elapsed_ms: u64,
    publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open: u64,
    publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup: u64,
    publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count: u64,
    publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare: u64,
    publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step: u64,
    publication_truth_export_checkpoint_headline_attempted: bool,
    publication_truth_export_checkpoint_headline_completed: bool,
    publication_truth_export_checkpoint_headline_budget_exhausted: bool,
    publication_truth_export_checkpoint_headline_stage: Option<String>,
    publication_truth_export_checkpoint_headline_explanation: Option<String>,
    publication_truth_export_blocker_enrichment_attempted: bool,
    publication_truth_export_blocker_enrichment_completed: bool,
    publication_truth_export_blocker_enrichment_budget_exhausted: bool,
    publication_truth_export_blocker_enrichment_stage: Option<String>,
    publication_truth_export_blocker_enrichment_explanation: Option<String>,
    config_path: String,
    runtime_db_path: Option<String>,
    recent_raw_db_path: Option<String>,
    export_gate_runtime_mode: Option<String>,
    export_gate_reason: Option<String>,
    publication_truth_complete: Option<bool>,
    fresh_under_export_gate: Option<bool>,
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    published_wallet_count: Option<usize>,
    persisted_rebuild_checkpoint_exists: Option<bool>,
    persisted_rebuild_checkpoint_updated_at: Option<DateTime<Utc>>,
    persisted_rebuild_checkpoint_state_json_bytes: Option<usize>,
    persisted_rebuild_checkpoint_state_json_bytes_probe_attempted: bool,
    persisted_rebuild_checkpoint_state_json_bytes_probe_completed: bool,
    persisted_rebuild_checkpoint_state_json_bytes_probe_budget_exhausted: bool,
    persisted_rebuild_checkpoint_state_json_bytes_probe_stage: Option<String>,
    persisted_rebuild_checkpoint_state_json_bytes_probe_explanation: Option<String>,
    rebuild_phase: Option<String>,
    rebuild_replay_subphase: Option<String>,
    publishable_checkpoint_blocker: Option<String>,
    replay_incomplete: Option<bool>,
    replay_sol_leg_incomplete_reason_class: Option<ReplaySolLegIncompleteReasonClass>,
    replay_sol_leg_incomplete_explanation: Option<String>,
    checkpoint_exact_target_surface_exists: Option<bool>,
    checkpoint_exact_target_surface_repairable_for_resume: Option<bool>,
    checkpoint_replay_candidate_activity_backfill_required: Option<bool>,
    checkpoint_replay_candidate_activity_backfill_pending: Option<bool>,
    source_comparison_applicable: Option<bool>,
    source_scan_target_buy_mint_filter_active: Option<bool>,
    source_scan_target_buy_mint_count: Option<usize>,
    source_rows_exist_beyond_stored_replay_checkpoint: Option<bool>,
    source_rows_ahead_count: Option<usize>,
    source_rows_ahead_pages_scanned: Option<usize>,
    source_rows_ahead_first_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    source_rows_ahead_last_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    source_scan_access_path: Option<String>,
    source_scan_time_budget_exhausted: Option<bool>,
}

impl PublicationTruthExportBlockerDiagnostic {
    fn unproven(config_path: &Path, explanation: String) -> Self {
        Self {
            publication_truth_export_blocker_observed: false,
            publication_truth_export_blocker_reason_class:
                PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockerUnprovenDueToMissingEvidence,
            publication_truth_export_blocker_explanation: explanation,
            publication_truth_export_blocker_top_level_proven_from_publication_state: false,
            publication_truth_export_checkpoint_headline_budget_ms:
                DEFAULT_PUBLICATION_TRUTH_CHECKPOINT_HEADLINE_BUDGET_MS,
            publication_truth_export_checkpoint_headline_budget_source:
                DEFAULT_PUBLICATION_TRUTH_CHECKPOINT_HEADLINE_BUDGET_SOURCE.to_string(),
            publication_truth_export_checkpoint_headline_total_elapsed_ms: 0,
            publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms: 0,
            publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms: 0,
            publication_truth_export_checkpoint_headline_row_count_elapsed_ms: 0,
            publication_truth_export_checkpoint_headline_prepare_elapsed_ms: 0,
            publication_truth_export_checkpoint_headline_step_elapsed_ms: 0,
            publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open: 0,
            publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup: 0,
            publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count: 0,
            publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare: 0,
            publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step: 0,
            publication_truth_export_checkpoint_headline_attempted: false,
            publication_truth_export_checkpoint_headline_completed: false,
            publication_truth_export_checkpoint_headline_budget_exhausted: false,
            publication_truth_export_checkpoint_headline_stage: None,
            publication_truth_export_checkpoint_headline_explanation: None,
            publication_truth_export_blocker_enrichment_attempted: false,
            publication_truth_export_blocker_enrichment_completed: false,
            publication_truth_export_blocker_enrichment_budget_exhausted: false,
            publication_truth_export_blocker_enrichment_stage: None,
            publication_truth_export_blocker_enrichment_explanation: None,
            config_path: config_path.display().to_string(),
            runtime_db_path: None,
            recent_raw_db_path: None,
            export_gate_runtime_mode: None,
            export_gate_reason: None,
            publication_truth_complete: None,
            fresh_under_export_gate: None,
            last_published_at: None,
            last_published_window_start: None,
            published_wallet_count: None,
            persisted_rebuild_checkpoint_exists: None,
            persisted_rebuild_checkpoint_updated_at: None,
            persisted_rebuild_checkpoint_state_json_bytes: None,
            persisted_rebuild_checkpoint_state_json_bytes_probe_attempted: false,
            persisted_rebuild_checkpoint_state_json_bytes_probe_completed: false,
            persisted_rebuild_checkpoint_state_json_bytes_probe_budget_exhausted: false,
            persisted_rebuild_checkpoint_state_json_bytes_probe_stage: None,
            persisted_rebuild_checkpoint_state_json_bytes_probe_explanation: None,
            rebuild_phase: None,
            rebuild_replay_subphase: None,
            publishable_checkpoint_blocker: None,
            replay_incomplete: None,
            replay_sol_leg_incomplete_reason_class: None,
            replay_sol_leg_incomplete_explanation: None,
            checkpoint_exact_target_surface_exists: None,
            checkpoint_exact_target_surface_repairable_for_resume: None,
            checkpoint_replay_candidate_activity_backfill_required: None,
            checkpoint_replay_candidate_activity_backfill_pending: None,
            source_comparison_applicable: None,
            source_scan_target_buy_mint_filter_active: None,
            source_scan_target_buy_mint_count: None,
            source_rows_exist_beyond_stored_replay_checkpoint: None,
            source_rows_ahead_count: None,
            source_rows_ahead_pages_scanned: None,
            source_rows_ahead_first_cursor: None,
            source_rows_ahead_last_cursor: None,
            source_scan_access_path: None,
            source_scan_time_budget_exhausted: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplaySolLegBlockerReasonClass {
    ReplaySolLegBlockerProvenCurrent,
    ReplaySolLegBlockerNotCurrentExportBlocker,
    ReplaySolLegBlockerUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReplaySolLegBlockerDiagnostic {
    replay_sol_leg_blocker_observed: bool,
    replay_sol_leg_blocker_reason_class: ReplaySolLegBlockerReasonClass,
    replay_sol_leg_blocker_explanation: String,
    replay_sol_leg_blocker_prerequisite_reason_class: PublicationTruthExportBlockerReasonClass,
    replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed: bool,
    replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted: bool,
    replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage: Option<String>,
    replay_sol_leg_blocker_prerequisite_total_elapsed_ms: u64,
    config_path: String,
    runtime_db_path: Option<String>,
    recent_raw_db_path: Option<String>,
    export_gate_runtime_mode: Option<String>,
    export_gate_reason: Option<String>,
    publication_truth_complete: Option<bool>,
    fresh_under_export_gate: Option<bool>,
    persisted_rebuild_checkpoint_exists: Option<bool>,
    persisted_rebuild_checkpoint_updated_at: Option<DateTime<Utc>>,
    rebuild_phase: Option<String>,
    rebuild_replay_subphase: Option<String>,
    replay_incomplete: Option<bool>,
    replay_sol_leg_incomplete_reason_class: Option<ReplaySolLegIncompleteReasonClass>,
    replay_sol_leg_incomplete_explanation: Option<String>,
    checkpoint_exact_target_surface_exists: Option<bool>,
    checkpoint_exact_target_surface_repairable_for_resume: Option<bool>,
    checkpoint_replay_candidate_activity_backfill_required: Option<bool>,
    checkpoint_replay_candidate_activity_backfill_pending: Option<bool>,
    source_comparison_applicable: Option<bool>,
    source_scan_target_buy_mint_filter_active: Option<bool>,
    source_scan_target_buy_mint_count: Option<usize>,
    source_rows_exist_beyond_stored_replay_checkpoint: Option<bool>,
    source_rows_ahead_count: Option<usize>,
    source_rows_ahead_pages_scanned: Option<usize>,
    source_rows_ahead_first_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    source_rows_ahead_last_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    source_scan_access_path: Option<String>,
    source_scan_time_budget_exhausted: Option<bool>,
    replay_sol_leg_blocker_budget_ms: u64,
    replay_sol_leg_blocker_budget_source: String,
    replay_sol_leg_blocker_total_elapsed_ms: u64,
    replay_sol_leg_blocker_checkpoint_headline_elapsed_ms: u64,
    replay_sol_leg_blocker_deep_reason_elapsed_ms: u64,
    replay_sol_leg_blocker_budget_exhausted: bool,
    replay_sol_leg_blocker_budget_exhausted_stage: Option<String>,
}

impl ReplaySolLegBlockerDiagnostic {
    fn from_publication_truth_export_blocker(
        diagnostic: &PublicationTruthExportBlockerDiagnostic,
    ) -> Self {
        Self {
            replay_sol_leg_blocker_observed: diagnostic.publication_truth_export_blocker_observed,
            replay_sol_leg_blocker_reason_class:
                ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence,
            replay_sol_leg_blocker_explanation: diagnostic
                .publication_truth_export_blocker_explanation
                .clone(),
            replay_sol_leg_blocker_prerequisite_reason_class: diagnostic
                .publication_truth_export_blocker_reason_class,
            replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed: diagnostic
                .publication_truth_export_checkpoint_headline_completed,
            replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted: diagnostic
                .publication_truth_export_checkpoint_headline_budget_exhausted,
            replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage: diagnostic
                .publication_truth_export_checkpoint_headline_stage
                .clone(),
            replay_sol_leg_blocker_prerequisite_total_elapsed_ms: 0,
            config_path: diagnostic.config_path.clone(),
            runtime_db_path: diagnostic.runtime_db_path.clone(),
            recent_raw_db_path: diagnostic.recent_raw_db_path.clone(),
            export_gate_runtime_mode: diagnostic.export_gate_runtime_mode.clone(),
            export_gate_reason: diagnostic.export_gate_reason.clone(),
            publication_truth_complete: diagnostic.publication_truth_complete,
            fresh_under_export_gate: diagnostic.fresh_under_export_gate,
            persisted_rebuild_checkpoint_exists: diagnostic.persisted_rebuild_checkpoint_exists,
            persisted_rebuild_checkpoint_updated_at: diagnostic
                .persisted_rebuild_checkpoint_updated_at,
            rebuild_phase: diagnostic.rebuild_phase.clone(),
            rebuild_replay_subphase: diagnostic.rebuild_replay_subphase.clone(),
            replay_incomplete: diagnostic.replay_incomplete,
            replay_sol_leg_incomplete_reason_class: None,
            replay_sol_leg_incomplete_explanation: None,
            checkpoint_exact_target_surface_exists: None,
            checkpoint_exact_target_surface_repairable_for_resume: None,
            checkpoint_replay_candidate_activity_backfill_required: None,
            checkpoint_replay_candidate_activity_backfill_pending: None,
            source_comparison_applicable: None,
            source_scan_target_buy_mint_filter_active: None,
            source_scan_target_buy_mint_count: None,
            source_rows_exist_beyond_stored_replay_checkpoint: None,
            source_rows_ahead_count: None,
            source_rows_ahead_pages_scanned: None,
            source_rows_ahead_first_cursor: None,
            source_rows_ahead_last_cursor: None,
            source_scan_access_path: None,
            source_scan_time_budget_exhausted: None,
            replay_sol_leg_blocker_budget_ms: DEFAULT_REPLAY_SOL_LEG_BLOCKER_BUDGET_MS,
            replay_sol_leg_blocker_budget_source:
                DEFAULT_REPLAY_SOL_LEG_BLOCKER_BUDGET_SOURCE.to_string(),
            replay_sol_leg_blocker_total_elapsed_ms: 0,
            replay_sol_leg_blocker_checkpoint_headline_elapsed_ms: diagnostic
                .publication_truth_export_checkpoint_headline_total_elapsed_ms,
            replay_sol_leg_blocker_deep_reason_elapsed_ms: 0,
            replay_sol_leg_blocker_budget_exhausted: false,
            replay_sol_leg_blocker_budget_exhausted_stage: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PublicationTruthExportBlockerTopLevelState {
    GateSatisfied,
    ReplaySolLegIncomplete,
    OtherCheckpoint(String),
    IncompleteOrStaleWithoutCheckpointExplanation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationTruthExportCheckpointHeadlineOutcome {
    Completed { allow_deep_replay: bool },
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationTruthExportCheckpointRowMetaStage {
    SchemaLookup,
    RowCount,
    PreparePrimaryKeyLookup,
    StepPrimaryKeyLookup,
}

impl PublicationTruthExportCheckpointRowMetaStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::SchemaLookup => "load_persisted_rebuild_row_meta_schema_lookup",
            Self::RowCount => "load_persisted_rebuild_row_meta_row_count",
            Self::PreparePrimaryKeyLookup => {
                "load_persisted_rebuild_row_meta_prepare_primary_key_lookup"
            }
            Self::StepPrimaryKeyLookup => "load_persisted_rebuild_row_meta_step_primary_key_lookup",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicationTruthExportCheckpointRowMeta {
    checkpoint_exists: bool,
    rebuild_phase: Option<String>,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PublicationTruthExportCheckpointHeadlineTiming {
    total_elapsed_ms: u64,
    runtime_db_open_elapsed_ms: u64,
    schema_lookup_elapsed_ms: u64,
    row_count_elapsed_ms: u64,
    prepare_elapsed_ms: u64,
    step_elapsed_ms: u64,
    budget_remaining_ms_after_open: u64,
    budget_remaining_ms_after_schema_lookup: u64,
    budget_remaining_ms_after_row_count: u64,
    budget_remaining_ms_after_prepare: u64,
    budget_remaining_ms_after_step: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PublicationTruthExportCheckpointRowMetaLoadOutcome {
    Completed(PublicationTruthExportCheckpointRowMeta),
    BudgetExhausted(PublicationTruthExportCheckpointRowMetaStage),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicationTruthExportCheckpointRowMetaLoadResult {
    outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome,
    timing: PublicationTruthExportCheckpointHeadlineTiming,
}

fn export_gate_reason_publishable_checkpoint_blocker(reason: &str) -> Option<&str> {
    reason
        .strip_prefix(PUBLICATION_TRUTH_WITHHELD_PREFIX)
        .filter(|blocker| !blocker.is_empty())
}

fn infer_checkpoint_phase_and_subphase_from_blocker(
    blocker: &str,
) -> (Option<&'static str>, Option<&'static str>, Option<bool>) {
    match blocker {
        "replay_wallet_stats_incomplete" => (Some("replay"), Some("wallet_stats"), Some(true)),
        "replay_candidate_activity_backfill_incomplete" => {
            (Some("replay"), Some("activity_backfill"), Some(true))
        }
        "replay_sol_leg_incomplete" => (Some("replay"), Some("sol_leg"), Some(true)),
        "replay_post_wallet_stats_handoff_pending" => (Some("replay"), None, Some(true)),
        blocker if blocker.starts_with("collect_buy_mints_") => {
            (Some("collect_buy_mints"), None, Some(false))
        }
        blocker if blocker.starts_with("token_quality_") => {
            (Some("resolve_token_quality"), None, Some(false))
        }
        blocker if blocker.starts_with("publish_pending_") => {
            (Some("publish_pending"), None, Some(false))
        }
        _ => (None, None, None),
    }
}

fn set_publication_truth_export_checkpoint_headline_not_attempted(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    explanation: impl Into<String>,
) {
    diagnostic.publication_truth_export_checkpoint_headline_attempted = false;
    diagnostic.publication_truth_export_checkpoint_headline_completed = false;
    diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted = false;
    diagnostic.publication_truth_export_checkpoint_headline_stage = None;
    diagnostic.publication_truth_export_checkpoint_headline_explanation = Some(explanation.into());
}

fn set_publication_truth_export_checkpoint_headline_failed(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    stage: Option<String>,
    budget_exhausted: bool,
    explanation: impl Into<String>,
) {
    diagnostic.publication_truth_export_checkpoint_headline_attempted = true;
    diagnostic.publication_truth_export_checkpoint_headline_completed = false;
    diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted = budget_exhausted;
    diagnostic.publication_truth_export_checkpoint_headline_stage = stage;
    diagnostic.publication_truth_export_checkpoint_headline_explanation = Some(explanation.into());
}

fn set_publication_truth_export_checkpoint_headline_completed(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    stage: Option<String>,
    explanation: impl Into<String>,
) {
    diagnostic.publication_truth_export_checkpoint_headline_attempted = true;
    diagnostic.publication_truth_export_checkpoint_headline_completed = true;
    diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted = false;
    diagnostic.publication_truth_export_checkpoint_headline_stage = stage;
    diagnostic.publication_truth_export_checkpoint_headline_explanation = Some(explanation.into());
}

fn sync_publication_truth_export_checkpoint_headline_timing(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    timing: &PublicationTruthExportCheckpointHeadlineTiming,
) {
    diagnostic.publication_truth_export_checkpoint_headline_total_elapsed_ms =
        timing.total_elapsed_ms;
    diagnostic.publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms =
        timing.runtime_db_open_elapsed_ms;
    diagnostic.publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms =
        timing.schema_lookup_elapsed_ms;
    diagnostic.publication_truth_export_checkpoint_headline_row_count_elapsed_ms =
        timing.row_count_elapsed_ms;
    diagnostic.publication_truth_export_checkpoint_headline_prepare_elapsed_ms =
        timing.prepare_elapsed_ms;
    diagnostic.publication_truth_export_checkpoint_headline_step_elapsed_ms =
        timing.step_elapsed_ms;
    diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open =
        timing.budget_remaining_ms_after_open;
    diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup =
        timing.budget_remaining_ms_after_schema_lookup;
    diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count =
        timing.budget_remaining_ms_after_row_count;
    diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare =
        timing.budget_remaining_ms_after_prepare;
    diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step =
        timing.budget_remaining_ms_after_step;
}

fn set_publication_truth_export_state_json_bytes_probe_not_attempted(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    explanation: impl Into<String>,
) {
    diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_attempted = false;
    diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_completed = false;
    diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_budget_exhausted = false;
    diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_stage = None;
    diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_explanation =
        Some(explanation.into());
}

fn set_publication_truth_export_enrichment_not_attempted(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    explanation: impl Into<String>,
) {
    diagnostic.publication_truth_export_blocker_enrichment_attempted = false;
    diagnostic.publication_truth_export_blocker_enrichment_completed = false;
    diagnostic.publication_truth_export_blocker_enrichment_budget_exhausted = false;
    diagnostic.publication_truth_export_blocker_enrichment_stage = None;
    diagnostic.publication_truth_export_blocker_enrichment_explanation = Some(explanation.into());
}

fn set_publication_truth_export_enrichment_failed(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    stage: Option<String>,
    budget_exhausted: bool,
    explanation: impl Into<String>,
) {
    diagnostic.publication_truth_export_blocker_enrichment_attempted = true;
    diagnostic.publication_truth_export_blocker_enrichment_completed = false;
    diagnostic.publication_truth_export_blocker_enrichment_budget_exhausted = budget_exhausted;
    diagnostic.publication_truth_export_blocker_enrichment_stage = stage;
    diagnostic.publication_truth_export_blocker_enrichment_explanation = Some(explanation.into());
}

fn set_publication_truth_export_enrichment_completed(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    stage: Option<String>,
    explanation: impl Into<String>,
) {
    diagnostic.publication_truth_export_blocker_enrichment_attempted = true;
    diagnostic.publication_truth_export_blocker_enrichment_completed = true;
    diagnostic.publication_truth_export_blocker_enrichment_budget_exhausted = false;
    diagnostic.publication_truth_export_blocker_enrichment_stage = stage;
    diagnostic.publication_truth_export_blocker_enrichment_explanation = Some(explanation.into());
}

fn classify_publication_truth_export_blocker_from_publication_state(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    publication_truth_complete: bool,
    fresh_under_export_gate: bool,
) -> PublicationTruthExportBlockerTopLevelState {
    diagnostic.publication_truth_export_blocker_observed = true;
    diagnostic.publication_truth_export_blocker_top_level_proven_from_publication_state = true;
    let runtime_mode = diagnostic
        .export_gate_runtime_mode
        .as_deref()
        .unwrap_or("unknown");
    let export_gate_reason = diagnostic
        .export_gate_reason
        .as_deref()
        .unwrap_or("unknown");

    if publication_truth_complete && fresh_under_export_gate {
        diagnostic.publication_truth_export_blocker_reason_class =
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportGateSatisfied;
        diagnostic.publication_truth_export_blocker_explanation = format!(
            "publication truth export gate is currently satisfied from persisted discovery publication state: runtime_mode={runtime_mode}, reason={export_gate_reason}, publication_truth_complete=true, fresh_under_export_gate=true"
        );
        set_publication_truth_export_checkpoint_headline_not_attempted(
            diagnostic,
            "checkpoint headline enrichment was not attempted because persisted publication truth already satisfies the export gate",
        );
        set_publication_truth_export_state_json_bytes_probe_not_attempted(
            diagnostic,
            "persisted rebuild state_json byte-length probing was not attempted because checkpoint headline enrichment was not needed after the export gate was already satisfied",
        );
        set_publication_truth_export_enrichment_not_attempted(
            diagnostic,
            "bounded checkpoint/replay enrichment was not attempted because persisted publication truth already satisfies the export gate",
        );
        return PublicationTruthExportBlockerTopLevelState::GateSatisfied;
    }

    if let Some(blocker) = diagnostic
        .export_gate_reason
        .as_deref()
        .and_then(export_gate_reason_publishable_checkpoint_blocker)
    {
        diagnostic.publishable_checkpoint_blocker = Some(blocker.to_string());
        let (phase, replay_subphase, replay_incomplete) =
            infer_checkpoint_phase_and_subphase_from_blocker(blocker);
        if let Some(phase) = phase {
            diagnostic.rebuild_phase = Some(phase.to_string());
        }
        if let Some(replay_subphase) = replay_subphase {
            diagnostic.rebuild_replay_subphase = Some(replay_subphase.to_string());
        }
        if let Some(replay_incomplete) = replay_incomplete {
            diagnostic.replay_incomplete = Some(replay_incomplete);
        }
        if blocker == "replay_sol_leg_incomplete" {
            diagnostic.publication_truth_export_blocker_reason_class =
                PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnReplaySolLegIncomplete;
            diagnostic.publication_truth_export_blocker_explanation = format!(
                "publication truth export is currently blocked on replay_sol_leg_incomplete as already persisted in discovery publication state: runtime_mode={runtime_mode}, reason={export_gate_reason}"
            );
            return PublicationTruthExportBlockerTopLevelState::ReplaySolLegIncomplete;
        }

        diagnostic.publication_truth_export_blocker_reason_class =
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnOtherPublishableCheckpointReason;
        diagnostic.publication_truth_export_blocker_explanation = format!(
            "publication truth export is currently blocked on publishable checkpoint blocker {blocker} as already persisted in discovery publication state: runtime_mode={runtime_mode}, reason={export_gate_reason}"
        );
        return PublicationTruthExportBlockerTopLevelState::OtherCheckpoint(blocker.to_string());
    }

    diagnostic.publication_truth_export_blocker_reason_class =
        PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnIncompleteOrStaleTruthWithoutCheckpointExplanation;
    diagnostic.publication_truth_export_blocker_explanation = format!(
        "publication truth export is currently unsatisfied without a checkpoint-coded export gate reason in persisted discovery publication state: runtime_mode={runtime_mode}, reason={export_gate_reason}, publication_truth_complete={publication_truth_complete}, fresh_under_export_gate={fresh_under_export_gate}"
    );
    set_publication_truth_export_checkpoint_headline_not_attempted(
        diagnostic,
        "checkpoint headline enrichment was not attempted because persisted discovery publication state does not encode a publishable-checkpoint blocker family",
    );
    set_publication_truth_export_state_json_bytes_probe_not_attempted(
        diagnostic,
        "persisted rebuild state_json byte-length probing was not attempted because persisted discovery publication state does not encode a publishable-checkpoint blocker family",
    );
    set_publication_truth_export_enrichment_not_attempted(
        diagnostic,
        "bounded checkpoint/replay enrichment was not attempted because persisted discovery publication state does not encode a publishable-checkpoint blocker family",
    );
    PublicationTruthExportBlockerTopLevelState::IncompleteOrStaleWithoutCheckpointExplanation
}

fn apply_publication_truth_export_checkpoint_headline_enrichment(
    diagnostic: &mut PublicationTruthExportBlockerDiagnostic,
    _top_level_state: &PublicationTruthExportBlockerTopLevelState,
    runtime_db_path: &Path,
    budget: StdDuration,
) -> PublicationTruthExportCheckpointHeadlineOutcome {
    set_publication_truth_export_state_json_bytes_probe_not_attempted(
        diagnostic,
        "persisted rebuild state_json byte-length probing was skipped in the primary operator checkpoint headline path to avoid scanning large persisted state blobs before deciding whether deeper work is worthwhile",
    );

    let row_meta = match load_publication_truth_export_checkpoint_row_meta_direct_with_budget(
        runtime_db_path,
        budget,
    ) {
        Ok(load_result) => {
            sync_publication_truth_export_checkpoint_headline_timing(
                diagnostic,
                &load_result.timing,
            );
            match load_result.outcome {
                PublicationTruthExportCheckpointRowMetaLoadOutcome::Completed(row_meta) => row_meta,
                PublicationTruthExportCheckpointRowMetaLoadOutcome::BudgetExhausted(stage) => {
                    set_publication_truth_export_checkpoint_headline_failed(
                        diagnostic,
                        Some(stage.as_str().to_string()),
                        true,
                        format!(
                            "runtime-db-only checkpoint headline enrichment exhausted its budget while running the cheap persisted rebuild row-meta path (stage={})",
                            stage.as_str()
                        ),
                    );
                    return PublicationTruthExportCheckpointHeadlineOutcome::Failed;
                }
            }
        }
        Err(error) => {
            set_publication_truth_export_checkpoint_headline_failed(
                diagnostic,
                Some("load_persisted_rebuild_row_meta_open_read_only".to_string()),
                false,
                format!(
                    "runtime-db-only checkpoint headline enrichment could not load cheap persisted rebuild row meta: {error:#}"
                ),
            );
            return PublicationTruthExportCheckpointHeadlineOutcome::Failed;
        }
    };

    diagnostic.persisted_rebuild_checkpoint_exists = Some(row_meta.checkpoint_exists);
    if let Some(updated_at) = row_meta.updated_at {
        diagnostic.persisted_rebuild_checkpoint_updated_at = Some(updated_at);
    }
    if let Some(rebuild_phase) = row_meta.rebuild_phase {
        diagnostic.rebuild_phase = Some(rebuild_phase);
    }

    let checkpoint_exists = row_meta.checkpoint_exists;
    let explanation = if checkpoint_exists {
        "runtime-db-only checkpoint headline enrichment completed from the proven cheap persisted rebuild row-meta path; state_json byte-length probing, persisted-state parsing, and recent_raw-backed replay proof were skipped on the primary operator hot path"
            .to_string()
    } else {
        "runtime-db-only checkpoint headline enrichment completed through the proven cheap persisted rebuild row-meta path and confirmed that no persisted rebuild checkpoint row is currently present"
            .to_string()
    };
    set_publication_truth_export_checkpoint_headline_completed(
        diagnostic,
        Some("complete".to_string()),
        explanation,
    );
    PublicationTruthExportCheckpointHeadlineOutcome::Completed {
        allow_deep_replay: false,
    }
}

fn load_publication_truth_export_checkpoint_row_meta_direct_with_budget(
    runtime_db_path: &Path,
    budget: StdDuration,
) -> Result<PublicationTruthExportCheckpointRowMetaLoadResult> {
    let total_started_at = Instant::now();
    let open_started_at = Instant::now();
    let conn = Connection::open_with_flags(
        runtime_db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| {
        format!(
            "failed opening runtime sqlite db read-only for cheap checkpoint row-meta path {}",
            runtime_db_path.display()
        )
    })?;
    let mut timing = PublicationTruthExportCheckpointHeadlineTiming::default();
    timing.runtime_db_open_elapsed_ms = elapsed_ms(open_started_at);
    timing.budget_remaining_ms_after_open = remaining_budget_ms(budget, total_started_at);
    if total_started_at.elapsed() >= budget {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::BudgetExhausted(
                PublicationTruthExportCheckpointRowMetaStage::SchemaLookup,
            ),
            timing,
        });
    }

    let schema_lookup_started_at = Instant::now();
    let table_exists = conn
        .query_row(CHECKPOINT_HEADLINE_ROW_META_TABLE_EXISTS_SQL, [], |row| {
            row.get::<_, i64>(0)
        })
        .context("failed checking discovery_persisted_rebuild_state table existence")?
        != 0;
    timing.schema_lookup_elapsed_ms = elapsed_ms(schema_lookup_started_at);
    timing.budget_remaining_ms_after_schema_lookup = remaining_budget_ms(budget, total_started_at);
    if total_started_at.elapsed() >= budget {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::BudgetExhausted(
                PublicationTruthExportCheckpointRowMetaStage::SchemaLookup,
            ),
            timing,
        });
    }
    if !table_exists {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::Completed(
                PublicationTruthExportCheckpointRowMeta {
                    checkpoint_exists: false,
                    rebuild_phase: None,
                    updated_at: None,
                },
            ),
            timing,
        });
    }

    let row_count_started_at = Instant::now();
    let row_count = conn
        .query_row(CHECKPOINT_HEADLINE_ROW_META_ROW_COUNT_SQL, [], |row| {
            row.get::<_, i64>(0)
        })
        .context("failed counting persisted rebuild checkpoint row id=1")?
        .max(0);
    timing.row_count_elapsed_ms = elapsed_ms(row_count_started_at);
    timing.budget_remaining_ms_after_row_count = remaining_budget_ms(budget, total_started_at);
    if total_started_at.elapsed() >= budget {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::BudgetExhausted(
                PublicationTruthExportCheckpointRowMetaStage::RowCount,
            ),
            timing,
        });
    }
    if row_count == 0 {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::Completed(
                PublicationTruthExportCheckpointRowMeta {
                    checkpoint_exists: false,
                    rebuild_phase: None,
                    updated_at: None,
                },
            ),
            timing,
        });
    }

    let prepare_started_at = Instant::now();
    let mut stmt = conn
        .prepare(CHECKPOINT_HEADLINE_ROW_META_SQL)
        .context("failed preparing persisted rebuild checkpoint row-meta query")?;
    timing.prepare_elapsed_ms = elapsed_ms(prepare_started_at);
    timing.budget_remaining_ms_after_prepare = remaining_budget_ms(budget, total_started_at);
    if total_started_at.elapsed() >= budget {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::BudgetExhausted(
                PublicationTruthExportCheckpointRowMetaStage::PreparePrimaryKeyLookup,
            ),
            timing,
        });
    }

    let step_started_at = Instant::now();
    let raw = stmt
        .query_row([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .optional()
        .context("failed stepping persisted rebuild checkpoint row-meta query")?;
    timing.step_elapsed_ms = elapsed_ms(step_started_at);
    timing.budget_remaining_ms_after_step = remaining_budget_ms(budget, total_started_at);
    if total_started_at.elapsed() >= budget {
        timing.total_elapsed_ms = elapsed_ms(total_started_at);
        return Ok(PublicationTruthExportCheckpointRowMetaLoadResult {
            outcome: PublicationTruthExportCheckpointRowMetaLoadOutcome::BudgetExhausted(
                PublicationTruthExportCheckpointRowMetaStage::StepPrimaryKeyLookup,
            ),
            timing,
        });
    }

    let outcome = match raw {
        Some((phase_raw, updated_at_raw)) => {
            let phase = DiscoveryPersistedRebuildPhase::parse(&phase_raw)
                .map(|phase| phase.as_str().to_string())
                .context("failed parsing persisted rebuild checkpoint phase")?;
            let updated_at = DateTime::parse_from_rfc3339(&updated_at_raw)
                .map(|value| value.with_timezone(&Utc))
                .with_context(|| {
                    format!(
                        "failed parsing persisted rebuild checkpoint updated_at as RFC3339: {}",
                        updated_at_raw
                    )
                })?;
            PublicationTruthExportCheckpointRowMetaLoadOutcome::Completed(
                PublicationTruthExportCheckpointRowMeta {
                    checkpoint_exists: true,
                    rebuild_phase: Some(phase),
                    updated_at: Some(updated_at),
                },
            )
        }
        None => PublicationTruthExportCheckpointRowMetaLoadOutcome::Completed(
            PublicationTruthExportCheckpointRowMeta {
                checkpoint_exists: false,
                rebuild_phase: None,
                updated_at: None,
            },
        ),
    };
    timing.total_elapsed_ms = elapsed_ms(total_started_at);
    Ok(PublicationTruthExportCheckpointRowMetaLoadResult { outcome, timing })
}

#[cfg(test)]
thread_local! {
    static TEST_FORCE_REPLAY_SOL_LEG_BLOCKER_DEEP_REASON_BUDGET_EXHAUSTED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn arm_test_force_replay_sol_leg_blocker_deep_reason_budget_exhausted() {
    TEST_FORCE_REPLAY_SOL_LEG_BLOCKER_DEEP_REASON_BUDGET_EXHAUSTED
        .with(|flag| flag.set(true));
}

#[cfg(test)]
fn take_test_force_replay_sol_leg_blocker_deep_reason_budget_exhausted() -> bool {
    TEST_FORCE_REPLAY_SOL_LEG_BLOCKER_DEEP_REASON_BUDGET_EXHAUSTED
        .with(|flag| flag.replace(false))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplaySolLegBlockerDeepReasonOutcome {
    Completed(ReplaySolLegIncompleteDiagnostic),
    BudgetExhausted {
        stage: String,
        explanation: String,
    },
}

fn explain_replay_sol_leg_blocker_deep_reason_read_only(
    runtime_store: &SqliteStore,
    recent_raw_store: &SqliteStore,
) -> Result<ReplaySolLegBlockerDeepReasonOutcome> {
    #[cfg(test)]
    if take_test_force_replay_sol_leg_blocker_deep_reason_budget_exhausted() {
        return Ok(ReplaySolLegBlockerDeepReasonOutcome::BudgetExhausted {
            stage: REPLAY_SOL_LEG_BLOCKER_BUDGET_EXHAUSTED_STAGE.to_string(),
            explanation:
                "forced test budget exhaustion for bounded replay_sol_leg blocker deep proof"
                    .to_string(),
        });
    }

    let diagnostic =
        DiscoveryService::explain_replay_sol_leg_incomplete_read_only(runtime_store, recent_raw_store)?;
    if diagnostic.replay_sol_leg_incomplete_reason_class
        == ReplaySolLegIncompleteReasonClass::SourceFrontierUnprovenDiagnosticScanBudgetExhausted
    {
        return Ok(ReplaySolLegBlockerDeepReasonOutcome::BudgetExhausted {
            stage: REPLAY_SOL_LEG_BLOCKER_BUDGET_EXHAUSTED_STAGE.to_string(),
            explanation: diagnostic.replay_sol_leg_incomplete_explanation.clone(),
        });
    }
    Ok(ReplaySolLegBlockerDeepReasonOutcome::Completed(diagnostic))
}

fn remaining_budget_ms(budget: StdDuration, started_at: Instant) -> u64 {
    budget
        .saturating_sub(started_at.elapsed())
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn parse_args() -> Result<Option<Command>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Command>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut explain_recent_raw_promotion_blocker = false;
    let mut explain_recent_raw_catch_up_status = false;
    let mut explain_recent_raw_source_window_contract = false;
    let mut explain_recent_raw_promoted_retention_contract = false;
    let mut explain_recent_raw_replacement_promotion_contract = false;
    let mut explain_recent_raw_replacement_progress_contract = false;
    let mut explain_recent_raw_replacement_artifact_history_contract = false;
    let mut explain_recent_raw_replacement_attempt_telemetry = false;
    let mut deep_attempt_telemetry_scan = false;
    let mut explain_recent_raw_replacement_convergence = false;
    let mut explain_publication_truth_export_blocker = false;
    let mut explain_replay_sol_leg_blocker = false;
    let mut explain_recent_raw_staged_lineage = false;
    let mut explain_recent_raw_staged_regression = false;
    let mut explain_recent_raw_staged_birth = false;
    let mut explain_recent_raw_staged_window_seeding = false;
    let mut config_path: Option<PathBuf> = None;
    let mut state_root: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut scheduled = false;
    let mut force = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--explain-recent-raw-promotion-blocker" => {
                explain_recent_raw_promotion_blocker = true;
            }
            "--explain-recent-raw-catch-up-status" => {
                explain_recent_raw_catch_up_status = true;
            }
            "--explain-recent-raw-source-window-contract" => {
                explain_recent_raw_source_window_contract = true;
            }
            "--explain-recent-raw-promoted-retention-contract" => {
                explain_recent_raw_promoted_retention_contract = true;
            }
            "--explain-recent-raw-replacement-promotion-contract" => {
                explain_recent_raw_replacement_promotion_contract = true;
            }
            "--explain-recent-raw-replacement-progress-contract" => {
                explain_recent_raw_replacement_progress_contract = true;
            }
            "--explain-recent-raw-replacement-artifact-history-contract" => {
                explain_recent_raw_replacement_artifact_history_contract = true;
            }
            "--explain-recent-raw-replacement-attempt-telemetry" => {
                explain_recent_raw_replacement_attempt_telemetry = true;
            }
            "--explain-recent-raw-replacement-convergence" => {
                explain_recent_raw_replacement_convergence = true;
            }
            "--explain-publication-truth-export-blocker" => {
                explain_publication_truth_export_blocker = true;
            }
            "--explain-replay-sol-leg-blocker" => {
                explain_replay_sol_leg_blocker = true;
            }
            "--deep-attempt-telemetry-scan" => {
                deep_attempt_telemetry_scan = true;
            }
            "--explain-recent-raw-staged-lineage" => {
                explain_recent_raw_staged_lineage = true;
            }
            "--explain-recent-raw-staged-regression" => {
                explain_recent_raw_staged_regression = true;
            }
            "--explain-recent-raw-staged-birth" => {
                explain_recent_raw_staged_birth = true;
            }
            "--explain-recent-raw-staged-window-seeding" => {
                explain_recent_raw_staged_window_seeding = true;
            }
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--state-root" => {
                state_root = Some(PathBuf::from(parse_string_arg(
                    "--state-root",
                    args.next(),
                )?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--scheduled" => scheduled = true,
            "--force" => force = true,
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let explain_mode_count = usize::from(explain_recent_raw_promotion_blocker)
        + usize::from(explain_recent_raw_catch_up_status)
        + usize::from(explain_recent_raw_source_window_contract)
        + usize::from(explain_recent_raw_promoted_retention_contract)
        + usize::from(explain_recent_raw_replacement_promotion_contract)
        + usize::from(explain_recent_raw_replacement_progress_contract)
        + usize::from(explain_recent_raw_replacement_artifact_history_contract)
        + usize::from(explain_recent_raw_replacement_attempt_telemetry)
        + usize::from(explain_recent_raw_replacement_convergence)
        + usize::from(explain_publication_truth_export_blocker)
        + usize::from(explain_replay_sol_leg_blocker)
        + usize::from(explain_recent_raw_staged_lineage)
        + usize::from(explain_recent_raw_staged_regression)
        + usize::from(explain_recent_raw_staged_birth)
        + usize::from(explain_recent_raw_staged_window_seeding);
    if explain_mode_count > 1 {
        bail!(
            "--explain-recent-raw-promotion-blocker, --explain-recent-raw-catch-up-status, --explain-recent-raw-source-window-contract, --explain-recent-raw-promoted-retention-contract, --explain-recent-raw-replacement-promotion-contract, --explain-recent-raw-replacement-progress-contract, --explain-recent-raw-replacement-artifact-history-contract, --explain-recent-raw-replacement-attempt-telemetry, --explain-recent-raw-replacement-convergence, --explain-publication-truth-export-blocker, --explain-replay-sol-leg-blocker, --explain-recent-raw-staged-lineage, --explain-recent-raw-staged-regression, --explain-recent-raw-staged-window-seeding, and --explain-recent-raw-staged-birth are mutually exclusive"
        );
    }
    if deep_attempt_telemetry_scan && !explain_recent_raw_replacement_attempt_telemetry {
        bail!(
            "--deep-attempt-telemetry-scan requires --explain-recent-raw-replacement-attempt-telemetry"
        );
    }

    if explain_recent_raw_promotion_blocker {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!(
                "--explain-recent-raw-promotion-blocker only accepts --state-root and optional --json"
            );
        }
        return Ok(Some(Command::ExplainRecentRawPromotionBlocker(
            ExplainRecentRawPromotionBlockerConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_catch_up_status {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-catch-up-status only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawCatchUpStatus(
            ExplainRecentRawCatchUpStatusConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_source_window_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-source-window-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawSourceWindowContract(
            ExplainRecentRawSourceWindowContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_promoted_retention_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-promoted-retention-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawPromotedRetentionContract(
            ExplainRecentRawPromotedRetentionContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_replacement_promotion_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-promotion-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawReplacementPromotionContract(
            ExplainRecentRawReplacementPromotionContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_replacement_progress_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-progress-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawReplacementProgressContract(
            ExplainRecentRawReplacementProgressContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_replacement_artifact_history_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-artifact-history-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(
            Command::ExplainRecentRawReplacementArtifactHistoryContract(
                ExplainRecentRawReplacementArtifactHistoryContractConfig {
                    state_root: state_root
                        .ok_or_else(|| anyhow!("missing required --state-root"))?,
                    json,
                },
            ),
        ));
    }

    if explain_recent_raw_replacement_attempt_telemetry {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-attempt-telemetry only accepts --state-root, optional --json, and optional --deep-attempt-telemetry-scan");
        }
        return Ok(Some(Command::ExplainRecentRawReplacementAttemptTelemetry(
            ExplainRecentRawReplacementAttemptTelemetryConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
                deep_attempt_telemetry_scan,
            },
        )));
    }

    if explain_recent_raw_replacement_convergence {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-convergence only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawReplacementConvergence(
            ExplainRecentRawReplacementConvergenceConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_publication_truth_export_blocker {
        if state_root.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
            || deep_attempt_telemetry_scan
        {
            bail!(
                "--explain-publication-truth-export-blocker only accepts --config and optional --json"
            );
        }
        return Ok(Some(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
                json,
            },
        )));
    }

    if explain_replay_sol_leg_blocker {
        if state_root.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
            || deep_attempt_telemetry_scan
        {
            bail!(
                "--explain-replay-sol-leg-blocker only accepts --config and optional --json"
            );
        }
        return Ok(Some(Command::ExplainReplaySolLegBlocker(
            ExplainReplaySolLegBlockerConfig {
                config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_lineage {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!(
                "--explain-recent-raw-staged-lineage only accepts --state-root and optional --json"
            );
        }
        return Ok(Some(Command::ExplainRecentRawStagedLineage(
            ExplainRecentRawStagedLineageConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_regression {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-staged-regression only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawStagedRegression(
            ExplainRecentRawStagedRegressionConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_birth {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!(
                "--explain-recent-raw-staged-birth only accepts --state-root and optional --json"
            );
        }
        return Ok(Some(Command::ExplainRecentRawStagedBirth(
            ExplainRecentRawStagedBirthConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_window_seeding {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-staged-window-seeding only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawStagedWindowSeeding(
            ExplainRecentRawStagedWindowSeedingConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if state_root.is_some() {
        bail!(
            "--state-root requires --explain-recent-raw-promotion-blocker, --explain-recent-raw-catch-up-status, --explain-recent-raw-source-window-contract, --explain-recent-raw-promoted-retention-contract, --explain-recent-raw-replacement-promotion-contract, --explain-recent-raw-replacement-progress-contract, --explain-recent-raw-replacement-artifact-history-contract, --explain-recent-raw-replacement-attempt-telemetry, --explain-recent-raw-replacement-convergence, --explain-recent-raw-staged-lineage, --explain-recent-raw-staged-regression, --explain-recent-raw-staged-window-seeding, or --explain-recent-raw-staged-birth"
        );
    }
    if scheduled == output_path.is_some() {
        bail!("exactly one of --output or --scheduled must be provided");
    }

    Ok(Some(Command::Export(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        output_path,
        scheduled,
        force,
        json,
        now: now.unwrap_or_else(Utc::now),
    })))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn run_command(command: Command) -> Result<String> {
    match command {
        Command::Export(config) => run(config),
        Command::ExplainRecentRawPromotionBlocker(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_promotion_blocker_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw promotion blocker json")
            } else {
                Ok(render_recent_raw_promotion_blocker_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawCatchUpStatus(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_catch_up_status_read_only(&config.state_root)?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw catch-up status json")
            } else {
                Ok(render_recent_raw_catch_up_status_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawSourceWindowContract(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_source_window_contract_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw source window contract json")
            } else {
                Ok(render_recent_raw_source_window_contract_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawPromotedRetentionContract(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_promoted_retention_contract_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw promoted retention contract json")
            } else {
                Ok(render_recent_raw_promoted_retention_contract_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawReplacementPromotionContract(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw replacement promotion contract json")
            } else {
                Ok(render_recent_raw_replacement_promotion_contract_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawReplacementProgressContract(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_replacement_progress_contract_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw replacement progress contract json")
            } else {
                Ok(render_recent_raw_replacement_progress_contract_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawReplacementArtifactHistoryContract(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_replacement_artifact_history_contract_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic).context(
                    "failed serializing recent_raw replacement artifact history contract json",
                )
            } else {
                Ok(render_recent_raw_replacement_artifact_history_contract_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawReplacementAttemptTelemetry(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_with_deep_scan_read_only(
                    &config.state_root,
                    config.deep_attempt_telemetry_scan,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw replacement attempt telemetry json")
            } else {
                Ok(render_recent_raw_replacement_attempt_telemetry_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawReplacementConvergence(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw replacement convergence json")
            } else {
                Ok(render_recent_raw_replacement_convergence_human(&diagnostic))
            }
        }
        Command::ExplainPublicationTruthExportBlocker(config) => {
            let diagnostic =
                explain_publication_truth_export_blocker_read_only(&config.config_path, Utc::now());
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing publication truth export blocker json")
            } else {
                Ok(render_publication_truth_export_blocker_human(&diagnostic))
            }
        }
        Command::ExplainReplaySolLegBlocker(config) => {
            let diagnostic = explain_replay_sol_leg_blocker_read_only(&config.config_path, Utc::now());
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing replay_sol_leg blocker json")
            } else {
                Ok(render_replay_sol_leg_blocker_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedLineage(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_staged_lineage_read_only(&config.state_root)?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged lineage json")
            } else {
                Ok(render_recent_raw_staged_lineage_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedRegression(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_staged_regression_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged regression json")
            } else {
                Ok(render_recent_raw_staged_regression_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedBirth(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_staged_birth_read_only(&config.state_root)?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged birth json")
            } else {
                Ok(render_recent_raw_staged_birth_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedWindowSeeding(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_staged_window_seeding_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged window seeding json")
            } else {
                Ok(render_recent_raw_staged_window_seeding_human(&diagnostic))
            }
        }
    }
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );

    let output = if config.scheduled {
        run_scheduled(
            &config,
            &loaded_config.runtime_restore_ops.artifact_dir,
            loaded_config.runtime_restore_ops.artifact_cadence_minutes,
            loaded_config.runtime_restore_ops.artifact_retention,
            &db_path,
            &store,
            &discovery,
        )?
    } else {
        let output_path = resolve_relative_to_config(
            &config.config_path,
            config
                .output_path
                .as_deref()
                .expect("validated explicit output path"),
        );
        let artifact =
            export_runtime_artifact(&store, &discovery, config.now).context("artifact export")?;
        let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
        write_json_atomic(&output_path, &artifact)
            .with_context(|| format!("failed writing {}", output_path.display()))?;
        render_output(
            "written",
            &config.config_path,
            &db_path,
            &output_path,
            None,
            None,
            None,
            &[],
            &artifact,
            freshness.fresh_under_export_gate,
            freshness.fresh_under_current_gate,
        )
    };

    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing export output json")
    } else {
        Ok(render_human(&output))
    }
}

fn run_scheduled(
    config: &Config,
    configured_artifact_dir: &str,
    cadence_minutes: u64,
    retention: usize,
    db_path: &Path,
    store: &SqliteStore,
    discovery: &DiscoveryService,
) -> Result<ExportOutput> {
    let artifact_dir =
        resolve_relative_to_config(&config.config_path, Path::new(configured_artifact_dir));
    let latest_path = artifact_latest_path(&artifact_dir);
    if !config.force && latest_path.exists() {
        let latest_artifact: DiscoveryRuntimeArtifact = load_json(&latest_path)?;
        if config
            .now
            .signed_duration_since(latest_artifact.exported_at)
            < Duration::minutes(cadence_minutes.max(1) as i64)
        {
            let freshness =
                discovery.assess_runtime_artifact_freshness(&latest_artifact, config.now);
            return Ok(render_output(
                "skipped_not_due",
                &config.config_path,
                db_path,
                &latest_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                &[],
                &latest_artifact,
                freshness.fresh_under_export_gate,
                freshness.fresh_under_current_gate,
            ));
        }
    }

    let artifact =
        export_runtime_artifact(store, discovery, config.now).context("artifact export")?;
    let archive_path = artifact_archive_path(&artifact_dir, config.now);
    write_json_atomic(&archive_path, &artifact)
        .with_context(|| format!("failed writing {}", archive_path.display()))?;
    copy_atomic(&archive_path, &latest_path)
        .with_context(|| format!("failed updating {}", latest_path.display()))?;
    let pruned = prune_rotated_archives(
        &artifact_dir,
        ARTIFACT_ARCHIVE_PREFIX,
        ARTIFACT_ARCHIVE_SUFFIX,
        retention,
    )?;
    let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
    Ok(render_output(
        "written",
        &config.config_path,
        db_path,
        &latest_path,
        Some(&archive_path),
        Some(cadence_minutes),
        Some(retention),
        &pruned,
        &artifact,
        freshness.fresh_under_export_gate,
        freshness.fresh_under_current_gate,
    ))
}

fn export_runtime_artifact(
    store: &SqliteStore,
    discovery: &DiscoveryService,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    store.export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())
}

fn render_output(
    state: &str,
    config_path: &Path,
    db_path: &Path,
    output_path: &Path,
    archive_path: Option<&Path>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_archive_paths: &[PathBuf],
    artifact: &DiscoveryRuntimeArtifact,
    fresh_under_export_gate: bool,
    fresh_under_current_gate: bool,
) -> ExportOutput {
    ExportOutput {
        event: "discovery_runtime_export".to_string(),
        state: state.to_string(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        output_path: output_path.display().to_string(),
        archive_path: archive_path.map(|path| path.display().to_string()),
        cadence_minutes,
        retention,
        pruned_archive_paths: pruned_archive_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        exported_at: artifact.exported_at,
        publication_runtime_mode: artifact.publication_state.runtime_mode.as_str().to_string(),
        publication_reason: artifact.publication_state.reason.clone(),
        publication_truth_complete: artifact.publication_state.has_complete_publication_truth(),
        fresh_under_export_gate,
        last_published_at: artifact.publication_state.last_published_at,
        last_published_window_start: artifact.publication_state.last_published_window_start,
        published_wallet_count: artifact
            .publication_state
            .published_wallet_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0),
        wallet_metrics_snapshot_rows: artifact.published_wallet_metrics_snapshot.len(),
        fresh_under_current_gate,
        runtime_cursor_ts: artifact.runtime_cursor.ts_utc,
        runtime_cursor_slot: artifact.runtime_cursor.slot,
        runtime_cursor_signature: artifact.runtime_cursor.signature.clone(),
    }
}

fn render_human(output: &ExportOutput) -> String {
    [
        format!("event={}", output.event),
        format!("state={}", output.state),
        format!("config_path={}", output.config_path),
        format!("db_path={}", output.db_path),
        format!("output_path={}", output.output_path),
        format!(
            "archive_path={}",
            output.archive_path.as_deref().unwrap_or("null")
        ),
        format!(
            "cadence_minutes={}",
            output
                .cadence_minutes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "retention={}",
            output
                .retention
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("pruned_archives={}", output.pruned_archive_paths.len()),
        format!("exported_at={}", output.exported_at.to_rfc3339()),
        format!(
            "publication_runtime_mode={}",
            output.publication_runtime_mode
        ),
        format!("publication_reason={}", output.publication_reason),
        format!(
            "publication_truth_complete={}",
            output.publication_truth_complete
        ),
        format!("fresh_under_export_gate={}", output.fresh_under_export_gate),
        format!(
            "last_published_at={}",
            format_optional_ts(output.last_published_at.as_ref())
        ),
        format!(
            "last_published_window_start={}",
            format_optional_ts(output.last_published_window_start.as_ref())
        ),
        format!("published_wallet_count={}", output.published_wallet_count),
        format!(
            "wallet_metrics_snapshot_rows={}",
            output.wallet_metrics_snapshot_rows
        ),
        format!(
            "fresh_under_current_gate={}",
            output.fresh_under_current_gate
        ),
        format!(
            "runtime_cursor_ts={}",
            output.runtime_cursor_ts.to_rfc3339()
        ),
        format!("runtime_cursor_slot={}", output.runtime_cursor_slot),
        format!(
            "runtime_cursor_signature={}",
            output.runtime_cursor_signature
        ),
    ]
    .join("\n")
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

fn explain_publication_truth_export_blocker_read_only(
    config_path: &Path,
    now: DateTime<Utc>,
) -> PublicationTruthExportBlockerDiagnostic {
    explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
        config_path,
        now,
        StdDuration::from_millis(DEFAULT_PUBLICATION_TRUTH_CHECKPOINT_HEADLINE_BUDGET_MS),
    )
}

fn explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
    config_path: &Path,
    now: DateTime<Utc>,
    checkpoint_headline_budget: StdDuration,
) -> PublicationTruthExportBlockerDiagnostic {
    let loaded_config = match load_from_path(config_path) {
        Ok(config) => config,
        Err(error) => {
            return PublicationTruthExportBlockerDiagnostic::unproven(
                config_path,
                format!(
                    "publication truth export blocker is unproven because config {} could not be loaded: {error:#}",
                    config_path.display()
                ),
            );
        }
    };

    let runtime_db_path = resolve_db_path(config_path, None, &loaded_config.sqlite.path);
    let recent_raw_dir = resolve_relative_to_config(
        config_path,
        Path::new(&loaded_config.runtime_restore_ops.journal_snapshot_dir),
    );
    let recent_raw_db_path = journal_snapshot_latest_path(&recent_raw_dir);
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let publication_gate = discovery.publication_freshness_gate();

    let mut diagnostic = PublicationTruthExportBlockerDiagnostic::unproven(
        config_path,
        "publication truth export blocker could not yet be proven from current config-relative evidence".to_string(),
    );
    diagnostic.runtime_db_path = Some(runtime_db_path.display().to_string());
    diagnostic.recent_raw_db_path = Some(recent_raw_db_path.display().to_string());

    let runtime_store = match SqliteStore::open_read_only(&runtime_db_path)
        .with_context(|| format!("failed opening runtime db {}", runtime_db_path.display()))
    {
        Ok(store) => store,
        Err(error) => {
            diagnostic.publication_truth_export_blocker_explanation = format!(
                "publication truth export blocker is unproven because runtime db {} could not be opened read-only: {error:#}",
                runtime_db_path.display()
            );
            return diagnostic;
        }
    };

    let publication_state = match runtime_store.discovery_publication_state_read_only() {
        Ok(Some(publication_state)) => {
            diagnostic.export_gate_runtime_mode =
                Some(publication_state.runtime_mode.as_str().to_string());
            diagnostic.export_gate_reason = Some(publication_state.reason.clone());
            diagnostic.publication_truth_complete =
                Some(publication_state.has_complete_publication_truth());
            diagnostic.fresh_under_export_gate =
                Some(publication_state.is_fresh_under_gate(publication_gate, now));
            diagnostic.last_published_at = publication_state.last_published_at;
            diagnostic.last_published_window_start = publication_state.last_published_window_start;
            diagnostic.published_wallet_count = Some(
                publication_state
                    .published_wallet_ids
                    .as_ref()
                    .map(Vec::len)
                    .unwrap_or(0),
            );
            publication_state
        }
        Ok(None) => {
            diagnostic.publication_truth_export_blocker_explanation = format!(
                "publication truth export blocker is unproven because runtime db {} does not contain a persisted discovery publication state row",
                runtime_db_path.display()
            );
            return diagnostic;
        }
        Err(error) => {
            diagnostic.publication_truth_export_blocker_explanation = format!(
                "publication truth export blocker is unproven because discovery publication state could not be read from runtime db {}: {error:#}",
                runtime_db_path.display()
            );
            return diagnostic;
        }
    };

    let publication_truth_complete = publication_state.has_complete_publication_truth();
    let fresh_under_export_gate = publication_state.is_fresh_under_gate(publication_gate, now);
    let top_level_state = classify_publication_truth_export_blocker_from_publication_state(
        &mut diagnostic,
        publication_truth_complete,
        fresh_under_export_gate,
    );
    if matches!(
        top_level_state,
        PublicationTruthExportBlockerTopLevelState::GateSatisfied
            | PublicationTruthExportBlockerTopLevelState::IncompleteOrStaleWithoutCheckpointExplanation
    ) {
        return diagnostic;
    }

    let checkpoint_headline_outcome = apply_publication_truth_export_checkpoint_headline_enrichment(
        &mut diagnostic,
        &top_level_state,
        &runtime_db_path,
        checkpoint_headline_budget,
    );
    match checkpoint_headline_outcome {
        PublicationTruthExportCheckpointHeadlineOutcome::Failed => {
            let stage = diagnostic
                .publication_truth_export_checkpoint_headline_stage
                .clone();
            let budget_exhausted =
                diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted;
            let explanation = diagnostic
                .publication_truth_export_checkpoint_headline_explanation
                .clone()
                .unwrap_or_else(|| {
                    "runtime-db-only checkpoint headline enrichment failed before bounded replay enrichment could proceed"
                        .to_string()
                });
            set_publication_truth_export_enrichment_failed(
                &mut diagnostic,
                stage,
                budget_exhausted,
                explanation,
            );
            return diagnostic;
        }
        PublicationTruthExportCheckpointHeadlineOutcome::Completed { allow_deep_replay } => {
            if matches!(
                top_level_state,
                PublicationTruthExportBlockerTopLevelState::OtherCheckpoint(_)
            ) {
                let explanation = diagnostic
                    .publication_truth_export_checkpoint_headline_explanation
                    .clone()
                    .unwrap_or_else(|| {
                        "runtime-db-only checkpoint headline enrichment completed for checkpoint-family publication blocker"
                            .to_string()
                    });
                set_publication_truth_export_enrichment_completed(
                    &mut diagnostic,
                    Some("checkpoint_headline".to_string()),
                    explanation,
                );
                return diagnostic;
            }

            if !allow_deep_replay {
                let explanation = diagnostic
                    .publication_truth_export_checkpoint_headline_explanation
                    .clone()
                    .unwrap_or_else(|| {
                        "runtime-db-only checkpoint headline enrichment completed without running recent_raw-backed deep replay proof"
                            .to_string()
                    });
                set_publication_truth_export_enrichment_completed(
                    &mut diagnostic,
                    Some("checkpoint_headline".to_string()),
                    explanation,
                );
                return diagnostic;
            }
        }
    }

    let recent_raw_store = match SqliteStore::open_read_only(&recent_raw_db_path).with_context(
        || {
            format!(
                "failed opening promoted recent_raw latest db {}",
                recent_raw_db_path.display()
            )
        },
    ) {
        Ok(store) => store,
        Err(error) => {
            set_publication_truth_export_enrichment_failed(
                &mut diagnostic,
                Some("open_recent_raw_latest_db".to_string()),
                false,
                format!(
                    "bounded replay-sol-leg enrichment could not open promoted recent_raw latest db {} read-only: {error:#}",
                    recent_raw_db_path.display()
                ),
            );
            return diagnostic;
        }
    };
    let replay_diagnostic = match DiscoveryService::explain_replay_sol_leg_incomplete_read_only(
        &runtime_store,
        &recent_raw_store,
    ) {
        Ok(diagnostic_result) => diagnostic_result,
        Err(error) => {
            set_publication_truth_export_enrichment_failed(
                &mut diagnostic,
                Some("replay_sol_leg_incomplete".to_string()),
                false,
                format!(
                    "bounded replay-sol-leg enrichment could not complete against runtime db {} and recent_raw db {}: {error:#}",
                    runtime_db_path.display(),
                    recent_raw_db_path.display()
                ),
            );
            return diagnostic;
        }
    };
    diagnostic.replay_sol_leg_incomplete_reason_class =
        Some(replay_diagnostic.replay_sol_leg_incomplete_reason_class);
    diagnostic.replay_sol_leg_incomplete_explanation = Some(
        replay_diagnostic
            .replay_sol_leg_incomplete_explanation
            .clone(),
    );
    diagnostic.checkpoint_exact_target_surface_exists =
        replay_diagnostic.checkpoint.exact_target_surface_exists;
    diagnostic.checkpoint_exact_target_surface_repairable_for_resume = replay_diagnostic
        .checkpoint
        .exact_target_surface_repairable_for_resume;
    diagnostic.checkpoint_replay_candidate_activity_backfill_required = replay_diagnostic
        .checkpoint
        .replay_candidate_activity_backfill_required;
    diagnostic.checkpoint_replay_candidate_activity_backfill_pending = replay_diagnostic
        .checkpoint
        .replay_candidate_activity_backfill_pending;
    diagnostic.source_comparison_applicable = Some(
        replay_diagnostic
            .source_vs_checkpoint
            .source_comparison_applicable,
    );
    diagnostic.source_scan_target_buy_mint_filter_active = replay_diagnostic
        .source_vs_checkpoint
        .source_scan_target_buy_mint_filter_active;
    diagnostic.source_scan_target_buy_mint_count = replay_diagnostic
        .source_vs_checkpoint
        .source_scan_target_buy_mint_count;
    diagnostic.source_rows_exist_beyond_stored_replay_checkpoint = replay_diagnostic
        .source_vs_checkpoint
        .source_rows_exist_beyond_stored_replay_checkpoint;
    diagnostic.source_rows_ahead_count = replay_diagnostic
        .source_vs_checkpoint
        .source_rows_ahead_count;
    diagnostic.source_rows_ahead_pages_scanned = replay_diagnostic
        .source_vs_checkpoint
        .source_rows_ahead_pages_scanned;
    diagnostic.source_rows_ahead_first_cursor = replay_diagnostic
        .source_vs_checkpoint
        .source_rows_ahead_first_cursor
        .clone();
    diagnostic.source_rows_ahead_last_cursor = replay_diagnostic
        .source_vs_checkpoint
        .source_rows_ahead_last_cursor
        .clone();
    diagnostic.source_scan_access_path = replay_diagnostic
        .source_vs_checkpoint
        .source_scan_access_path
        .clone();
    diagnostic.source_scan_time_budget_exhausted = replay_diagnostic
        .source_vs_checkpoint
        .source_scan_time_budget_exhausted;
    diagnostic.publication_truth_export_blocker_explanation = format!(
        "publication truth export is currently blocked on replay_sol_leg_incomplete as already persisted in discovery publication state: runtime_mode={}, reason={}, replay_sol_leg_incomplete_reason_class={}, replay_explanation={}",
        diagnostic
            .export_gate_runtime_mode
            .as_deref()
            .unwrap_or("unknown"),
        diagnostic.export_gate_reason.as_deref().unwrap_or("unknown"),
        serde_json::to_string(&diagnostic.replay_sol_leg_incomplete_reason_class)
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .trim_matches('"'),
        diagnostic
            .replay_sol_leg_incomplete_explanation
            .as_deref()
            .unwrap_or("unknown")
    );
    let blocker = diagnostic
        .publishable_checkpoint_blocker
        .clone()
        .unwrap_or_else(|| "replay_sol_leg_incomplete".to_string());
    set_publication_truth_export_enrichment_completed(
        &mut diagnostic,
        Some("replay_sol_leg_incomplete".to_string()),
        format!(
            "bounded replay-sol-leg enrichment completed for persisted top-level blocker {}",
            blocker
        ),
    );

    diagnostic
}

fn explain_replay_sol_leg_blocker_read_only(
    config_path: &Path,
    now: DateTime<Utc>,
) -> ReplaySolLegBlockerDiagnostic {
    let started_at = Instant::now();
    let prerequisite_started_at = Instant::now();
    let publication_diagnostic = explain_publication_truth_export_blocker_read_only(config_path, now);
    let prerequisite_total_elapsed_ms = elapsed_ms(prerequisite_started_at);
    explain_replay_sol_leg_blocker_from_prerequisite_diagnostic(
        started_at,
        prerequisite_total_elapsed_ms,
        publication_diagnostic,
    )
}

#[cfg(test)]
fn explain_replay_sol_leg_blocker_read_only_with_prerequisite_budget(
    config_path: &Path,
    now: DateTime<Utc>,
    checkpoint_headline_budget: StdDuration,
) -> ReplaySolLegBlockerDiagnostic {
    let started_at = Instant::now();
    let prerequisite_started_at = Instant::now();
    let publication_diagnostic =
        explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
            config_path,
            now,
            checkpoint_headline_budget,
        );
    let prerequisite_total_elapsed_ms = elapsed_ms(prerequisite_started_at);
    explain_replay_sol_leg_blocker_from_prerequisite_diagnostic(
        started_at,
        prerequisite_total_elapsed_ms,
        publication_diagnostic,
    )
}

fn explain_replay_sol_leg_blocker_from_prerequisite_diagnostic(
    started_at: Instant,
    prerequisite_total_elapsed_ms: u64,
    publication_diagnostic: PublicationTruthExportBlockerDiagnostic,
) -> ReplaySolLegBlockerDiagnostic {
    let mut diagnostic = ReplaySolLegBlockerDiagnostic::from_publication_truth_export_blocker(
        &publication_diagnostic,
    );
    diagnostic.replay_sol_leg_blocker_prerequisite_total_elapsed_ms =
        prerequisite_total_elapsed_ms;

    let top_level_reason =
        &publication_diagnostic.publication_truth_export_blocker_reason_class;
    if *top_level_reason
        == PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockerUnprovenDueToMissingEvidence
    {
        diagnostic.replay_sol_leg_blocker_observed = false;
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
        diagnostic.replay_sol_leg_blocker_explanation = format!(
            "replay_sol_leg blocker is unproven because the primary export-blocker prerequisite proof is missing or unreadable: {}",
            publication_diagnostic.publication_truth_export_blocker_explanation
        );
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    if *top_level_reason
        != PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnReplaySolLegIncomplete
    {
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerNotCurrentExportBlocker;
        diagnostic.replay_sol_leg_blocker_explanation = format!(
            "replay_sol_leg_incomplete is not the current export blocker under persisted publication state: publication_truth_export_blocker_reason_class={}, export_gate_reason={}",
            serde_json::to_string(top_level_reason)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"'),
            diagnostic.export_gate_reason.as_deref().unwrap_or("unknown")
        );
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    if !publication_diagnostic.publication_truth_export_blocker_top_level_proven_from_publication_state
    {
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
        diagnostic.replay_sol_leg_blocker_explanation =
            "replay_sol_leg blocker is unproven because persisted publication state did not complete top-level blocker proof"
                .to_string();
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    if !publication_diagnostic.publication_truth_export_checkpoint_headline_completed {
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
        diagnostic.replay_sol_leg_blocker_explanation = format!(
            "replay_sol_leg blocker is unproven because the cheap checkpoint-headline prerequisite path did not complete: stage={}, explanation={}",
            publication_diagnostic
                .publication_truth_export_checkpoint_headline_stage
                .as_deref()
                .unwrap_or("unknown"),
            publication_diagnostic
                .publication_truth_export_checkpoint_headline_explanation
                .as_deref()
                .unwrap_or("unknown")
        );
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    if diagnostic.persisted_rebuild_checkpoint_exists != Some(true)
        || diagnostic.rebuild_phase.as_deref() != Some("replay")
        || diagnostic.rebuild_replay_subphase.as_deref() != Some("sol_leg")
    {
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
        diagnostic.replay_sol_leg_blocker_explanation = format!(
            "replay_sol_leg blocker is unproven because the cheap checkpoint prerequisites are not all proven current: persisted_rebuild_checkpoint_exists={}, rebuild_phase={}, rebuild_replay_subphase={}",
            format_optional_bool(diagnostic.persisted_rebuild_checkpoint_exists),
            diagnostic.rebuild_phase.as_deref().unwrap_or("null"),
            diagnostic.rebuild_replay_subphase.as_deref().unwrap_or("null"),
        );
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    let Some(runtime_db_path_raw) = diagnostic.runtime_db_path.clone() else {
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
        diagnostic.replay_sol_leg_blocker_explanation =
            "replay_sol_leg blocker is unproven because the primary export-blocker prerequisite proof did not resolve a runtime db path"
                .to_string();
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    };
    let Some(recent_raw_db_path_raw) = diagnostic.recent_raw_db_path.clone() else {
        diagnostic.replay_sol_leg_blocker_reason_class =
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
        diagnostic.replay_sol_leg_blocker_explanation =
            "replay_sol_leg blocker is unproven because the primary export-blocker prerequisite proof did not resolve a promoted recent_raw latest db path"
                .to_string();
        diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    };

    let runtime_db_path = PathBuf::from(&runtime_db_path_raw);
    let recent_raw_db_path = PathBuf::from(&recent_raw_db_path_raw);
    let runtime_store = match SqliteStore::open_read_only(&runtime_db_path).with_context(|| {
        format!(
            "failed opening runtime db {} for replay_sol_leg blocker deep proof",
            runtime_db_path.display()
        )
    }) {
        Ok(store) => store,
        Err(error) => {
            diagnostic.replay_sol_leg_blocker_reason_class =
                ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
            diagnostic.replay_sol_leg_blocker_explanation = format!(
                "replay_sol_leg blocker is unproven because runtime db {} could not be reopened read-only for bounded deep replay proof: {error:#}",
                runtime_db_path.display()
            );
            diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
            return diagnostic;
        }
    };
    let recent_raw_store = match SqliteStore::open_read_only(&recent_raw_db_path).with_context(
        || {
            format!(
                "failed opening promoted recent_raw latest db {} for replay_sol_leg blocker deep proof",
                recent_raw_db_path.display()
            )
        },
    ) {
        Ok(store) => store,
        Err(error) => {
            diagnostic.replay_sol_leg_blocker_reason_class =
                ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
            diagnostic.replay_sol_leg_blocker_explanation = format!(
                "replay_sol_leg blocker is unproven because promoted recent_raw latest db {} could not be opened read-only for bounded deep replay proof: {error:#}",
                recent_raw_db_path.display()
            );
            diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
            return diagnostic;
        }
    };

    let deep_started_at = Instant::now();
    let deep_outcome =
        explain_replay_sol_leg_blocker_deep_reason_read_only(&runtime_store, &recent_raw_store);
    diagnostic.replay_sol_leg_blocker_deep_reason_elapsed_ms = elapsed_ms(deep_started_at);

    match deep_outcome {
        Ok(ReplaySolLegBlockerDeepReasonOutcome::Completed(replay_diagnostic)) => {
            if replay_diagnostic.publishable_checkpoint_blocker.as_deref()
                != Some("replay_sol_leg_incomplete")
                || replay_diagnostic.rebuild_phase.as_deref() != Some("replay")
                || replay_diagnostic.rebuild_replay_subphase.as_deref() != Some("sol_leg")
            {
                diagnostic.replay_sol_leg_blocker_reason_class =
                    ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
                diagnostic.replay_sol_leg_blocker_explanation = format!(
                    "replay_sol_leg blocker is unproven because bounded deep replay proof contradicted the cheap current-blocker prerequisites: publishable_checkpoint_blocker={}, rebuild_phase={}, rebuild_replay_subphase={}",
                    replay_diagnostic
                        .publishable_checkpoint_blocker
                        .as_deref()
                        .unwrap_or("null"),
                    replay_diagnostic.rebuild_phase.as_deref().unwrap_or("null"),
                    replay_diagnostic
                        .rebuild_replay_subphase
                        .as_deref()
                        .unwrap_or("null")
                );
                diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
                return diagnostic;
            }

            diagnostic.persisted_rebuild_checkpoint_exists =
                Some(replay_diagnostic.persisted_rebuild_checkpoint_exists);
            diagnostic.rebuild_phase = replay_diagnostic.rebuild_phase.clone();
            diagnostic.rebuild_replay_subphase =
                replay_diagnostic.rebuild_replay_subphase.clone();
            diagnostic.replay_incomplete = Some(replay_diagnostic.replay_incomplete);
            diagnostic.replay_sol_leg_incomplete_reason_class =
                Some(replay_diagnostic.replay_sol_leg_incomplete_reason_class);
            diagnostic.replay_sol_leg_incomplete_explanation =
                Some(replay_diagnostic.replay_sol_leg_incomplete_explanation.clone());
            diagnostic.checkpoint_exact_target_surface_exists =
                replay_diagnostic.checkpoint.exact_target_surface_exists;
            diagnostic.checkpoint_exact_target_surface_repairable_for_resume = replay_diagnostic
                .checkpoint
                .exact_target_surface_repairable_for_resume;
            diagnostic.checkpoint_replay_candidate_activity_backfill_required = replay_diagnostic
                .checkpoint
                .replay_candidate_activity_backfill_required;
            diagnostic.checkpoint_replay_candidate_activity_backfill_pending = replay_diagnostic
                .checkpoint
                .replay_candidate_activity_backfill_pending;
            diagnostic.source_comparison_applicable = Some(
                replay_diagnostic
                    .source_vs_checkpoint
                    .source_comparison_applicable,
            );
            diagnostic.source_scan_target_buy_mint_filter_active = replay_diagnostic
                .source_vs_checkpoint
                .source_scan_target_buy_mint_filter_active;
            diagnostic.source_scan_target_buy_mint_count = replay_diagnostic
                .source_vs_checkpoint
                .source_scan_target_buy_mint_count;
            diagnostic.source_rows_exist_beyond_stored_replay_checkpoint = replay_diagnostic
                .source_vs_checkpoint
                .source_rows_exist_beyond_stored_replay_checkpoint;
            diagnostic.source_rows_ahead_count = replay_diagnostic
                .source_vs_checkpoint
                .source_rows_ahead_count;
            diagnostic.source_rows_ahead_pages_scanned = replay_diagnostic
                .source_vs_checkpoint
                .source_rows_ahead_pages_scanned;
            diagnostic.source_rows_ahead_first_cursor = replay_diagnostic
                .source_vs_checkpoint
                .source_rows_ahead_first_cursor
                .clone();
            diagnostic.source_rows_ahead_last_cursor = replay_diagnostic
                .source_vs_checkpoint
                .source_rows_ahead_last_cursor
                .clone();
            diagnostic.source_scan_access_path = replay_diagnostic
                .source_vs_checkpoint
                .source_scan_access_path
                .clone();
            diagnostic.source_scan_time_budget_exhausted = replay_diagnostic
                .source_vs_checkpoint
                .source_scan_time_budget_exhausted;
            diagnostic.replay_sol_leg_blocker_reason_class =
                ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerProvenCurrent;
            diagnostic.replay_sol_leg_blocker_explanation = format!(
                "replay_sol_leg_incomplete is the current export blocker and bounded deep replay proof resolved its exact reason: replay_sol_leg_incomplete_reason_class={}, explanation={}",
                serde_json::to_string(&diagnostic.replay_sol_leg_incomplete_reason_class)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                diagnostic
                    .replay_sol_leg_incomplete_explanation
                    .as_deref()
                    .unwrap_or("unknown")
            );
        }
        Ok(ReplaySolLegBlockerDeepReasonOutcome::BudgetExhausted { stage, explanation }) => {
            diagnostic.replay_sol_leg_blocker_reason_class =
                ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerProvenCurrent;
            diagnostic.replay_sol_leg_blocker_budget_exhausted = true;
            diagnostic.replay_sol_leg_blocker_budget_exhausted_stage = Some(stage.clone());
            diagnostic.replay_sol_leg_blocker_explanation = format!(
                "replay_sol_leg_incomplete is the current export blocker and the cheap checkpoint prerequisites are proven current, but bounded deep replay proof exhausted its budget before resolving an exact deep reason (stage={stage}): {explanation}"
            );
        }
        Err(error) => {
            diagnostic.replay_sol_leg_blocker_reason_class =
                ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence;
            diagnostic.replay_sol_leg_blocker_explanation = format!(
                "replay_sol_leg blocker is unproven because bounded deep replay proof failed against runtime db {} and recent_raw db {}: {error:#}",
                runtime_db_path.display(),
                recent_raw_db_path.display()
            );
        }
    }

    diagnostic.replay_sol_leg_blocker_total_elapsed_ms = elapsed_ms(started_at);
    diagnostic
}

fn render_publication_truth_export_blocker_human(
    diagnostic: &PublicationTruthExportBlockerDiagnostic,
) -> String {
    [
        "event=discovery_publication_truth_export_blocker".to_string(),
        format!(
            "publication_truth_export_blocker_observed={}",
            diagnostic.publication_truth_export_blocker_observed
        ),
        format!(
            "publication_truth_export_blocker_reason_class={}",
            serde_json::to_string(&diagnostic.publication_truth_export_blocker_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "publication_truth_export_blocker_explanation={}",
            diagnostic.publication_truth_export_blocker_explanation
        ),
        format!(
            "publication_truth_export_blocker_top_level_proven_from_publication_state={}",
            diagnostic.publication_truth_export_blocker_top_level_proven_from_publication_state
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_budget_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_source={}",
            diagnostic.publication_truth_export_checkpoint_headline_budget_source
        ),
        format!(
            "publication_truth_export_checkpoint_headline_total_elapsed_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_total_elapsed_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_row_count_elapsed_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_row_count_elapsed_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_prepare_elapsed_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_prepare_elapsed_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_step_elapsed_ms={}",
            diagnostic.publication_truth_export_checkpoint_headline_step_elapsed_ms
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open={}",
            diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup={}",
            diagnostic
                .publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count={}",
            diagnostic
                .publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare={}",
            diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step={}",
            diagnostic.publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step
        ),
        format!(
            "publication_truth_export_checkpoint_headline_attempted={}",
            diagnostic.publication_truth_export_checkpoint_headline_attempted
        ),
        format!(
            "publication_truth_export_checkpoint_headline_completed={}",
            diagnostic.publication_truth_export_checkpoint_headline_completed
        ),
        format!(
            "publication_truth_export_checkpoint_headline_budget_exhausted={}",
            diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted
        ),
        format!(
            "publication_truth_export_checkpoint_headline_stage={}",
            diagnostic
                .publication_truth_export_checkpoint_headline_stage
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publication_truth_export_checkpoint_headline_explanation={}",
            diagnostic
                .publication_truth_export_checkpoint_headline_explanation
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publication_truth_export_blocker_enrichment_attempted={}",
            diagnostic.publication_truth_export_blocker_enrichment_attempted
        ),
        format!(
            "publication_truth_export_blocker_enrichment_completed={}",
            diagnostic.publication_truth_export_blocker_enrichment_completed
        ),
        format!(
            "publication_truth_export_blocker_enrichment_budget_exhausted={}",
            diagnostic.publication_truth_export_blocker_enrichment_budget_exhausted
        ),
        format!(
            "publication_truth_export_blocker_enrichment_stage={}",
            diagnostic
                .publication_truth_export_blocker_enrichment_stage
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publication_truth_export_blocker_enrichment_explanation={}",
            diagnostic
                .publication_truth_export_blocker_enrichment_explanation
                .as_deref()
                .unwrap_or("null")
        ),
        format!("config_path={}", diagnostic.config_path),
        format!(
            "runtime_db_path={}",
            diagnostic.runtime_db_path.as_deref().unwrap_or("null")
        ),
        format!(
            "recent_raw_db_path={}",
            diagnostic.recent_raw_db_path.as_deref().unwrap_or("null")
        ),
        format!(
            "export_gate_runtime_mode={}",
            diagnostic
                .export_gate_runtime_mode
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "export_gate_reason={}",
            diagnostic.export_gate_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "publication_truth_complete={}",
            format_optional_bool(diagnostic.publication_truth_complete)
        ),
        format!(
            "fresh_under_export_gate={}",
            format_optional_bool(diagnostic.fresh_under_export_gate)
        ),
        format!(
            "last_published_at={}",
            format_optional_ts(diagnostic.last_published_at.as_ref())
        ),
        format!(
            "last_published_window_start={}",
            format_optional_ts(diagnostic.last_published_window_start.as_ref())
        ),
        format!(
            "published_wallet_count={}",
            format_optional_usize(diagnostic.published_wallet_count)
        ),
        format!(
            "persisted_rebuild_checkpoint_exists={}",
            format_optional_bool(diagnostic.persisted_rebuild_checkpoint_exists)
        ),
        format!(
            "persisted_rebuild_checkpoint_updated_at={}",
            format_optional_ts(diagnostic.persisted_rebuild_checkpoint_updated_at.as_ref())
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes={}",
            diagnostic
                .persisted_rebuild_checkpoint_state_json_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes_probe_attempted={}",
            diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_attempted
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes_probe_completed={}",
            diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_completed
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes_probe_budget_exhausted={}",
            diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_budget_exhausted
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes_probe_stage={}",
            diagnostic
                .persisted_rebuild_checkpoint_state_json_bytes_probe_stage
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes_probe_explanation={}",
            diagnostic
                .persisted_rebuild_checkpoint_state_json_bytes_probe_explanation
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "rebuild_phase={}",
            diagnostic.rebuild_phase.as_deref().unwrap_or("null")
        ),
        format!(
            "rebuild_replay_subphase={}",
            diagnostic
                .rebuild_replay_subphase
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publishable_checkpoint_blocker={}",
            diagnostic
                .publishable_checkpoint_blocker
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "replay_incomplete={}",
            format_optional_bool(diagnostic.replay_incomplete)
        ),
        format!(
            "replay_sol_leg_incomplete_reason_class={}",
            format_optional_enum_json(&diagnostic.replay_sol_leg_incomplete_reason_class)
        ),
        format!(
            "replay_sol_leg_incomplete_explanation={}",
            diagnostic
                .replay_sol_leg_incomplete_explanation
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "checkpoint_exact_target_surface_exists={}",
            format_optional_bool(diagnostic.checkpoint_exact_target_surface_exists)
        ),
        format!(
            "checkpoint_exact_target_surface_repairable_for_resume={}",
            format_optional_bool(diagnostic.checkpoint_exact_target_surface_repairable_for_resume)
        ),
        format!(
            "checkpoint_replay_candidate_activity_backfill_required={}",
            format_optional_bool(diagnostic.checkpoint_replay_candidate_activity_backfill_required)
        ),
        format!(
            "checkpoint_replay_candidate_activity_backfill_pending={}",
            format_optional_bool(diagnostic.checkpoint_replay_candidate_activity_backfill_pending)
        ),
        format!(
            "source_comparison_applicable={}",
            format_optional_bool(diagnostic.source_comparison_applicable)
        ),
        format!(
            "source_scan_target_buy_mint_filter_active={}",
            format_optional_bool(diagnostic.source_scan_target_buy_mint_filter_active)
        ),
        format!(
            "source_scan_target_buy_mint_count={}",
            format_optional_usize(diagnostic.source_scan_target_buy_mint_count)
        ),
        format!(
            "source_rows_exist_beyond_stored_replay_checkpoint={}",
            format_optional_bool(diagnostic.source_rows_exist_beyond_stored_replay_checkpoint)
        ),
        format!(
            "source_rows_ahead_count={}",
            format_optional_usize(diagnostic.source_rows_ahead_count)
        ),
        format!(
            "source_rows_ahead_pages_scanned={}",
            format_optional_usize(diagnostic.source_rows_ahead_pages_scanned)
        ),
        format!(
            "source_rows_ahead_first_cursor={}",
            format_optional_json(&diagnostic.source_rows_ahead_first_cursor)
        ),
        format!(
            "source_rows_ahead_last_cursor={}",
            format_optional_json(&diagnostic.source_rows_ahead_last_cursor)
        ),
        format!(
            "source_scan_access_path={}",
            diagnostic
                .source_scan_access_path
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "source_scan_time_budget_exhausted={}",
            format_optional_bool(diagnostic.source_scan_time_budget_exhausted)
        ),
    ]
    .join("\n")
}

fn render_replay_sol_leg_blocker_human(diagnostic: &ReplaySolLegBlockerDiagnostic) -> String {
    [
        "event=discovery_replay_sol_leg_blocker".to_string(),
        format!(
            "replay_sol_leg_blocker_observed={}",
            diagnostic.replay_sol_leg_blocker_observed
        ),
        format!(
            "replay_sol_leg_blocker_reason_class={}",
            serde_json::to_string(&diagnostic.replay_sol_leg_blocker_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replay_sol_leg_blocker_explanation={}",
            diagnostic.replay_sol_leg_blocker_explanation
        ),
        format!(
            "replay_sol_leg_blocker_prerequisite_reason_class={}",
            serde_json::to_string(&diagnostic.replay_sol_leg_blocker_prerequisite_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed={}",
            diagnostic.replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed
        ),
        format!(
            "replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted={}",
            diagnostic.replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted
        ),
        format!(
            "replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage={}",
            diagnostic
                .replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "replay_sol_leg_blocker_prerequisite_total_elapsed_ms={}",
            diagnostic.replay_sol_leg_blocker_prerequisite_total_elapsed_ms
        ),
        format!("config_path={}", diagnostic.config_path),
        format!(
            "runtime_db_path={}",
            diagnostic.runtime_db_path.as_deref().unwrap_or("null")
        ),
        format!(
            "recent_raw_db_path={}",
            diagnostic.recent_raw_db_path.as_deref().unwrap_or("null")
        ),
        format!(
            "export_gate_runtime_mode={}",
            diagnostic
                .export_gate_runtime_mode
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "export_gate_reason={}",
            diagnostic.export_gate_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "publication_truth_complete={}",
            format_optional_bool(diagnostic.publication_truth_complete)
        ),
        format!(
            "fresh_under_export_gate={}",
            format_optional_bool(diagnostic.fresh_under_export_gate)
        ),
        format!(
            "persisted_rebuild_checkpoint_exists={}",
            format_optional_bool(diagnostic.persisted_rebuild_checkpoint_exists)
        ),
        format!(
            "persisted_rebuild_checkpoint_updated_at={}",
            format_optional_ts(diagnostic.persisted_rebuild_checkpoint_updated_at.as_ref())
        ),
        format!(
            "rebuild_phase={}",
            diagnostic.rebuild_phase.as_deref().unwrap_or("null")
        ),
        format!(
            "rebuild_replay_subphase={}",
            diagnostic
                .rebuild_replay_subphase
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "replay_incomplete={}",
            format_optional_bool(diagnostic.replay_incomplete)
        ),
        format!(
            "replay_sol_leg_incomplete_reason_class={}",
            format_optional_enum_json(&diagnostic.replay_sol_leg_incomplete_reason_class)
        ),
        format!(
            "replay_sol_leg_incomplete_explanation={}",
            diagnostic
                .replay_sol_leg_incomplete_explanation
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "checkpoint_exact_target_surface_exists={}",
            format_optional_bool(diagnostic.checkpoint_exact_target_surface_exists)
        ),
        format!(
            "checkpoint_exact_target_surface_repairable_for_resume={}",
            format_optional_bool(diagnostic.checkpoint_exact_target_surface_repairable_for_resume)
        ),
        format!(
            "checkpoint_replay_candidate_activity_backfill_required={}",
            format_optional_bool(diagnostic.checkpoint_replay_candidate_activity_backfill_required)
        ),
        format!(
            "checkpoint_replay_candidate_activity_backfill_pending={}",
            format_optional_bool(diagnostic.checkpoint_replay_candidate_activity_backfill_pending)
        ),
        format!(
            "source_comparison_applicable={}",
            format_optional_bool(diagnostic.source_comparison_applicable)
        ),
        format!(
            "source_scan_target_buy_mint_filter_active={}",
            format_optional_bool(diagnostic.source_scan_target_buy_mint_filter_active)
        ),
        format!(
            "source_scan_target_buy_mint_count={}",
            format_optional_usize(diagnostic.source_scan_target_buy_mint_count)
        ),
        format!(
            "source_rows_exist_beyond_stored_replay_checkpoint={}",
            format_optional_bool(diagnostic.source_rows_exist_beyond_stored_replay_checkpoint)
        ),
        format!(
            "source_rows_ahead_count={}",
            format_optional_usize(diagnostic.source_rows_ahead_count)
        ),
        format!(
            "source_rows_ahead_pages_scanned={}",
            format_optional_usize(diagnostic.source_rows_ahead_pages_scanned)
        ),
        format!(
            "source_rows_ahead_first_cursor={}",
            format_optional_json(&diagnostic.source_rows_ahead_first_cursor)
        ),
        format!(
            "source_rows_ahead_last_cursor={}",
            format_optional_json(&diagnostic.source_rows_ahead_last_cursor)
        ),
        format!(
            "source_scan_access_path={}",
            diagnostic.source_scan_access_path.as_deref().unwrap_or("null")
        ),
        format!(
            "source_scan_time_budget_exhausted={}",
            format_optional_bool(diagnostic.source_scan_time_budget_exhausted)
        ),
        format!(
            "replay_sol_leg_blocker_budget_ms={}",
            diagnostic.replay_sol_leg_blocker_budget_ms
        ),
        format!(
            "replay_sol_leg_blocker_budget_source={}",
            diagnostic.replay_sol_leg_blocker_budget_source
        ),
        format!(
            "replay_sol_leg_blocker_total_elapsed_ms={}",
            diagnostic.replay_sol_leg_blocker_total_elapsed_ms
        ),
        format!(
            "replay_sol_leg_blocker_checkpoint_headline_elapsed_ms={}",
            diagnostic.replay_sol_leg_blocker_checkpoint_headline_elapsed_ms
        ),
        format!(
            "replay_sol_leg_blocker_deep_reason_elapsed_ms={}",
            diagnostic.replay_sol_leg_blocker_deep_reason_elapsed_ms
        ),
        format!(
            "replay_sol_leg_blocker_budget_exhausted={}",
            diagnostic.replay_sol_leg_blocker_budget_exhausted
        ),
        format!(
            "replay_sol_leg_blocker_budget_exhausted_stage={}",
            diagnostic
                .replay_sol_leg_blocker_budget_exhausted_stage
                .as_deref()
                .unwrap_or("null")
        ),
    ]
    .join("\n")
}

fn format_optional_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_json<T>(value: &Option<T>) -> String
where
    T: Serialize,
{
    value
        .as_ref()
        .map(|value| serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()))
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_enum_json<T>(value: &Option<T>) -> String
where
    T: Serialize,
{
    format_optional_json(value).trim_matches('"').to_string()
}

fn render_recent_raw_promotion_blocker_human(
    diagnostic: &RecentRawPromotionBlockerDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_promotion_blocker".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "promotion_blocker_observed={}",
            diagnostic.recent_raw_promotion_blocker_observed
        ),
        format!(
            "promotion_ready_now={}",
            diagnostic.recent_raw_promotion_ready_now
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!("promoted_exists={}", diagnostic.recent_raw_promoted_exists),
        format!("staged_exists={}", diagnostic.recent_raw_staged_exists),
        format!(
            "staged_newer_than_promoted={}",
            diagnostic
                .recent_raw_staged_newer_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_truth_blocked_by_promotion={}",
            diagnostic.recent_raw_stage3_truth_blocked_by_promotion
        ),
        format!(
            "stage3_current_fresh_healthy_evidence_possible={}",
            diagnostic.recent_raw_stage3_current_fresh_healthy_evidence_possible
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_promotion_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_catch_up_status_human(diagnostic: &RecentRawCatchUpDiagnostic) -> String {
    [
        "event=discovery_recent_raw_catch_up_status".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "catch_up_status_observed={}",
            diagnostic.recent_raw_catch_up_status_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_catch_up_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_vs_staged_row_lag={}",
            diagnostic
                .recent_raw_source_vs_staged_row_lag
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_vs_promoted_row_lag={}",
            diagnostic
                .recent_raw_source_vs_promoted_row_lag
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_advancing={}",
            diagnostic
                .recent_raw_staged_advancing
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_ahead_of_promoted={}",
            diagnostic
                .recent_raw_staged_ahead_of_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "catch_up_progressing={}",
            diagnostic.recent_raw_catch_up_progressing
        ),
        format!(
            "catch_up_losing_to_source={}",
            diagnostic.recent_raw_catch_up_losing_to_source
        ),
        format!(
            "catch_up_indeterminate={}",
            diagnostic.recent_raw_catch_up_indeterminate
        ),
        format!("explanation={}", diagnostic.recent_raw_catch_up_explanation),
    ]
    .join("\n")
}

fn render_recent_raw_staged_lineage_human(diagnostic: &RecentRawStagedLineageDiagnostic) -> String {
    [
        "event=discovery_recent_raw_staged_lineage".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_lineage_observed={}",
            diagnostic.recent_raw_staged_lineage_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_lineage_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "same_source_db_as_promoted={}",
            diagnostic
                .recent_raw_staged_same_source_db_as_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "cursor_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_relation_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_relation_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_relation_explanation={}",
            diagnostic.recent_raw_staged_cursor_relation_explanation
        ),
        format!(
            "cursor_ts_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_slot_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_signature_equal_to_promoted={}",
            diagnostic
                .recent_raw_staged_cursor_signature_equal_to_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "row_count_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_row_count_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "covered_since_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_covered_since_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "staged_monotonic_relative_to_promoted={}",
            diagnostic
                .recent_raw_staged_monotonic_relative_to_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_regressed_relative_to_promoted={}",
            diagnostic
                .recent_raw_staged_regressed_relative_to_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_closer_to_source_than_promoted={}",
            diagnostic
                .recent_raw_staged_closer_to_source_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_lineage_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_staged_regression_human(
    diagnostic: &RecentRawStagedRegressionDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_staged_regression".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_regression_observed={}",
            diagnostic.recent_raw_staged_regression_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_regression_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_snapshot_path={}",
            diagnostic.recent_raw_promoted_snapshot_path
        ),
        format!(
            "promoted_metadata_path={}",
            diagnostic.recent_raw_promoted_metadata_path
        ),
        format!(
            "staged_snapshot_path={}",
            diagnostic.recent_raw_staged_snapshot_path
        ),
        format!(
            "staged_metadata_path={}",
            diagnostic.recent_raw_staged_metadata_path
        ),
        format!(
            "staged_candidate_count={}",
            diagnostic.recent_raw_staged_candidate_count
        ),
        format!(
            "multiple_staged_candidates_present={}",
            diagnostic.recent_raw_multiple_staged_candidates_present
        ),
        format!(
            "selected_staged_is_latest_candidate={}",
            diagnostic
                .recent_raw_selected_staged_is_latest_candidate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_staged_created_after_promoted={}",
            diagnostic
                .recent_raw_selected_staged_created_after_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_staged_frontier_behind_promoted_before_comparison={}",
            diagnostic
                .recent_raw_selected_staged_frontier_behind_promoted_before_comparison
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_staged_completed_after_creation={}",
            diagnostic
                .recent_raw_selected_staged_completed_after_creation
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selection_reason={}",
            diagnostic.recent_raw_staged_selection_reason
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_regression_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_staged_birth_human(diagnostic: &RecentRawStagedBirthDiagnostic) -> String {
    [
        "event=discovery_recent_raw_staged_birth".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_birth_observed={}",
            diagnostic.recent_raw_staged_birth_observed
        ),
        format!(
            "staged_birth_proven_from_current_artifacts={}",
            diagnostic.recent_raw_staged_birth_proven_from_current_artifacts
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_birth_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "staged_birth_snapshot_path={}",
            diagnostic.recent_raw_staged_birth_snapshot_path
        ),
        format!(
            "staged_birth_metadata_path={}",
            diagnostic.recent_raw_staged_birth_metadata_path
        ),
        format!(
            "staged_birth_created_after_promoted={}",
            diagnostic
                .recent_raw_staged_birth_created_after_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_window_later_start_than_promoted={}",
            diagnostic
                .recent_raw_staged_birth_window_later_start_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_window_narrower_or_older_than_promoted={}",
            diagnostic
                .recent_raw_staged_birth_window_narrower_or_older_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_manifest_matches_sqlite_content={}",
            diagnostic
                .recent_raw_staged_birth_manifest_matches_sqlite_content
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_manifest_sqlite_match_unproven={}",
            diagnostic.recent_raw_staged_birth_manifest_sqlite_match_unproven
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_birth_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_staged_window_seeding_human(
    diagnostic: &RecentRawStagedWindowSeedingDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_staged_window_seeding".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_window_seeding_observed={}",
            diagnostic.recent_raw_staged_window_seeding_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_window_seeding_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "staged_start_matches_promoted_start={}",
            diagnostic
                .recent_raw_staged_start_matches_promoted_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_start_matches_source_start={}",
            diagnostic
                .recent_raw_staged_start_matches_source_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_start_matches_current_window_cutoff={}",
            diagnostic
                .recent_raw_staged_start_matches_current_window_cutoff
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "historical_seeding_basis_proven_from_current_artifacts={}",
            diagnostic
                .recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts
        ),
        format!(
            "staged_start_matches_neither_promoted_nor_source={}",
            diagnostic
                .recent_raw_staged_start_matches_neither_promoted_nor_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_start_current_evidence_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_start_current_evidence_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_can_seed_staged_progress_under_current_code={}",
            diagnostic
                .recent_raw_promoted_can_seed_staged_progress_under_current_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_seed_blocked_by_source_contract_mismatch={}",
            diagnostic
                .recent_raw_promoted_seed_blocked_by_source_contract_mismatch
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_supersedes_staged_progress={}",
            diagnostic
                .recent_raw_promoted_supersedes_staged_progress
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_manifest_matches_sqlite_content={}",
            diagnostic
                .recent_raw_staged_manifest_matches_sqlite_content
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_manifest_sqlite_match_unproven={}",
            diagnostic.recent_raw_staged_manifest_sqlite_match_unproven
        ),
        format!(
            "current_evidence_explanation={}",
            diagnostic.recent_raw_staged_start_current_evidence_explanation
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_window_seeding_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_source_window_contract_human(
    diagnostic: &RecentRawSourceWindowContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_source_window_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "source_window_contract_observed={}",
            diagnostic.recent_raw_source_window_contract_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_contract_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_window_probe_bounded={}",
            diagnostic.recent_raw_source_window_probe_bounded
        ),
        format!(
            "source_window_probe_mode={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_probe_mode)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_cached_state_matches_bounded_probe={}",
            diagnostic
                .recent_raw_source_cached_state_matches_bounded_probe
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_start_later_than_promoted={}",
            diagnostic
                .recent_raw_source_start_later_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_contract_currently_excludes_older_rows={}",
            diagnostic
                .recent_raw_source_contract_currently_excludes_older_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_window_matches_current_bounded_contract={}",
            diagnostic
                .recent_raw_source_window_matches_current_bounded_contract
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_reflects_older_still_promoted_window={}",
            diagnostic
                .recent_raw_promoted_reflects_older_still_promoted_window
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_window_contract_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_contract_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_cached_state_matches_scanned_rows={}",
            diagnostic
                .recent_raw_source_cached_state_matches_scanned_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_prune_activity_recorded={}",
            diagnostic
                .recent_raw_source_prune_activity_recorded
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_window_probe_explanation={}",
            diagnostic.recent_raw_source_window_probe_explanation
        ),
        format!(
            "source_window_contract_basis_explanation={}",
            diagnostic.recent_raw_source_window_contract_basis_explanation
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_source_window_contract_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_promoted_retention_contract_human(
    diagnostic: &RecentRawPromotedRetentionContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_promoted_retention_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "promoted_retention_observed={}",
            diagnostic.recent_raw_promoted_retention_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promoted_retention_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_start_older_than_current_source={}",
            diagnostic
                .recent_raw_promoted_start_older_than_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_currently_retained_as_truth={}",
            diagnostic
                .recent_raw_promoted_currently_retained_as_truth
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_has_current_contract_invalidation_rule={}",
            diagnostic
                .recent_raw_promoted_has_current_contract_invalidation_rule
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_invalidated_by_current_source_window_shift={}",
            diagnostic
                .recent_raw_promoted_invalidated_by_current_source_window_shift
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_truth_currently_depends_on_retained_older_promoted_surface={}",
            diagnostic
                .recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_truth_can_advance_without_new_promotion={}",
            diagnostic
                .recent_raw_stage3_truth_can_advance_without_new_promotion
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promotion_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_window_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_contract_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_retention_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_promoted_retention_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_retention_basis_explanation={}",
            diagnostic.recent_raw_promoted_retention_basis_explanation
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_promoted_retention_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_promotion_contract_human(
    diagnostic: &RecentRawReplacementPromotionContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_promotion_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_promotion_observed={}",
            diagnostic.recent_raw_replacement_promotion_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replacement_candidate_exists={}",
            diagnostic.recent_raw_replacement_candidate_exists
        ),
        format!(
            "replacement_candidate_source_db_matches_promoted={}",
            diagnostic
                .recent_raw_replacement_candidate_source_db_matches_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_start_matches_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_start_matches_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_complete_against_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_complete_against_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_promotable_now={}",
            diagnostic
                .recent_raw_replacement_candidate_promotable_now
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_covered_through_relation_to_promoted={}",
            serde_json::to_string(
                &diagnostic.recent_raw_replacement_candidate_covered_through_relation_to_promoted
            )
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .trim_matches('"')
        ),
        format!(
            "replacement_candidate_row_count_relation_to_promoted={}",
            serde_json::to_string(
                &diagnostic.recent_raw_replacement_candidate_row_count_relation_to_promoted
            )
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .trim_matches('"')
        ),
        format!(
            "replacement_promotion_would_retire_current_promoted_truth={}",
            diagnostic
                .recent_raw_replacement_promotion_would_retire_current_promoted_truth
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_blocked_on_replacement_candidate={}",
            diagnostic
                .recent_raw_stage3_blocked_on_replacement_candidate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promotion_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promotion_ready_now={}",
            diagnostic.recent_raw_promotion_ready_now
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_promotion_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_progress_contract_human(
    diagnostic: &RecentRawReplacementProgressContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_progress_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_progress_observed={}",
            diagnostic.recent_raw_replacement_progress_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_progress_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replacement_candidate_exists={}",
            diagnostic.recent_raw_replacement_candidate_exists
        ),
        format!(
            "replacement_candidate_created_at={}",
            format_optional_ts(
                diagnostic
                    .recent_raw_replacement_candidate_created_at
                    .as_ref()
            )
        ),
        format!(
            "replacement_candidate_row_count={}",
            diagnostic
                .recent_raw_replacement_candidate_row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_covered_through={}",
            diagnostic
                .recent_raw_replacement_candidate_covered_through
                .as_ref()
                .map(|value| serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_last_batch_completed_at={}",
            format_optional_ts(
                diagnostic
                    .recent_raw_replacement_candidate_last_batch_completed_at
                    .as_ref()
            )
        ),
        format!(
            "replacement_candidate_completed_after_creation={}",
            diagnostic
                .recent_raw_replacement_candidate_completed_after_creation
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_previous_candidate_exists={}",
            diagnostic.recent_raw_replacement_previous_candidate_exists
        ),
        format!(
            "replacement_candidate_same_identity_as_previous_attempt={}",
            diagnostic
                .recent_raw_replacement_candidate_same_identity_as_previous_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_row_count_advanced={}",
            diagnostic
                .recent_raw_replacement_candidate_row_count_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_covered_through_advanced={}",
            diagnostic
                .recent_raw_replacement_candidate_covered_through_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_last_batch_completed_at_advanced={}",
            diagnostic
                .recent_raw_replacement_candidate_last_batch_completed_at_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_advancing={}",
            diagnostic
                .recent_raw_replacement_candidate_advancing
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_stalled={}",
            diagnostic
                .recent_raw_replacement_candidate_stalled
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_reset_or_recreated={}",
            diagnostic
                .recent_raw_replacement_candidate_reset_or_recreated
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_manifest_matches_sqlite_content={}",
            diagnostic
                .recent_raw_replacement_manifest_matches_sqlite_content
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_manifest_sqlite_match_unproven={}",
            diagnostic.recent_raw_replacement_manifest_sqlite_match_unproven
        ),
        format!(
            "replacement_candidate_complete_against_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_complete_against_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_promotion_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_progress_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_artifact_history_contract_human(
    diagnostic: &RecentRawReplacementArtifactHistoryContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_artifact_history_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_artifact_history_observed={}",
            diagnostic.recent_raw_replacement_artifact_history_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_artifact_history_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replacement_fixed_path_overwrite_contract={}",
            diagnostic.recent_raw_replacement_fixed_path_overwrite_contract
        ),
        format!(
            "replacement_fixed_snapshot_path={}",
            diagnostic.recent_raw_replacement_fixed_snapshot_path
        ),
        format!(
            "replacement_fixed_metadata_path={}",
            diagnostic.recent_raw_replacement_fixed_metadata_path
        ),
        format!(
            "replacement_current_fixed_candidate_exists={}",
            diagnostic.recent_raw_replacement_current_fixed_candidate_exists
        ),
        format!(
            "replacement_current_fixed_candidate_manifest_parseable={}",
            diagnostic.recent_raw_replacement_current_fixed_candidate_manifest_parseable
        ),
        format!(
            "replacement_staged_candidate_scan_succeeded={}",
            diagnostic.recent_raw_replacement_staged_candidate_scan_succeeded
        ),
        format!(
            "replacement_previous_artifact_archive_path_present={}",
            diagnostic.recent_raw_replacement_previous_artifact_archive_path_present
        ),
        format!(
            "replacement_previous_artifact_archive_candidate_count={}",
            diagnostic.recent_raw_replacement_previous_artifact_archive_candidate_count
        ),
        format!(
            "replacement_previous_artifact_archive_parseable_count={}",
            diagnostic.recent_raw_replacement_previous_artifact_archive_parseable_count
        ),
        format!(
            "replacement_previous_artifact_history_expected_under_current_contract={}",
            diagnostic
                .recent_raw_replacement_previous_artifact_history_expected_under_current_contract
        ),
        format!(
            "replacement_previous_artifact_history_missing_due_to_unproven_evidence={}",
            diagnostic
                .recent_raw_replacement_previous_artifact_history_missing_due_to_unproven_evidence
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_artifact_history_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_attempt_telemetry_human(
    diagnostic: &RecentRawReplacementAttemptTelemetryDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_attempt_telemetry".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_attempt_telemetry_observed={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(
                &diagnostic.recent_raw_replacement_attempt_telemetry_reason_class
            )
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .trim_matches('"')
        ),
        format!(
            "replacement_attempt_telemetry_probe_bounded={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_bounded
        ),
        format!(
            "replacement_attempt_telemetry_probe_mode={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_mode
        ),
        format!(
            "replacement_attempt_telemetry_deep_scan_used={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_deep_scan_used
        ),
        format!(
            "replacement_attempt_telemetry_explicit_paths_checked={}",
            serde_json::to_string(
                &diagnostic.recent_raw_replacement_attempt_telemetry_explicit_paths_checked
            )
            .unwrap_or_else(|_| "[]".to_string())
        ),
        format!(
            "replacement_attempt_telemetry_scan_truncated={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_scan_truncated
        ),
        format!(
            "replacement_attempt_telemetry_artifact_count={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_artifact_count
        ),
        format!(
            "replacement_attempt_telemetry_parseable_count={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_parseable_count
        ),
        format!(
            "replacement_attempt_telemetry_latest_path={}",
            diagnostic
                .recent_raw_replacement_attempt_telemetry_latest_path
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "replacement_attempt_telemetry_latest_timestamp={}",
            format_optional_ts(
                diagnostic
                    .recent_raw_replacement_attempt_telemetry_latest_timestamp
                    .as_ref()
            )
        ),
        format!(
            "replacement_attempt_telemetry_proves_advancing={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_proves_advancing
        ),
        format!(
            "replacement_attempt_telemetry_proves_stalled={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_proves_stalled
        ),
        format!(
            "replacement_attempt_telemetry_proves_reset_or_recreated={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_proves_reset_or_recreated
        ),
        format!(
            "replacement_attempt_telemetry_last_row_count_before={}",
            diagnostic
                .recent_raw_replacement_attempt_telemetry_last_row_count_before
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_attempt_telemetry_last_row_count_after={}",
            diagnostic
                .recent_raw_replacement_attempt_telemetry_last_row_count_after
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_attempt_telemetry_staged_progress_advanced={}",
            diagnostic
                .recent_raw_replacement_attempt_telemetry_staged_progress_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_attempt_telemetry_staged_progress_resumed={}",
            diagnostic
                .recent_raw_replacement_attempt_telemetry_staged_progress_resumed
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_attempt_telemetry_staged_progress_preserved_for_retry={}",
            diagnostic
                .recent_raw_replacement_attempt_telemetry_staged_progress_preserved_for_retry
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_convergence_human(
    diagnostic: &RecentRawReplacementConvergenceDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_convergence".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_convergence_observed={}",
            diagnostic.recent_raw_replacement_convergence_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_convergence_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replacement_candidate_exists={}",
            diagnostic.recent_raw_replacement_candidate_exists
        ),
        format!(
            "replacement_candidate_row_count={}",
            diagnostic
                .recent_raw_replacement_candidate_row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_row_count={}",
            diagnostic
                .recent_raw_source_row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_rows_remaining_to_current_source={}",
            diagnostic
                .recent_raw_replacement_rows_remaining_to_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_latest_attempt_row_count_delta={}",
            diagnostic
                .recent_raw_replacement_latest_attempt_row_count_delta
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_latest_attempt_advanced={}",
            diagnostic
                .recent_raw_replacement_latest_attempt_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_latest_attempt_resumed={}",
            diagnostic
                .recent_raw_replacement_latest_attempt_resumed
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_latest_attempt_preserved_for_retry={}",
            diagnostic
                .recent_raw_replacement_latest_attempt_preserved_for_retry
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_estimated_attempts_to_current_source={}",
            diagnostic
                .recent_raw_replacement_estimated_attempts_to_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_complete_against_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_complete_against_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_promotable_now={}",
            diagnostic
                .recent_raw_replacement_candidate_promotable_now
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_attempt_telemetry_path={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_path
        ),
        format!(
            "replacement_attempt_telemetry_parseable={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_parseable
        ),
        format!(
            "replacement_attempt_telemetry_probe_bounded={}",
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_bounded
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_convergence_explanation
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        arm_test_force_replay_sol_leg_blocker_deep_reason_budget_exhausted,
        explain_replay_sol_leg_blocker_read_only,
        explain_replay_sol_leg_blocker_read_only_with_prerequisite_budget,
        explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget,
        load_json, parse_args_from, run, run_command, write_json_atomic, Command, Config,
        ExplainPublicationTruthExportBlockerConfig, ExplainRecentRawCatchUpStatusConfig,
        ExplainReplaySolLegBlockerConfig,
        ExplainRecentRawPromotedRetentionContractConfig, ExplainRecentRawPromotionBlockerConfig,
        ExplainRecentRawReplacementArtifactHistoryContractConfig,
        ExplainRecentRawReplacementAttemptTelemetryConfig,
        ExplainRecentRawReplacementConvergenceConfig,
        ExplainRecentRawReplacementProgressContractConfig,
        ExplainRecentRawReplacementPromotionContractConfig,
        ExplainRecentRawSourceWindowContractConfig, ExplainRecentRawStagedBirthConfig,
        ExplainRecentRawStagedLineageConfig, ExplainRecentRawStagedRegressionConfig,
        ExplainRecentRawStagedWindowSeedingConfig, PublicationTruthExportBlockerReasonClass,
        ReplaySolLegBlockerReasonClass,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
        DiscoveryRuntimeMode, SqliteStore, WalletMetricRow, WalletUpsertRow,
    };
    use serde_json::Value;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration as StdDuration;
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_accepts_scheduled_force_json_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--scheduled".to_string(),
            "--force".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::Export(parsed) = parsed else {
            panic!("expected export command");
        };

        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.output_path.is_none());
        assert!(parsed.scheduled);
        assert!(parsed.force);
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_promotion_blocker_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-promotion-blocker".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawPromotionBlocker(parsed) = parsed else {
            panic!("expected explain command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_catch_up_status_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-catch-up-status".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawCatchUpStatus(parsed) = parsed else {
            panic!("expected catch-up command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_lineage_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-lineage".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedLineage(parsed) = parsed else {
            panic!("expected staged lineage command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_regression_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-regression".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedRegression(parsed) = parsed else {
            panic!("expected staged regression command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_birth_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-birth".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedBirth(parsed) = parsed else {
            panic!("expected staged birth command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_window_seeding_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-window-seeding".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedWindowSeeding(parsed) = parsed else {
            panic!("expected staged window seeding command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_source_window_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-source-window-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawSourceWindowContract(parsed) = parsed else {
            panic!("expected source window contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_promoted_retention_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-promoted-retention-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawPromotedRetentionContract(parsed) = parsed else {
            panic!("expected promoted retention contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_promotion_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-promotion-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementPromotionContract(parsed) = parsed else {
            panic!("expected replacement promotion contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_progress_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-progress-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementProgressContract(parsed) = parsed else {
            panic!("expected replacement progress contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_artifact_history_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-artifact-history-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementArtifactHistoryContract(parsed) = parsed else {
            panic!("expected replacement artifact history contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_attempt_telemetry_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-attempt-telemetry".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementAttemptTelemetry(parsed) = parsed else {
            panic!("expected replacement attempt telemetry command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
        assert!(!parsed.deep_attempt_telemetry_scan);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_attempt_telemetry_deep_scan_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-attempt-telemetry".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--deep-attempt-telemetry-scan".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementAttemptTelemetry(parsed) = parsed else {
            panic!("expected replacement attempt telemetry command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(!parsed.json);
        assert!(parsed.deep_attempt_telemetry_scan);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_convergence_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-convergence".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementConvergence(parsed) = parsed else {
            panic!("expected replacement convergence command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_publication_truth_export_blocker_mode() {
        let parsed = parse_args_from(vec![
            "--explain-publication-truth-export-blocker".to_string(),
            "--config".to_string(),
            "/tmp/live.server.toml".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainPublicationTruthExportBlocker(parsed) = parsed else {
            panic!("expected publication truth export blocker command");
        };
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/live.server.toml"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_replay_sol_leg_blocker_mode() {
        let parsed = parse_args_from(vec![
            "--explain-replay-sol-leg-blocker".to_string(),
            "--config".to_string(),
            "/tmp/live.server.toml".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainReplaySolLegBlocker(parsed) = parsed else {
            panic!("expected replay sol leg blocker command");
        };
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/live.server.toml"));
        assert!(parsed.json);
    }

    #[test]
    fn run_command_publication_truth_export_blocker_green_fixture_short_circuits_before_recent_raw_open(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-green")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        let diagnostic = explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
            &fixture.config_path,
            now,
            StdDuration::from_secs(1),
        );
        assert_eq!(
            diagnostic.publication_truth_export_blocker_reason_class,
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportGateSatisfied
        );
        assert!(diagnostic.publication_truth_export_blocker_observed);
        assert_eq!(diagnostic.export_gate_runtime_mode.as_deref(), Some("healthy"));
        assert_eq!(diagnostic.publication_truth_complete, Some(true));
        assert_eq!(diagnostic.fresh_under_export_gate, Some(true));
        assert!(diagnostic.publication_truth_export_blocker_top_level_proven_from_publication_state);
        assert!(!diagnostic.publication_truth_export_checkpoint_headline_attempted);
        assert!(!diagnostic.publication_truth_export_blocker_enrichment_attempted);
        assert_eq!(diagnostic.persisted_rebuild_checkpoint_exists, None);
        assert_eq!(diagnostic.source_comparison_applicable, None);
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_complete_fail_closed_fixture_returns_satisfied(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-fail-closed-green")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "fail_closed_current_runtime_but_recent_publication_truth_is_complete"
                    .to_string(),
                last_published_at: Some(now - Duration::minutes(5)),
                last_published_window_start: Some(metrics_window_start(now)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
            })?;
        let recent_raw_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        write_recent_raw_snapshot_sqlite_content(
            &recent_raw_dir.join("latest.sqlite"),
            &[recent_raw_swap(
                "raw-wallet",
                "sig-fail-closed-green",
                parse_ts("2026-04-16T09:55:00Z")?,
            )],
            parse_ts("2026-04-16T09:56:00Z")?,
        )?;

        let diagnostic = explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
            &fixture.config_path,
            now,
            StdDuration::from_secs(1),
        );
        assert_eq!(
            diagnostic.publication_truth_export_blocker_reason_class,
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportGateSatisfied
        );
        assert!(diagnostic.publication_truth_export_blocker_observed);
        assert_eq!(diagnostic.export_gate_runtime_mode.as_deref(), Some("fail_closed"));
        assert_eq!(diagnostic.publication_truth_complete, Some(true));
        assert_eq!(diagnostic.fresh_under_export_gate, Some(true));
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_replay_sol_leg_fixture_returns_cheap_checkpoint_headline(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-replay-sol-leg")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-checkpoint".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_checkpoint_state_json(false, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;
        let recent_raw_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        let recent_raw_latest = recent_raw_dir.join("latest.sqlite");
        write_recent_raw_snapshot_sqlite_content(
            &recent_raw_latest,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-source-ahead",
                parse_ts("2026-04-16T09:45:00Z")?,
            )],
            parse_ts("2026-04-16T09:46:00Z")?,
        )?;
        let recent_raw_store = SqliteStore::open(&recent_raw_latest)?;
        recent_raw_store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: parse_ts("2026-04-16T09:45:00Z")?,
            slot: 101,
            signature: "sig-source-ahead".to_string(),
        })?;

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocked_on_replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["export_gate_reason"],
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["publishable_checkpoint_blocker"],
            "replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["replay_sol_leg_incomplete_reason_class"],
            Value::Null
        );
        assert_eq!(parsed["persisted_rebuild_checkpoint_exists"], true);
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_completed"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_ms"],
            1000
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_source"],
            "fixed_constant_default_primary_operator"
        );
        assert!(parsed["publication_truth_export_checkpoint_headline_total_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_row_count_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_prepare_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_step_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(
            parsed["publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open"]
                .as_u64()
                .is_some()
        );
        assert!(
            parsed["publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup"]
                .as_u64()
                .is_some()
        );
        assert!(
            parsed["publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count"]
                .as_u64()
                .is_some()
        );
        assert!(
            parsed["publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare"]
                .as_u64()
                .is_some()
        );
        assert!(
            parsed["publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step"]
                .as_u64()
                .is_some()
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_exhausted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_stage"],
            "complete"
        );
        assert_eq!(parsed["rebuild_phase"], "replay");
        assert_eq!(parsed["rebuild_replay_subphase"], "sol_leg");
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_updated_at"],
            Value::String(
                (now - Duration::minutes(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            )
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes"],
            Value::Null
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes_probe_attempted"],
            false
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes_probe_completed"],
            false
        );
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        assert_eq!(
            parsed["source_scan_target_buy_mint_filter_active"],
            Value::Null
        );
        for key in [
            "publication_truth_export_blocker_observed",
            "publication_truth_export_blocker_reason_class",
            "publication_truth_export_blocker_explanation",
            "publication_truth_export_blocker_top_level_proven_from_publication_state",
            "publication_truth_export_checkpoint_headline_budget_ms",
            "publication_truth_export_checkpoint_headline_budget_source",
            "publication_truth_export_checkpoint_headline_total_elapsed_ms",
            "publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms",
            "publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms",
            "publication_truth_export_checkpoint_headline_row_count_elapsed_ms",
            "publication_truth_export_checkpoint_headline_prepare_elapsed_ms",
            "publication_truth_export_checkpoint_headline_step_elapsed_ms",
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open",
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup",
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_row_count",
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_prepare",
            "publication_truth_export_checkpoint_headline_budget_remaining_ms_after_step",
            "publication_truth_export_checkpoint_headline_attempted",
            "publication_truth_export_checkpoint_headline_completed",
            "publication_truth_export_checkpoint_headline_budget_exhausted",
            "publication_truth_export_checkpoint_headline_stage",
            "publication_truth_export_checkpoint_headline_explanation",
            "publication_truth_export_blocker_enrichment_attempted",
            "publication_truth_export_blocker_enrichment_completed",
            "publication_truth_export_blocker_enrichment_budget_exhausted",
            "publication_truth_export_blocker_enrichment_stage",
            "publication_truth_export_blocker_enrichment_explanation",
            "config_path",
            "runtime_db_path",
            "recent_raw_db_path",
            "export_gate_runtime_mode",
            "export_gate_reason",
            "publication_truth_complete",
            "fresh_under_export_gate",
            "last_published_at",
            "last_published_window_start",
            "published_wallet_count",
            "persisted_rebuild_checkpoint_exists",
            "persisted_rebuild_checkpoint_updated_at",
            "persisted_rebuild_checkpoint_state_json_bytes",
            "persisted_rebuild_checkpoint_state_json_bytes_probe_attempted",
            "persisted_rebuild_checkpoint_state_json_bytes_probe_completed",
            "persisted_rebuild_checkpoint_state_json_bytes_probe_budget_exhausted",
            "persisted_rebuild_checkpoint_state_json_bytes_probe_stage",
            "persisted_rebuild_checkpoint_state_json_bytes_probe_explanation",
            "rebuild_phase",
            "rebuild_replay_subphase",
            "publishable_checkpoint_blocker",
            "replay_incomplete",
            "replay_sol_leg_incomplete_reason_class",
            "replay_sol_leg_incomplete_explanation",
            "checkpoint_exact_target_surface_exists",
            "checkpoint_exact_target_surface_repairable_for_resume",
            "checkpoint_replay_candidate_activity_backfill_required",
            "checkpoint_replay_candidate_activity_backfill_pending",
            "source_comparison_applicable",
            "source_scan_target_buy_mint_filter_active",
            "source_scan_target_buy_mint_count",
            "source_rows_exist_beyond_stored_replay_checkpoint",
            "source_rows_ahead_count",
            "source_rows_ahead_pages_scanned",
            "source_rows_ahead_first_cursor",
            "source_rows_ahead_last_cursor",
            "source_scan_access_path",
            "source_scan_time_budget_exhausted",
        ] {
            assert!(
                parsed.get(key).is_some(),
                "expected required publication truth export blocker field {key}"
            );
        }
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_missing_recent_raw_without_checkpoint_blocker_returns_noncheckpoint_reason(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-missing-recent-raw")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_without_publishable_checkpoint".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocked_on_incomplete_or_stale_truth_without_checkpoint_explanation"
        );
        assert_eq!(parsed["publication_truth_export_blocker_observed"], true);
        assert_eq!(
            parsed["runtime_db_path"],
            fixture.db_path.display().to_string()
        );
        assert_eq!(parsed["persisted_rebuild_checkpoint_exists"], Value::Null);
        assert_eq!(parsed["publishable_checkpoint_blocker"], Value::Null);
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        assert_eq!(
            parsed["publication_truth_export_blocker_top_level_proven_from_publication_state"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_attempted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_attempted"],
            false
        );
        assert!(parsed["publication_truth_export_blocker_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("unsatisfied without a checkpoint-coded export gate reason"));
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_other_checkpoint_reason_short_circuits_before_recent_raw_open(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-other-checkpoint-blocker")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason:
                    "publication_truth_withheld_while_replay_candidate_activity_backfill_incomplete"
                        .to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-checkpoint-other".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_checkpoint_state_json(false, true)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocked_on_other_publishable_checkpoint_reason"
        );
        assert_eq!(parsed["publication_truth_export_blocker_observed"], true);
        assert_eq!(
            parsed["publishable_checkpoint_blocker"],
            "replay_candidate_activity_backfill_incomplete"
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_top_level_proven_from_publication_state"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_completed"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_exhausted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_completed"],
            true
        );
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        assert_eq!(
            parsed["replay_sol_leg_incomplete_reason_class"],
            Value::Null
        );
        assert!(parsed["publication_truth_export_blocker_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains(
                "publishable checkpoint blocker replay_candidate_activity_backfill_incomplete"
            ));
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_replay_missing_recent_raw_returns_top_level_blocker(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-replay-missing-recent-raw")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-checkpoint-replay-missing".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_checkpoint_state_json(false, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocked_on_replay_sol_leg_incomplete"
        );
        assert_eq!(parsed["publication_truth_export_blocker_observed"], true);
        assert_eq!(
            parsed["publishable_checkpoint_blocker"],
            "replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_top_level_proven_from_publication_state"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_completed"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_exhausted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_stage"],
            "complete"
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_completed"],
            true
        );
        assert_eq!(
            parsed["replay_sol_leg_incomplete_reason_class"],
            Value::Null
        );
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        assert_eq!(parsed["source_scan_time_budget_exhausted"], Value::Null);
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes"],
            Value::Null
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes_probe_attempted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_stage"],
            "checkpoint_headline"
        );
        assert!(parsed["publication_truth_export_blocker_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("blocked on replay_sol_leg_incomplete as already persisted in discovery publication state"));
        assert!(
            parsed["publication_truth_export_blocker_enrichment_explanation"]
                .as_str()
                .unwrap_or_default()
                .contains("proven cheap persisted rebuild row-meta path")
        );
        Ok(())
    }

    #[test]
    fn publication_truth_export_checkpoint_headline_other_checkpoint_meta_only_completion_preserves_top_level(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-other-checkpoint-meta-only")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason:
                    "publication_truth_withheld_while_replay_candidate_activity_backfill_incomplete"
                        .to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-checkpoint-other-meta-only".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: "{large blob intentionally ignored}".to_string(),
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let diagnostic = explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
            &fixture.config_path,
            now,
            StdDuration::from_secs(1),
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_reason_class,
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnOtherPublishableCheckpointReason
        );
        assert!(
            diagnostic.publication_truth_export_blocker_top_level_proven_from_publication_state
        );
        assert!(diagnostic.publication_truth_export_checkpoint_headline_attempted);
        assert!(diagnostic.publication_truth_export_checkpoint_headline_completed);
        assert!(!diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted);
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_stage
                .as_deref(),
            Some("complete")
        );
        assert_eq!(diagnostic.persisted_rebuild_checkpoint_exists, Some(true));
        assert_eq!(
            diagnostic.persisted_rebuild_checkpoint_updated_at,
            Some(now - Duration::minutes(1))
        );
        assert_eq!(diagnostic.persisted_rebuild_checkpoint_state_json_bytes, None);
        assert!(!diagnostic.persisted_rebuild_checkpoint_state_json_bytes_probe_attempted);
        assert_eq!(diagnostic.rebuild_phase.as_deref(), Some("replay"));
        assert_eq!(
            diagnostic.rebuild_replay_subphase.as_deref(),
            Some("activity_backfill")
        );
        assert_eq!(
            diagnostic.publishable_checkpoint_blocker.as_deref(),
            Some("replay_candidate_activity_backfill_incomplete")
        );
        Ok(())
    }

    #[test]
    fn publication_truth_export_checkpoint_headline_replay_budget_exhaustion_preserves_top_level(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-headline-budget-exhausted")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-checkpoint-budget".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: "{not touched}".to_string(),
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let diagnostic = explain_publication_truth_export_blocker_read_only_with_checkpoint_headline_budget(
            &fixture.config_path,
            now,
            StdDuration::ZERO,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_reason_class,
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnReplaySolLegIncomplete
        );
        assert!(
            diagnostic.publication_truth_export_blocker_top_level_proven_from_publication_state
        );
        assert!(diagnostic.publication_truth_export_checkpoint_headline_attempted);
        assert!(!diagnostic.publication_truth_export_checkpoint_headline_completed);
        assert_eq!(
            diagnostic.publication_truth_export_checkpoint_headline_budget_ms,
            1000
        );
        assert_eq!(
            diagnostic.publication_truth_export_checkpoint_headline_budget_source,
            "fixed_constant_default_primary_operator"
        );
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_budget_remaining_ms_after_open,
            0
        );
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_budget_remaining_ms_after_schema_lookup,
            0
        );
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms,
            0
        );
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_row_count_elapsed_ms,
            0
        );
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_prepare_elapsed_ms,
            0
        );
        assert_eq!(
            diagnostic.publication_truth_export_checkpoint_headline_step_elapsed_ms,
            0
        );
        assert!(diagnostic.publication_truth_export_checkpoint_headline_budget_exhausted);
        assert_eq!(
            diagnostic
                .publication_truth_export_checkpoint_headline_stage
                .as_deref(),
            Some("load_persisted_rebuild_row_meta_schema_lookup")
        );
        assert_eq!(diagnostic.replay_sol_leg_incomplete_reason_class, None);
        assert_eq!(diagnostic.source_comparison_applicable, None);
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_replay_missing_checkpoint_row_preserves_top_level(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-replay-missing-checkpoint-row")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocked_on_replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_completed"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_ms"],
            1000
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_source"],
            "fixed_constant_default_primary_operator"
        );
        assert!(parsed["publication_truth_export_checkpoint_headline_total_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_runtime_db_open_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_schema_lookup_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["publication_truth_export_checkpoint_headline_row_count_elapsed_ms"]
            .as_u64()
            .is_some());
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_prepare_elapsed_ms"],
            0
        );
        assert_eq!(parsed["publication_truth_export_checkpoint_headline_step_elapsed_ms"], 0);
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_exhausted"],
            false
        );
        assert_eq!(parsed["publication_truth_export_checkpoint_headline_stage"], "complete");
        assert_eq!(parsed["persisted_rebuild_checkpoint_exists"], false);
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_updated_at"],
            Value::Null
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes_probe_attempted"],
            false
        );
        assert_eq!(parsed["replay_sol_leg_incomplete_reason_class"], Value::Null);
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_checkpoint_headline_malformed_state_still_stops_before_recent_raw_open(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-publication-truth-headline-failure")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-checkpoint-headline-failure".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_checkpoint_state_json(false, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;
        let conn = rusqlite::Connection::open(&fixture.db_path)?;
        conn.execute(
            "UPDATE discovery_persisted_rebuild_state SET state_json = ?1 WHERE id = 1",
            ["{not valid json"],
        )?;

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocked_on_replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_attempted"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_completed"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_budget_exhausted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_checkpoint_headline_stage"],
            "complete"
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_stage"],
            "checkpoint_headline"
        );
        assert_eq!(
            parsed["replay_sol_leg_incomplete_reason_class"],
            Value::Null
        );
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes_probe_attempted"],
            false
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_completed"],
            true
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_enrichment_stage"],
            "checkpoint_headline"
        );
        assert!(parsed["publication_truth_export_checkpoint_headline_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("proven cheap persisted rebuild row-meta path"));
        Ok(())
    }

    #[test]
    fn run_command_publication_truth_export_blocker_missing_config_returns_unproven() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let config_path = temp.path().join("missing-live.server.toml");

        let rendered = run_command(Command::ExplainPublicationTruthExportBlocker(
            ExplainPublicationTruthExportBlockerConfig {
                config_path: config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["publication_truth_export_blocker_reason_class"],
            "publication_truth_export_blocker_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["publication_truth_export_blocker_observed"], false);
        assert_eq!(parsed["config_path"], config_path.display().to_string());
        assert_eq!(parsed["runtime_db_path"], Value::Null);
        assert_eq!(parsed["recent_raw_db_path"], Value::Null);
        assert!(parsed["publication_truth_export_blocker_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("could not be loaded"));
        Ok(())
    }

    #[test]
    fn run_command_replay_sol_leg_blocker_current_fixture_with_successful_deep_proof_fills_reason(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-replay-sol-leg-blocker-success")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-replay-sol-leg-checkpoint".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_exact_target_checkpoint_state_json(true, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;
        let recent_raw_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        let recent_raw_latest = recent_raw_dir.join("latest.sqlite");
        write_recent_raw_snapshot_sqlite_content(
            &recent_raw_latest,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-replay-sol-leg-ahead",
                parse_ts("2026-04-16T09:45:00Z")?,
            )],
            parse_ts("2026-04-16T09:46:00Z")?,
        )?;
        let recent_raw_store = SqliteStore::open(&recent_raw_latest)?;
        recent_raw_store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: parse_ts("2026-04-16T09:45:00Z")?,
            slot: 101,
            signature: "sig-replay-sol-leg-ahead".to_string(),
        })?;

        let rendered = run_command(Command::ExplainReplaySolLegBlocker(
            ExplainReplaySolLegBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["replay_sol_leg_blocker_reason_class"],
            "replay_sol_leg_blocker_proven_current"
        );
        assert_eq!(parsed["replay_sol_leg_blocker_observed"], true);
        assert_eq!(
            parsed["replay_sol_leg_blocker_prerequisite_reason_class"],
            "publication_truth_export_blocked_on_replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed"],
            true
        );
        assert_eq!(
            parsed["replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted"],
            false
        );
        assert_eq!(
            parsed["replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage"],
            "complete"
        );
        assert!(parsed["replay_sol_leg_blocker_prerequisite_total_elapsed_ms"]
            .as_u64()
            .is_some());
        assert_eq!(
            parsed["export_gate_reason"],
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(parsed["persisted_rebuild_checkpoint_exists"], true);
        assert_eq!(parsed["rebuild_phase"], "replay");
        assert_eq!(parsed["rebuild_replay_subphase"], "sol_leg");
        assert_eq!(parsed["replay_incomplete"], true);
        assert_eq!(
            parsed["replay_sol_leg_incomplete_reason_class"],
            "source_frontier_still_ahead_exact_target_filtered"
        );
        assert!(parsed["replay_sol_leg_incomplete_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("exact-target SOL-leg rows"));
        assert_eq!(parsed["checkpoint_exact_target_surface_exists"], true);
        assert_eq!(parsed["source_comparison_applicable"], true);
        assert_eq!(parsed["source_scan_target_buy_mint_filter_active"], true);
        assert_eq!(
            parsed["source_rows_exist_beyond_stored_replay_checkpoint"],
            true
        );
        assert_eq!(parsed["source_rows_ahead_count"], 1);
        assert_eq!(parsed["source_scan_time_budget_exhausted"], false);
        assert_eq!(parsed["replay_sol_leg_blocker_budget_ms"], 30000);
        assert_eq!(
            parsed["replay_sol_leg_blocker_budget_source"],
            "copybot_discovery_default_replay_sol_leg_read_only_source_scan_budget"
        );
        assert!(parsed["replay_sol_leg_blocker_total_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["replay_sol_leg_blocker_checkpoint_headline_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["replay_sol_leg_blocker_deep_reason_elapsed_ms"]
            .as_u64()
            .is_some());
        assert_eq!(parsed["replay_sol_leg_blocker_budget_exhausted"], false);
        assert_eq!(parsed["replay_sol_leg_blocker_budget_exhausted_stage"], Value::Null);
        for key in [
            "replay_sol_leg_blocker_observed",
            "replay_sol_leg_blocker_reason_class",
            "replay_sol_leg_blocker_explanation",
            "replay_sol_leg_blocker_prerequisite_reason_class",
            "replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed",
            "replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted",
            "replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage",
            "replay_sol_leg_blocker_prerequisite_total_elapsed_ms",
            "config_path",
            "runtime_db_path",
            "recent_raw_db_path",
            "export_gate_runtime_mode",
            "export_gate_reason",
            "publication_truth_complete",
            "fresh_under_export_gate",
            "persisted_rebuild_checkpoint_exists",
            "persisted_rebuild_checkpoint_updated_at",
            "rebuild_phase",
            "rebuild_replay_subphase",
            "replay_incomplete",
            "replay_sol_leg_incomplete_reason_class",
            "replay_sol_leg_incomplete_explanation",
            "checkpoint_exact_target_surface_exists",
            "checkpoint_exact_target_surface_repairable_for_resume",
            "checkpoint_replay_candidate_activity_backfill_required",
            "checkpoint_replay_candidate_activity_backfill_pending",
            "source_comparison_applicable",
            "source_scan_target_buy_mint_filter_active",
            "source_scan_target_buy_mint_count",
            "source_rows_exist_beyond_stored_replay_checkpoint",
            "source_rows_ahead_count",
            "source_rows_ahead_pages_scanned",
            "source_rows_ahead_first_cursor",
            "source_rows_ahead_last_cursor",
            "source_scan_access_path",
            "source_scan_time_budget_exhausted",
            "replay_sol_leg_blocker_budget_ms",
            "replay_sol_leg_blocker_budget_source",
            "replay_sol_leg_blocker_total_elapsed_ms",
            "replay_sol_leg_blocker_checkpoint_headline_elapsed_ms",
            "replay_sol_leg_blocker_deep_reason_elapsed_ms",
            "replay_sol_leg_blocker_budget_exhausted",
            "replay_sol_leg_blocker_budget_exhausted_stage",
        ] {
            assert!(parsed.get(key).is_some(), "missing key {key}");
        }
        Ok(())
    }

    #[test]
    fn run_command_replay_sol_leg_blocker_non_replay_current_blocker_skips_deep_proof(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-replay-sol-leg-blocker-not-current")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason:
                    "publication_truth_withheld_while_replay_candidate_activity_backfill_incomplete"
                        .to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-replay-activity-backfill".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_checkpoint_state_json(false, true)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let rendered = run_command(Command::ExplainReplaySolLegBlocker(
            ExplainReplaySolLegBlockerConfig {
                config_path: fixture.config_path.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["replay_sol_leg_blocker_reason_class"],
            "replay_sol_leg_blocker_not_current_export_blocker"
        );
        assert_eq!(
            parsed["replay_sol_leg_blocker_prerequisite_reason_class"],
            "publication_truth_export_blocked_on_other_publishable_checkpoint_reason"
        );
        assert_eq!(
            parsed["export_gate_reason"],
            "publication_truth_withheld_while_replay_candidate_activity_backfill_incomplete"
        );
        assert_eq!(parsed["replay_sol_leg_incomplete_reason_class"], Value::Null);
        assert_eq!(parsed["source_comparison_applicable"], Value::Null);
        assert_eq!(parsed["replay_sol_leg_blocker_deep_reason_elapsed_ms"], 0);
        assert_eq!(parsed["replay_sol_leg_blocker_budget_exhausted"], false);
        Ok(())
    }

    #[test]
    fn run_command_replay_sol_leg_blocker_missing_recent_raw_returns_unproven() -> Result<()> {
        let fixture = make_fixture("runtime-export-replay-sol-leg-blocker-missing-recent-raw")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-replay-sol-leg-missing-recent-raw".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_exact_target_checkpoint_state_json(true, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let diagnostic = explain_replay_sol_leg_blocker_read_only(&fixture.config_path, now);
        assert_eq!(
            diagnostic.replay_sol_leg_blocker_reason_class,
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence
        );
        assert_eq!(
            diagnostic.replay_sol_leg_blocker_prerequisite_reason_class,
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnReplaySolLegIncomplete
        );
        assert!(
            diagnostic.replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed
        );
        assert!(
            !diagnostic.replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted
        );
        assert_eq!(
            diagnostic
                .replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage
                .as_deref(),
            Some("complete")
        );
        assert!(diagnostic.replay_sol_leg_blocker_explanation.contains(
            "promoted recent_raw latest db"
        ));
        assert_eq!(diagnostic.replay_sol_leg_blocker_deep_reason_elapsed_ms, 0);
        assert_eq!(diagnostic.replay_sol_leg_incomplete_reason_class, None);
        Ok(())
    }

    #[test]
    fn replay_sol_leg_blocker_forced_deep_budget_exhaustion_preserves_current_blocker_proof(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-export-replay-sol-leg-blocker-budget-exhausted")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-replay-sol-leg-budget-exhausted".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_exact_target_checkpoint_state_json(true, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;
        let recent_raw_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        let recent_raw_latest = recent_raw_dir.join("latest.sqlite");
        write_recent_raw_snapshot_sqlite_content(
            &recent_raw_latest,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-replay-sol-leg-budget-exhausted-ahead",
                parse_ts("2026-04-16T09:45:00Z")?,
            )],
            parse_ts("2026-04-16T09:46:00Z")?,
        )?;

        arm_test_force_replay_sol_leg_blocker_deep_reason_budget_exhausted();
        let diagnostic = explain_replay_sol_leg_blocker_read_only(&fixture.config_path, now);
        assert_eq!(
            diagnostic.replay_sol_leg_blocker_reason_class,
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerProvenCurrent
        );
        assert!(diagnostic.replay_sol_leg_blocker_budget_exhausted);
        assert_eq!(
            diagnostic
                .replay_sol_leg_blocker_budget_exhausted_stage
                .as_deref(),
            Some("deep_replay_sol_leg_reason_source_scan_budget_exhausted")
        );
        assert_eq!(diagnostic.persisted_rebuild_checkpoint_exists, Some(true));
        assert_eq!(diagnostic.rebuild_phase.as_deref(), Some("replay"));
        assert_eq!(diagnostic.rebuild_replay_subphase.as_deref(), Some("sol_leg"));
        assert_eq!(diagnostic.replay_sol_leg_incomplete_reason_class, None);
        assert_eq!(diagnostic.replay_sol_leg_incomplete_explanation, None);
        assert_eq!(diagnostic.source_comparison_applicable, None);
        assert_eq!(diagnostic.source_scan_time_budget_exhausted, None);
        Ok(())
    }

    #[test]
    fn replay_sol_leg_blocker_forced_prerequisite_failure_reports_prerequisite_telemetry(
    ) -> Result<()> {
        let fixture =
            make_fixture("runtime-export-replay-sol-leg-blocker-prerequisite-budget-exhausted")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now) - Duration::hours(1)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                window_start: metrics_window_start(now),
                horizon_end: metrics_window_start(now) + Duration::days(7),
                metrics_window_start: metrics_window_start(now),
                phase_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: parse_ts("2026-04-16T09:40:00Z")?,
                    slot: 100,
                    signature: "sig-replay-sol-leg-prerequisite-budget".to_string(),
                }),
                prepass_rows_processed: 0,
                prepass_pages_processed: 0,
                replay_rows_processed: 1,
                replay_pages_processed: 1,
                chunks_completed: 0,
                state_json: replay_sol_leg_exact_target_checkpoint_state_json(true, false)?,
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;

        let diagnostic = explain_replay_sol_leg_blocker_read_only_with_prerequisite_budget(
            &fixture.config_path,
            now,
            StdDuration::ZERO,
        );
        assert_eq!(
            diagnostic.replay_sol_leg_blocker_reason_class,
            ReplaySolLegBlockerReasonClass::ReplaySolLegBlockerUnprovenDueToMissingEvidence
        );
        assert_eq!(
            diagnostic.replay_sol_leg_blocker_prerequisite_reason_class,
            PublicationTruthExportBlockerReasonClass::PublicationTruthExportBlockedOnReplaySolLegIncomplete
        );
        assert!(
            !diagnostic.replay_sol_leg_blocker_prerequisite_checkpoint_headline_completed
        );
        assert!(
            diagnostic.replay_sol_leg_blocker_prerequisite_checkpoint_headline_budget_exhausted
        );
        assert_eq!(
            diagnostic
                .replay_sol_leg_blocker_prerequisite_checkpoint_headline_stage
                .as_deref(),
            Some("load_persisted_rebuild_row_meta_schema_lookup")
        );
        assert_eq!(diagnostic.replay_sol_leg_blocker_deep_reason_elapsed_ms, 0);
        assert!(
            diagnostic.replay_sol_leg_blocker_total_elapsed_ms
                >= diagnostic.replay_sol_leg_blocker_prerequisite_total_elapsed_ms
        );
        assert!(diagnostic
            .replay_sol_leg_blocker_explanation
            .contains("cheap checkpoint-headline prerequisite path did not complete"));
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_promotion_blocker_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-promotion-blocker")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawPromotionBlocker(
            ExplainRecentRawPromotionBlockerConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_promotion_reason_class"],
            "recent_raw_promotion_ready_now"
        );
        assert_eq!(parsed["recent_raw_promoted_exists"], true);
        assert_eq!(parsed["recent_raw_staged_exists"], true);
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_catch_up_status_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-catch-up-status")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawCatchUpStatus(
            ExplainRecentRawCatchUpStatusConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_catch_up_reason_class"],
            "recent_raw_catch_up_caught_up"
        );
        assert_eq!(parsed["recent_raw_promoted_exists"], true);
        assert_eq!(parsed["recent_raw_staged_exists"], true);
        assert_eq!(parsed["recent_raw_catch_up_progressing"], false);
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_lineage_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-lineage")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawStagedLineage(
            ExplainRecentRawStagedLineageConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_staged_lineage_reason_class"],
            "recent_raw_staged_lineage_monotonic_but_incomplete"
        );
        assert_eq!(parsed["recent_raw_staged_same_source_db_as_promoted"], true);
        assert_eq!(
            parsed["recent_raw_staged_cursor_relation_to_promoted"],
            "ahead"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_relation_basis"],
            "direct_covered_through_cursor_comparison"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_ts_relation_to_promoted"],
            "ahead"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_slot_relation_to_promoted"],
            "ahead"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_signature_equal_to_promoted"],
            false
        );
        assert!(parsed["recent_raw_staged_cursor_relation_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("direct covered-through cursor comparison"));
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_regression_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-regression")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawStagedRegression(
            ExplainRecentRawStagedRegressionConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_staged_regression_reason_class"],
            "recent_raw_staged_regression_artifact_itself_already_behind"
        );
        assert_eq!(
            parsed["recent_raw_selected_staged_created_after_promoted"],
            true
        );
        assert_eq!(
            parsed["recent_raw_selected_staged_frontier_behind_promoted_before_comparison"],
            true
        );
        assert_eq!(
            parsed["recent_raw_selected_staged_completed_after_creation"],
            false
        );
        assert_eq!(parsed["recent_raw_staged_candidate_count"], 1);
        assert_eq!(
            parsed["recent_raw_multiple_staged_candidates_present"],
            false
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_birth_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-birth")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawStagedBirth(
            ExplainRecentRawStagedBirthConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_staged_birth_reason_class"],
            "recent_raw_staged_current_artifact_manifest_and_sqlite_content_agree_but_behind"
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_proven_from_current_artifacts"],
            false
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_manifest_matches_sqlite_content"],
            true
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_manifest_sqlite_match_unproven"],
            false
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_covered_through_relation_to_promoted"],
            "behind"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_window_seeding_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-window-seeding")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-early",
                    parse_ts("2026-04-14T07:54:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-late",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");

        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
            )],
            parse_ts("2026-04-14T08:03:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:03:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:55:00Z",
                "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                "signature": "sig-staged"
            },
            "last_batch_completed_at": "2026-04-14T08:03:00Z",
            "updated_at": "2026-04-14T08:03:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawStagedWindowSeeding(
            ExplainRecentRawStagedWindowSeedingConfig {
                state_root: state_root.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_staged_window_seeding_reason_class"],
            "recent_raw_staged_window_current_start_matches_promoted_start"
        );
        assert_eq!(
            parsed["recent_raw_staged_start_matches_promoted_start"],
            true
        );
        assert_eq!(
            parsed["recent_raw_staged_start_current_evidence_basis"],
            "matches_promoted_start"
        );
        assert_eq!(
            parsed
                ["recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promoted_can_seed_staged_progress_under_current_code"],
            true
        );
        assert_eq!(
            parsed["recent_raw_staged_manifest_matches_sqlite_content"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_source_window_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-source-window-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:54:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.store.prune_recent_raw_journal_before_batch(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:57:00Z",
                "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawSourceWindowContract(
            ExplainRecentRawSourceWindowContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_source_window_contract_reason_class"],
            "recent_raw_source_window_current_contract_excludes_older_rows"
        );
        assert_eq!(parsed["recent_raw_source_start_later_than_promoted"], true);
        assert_eq!(
            parsed["recent_raw_source_contract_currently_excludes_older_rows"],
            true
        );
        assert_eq!(
            parsed["recent_raw_source_window_matches_current_bounded_contract"],
            true
        );
        assert_eq!(parsed["recent_raw_source_window_probe_bounded"], true);
        assert_eq!(
            parsed["recent_raw_source_window_probe_mode"],
            "bounded_index_edges"
        );
        assert_eq!(
            parsed["recent_raw_source_cached_state_matches_bounded_probe"],
            true
        );
        assert!(parsed["recent_raw_source_bounded_probe_covered_since"].is_string());
        assert!(parsed["recent_raw_source_bounded_probe_covered_through"].is_object());
        assert!(parsed["recent_raw_source_scanned_covered_since"].is_null());
        assert!(parsed["recent_raw_source_scanned_covered_through"].is_null());
        assert!(parsed["recent_raw_source_scanned_row_count"].is_null());
        assert!(parsed["recent_raw_source_cached_state_matches_scanned_rows"].is_null());
        assert_eq!(
            parsed["recent_raw_promoted_reflects_older_still_promoted_window"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_promoted_retention_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-promoted-retention-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:54:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.store.prune_recent_raw_journal_before_batch(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:57:00Z",
                "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawPromotedRetentionContract(
            ExplainRecentRawPromotedRetentionContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_promoted_retention_reason_class"],
            "recent_raw_promoted_retained_by_design_despite_older_window"
        );
        assert_eq!(
            parsed["recent_raw_promoted_currently_retained_as_truth"],
            true
        );
        assert_eq!(
            parsed["recent_raw_promoted_has_current_contract_invalidation_rule"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promoted_invalidated_by_current_source_window_shift"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promoted_start_older_than_current_source"],
            true
        );
        assert_eq!(
            parsed["recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface"],
            true
        );
        assert_eq!(
            parsed["recent_raw_stage3_truth_can_advance_without_new_promotion"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promotion_reason_class"],
            "recent_raw_promotion_blocked_by_missing_staged_snapshot"
        );
        assert_eq!(
            parsed["recent_raw_source_window_contract_reason_class"],
            "recent_raw_source_window_current_contract_excludes_older_rows"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_promotion_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-replacement-promotion-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:55:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-promoted",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.store.prune_recent_raw_journal_before_batch(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");

        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-promoted",
                parse_ts("2026-04-14T07:56:00Z")?,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:05:00Z",
            "updated_at": "2026-04-14T08:05:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawReplacementPromotionContract(
            ExplainRecentRawReplacementPromotionContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_promotion_reason_class"],
            "recent_raw_replacement_candidate_incomplete_against_current_source"
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_exists"], true);
        assert_eq!(
            parsed["recent_raw_replacement_candidate_source_db_matches_promoted"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_start_matches_current_source"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_complete_against_current_source"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_promotable_now"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_promotion_would_retire_current_promoted_truth"],
            true
        );
        assert_eq!(
            parsed["recent_raw_stage3_blocked_on_replacement_candidate"],
            true
        );
        assert_eq!(
            parsed["recent_raw_promotion_reason_class"],
            "recent_raw_promotion_blocked_by_incomplete_staged_coverage"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_progress_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-replacement-progress-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-c",
                    parse_ts("2026-04-14T07:58:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:12:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        let previous_staged_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.prev");
        let previous_staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.prev.json");

        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:10:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:57:00Z",
                "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                "signature": "sig-source-b"
            },
            "last_batch_completed_at": "2026-04-14T08:10:00Z",
            "updated_at": "2026-04-14T08:10:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;
        fs::write(&previous_staged_path, b"snapshot")
            .with_context(|| format!("failed writing {}", previous_staged_path.display()))?;
        let previous_staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": previous_staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-source-a"
            },
            "last_batch_completed_at": "2026-04-14T08:05:00Z",
            "updated_at": "2026-04-14T08:05:00Z",
            "snapshot_bytes": 8u64
        });
        write_json_atomic(&previous_staged_metadata_path, &previous_staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawReplacementProgressContract(
            ExplainRecentRawReplacementProgressContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_progress_reason_class"],
            "recent_raw_replacement_progress_advancing_but_incomplete"
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_exists"], true);
        assert_eq!(
            parsed["recent_raw_replacement_candidate_same_identity_as_previous_attempt"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_row_count_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_covered_through_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_last_batch_completed_at_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_completed_after_creation"],
            true
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_advancing"], true);
        assert_eq!(parsed["recent_raw_replacement_candidate_stalled"], false);
        assert_eq!(
            parsed["recent_raw_replacement_candidate_reset_or_recreated"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_promotion_reason_class"],
            "recent_raw_replacement_candidate_incomplete_against_current_source"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_artifact_history_contract_renders_json() -> Result<()> {
        let fixture =
            make_fixture("runtime-export-recent-raw-replacement-artifact-history-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-source-a",
                parse_ts("2026-04-14T07:56:00Z")?,
            )],
            parse_ts("2026-04-14T08:10:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-source-a"
            },
            "last_batch_completed_at": "2026-04-14T08:10:00Z",
            "updated_at": "2026-04-14T08:10:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawReplacementArtifactHistoryContract(
            ExplainRecentRawReplacementArtifactHistoryContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_artifact_history_reason_class"],
            "recent_raw_replacement_artifact_history_fixed_path_overwrite_by_design"
        );
        assert_eq!(
            parsed["recent_raw_replacement_fixed_path_overwrite_contract"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_previous_artifact_archive_path_present"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_previous_artifact_history_expected_under_current_contract"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_previous_artifact_history_missing_due_to_unproven_evidence"],
            false
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_attempt_telemetry_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-replacement-attempt-telemetry")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        let telemetry_path =
            recent_raw_dir.join("discovery_recent_raw_snapshot_attempt_latest.json");
        write_json_atomic(
            &telemetry_path,
            &serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": true,
                "staged_seeded_from_latest_surface": false,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": true,
                "staged_row_count_before_attempt": 1,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-source-a"
                },
                "staged_covered_through_cursor_after_attempt": {
                    "ts_utc": "2026-04-14T07:57:00Z",
                    "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                    "signature": "sig-source-b"
                },
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let rendered = run_command(Command::ExplainRecentRawReplacementAttemptTelemetry(
            ExplainRecentRawReplacementAttemptTelemetryConfig {
                state_root,
                json: true,
                deep_attempt_telemetry_scan: false,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_reason_class"],
            "recent_raw_replacement_attempt_telemetry_advancing_but_incomplete"
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_artifact_count"],
            1
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_parseable_count"],
            1
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_probe_mode"],
            "bounded_explicit_paths"
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_probe_bounded"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_deep_scan_used"],
            false
        );
        assert!(
            parsed["recent_raw_replacement_attempt_telemetry_explicit_paths_checked"]
                .as_array()
                .is_some_and(|paths| !paths.is_empty())
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_scanned_dirs"],
            serde_json::json!([])
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_proves_advancing"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_last_row_count_before"],
            1
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_last_row_count_after"],
            2
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_staged_progress_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_staged_progress_resumed"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_staged_progress_preserved_for_retry"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_convergence_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-replacement-convergence")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-promoted",
                    parse_ts("2026-04-14T07:55:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-staged",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &recent_raw_dir.join("discovery_recent_raw_snapshot_attempt_latest.json"),
            &serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": true,
                "staged_seeded_from_latest_surface": false,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": true,
                "staged_row_count_before_attempt": 1,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "staged_covered_through_cursor_after_attempt": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let rendered = run_command(Command::ExplainRecentRawReplacementConvergence(
            ExplainRecentRawReplacementConvergenceConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_convergence_reason_class"],
            "recent_raw_replacement_convergence_advancing_but_incomplete"
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_exists"], true);
        assert_eq!(parsed["recent_raw_replacement_candidate_row_count"], 2);
        assert_eq!(parsed["recent_raw_source_row_count"], 3);
        assert_eq!(
            parsed["recent_raw_replacement_rows_remaining_to_current_source"],
            1
        );
        assert_eq!(
            parsed["recent_raw_replacement_latest_attempt_row_count_delta"],
            1
        );
        assert_eq!(
            parsed["recent_raw_replacement_estimated_attempts_to_current_source"],
            1
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_parseable"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_attempt_telemetry_probe_bounded"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_writes_runtime_artifact_json() -> Result<()> {
        let fixture = make_fixture("runtime-export")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export.json")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })?;

        assert!(output.contains("event=discovery_runtime_export"));
        let artifact_path = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("artifacts/runtime-export.json");
        let artifact: DiscoveryRuntimeArtifact = load_json(&artifact_path)?;
        assert_eq!(artifact.runtime_cursor.signature, "runtime-export-cursor");
        assert_eq!(
            artifact.publication_state.last_published_at,
            Some(now - Duration::minutes(5))
        );
        assert_eq!(artifact.published_wallet_metrics_snapshot.len(), 2);
        Ok(())
    }

    #[test]
    fn run_exports_fresh_complete_fail_closed_publication_truth() -> Result<()> {
        let fixture = make_fixture("runtime-export-fail-closed-publication-truth")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        let metrics_window_start = metrics_window_start(now);
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "fail_closed_current_runtime_but_recent_publication_truth_is_complete"
                    .to_string(),
                last_published_at: Some(now - Duration::minutes(5)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
            })?;

        let output_json = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export-fail-closed.json")),
            scheduled: false,
            force: false,
            json: true,
            now,
        })?;

        let output: serde_json::Value =
            serde_json::from_str(&output_json).context("failed parsing json")?;
        assert_eq!(output["state"], "written");
        assert_eq!(output["publication_runtime_mode"], "fail_closed");
        assert_eq!(output["publication_truth_complete"], true);
        assert_eq!(output["fresh_under_export_gate"], true);

        let artifact_path = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("artifacts/runtime-export-fail-closed.json");
        let artifact: DiscoveryRuntimeArtifact = load_json(&artifact_path)?;
        assert_eq!(
            artifact.publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert_eq!(
            artifact.publication_state.last_published_at,
            Some(now - Duration::minutes(5))
        );
        Ok(())
    }

    #[test]
    fn run_refuses_stale_publication_truth_with_actionable_diagnostics() -> Result<()> {
        let fixture = make_fixture("runtime-export-stale-publication-truth")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "stale_publication_truth_for_export".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
            })?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export-stale.json")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })
        .expect_err("stale publication truth must refuse export");
        let error_text = format!("{error:#}");
        assert!(error_text.contains("requires fresh publication truth under export gate"));
        assert!(error_text.contains("runtime_mode=healthy"));
        assert!(error_text.contains("fresh_under_export_gate=false"));
        assert!(error_text.contains("published_wallet_count=1"));
        Ok(())
    }

    #[test]
    fn run_refuses_incomplete_publication_truth_with_actionable_diagnostics() -> Result<()> {
        let fixture = make_fixture("runtime-export-incomplete-publication-truth")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "missing_wallet_ids".to_string(),
                last_published_at: Some(now - Duration::minutes(5)),
                last_published_window_start: Some(metrics_window_start(now)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export-incomplete.json")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })
        .expect_err("incomplete publication truth must refuse export");
        let error_text = format!("{error:#}");
        assert!(error_text.contains("requires complete publication truth"));
        assert!(error_text.contains("runtime_mode=fail_closed"));
        assert!(error_text.contains("missing_fields=published_wallet_ids"));
        Ok(())
    }

    #[test]
    fn scheduled_run_writes_latest_and_prunes_archives() -> Result<()> {
        let fixture = make_fixture("runtime-export-scheduled")?;
        let first_now = parse_ts("2026-03-23T12:10:00Z")?;
        let second_now = parse_ts("2026-03-23T12:21:00Z")?;
        let third_now = parse_ts("2026-03-23T12:32:00Z")?;
        seed_runtime_export_source(&fixture.store, third_now)?;

        for now in [first_now, second_now, third_now] {
            run(Config {
                config_path: fixture.config_path.clone(),
                db_path: None,
                output_path: None,
                scheduled: true,
                force: true,
                json: false,
                now,
            })?;
        }

        let archive_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/artifacts");
        let archives = std::fs::read_dir(&archive_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("discovery_runtime_") && name.ends_with(".json")
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(archives.len(), 2, "retention should prune oldest archive");
        assert!(archive_dir.join("latest.json").exists());
        Ok(())
    }

    #[test]
    fn scheduled_run_skips_when_cadence_not_elapsed() -> Result<()> {
        let fixture = make_fixture("runtime-export-skip")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;

        run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: None,
            scheduled: true,
            force: false,
            json: false,
            now,
        })?;

        let skipped = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: now + Duration::minutes(3),
        })?;
        let output: serde_json::Value =
            serde_json::from_str(&skipped).context("failed parsing json")?;
        assert_eq!(output["state"], "skipped_not_due");
        Ok(())
    }

    struct Fixture {
        store: SqliteStore,
        config_path: PathBuf,
        db_path: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(format!("{name}.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        std::fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[runtime_restore_ops]\nartifact_retention = 2\nartifact_cadence_minutes = 10\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(Fixture {
            store,
            config_path,
            db_path,
            _temp: temp,
        })
    }

    fn seed_runtime_export_source(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let metrics_window_start = metrics_window_start(now);
        store.persist_discovery_cycle(
            &[
                WalletUpsertRow {
                    wallet_id: "wallet-alpha".to_string(),
                    first_seen: now - Duration::days(3),
                    last_seen: now - Duration::minutes(2),
                    status: "candidate".to_string(),
                },
                WalletUpsertRow {
                    wallet_id: "wallet-beta".to_string(),
                    first_seen: now - Duration::days(2),
                    last_seen: now - Duration::minutes(1),
                    status: "observed".to_string(),
                },
            ],
            &[
                WalletMetricRow {
                    wallet_id: "wallet-alpha".to_string(),
                    window_start: metrics_window_start,
                    pnl: 2.8,
                    win_rate: 0.8,
                    trades: 6,
                    closed_trades: 6,
                    hold_median_seconds: 120,
                    score: 1.0,
                    buy_total: 6,
                    tradable_ratio: 1.0,
                    rug_ratio: 0.0,
                },
                WalletMetricRow {
                    wallet_id: "wallet-beta".to_string(),
                    window_start: metrics_window_start,
                    pnl: 0.3,
                    win_rate: 0.5,
                    trades: 4,
                    closed_trades: 4,
                    hold_median_seconds: 240,
                    score: 0.1,
                    buy_total: 4,
                    tradable_ratio: 0.5,
                    rug_ratio: 0.25,
                },
            ],
            &["wallet-alpha".to_string()],
            true,
            true,
            now - Duration::minutes(5),
            "seed_runtime_export",
        )?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "seed_runtime_export".to_string(),
            last_published_at: Some(now - Duration::minutes(5)),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
        })?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(1),
            slot: 42,
            signature: "runtime-export-cursor".to_string(),
        })?;
        Ok(())
    }

    fn seed_recent_raw_source_state(
        store: &SqliteStore,
        _db_path: &Path,
        recent_raw_dir: &Path,
    ) -> Result<()> {
        let now = parse_ts("2026-04-14T08:06:00Z")?;
        store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-promoted",
                    parse_ts("2026-04-14T07:55:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-staged",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
            ],
            now,
        )?;
        std::fs::create_dir_all(recent_raw_dir)
            .with_context(|| format!("failed creating {}", recent_raw_dir.display()))?;
        Ok(())
    }

    fn write_recent_raw_snapshot_sqlite_content(
        snapshot_path: &Path,
        swaps: &[copybot_core_types::SwapEvent],
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        if snapshot_path.exists() {
            fs::remove_file(snapshot_path)
                .with_context(|| format!("failed removing {}", snapshot_path.display()))?;
        }
        let store = SqliteStore::open(snapshot_path)
            .with_context(|| format!("failed opening {}", snapshot_path.display()))?;
        store.insert_recent_raw_journal_batch(swaps, completed_at)?;
        Ok(())
    }

    fn metrics_window_start(now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = 1_800_i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(7)
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn recent_raw_swap(
        wallet: &str,
        signature: &str,
        ts_utc: DateTime<Utc>,
    ) -> copybot_core_types::SwapEvent {
        copybot_core_types::SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenMint1111111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot: ts_utc.timestamp().max(0) as u64,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn replay_sol_leg_checkpoint_state_json(
        replay_candidate_activity_backfill_required: bool,
        replay_candidate_activity_backfill_pending: bool,
    ) -> Result<String> {
        serde_json::to_string(&serde_json::json!({
            "unique_buy_mints": [],
            "discovery_critical_target_buy_mints": [],
            "buy_mint_counts": {},
            "replay_wallet_stats_complete": true,
            "replay_sol_leg_reentry_pending": false,
            "replay_candidate_activity_backfill_required":
                replay_candidate_activity_backfill_required,
            "replay_candidate_activity_backfill_pending":
                replay_candidate_activity_backfill_pending,
            "token_quality_cache": {},
            "token_quality_progress": {
                "next_mint_index": 0,
                "rpc_attempted": 0,
                "rpc_spent_ms": 0
            },
            "by_wallet": {},
            "token_states": {},
            "token_recent_sol_trades": {},
            "pending_rug_checks": [],
            "token_pending_buy_starts": {},
            "completed_snapshots": []
        }))
        .context("failed serializing replay checkpoint state json")
    }

    fn replay_sol_leg_exact_target_checkpoint_state_json(
        replay_candidate_activity_backfill_required: bool,
        replay_candidate_activity_backfill_pending: bool,
    ) -> Result<String> {
        serde_json::to_string(&serde_json::json!({
            "unique_buy_mints": ["TokenMint1111111111111111111111111111111111"],
            "discovery_critical_target_buy_mints": ["TokenMint1111111111111111111111111111111111"],
            "buy_mint_counts": {
                "TokenMint1111111111111111111111111111111111": 1
            },
            "replay_wallet_stats_complete": true,
            "replay_sol_leg_reentry_pending": false,
            "replay_candidate_activity_backfill_required":
                replay_candidate_activity_backfill_required,
            "replay_candidate_activity_backfill_pending":
                replay_candidate_activity_backfill_pending,
            "token_quality_cache": {},
            "token_quality_progress": {
                "next_mint_index": 0,
                "rpc_attempted": 0,
                "rpc_spent_ms": 0
            },
            "by_wallet": {},
            "token_states": {},
            "token_recent_sol_trades": {},
            "pending_rug_checks": [],
            "token_pending_buy_starts": {},
            "completed_snapshots": []
        }))
        .context("failed serializing exact-target replay checkpoint state json")
    }
}
