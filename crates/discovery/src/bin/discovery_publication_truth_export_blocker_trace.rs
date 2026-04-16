use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{runtime_restore_ops::resolve_db_path, DiscoveryService};
use copybot_storage::{DiscoveryPublicationStateRow, DiscoveryRuntimeMode};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

const USAGE: &str = "usage:
  discovery_publication_truth_export_blocker_trace --config <path> [--json]";
const DEFAULT_TRACE_BUDGET_MS: u64 = 1_000;
const PUBLICATION_TRUTH_WITHHELD_PREFIX: &str = "publication_truth_withheld_while_";

// Timings in this diagnostic use the following convention:
// - always-on stages (config/runtime-db open/publication-state load) use 0 when not completed
// - row-meta substep timings use null when skipped or incomplete
// - row-meta total uses 0 when the row-meta path was not started
const PUBLICATION_STATE_SQL: &str = "SELECT
    publication_runtime_mode,
    publication_reason,
    publication_last_published_at,
    publication_last_published_window_start,
    publication_scoring_source,
    publication_wallet_ids_json,
    publication_policy_fingerprint,
    updated_at
 FROM discovery_strategy_state
 WHERE id = 1";
const CHECKPOINT_TABLE_EXISTS_SQL: &str = "SELECT EXISTS(
    SELECT 1
    FROM sqlite_master
    WHERE type = 'table' AND name = 'discovery_persisted_rebuild_state'
)";
const CHECKPOINT_ROW_COUNT_SQL: &str =
    "SELECT COUNT(1) FROM discovery_persisted_rebuild_state WHERE id = 1";
const CHECKPOINT_ROW_META_SQL: &str =
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
struct ExplainConfig {
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
enum Command {
    Explain(ExplainConfig),
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PublicationTruthExportBlockerTraceReasonClass {
    PublicationTruthExportBlockerTraceGreen,
    PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily,
    PublicationTruthExportBlockerTraceNonGreenWithoutCheckpointFamily,
    PublicationTruthExportBlockerTraceUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PublicationTruthExportBlockerTraceStage {
    LoadConfig,
    OpenRuntimeDbReadOnly,
    LoadPublicationState,
    CheckpointRowMetaSchemaLookup,
    CheckpointRowMetaRowCount,
    CheckpointRowMetaPreparePrimaryKeyLookup,
    CheckpointRowMetaStepPrimaryKeyLookup,
    Complete,
}

impl PublicationTruthExportBlockerTraceStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::LoadConfig => "load_config",
            Self::OpenRuntimeDbReadOnly => "open_runtime_db_read_only",
            Self::LoadPublicationState => "load_publication_state",
            Self::CheckpointRowMetaSchemaLookup => "checkpoint_row_meta_schema_lookup",
            Self::CheckpointRowMetaRowCount => "checkpoint_row_meta_row_count",
            Self::CheckpointRowMetaPreparePrimaryKeyLookup => {
                "checkpoint_row_meta_prepare_primary_key_lookup"
            }
            Self::CheckpointRowMetaStepPrimaryKeyLookup => {
                "checkpoint_row_meta_step_primary_key_lookup"
            }
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct PublicationTruthExportBlockerTraceDiagnostic {
    publication_truth_export_blocker_trace_observed: bool,
    publication_truth_export_blocker_trace_reason_class:
        PublicationTruthExportBlockerTraceReasonClass,
    publication_truth_export_blocker_trace_explanation: String,
    config_path: String,
    runtime_db_path: Option<String>,
    runtime_db_opened_read_only: bool,
    publication_state_loaded: bool,
    publication_state_runtime_mode: Option<String>,
    publication_state_reason: Option<String>,
    publication_state_complete: Option<bool>,
    publication_state_fresh_under_export_gate: Option<bool>,
    trace_checkpoint_family_blocker_from_publication_state: Option<String>,
    trace_row_meta_attempted: bool,
    trace_row_meta_completed: bool,
    trace_budget_exhausted: bool,
    trace_budget_exhausted_stage: Option<PublicationTruthExportBlockerTraceStage>,
    trace_total_elapsed_ms: u64,
    trace_config_load_elapsed_ms: u64,
    trace_runtime_db_open_elapsed_ms: u64,
    trace_publication_state_load_elapsed_ms: u64,
    trace_checkpoint_row_meta_total_elapsed_ms: u64,
    trace_checkpoint_row_meta_query_path: Option<String>,
    trace_checkpoint_row_meta_schema_lookup_elapsed_ms: Option<u64>,
    trace_checkpoint_row_meta_row_count_elapsed_ms: Option<u64>,
    trace_checkpoint_row_meta_prepare_elapsed_ms: Option<u64>,
    trace_checkpoint_row_meta_step_elapsed_ms: Option<u64>,
    trace_persisted_rebuild_checkpoint_exists: Option<bool>,
    trace_rebuild_phase: Option<String>,
    trace_persisted_rebuild_checkpoint_updated_at: Option<DateTime<Utc>>,
}

impl PublicationTruthExportBlockerTraceDiagnostic {
    fn unproven(config_path: &Path, explanation: String) -> Self {
        Self {
            publication_truth_export_blocker_trace_observed: false,
            publication_truth_export_blocker_trace_reason_class:
                PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceUnprovenDueToMissingEvidence,
            publication_truth_export_blocker_trace_explanation: explanation,
            config_path: config_path.display().to_string(),
            runtime_db_path: None,
            runtime_db_opened_read_only: false,
            publication_state_loaded: false,
            publication_state_runtime_mode: None,
            publication_state_reason: None,
            publication_state_complete: None,
            publication_state_fresh_under_export_gate: None,
            trace_checkpoint_family_blocker_from_publication_state: None,
            trace_row_meta_attempted: false,
            trace_row_meta_completed: false,
            trace_budget_exhausted: false,
            trace_budget_exhausted_stage: None,
            trace_total_elapsed_ms: 0,
            trace_config_load_elapsed_ms: 0,
            trace_runtime_db_open_elapsed_ms: 0,
            trace_publication_state_load_elapsed_ms: 0,
            trace_checkpoint_row_meta_total_elapsed_ms: 0,
            trace_checkpoint_row_meta_query_path: None,
            trace_checkpoint_row_meta_schema_lookup_elapsed_ms: None,
            trace_checkpoint_row_meta_row_count_elapsed_ms: None,
            trace_checkpoint_row_meta_prepare_elapsed_ms: None,
            trace_checkpoint_row_meta_step_elapsed_ms: None,
            trace_persisted_rebuild_checkpoint_exists: None,
            trace_rebuild_phase: None,
            trace_persisted_rebuild_checkpoint_updated_at: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LoadedPublicationState {
    runtime_mode: String,
    reason: String,
    publication_truth_complete: bool,
    fresh_under_export_gate: bool,
    checkpoint_family_blocker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointRowMetaSuccess {
    checkpoint_exists: bool,
    rebuild_phase: Option<String>,
    updated_at: Option<DateTime<Utc>>,
    query_path: String,
    total_elapsed_ms: u64,
    schema_lookup_elapsed_ms: Option<u64>,
    row_count_elapsed_ms: Option<u64>,
    prepare_elapsed_ms: Option<u64>,
    step_elapsed_ms: Option<u64>,
}

#[derive(Debug)]
enum WorkerMessage {
    Entered(PublicationTruthExportBlockerTraceStage),
    OpenedReadOnly {
        elapsed_ms: u64,
    },
    PublicationStateLoaded {
        state: LoadedPublicationState,
        elapsed_ms: u64,
    },
    Finished(Result<Option<CheckpointRowMetaSuccess>, String>),
}

fn parse_args() -> Result<Option<Command>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Command>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --config"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}\n{USAGE}"),
        }
    }

    Ok(Some(Command::Explain(ExplainConfig {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        json,
    })))
}

fn run_command(command: Command) -> Result<String> {
    match command {
        Command::Explain(config) => {
            let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
                &config.config_path,
                DEFAULT_TRACE_BUDGET_MS,
                Utc::now(),
            );
            if config.json {
                serde_json::to_string_pretty(&diagnostic).context(
                    "failed to serialize publication truth export blocker trace diagnostic",
                )
            } else {
                Ok(render_publication_truth_export_blocker_trace_human(
                    &diagnostic,
                ))
            }
        }
    }
}

fn explain_publication_truth_export_blocker_trace_read_only(
    config_path: &Path,
    budget_ms: u64,
    now: DateTime<Utc>,
) -> PublicationTruthExportBlockerTraceDiagnostic {
    let started_at = Instant::now();
    let mut diagnostic = PublicationTruthExportBlockerTraceDiagnostic::unproven(
        config_path,
        "publication truth export blocker trace could not yet be proven from config-relative runtime-db evidence"
            .to_string(),
    );
    diagnostic.trace_budget_exhausted_stage = Some(PublicationTruthExportBlockerTraceStage::LoadConfig);

    let config_load_started = Instant::now();
    let loaded_config = match load_from_path(config_path) {
        Ok(config) => config,
        Err(error) => {
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace is unproven because config {} could not be loaded: {error:#}",
                config_path.display()
            );
            diagnostic.trace_config_load_elapsed_ms = elapsed_ms(config_load_started);
            diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
            return diagnostic;
        }
    };
    diagnostic.trace_config_load_elapsed_ms = elapsed_ms(config_load_started);

    if started_at.elapsed() >= StdDuration::from_millis(budget_ms) {
        diagnostic.trace_budget_exhausted = true;
        diagnostic.publication_truth_export_blocker_trace_explanation = format!(
            "publication truth export blocker trace is unproven because the bounded trace budget exhausted during {}",
            PublicationTruthExportBlockerTraceStage::LoadConfig.as_str()
        );
        diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    let runtime_db_path = resolve_db_path(config_path, None, &loaded_config.sqlite.path);
    diagnostic.runtime_db_path = Some(runtime_db_path.display().to_string());
    let publication_gate = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    )
    .publication_freshness_gate();

    let deadline = started_at + StdDuration::from_millis(budget_ms);
    let (tx, rx) = mpsc::sync_channel(16);
    let runtime_db_path_for_worker = runtime_db_path.clone();
    thread::spawn(move || {
        let send_finished = |result: Result<Option<CheckpointRowMetaSuccess>>| {
            let _ = tx.send(WorkerMessage::Finished(
                result.map_err(|error| format!("{error:#}")),
            ));
        };

        let _ = tx.send(WorkerMessage::Entered(
            PublicationTruthExportBlockerTraceStage::OpenRuntimeDbReadOnly,
        ));
        #[cfg(test)]
        if let Some(delay_ms) = take_test_force_open_delay_ms() {
            thread::sleep(StdDuration::from_millis(delay_ms));
        }
        let open_started = Instant::now();
        let conn = match Connection::open_with_flags(
            &runtime_db_path_for_worker,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| {
            format!(
                "failed opening runtime sqlite db read-only {}",
                runtime_db_path_for_worker.display()
            )
        }) {
            Ok(conn) => conn,
            Err(error) => {
                send_finished(Err(error));
                return;
            }
        };
        if tx
            .send(WorkerMessage::OpenedReadOnly {
                elapsed_ms: elapsed_ms(open_started),
            })
            .is_err()
        {
            return;
        }

        let publication_state = match load_publication_state_for_trace(&conn, publication_gate, now, &tx)
        {
            Ok(Some(state)) => state,
            Ok(None) => {
                send_finished(Ok(None));
                return;
            }
            Err(error) => {
                send_finished(Err(error));
                return;
            }
        };

        if publication_state.checkpoint_family_blocker.is_none()
            || (publication_state.publication_truth_complete
                && publication_state.fresh_under_export_gate)
        {
            send_finished(Ok(None));
            return;
        }

        send_finished(load_checkpoint_row_meta_for_trace(&conn, &tx));
    });

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            diagnostic.trace_budget_exhausted = true;
            diagnostic.publication_truth_export_blocker_trace_explanation = build_budget_exhausted_explanation(&diagnostic);
            diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
            return diagnostic;
        }

        match rx.recv_timeout(remaining) {
            Ok(WorkerMessage::Entered(stage)) => {
                diagnostic.trace_budget_exhausted_stage = Some(stage);
                if matches!(
                    stage,
                    PublicationTruthExportBlockerTraceStage::CheckpointRowMetaSchemaLookup
                        | PublicationTruthExportBlockerTraceStage::CheckpointRowMetaRowCount
                        | PublicationTruthExportBlockerTraceStage::CheckpointRowMetaPreparePrimaryKeyLookup
                        | PublicationTruthExportBlockerTraceStage::CheckpointRowMetaStepPrimaryKeyLookup
                ) {
                    diagnostic.trace_row_meta_attempted = true;
                }
            }
            Ok(WorkerMessage::OpenedReadOnly { elapsed_ms }) => {
                diagnostic.runtime_db_opened_read_only = true;
                diagnostic.trace_runtime_db_open_elapsed_ms = elapsed_ms;
            }
            Ok(WorkerMessage::PublicationStateLoaded { state, elapsed_ms }) => {
                diagnostic.publication_state_loaded = true;
                diagnostic.publication_state_runtime_mode = Some(state.runtime_mode.clone());
                diagnostic.publication_state_reason = Some(state.reason.clone());
                diagnostic.publication_state_complete = Some(state.publication_truth_complete);
                diagnostic.publication_state_fresh_under_export_gate =
                    Some(state.fresh_under_export_gate);
                diagnostic.trace_checkpoint_family_blocker_from_publication_state =
                    state.checkpoint_family_blocker.clone();
                diagnostic.trace_publication_state_load_elapsed_ms = elapsed_ms;
                apply_publication_state_classification(&mut diagnostic);
            }
            Ok(WorkerMessage::Finished(result)) => {
                diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
                return match result {
                    Ok(row_meta) => {
                        if !diagnostic.publication_state_loaded {
                            diagnostic.publication_truth_export_blocker_trace_explanation =
                                format!(
                                    "publication truth export blocker trace is unproven because runtime db {} does not contain a persisted discovery publication state row",
                                    runtime_db_path.display()
                                );
                            return diagnostic;
                        }
                        if let Some(row_meta) = row_meta {
                            diagnostic.trace_row_meta_attempted = true;
                            diagnostic.trace_row_meta_completed = true;
                            diagnostic.trace_checkpoint_row_meta_total_elapsed_ms =
                                row_meta.total_elapsed_ms;
                            diagnostic.trace_checkpoint_row_meta_query_path =
                                Some(row_meta.query_path.clone());
                            diagnostic.trace_checkpoint_row_meta_schema_lookup_elapsed_ms =
                                row_meta.schema_lookup_elapsed_ms;
                            diagnostic.trace_checkpoint_row_meta_row_count_elapsed_ms =
                                row_meta.row_count_elapsed_ms;
                            diagnostic.trace_checkpoint_row_meta_prepare_elapsed_ms =
                                row_meta.prepare_elapsed_ms;
                            diagnostic.trace_checkpoint_row_meta_step_elapsed_ms =
                                row_meta.step_elapsed_ms;
                            diagnostic.trace_persisted_rebuild_checkpoint_exists =
                                Some(row_meta.checkpoint_exists);
                            diagnostic.trace_rebuild_phase = row_meta.rebuild_phase.clone();
                            diagnostic.trace_persisted_rebuild_checkpoint_updated_at =
                                row_meta.updated_at;
                        }
                        diagnostic.trace_budget_exhausted_stage =
                            Some(PublicationTruthExportBlockerTraceStage::Complete);
                        finalize_success_explanation(&mut diagnostic);
                        diagnostic
                    }
                    Err(error) => {
                        if diagnostic.publication_state_loaded {
                            diagnostic.publication_truth_export_blocker_trace_explanation =
                                format!(
                                    "publication-state proof completed for runtime db {} but the bounded checkpoint row-meta trace failed before completion: {error}",
                                    runtime_db_path.display()
                                );
                        } else {
                            diagnostic.publication_truth_export_blocker_trace_explanation =
                                format!(
                                    "publication truth export blocker trace is unproven because runtime db {} could not be read through the bounded publication-state and checkpoint-row-meta path: {error}",
                                    runtime_db_path.display()
                                );
                        }
                        diagnostic
                    }
                };
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                diagnostic.trace_budget_exhausted = true;
                diagnostic.publication_truth_export_blocker_trace_explanation =
                    build_budget_exhausted_explanation(&diagnostic);
                diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
                return diagnostic;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                    "publication truth export blocker trace is unproven because the bounded trace worker disconnected before returning a result for runtime db {}",
                    runtime_db_path.display()
                );
                diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
                return diagnostic;
            }
        }
    }
}

fn load_publication_state_for_trace(
    conn: &Connection,
    publication_gate: copybot_storage::DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
    tx: &mpsc::SyncSender<WorkerMessage>,
) -> Result<Option<LoadedPublicationState>> {
    tx.send(WorkerMessage::Entered(
        PublicationTruthExportBlockerTraceStage::LoadPublicationState,
    ))
    .map_err(|_| anyhow!("trace worker stage channel closed before publication-state load"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_publication_state_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let started = Instant::now();
    let raw = conn
        .query_row(PUBLICATION_STATE_SQL, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
            ))
        })
        .optional()
        .context("failed reading discovery publication state")?;
    let elapsed_ms = elapsed_ms(started);

    let Some((
        runtime_mode_raw,
        reason,
        last_published_at_raw,
        last_published_window_start_raw,
        published_scoring_source,
        published_wallet_ids_raw,
        publication_policy_fingerprint,
        updated_at_raw,
    )) = raw
    else {
        return Ok(None);
    };

    let publication_state = DiscoveryPublicationStateRow {
        runtime_mode: DiscoveryRuntimeMode::parse(&runtime_mode_raw)?,
        reason: reason.clone(),
        last_published_at: parse_optional_rfc3339_utc(
            last_published_at_raw.as_deref(),
            "discovery_strategy_state.publication_last_published_at",
        )?,
        last_published_window_start: parse_optional_rfc3339_utc(
            last_published_window_start_raw.as_deref(),
            "discovery_strategy_state.publication_last_published_window_start",
        )?,
        published_scoring_source,
        published_wallet_ids: parse_optional_wallet_ids_json(
            published_wallet_ids_raw.as_deref(),
            "discovery_strategy_state.publication_wallet_ids_json",
        )?,
        publication_policy_fingerprint,
        updated_at: parse_rfc3339_utc(&updated_at_raw, "discovery_strategy_state.updated_at")?,
    };
    let loaded = LoadedPublicationState {
        runtime_mode: publication_state.runtime_mode.as_str().to_string(),
        reason,
        publication_truth_complete: publication_state.has_complete_publication_truth(),
        fresh_under_export_gate: publication_state.is_fresh_under_gate(publication_gate, now),
        checkpoint_family_blocker: export_gate_reason_publishable_checkpoint_blocker(
            &publication_state.reason,
        )
        .map(str::to_string),
    };
    tx.send(WorkerMessage::PublicationStateLoaded {
        state: loaded.clone(),
        elapsed_ms,
    })
    .map_err(|_| anyhow!("trace worker stage channel closed after publication-state load"))?;
    Ok(Some(loaded))
}

fn load_checkpoint_row_meta_for_trace(
    conn: &Connection,
    tx: &mpsc::SyncSender<WorkerMessage>,
) -> Result<Option<CheckpointRowMetaSuccess>> {
    tx.send(WorkerMessage::Entered(
        PublicationTruthExportBlockerTraceStage::CheckpointRowMetaSchemaLookup,
    ))
    .map_err(|_| anyhow!("trace worker stage channel closed before schema lookup"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_schema_lookup_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let row_meta_started = Instant::now();
    let schema_lookup_started = Instant::now();
    let table_exists = conn
        .query_row(CHECKPOINT_TABLE_EXISTS_SQL, [], |row| row.get::<_, i64>(0))
        .context("failed checking discovery_persisted_rebuild_state table existence")?
        != 0;
    let schema_lookup_elapsed_ms = elapsed_ms(schema_lookup_started);
    if !table_exists {
        return Ok(Some(CheckpointRowMetaSuccess {
            checkpoint_exists: false,
            rebuild_phase: None,
            updated_at: None,
            query_path: "schema_lookup_only_table_missing".to_string(),
            total_elapsed_ms: elapsed_ms(row_meta_started),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            row_count_elapsed_ms: None,
            prepare_elapsed_ms: None,
            step_elapsed_ms: None,
        }));
    }

    tx.send(WorkerMessage::Entered(
        PublicationTruthExportBlockerTraceStage::CheckpointRowMetaRowCount,
    ))
    .map_err(|_| anyhow!("trace worker stage channel closed before row count"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_row_count_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let row_count_started = Instant::now();
    let row_count = conn
        .query_row(CHECKPOINT_ROW_COUNT_SQL, [], |row| row.get::<_, i64>(0))
        .context("failed counting persisted rebuild row id=1")?
        .max(0);
    let row_count_elapsed_ms = elapsed_ms(row_count_started);
    if row_count == 0 {
        return Ok(Some(CheckpointRowMetaSuccess {
            checkpoint_exists: false,
            rebuild_phase: None,
            updated_at: None,
            query_path: "schema_lookup_then_row_count".to_string(),
            total_elapsed_ms: elapsed_ms(row_meta_started),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            row_count_elapsed_ms: Some(row_count_elapsed_ms),
            prepare_elapsed_ms: None,
            step_elapsed_ms: None,
        }));
    }

    tx.send(WorkerMessage::Entered(
        PublicationTruthExportBlockerTraceStage::CheckpointRowMetaPreparePrimaryKeyLookup,
    ))
    .map_err(|_| anyhow!("trace worker stage channel closed before prepare"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_prepare_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let prepare_started = Instant::now();
    let mut stmt = conn
        .prepare(CHECKPOINT_ROW_META_SQL)
        .context("failed preparing persisted rebuild row-meta query")?;
    let prepare_elapsed_ms = elapsed_ms(prepare_started);

    tx.send(WorkerMessage::Entered(
        PublicationTruthExportBlockerTraceStage::CheckpointRowMetaStepPrimaryKeyLookup,
    ))
    .map_err(|_| anyhow!("trace worker stage channel closed before step"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_step_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let step_started = Instant::now();
    let raw = stmt
        .query_row([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .optional()
        .context("failed stepping persisted rebuild row-meta query")?;
    let step_elapsed_ms = elapsed_ms(step_started);
    let Some((phase, updated_at_raw)) = raw else {
        return Ok(Some(CheckpointRowMetaSuccess {
            checkpoint_exists: false,
            rebuild_phase: None,
            updated_at: None,
            query_path: "schema_lookup_then_row_count_then_primary_key_lookup".to_string(),
            total_elapsed_ms: elapsed_ms(row_meta_started),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            row_count_elapsed_ms: Some(row_count_elapsed_ms),
            prepare_elapsed_ms: Some(prepare_elapsed_ms),
            step_elapsed_ms: Some(step_elapsed_ms),
        }));
    };

    Ok(Some(CheckpointRowMetaSuccess {
        checkpoint_exists: true,
        rebuild_phase: Some(phase),
        updated_at: Some(parse_rfc3339_utc(
            &updated_at_raw,
            "discovery_persisted_rebuild_state.updated_at",
        )?),
        query_path: "schema_lookup_then_row_count_then_primary_key_lookup".to_string(),
        total_elapsed_ms: elapsed_ms(row_meta_started),
        schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
        row_count_elapsed_ms: Some(row_count_elapsed_ms),
        prepare_elapsed_ms: Some(prepare_elapsed_ms),
        step_elapsed_ms: Some(step_elapsed_ms),
    }))
}

fn apply_publication_state_classification(
    diagnostic: &mut PublicationTruthExportBlockerTraceDiagnostic,
) {
    let runtime_mode = diagnostic
        .publication_state_runtime_mode
        .as_deref()
        .unwrap_or("unknown");
    let reason = diagnostic
        .publication_state_reason
        .as_deref()
        .unwrap_or("unknown");
    let complete = diagnostic.publication_state_complete.unwrap_or(false);
    let fresh = diagnostic
        .publication_state_fresh_under_export_gate
        .unwrap_or(false);

    if complete && fresh {
        diagnostic.publication_truth_export_blocker_trace_observed = true;
        diagnostic.publication_truth_export_blocker_trace_reason_class =
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceGreen;
        diagnostic.publication_truth_export_blocker_trace_explanation = format!(
            "persisted discovery publication state is green under the export gate: runtime_mode={runtime_mode}, reason={reason}, publication_truth_complete=true, fresh_under_export_gate=true"
        );
        return;
    }

    if let Some(blocker) = diagnostic
        .trace_checkpoint_family_blocker_from_publication_state
        .clone()
    {
        diagnostic.publication_truth_export_blocker_trace_observed = true;
        diagnostic.publication_truth_export_blocker_trace_reason_class =
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily;
        diagnostic.publication_truth_export_blocker_trace_explanation = format!(
            "persisted discovery publication state is non-green and already encodes checkpoint-family blocker {blocker}: runtime_mode={runtime_mode}, reason={reason}, publication_truth_complete={complete}, fresh_under_export_gate={fresh}"
        );
        return;
    }

    diagnostic.publication_truth_export_blocker_trace_observed = true;
    diagnostic.publication_truth_export_blocker_trace_reason_class =
        PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithoutCheckpointFamily;
    diagnostic.publication_truth_export_blocker_trace_explanation = format!(
        "persisted discovery publication state is non-green without a checkpoint-family export gate reason: runtime_mode={runtime_mode}, reason={reason}, publication_truth_complete={complete}, fresh_under_export_gate={fresh}"
    );
}

fn finalize_success_explanation(diagnostic: &mut PublicationTruthExportBlockerTraceDiagnostic) {
    if diagnostic.trace_row_meta_completed {
        let query_path = diagnostic
            .trace_checkpoint_row_meta_query_path
            .as_deref()
            .unwrap_or("unknown");
        diagnostic.publication_truth_export_blocker_trace_explanation = format!(
            "{}; bounded runtime-db checkpoint row-meta tracing completed through {} without touching state_json bytes or recent_raw",
            diagnostic.publication_truth_export_blocker_trace_explanation,
            query_path
        );
    }
}

fn build_budget_exhausted_explanation(
    diagnostic: &PublicationTruthExportBlockerTraceDiagnostic,
) -> String {
    let stage = diagnostic
        .trace_budget_exhausted_stage
        .map(PublicationTruthExportBlockerTraceStage::as_str)
        .unwrap_or("unknown");
    if diagnostic.publication_state_loaded {
        return format!(
            "{}; bounded trace budget exhausted at stage={stage}",
            diagnostic.publication_truth_export_blocker_trace_explanation
        );
    }
    format!(
        "publication truth export blocker trace is unproven because the bounded trace budget exhausted before publication-state proof completed (stage={stage})"
    )
}

fn export_gate_reason_publishable_checkpoint_blocker(reason: &str) -> Option<&str> {
    reason
        .strip_prefix(PUBLICATION_TRUTH_WITHHELD_PREFIX)
        .filter(|value| !value.is_empty())
}

fn parse_rfc3339_utc(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid RFC3339 timestamp in {field}: {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

fn parse_optional_rfc3339_utc(raw: Option<&str>, field: &str) -> Result<Option<DateTime<Utc>>> {
    raw.map(|value| parse_rfc3339_utc(value, field)).transpose()
}

fn parse_optional_wallet_ids_json(raw: Option<&str>, field: &str) -> Result<Option<Vec<String>>> {
    raw.map(|value| {
        serde_json::from_str::<Vec<String>>(value)
            .with_context(|| format!("invalid wallet id json in {field}: {value}"))
    })
    .transpose()
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn render_publication_truth_export_blocker_trace_human(
    diagnostic: &PublicationTruthExportBlockerTraceDiagnostic,
) -> String {
    [
        "event=discovery_publication_truth_export_blocker_trace".to_string(),
        format!(
            "publication_truth_export_blocker_trace_observed={}",
            diagnostic.publication_truth_export_blocker_trace_observed
        ),
        format!(
            "publication_truth_export_blocker_trace_reason_class={}",
            serde_json::to_string(&diagnostic.publication_truth_export_blocker_trace_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "publication_truth_export_blocker_trace_explanation={}",
            diagnostic.publication_truth_export_blocker_trace_explanation
        ),
        format!("config_path={}", diagnostic.config_path),
        format!(
            "runtime_db_path={}",
            diagnostic.runtime_db_path.as_deref().unwrap_or("null")
        ),
        format!(
            "runtime_db_opened_read_only={}",
            diagnostic.runtime_db_opened_read_only
        ),
        format!("publication_state_loaded={}", diagnostic.publication_state_loaded),
        format!(
            "publication_state_runtime_mode={}",
            diagnostic
                .publication_state_runtime_mode
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publication_state_reason={}",
            diagnostic.publication_state_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "publication_state_complete={}",
            format_optional_bool(diagnostic.publication_state_complete)
        ),
        format!(
            "publication_state_fresh_under_export_gate={}",
            format_optional_bool(diagnostic.publication_state_fresh_under_export_gate)
        ),
        format!(
            "trace_checkpoint_family_blocker_from_publication_state={}",
            diagnostic
                .trace_checkpoint_family_blocker_from_publication_state
                .as_deref()
                .unwrap_or("null")
        ),
        format!("trace_row_meta_attempted={}", diagnostic.trace_row_meta_attempted),
        format!("trace_row_meta_completed={}", diagnostic.trace_row_meta_completed),
        format!("trace_budget_exhausted={}", diagnostic.trace_budget_exhausted),
        format!(
            "trace_budget_exhausted_stage={}",
            diagnostic
                .trace_budget_exhausted_stage
                .map(PublicationTruthExportBlockerTraceStage::as_str)
                .unwrap_or("null")
        ),
        format!("trace_total_elapsed_ms={}", diagnostic.trace_total_elapsed_ms),
        format!(
            "trace_config_load_elapsed_ms={}",
            diagnostic.trace_config_load_elapsed_ms
        ),
        format!(
            "trace_runtime_db_open_elapsed_ms={}",
            diagnostic.trace_runtime_db_open_elapsed_ms
        ),
        format!(
            "trace_publication_state_load_elapsed_ms={}",
            diagnostic.trace_publication_state_load_elapsed_ms
        ),
        format!(
            "trace_checkpoint_row_meta_total_elapsed_ms={}",
            diagnostic.trace_checkpoint_row_meta_total_elapsed_ms
        ),
        format!(
            "trace_checkpoint_row_meta_query_path={}",
            diagnostic
                .trace_checkpoint_row_meta_query_path
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "trace_checkpoint_row_meta_schema_lookup_elapsed_ms={}",
            format_optional_u64(diagnostic.trace_checkpoint_row_meta_schema_lookup_elapsed_ms)
        ),
        format!(
            "trace_checkpoint_row_meta_row_count_elapsed_ms={}",
            format_optional_u64(diagnostic.trace_checkpoint_row_meta_row_count_elapsed_ms)
        ),
        format!(
            "trace_checkpoint_row_meta_prepare_elapsed_ms={}",
            format_optional_u64(diagnostic.trace_checkpoint_row_meta_prepare_elapsed_ms)
        ),
        format!(
            "trace_checkpoint_row_meta_step_elapsed_ms={}",
            format_optional_u64(diagnostic.trace_checkpoint_row_meta_step_elapsed_ms)
        ),
        format!(
            "trace_persisted_rebuild_checkpoint_exists={}",
            format_optional_bool(diagnostic.trace_persisted_rebuild_checkpoint_exists)
        ),
        format!(
            "trace_rebuild_phase={}",
            diagnostic.trace_rebuild_phase.as_deref().unwrap_or("null")
        ),
        format!(
            "trace_persisted_rebuild_checkpoint_updated_at={}",
            format_optional_ts(diagnostic.trace_persisted_rebuild_checkpoint_updated_at.as_ref())
        ),
    ]
    .join("\n")
}

fn format_optional_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
static TEST_SERIAL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
#[cfg(test)]
static TEST_FORCE_OPEN_DELAY_MS: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_PUBLICATION_STATE_DELAY_MS: std::sync::Mutex<Option<u64>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_SCHEMA_LOOKUP_DELAY_MS: std::sync::Mutex<Option<u64>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_ROW_COUNT_DELAY_MS: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_PREPARE_DELAY_MS: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_STEP_DELAY_MS: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);

#[cfg(test)]
fn arm_test_force_row_count_delay_ms(delay_ms: u64) {
    *TEST_FORCE_ROW_COUNT_DELAY_MS
        .lock()
        .expect("row-count delay mutex poisoned") = Some(delay_ms);
}

#[cfg(test)]
fn take_test_force_open_delay_ms() -> Option<u64> {
    TEST_FORCE_OPEN_DELAY_MS
        .lock()
        .expect("open delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_publication_state_delay_ms() -> Option<u64> {
    TEST_FORCE_PUBLICATION_STATE_DELAY_MS
        .lock()
        .expect("publication-state delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_schema_lookup_delay_ms() -> Option<u64> {
    TEST_FORCE_SCHEMA_LOOKUP_DELAY_MS
        .lock()
        .expect("schema-lookup delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_row_count_delay_ms() -> Option<u64> {
    TEST_FORCE_ROW_COUNT_DELAY_MS
        .lock()
        .expect("row-count delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_prepare_delay_ms() -> Option<u64> {
    TEST_FORCE_PREPARE_DELAY_MS
        .lock()
        .expect("prepare delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_step_delay_ms() -> Option<u64> {
    TEST_FORCE_STEP_DELAY_MS
        .lock()
        .expect("step delay mutex poisoned")
        .take()
}

#[cfg(test)]
mod tests {
    use super::{
        arm_test_force_row_count_delay_ms, explain_publication_truth_export_blocker_trace_read_only,
        parse_args_from, run_command, Command, ExplainConfig,
        PublicationTruthExportBlockerTraceReasonClass, PublicationTruthExportBlockerTraceStage,
        CHECKPOINT_ROW_COUNT_SQL, CHECKPOINT_ROW_META_SQL, CHECKPOINT_TABLE_EXISTS_SQL,
        PUBLICATION_STATE_SQL,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
    };
    use serde_json::Value;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::{tempdir, TempDir};

    struct Fixture {
        _temp: TempDir,
        config_path: PathBuf,
        store: SqliteStore,
    }

    #[test]
    fn parse_args_from_accepts_config_and_json() {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "/tmp/live.server.toml".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::Explain(parsed) = parsed;
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/live.server.toml"));
        assert!(parsed.json);
    }

    #[test]
    fn green_fixture_returns_green_with_row_meta_skipped() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("publication-truth-trace-green")?;
        let now = parse_ts("2026-04-16T12:00:00Z")?;
        seed_publication_state(
            &fixture.store,
            now,
            DiscoveryRuntimeMode::Healthy,
            "healthy_recent_exact_publish",
            Some(now - Duration::minutes(5)),
            Some(now - Duration::days(7)),
            Some(vec!["wallet-a".to_string()]),
        )?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            1_000,
            now,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceGreen
        );
        assert!(diagnostic.publication_truth_export_blocker_trace_observed);
        assert!(diagnostic.publication_state_loaded);
        assert!(!diagnostic.trace_row_meta_attempted);
        assert!(!diagnostic.trace_row_meta_completed);
        assert_eq!(diagnostic.trace_checkpoint_row_meta_total_elapsed_ms, 0);
        assert_eq!(diagnostic.trace_rebuild_phase, None);
        Ok(())
    }

    #[test]
    fn non_green_checkpoint_family_fixture_returns_checkpoint_family_and_fills_publication_fields(
    ) -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("publication-truth-trace-checkpoint-family")?;
        let now = parse_ts("2026-04-16T12:00:00Z")?;
        seed_publication_state(
            &fixture.store,
            now,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
            Some(now - Duration::hours(10)),
            Some(now - Duration::days(9)),
            Some(vec!["wallet-a".to_string()]),
        )?;
        seed_replay_checkpoint_row(&fixture.store, "{invalid json not used}")?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["publication_truth_export_blocker_trace_reason_class"],
            "publication_truth_export_blocker_trace_non_green_with_checkpoint_family"
        );
        assert_eq!(parsed["publication_state_loaded"], true);
        assert_eq!(parsed["publication_state_runtime_mode"], "fail_closed");
        assert_eq!(
            parsed["publication_state_reason"],
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(
            parsed["trace_checkpoint_family_blocker_from_publication_state"],
            "replay_sol_leg_incomplete"
        );
        Ok(())
    }

    #[test]
    fn non_green_checkpoint_family_fixture_with_row_meta_success_fills_phase_updated_at_and_timings(
    ) -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("publication-truth-trace-row-meta-success")?;
        let now = parse_ts("2026-04-16T12:00:00Z")?;
        seed_publication_state(
            &fixture.store,
            now,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
            Some(now - Duration::hours(10)),
            Some(now - Duration::days(9)),
            Some(vec!["wallet-a".to_string()]),
        )?;
        seed_replay_checkpoint_row(&fixture.store, "{\"unused\":\"state\"}")?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            1_000,
            now,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily
        );
        assert!(diagnostic.trace_row_meta_attempted);
        assert!(diagnostic.trace_row_meta_completed);
        assert_eq!(
            diagnostic.trace_checkpoint_row_meta_query_path.as_deref(),
            Some("schema_lookup_then_row_count_then_primary_key_lookup")
        );
        assert_eq!(diagnostic.trace_persisted_rebuild_checkpoint_exists, Some(true));
        assert_eq!(diagnostic.trace_rebuild_phase.as_deref(), Some("replay"));
        assert!(diagnostic.trace_persisted_rebuild_checkpoint_updated_at.is_some());
        assert!(diagnostic
            .trace_checkpoint_row_meta_schema_lookup_elapsed_ms
            .is_some());
        assert!(diagnostic
            .trace_checkpoint_row_meta_row_count_elapsed_ms
            .is_some());
        assert!(diagnostic.trace_checkpoint_row_meta_prepare_elapsed_ms.is_some());
        assert!(diagnostic.trace_checkpoint_row_meta_step_elapsed_ms.is_some());
        Ok(())
    }

    #[test]
    fn non_green_without_checkpoint_family_returns_non_green_without_row_meta() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("publication-truth-trace-no-checkpoint-family")?;
        let now = parse_ts("2026-04-16T12:00:00Z")?;
        seed_publication_state(
            &fixture.store,
            now,
            DiscoveryRuntimeMode::FailClosed,
            "raw_window_incomplete_no_recent_published_universe",
            Some(now - Duration::hours(10)),
            Some(now - Duration::days(9)),
            Some(vec!["wallet-a".to_string()]),
        )?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            1_000,
            now,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithoutCheckpointFamily
        );
        assert!(!diagnostic.trace_row_meta_attempted);
        assert_eq!(
            diagnostic.trace_checkpoint_family_blocker_from_publication_state,
            None
        );
        Ok(())
    }

    #[test]
    fn missing_config_returns_unproven() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_config = temp.path().join("missing.server.toml");

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: missing_config.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["publication_truth_export_blocker_trace_reason_class"],
            "publication_truth_export_blocker_trace_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["publication_truth_export_blocker_trace_observed"], false);
        assert_eq!(parsed["runtime_db_path"], Value::Null);
        Ok(())
    }

    #[test]
    fn unreadable_runtime_db_returns_unproven() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_db = temp.path().join("missing-runtime.db");
        let config_path = write_config(&temp, "publication-truth-trace-unreadable", &missing_db)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["publication_truth_export_blocker_trace_reason_class"],
            "publication_truth_export_blocker_trace_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["runtime_db_opened_read_only"], false);
        assert_eq!(parsed["trace_budget_exhausted"], false);
        assert_eq!(parsed["trace_budget_exhausted_stage"], "open_runtime_db_read_only");
        Ok(())
    }

    #[test]
    fn bounded_budget_exhaustion_returns_partial_proof_and_exact_stage() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("publication-truth-trace-budget")?;
        let now = parse_ts("2026-04-16T12:00:00Z")?;
        seed_publication_state(
            &fixture.store,
            now,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
            Some(now - Duration::hours(10)),
            Some(now - Duration::days(9)),
            Some(vec!["wallet-a".to_string()]),
        )?;
        seed_replay_checkpoint_row(&fixture.store, "{\"unused\":\"state\"}")?;
        arm_test_force_row_count_delay_ms(1_500);

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            100,
            now,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily
        );
        assert!(diagnostic.publication_truth_export_blocker_trace_observed);
        assert!(diagnostic.trace_budget_exhausted);
        assert_eq!(
            diagnostic.trace_budget_exhausted_stage,
            Some(PublicationTruthExportBlockerTraceStage::CheckpointRowMetaRowCount)
        );
        assert!(diagnostic.trace_row_meta_attempted);
        assert!(!diagnostic.trace_row_meta_completed);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("publication-truth-trace-json-fields")?;
        let now = parse_ts("2026-04-16T12:00:00Z")?;
        seed_publication_state(
            &fixture.store,
            now,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
            Some(now - Duration::hours(10)),
            Some(now - Duration::days(9)),
            Some(vec!["wallet-a".to_string()]),
        )?;
        seed_replay_checkpoint_row(&fixture.store, "{\"unused\":\"state\"}")?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        for key in [
            "publication_truth_export_blocker_trace_observed",
            "publication_truth_export_blocker_trace_reason_class",
            "publication_truth_export_blocker_trace_explanation",
            "config_path",
            "runtime_db_path",
            "runtime_db_opened_read_only",
            "publication_state_loaded",
            "publication_state_runtime_mode",
            "publication_state_reason",
            "publication_state_complete",
            "publication_state_fresh_under_export_gate",
            "trace_checkpoint_family_blocker_from_publication_state",
            "trace_row_meta_attempted",
            "trace_row_meta_completed",
            "trace_budget_exhausted",
            "trace_budget_exhausted_stage",
            "trace_total_elapsed_ms",
            "trace_config_load_elapsed_ms",
            "trace_runtime_db_open_elapsed_ms",
            "trace_publication_state_load_elapsed_ms",
            "trace_checkpoint_row_meta_total_elapsed_ms",
            "trace_checkpoint_row_meta_query_path",
            "trace_checkpoint_row_meta_schema_lookup_elapsed_ms",
            "trace_checkpoint_row_meta_row_count_elapsed_ms",
            "trace_checkpoint_row_meta_prepare_elapsed_ms",
            "trace_checkpoint_row_meta_step_elapsed_ms",
            "trace_persisted_rebuild_checkpoint_exists",
            "trace_rebuild_phase",
            "trace_persisted_rebuild_checkpoint_updated_at",
        ] {
            assert!(
                parsed.get(key).is_some(),
                "expected required publication trace field {key}"
            );
        }
        Ok(())
    }

    #[test]
    fn trace_sql_never_references_state_json_length_or_recent_raw() {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        for sql in [
            PUBLICATION_STATE_SQL,
            CHECKPOINT_TABLE_EXISTS_SQL,
            CHECKPOINT_ROW_COUNT_SQL,
            CHECKPOINT_ROW_META_SQL,
        ] {
            assert!(!sql.contains("state_json"));
            assert!(!sql.contains("length("));
            assert!(!sql.contains("recent_raw"));
        }
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(format!("{name}.db"));
        let config_path = write_config(&temp, name, &db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        Ok(Fixture {
            _temp: temp,
            config_path,
            store,
        })
    }

    fn write_config(temp: &TempDir, name: &str, db_path: &Path) -> Result<PathBuf> {
        let config_path = temp.path().join(format!("{name}.toml"));
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[runtime_restore_ops]\nartifact_retention = 2\nartifact_cadence_minutes = 10\njournal_snapshot_dir = \"missing/recent_raw\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(config_path)
    }

    fn seed_publication_state(
        store: &SqliteStore,
        now: DateTime<Utc>,
        runtime_mode: DiscoveryRuntimeMode,
        reason: &str,
        last_published_at: Option<DateTime<Utc>>,
        last_published_window_start: Option<DateTime<Utc>>,
        published_wallet_ids: Option<Vec<String>>,
    ) -> Result<()> {
        let _ = now;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode,
            reason: reason.to_string(),
            last_published_at,
            last_published_window_start,
            published_scoring_source: Some("trace_test".to_string()),
            published_wallet_ids,
        })?;
        Ok(())
    }

    fn seed_replay_checkpoint_row(
        store: &SqliteStore,
        state_json: &str,
    ) -> Result<DiscoveryPersistedRebuildStateRow> {
        let row = DiscoveryPersistedRebuildStateRow {
            phase: DiscoveryPersistedRebuildPhase::Replay,
            window_start: parse_ts("2026-04-16T08:00:00Z")?,
            horizon_end: parse_ts("2026-04-21T08:00:00Z")?,
            metrics_window_start: parse_ts("2026-04-16T08:00:00Z")?,
            phase_cursor: Some(DiscoveryRuntimeCursor {
                ts_utc: parse_ts("2026-04-16T09:30:00Z")?,
                slot: 4242,
                signature: "checkpoint-cursor-signature".to_string(),
            }),
            prepass_rows_processed: 11,
            prepass_pages_processed: 3,
            replay_rows_processed: 22,
            replay_pages_processed: 4,
            chunks_completed: 5,
            state_json: state_json.to_string(),
            started_at: parse_ts("2026-04-16T08:05:00Z")?,
            updated_at: parse_ts("2026-04-16T09:35:00Z")?,
        };
        store.upsert_discovery_persisted_rebuild_state(&row)?;
        Ok(row)
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(raw)
            .with_context(|| format!("invalid test timestamp {raw}"))
            .map(|value| value.with_timezone(&Utc))
    }
}
