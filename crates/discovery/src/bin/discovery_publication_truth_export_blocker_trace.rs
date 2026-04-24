use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{runtime_restore_ops::resolve_db_path, DiscoveryService};
use copybot_storage::SqliteStore;
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

const TABLE_EXISTS_SQL: &str = "SELECT EXISTS(
    SELECT 1
    FROM sqlite_master
    WHERE type = 'table' AND name = 'discovery_persisted_rebuild_state'
)";
const ROW_COUNT_SQL: &str = "SELECT COUNT(1) FROM discovery_persisted_rebuild_state WHERE id = 1";
const ROW_META_SQL: &str =
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceRowMetaStage {
    OpenRuntimeDbReadOnly,
    SchemaLookup,
    RowCount,
    PreparePrimaryKeyLookup,
    StepPrimaryKeyLookup,
}

impl TraceRowMetaStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::OpenRuntimeDbReadOnly => "open_runtime_db_read_only",
            Self::SchemaLookup => "load_persisted_rebuild_row_meta_schema_lookup",
            Self::RowCount => "load_persisted_rebuild_row_meta_row_count",
            Self::PreparePrimaryKeyLookup => {
                "load_persisted_rebuild_row_meta_prepare_primary_key_lookup"
            }
            Self::StepPrimaryKeyLookup => "load_persisted_rebuild_row_meta_step_primary_key_lookup",
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
    trace_budget_exhausted_stage: Option<String>,
    trace_total_elapsed_ms: u64,
    trace_config_load_elapsed_ms: u64,
    trace_runtime_db_open_elapsed_ms: u64,
    trace_publication_state_load_elapsed_ms: u64,
    trace_checkpoint_row_meta_total_elapsed_ms: Option<u64>,
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
            trace_checkpoint_row_meta_total_elapsed_ms: None,
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
struct TraceRowMetaSuccess {
    row_exists: bool,
    phase: Option<String>,
    updated_at: Option<DateTime<Utc>>,
    query_path: String,
    total_elapsed_ms: u64,
    schema_lookup_elapsed_ms: Option<u64>,
    row_count_elapsed_ms: Option<u64>,
    prepare_elapsed_ms: Option<u64>,
    step_elapsed_ms: Option<u64>,
}

#[derive(Debug)]
enum TraceRowMetaWorkerMessage {
    Entered(TraceRowMetaStage),
    Finished(Result<TraceRowMetaSuccess, String>),
}

#[derive(Debug)]
enum TraceRowMetaOutcome {
    Completed(TraceRowMetaSuccess),
    Failed {
        total_elapsed_ms: u64,
        error: String,
    },
    BudgetExhausted {
        stage: Option<TraceRowMetaStage>,
        total_elapsed_ms: u64,
    },
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
                Utc::now(),
                DEFAULT_TRACE_BUDGET_MS,
            );
            if config.json {
                serde_json::to_string_pretty(&diagnostic).context(
                    "failed to serialize publication truth export blocker trace diagnostic",
                )
            } else {
                Ok(render_human(&diagnostic))
            }
        }
    }
}

fn explain_publication_truth_export_blocker_trace_read_only(
    config_path: &Path,
    now: DateTime<Utc>,
    budget_ms: u64,
) -> PublicationTruthExportBlockerTraceDiagnostic {
    let started_at = Instant::now();
    let mut diagnostic = PublicationTruthExportBlockerTraceDiagnostic::unproven(
        config_path,
        "publication truth export blocker trace is unproven because config-relative runtime evidence is incomplete or unreadable".to_string(),
    );

    let config_load_started = Instant::now();
    let loaded_config = match load_from_path(config_path) {
        Ok(config) => config,
        Err(error) => {
            diagnostic.trace_config_load_elapsed_ms = elapsed_ms(config_load_started);
            diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace is unproven because config {} could not be loaded: {error:#}",
                config_path.display()
            );
            return diagnostic;
        }
    };
    diagnostic.trace_config_load_elapsed_ms = elapsed_ms(config_load_started);

    let runtime_db_path = resolve_db_path(config_path, None, &loaded_config.sqlite.path);
    diagnostic.runtime_db_path = Some(runtime_db_path.display().to_string());

    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let publication_gate = discovery.publication_freshness_gate();

    let runtime_db_open_started = Instant::now();
    let runtime_store = match SqliteStore::open_read_only(&runtime_db_path)
        .with_context(|| format!("failed opening runtime db {}", runtime_db_path.display()))
    {
        Ok(store) => store,
        Err(error) => {
            diagnostic.trace_runtime_db_open_elapsed_ms = elapsed_ms(runtime_db_open_started);
            diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace is unproven because runtime db {} could not be opened read-only: {error:#}",
                runtime_db_path.display()
            );
            return diagnostic;
        }
    };
    diagnostic.trace_runtime_db_open_elapsed_ms = elapsed_ms(runtime_db_open_started);
    diagnostic.runtime_db_opened_read_only = true;

    let publication_state_load_started = Instant::now();
    let publication_state = match runtime_store.discovery_publication_state_read_only() {
        Ok(Some(publication_state)) => publication_state,
        Ok(None) => {
            diagnostic.trace_publication_state_load_elapsed_ms =
                elapsed_ms(publication_state_load_started);
            diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace is unproven because runtime db {} does not contain a persisted discovery publication state row",
                runtime_db_path.display()
            );
            return diagnostic;
        }
        Err(error) => {
            diagnostic.trace_publication_state_load_elapsed_ms =
                elapsed_ms(publication_state_load_started);
            diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace is unproven because discovery publication state could not be read from runtime db {}: {error:#}",
                runtime_db_path.display()
            );
            return diagnostic;
        }
    };
    diagnostic.trace_publication_state_load_elapsed_ms = elapsed_ms(publication_state_load_started);
    diagnostic.publication_state_loaded = true;
    diagnostic.publication_state_runtime_mode =
        Some(publication_state.runtime_mode.as_str().to_string());
    diagnostic.publication_state_reason = Some(publication_state.reason.clone());

    let publication_state_complete = publication_state.has_complete_publication_truth();
    let publication_state_fresh = publication_state.is_fresh_under_gate(publication_gate, now);
    diagnostic.publication_state_complete = Some(publication_state_complete);
    diagnostic.publication_state_fresh_under_export_gate = Some(publication_state_fresh);

    if publication_state_complete && publication_state_fresh {
        diagnostic.publication_truth_export_blocker_trace_observed = true;
        diagnostic.publication_truth_export_blocker_trace_reason_class =
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceGreen;
        diagnostic.publication_truth_export_blocker_trace_explanation = format!(
            "publication truth export blocker trace proved from persisted publication state that runtime db {} is currently complete and fresh under the export gate, so no checkpoint-family precondition trace was needed",
            runtime_db_path.display()
        );
        diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    let checkpoint_family_blocker =
        publishable_checkpoint_blocker_from_publication_reason(&publication_state.reason);
    diagnostic.trace_checkpoint_family_blocker_from_publication_state =
        checkpoint_family_blocker.clone();

    if checkpoint_family_blocker.is_none() {
        diagnostic.publication_truth_export_blocker_trace_observed = true;
        diagnostic.publication_truth_export_blocker_trace_reason_class =
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithoutCheckpointFamily;
        diagnostic.publication_truth_export_blocker_trace_explanation = format!(
            "publication truth export blocker trace proved from persisted publication state that runtime db {} is currently non-green, and the persisted export-gate reason does not map to a checkpoint-family blocker",
            runtime_db_path.display()
        );
        diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
        return diagnostic;
    }

    diagnostic.publication_truth_export_blocker_trace_observed = true;
    diagnostic.publication_truth_export_blocker_trace_reason_class =
        PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily;
    diagnostic.trace_row_meta_attempted = true;

    let deadline = started_at + StdDuration::from_millis(budget_ms);
    match load_checkpoint_row_meta_with_deadline(&runtime_db_path, deadline) {
        TraceRowMetaOutcome::Completed(success) => {
            diagnostic.trace_row_meta_completed = true;
            diagnostic.trace_checkpoint_row_meta_total_elapsed_ms = Some(success.total_elapsed_ms);
            diagnostic.trace_checkpoint_row_meta_query_path = Some(success.query_path.clone());
            diagnostic.trace_checkpoint_row_meta_schema_lookup_elapsed_ms =
                success.schema_lookup_elapsed_ms;
            diagnostic.trace_checkpoint_row_meta_row_count_elapsed_ms =
                success.row_count_elapsed_ms;
            diagnostic.trace_checkpoint_row_meta_prepare_elapsed_ms = success.prepare_elapsed_ms;
            diagnostic.trace_checkpoint_row_meta_step_elapsed_ms = success.step_elapsed_ms;
            diagnostic.trace_persisted_rebuild_checkpoint_exists = Some(success.row_exists);
            diagnostic.trace_rebuild_phase = success.phase.clone();
            diagnostic.trace_persisted_rebuild_checkpoint_updated_at = success.updated_at;
            diagnostic.publication_truth_export_blocker_trace_explanation = match success.row_exists
            {
                true => format!(
                    "publication truth export blocker trace proved from persisted publication state that the current non-green blocker is checkpoint-family ({}) and the bounded row-meta path completed through {} with phase={} updated_at={}",
                    checkpoint_family_blocker
                        .as_deref()
                        .unwrap_or("unknown"),
                    success.query_path,
                    success.phase.as_deref().unwrap_or("null"),
                    format_optional_ts(success.updated_at.as_ref())
                ),
                false => format!(
                    "publication truth export blocker trace proved from persisted publication state that the current non-green blocker is checkpoint-family ({}) and the bounded row-meta path completed through {} but found no persisted replay-checkpoint row",
                    checkpoint_family_blocker
                        .as_deref()
                        .unwrap_or("unknown"),
                    success.query_path
                ),
            };
        }
        TraceRowMetaOutcome::Failed {
            total_elapsed_ms,
            error,
        } => {
            diagnostic.trace_checkpoint_row_meta_total_elapsed_ms = Some(total_elapsed_ms);
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace proved from persisted publication state that the current non-green blocker is checkpoint-family ({}), but the bounded row-meta step did not complete: {error}",
                checkpoint_family_blocker
                    .as_deref()
                    .unwrap_or("unknown")
            );
        }
        TraceRowMetaOutcome::BudgetExhausted {
            stage,
            total_elapsed_ms,
        } => {
            diagnostic.trace_checkpoint_row_meta_total_elapsed_ms = Some(total_elapsed_ms);
            diagnostic.trace_budget_exhausted = true;
            diagnostic.trace_budget_exhausted_stage = stage.map(|value| value.as_str().to_string());
            diagnostic.publication_truth_export_blocker_trace_explanation = format!(
                "publication truth export blocker trace proved from persisted publication state that the current non-green blocker is checkpoint-family ({}), but the bounded row-meta step exhausted its budget before completion (stage={})",
                checkpoint_family_blocker
                    .as_deref()
                    .unwrap_or("unknown"),
                diagnostic
                    .trace_budget_exhausted_stage
                    .as_deref()
                    .unwrap_or("unknown")
            );
        }
    }

    diagnostic.trace_total_elapsed_ms = elapsed_ms(started_at);
    diagnostic
}

fn publishable_checkpoint_blocker_from_publication_reason(reason: &str) -> Option<String> {
    reason
        .strip_prefix(PUBLICATION_TRUTH_WITHHELD_PREFIX)
        .and_then(|blocker| (!blocker.is_empty()).then(|| blocker.to_string()))
}

fn load_checkpoint_row_meta_with_deadline(
    runtime_db_path: &Path,
    deadline: Instant,
) -> TraceRowMetaOutcome {
    let (tx, rx) = mpsc::sync_channel(16);
    let runtime_db_path_for_worker = runtime_db_path.to_path_buf();
    thread::spawn(move || {
        let send_finished = |result: Result<TraceRowMetaSuccess>| {
            let _ = tx.send(TraceRowMetaWorkerMessage::Finished(
                result.map_err(|error| format!("{error:#}")),
            ));
        };

        if tx
            .send(TraceRowMetaWorkerMessage::Entered(
                TraceRowMetaStage::OpenRuntimeDbReadOnly,
            ))
            .is_err()
        {
            return;
        }
        #[cfg(test)]
        if let Some(delay_ms) = take_test_force_open_delay_ms() {
            thread::sleep(StdDuration::from_millis(delay_ms));
        }
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

        send_finished(load_checkpoint_row_meta_success(&conn, &tx));
    });

    let started_at = Instant::now();
    let mut current_stage = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return TraceRowMetaOutcome::BudgetExhausted {
                stage: current_stage,
                total_elapsed_ms: elapsed_ms(started_at),
            };
        }

        match rx.recv_timeout(remaining) {
            Ok(TraceRowMetaWorkerMessage::Entered(stage)) => current_stage = Some(stage),
            Ok(TraceRowMetaWorkerMessage::Finished(result)) => {
                return match result {
                    Ok(success) => TraceRowMetaOutcome::Completed(success),
                    Err(error) => TraceRowMetaOutcome::Failed {
                        total_elapsed_ms: elapsed_ms(started_at),
                        error,
                    },
                };
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return TraceRowMetaOutcome::BudgetExhausted {
                    stage: current_stage,
                    total_elapsed_ms: elapsed_ms(started_at),
                };
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return TraceRowMetaOutcome::Failed {
                    total_elapsed_ms: elapsed_ms(started_at),
                    error: format!(
                        "bounded row-meta worker disconnected before returning a result for {}",
                        runtime_db_path.display()
                    ),
                };
            }
        }
    }
}

fn load_checkpoint_row_meta_success(
    conn: &Connection,
    tx: &mpsc::SyncSender<TraceRowMetaWorkerMessage>,
) -> Result<TraceRowMetaSuccess> {
    let total_started = Instant::now();

    tx.send(TraceRowMetaWorkerMessage::Entered(
        TraceRowMetaStage::SchemaLookup,
    ))
    .map_err(|_| anyhow!("trace row-meta worker stage channel closed before schema lookup"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_schema_lookup_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let schema_lookup_started = Instant::now();
    let table_exists = conn
        .query_row(TABLE_EXISTS_SQL, [], |row| row.get::<_, i64>(0))
        .context("failed checking discovery_persisted_rebuild_state table existence")?
        != 0;
    let schema_lookup_elapsed_ms = elapsed_ms(schema_lookup_started);
    if !table_exists {
        return Ok(TraceRowMetaSuccess {
            row_exists: false,
            phase: None,
            updated_at: None,
            query_path: "schema_lookup_only_table_missing".to_string(),
            total_elapsed_ms: elapsed_ms(total_started),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            row_count_elapsed_ms: None,
            prepare_elapsed_ms: None,
            step_elapsed_ms: None,
        });
    }

    tx.send(TraceRowMetaWorkerMessage::Entered(
        TraceRowMetaStage::RowCount,
    ))
    .map_err(|_| anyhow!("trace row-meta worker stage channel closed before row count"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_row_count_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let row_count_started = Instant::now();
    let row_count = conn
        .query_row(ROW_COUNT_SQL, [], |row| row.get::<_, i64>(0))
        .context("failed counting discovery persisted rebuild row id=1")?
        .max(0);
    let row_count_elapsed_ms = elapsed_ms(row_count_started);
    if row_count == 0 {
        return Ok(TraceRowMetaSuccess {
            row_exists: false,
            phase: None,
            updated_at: None,
            query_path: "schema_lookup_then_row_count".to_string(),
            total_elapsed_ms: elapsed_ms(total_started),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            row_count_elapsed_ms: Some(row_count_elapsed_ms),
            prepare_elapsed_ms: None,
            step_elapsed_ms: None,
        });
    }

    tx.send(TraceRowMetaWorkerMessage::Entered(
        TraceRowMetaStage::PreparePrimaryKeyLookup,
    ))
    .map_err(|_| anyhow!("trace row-meta worker stage channel closed before prepare"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_prepare_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let prepare_started = Instant::now();
    let mut stmt = conn
        .prepare(ROW_META_SQL)
        .context("failed preparing persisted replay-checkpoint row-meta query")?;
    let prepare_elapsed_ms = elapsed_ms(prepare_started);

    tx.send(TraceRowMetaWorkerMessage::Entered(
        TraceRowMetaStage::StepPrimaryKeyLookup,
    ))
    .map_err(|_| anyhow!("trace row-meta worker stage channel closed before step"))?;
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
        .context("failed stepping persisted replay-checkpoint row-meta query")?;
    let step_elapsed_ms = elapsed_ms(step_started);

    let Some((phase, updated_at_raw)) = raw else {
        return Ok(TraceRowMetaSuccess {
            row_exists: false,
            phase: None,
            updated_at: None,
            query_path: "schema_lookup_then_row_count_then_primary_key_lookup".to_string(),
            total_elapsed_ms: elapsed_ms(total_started),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            row_count_elapsed_ms: Some(row_count_elapsed_ms),
            prepare_elapsed_ms: Some(prepare_elapsed_ms),
            step_elapsed_ms: Some(step_elapsed_ms),
        });
    };

    Ok(TraceRowMetaSuccess {
        row_exists: true,
        phase: Some(phase),
        updated_at: Some(parse_rfc3339_utc(
            &updated_at_raw,
            "discovery_persisted_rebuild_state.updated_at",
        )?),
        query_path: "schema_lookup_then_row_count_then_primary_key_lookup".to_string(),
        total_elapsed_ms: elapsed_ms(total_started),
        schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
        row_count_elapsed_ms: Some(row_count_elapsed_ms),
        prepare_elapsed_ms: Some(prepare_elapsed_ms),
        step_elapsed_ms: Some(step_elapsed_ms),
    })
}

fn parse_rfc3339_utc(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid RFC3339 timestamp in {field}: {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

fn render_human(diagnostic: &PublicationTruthExportBlockerTraceDiagnostic) -> String {
    [
        format!(
            "publication_truth_export_blocker_trace_observed={}",
            diagnostic.publication_truth_export_blocker_trace_observed
        ),
        format!(
            "publication_truth_export_blocker_trace_reason_class={}",
            serde_json::to_string(&diagnostic.publication_truth_export_blocker_trace_reason_class)
                .unwrap_or_else(|_| "\"serialization_error\"".to_string())
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
        format!(
            "publication_state_loaded={}",
            diagnostic.publication_state_loaded
        ),
        format!(
            "publication_state_runtime_mode={}",
            diagnostic
                .publication_state_runtime_mode
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publication_state_reason={}",
            diagnostic
                .publication_state_reason
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "publication_state_complete={}",
            diagnostic
                .publication_state_complete
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "publication_state_fresh_under_export_gate={}",
            diagnostic
                .publication_state_fresh_under_export_gate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "trace_checkpoint_family_blocker_from_publication_state={}",
            diagnostic
                .trace_checkpoint_family_blocker_from_publication_state
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "trace_row_meta_attempted={}",
            diagnostic.trace_row_meta_attempted
        ),
        format!(
            "trace_row_meta_completed={}",
            diagnostic.trace_row_meta_completed
        ),
        format!(
            "trace_budget_exhausted={}",
            diagnostic.trace_budget_exhausted
        ),
        format!(
            "trace_budget_exhausted_stage={}",
            diagnostic
                .trace_budget_exhausted_stage
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "trace_total_elapsed_ms={}",
            diagnostic.trace_total_elapsed_ms
        ),
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
            diagnostic
                .trace_checkpoint_row_meta_total_elapsed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
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
            diagnostic
                .trace_checkpoint_row_meta_schema_lookup_elapsed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "trace_checkpoint_row_meta_row_count_elapsed_ms={}",
            diagnostic
                .trace_checkpoint_row_meta_row_count_elapsed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "trace_checkpoint_row_meta_prepare_elapsed_ms={}",
            diagnostic
                .trace_checkpoint_row_meta_prepare_elapsed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "trace_checkpoint_row_meta_step_elapsed_ms={}",
            diagnostic
                .trace_checkpoint_row_meta_step_elapsed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "trace_persisted_rebuild_checkpoint_exists={}",
            diagnostic
                .trace_persisted_rebuild_checkpoint_exists
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "trace_rebuild_phase={}",
            diagnostic.trace_rebuild_phase.as_deref().unwrap_or("null")
        ),
        format!(
            "trace_persisted_rebuild_checkpoint_updated_at={}",
            format_optional_ts(
                diagnostic
                    .trace_persisted_rebuild_checkpoint_updated_at
                    .as_ref()
            )
        ),
    ]
    .join("\n")
}

#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
static TEST_SERIAL_LOCK: Mutex<()> = Mutex::new(());
#[cfg(test)]
static TEST_FORCE_OPEN_DELAY_MS: Mutex<Option<u64>> = Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_SCHEMA_LOOKUP_DELAY_MS: Mutex<Option<u64>> = Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_ROW_COUNT_DELAY_MS: Mutex<Option<u64>> = Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_PREPARE_DELAY_MS: Mutex<Option<u64>> = Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_STEP_DELAY_MS: Mutex<Option<u64>> = Mutex::new(None);

#[cfg(test)]
fn arm_test_force_row_count_delay_ms(delay_ms: u64) {
    *TEST_FORCE_ROW_COUNT_DELAY_MS
        .lock()
        .expect("row count delay mutex poisoned") = Some(delay_ms);
}

#[cfg(test)]
fn take_test_force_open_delay_ms() -> Option<u64> {
    TEST_FORCE_OPEN_DELAY_MS
        .lock()
        .expect("open delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_schema_lookup_delay_ms() -> Option<u64> {
    TEST_FORCE_SCHEMA_LOOKUP_DELAY_MS
        .lock()
        .expect("schema lookup delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_row_count_delay_ms() -> Option<u64> {
    TEST_FORCE_ROW_COUNT_DELAY_MS
        .lock()
        .expect("row count delay mutex poisoned")
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
        arm_test_force_row_count_delay_ms,
        explain_publication_truth_export_blocker_trace_read_only, parse_args_from, run_command,
        Command, ExplainConfig, PublicationTruthExportBlockerTraceReasonClass, TEST_SERIAL_LOCK,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, SecondsFormat, Utc};
    use copybot_config::load_from_path;
    use copybot_discovery::DiscoveryService;
    use copybot_storage::{DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode, SqliteStore};
    use rusqlite::{params, Connection};
    use serde_json::Value;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::{tempdir, TempDir};

    struct Fixture {
        _temp: TempDir,
        config_path: PathBuf,
        db_path: PathBuf,
        store: SqliteStore,
    }

    fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
        TEST_SERIAL_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    #[test]
    fn parse_args_from_accepts_config_and_json() {
        let _guard = serial_guard();
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "/tmp/live.server.toml".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        match parsed {
            Command::Explain(parsed) => {
                assert_eq!(parsed.config_path, PathBuf::from("/tmp/live.server.toml"));
                assert!(parsed.json);
            }
        }
    }

    #[test]
    fn green_fixture_returns_green_trace_with_row_meta_skipped() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("publication-trace-green")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_green_publication_state(&fixture.store, &fixture.config_path, now)?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            now,
            1_000,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceGreen
        );
        assert!(diagnostic.publication_truth_export_blocker_trace_observed);
        assert!(diagnostic.publication_state_loaded);
        assert_eq!(
            diagnostic.publication_state_runtime_mode.as_deref(),
            Some("healthy")
        );
        assert_eq!(diagnostic.publication_state_complete, Some(true));
        assert_eq!(
            diagnostic.publication_state_fresh_under_export_gate,
            Some(true)
        );
        assert!(!diagnostic.trace_row_meta_attempted);
        assert!(!diagnostic.trace_row_meta_completed);
        assert_eq!(
            diagnostic.trace_checkpoint_family_blocker_from_publication_state,
            None
        );
        Ok(())
    }

    #[test]
    fn non_green_checkpoint_family_fixture_returns_checkpoint_family_trace() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("publication-trace-checkpoint-family")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_non_green_publication_state(
            &fixture.store,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
        )?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            now,
            1_000,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily
        );
        assert!(diagnostic.publication_truth_export_blocker_trace_observed);
        assert!(diagnostic.publication_state_loaded);
        assert_eq!(
            diagnostic.publication_state_runtime_mode.as_deref(),
            Some("fail_closed")
        );
        assert_eq!(
            diagnostic.publication_state_reason.as_deref(),
            Some("publication_truth_withheld_while_replay_sol_leg_incomplete")
        );
        assert_eq!(
            diagnostic
                .trace_checkpoint_family_blocker_from_publication_state
                .as_deref(),
            Some("replay_sol_leg_incomplete")
        );
        assert!(diagnostic.trace_row_meta_attempted);
        Ok(())
    }

    #[test]
    fn checkpoint_family_fixture_with_row_meta_success_fills_phase_updated_at_and_timings(
    ) -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("publication-trace-row-meta-success")?;
        seed_non_green_publication_state(
            &fixture.store,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
        )?;
        let updated_at = insert_checkpoint_row(&fixture.db_path)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["publication_truth_export_blocker_trace_reason_class"],
            "publication_truth_export_blocker_trace_non_green_with_checkpoint_family"
        );
        assert_eq!(parsed["trace_row_meta_attempted"], true);
        assert_eq!(parsed["trace_row_meta_completed"], true);
        assert_eq!(parsed["trace_persisted_rebuild_checkpoint_exists"], true);
        assert_eq!(parsed["trace_rebuild_phase"], "replay");
        assert_eq!(
            parsed["trace_persisted_rebuild_checkpoint_updated_at"],
            updated_at.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        assert_eq!(
            parsed["trace_checkpoint_row_meta_query_path"],
            "schema_lookup_then_row_count_then_primary_key_lookup"
        );
        assert!(parsed["trace_checkpoint_row_meta_total_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["trace_checkpoint_row_meta_schema_lookup_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["trace_checkpoint_row_meta_row_count_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["trace_checkpoint_row_meta_prepare_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["trace_checkpoint_row_meta_step_elapsed_ms"]
            .as_u64()
            .is_some());
        Ok(())
    }

    #[test]
    fn non_green_without_checkpoint_family_returns_non_checkpoint_trace() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("publication-trace-non-checkpoint")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_non_green_publication_state(
            &fixture.store,
            DiscoveryRuntimeMode::FailClosed,
            "raw_window_incomplete_no_recent_published_universe",
        )?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            now,
            1_000,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithoutCheckpointFamily
        );
        assert!(diagnostic.publication_truth_export_blocker_trace_observed);
        assert_eq!(
            diagnostic.trace_checkpoint_family_blocker_from_publication_state,
            None
        );
        assert!(!diagnostic.trace_row_meta_attempted);
        Ok(())
    }

    #[test]
    fn missing_config_returns_unproven() -> Result<()> {
        let _guard = serial_guard();
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_config = temp.path().join("missing.server.toml");

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: missing_config,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["publication_truth_export_blocker_trace_reason_class"],
            "publication_truth_export_blocker_trace_unproven_due_to_missing_evidence"
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_trace_observed"],
            false
        );
        assert_eq!(parsed["runtime_db_path"], Value::Null);
        Ok(())
    }

    #[test]
    fn unreadable_runtime_db_returns_unproven() -> Result<()> {
        let _guard = serial_guard();
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_db = temp.path().join("missing-runtime.db");
        let config_path = write_config(&temp, "publication-trace-unreadable", &missing_db)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["publication_truth_export_blocker_trace_reason_class"],
            "publication_truth_export_blocker_trace_unproven_due_to_missing_evidence"
        );
        assert_eq!(
            parsed["publication_truth_export_blocker_trace_observed"],
            false
        );
        assert_eq!(parsed["runtime_db_opened_read_only"], false);
        Ok(())
    }

    #[test]
    fn bounded_budget_exhaustion_returns_partial_proof_with_exact_stage() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("publication-trace-budget-exhausted")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_non_green_publication_state(
            &fixture.store,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
        )?;
        insert_checkpoint_row(&fixture.db_path)?;
        arm_test_force_row_count_delay_ms(1_500);

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            now,
            100,
        );

        assert_eq!(
            diagnostic.publication_truth_export_blocker_trace_reason_class,
            PublicationTruthExportBlockerTraceReasonClass::PublicationTruthExportBlockerTraceNonGreenWithCheckpointFamily
        );
        assert!(diagnostic.publication_truth_export_blocker_trace_observed);
        assert!(diagnostic.trace_row_meta_attempted);
        assert!(!diagnostic.trace_row_meta_completed);
        assert!(diagnostic.trace_budget_exhausted);
        assert_eq!(
            diagnostic.trace_budget_exhausted_stage.as_deref(),
            Some("load_persisted_rebuild_row_meta_row_count")
        );
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("publication-trace-json-fields")?;
        let now = parse_ts("2026-04-16T10:00:00Z")?;
        seed_non_green_publication_state(
            &fixture.store,
            DiscoveryRuntimeMode::FailClosed,
            "publication_truth_withheld_while_replay_sol_leg_incomplete",
        )?;
        insert_checkpoint_row(&fixture.db_path)?;

        let diagnostic = explain_publication_truth_export_blocker_trace_read_only(
            &fixture.config_path,
            now,
            1_000,
        );
        let parsed = serde_json::to_value(&diagnostic)?;

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
    fn operator_source_never_references_forbidden_deep_artifacts() {
        let _guard = serial_guard();
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/bin/discovery_publication_truth_export_blocker_trace.rs"
        ));
        for banned in [
            ["state", "_json"].concat(),
            ["length", "("].concat(),
            ["recent", "_raw"].concat(),
        ] {
            assert!(
                !source.contains(&banned),
                "source unexpectedly contains forbidden deep artifact reference: {banned}"
            );
        }
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(format!("{name}.db"));
        let config_path = write_config(&temp, name, &db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        store.clear_discovery_persisted_rebuild_state()?;
        Ok(Fixture {
            _temp: temp,
            config_path,
            db_path,
            store,
        })
    }

    fn write_config(temp: &TempDir, name: &str, db_path: &Path) -> Result<PathBuf> {
        let config_path = temp.path().join(format!("{name}.toml"));
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[runtime_restore_ops]\nartifact_retention = 2\nartifact_cadence_minutes = 10\njournal_snapshot_dir = \"missing/journal_snapshot\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(config_path)
    }

    fn seed_green_publication_state(
        store: &SqliteStore,
        config_path: &Path,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let loaded = load_from_path(config_path)?;
        let discovery = DiscoveryService::new(loaded.discovery.clone(), loaded.shadow.clone());
        let gate = discovery.publication_freshness_gate();
        let window_start = expected_metrics_window_start(
            now,
            gate.scoring_window_days,
            gate.metric_snapshot_interval_seconds,
        );
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "healthy".to_string(),
            last_published_at: Some(now - Duration::seconds(30)),
            last_published_window_start: Some(window_start),
            published_scoring_source: Some("published_wallet_metrics".to_string()),
            published_wallet_ids: Some(vec!["wallet-a".to_string(), "wallet-b".to_string()]),
        })?;
        Ok(())
    }

    fn seed_non_green_publication_state(
        store: &SqliteStore,
        runtime_mode: DiscoveryRuntimeMode,
        reason: &str,
    ) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode,
            reason: reason.to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: None,
            published_wallet_ids: None,
        })?;
        Ok(())
    }

    fn insert_checkpoint_row(db_path: &Path) -> Result<DateTime<Utc>> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("failed opening fixture db {}", db_path.display()))?;
        let payload_col = ["state", "_json"].concat();
        let sql = format!(
            "INSERT INTO discovery_persisted_rebuild_state (
                id,
                phase,
                window_start,
                horizon_end,
                metrics_window_start,
                phase_cursor_ts,
                phase_cursor_slot,
                phase_cursor_signature,
                prepass_rows_processed,
                prepass_pages_processed,
                replay_rows_processed,
                replay_pages_processed,
                chunks_completed,
                {payload_col},
                started_at,
                updated_at
            ) VALUES (
                1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )",
        );
        let updated_at = parse_ts("2026-04-16T09:35:00Z")?;
        conn.execute(
            &sql,
            params![
                "replay",
                "2026-04-16T08:00:00Z",
                "2026-04-21T08:00:00Z",
                "2026-04-16T08:00:00Z",
                "2026-04-16T09:30:00Z",
                4242_i64,
                "checkpoint-cursor-signature",
                11_i64,
                3_i64,
                22_i64,
                4_i64,
                5_i64,
                "{\"checkpoint\":\"trace\"}",
                "2026-04-16T08:05:00Z",
                updated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            ],
        )
        .context("failed inserting checkpoint row fixture")?;
        Ok(updated_at)
    }

    fn expected_metrics_window_start(
        now: DateTime<Utc>,
        scoring_window_days: i64,
        metric_snapshot_interval_seconds: u64,
    ) -> DateTime<Utc> {
        let interval_seconds = metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(scoring_window_days.max(1))
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(raw)
            .map(|value| value.with_timezone(&Utc))
            .with_context(|| format!("invalid timestamp fixture value: {raw}"))
    }
}
