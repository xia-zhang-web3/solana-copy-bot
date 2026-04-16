use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::runtime_restore_ops::resolve_db_path;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

const USAGE: &str = "usage:
  discovery_replay_checkpoint_headline --config <path> [--probe-state-json-bytes] [--json]";
const DEFAULT_REPLAY_CHECKPOINT_HEADLINE_BUDGET_MS: u64 = 30_000;

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
    probe_state_json_bytes: bool,
    json: bool,
}

#[derive(Debug, Clone)]
enum Command {
    Explain(ExplainConfig),
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplayCheckpointHeadlineReasonClass {
    ReplayCheckpointHeadlineRowPresent,
    ReplayCheckpointHeadlineRowMissing,
    ReplayCheckpointHeadlineUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplayCheckpointHeadlineStage {
    OpenRuntimeDbReadOnly,
    QueryRowHeadline,
    Complete,
}

impl ReplayCheckpointHeadlineStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::OpenRuntimeDbReadOnly => "open_runtime_db_read_only",
            Self::QueryRowHeadline => "query_row_headline",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplayCheckpointHeadlineStateJsonBytesProbeStage {
    OpenRuntimeDbReadOnly,
    QueryStateJsonBytes,
    Complete,
}

impl ReplayCheckpointHeadlineStateJsonBytesProbeStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::OpenRuntimeDbReadOnly => "open_runtime_db_read_only",
            Self::QueryStateJsonBytes => "query_state_json_bytes",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReplayCheckpointHeadlineDiagnostic {
    replay_checkpoint_headline_observed: bool,
    replay_checkpoint_headline_reason_class: ReplayCheckpointHeadlineReasonClass,
    replay_checkpoint_headline_explanation: String,
    config_path: String,
    runtime_db_path: Option<String>,
    runtime_db_opened_read_only: bool,
    persisted_rebuild_checkpoint_exists: Option<bool>,
    persisted_rebuild_checkpoint_phase: Option<String>,
    persisted_rebuild_checkpoint_updated_at: Option<DateTime<Utc>>,
    persisted_rebuild_checkpoint_state_json_bytes: Option<usize>,
    persisted_rebuild_checkpoint_window_start: Option<DateTime<Utc>>,
    persisted_rebuild_checkpoint_horizon_end: Option<DateTime<Utc>>,
    persisted_rebuild_checkpoint_metrics_window_start: Option<DateTime<Utc>>,
    persisted_rebuild_checkpoint_phase_cursor_ts_utc: Option<DateTime<Utc>>,
    persisted_rebuild_checkpoint_phase_cursor_slot: Option<u64>,
    persisted_rebuild_checkpoint_phase_cursor_signature: Option<String>,
    persisted_rebuild_checkpoint_prepass_rows_processed: Option<usize>,
    persisted_rebuild_checkpoint_prepass_pages_processed: Option<usize>,
    persisted_rebuild_checkpoint_replay_rows_processed: Option<usize>,
    persisted_rebuild_checkpoint_replay_pages_processed: Option<usize>,
    persisted_rebuild_checkpoint_chunks_completed: Option<usize>,
    replay_checkpoint_headline_state_json_bytes_probe_attempted: bool,
    replay_checkpoint_headline_state_json_bytes_probe_completed: bool,
    replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted: bool,
    replay_checkpoint_headline_state_json_bytes_probe_stage:
        Option<ReplayCheckpointHeadlineStateJsonBytesProbeStage>,
    replay_checkpoint_headline_state_json_bytes_probe_explanation: String,
    replay_checkpoint_headline_budget_exhausted: bool,
    replay_checkpoint_headline_stage: Option<ReplayCheckpointHeadlineStage>,
    replay_checkpoint_headline_total_elapsed_ms: u64,
}

impl ReplayCheckpointHeadlineDiagnostic {
    fn unproven(config_path: &Path, explanation: String) -> Self {
        Self {
            replay_checkpoint_headline_observed: false,
            replay_checkpoint_headline_reason_class:
                ReplayCheckpointHeadlineReasonClass::ReplayCheckpointHeadlineUnprovenDueToMissingEvidence,
            replay_checkpoint_headline_explanation: explanation,
            config_path: config_path.display().to_string(),
            runtime_db_path: None,
            runtime_db_opened_read_only: false,
            persisted_rebuild_checkpoint_exists: None,
            persisted_rebuild_checkpoint_phase: None,
            persisted_rebuild_checkpoint_updated_at: None,
            persisted_rebuild_checkpoint_state_json_bytes: None,
            persisted_rebuild_checkpoint_window_start: None,
            persisted_rebuild_checkpoint_horizon_end: None,
            persisted_rebuild_checkpoint_metrics_window_start: None,
            persisted_rebuild_checkpoint_phase_cursor_ts_utc: None,
            persisted_rebuild_checkpoint_phase_cursor_slot: None,
            persisted_rebuild_checkpoint_phase_cursor_signature: None,
            persisted_rebuild_checkpoint_prepass_rows_processed: None,
            persisted_rebuild_checkpoint_prepass_pages_processed: None,
            persisted_rebuild_checkpoint_replay_rows_processed: None,
            persisted_rebuild_checkpoint_replay_pages_processed: None,
            persisted_rebuild_checkpoint_chunks_completed: None,
            replay_checkpoint_headline_state_json_bytes_probe_attempted: false,
            replay_checkpoint_headline_state_json_bytes_probe_completed: false,
            replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted: false,
            replay_checkpoint_headline_state_json_bytes_probe_stage: None,
            replay_checkpoint_headline_state_json_bytes_probe_explanation:
                "state_json byte-length probing was skipped because --probe-state-json-bytes was not requested"
                    .to_string(),
            replay_checkpoint_headline_budget_exhausted: false,
            replay_checkpoint_headline_stage: None,
            replay_checkpoint_headline_total_elapsed_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct ReplayCheckpointHeadlineRow {
    phase: String,
    updated_at: DateTime<Utc>,
    window_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    phase_cursor_ts_utc: Option<DateTime<Utc>>,
    phase_cursor_slot: Option<u64>,
    phase_cursor_signature: Option<String>,
    prepass_rows_processed: usize,
    prepass_pages_processed: usize,
    replay_rows_processed: usize,
    replay_pages_processed: usize,
    chunks_completed: usize,
}

#[derive(Debug)]
enum HeadlineWorkerMessage {
    Entered(ReplayCheckpointHeadlineStage),
    OpenedReadOnly,
    Finished(Result<Option<ReplayCheckpointHeadlineRow>, String>),
}

#[derive(Debug)]
enum StateJsonBytesProbeWorkerMessage {
    Entered(ReplayCheckpointHeadlineStateJsonBytesProbeStage),
    Finished(Result<Option<usize>, String>),
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
    let mut probe_state_json_bytes = false;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --config"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--probe-state-json-bytes" => probe_state_json_bytes = true,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}\n{USAGE}"),
        }
    }

    Ok(Some(Command::Explain(ExplainConfig {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        probe_state_json_bytes,
        json,
    })))
}

fn run_command(command: Command) -> Result<String> {
    match command {
        Command::Explain(config) => {
            let diagnostic = explain_replay_checkpoint_headline_read_only(
                &config.config_path,
                DEFAULT_REPLAY_CHECKPOINT_HEADLINE_BUDGET_MS,
                config.probe_state_json_bytes,
            );
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed to serialize replay checkpoint headline diagnostic")
            } else {
                Ok(render_replay_checkpoint_headline_human(&diagnostic))
            }
        }
    }
}

fn explain_replay_checkpoint_headline_read_only(
    config_path: &Path,
    budget_ms: u64,
    probe_state_json_bytes: bool,
) -> ReplayCheckpointHeadlineDiagnostic {
    let started_at = Instant::now();
    let loaded_config = match load_from_path(config_path) {
        Ok(config) => config,
        Err(error) => {
            let mut diagnostic = ReplayCheckpointHeadlineDiagnostic::unproven(
                config_path,
                format!(
                    "replay checkpoint headline is unproven because config {} could not be loaded: {error:#}",
                    config_path.display()
                ),
            );
            if probe_state_json_bytes {
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                    "state_json byte-length probing was requested but skipped because the config could not be loaded"
                        .to_string();
            }
            diagnostic.replay_checkpoint_headline_total_elapsed_ms =
                started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
            return diagnostic;
        }
    };

    let runtime_db_path = resolve_db_path(config_path, None, &loaded_config.sqlite.path);
    let mut diagnostic = ReplayCheckpointHeadlineDiagnostic::unproven(
        config_path,
        "replay checkpoint headline could not yet be proven from config-relative runtime-db evidence"
            .to_string(),
    );
    diagnostic.runtime_db_path = Some(runtime_db_path.display().to_string());
    if probe_state_json_bytes {
        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
            "state_json byte-length probing was requested and will be attempted only if the core headline path proves the persisted replay checkpoint row is present"
                .to_string();
    }

    let deadline = started_at + StdDuration::from_millis(budget_ms);
    match load_replay_checkpoint_headline_with_deadline(&runtime_db_path, deadline) {
        HeadlineLoadOutcome::BudgetExhausted(stage, opened_read_only) => {
            diagnostic.runtime_db_opened_read_only = opened_read_only;
            diagnostic.replay_checkpoint_headline_budget_exhausted = true;
            diagnostic.replay_checkpoint_headline_stage = Some(stage);
            diagnostic.replay_checkpoint_headline_explanation = format!(
                "replay checkpoint headline is unproven because the bounded runtime-db headline query exhausted its budget before completion (stage={})",
                stage.as_str()
            );
            if probe_state_json_bytes {
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                    "state_json byte-length probing was requested but skipped because the core headline path exhausted its budget before proving the persisted replay checkpoint row"
                        .to_string();
            }
        }
        HeadlineLoadOutcome::Error {
            opened_read_only,
            stage,
            error,
        } => {
            diagnostic.runtime_db_opened_read_only = opened_read_only;
            diagnostic.replay_checkpoint_headline_stage = Some(stage);
            diagnostic.replay_checkpoint_headline_explanation = format!(
                "replay checkpoint headline is unproven because runtime db {} could not be read through the bounded headline path: {error}",
                runtime_db_path.display()
            );
            if probe_state_json_bytes {
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                    "state_json byte-length probing was requested but skipped because the core headline path could not read the persisted replay checkpoint row"
                        .to_string();
            }
        }
        HeadlineLoadOutcome::Missing {
            opened_read_only,
            row_missing,
        } => {
            diagnostic.runtime_db_opened_read_only = opened_read_only;
            diagnostic.replay_checkpoint_headline_observed = true;
            diagnostic.replay_checkpoint_headline_reason_class =
                ReplayCheckpointHeadlineReasonClass::ReplayCheckpointHeadlineRowMissing;
            diagnostic.persisted_rebuild_checkpoint_exists = Some(false);
            diagnostic.replay_checkpoint_headline_stage =
                Some(ReplayCheckpointHeadlineStage::Complete);
            diagnostic.replay_checkpoint_headline_explanation = format!(
                "runtime db {} is readable read-only, but discovery_persisted_rebuild_state(id=1) is missing so no persisted replay checkpoint headline row currently exists",
                runtime_db_path.display()
            );
            if probe_state_json_bytes {
                let _ = row_missing;
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                    "state_json byte-length probing was requested but skipped because the persisted replay checkpoint row is missing"
                        .to_string();
            }
        }
        HeadlineLoadOutcome::Present {
            opened_read_only,
            row,
        } => {
            diagnostic.runtime_db_opened_read_only = opened_read_only;
            diagnostic.replay_checkpoint_headline_observed = true;
            diagnostic.replay_checkpoint_headline_reason_class =
                ReplayCheckpointHeadlineReasonClass::ReplayCheckpointHeadlineRowPresent;
            diagnostic.persisted_rebuild_checkpoint_exists = Some(true);
            diagnostic.persisted_rebuild_checkpoint_phase = Some(row.phase);
            diagnostic.persisted_rebuild_checkpoint_updated_at = Some(row.updated_at);
            diagnostic.persisted_rebuild_checkpoint_window_start = Some(row.window_start);
            diagnostic.persisted_rebuild_checkpoint_horizon_end = Some(row.horizon_end);
            diagnostic.persisted_rebuild_checkpoint_metrics_window_start =
                Some(row.metrics_window_start);
            diagnostic.persisted_rebuild_checkpoint_phase_cursor_ts_utc = row.phase_cursor_ts_utc;
            diagnostic.persisted_rebuild_checkpoint_phase_cursor_slot = row.phase_cursor_slot;
            diagnostic.persisted_rebuild_checkpoint_phase_cursor_signature =
                row.phase_cursor_signature;
            diagnostic.persisted_rebuild_checkpoint_prepass_rows_processed =
                Some(row.prepass_rows_processed);
            diagnostic.persisted_rebuild_checkpoint_prepass_pages_processed =
                Some(row.prepass_pages_processed);
            diagnostic.persisted_rebuild_checkpoint_replay_rows_processed =
                Some(row.replay_rows_processed);
            diagnostic.persisted_rebuild_checkpoint_replay_pages_processed =
                Some(row.replay_pages_processed);
            diagnostic.persisted_rebuild_checkpoint_chunks_completed = Some(row.chunks_completed);
            diagnostic.replay_checkpoint_headline_stage =
                Some(ReplayCheckpointHeadlineStage::Complete);
            diagnostic.replay_checkpoint_headline_explanation = format!(
                "persisted replay checkpoint headline row is present in runtime db {} and was read from bounded read-only headline columns only",
                runtime_db_path.display()
            );

            if probe_state_json_bytes {
                apply_state_json_bytes_probe(&mut diagnostic, &runtime_db_path, deadline);
            }
        }
    }

    diagnostic.replay_checkpoint_headline_total_elapsed_ms =
        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
    diagnostic
}

enum HeadlineLoadOutcome {
    Present {
        opened_read_only: bool,
        row: ReplayCheckpointHeadlineRow,
    },
    Missing {
        opened_read_only: bool,
        row_missing: bool,
    },
    Error {
        opened_read_only: bool,
        stage: ReplayCheckpointHeadlineStage,
        error: String,
    },
    BudgetExhausted(ReplayCheckpointHeadlineStage, bool),
}

fn load_replay_checkpoint_headline_with_deadline(
    runtime_db_path: &Path,
    deadline: Instant,
) -> HeadlineLoadOutcome {
    let (tx, rx) = mpsc::sync_channel(8);
    let runtime_db_path_for_worker = runtime_db_path.to_path_buf();
    thread::spawn(move || {
        let send_finished = |result: Result<Option<ReplayCheckpointHeadlineRow>>| {
            let _ = tx.send(HeadlineWorkerMessage::Finished(
                result.map_err(|error| format!("{error:#}")),
            ));
        };

        let _ = tx.send(HeadlineWorkerMessage::Entered(
            ReplayCheckpointHeadlineStage::OpenRuntimeDbReadOnly,
        ));
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
        if tx.send(HeadlineWorkerMessage::OpenedReadOnly).is_err() {
            return;
        }

        let _ = tx.send(HeadlineWorkerMessage::Entered(
            ReplayCheckpointHeadlineStage::QueryRowHeadline,
        ));
        #[cfg(test)]
        if let Some(delay_ms) = take_test_force_query_delay_ms() {
            thread::sleep(StdDuration::from_millis(delay_ms));
        }
        send_finished(load_replay_checkpoint_headline_row(&conn));
    });

    let mut opened_read_only = false;
    let mut current_stage = ReplayCheckpointHeadlineStage::OpenRuntimeDbReadOnly;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return HeadlineLoadOutcome::BudgetExhausted(current_stage, opened_read_only);
        }

        match rx.recv_timeout(remaining) {
            Ok(HeadlineWorkerMessage::Entered(stage)) => {
                current_stage = stage;
            }
            Ok(HeadlineWorkerMessage::OpenedReadOnly) => {
                opened_read_only = true;
            }
            Ok(HeadlineWorkerMessage::Finished(result)) => {
                return match result {
                    Ok(Some(row)) => HeadlineLoadOutcome::Present {
                        opened_read_only,
                        row,
                    },
                    Ok(None) => HeadlineLoadOutcome::Missing {
                        opened_read_only,
                        row_missing: true,
                    },
                    Err(error) => HeadlineLoadOutcome::Error {
                        opened_read_only,
                        stage: current_stage,
                        error,
                    },
                };
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return HeadlineLoadOutcome::BudgetExhausted(current_stage, opened_read_only);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return HeadlineLoadOutcome::Error {
                    opened_read_only,
                    stage: current_stage,
                    error: format!(
                        "the bounded runtime-db headline worker disconnected before returning a result for {}",
                        runtime_db_path.display()
                    ),
                };
            }
        }
    }
}

fn load_replay_checkpoint_headline_row(
    conn: &Connection,
) -> Result<Option<ReplayCheckpointHeadlineRow>> {
    if !headline_table_exists(conn)? {
        return Ok(None);
    }

    let raw = conn
        .query_row(
            "SELECT
                phase,
                updated_at,
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
                chunks_completed
             FROM discovery_persisted_rebuild_state
             WHERE id = 1",
            [],
            |row| {
                Ok(ReplayCheckpointHeadlineRowRaw {
                    phase_raw: row.get(0)?,
                    updated_at_raw: row.get(1)?,
                    window_start_raw: row.get(2)?,
                    horizon_end_raw: row.get(3)?,
                    metrics_window_start_raw: row.get(4)?,
                    phase_cursor_ts_raw: row.get(5)?,
                    phase_cursor_slot_raw: row.get(6)?,
                    phase_cursor_signature: row.get(7)?,
                    prepass_rows_processed: row.get(8)?,
                    prepass_pages_processed: row.get(9)?,
                    replay_rows_processed: row.get(10)?,
                    replay_pages_processed: row.get(11)?,
                    chunks_completed: row.get(12)?,
                })
            },
        )
        .optional()
        .context("failed querying discovery persisted rebuild headline row")?;

    raw.map(parse_replay_checkpoint_headline_row).transpose()
}

fn parse_replay_checkpoint_headline_row(
    raw: ReplayCheckpointHeadlineRowRaw,
) -> Result<ReplayCheckpointHeadlineRow> {
    let phase_cursor = match (
        raw.phase_cursor_ts_raw,
        raw.phase_cursor_slot_raw,
        raw.phase_cursor_signature,
    ) {
        (None, None, None) => (None, None, None),
        (Some(ts_raw), Some(slot_raw), Some(signature)) => (
            Some(parse_rfc3339_utc(
                &ts_raw,
                "discovery_persisted_rebuild_state.phase_cursor_ts",
            )?),
            Some(slot_raw.max(0) as u64),
            Some(signature),
        ),
        _ => {
            bail!("discovery_persisted_rebuild_state contains partial phase cursor state");
        }
    };

    Ok(ReplayCheckpointHeadlineRow {
        phase: parse_rebuild_phase(&raw.phase_raw)?.to_string(),
        updated_at: parse_rfc3339_utc(
            &raw.updated_at_raw,
            "discovery_persisted_rebuild_state.updated_at",
        )?,
        window_start: parse_rfc3339_utc(
            &raw.window_start_raw,
            "discovery_persisted_rebuild_state.window_start",
        )?,
        horizon_end: parse_rfc3339_utc(
            &raw.horizon_end_raw,
            "discovery_persisted_rebuild_state.horizon_end",
        )?,
        metrics_window_start: parse_rfc3339_utc(
            &raw.metrics_window_start_raw,
            "discovery_persisted_rebuild_state.metrics_window_start",
        )?,
        phase_cursor_ts_utc: phase_cursor.0,
        phase_cursor_slot: phase_cursor.1,
        phase_cursor_signature: phase_cursor.2,
        prepass_rows_processed: raw.prepass_rows_processed.max(0) as usize,
        prepass_pages_processed: raw.prepass_pages_processed.max(0) as usize,
        replay_rows_processed: raw.replay_rows_processed.max(0) as usize,
        replay_pages_processed: raw.replay_pages_processed.max(0) as usize,
        chunks_completed: raw.chunks_completed.max(0) as usize,
    })
}

#[derive(Debug)]
struct ReplayCheckpointHeadlineRowRaw {
    phase_raw: String,
    updated_at_raw: String,
    window_start_raw: String,
    horizon_end_raw: String,
    metrics_window_start_raw: String,
    phase_cursor_ts_raw: Option<String>,
    phase_cursor_slot_raw: Option<i64>,
    phase_cursor_signature: Option<String>,
    prepass_rows_processed: i64,
    prepass_pages_processed: i64,
    replay_rows_processed: i64,
    replay_pages_processed: i64,
    chunks_completed: i64,
}

fn apply_state_json_bytes_probe(
    diagnostic: &mut ReplayCheckpointHeadlineDiagnostic,
    runtime_db_path: &Path,
    deadline: Instant,
) {
    diagnostic.replay_checkpoint_headline_state_json_bytes_probe_attempted = true;

    let (tx, rx) = mpsc::sync_channel(8);
    let runtime_db_path_for_worker = runtime_db_path.to_path_buf();
    thread::spawn(move || {
        let send_finished = |result: Result<Option<usize>>| {
            let _ = tx.send(StateJsonBytesProbeWorkerMessage::Finished(
                result.map_err(|error| format!("{error:#}")),
            ));
        };

        let _ = tx.send(StateJsonBytesProbeWorkerMessage::Entered(
            ReplayCheckpointHeadlineStateJsonBytesProbeStage::OpenRuntimeDbReadOnly,
        ));
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

        let _ = tx.send(StateJsonBytesProbeWorkerMessage::Entered(
            ReplayCheckpointHeadlineStateJsonBytesProbeStage::QueryStateJsonBytes,
        ));
        #[cfg(test)]
        if let Some(delay_ms) = take_test_force_state_json_bytes_probe_delay_ms() {
            thread::sleep(StdDuration::from_millis(delay_ms));
        }
        send_finished(load_replay_checkpoint_state_json_bytes(&conn));
    });

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted = true;
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation = format!(
                "state_json byte-length probing exhausted its bounded budget before completion (stage={})",
                diagnostic
                    .replay_checkpoint_headline_state_json_bytes_probe_stage
                    .map(ReplayCheckpointHeadlineStateJsonBytesProbeStage::as_str)
                    .unwrap_or("unknown")
            );
            return;
        }

        match rx.recv_timeout(remaining) {
            Ok(StateJsonBytesProbeWorkerMessage::Entered(stage)) => {
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_stage = Some(stage);
            }
            Ok(StateJsonBytesProbeWorkerMessage::Finished(result)) => {
                match result {
                    Ok(Some(bytes)) => {
                        diagnostic.persisted_rebuild_checkpoint_state_json_bytes = Some(bytes);
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_completed =
                            true;
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_stage =
                            Some(ReplayCheckpointHeadlineStateJsonBytesProbeStage::Complete);
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                            format!(
                                "state_json byte-length probing completed through a bounded read-only length(state_json) query against runtime db {}",
                                runtime_db_path.display()
                            );
                    }
                    Ok(None) => {
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_completed =
                            true;
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_stage =
                            Some(ReplayCheckpointHeadlineStateJsonBytesProbeStage::Complete);
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                            "state_json byte-length probing completed, but the persisted replay checkpoint row was no longer present when the byte-length query ran"
                                .to_string();
                    }
                    Err(error) => {
                        diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation = format!(
                            "state_json byte-length probing failed against runtime db {}: {error}",
                            runtime_db_path.display()
                        );
                    }
                }
                return;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted =
                    true;
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation = format!(
                    "state_json byte-length probing exhausted its bounded budget before completion (stage={})",
                    diagnostic
                        .replay_checkpoint_headline_state_json_bytes_probe_stage
                        .map(ReplayCheckpointHeadlineStateJsonBytesProbeStage::as_str)
                        .unwrap_or("unknown")
                );
                return;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation =
                    format!(
                        "state_json byte-length probing disconnected before returning a result for runtime db {}",
                        runtime_db_path.display()
                    );
                return;
            }
        }
    }
}

fn load_replay_checkpoint_state_json_bytes(conn: &Connection) -> Result<Option<usize>> {
    if !headline_table_exists(conn)? {
        return Ok(None);
    }

    let bytes = conn
        .query_row(
            "SELECT length(state_json)
             FROM discovery_persisted_rebuild_state
             WHERE id = 1",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()
        .context("failed querying discovery persisted rebuild state_json byte length")?
        .flatten()
        .map(|value| value.max(0) as usize);
    Ok(bytes)
}

fn headline_table_exists(conn: &Connection) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'discovery_persisted_rebuild_state'
            )",
            [],
            |row| row.get::<_, i64>(0),
        )
        .context("failed checking discovery_persisted_rebuild_state table existence")?
        != 0)
}

fn parse_rebuild_phase(raw: &str) -> Result<&'static str> {
    match raw {
        "collect_buy_mints" => Ok("collect_buy_mints"),
        "resolve_token_quality" => Ok("resolve_token_quality"),
        "replay" => Ok("replay"),
        "publish_pending" => Ok("publish_pending"),
        _ => Err(anyhow!("invalid discovery persisted rebuild phase: {raw}")),
    }
}

fn parse_rfc3339_utc(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid RFC3339 timestamp in {field}: {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

fn render_replay_checkpoint_headline_human(
    diagnostic: &ReplayCheckpointHeadlineDiagnostic,
) -> String {
    [
        "event=discovery_replay_checkpoint_headline".to_string(),
        format!(
            "replay_checkpoint_headline_observed={}",
            diagnostic.replay_checkpoint_headline_observed
        ),
        format!(
            "replay_checkpoint_headline_reason_class={}",
            serde_json::to_string(&diagnostic.replay_checkpoint_headline_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replay_checkpoint_headline_explanation={}",
            diagnostic.replay_checkpoint_headline_explanation
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
            "persisted_rebuild_checkpoint_exists={}",
            format_optional_bool(diagnostic.persisted_rebuild_checkpoint_exists)
        ),
        format!(
            "persisted_rebuild_checkpoint_phase={}",
            diagnostic
                .persisted_rebuild_checkpoint_phase
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "persisted_rebuild_checkpoint_updated_at={}",
            format_optional_ts(diagnostic.persisted_rebuild_checkpoint_updated_at.as_ref())
        ),
        format!(
            "persisted_rebuild_checkpoint_state_json_bytes={}",
            format_optional_usize(diagnostic.persisted_rebuild_checkpoint_state_json_bytes)
        ),
        format!(
            "persisted_rebuild_checkpoint_window_start={}",
            format_optional_ts(
                diagnostic
                    .persisted_rebuild_checkpoint_window_start
                    .as_ref()
            )
        ),
        format!(
            "persisted_rebuild_checkpoint_horizon_end={}",
            format_optional_ts(diagnostic.persisted_rebuild_checkpoint_horizon_end.as_ref())
        ),
        format!(
            "persisted_rebuild_checkpoint_metrics_window_start={}",
            format_optional_ts(
                diagnostic
                    .persisted_rebuild_checkpoint_metrics_window_start
                    .as_ref()
            )
        ),
        format!(
            "persisted_rebuild_checkpoint_phase_cursor_ts_utc={}",
            format_optional_ts(
                diagnostic
                    .persisted_rebuild_checkpoint_phase_cursor_ts_utc
                    .as_ref()
            )
        ),
        format!(
            "persisted_rebuild_checkpoint_phase_cursor_slot={}",
            format_optional_u64(diagnostic.persisted_rebuild_checkpoint_phase_cursor_slot)
        ),
        format!(
            "persisted_rebuild_checkpoint_phase_cursor_signature={}",
            diagnostic
                .persisted_rebuild_checkpoint_phase_cursor_signature
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "persisted_rebuild_checkpoint_prepass_rows_processed={}",
            format_optional_usize(diagnostic.persisted_rebuild_checkpoint_prepass_rows_processed)
        ),
        format!(
            "persisted_rebuild_checkpoint_prepass_pages_processed={}",
            format_optional_usize(diagnostic.persisted_rebuild_checkpoint_prepass_pages_processed)
        ),
        format!(
            "persisted_rebuild_checkpoint_replay_rows_processed={}",
            format_optional_usize(diagnostic.persisted_rebuild_checkpoint_replay_rows_processed)
        ),
        format!(
            "persisted_rebuild_checkpoint_replay_pages_processed={}",
            format_optional_usize(diagnostic.persisted_rebuild_checkpoint_replay_pages_processed)
        ),
        format!(
            "persisted_rebuild_checkpoint_chunks_completed={}",
            format_optional_usize(diagnostic.persisted_rebuild_checkpoint_chunks_completed)
        ),
        format!(
            "replay_checkpoint_headline_state_json_bytes_probe_attempted={}",
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_attempted
        ),
        format!(
            "replay_checkpoint_headline_state_json_bytes_probe_completed={}",
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_completed
        ),
        format!(
            "replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted={}",
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted
        ),
        format!(
            "replay_checkpoint_headline_state_json_bytes_probe_stage={}",
            diagnostic
                .replay_checkpoint_headline_state_json_bytes_probe_stage
                .map(ReplayCheckpointHeadlineStateJsonBytesProbeStage::as_str)
                .unwrap_or("null")
        ),
        format!(
            "replay_checkpoint_headline_state_json_bytes_probe_explanation={}",
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_explanation
        ),
        format!(
            "replay_checkpoint_headline_budget_exhausted={}",
            diagnostic.replay_checkpoint_headline_budget_exhausted
        ),
        format!(
            "replay_checkpoint_headline_stage={}",
            diagnostic
                .replay_checkpoint_headline_stage
                .map(ReplayCheckpointHeadlineStage::as_str)
                .unwrap_or("null")
        ),
        format!(
            "replay_checkpoint_headline_total_elapsed_ms={}",
            diagnostic.replay_checkpoint_headline_total_elapsed_ms
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
static TEST_FORCE_QUERY_DELAY_MS: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_FORCE_STATE_JSON_BYTES_PROBE_DELAY_MS: std::sync::Mutex<Option<u64>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn arm_test_force_query_delay_ms(delay_ms: u64) {
    *TEST_FORCE_QUERY_DELAY_MS
        .lock()
        .expect("query delay mutex poisoned") = Some(delay_ms);
}

#[cfg(test)]
fn arm_test_force_state_json_bytes_probe_delay_ms(delay_ms: u64) {
    *TEST_FORCE_STATE_JSON_BYTES_PROBE_DELAY_MS
        .lock()
        .expect("state_json byte probe delay mutex poisoned") = Some(delay_ms);
}

#[cfg(test)]
fn take_test_force_open_delay_ms() -> Option<u64> {
    TEST_FORCE_OPEN_DELAY_MS
        .lock()
        .expect("open delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_query_delay_ms() -> Option<u64> {
    TEST_FORCE_QUERY_DELAY_MS
        .lock()
        .expect("query delay mutex poisoned")
        .take()
}

#[cfg(test)]
fn take_test_force_state_json_bytes_probe_delay_ms() -> Option<u64> {
    TEST_FORCE_STATE_JSON_BYTES_PROBE_DELAY_MS
        .lock()
        .expect("state_json byte probe delay mutex poisoned")
        .take()
}

#[cfg(test)]
mod tests {
    use super::{
        arm_test_force_query_delay_ms, arm_test_force_state_json_bytes_probe_delay_ms,
        explain_replay_checkpoint_headline_read_only, parse_args_from, run_command, Command,
        ExplainConfig, ReplayCheckpointHeadlineReasonClass,
        ReplayCheckpointHeadlineStateJsonBytesProbeStage, TEST_SERIAL_LOCK,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, SecondsFormat, Utc};
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor,
        SqliteStore,
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
                assert!(!parsed.probe_state_json_bytes);
            }
        }
    }

    #[test]
    fn parse_args_from_accepts_probe_state_json_bytes_flag() {
        let _guard = serial_guard();
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "/tmp/live.server.toml".to_string(),
            "--probe-state-json-bytes".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        match parsed {
            Command::Explain(parsed) => {
                assert!(parsed.probe_state_json_bytes);
            }
        }
    }

    #[test]
    fn default_mode_with_present_row_returns_row_present_and_skips_state_json_probe() -> Result<()>
    {
        let _guard = serial_guard();
        let fixture = make_fixture("replay-checkpoint-headline-present")?;
        let row = seed_replay_checkpoint_row(&fixture.store)?;
        arm_test_force_state_json_bytes_probe_delay_ms(1_500);

        let diagnostic =
            explain_replay_checkpoint_headline_read_only(&fixture.config_path, 100, false);
        assert_eq!(
            diagnostic.replay_checkpoint_headline_reason_class,
            ReplayCheckpointHeadlineReasonClass::ReplayCheckpointHeadlineRowPresent
        );
        assert!(diagnostic.replay_checkpoint_headline_observed);
        assert_eq!(
            diagnostic.persisted_rebuild_checkpoint_phase.as_deref(),
            Some("replay")
        );
        assert_eq!(
            diagnostic.persisted_rebuild_checkpoint_updated_at,
            Some(row.updated_at)
        );
        assert_eq!(
            diagnostic.persisted_rebuild_checkpoint_state_json_bytes,
            None
        );
        assert!(!diagnostic.replay_checkpoint_headline_state_json_bytes_probe_attempted);
        assert!(!diagnostic.replay_checkpoint_headline_state_json_bytes_probe_completed);
        assert!(!diagnostic.replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted);
        assert_eq!(
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_stage,
            None
        );
        assert!(diagnostic
            .replay_checkpoint_headline_state_json_bytes_probe_explanation
            .contains("--probe-state-json-bytes was not requested"));

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            probe_state_json_bytes: false,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["replay_checkpoint_headline_reason_class"],
            "replay_checkpoint_headline_row_present"
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_updated_at"],
            row.updated_at.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes"],
            Value::Null
        );
        assert_eq!(
            parsed["replay_checkpoint_headline_state_json_bytes_probe_attempted"],
            false
        );
        Ok(())
    }

    #[test]
    fn default_mode_with_missing_row_returns_row_missing() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("replay-checkpoint-headline-missing-row")?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            probe_state_json_bytes: false,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_headline_reason_class"],
            "replay_checkpoint_headline_row_missing"
        );
        assert_eq!(parsed["replay_checkpoint_headline_observed"], true);
        assert_eq!(parsed["runtime_db_opened_read_only"], true);
        assert_eq!(parsed["persisted_rebuild_checkpoint_exists"], false);
        assert_eq!(parsed["persisted_rebuild_checkpoint_phase"], Value::Null);
        assert_eq!(
            parsed["replay_checkpoint_headline_state_json_bytes_probe_attempted"],
            false
        );
        Ok(())
    }

    #[test]
    fn missing_config_returns_unproven() -> Result<()> {
        let _guard = serial_guard();
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_config = temp.path().join("missing.server.toml");

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: missing_config.clone(),
            probe_state_json_bytes: false,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_headline_reason_class"],
            "replay_checkpoint_headline_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["replay_checkpoint_headline_observed"], false);
        assert_eq!(parsed["runtime_db_path"], Value::Null);
        assert_eq!(parsed["runtime_db_opened_read_only"], false);
        Ok(())
    }

    #[test]
    fn unreadable_runtime_db_returns_unproven() -> Result<()> {
        let _guard = serial_guard();
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_db = temp.path().join("missing-runtime.db");
        let config_path = write_config(&temp, "headline-unreadable", &missing_db)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path,
            probe_state_json_bytes: false,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_headline_reason_class"],
            "replay_checkpoint_headline_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["replay_checkpoint_headline_observed"], false);
        assert_eq!(parsed["runtime_db_opened_read_only"], false);
        assert_eq!(
            parsed["replay_checkpoint_headline_stage"],
            "open_runtime_db_read_only"
        );
        Ok(())
    }

    #[test]
    fn explicit_probe_state_json_bytes_success_fills_bytes_and_marks_probe_completed() -> Result<()>
    {
        let _guard = serial_guard();
        let fixture = make_fixture("replay-checkpoint-headline-probe-success")?;
        let row = seed_replay_checkpoint_row(&fixture.store)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            probe_state_json_bytes: true,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_headline_reason_class"],
            "replay_checkpoint_headline_row_present"
        );
        assert_eq!(
            parsed["persisted_rebuild_checkpoint_state_json_bytes"],
            row.state_json.len()
        );
        assert_eq!(
            parsed["replay_checkpoint_headline_state_json_bytes_probe_attempted"],
            true
        );
        assert_eq!(
            parsed["replay_checkpoint_headline_state_json_bytes_probe_completed"],
            true
        );
        assert_eq!(
            parsed["replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted"],
            false
        );
        assert_eq!(
            parsed["replay_checkpoint_headline_state_json_bytes_probe_stage"],
            "complete"
        );
        Ok(())
    }

    #[test]
    fn explicit_probe_budget_exhaustion_preserves_row_present() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("replay-checkpoint-headline-probe-budget-exhausted")?;
        seed_replay_checkpoint_row(&fixture.store)?;
        arm_test_force_state_json_bytes_probe_delay_ms(1_500);

        let diagnostic =
            explain_replay_checkpoint_headline_read_only(&fixture.config_path, 100, true);

        assert_eq!(
            diagnostic.replay_checkpoint_headline_reason_class,
            ReplayCheckpointHeadlineReasonClass::ReplayCheckpointHeadlineRowPresent
        );
        assert!(diagnostic.replay_checkpoint_headline_observed);
        assert!(!diagnostic.replay_checkpoint_headline_budget_exhausted);
        assert!(diagnostic.replay_checkpoint_headline_state_json_bytes_probe_attempted);
        assert!(!diagnostic.replay_checkpoint_headline_state_json_bytes_probe_completed);
        assert!(diagnostic.replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted);
        assert_eq!(
            diagnostic.replay_checkpoint_headline_state_json_bytes_probe_stage,
            Some(ReplayCheckpointHeadlineStateJsonBytesProbeStage::QueryStateJsonBytes)
        );
        assert_eq!(
            diagnostic.persisted_rebuild_checkpoint_state_json_bytes,
            None
        );
        Ok(())
    }

    #[test]
    fn core_budget_exhaustion_returns_unproven_with_core_budget_flag() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("replay-checkpoint-headline-budget-exhausted")?;
        seed_replay_checkpoint_row(&fixture.store)?;
        arm_test_force_query_delay_ms(1_500);

        let diagnostic =
            explain_replay_checkpoint_headline_read_only(&fixture.config_path, 100, false);

        assert_eq!(
            diagnostic.replay_checkpoint_headline_reason_class,
            ReplayCheckpointHeadlineReasonClass::ReplayCheckpointHeadlineUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.replay_checkpoint_headline_observed);
        assert!(diagnostic.replay_checkpoint_headline_budget_exhausted);
        assert_eq!(
            diagnostic.replay_checkpoint_headline_stage,
            Some(super::ReplayCheckpointHeadlineStage::QueryRowHeadline)
        );
        assert!(diagnostic.runtime_db_opened_read_only);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_new_probe_status_fields() -> Result<()> {
        let _guard = serial_guard();
        let fixture = make_fixture("replay-checkpoint-headline-json-fields")?;
        seed_replay_checkpoint_row(&fixture.store)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            probe_state_json_bytes: true,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        for key in [
            "replay_checkpoint_headline_observed",
            "replay_checkpoint_headline_reason_class",
            "replay_checkpoint_headline_explanation",
            "config_path",
            "runtime_db_path",
            "runtime_db_opened_read_only",
            "persisted_rebuild_checkpoint_exists",
            "persisted_rebuild_checkpoint_phase",
            "persisted_rebuild_checkpoint_updated_at",
            "persisted_rebuild_checkpoint_state_json_bytes",
            "replay_checkpoint_headline_state_json_bytes_probe_attempted",
            "replay_checkpoint_headline_state_json_bytes_probe_completed",
            "replay_checkpoint_headline_state_json_bytes_probe_budget_exhausted",
            "replay_checkpoint_headline_state_json_bytes_probe_stage",
            "replay_checkpoint_headline_state_json_bytes_probe_explanation",
            "replay_checkpoint_headline_budget_exhausted",
            "replay_checkpoint_headline_stage",
            "replay_checkpoint_headline_total_elapsed_ms",
        ] {
            assert!(
                parsed.get(key).is_some(),
                "expected required replay checkpoint headline field {key}"
            );
        }
        Ok(())
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
                "[sqlite]\npath = \"{}\"\n\n[runtime_restore_ops]\nartifact_retention = 2\nartifact_cadence_minutes = 10\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(config_path)
    }

    fn seed_replay_checkpoint_row(
        store: &SqliteStore,
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
            state_json: "{\"checkpoint\":\"headline\"}".to_string(),
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
