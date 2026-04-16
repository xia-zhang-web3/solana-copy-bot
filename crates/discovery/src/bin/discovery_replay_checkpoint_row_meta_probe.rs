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
  discovery_replay_checkpoint_row_meta_probe --config <path> [--json]";
const DEFAULT_REPLAY_CHECKPOINT_ROW_META_PROBE_BUDGET_MS: u64 = 1_000;

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
enum ReplayCheckpointRowMetaProbeReasonClass {
    ReplayCheckpointRowMetaProbeRowPresent,
    ReplayCheckpointRowMetaProbeRowMissing,
    ReplayCheckpointRowMetaProbeUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplayCheckpointRowMetaProbeStage {
    OpenRuntimeDbReadOnly,
    SchemaLookup,
    RowCount,
    PreparePrimaryKeyLookup,
    StepPrimaryKeyLookup,
    Complete,
}

impl ReplayCheckpointRowMetaProbeStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::OpenRuntimeDbReadOnly => "open_runtime_db_read_only",
            Self::SchemaLookup => "schema_lookup",
            Self::RowCount => "row_count",
            Self::PreparePrimaryKeyLookup => "prepare_primary_key_lookup",
            Self::StepPrimaryKeyLookup => "step_primary_key_lookup",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReplayCheckpointRowMetaProbeDiagnostic {
    replay_checkpoint_row_meta_probe_observed: bool,
    replay_checkpoint_row_meta_probe_reason_class: ReplayCheckpointRowMetaProbeReasonClass,
    replay_checkpoint_row_meta_probe_explanation: String,
    config_path: String,
    runtime_db_path: Option<String>,
    runtime_db_opened_read_only: bool,
    replay_checkpoint_row_meta_probe_budget_exhausted: bool,
    replay_checkpoint_row_meta_probe_stage: Option<ReplayCheckpointRowMetaProbeStage>,
    replay_checkpoint_row_meta_probe_total_elapsed_ms: u64,
    replay_checkpoint_row_meta_table_exists: Option<bool>,
    replay_checkpoint_row_meta_row_exists: Option<bool>,
    replay_checkpoint_row_meta_phase: Option<String>,
    replay_checkpoint_row_meta_updated_at: Option<DateTime<Utc>>,
    replay_checkpoint_row_meta_query_path: Option<String>,
    replay_checkpoint_row_meta_query_elapsed_ms: Option<u64>,
    replay_checkpoint_row_meta_row_count_elapsed_ms: Option<u64>,
    replay_checkpoint_row_meta_schema_lookup_elapsed_ms: Option<u64>,
    replay_checkpoint_row_meta_prepare_elapsed_ms: Option<u64>,
    replay_checkpoint_row_meta_step_elapsed_ms: Option<u64>,
}

impl ReplayCheckpointRowMetaProbeDiagnostic {
    fn unproven(config_path: &Path, explanation: String) -> Self {
        Self {
            replay_checkpoint_row_meta_probe_observed: false,
            replay_checkpoint_row_meta_probe_reason_class:
                ReplayCheckpointRowMetaProbeReasonClass::ReplayCheckpointRowMetaProbeUnprovenDueToMissingEvidence,
            replay_checkpoint_row_meta_probe_explanation: explanation,
            config_path: config_path.display().to_string(),
            runtime_db_path: None,
            runtime_db_opened_read_only: false,
            replay_checkpoint_row_meta_probe_budget_exhausted: false,
            replay_checkpoint_row_meta_probe_stage: None,
            replay_checkpoint_row_meta_probe_total_elapsed_ms: 0,
            replay_checkpoint_row_meta_table_exists: None,
            replay_checkpoint_row_meta_row_exists: None,
            replay_checkpoint_row_meta_phase: None,
            replay_checkpoint_row_meta_updated_at: None,
            replay_checkpoint_row_meta_query_path: None,
            replay_checkpoint_row_meta_query_elapsed_ms: None,
            replay_checkpoint_row_meta_row_count_elapsed_ms: None,
            replay_checkpoint_row_meta_schema_lookup_elapsed_ms: None,
            replay_checkpoint_row_meta_prepare_elapsed_ms: None,
            replay_checkpoint_row_meta_step_elapsed_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayCheckpointRowMetaProbeSuccess {
    table_exists: bool,
    row_exists: Option<bool>,
    phase: Option<String>,
    updated_at: Option<DateTime<Utc>>,
    query_path: String,
    query_elapsed_ms: Option<u64>,
    row_count_elapsed_ms: Option<u64>,
    schema_lookup_elapsed_ms: Option<u64>,
    prepare_elapsed_ms: Option<u64>,
    step_elapsed_ms: Option<u64>,
}

#[derive(Debug)]
enum WorkerMessage {
    Entered(ReplayCheckpointRowMetaProbeStage),
    OpenedReadOnly,
    Finished(Result<ReplayCheckpointRowMetaProbeSuccess, String>),
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
            let diagnostic = explain_replay_checkpoint_row_meta_probe_read_only(
                &config.config_path,
                DEFAULT_REPLAY_CHECKPOINT_ROW_META_PROBE_BUDGET_MS,
            );
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed to serialize replay checkpoint row-meta probe diagnostic")
            } else {
                Ok(render_replay_checkpoint_row_meta_probe_human(&diagnostic))
            }
        }
    }
}

fn explain_replay_checkpoint_row_meta_probe_read_only(
    config_path: &Path,
    budget_ms: u64,
) -> ReplayCheckpointRowMetaProbeDiagnostic {
    let started_at = Instant::now();
    let loaded_config = match load_from_path(config_path) {
        Ok(config) => config,
        Err(error) => {
            let mut diagnostic = ReplayCheckpointRowMetaProbeDiagnostic::unproven(
                config_path,
                format!(
                    "replay checkpoint row-meta probe is unproven because config {} could not be loaded: {error:#}",
                    config_path.display()
                ),
            );
            diagnostic.replay_checkpoint_row_meta_probe_total_elapsed_ms = elapsed_ms(started_at);
            return diagnostic;
        }
    };

    let runtime_db_path = resolve_db_path(config_path, None, &loaded_config.sqlite.path);
    let mut diagnostic = ReplayCheckpointRowMetaProbeDiagnostic::unproven(
        config_path,
        "replay checkpoint row-meta probe could not yet be proven from config-relative runtime-db evidence"
            .to_string(),
    );
    diagnostic.runtime_db_path = Some(runtime_db_path.display().to_string());

    let deadline = started_at + StdDuration::from_millis(budget_ms);
    let (tx, rx) = mpsc::sync_channel(16);
    let runtime_db_path_for_worker = runtime_db_path.clone();
    thread::spawn(move || {
        let send_finished = |result: Result<ReplayCheckpointRowMetaProbeSuccess>| {
            let _ = tx.send(WorkerMessage::Finished(
                result.map_err(|error| format!("{error:#}")),
            ));
        };

        let _ = tx.send(WorkerMessage::Entered(
            ReplayCheckpointRowMetaProbeStage::OpenRuntimeDbReadOnly,
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
        if tx.send(WorkerMessage::OpenedReadOnly).is_err() {
            return;
        }

        send_finished(load_replay_checkpoint_row_meta_probe_success(&conn, &tx));
    });

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            diagnostic.replay_checkpoint_row_meta_probe_budget_exhausted = true;
            diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                "replay checkpoint row-meta probe is unproven because the bounded runtime-db row-meta path exhausted its budget before completion (stage={})",
                diagnostic
                    .replay_checkpoint_row_meta_probe_stage
                    .map(ReplayCheckpointRowMetaProbeStage::as_str)
                    .unwrap_or("unknown")
            );
            diagnostic.replay_checkpoint_row_meta_probe_total_elapsed_ms = elapsed_ms(started_at);
            return diagnostic;
        }

        match rx.recv_timeout(remaining) {
            Ok(WorkerMessage::Entered(stage)) => {
                diagnostic.replay_checkpoint_row_meta_probe_stage = Some(stage);
            }
            Ok(WorkerMessage::OpenedReadOnly) => {
                diagnostic.runtime_db_opened_read_only = true;
            }
            Ok(WorkerMessage::Finished(result)) => {
                diagnostic.replay_checkpoint_row_meta_probe_total_elapsed_ms =
                    elapsed_ms(started_at);
                return match result {
                    Ok(success) => {
                        diagnostic.replay_checkpoint_row_meta_table_exists =
                            Some(success.table_exists);
                        diagnostic.replay_checkpoint_row_meta_row_exists = success.row_exists;
                        diagnostic.replay_checkpoint_row_meta_phase = success.phase;
                        diagnostic.replay_checkpoint_row_meta_updated_at = success.updated_at;
                        diagnostic.replay_checkpoint_row_meta_query_path =
                            Some(success.query_path.clone());
                        diagnostic.replay_checkpoint_row_meta_query_elapsed_ms =
                            success.query_elapsed_ms;
                        diagnostic.replay_checkpoint_row_meta_row_count_elapsed_ms =
                            success.row_count_elapsed_ms;
                        diagnostic.replay_checkpoint_row_meta_schema_lookup_elapsed_ms =
                            success.schema_lookup_elapsed_ms;
                        diagnostic.replay_checkpoint_row_meta_prepare_elapsed_ms =
                            success.prepare_elapsed_ms;
                        diagnostic.replay_checkpoint_row_meta_step_elapsed_ms =
                            success.step_elapsed_ms;
                        diagnostic.replay_checkpoint_row_meta_probe_stage =
                            Some(ReplayCheckpointRowMetaProbeStage::Complete);

                        match (success.table_exists, success.row_exists) {
                            (true, Some(true)) => {
                                diagnostic.replay_checkpoint_row_meta_probe_observed = true;
                                diagnostic.replay_checkpoint_row_meta_probe_reason_class =
                                    ReplayCheckpointRowMetaProbeReasonClass::ReplayCheckpointRowMetaProbeRowPresent;
                                diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                                    "runtime db {} is readable read-only and the persisted replay-checkpoint row-meta query succeeded through {} without touching state_json bytes or recent_raw artifacts",
                                    runtime_db_path.display(),
                                    success.query_path
                                );
                                diagnostic
                            }
                            (true, Some(false)) => {
                                diagnostic.replay_checkpoint_row_meta_probe_observed = true;
                                diagnostic.replay_checkpoint_row_meta_probe_reason_class =
                                    ReplayCheckpointRowMetaProbeReasonClass::ReplayCheckpointRowMetaProbeRowMissing;
                                diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                                    "runtime db {} is readable read-only, discovery_persisted_rebuild_state exists, and bounded row-meta probing proved that id=1 is absent through {}",
                                    runtime_db_path.display(),
                                    success.query_path
                                );
                                diagnostic
                            }
                            (false, _) => {
                                diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                                    "replay checkpoint row-meta probe is unproven because runtime db {} is readable read-only but discovery_persisted_rebuild_state is absent, so no persisted replay-checkpoint row-meta evidence is currently available",
                                    runtime_db_path.display()
                                );
                                diagnostic
                            }
                            _ => {
                                diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                                    "replay checkpoint row-meta probe is unproven because runtime db {} returned incomplete row-meta evidence after bounded probing through {}",
                                    runtime_db_path.display(),
                                    success.query_path
                                );
                                diagnostic
                            }
                        }
                    }
                    Err(error) => {
                        diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                            "replay checkpoint row-meta probe is unproven because runtime db {} could not be read through the bounded row-meta path: {error}",
                            runtime_db_path.display()
                        );
                        diagnostic
                    }
                };
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                diagnostic.replay_checkpoint_row_meta_probe_budget_exhausted = true;
                diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                    "replay checkpoint row-meta probe is unproven because the bounded runtime-db row-meta path exhausted its budget before completion (stage={})",
                    diagnostic
                        .replay_checkpoint_row_meta_probe_stage
                        .map(ReplayCheckpointRowMetaProbeStage::as_str)
                        .unwrap_or("unknown")
                );
                diagnostic.replay_checkpoint_row_meta_probe_total_elapsed_ms =
                    elapsed_ms(started_at);
                return diagnostic;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                diagnostic.replay_checkpoint_row_meta_probe_explanation = format!(
                    "replay checkpoint row-meta probe is unproven because the bounded runtime-db row-meta worker disconnected before returning a result for {}",
                    runtime_db_path.display()
                );
                diagnostic.replay_checkpoint_row_meta_probe_total_elapsed_ms =
                    elapsed_ms(started_at);
                return diagnostic;
            }
        }
    }
}

fn load_replay_checkpoint_row_meta_probe_success(
    conn: &Connection,
    tx: &mpsc::SyncSender<WorkerMessage>,
) -> Result<ReplayCheckpointRowMetaProbeSuccess> {
    tx.send(WorkerMessage::Entered(
        ReplayCheckpointRowMetaProbeStage::SchemaLookup,
    ))
    .map_err(|_| anyhow!("row-meta worker stage channel closed before schema lookup"))?;
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
        return Ok(ReplayCheckpointRowMetaProbeSuccess {
            table_exists: false,
            row_exists: None,
            phase: None,
            updated_at: None,
            query_path: "schema_lookup_only_table_missing".to_string(),
            query_elapsed_ms: None,
            row_count_elapsed_ms: None,
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            prepare_elapsed_ms: None,
            step_elapsed_ms: None,
        });
    }

    tx.send(WorkerMessage::Entered(
        ReplayCheckpointRowMetaProbeStage::RowCount,
    ))
    .map_err(|_| anyhow!("row-meta worker stage channel closed before row count"))?;
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
        return Ok(ReplayCheckpointRowMetaProbeSuccess {
            table_exists: true,
            row_exists: Some(false),
            phase: None,
            updated_at: None,
            query_path: "schema_lookup_then_row_count".to_string(),
            query_elapsed_ms: None,
            row_count_elapsed_ms: Some(row_count_elapsed_ms),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            prepare_elapsed_ms: None,
            step_elapsed_ms: None,
        });
    }

    let query_started = Instant::now();
    tx.send(WorkerMessage::Entered(
        ReplayCheckpointRowMetaProbeStage::PreparePrimaryKeyLookup,
    ))
    .map_err(|_| anyhow!("row-meta worker stage channel closed before prepare"))?;
    #[cfg(test)]
    if let Some(delay_ms) = take_test_force_prepare_delay_ms() {
        thread::sleep(StdDuration::from_millis(delay_ms));
    }
    let prepare_started = Instant::now();
    let mut stmt = conn
        .prepare(ROW_META_SQL)
        .context("failed preparing persisted replay-checkpoint row-meta query")?;
    let prepare_elapsed_ms = elapsed_ms(prepare_started);

    tx.send(WorkerMessage::Entered(
        ReplayCheckpointRowMetaProbeStage::StepPrimaryKeyLookup,
    ))
    .map_err(|_| anyhow!("row-meta worker stage channel closed before step"))?;
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
    let query_elapsed_ms = elapsed_ms(query_started);

    let Some((phase, updated_at_raw)) = raw else {
        return Ok(ReplayCheckpointRowMetaProbeSuccess {
            table_exists: true,
            row_exists: Some(false),
            phase: None,
            updated_at: None,
            query_path: "schema_lookup_then_row_count_then_primary_key_lookup".to_string(),
            query_elapsed_ms: Some(query_elapsed_ms),
            row_count_elapsed_ms: Some(row_count_elapsed_ms),
            schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
            prepare_elapsed_ms: Some(prepare_elapsed_ms),
            step_elapsed_ms: Some(step_elapsed_ms),
        });
    };

    Ok(ReplayCheckpointRowMetaProbeSuccess {
        table_exists: true,
        row_exists: Some(true),
        phase: Some(phase),
        updated_at: Some(parse_rfc3339_utc(
            &updated_at_raw,
            "discovery_persisted_rebuild_state.updated_at",
        )?),
        query_path: "schema_lookup_then_row_count_then_primary_key_lookup".to_string(),
        query_elapsed_ms: Some(query_elapsed_ms),
        row_count_elapsed_ms: Some(row_count_elapsed_ms),
        schema_lookup_elapsed_ms: Some(schema_lookup_elapsed_ms),
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
    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn render_replay_checkpoint_row_meta_probe_human(
    diagnostic: &ReplayCheckpointRowMetaProbeDiagnostic,
) -> String {
    [
        "event=discovery_replay_checkpoint_row_meta_probe".to_string(),
        format!(
            "replay_checkpoint_row_meta_probe_observed={}",
            diagnostic.replay_checkpoint_row_meta_probe_observed
        ),
        format!(
            "replay_checkpoint_row_meta_probe_reason_class={}",
            serde_json::to_string(&diagnostic.replay_checkpoint_row_meta_probe_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replay_checkpoint_row_meta_probe_explanation={}",
            diagnostic.replay_checkpoint_row_meta_probe_explanation
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
            "replay_checkpoint_row_meta_probe_budget_exhausted={}",
            diagnostic.replay_checkpoint_row_meta_probe_budget_exhausted
        ),
        format!(
            "replay_checkpoint_row_meta_probe_stage={}",
            diagnostic
                .replay_checkpoint_row_meta_probe_stage
                .map(ReplayCheckpointRowMetaProbeStage::as_str)
                .unwrap_or("null")
        ),
        format!(
            "replay_checkpoint_row_meta_probe_total_elapsed_ms={}",
            diagnostic.replay_checkpoint_row_meta_probe_total_elapsed_ms
        ),
        format!(
            "replay_checkpoint_row_meta_table_exists={}",
            format_optional_bool(diagnostic.replay_checkpoint_row_meta_table_exists)
        ),
        format!(
            "replay_checkpoint_row_meta_row_exists={}",
            format_optional_bool(diagnostic.replay_checkpoint_row_meta_row_exists)
        ),
        format!(
            "replay_checkpoint_row_meta_phase={}",
            diagnostic
                .replay_checkpoint_row_meta_phase
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "replay_checkpoint_row_meta_updated_at={}",
            format_optional_ts(diagnostic.replay_checkpoint_row_meta_updated_at.as_ref())
        ),
        format!(
            "replay_checkpoint_row_meta_query_path={}",
            diagnostic
                .replay_checkpoint_row_meta_query_path
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "replay_checkpoint_row_meta_query_elapsed_ms={}",
            format_optional_u64(diagnostic.replay_checkpoint_row_meta_query_elapsed_ms)
        ),
        format!(
            "replay_checkpoint_row_meta_row_count_elapsed_ms={}",
            format_optional_u64(diagnostic.replay_checkpoint_row_meta_row_count_elapsed_ms)
        ),
        format!(
            "replay_checkpoint_row_meta_schema_lookup_elapsed_ms={}",
            format_optional_u64(diagnostic.replay_checkpoint_row_meta_schema_lookup_elapsed_ms)
        ),
        format!(
            "replay_checkpoint_row_meta_prepare_elapsed_ms={}",
            format_optional_u64(diagnostic.replay_checkpoint_row_meta_prepare_elapsed_ms)
        ),
        format!(
            "replay_checkpoint_row_meta_step_elapsed_ms={}",
            format_optional_u64(diagnostic.replay_checkpoint_row_meta_step_elapsed_ms)
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
        arm_test_force_row_count_delay_ms, explain_replay_checkpoint_row_meta_probe_read_only,
        parse_args_from, run_command, Command, ExplainConfig,
        ReplayCheckpointRowMetaProbeReasonClass, ReplayCheckpointRowMetaProbeStage, ROW_COUNT_SQL,
        ROW_META_SQL, TABLE_EXISTS_SQL,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
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
    fn present_row_fixture_returns_row_present_and_fills_phase_and_updated_at_without_recent_raw(
    ) -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("replay-checkpoint-row-meta-probe-present")?;
        seed_replay_checkpoint_row(&fixture.store, "{not valid json")?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_row_meta_probe_reason_class"],
            "replay_checkpoint_row_meta_probe_row_present"
        );
        assert_eq!(parsed["replay_checkpoint_row_meta_probe_observed"], true);
        assert_eq!(parsed["runtime_db_opened_read_only"], true);
        assert_eq!(parsed["replay_checkpoint_row_meta_table_exists"], true);
        assert_eq!(parsed["replay_checkpoint_row_meta_row_exists"], true);
        assert_eq!(parsed["replay_checkpoint_row_meta_phase"], "replay");
        assert_eq!(
            parsed["replay_checkpoint_row_meta_query_path"],
            "schema_lookup_then_row_count_then_primary_key_lookup"
        );
        assert!(parsed["replay_checkpoint_row_meta_query_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["replay_checkpoint_row_meta_prepare_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(parsed["replay_checkpoint_row_meta_step_elapsed_ms"]
            .as_u64()
            .is_some());
        assert!(
            parsed["replay_checkpoint_row_meta_schema_lookup_elapsed_ms"]
                .as_u64()
                .is_some()
        );
        assert!(parsed["replay_checkpoint_row_meta_row_count_elapsed_ms"]
            .as_u64()
            .is_some());
        assert_eq!(
            parsed["replay_checkpoint_row_meta_probe_budget_exhausted"],
            false
        );
        assert_eq!(parsed["replay_checkpoint_row_meta_probe_stage"], "complete");
        Ok(())
    }

    #[test]
    fn missing_row_fixture_returns_row_missing() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("replay-checkpoint-row-meta-probe-missing-row")?;
        fixture.store.clear_discovery_persisted_rebuild_state()?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_row_meta_probe_reason_class"],
            "replay_checkpoint_row_meta_probe_row_missing"
        );
        assert_eq!(parsed["replay_checkpoint_row_meta_probe_observed"], true);
        assert_eq!(parsed["runtime_db_opened_read_only"], true);
        assert_eq!(parsed["replay_checkpoint_row_meta_table_exists"], true);
        assert_eq!(parsed["replay_checkpoint_row_meta_row_exists"], false);
        assert_eq!(
            parsed["replay_checkpoint_row_meta_query_path"],
            "schema_lookup_then_row_count"
        );
        assert_eq!(parsed["replay_checkpoint_row_meta_phase"], Value::Null);
        assert_eq!(
            parsed["replay_checkpoint_row_meta_prepare_elapsed_ms"],
            Value::Null
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
            parsed["replay_checkpoint_row_meta_probe_reason_class"],
            "replay_checkpoint_row_meta_probe_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["replay_checkpoint_row_meta_probe_observed"], false);
        assert_eq!(parsed["runtime_db_path"], Value::Null);
        assert_eq!(parsed["runtime_db_opened_read_only"], false);
        Ok(())
    }

    #[test]
    fn unreadable_runtime_db_returns_unproven() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let temp = tempdir().context("failed to create tempdir")?;
        let missing_db = temp.path().join("missing-runtime.db");
        let config_path = write_config(&temp, "row-meta-probe-unreadable", &missing_db)?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path,
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed["replay_checkpoint_row_meta_probe_reason_class"],
            "replay_checkpoint_row_meta_probe_unproven_due_to_missing_evidence"
        );
        assert_eq!(parsed["replay_checkpoint_row_meta_probe_observed"], false);
        assert_eq!(parsed["runtime_db_opened_read_only"], false);
        assert_eq!(
            parsed["replay_checkpoint_row_meta_probe_stage"],
            "open_runtime_db_read_only"
        );
        Ok(())
    }

    #[test]
    fn bounded_budget_exhaustion_returns_unproven_with_exact_stage() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("replay-checkpoint-row-meta-probe-budget-exhausted")?;
        seed_replay_checkpoint_row(&fixture.store, "{\"checkpoint\":\"probe\"}")?;
        arm_test_force_row_count_delay_ms(1_500);

        let diagnostic =
            explain_replay_checkpoint_row_meta_probe_read_only(&fixture.config_path, 100);

        assert_eq!(
            diagnostic.replay_checkpoint_row_meta_probe_reason_class,
            ReplayCheckpointRowMetaProbeReasonClass::ReplayCheckpointRowMetaProbeUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.replay_checkpoint_row_meta_probe_observed);
        assert!(diagnostic.replay_checkpoint_row_meta_probe_budget_exhausted);
        assert_eq!(
            diagnostic.replay_checkpoint_row_meta_probe_stage,
            Some(ReplayCheckpointRowMetaProbeStage::RowCount)
        );
        assert!(diagnostic.runtime_db_opened_read_only);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        let fixture = make_fixture("replay-checkpoint-row-meta-probe-json-fields")?;
        seed_replay_checkpoint_row(&fixture.store, "{\"checkpoint\":\"probe\"}")?;

        let rendered = run_command(Command::Explain(ExplainConfig {
            config_path: fixture.config_path.clone(),
            json: true,
        }))?;
        let parsed: Value = serde_json::from_str(&rendered)?;

        for key in [
            "replay_checkpoint_row_meta_probe_observed",
            "replay_checkpoint_row_meta_probe_reason_class",
            "replay_checkpoint_row_meta_probe_explanation",
            "config_path",
            "runtime_db_path",
            "runtime_db_opened_read_only",
            "replay_checkpoint_row_meta_probe_budget_exhausted",
            "replay_checkpoint_row_meta_probe_stage",
            "replay_checkpoint_row_meta_probe_total_elapsed_ms",
            "replay_checkpoint_row_meta_table_exists",
            "replay_checkpoint_row_meta_row_exists",
            "replay_checkpoint_row_meta_phase",
            "replay_checkpoint_row_meta_updated_at",
            "replay_checkpoint_row_meta_query_path",
            "replay_checkpoint_row_meta_query_elapsed_ms",
            "replay_checkpoint_row_meta_row_count_elapsed_ms",
            "replay_checkpoint_row_meta_schema_lookup_elapsed_ms",
            "replay_checkpoint_row_meta_prepare_elapsed_ms",
            "replay_checkpoint_row_meta_step_elapsed_ms",
        ] {
            assert!(
                parsed.get(key).is_some(),
                "expected required replay checkpoint row-meta probe field {key}"
            );
        }
        Ok(())
    }

    #[test]
    fn probe_sql_never_references_state_json_or_recent_raw() {
        let _guard = super::TEST_SERIAL_LOCK
            .lock()
            .expect("serial lock poisoned");
        for sql in [TABLE_EXISTS_SQL, ROW_COUNT_SQL, ROW_META_SQL] {
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
