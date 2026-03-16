use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    AggregateBackfillProgressStatus, AggregateReadinessBlocker, AggregateReadinessStatus,
    DiscoveryService,
};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: aggregate_readiness_status --config <path> [--db-path <path>] [--json] [--now <rfc3339>]";

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
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct JsonCursor {
    ts_utc: DateTime<Utc>,
    slot: u64,
    signature: String,
}

#[derive(Debug, Serialize)]
struct JsonBackfillProgress {
    start_ts: DateTime<Utc>,
    cursor: JsonCursor,
}

#[derive(Debug, Serialize)]
struct JsonAggregateReadinessStatus {
    config_path: String,
    db_path: String,
    now: DateTime<Utc>,
    window_start: DateTime<Utc>,
    writes_enabled: bool,
    reads_enabled: bool,
    runtime_gate_max_lag_seconds: u64,
    audit_max_lag_buckets: u64,
    audit_max_lag_seconds: u64,
    covered_since: Option<DateTime<Utc>>,
    covered_through_ts: Option<DateTime<Utc>>,
    covered_through_cursor: Option<JsonCursor>,
    covered_through_lag_seconds: Option<u64>,
    materialization_gap_cursor: Option<JsonCursor>,
    backfill_progress: Option<JsonBackfillProgress>,
    backfill_protected_since: Option<DateTime<Utc>>,
    backfill_active: bool,
    backfill_resume_required: bool,
    coverage_markers_pending_backfill_completion: bool,
    scoring_horizon_covered: bool,
    covered_through_within_runtime_lag: bool,
    covered_through_within_audit_lag: bool,
    storage_ready_for_runtime_gate: bool,
    effective_writes_ready: bool,
    effective_reads_ready: bool,
    write_blockers: Vec<String>,
    read_blockers: Vec<String>,
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
    let mut db_path: Option<PathBuf> = None;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
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

fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    let configured_db_path = PathBuf::from(configured_db_path);
    if configured_db_path.is_absolute() {
        return configured_db_path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_db_path)
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open_read_only(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let status = discovery.aggregate_readiness_status(&store, config.now)?;

    if config.json {
        render_json(&config.config_path, &db_path, config.now, &status)
    } else {
        Ok(render_human(
            &config.config_path,
            &db_path,
            config.now,
            &status,
        ))
    }
}

fn render_human(
    config_path: &Path,
    db_path: &Path,
    now: DateTime<Utc>,
    status: &AggregateReadinessStatus,
) -> String {
    let lines = vec![
        "event=aggregate_readiness_status".to_string(),
        format!("config_path={}", config_path.display()),
        format!("db_path={}", db_path.display()),
        format!("now={}", now.to_rfc3339()),
        format!("window_start={}", status.window_start.to_rfc3339()),
        format!("writes_enabled={}", status.writes_enabled),
        format!("reads_enabled={}", status.reads_enabled),
        format!(
            "runtime_gate_max_lag_seconds={}",
            status.runtime_gate_max_lag_seconds
        ),
        format!("audit_max_lag_buckets={}", status.audit_max_lag_buckets),
        format!("audit_max_lag_seconds={}", status.audit_max_lag_seconds),
        format!(
            "covered_since={}",
            format_optional_ts(status.covered_since.as_ref())
        ),
        format!(
            "covered_through_ts={}",
            format_optional_ts(status.covered_through_ts.as_ref())
        ),
        format!(
            "covered_through_cursor={}",
            format_optional_cursor(status.covered_through_cursor.as_ref())
        ),
        format!(
            "covered_through_lag_seconds={}",
            format_optional_u64(status.covered_through_lag_seconds)
        ),
        format!(
            "materialization_gap_cursor={}",
            format_optional_cursor(status.materialization_gap_cursor.as_ref())
        ),
        format!(
            "backfill_progress={}",
            format_optional_backfill_progress(status.backfill_progress.as_ref())
        ),
        format!(
            "backfill_protected_since={}",
            format_optional_ts(status.backfill_protected_since.as_ref())
        ),
        format!("backfill_active={}", status.backfill_active),
        format!(
            "backfill_resume_required={}",
            status.backfill_resume_required
        ),
        format!(
            "coverage_markers_pending_backfill_completion={}",
            status.coverage_markers_pending_backfill_completion
        ),
        format!("scoring_horizon_covered={}", status.scoring_horizon_covered),
        format!(
            "covered_through_within_runtime_lag={}",
            status.covered_through_within_runtime_lag
        ),
        format!(
            "covered_through_within_audit_lag={}",
            status.covered_through_within_audit_lag
        ),
        format!(
            "storage_ready_for_runtime_gate={}",
            status.storage_ready_for_runtime_gate
        ),
        format!("effective_writes_ready={}", status.effective_writes_ready),
        format!("effective_reads_ready={}", status.effective_reads_ready),
        format!("write_blockers={}", format_blockers(&status.write_blockers)),
        format!("read_blockers={}", format_blockers(&status.read_blockers)),
    ];
    lines.join("\n")
}

fn render_json(
    config_path: &Path,
    db_path: &Path,
    now: DateTime<Utc>,
    status: &AggregateReadinessStatus,
) -> Result<String> {
    serde_json::to_string_pretty(&JsonAggregateReadinessStatus {
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        now,
        window_start: status.window_start,
        writes_enabled: status.writes_enabled,
        reads_enabled: status.reads_enabled,
        runtime_gate_max_lag_seconds: status.runtime_gate_max_lag_seconds,
        audit_max_lag_buckets: status.audit_max_lag_buckets,
        audit_max_lag_seconds: status.audit_max_lag_seconds,
        covered_since: status.covered_since,
        covered_through_ts: status.covered_through_ts,
        covered_through_cursor: status.covered_through_cursor.as_ref().map(cursor_to_json),
        covered_through_lag_seconds: status.covered_through_lag_seconds,
        materialization_gap_cursor: status
            .materialization_gap_cursor
            .as_ref()
            .map(cursor_to_json),
        backfill_progress: status
            .backfill_progress
            .as_ref()
            .map(backfill_progress_to_json),
        backfill_protected_since: status.backfill_protected_since,
        backfill_active: status.backfill_active,
        backfill_resume_required: status.backfill_resume_required,
        coverage_markers_pending_backfill_completion: status
            .coverage_markers_pending_backfill_completion,
        scoring_horizon_covered: status.scoring_horizon_covered,
        covered_through_within_runtime_lag: status.covered_through_within_runtime_lag,
        covered_through_within_audit_lag: status.covered_through_within_audit_lag,
        storage_ready_for_runtime_gate: status.storage_ready_for_runtime_gate,
        effective_writes_ready: status.effective_writes_ready,
        effective_reads_ready: status.effective_reads_ready,
        write_blockers: status
            .write_blockers
            .iter()
            .map(|blocker| blocker.as_str().to_string())
            .collect(),
        read_blockers: status
            .read_blockers
            .iter()
            .map(|blocker| blocker.as_str().to_string())
            .collect(),
    })
    .context("failed serializing aggregate readiness status json")
}

fn cursor_to_json(cursor: &DiscoveryRuntimeCursor) -> JsonCursor {
    JsonCursor {
        ts_utc: cursor.ts_utc,
        slot: cursor.slot,
        signature: cursor.signature.clone(),
    }
}

fn backfill_progress_to_json(progress: &AggregateBackfillProgressStatus) -> JsonBackfillProgress {
    JsonBackfillProgress {
        start_ts: progress.start_ts,
        cursor: cursor_to_json(&progress.cursor),
    }
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_cursor(value: Option<&DiscoveryRuntimeCursor>) -> String {
    value
        .map(|cursor| {
            format!(
                "ts_utc={},slot={},signature={}",
                cursor.ts_utc.to_rfc3339(),
                cursor.slot,
                cursor.signature
            )
        })
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_backfill_progress(value: Option<&AggregateBackfillProgressStatus>) -> String {
    value
        .map(|progress| {
            format!(
                "start_ts={},cursor=({})",
                progress.start_ts.to_rfc3339(),
                format_optional_cursor(Some(&progress.cursor))
            )
        })
        .unwrap_or_else(|| "null".to_string())
}

fn format_blockers(blockers: &[AggregateReadinessBlocker]) -> String {
    if blockers.is_empty() {
        return "[]".to_string();
    }
    format!(
        "[{}]",
        blockers
            .iter()
            .map(|blocker| blocker.as_str())
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::{parse_args_from, render_human, resolve_db_path, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_discovery::AggregateReadinessBlocker;
    use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
    use serde_json::Value;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_returns_none_for_help_flag() {
        let parsed = parse_args_from(vec!["--help".to_string()]).expect("help should succeed");
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_args_from_accepts_optional_db_path_json_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-16T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");
        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-16T12:00:00+00:00");
    }

    #[test]
    fn resolve_db_path_uses_config_parent_for_relative_configured_sqlite_path() {
        let config_path = PathBuf::from("/tmp/copybot/configs/live.toml");
        let resolved = resolve_db_path(&config_path, None, "../state/copybot.db");
        assert_eq!(
            resolved,
            PathBuf::from("/tmp/copybot/configs/../state/copybot.db")
        );
    }

    #[test]
    fn run_json_outputs_blocker_inventory_from_real_db() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-readiness-cli.db");
        let config_path = temp.path().join("aggregate-readiness-cli.toml");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        let window_start = now - Duration::days(7);
        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(5),
            slot: 42,
            signature: "aggregate-cli-covered-through".to_string(),
        })?;
        drop(store);

        std::fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nscoring_aggregates_write_enabled = true\nscoring_aggregates_enabled = true\n",
                db_path.display()
            ),
        )
        .context("failed writing test config")?;

        let output = run(Config {
            config_path: config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value =
            serde_json::from_str(&output).context("json output should be valid json")?;
        assert_eq!(
            parsed
                .get("effective_writes_ready")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed.get("effective_reads_ready").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .get("write_blockers")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn run_json_resolves_relative_sqlite_path_from_config_dir() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let config_dir = temp.path().join("configs");
        let state_dir = temp.path().join("state");
        std::fs::create_dir_all(&config_dir).context("failed creating config dir")?;
        std::fs::create_dir_all(&state_dir).context("failed creating state dir")?;

        let db_path = state_dir.join("aggregate-readiness-relative.db");
        let config_path = config_dir.join("live.toml");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        let window_start = now - Duration::days(7);
        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(5),
            slot: 7,
            signature: "aggregate-cli-relative-covered-through".to_string(),
        })?;
        drop(store);

        std::fs::write(
            &config_path,
            "[sqlite]\npath = \"../state/aggregate-readiness-relative.db\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nscoring_aggregates_write_enabled = true\nscoring_aggregates_enabled = true\n",
        )
        .context("failed writing relative-path test config")?;

        let output = run(Config {
            config_path: config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value =
            serde_json::from_str(&output).context("json output should be valid json")?;
        let resolved_db_path = PathBuf::from(
            parsed
                .get("db_path")
                .and_then(Value::as_str)
                .expect("db_path should be present"),
        );
        assert_eq!(resolved_db_path.canonicalize()?, db_path.canonicalize()?);
        assert_eq!(
            parsed.get("effective_reads_ready").and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn render_human_includes_named_blockers() {
        let status = copybot_discovery::AggregateReadinessStatus {
            window_start: DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            writes_enabled: false,
            reads_enabled: false,
            runtime_gate_max_lag_seconds: 600,
            audit_max_lag_buckets: 2,
            audit_max_lag_seconds: 3_600,
            covered_since: None,
            covered_through_ts: None,
            covered_through_cursor: None,
            covered_through_lag_seconds: None,
            materialization_gap_cursor: None,
            backfill_progress: None,
            backfill_protected_since: None,
            backfill_active: false,
            backfill_resume_required: false,
            coverage_markers_pending_backfill_completion: false,
            scoring_horizon_covered: false,
            covered_through_within_runtime_lag: false,
            covered_through_within_audit_lag: false,
            storage_ready_for_runtime_gate: false,
            effective_writes_ready: false,
            effective_reads_ready: false,
            write_blockers: vec![AggregateReadinessBlocker::WritesDisabledByConfig],
            read_blockers: vec![
                AggregateReadinessBlocker::ReadsDisabledByConfig,
                AggregateReadinessBlocker::MissingCoveredSince,
            ],
        };

        let output = render_human(
            Path::new("configs/live.toml"),
            Path::new("state/copybot.db"),
            DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            &status,
        );
        assert!(output.contains("write_blockers=[writes_disabled_by_config]"));
        assert!(output.contains("read_blockers=[reads_disabled_by_config,missing_covered_since]"));
    }
}
