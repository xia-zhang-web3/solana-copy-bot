use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, DiscoveryConfig};
use copybot_storage::SqliteStore;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = concat!(
    "usage: discovery_persisted_freshness_lineage_audit",
    " --db <path> --config <path> --source-stat <path> --now <rfc3339> [--json]",
);

const CONCLUSION_ARTIFACT_AND_CONFIGURED_DB_PATH_MATCH_BUT_FRONTIER_IS_STALE: &str =
    "artifact_and_configured_db_path_match_but_frontier_is_stale";
const CONCLUSION_ARTIFACT_FRONTIER_IS_STALE_BUT_SOURCE_DB_LINEAGE_UNPROVEN: &str =
    "artifact_frontier_is_stale_but_source_db_lineage_unproven";
const CONCLUSION_CONFIGURED_DB_PATH_DIFFERS_FROM_AUDIT_DB_PATH: &str =
    "configured_db_path_differs_from_audit_db_path";
const CONCLUSION_SOURCE_STAT_CONFLICTS_WITH_AUDIT_ARTIFACT: &str =
    "source_stat_conflicts_with_audit_artifact";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!("{}", render_output(&report, config.json)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    config_path: PathBuf,
    source_stat_path: PathBuf,
    now: DateTime<Utc>,
    json: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ParsedSourceStat {
    path: Option<String>,
    size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct PersistedFreshnessLineageAuditReport {
    config_path: String,
    configured_sqlite_path: String,
    configured_sqlite_path_exists: bool,
    audit_db_path: String,
    audit_db_path_exists: bool,
    audit_db_is_same_path_as_configured_sqlite: bool,
    runtime_db_source_stat_present: bool,
    audit_db_file_size_bytes: u64,
    audit_db_file_mtime_utc: DateTime<Utc>,
    audit_db_page_size: u64,
    audit_db_page_count: u64,
    source_stat_raw_text: Option<String>,
    source_stat_parsed_path: Option<String>,
    source_stat_parsed_size_bytes: Option<u64>,
    source_stat_matches_audit_db_size: Option<bool>,
    source_stat_matches_configured_sqlite_path: Option<bool>,
    observed_swaps_total_rows: u64,
    observed_swaps_min_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_total_rows: u64,
    wallet_activity_days_min_day_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    window_start_utc: DateTime<Utc>,
    observed_swaps_rows_in_window: u64,
    wallet_activity_days_rows_in_window: u64,
    observed_swaps_latest_is_before_window_start: bool,
    wallet_activity_days_latest_is_before_window_start: bool,
    observed_swaps_retention_days: u64,
    retention_cutoff_utc: DateTime<Utc>,
    observed_swaps_latest_is_newer_than_retention_cutoff: bool,
    retention_alone_cannot_explain_gap: bool,
    persisted_freshness_lineage_conclusion: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut db_path: Option<PathBuf> = None;
    let mut config_path: Option<PathBuf> = None;
    let mut source_stat_path: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db_path = Some(PathBuf::from(parse_string_arg("--db", args.next())?)),
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--source-stat" => {
                source_stat_path = Some(PathBuf::from(parse_string_arg(
                    "--source-stat",
                    args.next(),
                )?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db"))?,
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        source_stat_path: source_stat_path
            .ok_or_else(|| anyhow!("missing required --source-stat"))?,
        now: now.ok_or_else(|| anyhow!("missing required --now"))?,
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

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn run(config: &Config) -> Result<PersistedFreshnessLineageAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let configured_sqlite_path = PathBuf::from(loaded_config.sqlite.path.clone());
    let db_metadata = fs::metadata(&config.db_path)
        .with_context(|| format!("failed reading db metadata {}", config.db_path.display()))?;
    let store = SqliteStore::open_read_only_immutable(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let sqlite_facts = store.sqlite_read_only_probe_facts().with_context(|| {
        format!(
            "failed probing sqlite read-only facts {}",
            config.db_path.display()
        )
    })?;
    let observed_swaps = store.observed_swaps_coverage_snapshot().with_context(|| {
        format!(
            "failed loading observed_swaps frontier from {}",
            config.db_path.display()
        )
    })?;
    let wallet_activity_days = store
        .wallet_activity_days_coverage_snapshot()
        .with_context(|| {
            format!(
                "failed loading wallet_activity_days frontier from {}",
                config.db_path.display()
            )
        })?;

    let window_start_utc = metrics_window_start(&loaded_config.discovery, config.now);
    let observed_swaps_rows_in_window = store
        .observed_swaps_row_count_since(window_start_utc)
        .with_context(|| {
            format!(
                "failed counting observed_swaps rows in proof window {}",
                config.db_path.display()
            )
        })?;
    let wallet_activity_days_rows_in_window = store
        .wallet_activity_days_row_count_since(window_start_utc)
        .with_context(|| {
            format!(
                "failed counting wallet_activity_days rows in proof window {}",
                config.db_path.display()
            )
        })?;

    let observed_swaps_max_ts_utc = observed_swaps
        .covered_through_cursor
        .map(|cursor| cursor.ts_utc);
    let source_stat = load_source_stat(&config.source_stat_path)?;
    let source_stat_matches_configured_sqlite_path = source_stat
        .parsed
        .path
        .as_ref()
        .map(|parsed_path| explicit_path_eq(Path::new(parsed_path), &configured_sqlite_path));
    let source_stat_matches_audit_db_size = source_stat
        .parsed
        .size_bytes
        .map(|parsed_size| parsed_size == db_metadata.len());

    let observed_swaps_latest_is_before_window_start =
        observed_swaps_max_ts_utc.is_some_and(|latest| latest < window_start_utc);
    let wallet_activity_days_latest_is_before_window_start = wallet_activity_days
        .covered_through_day_utc
        .is_some_and(|latest| latest < window_start_utc);
    let observed_swaps_retention_days =
        loaded_config.discovery.observed_swaps_retention_days.max(1) as u64;
    let retention_cutoff_utc = config.now - Duration::days(observed_swaps_retention_days as i64);
    let observed_swaps_latest_is_newer_than_retention_cutoff =
        observed_swaps_max_ts_utc.is_some_and(|latest| latest >= retention_cutoff_utc);
    let retention_alone_cannot_explain_gap = observed_swaps_max_ts_utc
        .is_some_and(|latest| latest >= retention_cutoff_utc && latest < config.now);

    let report = PersistedFreshnessLineageAuditReport {
        config_path: config.config_path.display().to_string(),
        configured_sqlite_path: configured_sqlite_path.display().to_string(),
        configured_sqlite_path_exists: configured_sqlite_path.exists(),
        audit_db_path: config.db_path.display().to_string(),
        audit_db_path_exists: config.db_path.exists(),
        audit_db_is_same_path_as_configured_sqlite: explicit_path_eq(
            &configured_sqlite_path,
            &config.db_path,
        ),
        runtime_db_source_stat_present: source_stat.present,
        audit_db_file_size_bytes: db_metadata.len(),
        audit_db_file_mtime_utc: db_metadata
            .modified()
            .map(DateTime::<Utc>::from)
            .with_context(|| {
                format!(
                    "failed reading db modified time {}",
                    config.db_path.display()
                )
            })?,
        audit_db_page_size: sqlite_facts.page_size as u64,
        audit_db_page_count: sqlite_facts.page_count as u64,
        source_stat_raw_text: source_stat.raw_text,
        source_stat_parsed_path: source_stat.parsed.path,
        source_stat_parsed_size_bytes: source_stat.parsed.size_bytes,
        source_stat_matches_audit_db_size,
        source_stat_matches_configured_sqlite_path,
        observed_swaps_total_rows: observed_swaps.row_count as u64,
        observed_swaps_min_ts_utc: observed_swaps.covered_since,
        observed_swaps_max_ts_utc,
        wallet_activity_days_total_rows: wallet_activity_days.row_count,
        wallet_activity_days_min_day_utc: wallet_activity_days.covered_since_day_utc,
        wallet_activity_days_max_day_utc: wallet_activity_days.covered_through_day_utc,
        window_start_utc,
        observed_swaps_rows_in_window,
        wallet_activity_days_rows_in_window,
        observed_swaps_latest_is_before_window_start,
        wallet_activity_days_latest_is_before_window_start,
        observed_swaps_retention_days,
        retention_cutoff_utc,
        observed_swaps_latest_is_newer_than_retention_cutoff,
        retention_alone_cannot_explain_gap,
        persisted_freshness_lineage_conclusion: determine_conclusion(
            explicit_path_eq(&configured_sqlite_path, &config.db_path),
            source_stat.present,
            source_stat_matches_configured_sqlite_path,
            source_stat_matches_audit_db_size,
            observed_swaps_latest_is_before_window_start,
            wallet_activity_days_latest_is_before_window_start,
        )
        .to_string(),
    };

    Ok(report)
}

fn metrics_window_start(config: &DiscoveryConfig, now: DateTime<Utc>) -> DateTime<Utc> {
    let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
}

fn explicit_path_eq(left: &Path, right: &Path) -> bool {
    left == right
}

struct LoadedSourceStat {
    present: bool,
    raw_text: Option<String>,
    parsed: ParsedSourceStat,
}

fn load_source_stat(path: &Path) -> Result<LoadedSourceStat> {
    if !path.exists() {
        return Ok(LoadedSourceStat {
            present: false,
            raw_text: None,
            parsed: ParsedSourceStat::default(),
        });
    }
    let raw =
        fs::read(path).with_context(|| format!("failed reading source stat {}", path.display()))?;
    let raw_text = String::from_utf8_lossy(&raw).into_owned();
    let parsed = parse_source_stat(&raw_text);
    Ok(LoadedSourceStat {
        present: true,
        raw_text: Some(raw_text),
        parsed,
    })
}

fn parse_source_stat(raw: &str) -> ParsedSourceStat {
    ParsedSourceStat {
        path: parse_source_stat_path(raw),
        size_bytes: parse_source_stat_size(raw),
    }
}

fn parse_source_stat_path(raw: &str) -> Option<String> {
    raw.lines()
        .find_map(|line| line.strip_prefix("db_path=").map(str::trim))
        .filter(|value| {
            !value.is_empty() && !value.contains("db_size_bytes=") && !value.contains("db_mtime=")
        })
        .map(ToString::to_string)
}

fn parse_source_stat_size(raw: &str) -> Option<u64> {
    if let Some(from_line) = raw
        .lines()
        .find_map(|line| line.strip_prefix("db_size_bytes=").map(str::trim))
    {
        if from_line.chars().all(|ch| ch.is_ascii_digit()) {
            return from_line.parse().ok();
        }
    }

    let marker = "db_size_bytes=";
    let start = raw.find(marker)? + marker.len();
    let digits: String = raw[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn determine_conclusion(
    audit_matches_configured_path: bool,
    source_stat_present: bool,
    source_stat_matches_configured_path: Option<bool>,
    source_stat_matches_audit_size: Option<bool>,
    observed_swaps_latest_is_before_window_start: bool,
    wallet_activity_days_latest_is_before_window_start: bool,
) -> &'static str {
    if matches!(source_stat_matches_configured_path, Some(false))
        || matches!(source_stat_matches_audit_size, Some(false))
    {
        return CONCLUSION_SOURCE_STAT_CONFLICTS_WITH_AUDIT_ARTIFACT;
    }

    if audit_matches_configured_path {
        if observed_swaps_latest_is_before_window_start
            || wallet_activity_days_latest_is_before_window_start
        {
            return CONCLUSION_ARTIFACT_AND_CONFIGURED_DB_PATH_MATCH_BUT_FRONTIER_IS_STALE;
        }
        return CONCLUSION_INSUFFICIENT_EVIDENCE;
    }

    if source_stat_present {
        CONCLUSION_ARTIFACT_FRONTIER_IS_STALE_BUT_SOURCE_DB_LINEAGE_UNPROVEN
    } else {
        CONCLUSION_CONFIGURED_DB_PATH_DIFFERS_FROM_AUDIT_DB_PATH
    }
}

fn format_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|raw| raw.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_bool(value: Option<bool>) -> String {
    value
        .map(|raw| raw.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_string(value: Option<&String>) -> String {
    value
        .map(|raw| serde_json::to_string(raw).unwrap_or_else(|_| "\"<encode-error>\"".to_string()))
        .unwrap_or_else(|| "null".to_string())
}

fn render_output(report: &PersistedFreshnessLineageAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing persisted freshness lineage audit json");
    }

    Ok(format!(
        concat!(
            "event=discovery_persisted_freshness_lineage_audit\n",
            "config_path={config_path}\n",
            "configured_sqlite_path={configured_sqlite_path}\n",
            "configured_sqlite_path_exists={configured_sqlite_path_exists}\n",
            "audit_db_path={audit_db_path}\n",
            "audit_db_path_exists={audit_db_path_exists}\n",
            "audit_db_is_same_path_as_configured_sqlite={audit_db_is_same_path_as_configured_sqlite}\n",
            "runtime_db_source_stat_present={runtime_db_source_stat_present}\n",
            "audit_db_file_size_bytes={audit_db_file_size_bytes}\n",
            "audit_db_file_mtime_utc={audit_db_file_mtime_utc}\n",
            "audit_db_page_size={audit_db_page_size}\n",
            "audit_db_page_count={audit_db_page_count}\n",
            "source_stat_raw_text={source_stat_raw_text}\n",
            "source_stat_parsed_path={source_stat_parsed_path}\n",
            "source_stat_parsed_size_bytes={source_stat_parsed_size_bytes}\n",
            "source_stat_matches_audit_db_size={source_stat_matches_audit_db_size}\n",
            "source_stat_matches_configured_sqlite_path={source_stat_matches_configured_sqlite_path}\n",
            "observed_swaps_total_rows={observed_swaps_total_rows}\n",
            "observed_swaps_min_ts_utc={observed_swaps_min_ts_utc}\n",
            "observed_swaps_max_ts_utc={observed_swaps_max_ts_utc}\n",
            "wallet_activity_days_total_rows={wallet_activity_days_total_rows}\n",
            "wallet_activity_days_min_day_utc={wallet_activity_days_min_day_utc}\n",
            "wallet_activity_days_max_day_utc={wallet_activity_days_max_day_utc}\n",
            "window_start_utc={window_start_utc}\n",
            "observed_swaps_rows_in_window={observed_swaps_rows_in_window}\n",
            "wallet_activity_days_rows_in_window={wallet_activity_days_rows_in_window}\n",
            "observed_swaps_latest_is_before_window_start={observed_swaps_latest_is_before_window_start}\n",
            "wallet_activity_days_latest_is_before_window_start={wallet_activity_days_latest_is_before_window_start}\n",
            "observed_swaps_retention_days={observed_swaps_retention_days}\n",
            "retention_cutoff_utc={retention_cutoff_utc}\n",
            "observed_swaps_latest_is_newer_than_retention_cutoff={observed_swaps_latest_is_newer_than_retention_cutoff}\n",
            "retention_alone_cannot_explain_gap={retention_alone_cannot_explain_gap}\n",
            "persisted_freshness_lineage_conclusion={persisted_freshness_lineage_conclusion}"
        ),
        config_path = report.config_path,
        configured_sqlite_path = report.configured_sqlite_path,
        configured_sqlite_path_exists = report.configured_sqlite_path_exists,
        audit_db_path = report.audit_db_path,
        audit_db_path_exists = report.audit_db_path_exists,
        audit_db_is_same_path_as_configured_sqlite = report.audit_db_is_same_path_as_configured_sqlite,
        runtime_db_source_stat_present = report.runtime_db_source_stat_present,
        audit_db_file_size_bytes = report.audit_db_file_size_bytes,
        audit_db_file_mtime_utc = report.audit_db_file_mtime_utc.to_rfc3339(),
        audit_db_page_size = report.audit_db_page_size,
        audit_db_page_count = report.audit_db_page_count,
        source_stat_raw_text = format_optional_string(report.source_stat_raw_text.as_ref()),
        source_stat_parsed_path = format_optional_string(report.source_stat_parsed_path.as_ref()),
        source_stat_parsed_size_bytes = format_optional_u64(report.source_stat_parsed_size_bytes),
        source_stat_matches_audit_db_size = format_optional_bool(report.source_stat_matches_audit_db_size),
        source_stat_matches_configured_sqlite_path =
            format_optional_bool(report.source_stat_matches_configured_sqlite_path),
        observed_swaps_total_rows = report.observed_swaps_total_rows,
        observed_swaps_min_ts_utc = format_optional_ts(report.observed_swaps_min_ts_utc),
        observed_swaps_max_ts_utc = format_optional_ts(report.observed_swaps_max_ts_utc),
        wallet_activity_days_total_rows = report.wallet_activity_days_total_rows,
        wallet_activity_days_min_day_utc = format_optional_ts(report.wallet_activity_days_min_day_utc),
        wallet_activity_days_max_day_utc = format_optional_ts(report.wallet_activity_days_max_day_utc),
        window_start_utc = report.window_start_utc.to_rfc3339(),
        observed_swaps_rows_in_window = report.observed_swaps_rows_in_window,
        wallet_activity_days_rows_in_window = report.wallet_activity_days_rows_in_window,
        observed_swaps_latest_is_before_window_start = report.observed_swaps_latest_is_before_window_start,
        wallet_activity_days_latest_is_before_window_start =
            report.wallet_activity_days_latest_is_before_window_start,
        observed_swaps_retention_days = report.observed_swaps_retention_days,
        retention_cutoff_utc = report.retention_cutoff_utc.to_rfc3339(),
        observed_swaps_latest_is_newer_than_retention_cutoff =
            report.observed_swaps_latest_is_newer_than_retention_cutoff,
        retention_alone_cannot_explain_gap = report.retention_alone_cannot_explain_gap,
        persisted_freshness_lineage_conclusion = report.persisted_freshness_lineage_conclusion,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use serde_json::Value;
    use tempfile::TempDir;

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    #[test]
    fn parse_args_reads_required_lineage_audit_flags() -> Result<()> {
        let parsed = parse_args_from([
            "--db".to_string(),
            "/tmp/example.db".to_string(),
            "--config".to_string(),
            "/tmp/example.toml".to_string(),
            "--source-stat".to_string(),
            "/tmp/example.stat".to_string(),
            "--now".to_string(),
            "2026-04-21T16:07:06Z".to_string(),
            "--json".to_string(),
        ])?
        .expect("config should parse");

        assert_eq!(parsed.db_path, PathBuf::from("/tmp/example.db"));
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/example.toml"));
        assert_eq!(parsed.source_stat_path, PathBuf::from("/tmp/example.stat"));
        assert_eq!(parsed.now.to_rfc3339(), "2026-04-21T16:07:06+00:00");
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::StaleSamePath)?;
        let report = run(&fixture.config(true))?;
        let json = render_output(&report, true)?;
        let parsed: Value = serde_json::from_str(&json)?;

        for field in [
            "config_path",
            "configured_sqlite_path",
            "configured_sqlite_path_exists",
            "audit_db_path",
            "audit_db_path_exists",
            "audit_db_is_same_path_as_configured_sqlite",
            "runtime_db_source_stat_present",
            "audit_db_file_size_bytes",
            "audit_db_file_mtime_utc",
            "audit_db_page_size",
            "audit_db_page_count",
            "source_stat_raw_text",
            "source_stat_parsed_path",
            "source_stat_parsed_size_bytes",
            "source_stat_matches_audit_db_size",
            "source_stat_matches_configured_sqlite_path",
            "observed_swaps_total_rows",
            "observed_swaps_min_ts_utc",
            "observed_swaps_max_ts_utc",
            "wallet_activity_days_total_rows",
            "wallet_activity_days_min_day_utc",
            "wallet_activity_days_max_day_utc",
            "window_start_utc",
            "observed_swaps_rows_in_window",
            "wallet_activity_days_rows_in_window",
            "observed_swaps_latest_is_before_window_start",
            "wallet_activity_days_latest_is_before_window_start",
            "observed_swaps_retention_days",
            "retention_cutoff_utc",
            "observed_swaps_latest_is_newer_than_retention_cutoff",
            "retention_alone_cannot_explain_gap",
            "persisted_freshness_lineage_conclusion",
        ] {
            assert!(parsed.get(field).is_some(), "missing json field {field}");
        }
        Ok(())
    }

    #[test]
    fn same_path_stale_frontier_fixture_reports_stale_match_conclusion() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::StaleSamePath)?;
        let report = run(&fixture.config(false))?;

        assert!(report.audit_db_is_same_path_as_configured_sqlite);
        assert_eq!(report.observed_swaps_total_rows, 1);
        assert_eq!(
            report.observed_swaps_max_ts_utc.map(|ts| ts.to_rfc3339()),
            Some("2026-04-13T16:07:06+00:00".to_string())
        );
        assert_eq!(
            report
                .wallet_activity_days_max_day_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-13T00:00:00+00:00".to_string())
        );
        assert_eq!(report.observed_swaps_rows_in_window, 0);
        assert_eq!(report.wallet_activity_days_rows_in_window, 0);
        assert!(report.observed_swaps_latest_is_before_window_start);
        assert!(report.wallet_activity_days_latest_is_before_window_start);
        assert_eq!(
            report.persisted_freshness_lineage_conclusion,
            CONCLUSION_ARTIFACT_AND_CONFIGURED_DB_PATH_MATCH_BUT_FRONTIER_IS_STALE
        );
        Ok(())
    }

    #[test]
    fn differing_configured_path_fixture_reports_path_mismatch_conclusion() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::ConfiguredPathDiffers)?;
        let report = run(&fixture.config(false))?;

        assert!(!report.audit_db_is_same_path_as_configured_sqlite);
        assert!(!report.runtime_db_source_stat_present);
        assert_eq!(
            report.persisted_freshness_lineage_conclusion,
            CONCLUSION_CONFIGURED_DB_PATH_DIFFERS_FROM_AUDIT_DB_PATH
        );
        Ok(())
    }

    #[test]
    fn source_stat_parsing_is_conservative_and_preserves_raw_text() -> Result<()> {
        let parsable = TestFixture::new(FixtureSeed::ParsableSourceStat)?;
        let parsable_report = run(&parsable.config(false))?;
        assert!(parsable_report.runtime_db_source_stat_present);
        assert_eq!(
            parsable_report.source_stat_parsed_path,
            Some(parsable.configured_sqlite_path.display().to_string())
        );
        assert_eq!(
            parsable_report.source_stat_parsed_size_bytes,
            Some(parsable_report.audit_db_file_size_bytes)
        );
        assert_eq!(
            parsable_report.source_stat_matches_audit_db_size,
            Some(true)
        );
        assert_eq!(
            parsable_report.source_stat_matches_configured_sqlite_path,
            Some(true)
        );
        assert!(parsable_report
            .source_stat_raw_text
            .as_deref()
            .unwrap_or_default()
            .contains("db_path="));

        let unparsable = TestFixture::new(FixtureSeed::UnparsableSourceStat)?;
        let unparsable_report = run(&unparsable.config(false))?;
        assert!(unparsable_report.runtime_db_source_stat_present);
        assert!(unparsable_report.source_stat_raw_text.is_some());
        assert_eq!(unparsable_report.source_stat_parsed_path, None);
        assert_eq!(unparsable_report.source_stat_parsed_size_bytes, None);
        assert_eq!(unparsable_report.source_stat_matches_audit_db_size, None);
        assert_eq!(
            unparsable_report.source_stat_matches_configured_sqlite_path,
            None
        );
        Ok(())
    }

    #[test]
    fn repeated_runs_with_fixed_inputs_produce_identical_json() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::ParsableSourceStat)?;
        let first = render_output(&run(&fixture.config(true))?, true)?;
        let second = render_output(&run(&fixture.config(true))?, true)?;

        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn read_only_audit_does_not_create_wal_or_shm_side_files() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::StaleSamePath)?;
        let wal_path = sidecar_path(&fixture.db_path, "-wal");
        let shm_path = sidecar_path(&fixture.db_path, "-shm");
        assert!(!wal_path.exists(), "fixture should start without wal");
        assert!(!shm_path.exists(), "fixture should start without shm");

        let _report = run(&fixture.config(false))?;

        assert!(!wal_path.exists(), "audit path must not create wal");
        assert!(!shm_path.exists(), "audit path must not create shm");
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum FixtureSeed {
        StaleSamePath,
        ConfiguredPathDiffers,
        ParsableSourceStat,
        UnparsableSourceStat,
    }

    struct TestFixture {
        _tempdir: TempDir,
        db_path: PathBuf,
        config_path: PathBuf,
        source_stat_path: PathBuf,
        configured_sqlite_path: PathBuf,
        now: DateTime<Utc>,
    }

    impl TestFixture {
        fn new(seed: FixtureSeed) -> Result<Self> {
            let tempdir = TempDir::new().context("failed creating tempdir")?;
            let db_path = tempdir.path().join("freshness-lineage.sqlite");
            let config_path = tempdir.path().join("freshness-lineage.toml");
            let source_stat_path = tempdir.path().join("runtime_db_source.stat");
            let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
                .map(|ts| ts.with_timezone(&Utc))
                .context("failed parsing fixed fixture now")?;
            create_sqlite_fixture(&db_path, seed, now)?;
            copy_live_config_fixture(&config_path)?;
            let configured_sqlite_path = match seed {
                FixtureSeed::ConfiguredPathDiffers => {
                    tempdir.path().join("configured-runtime-target.sqlite")
                }
                _ => db_path.clone(),
            };
            rewrite_test_config(&config_path, &configured_sqlite_path)?;

            match seed {
                FixtureSeed::ParsableSourceStat => {
                    let metadata = fs::metadata(&db_path)?;
                    fs::write(
                        &source_stat_path,
                        format!(
                            "db_path={}\ndb_size_bytes={}\ndb_mtime=2026-04-21 14:36:47 +0000\n",
                            configured_sqlite_path.display(),
                            metadata.len()
                        ),
                    )?;
                }
                FixtureSeed::UnparsableSourceStat => {
                    fs::write(
                        &source_stat_path,
                        "db_path=/broken/db_size_bytes=not-a-number",
                    )?;
                }
                _ => {}
            }

            Ok(Self {
                _tempdir: tempdir,
                db_path,
                config_path,
                source_stat_path,
                configured_sqlite_path,
                now,
            })
        }

        fn config(&self, json: bool) -> Config {
            Config {
                db_path: self.db_path.clone(),
                config_path: self.config_path.clone(),
                source_stat_path: self.source_stat_path.clone(),
                now: self.now,
                json,
            }
        }
    }

    fn create_sqlite_fixture(path: &Path, seed: FixtureSeed, now: DateTime<Utc>) -> Result<()> {
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(path)
            .with_context(|| format!("failed creating sqlite fixture {}", path.display()))?;
        store.run_migrations(&migration_dir).with_context(|| {
            format!("failed running migrations from {}", migration_dir.display())
        })?;

        match seed {
            FixtureSeed::StaleSamePath => {
                store.insert_observed_swaps_batch_with_activity_days(&[buy_swap(
                    "sig-stale",
                    "wallet-stale",
                    "TokenStale111",
                    now - Duration::days(8),
                    1,
                )])?;
            }
            FixtureSeed::ConfiguredPathDiffers
            | FixtureSeed::ParsableSourceStat
            | FixtureSeed::UnparsableSourceStat => {
                store.insert_observed_swaps_batch_with_activity_days(&[buy_swap(
                    "sig-fresh",
                    "wallet-fresh",
                    "TokenFresh111",
                    now - Duration::hours(1),
                    2,
                )])?;
            }
        }
        Ok(())
    }

    fn copy_live_config_fixture(destination: &Path) -> Result<()> {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/live.toml");
        fs::copy(&source, destination).with_context(|| {
            format!(
                "failed copying config fixture {} -> {}",
                source.display(),
                destination.display()
            )
        })?;
        Ok(())
    }

    fn rewrite_test_config(path: &Path, sqlite_path: &Path) -> Result<()> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed reading {}", path.display()))?;
        let replacement = format!("path = \"{}\"", sqlite_path.display());
        let mut inside_sqlite = false;
        let mut replaced = false;
        let mut updated_lines = Vec::new();
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                inside_sqlite = trimmed == "[sqlite]";
            }
            if inside_sqlite && trimmed.starts_with("path = ") && !replaced {
                updated_lines.push(replacement.clone());
                replaced = true;
                continue;
            }
            updated_lines.push(line.to_string());
        }
        let updated = if raw.ends_with('\n') {
            format!("{}\n", updated_lines.join("\n"))
        } else {
            updated_lines.join("\n")
        };
        if !replaced {
            bail!("failed to rewrite [sqlite].path in {}", path.display());
        }
        fs::write(path, updated)
            .with_context(|| format!("failed writing rewritten config {}", path.display()))?;
        Ok(())
    }

    fn buy_swap(
        signature: &str,
        wallet: &str,
        token_out: &str,
        ts_utc: DateTime<Utc>,
        slot: u64,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: token_out.to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
        PathBuf::from(format!("{}{}", path.display(), suffix))
    }
}
