use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_storage::SqliteStore;
use serde::Serialize;
use std::env;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;

const USAGE: &str =
    "usage: copybot_runtime_writer_commit_path_audit --config <path> --now <rfc3339> [--json]";

const WRITER_WRITE_TARGET_PATH_SOURCE_CONFIG_SQLITE_PATH: &str = "config.sqlite.path";
const WRITER_WRITE_TARGET_PATH_SOURCE_OTHER: &str = "other";
const WRITER_WRITE_TARGET_PATH_SOURCE_UNKNOWN: &str = "unknown";

const CONCLUSION_CONFIGURED_DB_PATH_NOT_REACHED_BY_WRITER: &str =
    "configured_db_path_not_reached_by_writer";
const CONCLUSION_WRITER_REACHES_CONFIGURED_DB_BUT_COMMIT_PATH_STALLS_OR_ERRORS: &str =
    "writer_reaches_configured_db_but_commit_path_stalls_or_errors";
const CONCLUSION_WRITER_COMMITS_ELSEWHERE_THAN_CONFIGURED_DB: &str =
    "writer_commits_elsewhere_than_configured_db";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const OBSERVED_SWAP_WRITER_ENTRYPOINT: &str =
    "copybot_app::observed_swap_writer::ObservedSwapWriter::start_with_recent_raw_journal";
const OBSERVED_SWAP_BATCH_INSERT_ENTRYPOINT: &str =
    "copybot_storage::market_data::SqliteStore::insert_observed_swaps_batch_with_activity_days_measured";
const WRITE_BATCH_TRANSACTIONAL_SURFACE: &str =
    "copybot_storage::market_data::SqliteStore::insert_observed_swaps_batch_with_activity_days_measured";

const MAIN_SOURCE: &str = include_str!("../main.rs");
const OBSERVED_SWAP_WRITER_SOURCE: &str = include_str!("../observed_swap_writer.rs");
const STORAGE_MARKET_DATA_SOURCE: &str = include_str!("../../../storage/src/market_data.rs");

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
    config_path: PathBuf,
    now: DateTime<Utc>,
    json: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RuntimeWriterCommitPathAuditReport {
    config_path: String,
    configured_sqlite_path: String,
    configured_sqlite_path_exists: bool,
    observed_swap_writer_enabled: bool,
    resolved_recent_raw_journal_path: Option<String>,
    writer_constructs_sqlite_store_from_configured_path: bool,
    writer_write_target_path_source: String,
    observed_swap_writer_entrypoint: String,
    observed_swap_batch_insert_entrypoint: String,
    wallet_activity_days_upsert_same_batch_path: bool,
    retention_delete_same_path_as_insert: bool,
    write_batch_transactional_surface: String,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    observed_swaps_total_rows: u64,
    wallet_activity_days_total_rows: u64,
    runtime_writer_commit_path_conclusion: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RuntimeWriterCommitPathEvidence<'a> {
    writer_constructs_sqlite_store_from_configured_path: bool,
    writer_write_target_path_source: &'a str,
    frontier_is_stale: bool,
    wallet_activity_days_upsert_same_batch_path: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CurrentCodePathFacts {
    observed_swap_writer_enabled: bool,
    writer_constructs_sqlite_store_from_configured_path: bool,
    writer_write_target_path_source: &'static str,
    wallet_activity_days_upsert_same_batch_path: bool,
    retention_delete_same_path_as_insert: bool,
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
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
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

fn run(config: &Config) -> Result<RuntimeWriterCommitPathAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let configured_sqlite_path = PathBuf::from(loaded_config.sqlite.path.clone());
    let configured_sqlite_path_exists = configured_sqlite_path.exists();
    let current_code_facts = current_code_path_facts();

    let (
        observed_swaps_max_ts_utc,
        observed_swaps_total_rows,
        wallet_activity_days_max_day_utc,
        wallet_activity_days_total_rows,
    ) = if configured_sqlite_path_exists {
        let store = SqliteStore::open_read_only(&configured_sqlite_path).with_context(|| {
            format!(
                "failed opening configured sqlite db read-only: {}",
                configured_sqlite_path.display()
            )
        })?;
        let observed_swaps = store.observed_swaps_coverage_snapshot().with_context(|| {
            format!(
                "failed loading observed_swaps frontier from configured sqlite db {}",
                configured_sqlite_path.display()
            )
        })?;
        let wallet_activity_days = store
            .wallet_activity_days_coverage_snapshot()
            .with_context(|| {
                format!(
                    "failed loading wallet_activity_days frontier from configured sqlite db {}",
                    configured_sqlite_path.display()
                )
            })?;
        (
            observed_swaps
                .covered_through_cursor
                .map(|cursor| cursor.ts_utc),
            observed_swaps.row_count as u64,
            wallet_activity_days.covered_through_day_utc,
            wallet_activity_days.row_count,
        )
    } else {
        (None, 0, None, 0)
    };

    let frontier_is_stale = configured_frontier_is_stale(
        config.now,
        observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc,
    );
    let conclusion =
        select_runtime_writer_commit_path_conclusion(RuntimeWriterCommitPathEvidence {
            writer_constructs_sqlite_store_from_configured_path: current_code_facts
                .writer_constructs_sqlite_store_from_configured_path,
            writer_write_target_path_source: current_code_facts.writer_write_target_path_source,
            frontier_is_stale,
            wallet_activity_days_upsert_same_batch_path: current_code_facts
                .wallet_activity_days_upsert_same_batch_path,
        })
        .to_string();

    Ok(RuntimeWriterCommitPathAuditReport {
        config_path: config.config_path.display().to_string(),
        configured_sqlite_path: configured_sqlite_path.display().to_string(),
        configured_sqlite_path_exists,
        observed_swap_writer_enabled: current_code_facts.observed_swap_writer_enabled,
        resolved_recent_raw_journal_path: Some(loaded_config.recent_raw_journal.path.clone()),
        writer_constructs_sqlite_store_from_configured_path: current_code_facts
            .writer_constructs_sqlite_store_from_configured_path,
        writer_write_target_path_source: current_code_facts
            .writer_write_target_path_source
            .to_string(),
        observed_swap_writer_entrypoint: OBSERVED_SWAP_WRITER_ENTRYPOINT.to_string(),
        observed_swap_batch_insert_entrypoint: OBSERVED_SWAP_BATCH_INSERT_ENTRYPOINT.to_string(),
        wallet_activity_days_upsert_same_batch_path: current_code_facts
            .wallet_activity_days_upsert_same_batch_path,
        retention_delete_same_path_as_insert: current_code_facts
            .retention_delete_same_path_as_insert,
        write_batch_transactional_surface: WRITE_BATCH_TRANSACTIONAL_SURFACE.to_string(),
        observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc,
        observed_swaps_total_rows,
        wallet_activity_days_total_rows,
        runtime_writer_commit_path_conclusion: conclusion,
    })
}

fn current_code_path_facts() -> CurrentCodePathFacts {
    let observed_swap_writer_enabled = MAIN_SOURCE
        .contains("let observed_swap_writer = ObservedSwapWriter::start_with_recent_raw_journal(");
    let writer_constructs_sqlite_store_from_configured_path = MAIN_SOURCE
        .contains("config.sqlite.path.clone(),")
        && MAIN_SOURCE.contains("ObservedSwapWriter::start_with_recent_raw_journal(")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("Self::start_with_config(")
        && OBSERVED_SWAP_WRITER_SOURCE
            .contains("let store = SqliteStore::open(Path::new(&sqlite_path))");
    let writer_write_target_path_source = if MAIN_SOURCE.contains("config.sqlite.path.clone(),")
        && MAIN_SOURCE.contains("ObservedSwapWriter::start_with_recent_raw_journal(")
    {
        WRITER_WRITE_TARGET_PATH_SOURCE_CONFIG_SQLITE_PATH
    } else {
        WRITER_WRITE_TARGET_PATH_SOURCE_UNKNOWN
    };
    let wallet_activity_days_upsert_same_batch_path = STORAGE_MARKET_DATA_SOURCE
        .contains("pub fn insert_observed_swaps_batch_with_activity_days_measured(")
        && STORAGE_MARKET_DATA_SOURCE
            .contains("upsert_wallet_activity_days_on_conn(conn, &activity_rows)?;");
    let retention_delete_same_path_as_insert = MAIN_SOURCE
        .contains("run_observed_swap_retention_maintenance_once(")
        && MAIN_SOURCE.contains("&sqlite_path,")
        && OBSERVED_SWAP_WRITER_SOURCE
            .contains("let store = SqliteStore::open(Path::new(sqlite_path)).with_context(||")
        && OBSERVED_SWAP_WRITER_SOURCE.contains(".delete_observed_swaps_before_batch(");
    CurrentCodePathFacts {
        observed_swap_writer_enabled,
        writer_constructs_sqlite_store_from_configured_path,
        writer_write_target_path_source,
        wallet_activity_days_upsert_same_batch_path,
        retention_delete_same_path_as_insert,
    }
}

fn configured_frontier_is_stale(
    now: DateTime<Utc>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
) -> bool {
    let stale_cutoff = now - Duration::hours(24);
    let observed_swaps_stale =
        observed_swaps_max_ts_utc.is_some_and(|latest_ts| latest_ts < stale_cutoff);
    let wallet_activity_days_stale = wallet_activity_days_max_day_utc
        .is_some_and(|latest_day| latest_day.date_naive() < stale_cutoff.date_naive());
    observed_swaps_stale || wallet_activity_days_stale
}

fn select_runtime_writer_commit_path_conclusion(
    evidence: RuntimeWriterCommitPathEvidence<'_>,
) -> &'static str {
    if evidence.writer_write_target_path_source == WRITER_WRITE_TARGET_PATH_SOURCE_OTHER {
        return CONCLUSION_WRITER_COMMITS_ELSEWHERE_THAN_CONFIGURED_DB;
    }
    if !evidence.writer_constructs_sqlite_store_from_configured_path {
        return CONCLUSION_CONFIGURED_DB_PATH_NOT_REACHED_BY_WRITER;
    }
    if evidence.writer_constructs_sqlite_store_from_configured_path
        && evidence.frontier_is_stale
        && evidence.wallet_activity_days_upsert_same_batch_path
    {
        return CONCLUSION_WRITER_REACHES_CONFIGURED_DB_BUT_COMMIT_PATH_STALLS_OR_ERRORS;
    }
    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn format_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_string(value: Option<&String>) -> String {
    value
        .map(|raw| serde_json::to_string(raw).unwrap_or_else(|_| "\"<encode-error>\"".to_string()))
        .unwrap_or_else(|| "null".to_string())
}

fn render_output(report: &RuntimeWriterCommitPathAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing runtime writer commit-path audit json");
    }

    Ok(format!(
        concat!(
            "event=copybot_runtime_writer_commit_path_audit\n",
            "config_path={config_path}\n",
            "configured_sqlite_path={configured_sqlite_path}\n",
            "configured_sqlite_path_exists={configured_sqlite_path_exists}\n",
            "observed_swap_writer_enabled={observed_swap_writer_enabled}\n",
            "resolved_recent_raw_journal_path={resolved_recent_raw_journal_path}\n",
            "writer_constructs_sqlite_store_from_configured_path={writer_constructs_sqlite_store_from_configured_path}\n",
            "writer_write_target_path_source={writer_write_target_path_source}\n",
            "observed_swap_writer_entrypoint={observed_swap_writer_entrypoint}\n",
            "observed_swap_batch_insert_entrypoint={observed_swap_batch_insert_entrypoint}\n",
            "wallet_activity_days_upsert_same_batch_path={wallet_activity_days_upsert_same_batch_path}\n",
            "retention_delete_same_path_as_insert={retention_delete_same_path_as_insert}\n",
            "write_batch_transactional_surface={write_batch_transactional_surface}\n",
            "observed_swaps_max_ts_utc={observed_swaps_max_ts_utc}\n",
            "wallet_activity_days_max_day_utc={wallet_activity_days_max_day_utc}\n",
            "observed_swaps_total_rows={observed_swaps_total_rows}\n",
            "wallet_activity_days_total_rows={wallet_activity_days_total_rows}\n",
            "runtime_writer_commit_path_conclusion={runtime_writer_commit_path_conclusion}"
        ),
        config_path = report.config_path,
        configured_sqlite_path = report.configured_sqlite_path,
        configured_sqlite_path_exists = report.configured_sqlite_path_exists,
        observed_swap_writer_enabled = report.observed_swap_writer_enabled,
        resolved_recent_raw_journal_path =
            format_optional_string(report.resolved_recent_raw_journal_path.as_ref()),
        writer_constructs_sqlite_store_from_configured_path =
            report.writer_constructs_sqlite_store_from_configured_path,
        writer_write_target_path_source = report.writer_write_target_path_source,
        observed_swap_writer_entrypoint = report.observed_swap_writer_entrypoint,
        observed_swap_batch_insert_entrypoint = report.observed_swap_batch_insert_entrypoint,
        wallet_activity_days_upsert_same_batch_path = report.wallet_activity_days_upsert_same_batch_path,
        retention_delete_same_path_as_insert = report.retention_delete_same_path_as_insert,
        write_batch_transactional_surface = report.write_batch_transactional_surface,
        observed_swaps_max_ts_utc = format_optional_ts(report.observed_swaps_max_ts_utc),
        wallet_activity_days_max_day_utc = format_optional_ts(report.wallet_activity_days_max_day_utc),
        observed_swaps_total_rows = report.observed_swaps_total_rows,
        wallet_activity_days_total_rows = report.wallet_activity_days_total_rows,
        runtime_writer_commit_path_conclusion = report.runtime_writer_commit_path_conclusion,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use copybot_core_types::SwapEvent;

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    #[test]
    fn parse_args_reads_required_runtime_writer_commit_path_audit_flags() -> Result<()> {
        let parsed = parse_args_from([
            "--config".to_string(),
            "/tmp/example.toml".to_string(),
            "--now".to_string(),
            "2026-04-21T16:07:06Z".to_string(),
            "--json".to_string(),
        ])?
        .expect("config should parse");

        assert_eq!(parsed.config_path, PathBuf::from("/tmp/example.toml"));
        assert_eq!(parsed.now.to_rfc3339(), "2026-04-21T16:07:06+00:00");
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::StaleConfiguredPath)?;
        let report = run(&fixture.config(true))?;
        let json = render_output(&report, true)?;
        let parsed: serde_json::Value = serde_json::from_str(&json)?;

        for field in [
            "config_path",
            "configured_sqlite_path",
            "configured_sqlite_path_exists",
            "observed_swap_writer_enabled",
            "resolved_recent_raw_journal_path",
            "writer_constructs_sqlite_store_from_configured_path",
            "writer_write_target_path_source",
            "observed_swap_writer_entrypoint",
            "observed_swap_batch_insert_entrypoint",
            "wallet_activity_days_upsert_same_batch_path",
            "retention_delete_same_path_as_insert",
            "write_batch_transactional_surface",
            "observed_swaps_max_ts_utc",
            "wallet_activity_days_max_day_utc",
            "observed_swaps_total_rows",
            "wallet_activity_days_total_rows",
            "runtime_writer_commit_path_conclusion",
        ] {
            assert!(parsed.get(field).is_some(), "missing json field {field}");
        }
        Ok(())
    }

    #[test]
    fn stale_configured_db_path_yields_commit_path_stalls_or_errors_conclusion() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::StaleConfiguredPath)?;
        let report = run(&fixture.config(false))?;

        assert!(report.configured_sqlite_path_exists);
        assert!(report.observed_swap_writer_enabled);
        assert!(report.writer_constructs_sqlite_store_from_configured_path);
        assert_eq!(
            report.writer_write_target_path_source,
            WRITER_WRITE_TARGET_PATH_SOURCE_CONFIG_SQLITE_PATH
        );
        assert!(report.wallet_activity_days_upsert_same_batch_path);
        assert!(report.retention_delete_same_path_as_insert);
        assert_eq!(
            report.runtime_writer_commit_path_conclusion,
            CONCLUSION_WRITER_REACHES_CONFIGURED_DB_BUT_COMMIT_PATH_STALLS_OR_ERRORS
        );
        Ok(())
    }

    #[test]
    fn conclusion_selection_reports_writer_commits_elsewhere_when_target_source_is_other() {
        let conclusion =
            select_runtime_writer_commit_path_conclusion(RuntimeWriterCommitPathEvidence {
                writer_constructs_sqlite_store_from_configured_path: false,
                writer_write_target_path_source: WRITER_WRITE_TARGET_PATH_SOURCE_OTHER,
                frontier_is_stale: true,
                wallet_activity_days_upsert_same_batch_path: false,
            });

        assert_eq!(
            conclusion,
            CONCLUSION_WRITER_COMMITS_ELSEWHERE_THAN_CONFIGURED_DB
        );
    }

    #[test]
    fn conclusion_selection_reports_insufficient_evidence_when_frontier_is_not_stale() {
        let conclusion =
            select_runtime_writer_commit_path_conclusion(RuntimeWriterCommitPathEvidence {
                writer_constructs_sqlite_store_from_configured_path: true,
                writer_write_target_path_source: WRITER_WRITE_TARGET_PATH_SOURCE_CONFIG_SQLITE_PATH,
                frontier_is_stale: false,
                wallet_activity_days_upsert_same_batch_path: true,
            });

        assert_eq!(conclusion, CONCLUSION_INSUFFICIENT_EVIDENCE);
    }

    #[test]
    fn helper_rendering_matches_current_runtime_writer_code_paths() {
        let facts = current_code_path_facts();

        assert!(facts.observed_swap_writer_enabled);
        assert!(facts.writer_constructs_sqlite_store_from_configured_path);
        assert_eq!(
            facts.writer_write_target_path_source,
            WRITER_WRITE_TARGET_PATH_SOURCE_CONFIG_SQLITE_PATH
        );
        assert!(facts.wallet_activity_days_upsert_same_batch_path);
        assert!(facts.retention_delete_same_path_as_insert);
        assert_eq!(
            OBSERVED_SWAP_WRITER_ENTRYPOINT,
            "copybot_app::observed_swap_writer::ObservedSwapWriter::start_with_recent_raw_journal"
        );
        assert_eq!(
            OBSERVED_SWAP_BATCH_INSERT_ENTRYPOINT,
            "copybot_storage::market_data::SqliteStore::insert_observed_swaps_batch_with_activity_days_measured"
        );
    }

    #[test]
    fn repeated_runs_with_fixed_inputs_produce_identical_json() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::StaleConfiguredPath)?;
        let first = render_output(&run(&fixture.config(true))?, true)?;
        let second = render_output(&run(&fixture.config(true))?, true)?;

        assert_eq!(first, second);
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum FixtureSeed {
        StaleConfiguredPath,
    }

    struct TestFixture {
        temp_dir: PathBuf,
        config_path: PathBuf,
        _db_path: PathBuf,
        now: DateTime<Utc>,
    }

    impl Drop for TestFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.temp_dir);
        }
    }

    impl TestFixture {
        fn new(seed: FixtureSeed) -> Result<Self> {
            let temp_dir = temp_dir("runtime_writer_commit_path_audit");
            fs::create_dir_all(&temp_dir)
                .with_context(|| format!("failed creating {}", temp_dir.display()))?;
            let config_path = temp_dir.join("runtime-writer-audit.toml");
            let db_path = temp_dir.join("runtime-writer-audit.db");
            let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
                .map(|ts| ts.with_timezone(&Utc))
                .context("failed parsing fixed fixture now")?;

            create_sqlite_fixture(&db_path, seed, now)?;
            copy_live_config_fixture(&config_path)?;
            rewrite_test_config(&config_path, &db_path)?;

            Ok(Self {
                temp_dir,
                config_path,
                _db_path: db_path,
                now,
            })
        }

        fn config(&self, json: bool) -> Config {
            Config {
                config_path: self.config_path.clone(),
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
            FixtureSeed::StaleConfiguredPath => {
                store.insert_observed_swaps_batch_with_activity_days(&[buy_swap(
                    "sig-stale",
                    "wallet-stale",
                    "TokenStale111",
                    now - Duration::days(4),
                    1,
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

    fn temp_dir(label: &str) -> PathBuf {
        let unique = format!(
            "{}_{}_{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        );
        env::temp_dir().join(unique)
    }
}
