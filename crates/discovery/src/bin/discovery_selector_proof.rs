use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    DiscoverySelectorProofReport, DiscoverySelectorProofRequest, DiscoveryService,
};
use copybot_storage::SqliteStore;
use std::env;
use std::fs;
use std::path::PathBuf;

const USAGE: &str =
    "usage: discovery_selector_proof --db <path> --config <path> --now <rfc3339> [--json]";

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
    now: DateTime<Utc>,
    json: bool,
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
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db_path = Some(PathBuf::from(parse_string_arg("--db", args.next())?)),
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
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db"))?,
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

fn run(config: &Config) -> Result<DiscoverySelectorProofReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let config_bytes = fs::read(&config.config_path)
        .with_context(|| format!("failed reading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );

    discovery.selector_proof_report(
        &store,
        &DiscoverySelectorProofRequest {
            db_path: config.db_path.clone(),
            config_path: config.config_path.clone(),
            config_bytes,
            fixed_now_utc: config.now,
        },
    )
}

fn render_output(report: &DiscoverySelectorProofReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing selector proof json");
    }

    Ok(format!(
        concat!(
            "event=discovery_selector_proof\n",
            "db_path={db_path}\n",
            "db_file_size_bytes={db_file_size_bytes}\n",
            "db_file_mtime_utc={db_file_mtime_utc}\n",
            "db_page_size={db_page_size}\n",
            "db_page_count={db_page_count}\n",
            "db_read_only_open_confirmed={db_read_only_open_confirmed}\n",
            "config_path={config_path}\n",
            "config_fingerprint_method={config_fingerprint_method}\n",
            "config_fingerprint={config_fingerprint}\n",
            "fixed_now_utc={fixed_now_utc}\n",
            "rpc_enabled={rpc_enabled}\n",
            "metrics_window_start_utc={metrics_window_start_utc}\n",
            "observed_swaps_loaded={observed_swaps_loaded}\n",
            "wallets_seen={wallets_seen}\n",
            "eligible_wallet_count={eligible_wallet_count}\n",
            "ranked_wallets={ranked_wallets:?}\n",
            "reject_breakdown={reject_breakdown:?}\n",
            "token_quality_coverage={token_quality_coverage:?}"
        ),
        db_path = report.db_path.as_str(),
        db_file_size_bytes = report.db_file_size_bytes,
        db_file_mtime_utc = report.db_file_mtime_utc.to_rfc3339(),
        db_page_size = report.db_page_size,
        db_page_count = report.db_page_count,
        db_read_only_open_confirmed = report.db_read_only_open_confirmed,
        config_path = report.config_path.as_str(),
        config_fingerprint_method = report.config_fingerprint_method.as_str(),
        config_fingerprint = report.config_fingerprint.as_str(),
        fixed_now_utc = report.fixed_now_utc.to_rfc3339(),
        rpc_enabled = report.rpc_enabled,
        metrics_window_start_utc = report
            .metrics_window_start_utc
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "null".to_string()),
        observed_swaps_loaded = report
            .observed_swaps_loaded
            .map(|count| count.to_string())
            .unwrap_or_else(|| "null".to_string()),
        wallets_seen = report
            .wallets_seen
            .map(|count| count.to_string())
            .unwrap_or_else(|| "null".to_string()),
        eligible_wallet_count = report
            .eligible_wallet_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "null".to_string()),
        ranked_wallets = &report.ranked_wallets,
        reject_breakdown = &report.reject_breakdown,
        token_quality_coverage = &report.token_quality_coverage,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_discovery::DISCOVERY_SELECTOR_PROOF_CONFIG_FINGERPRINT_METHOD;
    use rusqlite::Connection;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn parse_args_reads_required_selector_proof_flags() -> Result<()> {
        let parsed = parse_args_from([
            "--db".to_string(),
            "/tmp/example.db".to_string(),
            "--config".to_string(),
            "/tmp/example.toml".to_string(),
            "--now".to_string(),
            "2026-04-21T16:07:06Z".to_string(),
            "--json".to_string(),
        ])?
        .expect("config should parse");

        assert_eq!(parsed.db_path, PathBuf::from("/tmp/example.db"));
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/example.toml"));
        assert_eq!(parsed.now.to_rfc3339(), "2026-04-21T16:07:06+00:00");
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn proof_report_returns_identity_fields_on_temp_sqlite_fixture() -> Result<()> {
        let fixture = TestFixture::new()?;
        let report = run(&fixture.config(false))?;

        assert_eq!(report.db_path, fixture.db_path.display().to_string());
        assert_eq!(report.config_path, fixture.config_path.display().to_string());
        assert_eq!(
            report.config_fingerprint_method,
            DISCOVERY_SELECTOR_PROOF_CONFIG_FINGERPRINT_METHOD
        );
        assert_eq!(report.fixed_now_utc, fixture.now);
        assert!(!report.rpc_enabled);
        assert!(report.db_file_size_bytes > 0);
        assert!(report.db_page_size > 0);
        assert!(report.db_page_count > 0);
        assert!(report.db_read_only_open_confirmed);
        assert!(report.metrics_window_start_utc.is_none());
        assert!(report.observed_swaps_loaded.is_none());
        assert!(report.wallets_seen.is_none());
        assert!(report.eligible_wallet_count.is_none());
        assert!(report.ranked_wallets.is_empty());
        assert_eq!(report.reject_breakdown, BTreeMap::new());
        assert_eq!(report.token_quality_coverage, BTreeMap::new());
        Ok(())
    }

    #[test]
    fn repeated_runs_with_fixed_now_produce_identical_report_output() -> Result<()> {
        let fixture = TestFixture::new()?;
        let first = run(&fixture.config(true))?;
        let second = run(&fixture.config(true))?;

        assert_eq!(first, second);
        assert_eq!(
            render_output(&first, true)?,
            render_output(&second, true)?,
            "json output must remain stable for the same db/config/now inputs"
        );
        Ok(())
    }

    #[test]
    fn proof_path_confirms_read_only_open() -> Result<()> {
        let fixture = TestFixture::new()?;
        let report = run(&fixture.config(false))?;

        assert!(report.db_read_only_open_confirmed);
        Ok(())
    }

    #[test]
    fn proof_path_does_not_create_wal_or_shm_side_files_on_fixture() -> Result<()> {
        let fixture = TestFixture::new()?;
        let wal_path = sidecar_path(&fixture.db_path, "-wal");
        let shm_path = sidecar_path(&fixture.db_path, "-shm");
        assert!(!wal_path.exists(), "fixture should start without a wal file");
        assert!(!shm_path.exists(), "fixture should start without a shm file");

        let _report = run(&fixture.config(false))?;

        assert!(!wal_path.exists(), "proof path must not create a wal file");
        assert!(!shm_path.exists(), "proof path must not create a shm file");
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_batch1_fields() -> Result<()> {
        let fixture = TestFixture::new()?;
        let report = run(&fixture.config(true))?;
        let json = render_output(&report, true)?;
        let parsed: Value = serde_json::from_str(&json)?;

        for field in [
            "db_path",
            "db_file_size_bytes",
            "db_file_mtime_utc",
            "db_page_size",
            "db_page_count",
            "db_read_only_open_confirmed",
            "config_path",
            "config_fingerprint_method",
            "config_fingerprint",
            "fixed_now_utc",
            "rpc_enabled",
            "metrics_window_start_utc",
            "observed_swaps_loaded",
            "wallets_seen",
            "eligible_wallet_count",
            "ranked_wallets",
            "reject_breakdown",
            "token_quality_coverage",
        ] {
            assert!(parsed.get(field).is_some(), "missing json field {field}");
        }
        Ok(())
    }

    struct TestFixture {
        _tempdir: TempDir,
        db_path: PathBuf,
        config_path: PathBuf,
        now: DateTime<Utc>,
    }

    impl TestFixture {
        fn new() -> Result<Self> {
            let tempdir = TempDir::new().context("failed creating tempdir")?;
            let db_path = tempdir.path().join("selector-proof.sqlite");
            let config_path = tempdir.path().join("selector-proof.toml");
            create_sqlite_fixture(&db_path)?;
            copy_live_config_fixture(&config_path)?;
            Ok(Self {
                _tempdir: tempdir,
                db_path,
                config_path,
                now: DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
                    .map(|ts| ts.with_timezone(&Utc))
                    .context("failed parsing fixed fixture now")?,
            })
        }

        fn config(&self, json: bool) -> Config {
            Config {
                db_path: self.db_path.clone(),
                config_path: self.config_path.clone(),
                now: self.now,
                json,
            }
        }
    }

    fn create_sqlite_fixture(path: &Path) -> Result<()> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed creating sqlite fixture {}", path.display()))?;
        conn.pragma_update(None, "page_size", 4096)
            .context("failed setting fixture page_size")?;
        conn.pragma_update(None, "journal_mode", "DELETE")
            .context("failed setting fixture journal_mode")?;
        conn.execute_batch(
            "
            VACUUM;
            CREATE TABLE observed_swaps (
                id INTEGER PRIMARY KEY,
                wallet TEXT NOT NULL,
                observed_at TEXT NOT NULL
            );
            INSERT INTO observed_swaps (wallet, observed_at)
            VALUES ('wallet-1', '2026-04-21T16:07:06Z');
            ",
        )
        .context("failed seeding sqlite fixture")?;
        drop(conn);
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

    fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
        PathBuf::from(format!("{}{}", path.display(), suffix))
    }
}
