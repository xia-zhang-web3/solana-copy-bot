use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::operator_status::DiscoveryOperatorStatus;
use copybot_discovery::restore_verdict::{
    DiscoveryRuntimeArtifactFreshnessAssessment, DiscoveryRuntimeRestoreVerdict,
};
use copybot_discovery::DiscoveryService;
use copybot_storage::{
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor, SqliteStore,
    WalletMetricRow, WalletUpsertRow,
};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_runtime_restore --config <path> --artifact <path> [--db-path <path>] [--bootstrap-degraded] [--json] [--now <rfc3339>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("{}", run(config)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    artifact_path: PathBuf,
    db_path: Option<PathBuf>,
    bootstrap_degraded: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct RestoreOutput {
    db_path: String,
    freshness: DiscoveryRuntimeArtifactFreshnessAssessment,
    verdict: DiscoveryRuntimeRestoreVerdict,
    operator_status: DiscoveryOperatorStatus,
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
    let mut artifact_path: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut bootstrap_degraded = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--artifact" => {
                artifact_path = Some(PathBuf::from(parse_string_arg("--artifact", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--bootstrap-degraded" => bootstrap_degraded = true,
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        artifact_path: artifact_path.ok_or_else(|| anyhow!("missing required --artifact"))?,
        db_path,
        bootstrap_degraded,
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

fn resolve_migrations_dir(config_path: &Path, configured_migrations_dir: &str) -> PathBuf {
    let configured = PathBuf::from(configured_migrations_dir);
    if configured.is_absolute() || configured.exists() {
        return configured;
    }
    if let Some(config_parent) = config_path.parent() {
        let sibling_candidate = config_parent.join(&configured);
        if sibling_candidate.exists() {
            return sibling_candidate;
        }
        if let Some(project_root) = config_parent.parent() {
            let root_candidate = project_root.join(&configured);
            if root_candidate.exists() {
                return root_candidate;
            }
        }
    }
    let repo_fallback = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    if repo_fallback.exists() {
        return repo_fallback;
    }
    configured
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let migrations_dir =
        resolve_migrations_dir(&config.config_path, &loaded_config.system.migrations_dir);
    let artifact: DiscoveryRuntimeArtifact = serde_json::from_slice(
        &std::fs::read(&config.artifact_path)
            .with_context(|| format!("failed reading {}", config.artifact_path.display()))?,
    )
    .with_context(|| format!("failed parsing {}", config.artifact_path.display()))?;

    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
    if !freshness.fresh_for_normal_restore() && !config.bootstrap_degraded {
        bail!(
            "runtime artifact is stale for normal restore (fresh_under_export_gate={}, fresh_under_current_gate={}); rerun with --bootstrap-degraded if incident recovery is intentional",
            freshness.fresh_under_export_gate,
            freshness.fresh_under_current_gate
        );
    }
    if config.bootstrap_degraded && loaded_config.execution.enabled {
        bail!("--bootstrap-degraded requires execution.enabled=false");
    }

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    let mut store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    store.run_migrations(&migrations_dir).with_context(|| {
        format!(
            "failed applying migrations from {}",
            migrations_dir.display()
        )
    })?;
    store.restore_discovery_runtime_artifact(&artifact, config.now, config.bootstrap_degraded)?;

    let operator_status = discovery.operator_status(&store, config.now)?;
    let verdict = discovery.runtime_restore_verdict(&store, config.now)?;
    let output = RestoreOutput {
        db_path: db_path.display().to_string(),
        freshness,
        verdict,
        operator_status,
    };
    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing restore output json")
    } else {
        Ok(render_human(
            &config.config_path,
            &config.artifact_path,
            &output,
        ))
    }
}

fn render_human(config_path: &Path, artifact_path: &Path, output: &RestoreOutput) -> String {
    [
        "event=discovery_runtime_restore".to_string(),
        format!("config_path={}", config_path.display()),
        format!("artifact_path={}", artifact_path.display()),
        format!("db_path={}", output.db_path),
        format!("verdict={}", output.verdict.verdict),
        format!(
            "fresh_under_export_gate={}",
            output.freshness.fresh_under_export_gate
        ),
        format!(
            "fresh_under_current_gate={}",
            output.freshness.fresh_under_current_gate
        ),
        format!("runtime_state={}", output.verdict.runtime_state),
        format!("runtime_mode={}", output.verdict.runtime_mode),
        format!("scoring_source={}", output.verdict.scoring_source),
        format!(
            "runtime_cursor_restored={}",
            output.verdict.runtime_cursor_restored
        ),
        format!(
            "bootstrap_degraded_active={}",
            output.verdict.bootstrap_degraded_active
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        export_artifact, make_fixture, parse_ts, seed_runtime_artifact_source, SqliteStore, Value,
    };
    use super::{parse_args_from, run, Config};
    use anyhow::Result;
    use chrono::Duration;
    use std::path::{Path, PathBuf};

    #[test]
    fn parse_args_from_accepts_bootstrap_json_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--artifact".to_string(),
            "artifacts/runtime.json".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--bootstrap-degraded".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");
        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(
            parsed.artifact_path,
            PathBuf::from("artifacts/runtime.json")
        );
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.bootstrap_degraded);
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn run_roundtrips_runtime_artifact_into_fresh_db() -> Result<()> {
        let fixture = make_fixture("runtime-restore-roundtrip")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_runtime_artifact_source(&fixture.source_store, now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-roundtrip.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-roundtrip.db");
        let _ = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(restored_db_path.clone()),
            bootstrap_degraded: false,
            json: false,
            now,
        })?;

        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let publication_state = restored
            .discovery_publication_state()?
            .expect("publication state must be restored");
        assert_eq!(
            publication_state.last_published_at,
            artifact.publication_state.last_published_at
        );
        assert_eq!(
            publication_state.last_published_window_start,
            artifact.publication_state.last_published_window_start
        );
        assert_eq!(
            restored.load_discovery_runtime_cursor()?,
            Some(artifact.runtime_cursor.clone())
        );
        assert_eq!(
            restored
                .load_wallet_metric_snapshots_for_window(
                    artifact
                        .publication_state
                        .last_published_window_start
                        .expect("artifact window")
                )?
                .len(),
            artifact.published_wallet_metrics_snapshot.len()
        );
        Ok(())
    }

    #[test]
    fn run_rejects_stale_artifact_without_bootstrap_degraded() -> Result<()> {
        let fixture = make_fixture("runtime-restore-stale-reject")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_runtime_artifact_source(&fixture.source_store, now - Duration::hours(3))?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-stale.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(fixture.temp.path().join("restored-stale.db")),
            bootstrap_degraded: false,
            json: false,
            now,
        })
        .expect_err("stale artifact must be rejected for normal restore");

        assert!(error
            .to_string()
            .contains("runtime artifact is stale for normal restore"));
        Ok(())
    }

    #[test]
    fn run_restores_stale_artifact_in_bootstrap_degraded_mode() -> Result<()> {
        let fixture = make_fixture("runtime-restore-bootstrap")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_runtime_artifact_source(&fixture.source_store, now - Duration::hours(3))?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-bootstrap.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-bootstrap.db");

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(restored_db_path.clone()),
            bootstrap_degraded: true,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("bootstrap_degraded")
        );
        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let bootstrap_state = restored.discovery_bootstrap_degraded_state()?;
        assert!(bootstrap_state.active);
        let publication_state = restored
            .discovery_publication_state()?
            .expect("publication state must be restored");
        assert_eq!(
            publication_state.last_published_at,
            artifact.publication_state.last_published_at
        );
        assert_eq!(
            publication_state.last_published_window_start,
            artifact.publication_state.last_published_window_start
        );
        Ok(())
    }
}

struct Fixture {
    source_store: SqliteStore,
    config_path: PathBuf,
    temp: tempfile::TempDir,
}

fn make_fixture(name: &str) -> Result<Fixture> {
    let temp = tempdir().context("failed creating tempdir")?;
    let db_path = temp.path().join(format!("{name}.db"));
    let config_path = temp.path().join(format!("{name}.toml"));
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut source_store = SqliteStore::open(&db_path)?;
    source_store.run_migrations(&migration_dir)?;
    std::fs::write(
        &config_path,
        format!(
            "[system]\nmigrations_dir = \"{}\"\n\n[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[execution]\nenabled = false\n",
            migration_dir.display(),
            db_path.display()
        ),
    )?;
    Ok(Fixture {
        source_store,
        config_path,
        temp,
    })
}

fn export_artifact(
    store: &SqliteStore,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    let loaded_config = copybot_config::load_from_path(config_path)?;
    let discovery = DiscoveryService::new(loaded_config.discovery.clone(), loaded_config.shadow);
    store.export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())
}

fn seed_runtime_artifact_source(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
    let published_window_start = now - Duration::days(7);
    store.persist_discovery_cycle(
        &[WalletUpsertRow {
            wallet_id: "wallet-restore".to_string(),
            first_seen: published_window_start - Duration::days(1),
            last_seen: published_window_start + Duration::minutes(5),
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-restore".to_string(),
            window_start: published_window_start,
            pnl: 3.2,
            win_rate: 0.8,
            trades: 10,
            closed_trades: 10,
            hold_median_seconds: 120,
            score: 0.9,
            buy_total: 10,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &["wallet-restore".to_string()],
        true,
        true,
        now,
        "seed_runtime_artifact_source",
    )?;
    store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
        runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
        reason: "raw_window".to_string(),
        last_published_at: Some(now),
        last_published_window_start: Some(published_window_start),
        published_scoring_source: Some("raw_window".to_string()),
        published_wallet_ids: Some(vec!["wallet-restore".to_string()]),
    })?;
    store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now,
        slot: 42,
        signature: "sig-restore".to_string(),
    })?;
    Ok(())
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}
