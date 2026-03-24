use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
#[cfg(test)]
use copybot_core_types::SwapEvent;
use copybot_discovery::operator_status::DiscoveryOperatorStatus;
use copybot_discovery::restore_verdict::{
    DiscoveryRuntimeArtifactFreshnessAssessment, DiscoveryRuntimeRestoreVerdict,
};
use copybot_discovery::DiscoveryService;
#[cfg(test)]
use copybot_storage::{DiscoveryPublicationStateUpdate, WalletMetricRow, WalletUpsertRow};
use copybot_storage::{
    DiscoveryRecentRawRestoreStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    RecentRawJournalReplaySummary, SqliteStore,
};
use serde::Serialize;
#[cfg(test)]
use serde_json::Value;
use std::env;
use std::path::{Path, PathBuf};
#[cfg(test)]
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_runtime_restore --config <path> --artifact <path> [--db-path <path>] [--journal-db-path <path>] [--gap-fill-db-path <path>] [--bootstrap-degraded] [--json] [--now <rfc3339>]";

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
    journal_db_path: Option<PathBuf>,
    gap_fill_db_path: Option<PathBuf>,
    bootstrap_degraded: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct RestoreOutput {
    db_path: String,
    journal_db_path: String,
    gap_fill_db_path: Option<String>,
    freshness: DiscoveryRuntimeArtifactFreshnessAssessment,
    replay: RestoreReplaySummary,
    verdict: DiscoveryRuntimeRestoreVerdict,
    operator_status: DiscoveryOperatorStatus,
}

#[derive(Debug, Clone, Serialize)]
struct RestoreReplaySummary {
    required_window_start: DateTime<Utc>,
    artifact_runtime_cursor: DiscoveryRuntimeCursor,
    journal_available: bool,
    journal_covered_since: Option<DateTime<Utc>>,
    journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    journal_covers_artifact_cursor: bool,
    gap_fill_replayed: bool,
    gap_fill_covered_since: Option<DateTime<Utc>>,
    gap_fill_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    effective_covered_since: Option<DateTime<Utc>>,
    effective_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    gap_fill_replayed_rows: usize,
    replayed_rows: usize,
    raw_coverage_satisfied: bool,
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
    let mut journal_db_path: Option<PathBuf> = None;
    let mut gap_fill_db_path: Option<PathBuf> = None;
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
            "--journal-db-path" => {
                journal_db_path = Some(PathBuf::from(parse_string_arg(
                    "--journal-db-path",
                    args.next(),
                )?))
            }
            "--gap-fill-db-path" => {
                gap_fill_db_path = Some(PathBuf::from(parse_string_arg(
                    "--gap-fill-db-path",
                    args.next(),
                )?))
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
        journal_db_path,
        gap_fill_db_path,
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
    let journal_db_path = resolve_db_path(
        &config.config_path,
        config.journal_db_path.as_deref(),
        &loaded_config.recent_raw_journal.path,
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
    let replay = replay_recent_raw_journal(
        &store,
        &journal_db_path,
        config.gap_fill_db_path.as_deref(),
        config.now,
        i64::from(loaded_config.discovery.scoring_window_days.max(1)),
        loaded_config.recent_raw_journal.replay_batch_size.max(1),
        &artifact.runtime_cursor,
    )?;

    let operator_status = discovery.operator_status(&store, config.now)?;
    let verdict = discovery.runtime_restore_verdict(&store, config.now)?;
    let output = RestoreOutput {
        db_path: db_path.display().to_string(),
        journal_db_path: journal_db_path.display().to_string(),
        gap_fill_db_path: config
            .gap_fill_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        freshness,
        replay,
        verdict,
        operator_status,
    };
    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing restore output json")
    } else {
        Ok(render_human(
            &config.config_path,
            &config.artifact_path,
            &journal_db_path,
            &output,
        ))
    }
}

fn render_human(
    config_path: &Path,
    artifact_path: &Path,
    journal_db_path: &Path,
    output: &RestoreOutput,
) -> String {
    let gap_fill_db_path = output.gap_fill_db_path.as_deref().unwrap_or("null");
    [
        "event=discovery_runtime_restore".to_string(),
        format!("config_path={}", config_path.display()),
        format!("artifact_path={}", artifact_path.display()),
        format!("db_path={}", output.db_path),
        format!("journal_db_path={}", journal_db_path.display()),
        format!("gap_fill_db_path={gap_fill_db_path}"),
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
        format!("journal_available={}", output.verdict.journal_available),
        format!("journal_replayed={}", output.verdict.journal_replayed),
        format!(
            "journal_covers_artifact_cursor={}",
            output.verdict.journal_covers_artifact_cursor
        ),
        format!(
            "raw_coverage_satisfied={}",
            output.verdict.raw_coverage_satisfied
        ),
        format!(
            "journal_replayed_rows={}",
            output.verdict.journal_replayed_rows
        ),
        format!("gap_fill_replayed={}", output.replay.gap_fill_replayed),
        format!(
            "gap_fill_replayed_rows={}",
            output.replay.gap_fill_replayed_rows
        ),
        format!(
            "bootstrap_degraded_active={}",
            output.verdict.bootstrap_degraded_active
        ),
    ]
    .join("\n")
}

fn replay_recent_raw_journal(
    runtime_store: &SqliteStore,
    journal_db_path: &Path,
    gap_fill_db_path: Option<&Path>,
    now: DateTime<Utc>,
    scoring_window_days: i64,
    replay_batch_size: usize,
    artifact_runtime_cursor: &DiscoveryRuntimeCursor,
) -> Result<RestoreReplaySummary> {
    let required_window_start = now - Duration::days(scoring_window_days.max(1));
    runtime_store.set_discovery_recent_raw_restore_state(
        &DiscoveryRecentRawRestoreStateUpdate {
            journal_available: false,
            journal_replayed: false,
            required_window_start: Some(required_window_start),
            journal_covered_since: None,
            journal_covered_through_cursor: None,
            gap_fill_replayed: false,
            gap_fill_covered_since: None,
            gap_fill_covered_through_cursor: None,
            effective_covered_since: None,
            effective_covered_through_cursor: None,
            artifact_runtime_cursor: Some(artifact_runtime_cursor.clone()),
            journal_covers_artifact_cursor: false,
            raw_coverage_satisfied: false,
            gap_fill_replayed_rows: 0,
            replayed_rows: 0,
            reason: Some("journal_replay_started".to_string()),
            replay_started_at: Some(now),
            replay_completed_at: None,
        },
    )?;

    let replay = if journal_db_path.exists() {
        let journal_store = SqliteStore::open_read_only(journal_db_path).with_context(|| {
            format!(
                "failed opening recent raw journal db {}",
                journal_db_path.display()
            )
        })?;
        journal_store.replay_recent_raw_journal_into_runtime_store(
            runtime_store,
            required_window_start,
            artifact_runtime_cursor,
            replay_batch_size.max(1),
        )?
    } else {
        RecentRawJournalReplaySummary {
            required_window_start,
            artifact_runtime_cursor: artifact_runtime_cursor.clone(),
            journal_available: false,
            journal_covered_since: None,
            journal_covered_through_cursor: None,
            journal_covers_artifact_cursor: false,
            replayed_rows: 0,
            raw_coverage_satisfied: false,
        }
    };

    let gap_fill_replay = match gap_fill_db_path {
        Some(path) if path.exists() => {
            let gap_fill_store = SqliteStore::open_read_only(path)
                .with_context(|| format!("failed opening raw gap fill db {}", path.display()))?;
            Some(gap_fill_store.replay_recent_raw_journal_into_runtime_store(
                runtime_store,
                required_window_start,
                artifact_runtime_cursor,
                replay_batch_size.max(1),
            )?)
        }
        Some(_) | None => None,
    };

    let effective_covered_since = min_ts_opt(
        replay.journal_covered_since,
        gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_since),
    );
    let effective_covered_through_cursor = max_cursor_opt(
        replay.journal_covered_through_cursor.as_ref(),
        gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_through_cursor.as_ref()),
    );
    let runtime_window_has_rows = !runtime_store
        .load_recent_observed_swaps_since(required_window_start, 1)?
        .0
        .is_empty();
    let raw_coverage_satisfied = replay.journal_available
        && replay.journal_covers_artifact_cursor
        && effective_covered_since
            .is_some_and(|covered_since| covered_since <= required_window_start)
        && runtime_window_has_rows;
    let gap_fill_replayed = gap_fill_replay.is_some();
    let gap_fill_replayed_rows = gap_fill_replay
        .as_ref()
        .map(|summary| summary.replayed_rows)
        .unwrap_or(0);
    let replayed_rows = replay.replayed_rows.saturating_add(gap_fill_replayed_rows);

    let replay_summary = RestoreReplaySummary {
        required_window_start,
        artifact_runtime_cursor: artifact_runtime_cursor.clone(),
        journal_available: replay.journal_available,
        journal_covered_since: replay.journal_covered_since,
        journal_covered_through_cursor: replay.journal_covered_through_cursor.clone(),
        journal_covers_artifact_cursor: replay.journal_covers_artifact_cursor,
        gap_fill_replayed,
        gap_fill_covered_since: gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_since),
        gap_fill_covered_through_cursor: gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_through_cursor.clone()),
        effective_covered_since,
        effective_covered_through_cursor,
        gap_fill_replayed_rows,
        replayed_rows,
        raw_coverage_satisfied,
    };

    runtime_store.set_discovery_recent_raw_restore_state(
        &DiscoveryRecentRawRestoreStateUpdate {
            journal_available: replay.journal_available,
            journal_replayed: replay.replayed_rows > 0 || replay.journal_available,
            required_window_start: Some(replay_summary.required_window_start),
            journal_covered_since: replay.journal_covered_since,
            journal_covered_through_cursor: replay.journal_covered_through_cursor.clone(),
            gap_fill_replayed,
            gap_fill_covered_since: replay_summary.gap_fill_covered_since,
            gap_fill_covered_through_cursor: replay_summary.gap_fill_covered_through_cursor.clone(),
            effective_covered_since: replay_summary.effective_covered_since,
            effective_covered_through_cursor: replay_summary
                .effective_covered_through_cursor
                .clone(),
            artifact_runtime_cursor: Some(replay.artifact_runtime_cursor.clone()),
            journal_covers_artifact_cursor: replay.journal_covers_artifact_cursor,
            raw_coverage_satisfied,
            gap_fill_replayed_rows,
            replayed_rows,
            reason: Some(recent_raw_journal_restore_reason(
                &replay,
                gap_fill_replay.as_ref(),
                journal_db_path,
                gap_fill_db_path,
                raw_coverage_satisfied,
            )),
            replay_started_at: Some(now),
            replay_completed_at: Some(now),
        },
    )?;
    Ok(replay_summary)
}

fn recent_raw_journal_restore_reason(
    replay: &RecentRawJournalReplaySummary,
    gap_fill_replay: Option<&RecentRawJournalReplaySummary>,
    journal_db_path: &Path,
    gap_fill_db_path: Option<&Path>,
    raw_coverage_satisfied: bool,
) -> String {
    if !replay.journal_available {
        return format!(
            "recent_raw_journal_unavailable:{}",
            journal_db_path.display()
        );
    }
    if !replay.journal_covers_artifact_cursor {
        return "recent_raw_journal_missing_artifact_cursor_lineage".to_string();
    }
    if !raw_coverage_satisfied {
        if let Some(path) = gap_fill_db_path {
            if gap_fill_replay.is_some() {
                return format!(
                    "recent_raw_gap_fill_raw_coverage_unsatisfied:{}",
                    path.display()
                );
            }
        }
        return "recent_raw_journal_raw_coverage_unsatisfied".to_string();
    }
    if gap_fill_replay.is_some() {
        return "recent_raw_journal_gap_fill_replay_completed".to_string();
    }
    "recent_raw_journal_replay_completed".to_string()
}

fn min_ts_opt(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_cursor_opt(
    left: Option<&DiscoveryRuntimeCursor>,
    right: Option<&DiscoveryRuntimeCursor>,
) -> Option<DiscoveryRuntimeCursor> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if cmp_cursor(left, right) != std::cmp::Ordering::Less {
                Some(left.clone())
            } else {
                Some(right.clone())
            }
        }
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn cmp_cursor(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

#[cfg(test)]
mod tests {
    use super::{
        export_artifact, make_fixture, make_swap, parse_ts, seed_recent_raw_journal,
        seed_runtime_artifact_source, SqliteStore, Value,
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
        assert_eq!(parsed.journal_db_path, None);
        assert_eq!(parsed.gap_fill_db_path, None);
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
            journal_db_path: None,
            gap_fill_db_path: None,
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
            journal_db_path: None,
            gap_fill_db_path: None,
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
            journal_db_path: None,
            gap_fill_db_path: None,
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

    #[test]
    fn run_restore_verdict_stays_fail_closed_without_required_raw_coverage() -> Result<()> {
        let fixture = make_fixture("runtime-restore-short-journal")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-short-journal.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-short-journal.db");
        let journal_db_path = fixture.temp.path().join("recent-short-journal.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap(
                "journal-short-only",
                artifact_now + Duration::minutes(10),
                50,
            )],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: None,
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("fail_closed")
        );
        assert_eq!(
            parsed
                .pointer("/verdict/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            parsed
                .pointer("/operator_status/recent_raw_restore/journal_available")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/operator_status/recent_raw_restore/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn run_replays_recent_raw_journal_into_fresh_runtime_db_and_becomes_trading_ready() -> Result<()>
    {
        let fixture = make_fixture("runtime-restore-trading-ready")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-trading-ready.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-trading-ready.db");
        let journal_db_path = fixture.temp.path().join("recent-trading-ready.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[
                make_swap("journal-too-old", now - Duration::days(9), 10),
                make_swap("journal-window-start", now - Duration::days(7), 11),
                make_swap("journal-recent", now - Duration::minutes(5), 12),
            ],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: None,
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("trading_ready")
        );
        assert_eq!(
            parsed
                .pointer("/verdict/journal_available")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/verdict/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/replay/replayed_rows")
                .and_then(Value::as_u64),
            Some(2)
        );
        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let restored_signatures = restored
            .load_observed_swaps_since(now - Duration::days(30))?
            .into_iter()
            .map(|swap| swap.signature)
            .collect::<Vec<_>>();
        assert_eq!(
            restored_signatures,
            vec![
                "journal-window-start".to_string(),
                "journal-recent".to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn run_keeps_bootstrap_degraded_verdict_even_when_journal_replay_restores_raw_window(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-restore-bootstrap-journal")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        let artifact_now = now - Duration::hours(3);
        seed_runtime_artifact_source(&fixture.source_store, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-bootstrap-journal.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-bootstrap-journal.db");
        let journal_db_path = fixture.temp.path().join("recent-bootstrap-journal.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[
                make_swap("journal-bootstrap-window", now - Duration::days(7), 21),
                make_swap("journal-bootstrap-recent", now - Duration::minutes(5), 22),
            ],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: None,
            bootstrap_degraded: true,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("bootstrap_degraded")
        );
        assert_eq!(
            parsed
                .pointer("/operator_status/recent_raw_restore/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );
        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        assert!(restored.discovery_bootstrap_degraded_state()?.active);
        Ok(())
    }

    #[test]
    fn run_gap_fill_plus_recent_journal_restores_trading_ready_verdict() -> Result<()> {
        let fixture = make_fixture("runtime-restore-gap-fill-healthy")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-gap-fill-healthy.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-gap-fill-healthy.db");
        let journal_db_path = fixture.temp.path().join("recent-gap-fill-healthy.db");
        let gap_fill_db_path = fixture.temp.path().join("gap-fill-healthy.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap("journal-recent", now - Duration::minutes(5), 12)],
            now,
        )?;
        seed_recent_raw_journal(
            &gap_fill_db_path,
            &[make_swap(
                "gap-fill-window-start",
                now - Duration::days(7),
                11,
            )],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: Some(gap_fill_db_path),
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("trading_ready")
        );
        assert_eq!(
            parsed
                .pointer("/replay/gap_fill_replayed")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/replay/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );

        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let restore_state = restored.discovery_recent_raw_restore_state()?;
        assert!(restore_state.raw_coverage_satisfied);
        assert!(restore_state.gap_fill_replayed);
        assert_eq!(restore_state.gap_fill_replayed_rows, 1);
        Ok(())
    }

    #[test]
    fn run_partial_gap_fill_keeps_restore_fail_closed() -> Result<()> {
        let fixture = make_fixture("runtime-restore-gap-fill-partial")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-gap-fill-partial.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-gap-fill-partial.db");
        let journal_db_path = fixture.temp.path().join("recent-gap-fill-partial.db");
        let gap_fill_db_path = fixture.temp.path().join("gap-fill-partial.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap("journal-recent", now - Duration::minutes(5), 12)],
            now,
        )?;
        seed_recent_raw_journal(
            &gap_fill_db_path,
            &[make_swap(
                "gap-fill-too-new",
                now - Duration::days(6) + Duration::hours(1),
                11,
            )],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: Some(gap_fill_db_path),
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("fail_closed")
        );
        assert_eq!(
            parsed
                .pointer("/replay/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(false)
        );

        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let restore_state = restored.discovery_recent_raw_restore_state()?;
        assert!(!restore_state.raw_coverage_satisfied);
        assert!(restore_state.gap_fill_replayed);
        Ok(())
    }
}

#[cfg(test)]
struct Fixture {
    source_store: SqliteStore,
    config_path: PathBuf,
    temp: tempfile::TempDir,
}

#[cfg(test)]
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

#[cfg(test)]
fn export_artifact(
    store: &SqliteStore,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    let loaded_config = copybot_config::load_from_path(config_path)?;
    let discovery = DiscoveryService::new(loaded_config.discovery.clone(), loaded_config.shadow);
    store.export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())
}

#[cfg(test)]
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

#[cfg(test)]
fn make_swap(signature: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
    SwapEvent {
        wallet: "wallet-restore".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: format!("token-{signature}"),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

#[cfg(test)]
fn seed_recent_raw_journal(
    journal_db_path: &Path,
    swaps: &[SwapEvent],
    completed_at: DateTime<Utc>,
) -> Result<()> {
    let journal_store = SqliteStore::open(journal_db_path)?;
    journal_store.insert_recent_raw_journal_batch(swaps, completed_at)?;
    Ok(())
}

#[cfg(test)]
fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}
