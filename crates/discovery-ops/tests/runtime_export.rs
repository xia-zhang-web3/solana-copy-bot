use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use copybot_discovery_v2::{
    discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_runtime_artifacts::{artifact_latest_path, write_json_atomic};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only,
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, SqliteStore,
};
use serde_json::Value;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration as StdDuration, Instant as StdInstant};
use tempfile::tempdir;

fn test_now() -> DateTime<Utc> {
    Utc::now() - Duration::seconds(1)
}

fn write_config(dir: &Path) -> Result<PathBuf> {
    let config_path = dir.join("live.toml");
    let db_path = dir.join("runtime.db");
    let artifact_dir = dir.join("artifacts");
    fs::write(
        &config_path,
        format!(
            r#"
[sqlite]
path = "{}"

[runtime_restore_ops]
artifact_dir = "{}"
artifact_retention = 2
artifact_cadence_minutes = 10
journal_snapshot_dir = "{}"
journal_snapshot_retention = 2
journal_snapshot_cadence_minutes = 10
drill_workspace_dir = "{}"
"#,
            db_path.display(),
            artifact_dir.display(),
            dir.join("journal").display(),
            dir.join("drills").display()
        ),
    )?;
    Ok(config_path)
}

fn command_output_with_timeout(command: &mut Command) -> Result<Output> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().context("failed spawning CLI under test")?;
    let deadline = StdInstant::now() + StdDuration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().context("failed polling CLI under test")? {
            let mut stdout = Vec::new();
            if let Some(mut pipe) = child.stdout.take() {
                pipe.read_to_end(&mut stdout)
                    .context("failed reading CLI stdout")?;
            }
            let mut stderr = Vec::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_end(&mut stderr)
                    .context("failed reading CLI stderr")?;
            }
            return Ok(Output {
                status,
                stdout,
                stderr,
            });
        }
        if StdInstant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("CLI under test exceeded 20s timeout");
        }
        thread::sleep(StdDuration::from_millis(10));
    }
}

fn wallet_row(now: DateTime<Utc>) -> WalletUpsertRow {
    WalletUpsertRow {
        wallet_id: "wallet_a".to_string(),
        first_seen: now - Duration::minutes(10),
        last_seen: now,
        status: "active".to_string(),
    }
}

fn metric_row(window_start: DateTime<Utc>) -> WalletMetricRow {
    WalletMetricRow {
        wallet_id: "wallet_a".to_string(),
        window_start,
        pnl: 1.0,
        win_rate: 1.0,
        trades: 1,
        closed_trades: 1,
        hold_median_seconds: 60,
        score: 0.90,
        buy_total: 1,
        tradable_ratio: 1.0,
        rug_ratio: 0.0,
    }
}

fn publication_update(
    now: DateTime<Utc>,
    window_start: DateTime<Utc>,
    runtime_mode: DiscoveryRuntimeMode,
    reason: &str,
) -> DiscoveryPublicationStateUpdate {
    DiscoveryPublicationStateUpdate {
        runtime_mode,
        reason: reason.to_string(),
        last_published_at: Some(now),
        last_published_window_start: Some(window_start),
        published_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
        published_wallet_ids: Some(vec!["wallet_a".to_string()]),
    }
}

fn publication_gate(config: &AppConfig, now: DateTime<Utc>) -> DiscoveryPublicationFreshnessGate {
    let options =
        DiscoveryV2BuildOptions::from_config(&config.discovery, config.execution.enabled, now);
    DiscoveryPublicationFreshnessGate {
        scoring_window_days: i64::from(config.discovery.scoring_window_days.max(1)),
        window_minutes: Some(options.window_minutes),
        metric_snapshot_interval_seconds: config.discovery.metric_snapshot_interval_seconds.max(1),
        refresh_seconds: config.discovery.refresh_seconds.max(1),
        expected_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
        expected_policy_fingerprint: Some(discovery_v2_policy_fingerprint(
            &config.discovery,
            &config.shadow,
            &options,
        )),
    }
}

fn persist_publication(
    store: &SqliteStore,
    config: &AppConfig,
    now: DateTime<Utc>,
    fingerprint: &str,
) -> Result<DiscoveryRuntimeCursor> {
    persist_publication_with_mode(
        store,
        config,
        now,
        fingerprint,
        DiscoveryRuntimeMode::Healthy,
        "ready",
    )
}

fn persist_publication_with_mode(
    store: &SqliteStore,
    config: &AppConfig,
    now: DateTime<Utc>,
    fingerprint: &str,
    runtime_mode: DiscoveryRuntimeMode,
    reason: &str,
) -> Result<DiscoveryRuntimeCursor> {
    let window_start = now - Duration::days(i64::from(config.discovery.scoring_window_days.max(1)));
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: format!("tail-{}", now.timestamp()),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        reason,
        &publication_update(now, window_start, runtime_mode, reason),
        fingerprint,
        &cursor,
    )?;
    Ok(cursor)
}

fn run_scheduled_export_output(config_path: &Path) -> Result<Output> {
    checkpoint_test_runtime_db(config_path)?;
    command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_runtime_export")).args([
            "--config",
            config_path.to_str().context("non-utf8 config path")?,
            "--scheduled",
            "--json",
        ]),
    )
    .context("failed running discovery runtime export binary")
}

fn checkpoint_test_runtime_db(config_path: &Path) -> Result<()> {
    let config = load_from_path(config_path)?;
    let db_path = PathBuf::from(&config.sqlite.path);
    if !db_path.exists() {
        return Ok(());
    }
    let store = SqliteStore::open(&db_path)?;
    let _ = store.checkpoint_wal_passive()?;
    drop(store);
    let read_store = SqliteStore::open_read_only(&db_path)?;
    validate_discovery_v2_schema_read_only(&read_store)?;
    Ok(())
}

fn run_scheduled_export(config_path: &Path) -> Result<Value> {
    let output = run_scheduled_export_output(config_path)?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).context("invalid export json")
}

#[test]
fn runtime_export_rejects_operator_supplied_clock() -> Result<()> {
    let dir = tempdir()?;
    let config_path = write_config(dir.path())?;

    let output = command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_runtime_export")).args([
            "--config",
            config_path.to_str().context("non-utf8 config path")?,
            "--scheduled",
            "--json",
            "--now",
            "2026-05-03T10:00:00Z",
        ]),
    )
    .context("failed running discovery runtime export binary")?;

    assert!(
        !output.status.success(),
        "runtime export --now unexpectedly succeeded: stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("not accepted by production runtime export"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn scheduled_export_does_not_skip_artifact_from_stale_policy() -> Result<()> {
    let dir = tempdir()?;
    let config_path = write_config(dir.path())?;
    let config = load_from_path(&config_path)?;
    let db_path = dir.path().join("runtime.db");
    let artifact_dir = dir.path().join("artifacts");
    let store = SqliteStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let now = test_now();

    let current_gate = publication_gate(&config, now);
    let current_fingerprint = current_gate
        .expected_policy_fingerprint
        .clone()
        .expect("current fingerprint");
    let stale_fingerprint = "stale-policy-fingerprint";
    persist_publication(
        &store,
        &config,
        now - Duration::minutes(1),
        stale_fingerprint,
    )?;
    let stale_gate = DiscoveryPublicationFreshnessGate {
        expected_policy_fingerprint: Some(stale_fingerprint.to_string()),
        ..current_gate.clone()
    };
    let stale_artifact =
        store.export_discovery_runtime_artifact(now - Duration::minutes(1), stale_gate)?;
    write_json_atomic(&artifact_latest_path(&artifact_dir), &stale_artifact)?;
    persist_publication(&store, &config, now, &current_fingerprint)?;

    let output = run_scheduled_export(&config_path)?;
    assert_eq!(output["state"], "written");
    assert_eq!(output["fresh_under_current_gate"], true);
    assert_eq!(
        output["publication_policy_fingerprint"].as_str(),
        Some(current_fingerprint.as_str())
    );
    Ok(())
}

#[test]
fn scheduled_export_does_not_skip_when_db_publication_is_newer() -> Result<()> {
    let dir = tempdir()?;
    let config_path = write_config(dir.path())?;
    let config = load_from_path(&config_path)?;
    let db_path = dir.path().join("runtime.db");
    let artifact_dir = dir.path().join("artifacts");
    let store = SqliteStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let now = test_now();

    let current_gate = publication_gate(&config, now);
    let current_fingerprint = current_gate
        .expected_policy_fingerprint
        .clone()
        .expect("current fingerprint");
    let old_cursor = persist_publication(
        &store,
        &config,
        now - Duration::minutes(1),
        &current_fingerprint,
    )?;
    let stale_artifact =
        store.export_discovery_runtime_artifact(now - Duration::minutes(1), current_gate)?;
    write_json_atomic(&artifact_latest_path(&artifact_dir), &stale_artifact)?;
    let new_cursor = persist_publication(&store, &config, now, &current_fingerprint)?;

    let output = run_scheduled_export(&config_path)?;

    assert_ne!(old_cursor.signature, new_cursor.signature);
    assert_eq!(output["state"], "written");
    assert_eq!(
        output["runtime_cursor_signature"].as_str(),
        Some(new_cursor.signature.as_str())
    );
    Ok(())
}

#[test]
fn scheduled_export_does_not_skip_when_db_metric_snapshot_changed() -> Result<()> {
    let dir = tempdir()?;
    let config_path = write_config(dir.path())?;
    let config = load_from_path(&config_path)?;
    let db_path = dir.path().join("runtime.db");
    let artifact_dir = dir.path().join("artifacts");
    let store = SqliteStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let now = test_now();

    let current_gate = publication_gate(&config, now);
    let current_fingerprint = current_gate
        .expected_policy_fingerprint
        .clone()
        .expect("current fingerprint");
    persist_publication(&store, &config, now, &current_fingerprint)?;
    let artifact = store.export_discovery_runtime_artifact(now, current_gate)?;
    write_json_atomic(&artifact_latest_path(&artifact_dir), &artifact)?;
    let window_start = now - Duration::days(i64::from(config.discovery.scoring_window_days.max(1)));
    let mut drifted_metric = metric_row(window_start);
    drifted_metric.score = 0.42;
    store.persist_discovery_cycle(
        &[],
        &[drifted_metric],
        &["wallet_a".to_string()],
        false,
        false,
        now,
        "metric-drift",
    )?;

    let output = run_scheduled_export(&config_path)?;

    assert_eq!(output["state"], "written");
    Ok(())
}

#[test]
fn scheduled_export_does_not_skip_future_dated_latest_artifact() -> Result<()> {
    let dir = tempdir()?;
    let config_path = write_config(dir.path())?;
    let config = load_from_path(&config_path)?;
    let db_path = dir.path().join("runtime.db");
    let artifact_dir = dir.path().join("artifacts");
    let store = SqliteStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let now = test_now();

    let current_gate = publication_gate(&config, now);
    let current_fingerprint = current_gate
        .expected_policy_fingerprint
        .clone()
        .expect("current fingerprint");
    persist_publication(&store, &config, now, &current_fingerprint)?;
    let future_artifact =
        store.export_discovery_runtime_artifact(now + Duration::minutes(5), current_gate)?;
    write_json_atomic(&artifact_latest_path(&artifact_dir), &future_artifact)?;

    let output = run_scheduled_export(&config_path)?;

    assert_eq!(output["state"], "written");
    let exported_at = output["exported_at"]
        .as_str()
        .context("missing exported_at")?;
    assert_ne!(exported_at, future_artifact.exported_at.to_rfc3339());
    Ok(())
}

#[test]
fn scheduled_export_rejects_not_due_artifact_when_publication_is_not_healthy() -> Result<()> {
    let dir = tempdir()?;
    let config_path = write_config(dir.path())?;
    let config = load_from_path(&config_path)?;
    let db_path = dir.path().join("runtime.db");
    let artifact_dir = dir.path().join("artifacts");
    let store = SqliteStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let now = test_now();

    let current_gate = publication_gate(&config, now);
    let current_fingerprint = current_gate
        .expected_policy_fingerprint
        .clone()
        .expect("current fingerprint");
    persist_publication(&store, &config, now, &current_fingerprint)?;
    let mut artifact = store.export_discovery_runtime_artifact(now, current_gate)?;
    artifact.publication_state.runtime_mode = DiscoveryRuntimeMode::FailClosed;
    artifact.publication_state.reason = "blocked".to_string();
    write_json_atomic(&artifact_latest_path(&artifact_dir), &artifact)?;
    persist_publication_with_mode(
        &store,
        &config,
        now,
        &current_fingerprint,
        DiscoveryRuntimeMode::FailClosed,
        "blocked",
    )?;

    let output = run_scheduled_export_output(&config_path)?;

    assert!(
        !output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires healthy publication state"));
    Ok(())
}
