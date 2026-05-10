use super::*;
use chrono::Duration;
use tempfile::tempdir;

#[path = "91_recent_raw_runtime_artifact_tests.rs"]
mod recent_raw_runtime_artifact_tests;

fn metrics_window_start_for_gate(
    gate: &DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let interval_seconds = gate.metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(gate.scoring_window_days.max(1))
}

fn seed_runtime_artifact_source_store(
    source_store: &SqliteStore,
    now: DateTime<Utc>,
    export_gate: DiscoveryPublicationFreshnessGate,
) -> Result<DiscoveryRuntimeArtifact> {
    let metrics_window_start = metrics_window_start_for_gate(&export_gate, now);
    let published_at = now - Duration::minutes(5);
    let published_wallet_ids = vec!["wallet-alpha".to_string()];

    source_store.persist_discovery_cycle(
        &[
            WalletUpsertRow {
                wallet_id: "wallet-alpha".to_string(),
                first_seen: now - Duration::days(3),
                last_seen: now - Duration::minutes(2),
                status: "candidate".to_string(),
            },
            WalletUpsertRow {
                wallet_id: "wallet-beta".to_string(),
                first_seen: now - Duration::days(2),
                last_seen: now - Duration::minutes(1),
                status: "observed".to_string(),
            },
        ],
        &[
            WalletMetricRow {
                wallet_id: "wallet-alpha".to_string(),
                window_start: metrics_window_start,
                pnl: 3.4,
                win_rate: 0.88,
                trades: 8,
                closed_trades: 8,
                hold_median_seconds: 120,
                score: 1.4,
                buy_total: 8,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            },
            WalletMetricRow {
                wallet_id: "wallet-beta".to_string(),
                window_start: metrics_window_start,
                pnl: 0.4,
                win_rate: 0.5,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 240,
                score: 0.2,
                buy_total: 4,
                tradable_ratio: 0.5,
                rug_ratio: 0.25,
            },
        ],
        &published_wallet_ids,
        true,
        true,
        published_at,
        "seed_runtime_artifact_roundtrip",
    )?;
    let runtime_cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 77,
        signature: "runtime-artifact-cursor".to_string(),
    };
    source_store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
    source_store.set_discovery_publication_state_with_identity(
        &DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "seed_runtime_artifact_roundtrip".to_string(),
            last_published_at: Some(published_at),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("discovery_v2_operational_window".to_string()),
            published_wallet_ids: Some(published_wallet_ids.clone()),
        },
        false,
        Some("test-policy-fingerprint"),
        Some(&runtime_cursor),
    )?;
    source_store.export_discovery_runtime_artifact(now, export_gate)
}

#[test]
fn discovery_runtime_artifact_restore_rejects_fail_closed_artifact() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source-fail-closed.db");
    let restore_db_path = temp.path().join("runtime-artifact-restore-fail-closed.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let export_gate = DiscoveryPublicationFreshnessGate {
        scoring_window_days: 7,
        metric_snapshot_interval_seconds: 1_800,
        refresh_seconds: 600,
        expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
        expected_policy_fingerprint: Some("test-policy-fingerprint".to_string()),
    };
    let mut source_store = SqliteStore::open(Path::new(&source_db_path))?;
    source_store.run_migrations(&migration_dir)?;
    let mut artifact = seed_runtime_artifact_source_store(&source_store, now, export_gate)?;
    artifact.publication_state.runtime_mode = DiscoveryRuntimeMode::FailClosed;
    artifact.publication_state.reason = "blocked".to_string();

    let mut restore_store = SqliteStore::open(Path::new(&restore_db_path))?;
    restore_store.run_migrations(&migration_dir)?;
    let error = restore_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("restore must reject non-healthy runtime artifact");

    assert!(error
        .to_string()
        .contains("restore requires healthy publication state"));
    Ok(())
}

#[test]
fn discovery_runtime_artifact_restore_rejects_missing_publication_identity() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source-identity.db");
    let restore_db_path = temp.path().join("runtime-artifact-restore-identity.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let export_gate = DiscoveryPublicationFreshnessGate {
        scoring_window_days: 7,
        metric_snapshot_interval_seconds: 1_800,
        refresh_seconds: 600,
        expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
        expected_policy_fingerprint: Some("test-policy-fingerprint".to_string()),
    };
    let mut source_store = SqliteStore::open(Path::new(&source_db_path))?;
    source_store.run_migrations(&migration_dir)?;
    let mut artifact = seed_runtime_artifact_source_store(&source_store, now, export_gate)?;
    artifact.publication_state.publication_policy_fingerprint = None;

    let mut restore_store = SqliteStore::open(Path::new(&restore_db_path))?;
    restore_store.run_migrations(&migration_dir)?;
    let error = restore_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("restore must reject runtime artifact without publication identity");

    assert!(error
        .to_string()
        .contains("restore requires complete publication identity"));
    Ok(())
}

#[test]
fn runtime_artifact_restore_dirty_table_inventory_reports_runtime_bearing_tables() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("runtime-artifact-dirty-inventory.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.insert_shadow_lot("wallet-shadow", "token-shadow", 1.0, 0.3, now)?;
    store.insert_risk_event(
        "shadow_risk_pause",
        "warn",
        now,
        Some("{\"pause_type\":\"exposure_soft_cap\"}"),
    )?;

    let dirty_tables = store.runtime_artifact_restore_dirty_tables()?;
    assert!(dirty_tables
        .iter()
        .any(|entry| { entry.table == "shadow_lots" && entry.category == "shadow accounting" }));
    assert!(dirty_tables
        .iter()
        .any(|entry| { entry.table == "risk_events" && entry.category == "risk gating" }));
    Ok(())
}

#[test]
fn discovery_runtime_artifact_restore_rejects_existing_shadow_lots() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source-shadow.db");
    let restore_db_path = temp.path().join("runtime-artifact-restore-shadow.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let export_gate = DiscoveryPublicationFreshnessGate {
        scoring_window_days: 7,
        metric_snapshot_interval_seconds: 1_800,
        refresh_seconds: 600,
        expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
        expected_policy_fingerprint: Some("test-policy-fingerprint".to_string()),
    };

    let mut source_store = SqliteStore::open(Path::new(&source_db_path))?;
    source_store.run_migrations(&migration_dir)?;
    let artifact = seed_runtime_artifact_source_store(&source_store, now, export_gate)?;

    let mut restore_store = SqliteStore::open(Path::new(&restore_db_path))?;
    restore_store.run_migrations(&migration_dir)?;
    restore_store.insert_shadow_lot("wallet-shadow", "token-shadow", 1.0, 0.3, now)?;

    let error = restore_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("legacy restore must stay quarantined before dirty db inspection");
    assert!(error
        .to_string()
        .contains("legacy copybot-storage runtime artifact restore is quarantined"));
    Ok(())
}

#[test]
fn discovery_runtime_artifact_restore_rejects_existing_risk_events() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source-risk.db");
    let restore_db_path = temp.path().join("runtime-artifact-restore-risk.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let export_gate = DiscoveryPublicationFreshnessGate {
        scoring_window_days: 7,
        metric_snapshot_interval_seconds: 1_800,
        refresh_seconds: 600,
        expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
        expected_policy_fingerprint: Some("test-policy-fingerprint".to_string()),
    };

    let mut source_store = SqliteStore::open(Path::new(&source_db_path))?;
    source_store.run_migrations(&migration_dir)?;
    let artifact = seed_runtime_artifact_source_store(&source_store, now, export_gate)?;

    let mut restore_store = SqliteStore::open(Path::new(&restore_db_path))?;
    restore_store.run_migrations(&migration_dir)?;
    restore_store.insert_risk_event(
        "shadow_risk_pause",
        "warn",
        now,
        Some("{\"pause_type\":\"exposure_soft_cap\"}"),
    )?;

    let error = restore_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("legacy restore must stay quarantined before dirty db inspection");
    assert!(error
        .to_string()
        .contains("legacy copybot-storage runtime artifact restore is quarantined"));
    Ok(())
}
