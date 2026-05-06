use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use copybot_storage::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact,
    DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
};
use rusqlite::OptionalExtension;
use std::path::Path;
use tempfile::tempdir;

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

fn v2_export_gate(policy_fingerprint: &str) -> DiscoveryPublicationFreshnessGate {
    DiscoveryPublicationFreshnessGate {
        scoring_window_days: 7,
        metric_snapshot_interval_seconds: 1_800,
        refresh_seconds: 600,
        expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
        expected_policy_fingerprint: Some(policy_fingerprint.to_string()),
    }
}

fn migrated_store(db_path: &Path) -> Result<SqliteStore> {
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(db_path)?;
    store.run_migrations(&migration_dir)?;
    Ok(store)
}

#[test]
fn discovery_runtime_artifact_export_restore_roundtrip_stays_quarantined() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source.db");
    let restore_db_path = temp.path().join("runtime-artifact-restore.db");
    let source_store = migrated_store(&source_db_path)?;

    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let artifact = seed_runtime_artifact_source_store(
        &source_store,
        now,
        v2_export_gate("test-policy-fingerprint"),
    )?;

    let restore_store = migrated_store(&restore_db_path)?;
    let error = restore_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("legacy storage restore lane must stay quarantined");
    assert!(error
        .to_string()
        .contains("legacy copybot-storage runtime artifact restore is quarantined"));
    Ok(())
}

#[test]
fn discovery_runtime_artifact_restore_quarantine_does_not_prepare_schema() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source-no-mutate.db");
    let restore_db_path = temp.path().join("runtime-artifact-restore-no-mutate.db");
    let source_store = migrated_store(&source_db_path)?;
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let artifact = seed_runtime_artifact_source_store(
        &source_store,
        now,
        v2_export_gate("test-policy-fingerprint"),
    )?;

    let restore_store = SqliteStore::open(Path::new(&restore_db_path))?;
    let error = restore_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("legacy restore must stay quarantined");
    assert!(error
        .to_string()
        .contains("legacy copybot-storage runtime artifact restore is quarantined"));
    drop(restore_store);

    let table_exists: Option<i64> = rusqlite::Connection::open(&restore_db_path)?
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'discovery_strategy_state'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    assert_eq!(table_exists, None);
    Ok(())
}

#[test]
fn discovery_runtime_artifact_export_rejects_policy_fingerprint_mismatch() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source-wrong-policy.db");
    let source_store = migrated_store(&source_db_path)?;
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    let error = seed_runtime_artifact_source_store(
        &source_store,
        now,
        v2_export_gate("different-policy-fingerprint"),
    )
    .expect_err("legacy export must reject unexpected v2 policy fingerprint");

    let error_text = format!("{error:#}");
    assert!(
        error_text.contains("requires expected discovery v2 publication identity"),
        "{error_text}"
    );
    Ok(())
}

#[test]
fn discovery_runtime_artifact_export_requires_explicit_expected_identity() -> Result<()> {
    let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let cases = [
        (None, Some("test-policy-fingerprint".to_string())),
        (Some("discovery_v2_operational_window".to_string()), None),
    ];

    for (idx, (expected_scoring_source, expected_policy_fingerprint)) in
        cases.into_iter().enumerate()
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let source_db_path = temp
            .path()
            .join(format!("runtime-artifact-source-missing-identity-{idx}.db"));
        let source_store = migrated_store(&source_db_path)?;
        let export_gate = DiscoveryPublicationFreshnessGate {
            scoring_window_days: 7,
            metric_snapshot_interval_seconds: 1_800,
            refresh_seconds: 600,
            expected_scoring_source,
            expected_policy_fingerprint,
        };

        let error = seed_runtime_artifact_source_store(&source_store, now, export_gate)
            .expect_err("legacy export must require explicit expected v2 identity");

        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("requires explicit expected discovery v2 export gate identity"),
            "{error_text}"
        );
    }
    Ok(())
}

#[test]
fn discovery_publication_state_with_policy_read_only_does_not_prepare_schema() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("publication-state-read-only-old.db");
    let store = SqliteStore::open(Path::new(&db_path))?;

    let state = store.discovery_publication_state_with_policy_read_only()?;
    drop(store);
    let table_exists: Option<i64> = rusqlite::Connection::open(&db_path)?
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'discovery_strategy_state'",
            [],
            |row| row.get(0),
        )
        .optional()?;

    assert!(state.is_none());
    assert_eq!(table_exists, None);
    Ok(())
}
