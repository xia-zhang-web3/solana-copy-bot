use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_runtime_artifact_snapshot_shape,
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, SqliteDiscoveryStore,
};
use tempfile::tempdir;
const V2_SOURCE: &str = "discovery_v2_operational_window";
const V2_FINGERPRINT: &str = "test-v2-policy-fingerprint";

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
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
) -> DiscoveryPublicationStateUpdate {
    DiscoveryPublicationStateUpdate {
        runtime_mode: DiscoveryRuntimeMode::Healthy,
        reason: "ready".to_string(),
        last_published_at: Some(now),
        last_published_window_start: Some(window_start),
        published_scoring_source: Some(V2_SOURCE.to_string()),
        published_wallet_ids: Some(vec!["wallet_a".to_string()]),
    }
}

fn v2_gate() -> DiscoveryPublicationFreshnessGate {
    DiscoveryPublicationFreshnessGate {
        scoring_window_days: 1,
        metric_snapshot_interval_seconds: 60,
        refresh_seconds: 60,
        expected_scoring_source: Some(V2_SOURCE.to_string()),
        expected_policy_fingerprint: Some(V2_FINGERPRINT.to_string()),
    }
}

#[test]
fn v2_publication_persists_runtime_cursor_for_artifact_export() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };

    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;

    let artifact = store.export_discovery_runtime_artifact(now, v2_gate())?;

    assert_eq!(artifact.runtime_cursor, cursor);
    assert_eq!(
        artifact.publication_state.publication_runtime_cursor,
        Some(cursor.clone())
    );
    assert_eq!(
        artifact
            .publication_state
            .published_scoring_source
            .as_deref(),
        Some(V2_SOURCE)
    );
    assert_eq!(
        artifact
            .publication_state
            .publication_policy_fingerprint
            .as_deref(),
        Some(V2_FINGERPRINT)
    );
    assert_eq!(artifact.published_wallet_metrics_snapshot.len(), 1);
    Ok(())
}

#[test]
fn repeated_v2_publication_does_not_duplicate_active_follow_rows() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    for offset in [0_i64, 1] {
        store.persist_discovery_v2_publication(
            &[wallet_row(now)],
            &[metric_row(window_start)],
            &["wallet_a".to_string()],
            now + Duration::seconds(offset),
            "ready",
            &publication_update(now + Duration::seconds(offset), window_start),
            V2_FINGERPRINT,
            &cursor,
        )?;
    }

    assert_eq!(store.active_follow_wallet_row_count()?, 1);
    assert_eq!(
        store.list_active_follow_wallets()?,
        ["wallet_a".to_string()].into_iter().collect()
    );
    Ok(())
}

#[test]
fn runtime_export_rejects_runtime_cursor_drift_after_publication() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let publication_cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    let drifted_cursor = DiscoveryRuntimeCursor {
        ts_utc: now,
        slot: 43,
        signature: "tail-sig-2".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        V2_FINGERPRINT,
        &publication_cursor,
    )?;
    store.set_discovery_runtime_cursor(&drifted_cursor)?;

    let err = store
        .export_discovery_runtime_artifact(now, v2_gate())
        .expect_err("runtime cursor drift must fail closed");

    assert!(err
        .to_string()
        .contains("requires publication-bound runtime cursor"));
    Ok(())
}

#[test]
fn runtime_export_rejects_future_dated_publication_time() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let future_published_at = now + Duration::minutes(5);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        future_published_at,
        "future-dated",
        &publication_update(future_published_at, window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;

    let err = store
        .export_discovery_runtime_artifact(now, v2_gate())
        .expect_err("future publication time must fail closed");

    assert!(err
        .to_string()
        .contains("requires fresh publication truth under export gate"));
    Ok(())
}

#[test]
fn runtime_export_rejects_future_dated_publication_window() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let future_window_start = now + Duration::minutes(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(future_window_start)],
        &["wallet_a".to_string()],
        now,
        "future-window",
        &publication_update(now, future_window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;

    let err = store
        .export_discovery_runtime_artifact(now, v2_gate())
        .expect_err("future publication window must fail closed");

    assert!(err
        .to_string()
        .contains("requires fresh publication truth under export gate"));
    Ok(())
}

#[test]
fn runtime_export_rejects_publication_identity_mismatch() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };

    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        "different-fingerprint",
        &cursor,
    )?;

    let err = store
        .export_discovery_runtime_artifact(now, v2_gate())
        .expect_err("identity mismatch must fail closed");

    assert!(err
        .to_string()
        .contains("requires expected publication identity"));
    Ok(())
}

#[test]
fn runtime_export_rejects_fail_closed_publication_state() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    let mut update = publication_update(now, window_start);
    update.runtime_mode = DiscoveryRuntimeMode::FailClosed;
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "fail-closed",
        &update,
        V2_FINGERPRINT,
        &cursor,
    )?;

    let err = store
        .export_discovery_runtime_artifact(now, v2_gate())
        .expect_err("fail-closed publication must not export runtime artifact");

    assert!(err
        .to_string()
        .contains("requires healthy publication state"));
    Ok(())
}

#[test]
fn runtime_export_rejects_gate_without_expected_identity() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;
    let mut gate = v2_gate();
    gate.expected_policy_fingerprint = None;

    let err = store
        .export_discovery_runtime_artifact(now, gate)
        .expect_err("missing expected identity must fail closed");

    assert!(err
        .to_string()
        .contains("requires complete publication identity"));
    Ok(())
}

#[test]
fn runtime_artifact_snapshot_shape_rejects_extra_metric_rows() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;
    let mut artifact = store.export_discovery_runtime_artifact(now, v2_gate())?;
    let mut extra_metric = artifact.published_wallet_metrics_snapshot[0].clone();
    extra_metric.wallet_id = "wallet_b".to_string();
    artifact
        .published_wallet_metrics_snapshot
        .push(extra_metric);

    let err = validate_discovery_runtime_artifact_snapshot_shape(&artifact)
        .expect_err("extra metric rows must not validate");

    assert!(err
        .to_string()
        .contains("must exactly match published wallet ids"));
    Ok(())
}

#[test]
fn runtime_artifact_snapshot_shape_rejects_duplicate_metric_rows() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;
    let mut artifact = store.export_discovery_runtime_artifact(now, v2_gate())?;
    artifact
        .published_wallet_metrics_snapshot
        .push(artifact.published_wallet_metrics_snapshot[0].clone());

    let err = validate_discovery_runtime_artifact_snapshot_shape(&artifact)
        .expect_err("duplicate metric rows must not validate");

    assert!(err.to_string().contains("duplicate wallet rows"));
    Ok(())
}

#[test]
fn runtime_artifact_snapshot_shape_rejects_duplicate_published_wallet_ids() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "tail-sig".to_string(),
    };
    store.persist_discovery_v2_publication(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        now,
        "ready",
        &publication_update(now, window_start),
        V2_FINGERPRINT,
        &cursor,
    )?;
    let mut artifact = store.export_discovery_runtime_artifact(now, v2_gate())?;
    artifact
        .publication_state
        .published_wallet_ids
        .as_mut()
        .expect("published wallet ids")
        .push("wallet_a".to_string());

    let err = validate_discovery_runtime_artifact_snapshot_shape(&artifact)
        .expect_err("duplicate published wallet ids must not validate");

    assert!(err.to_string().contains("duplicate published wallet ids"));
    Ok(())
}

#[test]
fn runtime_export_rejects_publication_without_runtime_cursor() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-03T10:00:00Z")?;
    let window_start = now - Duration::days(1);

    store.persist_discovery_cycle(
        &[wallet_row(now)],
        &[metric_row(window_start)],
        &["wallet_a".to_string()],
        true,
        true,
        now,
        "legacy-path",
    )?;
    store.set_discovery_publication_state_with_options(
        &publication_update(now, window_start),
        false,
        Some(V2_FINGERPRINT),
    )?;

    let err = store
        .export_discovery_runtime_artifact(now, v2_gate())
        .expect_err("missing cursor must fail closed");

    assert!(err
        .to_string()
        .contains("requires complete publication identity"));
    Ok(())
}
