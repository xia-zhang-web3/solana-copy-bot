use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_discovery::DiscoveryService;
use copybot_storage::{
    DiscoveryPublicationStateRow, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, PersistedWalletMetricSnapshotRow,
    DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};

fn metric_window_start(now: DateTime<Utc>, config: &DiscoveryConfig) -> DateTime<Utc> {
    let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now)
        - Duration::days(config.scoring_window_days.max(1) as i64)
}

fn runtime_artifact_fixture() -> (DiscoveryService, DiscoveryRuntimeArtifact) {
    let config = DiscoveryConfig {
        scoring_window_days: 1,
        metric_snapshot_interval_seconds: 60,
        refresh_seconds: 60,
        ..DiscoveryConfig::default()
    };
    let shadow = ShadowConfig::default();
    let discovery = DiscoveryService::new(config.clone(), shadow);
    let now = DateTime::parse_from_rfc3339("2026-05-08T12:00:00Z")
        .expect("fixed restore verdict timestamp")
        .with_timezone(&Utc);
    let window_start = metric_window_start(now, &config);
    let runtime_cursor = DiscoveryRuntimeCursor {
        ts_utc: now,
        slot: 42,
        signature: "sig-runtime-cursor".to_string(),
    };
    let export_gate = discovery.publication_freshness_gate();
    let artifact = DiscoveryRuntimeArtifact {
        format_version: DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
        exported_at: now,
        export_gate,
        publication_state: DiscoveryPublicationStateRow {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "healthy".to_string(),
            last_published_at: Some(now),
            last_published_window_start: Some(window_start),
            published_scoring_source: Some("discovery_v2_operational_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-a".to_string()]),
            publication_policy_fingerprint: Some(
                discovery.discovery_v2_publication_policy_fingerprint(false),
            ),
            publication_runtime_cursor: Some(runtime_cursor.clone()),
            updated_at: now,
        },
        runtime_cursor,
        published_wallet_metrics_snapshot: vec![PersistedWalletMetricSnapshotRow {
            wallet_id: "wallet-a".to_string(),
            window_start,
            first_seen: window_start,
            last_seen: now,
            pnl: 1.0,
            win_rate: 1.0,
            trades: 2,
            closed_trades: 1,
            hold_median_seconds: 60,
            score: 1.0,
            buy_total: 2,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
    };
    (discovery, artifact)
}

#[test]
fn validate_normal_runtime_restore_artifact_accepts_current_v2_identity() -> Result<()> {
    let (discovery, artifact) = runtime_artifact_fixture();
    discovery.validate_normal_runtime_restore_artifact(&artifact, artifact.exported_at)?;
    Ok(())
}

#[test]
fn validate_normal_runtime_restore_artifact_rejects_wrong_fingerprint() {
    let (discovery, mut artifact) = runtime_artifact_fixture();
    artifact.publication_state.publication_policy_fingerprint = Some("wrong".to_string());
    let err = discovery
        .validate_normal_runtime_restore_artifact(&artifact, artifact.exported_at)
        .expect_err("wrong fingerprint must be rejected")
        .to_string();
    assert!(
        err.contains("identity"),
        "unexpected error for wrong fingerprint: {err}"
    );
}

#[test]
fn validate_normal_runtime_restore_artifact_rejects_duplicate_metric_rows() {
    let (discovery, mut artifact) = runtime_artifact_fixture();
    artifact
        .published_wallet_metrics_snapshot
        .push(artifact.published_wallet_metrics_snapshot[0].clone());
    let err = discovery
        .validate_normal_runtime_restore_artifact(&artifact, artifact.exported_at)
        .expect_err("duplicate metric rows must be rejected")
        .to_string();
    assert!(
        err.contains("duplicate wallet rows"),
        "unexpected error for duplicate metric rows: {err}"
    );
}

#[test]
fn validate_normal_runtime_restore_artifact_rejects_stale_runtime_cursor() {
    let (discovery, mut artifact) = runtime_artifact_fixture();
    artifact.runtime_cursor.ts_utc =
        artifact.exported_at - Duration::seconds(artifact.export_gate.refresh_seconds as i64 + 1);
    artifact.publication_state.publication_runtime_cursor = Some(artifact.runtime_cursor.clone());

    let err = discovery
        .validate_normal_runtime_restore_artifact(&artifact, artifact.exported_at)
        .expect_err("stale runtime cursor must be rejected")
        .to_string();

    assert!(
        err.contains("runtime cursor is stale"),
        "unexpected error for stale runtime cursor: {err}"
    );
}

#[test]
fn validate_normal_runtime_restore_artifact_rejects_future_exported_at() {
    let (discovery, mut artifact) = runtime_artifact_fixture();
    let now = artifact.exported_at;
    artifact.exported_at = now + Duration::seconds(1);

    let err = discovery
        .validate_normal_runtime_restore_artifact(&artifact, now)
        .expect_err("future exported_at must be rejected")
        .to_string();

    assert!(
        err.contains("future-dated"),
        "unexpected error for future exported_at: {err}"
    );
}
