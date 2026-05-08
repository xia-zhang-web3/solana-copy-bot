use super::*;

#[test]
fn startup_trusted_selection_gate_status_uses_latest_snapshot_metadata_when_row_missing(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("startup-trusted-selection-gate-status-metadata-only.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let effective_window_start = DateTime::parse_from_rfc3339("2026-03-16T12:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let now = effective_window_start + Duration::minutes(1);
    let snapshot_write = TrustedWalletMetricsSnapshotWrite {
        snapshot_id: "wallet_metrics:discovery_refresh:2026-03-16T12:00:00+00:00".to_string(),
        source_snapshot_id: None,
        source_window_start: Some(effective_window_start),
        effective_window_start,
        created_at: now,
        source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
        row_count: 1,
        trust_state: TrustedSelectionState::TrustedCurrent,
    };
    store.persist_discovery_cycle_with_snapshot_metadata(
        &[WalletUpsertRow {
            wallet_id: "wallet-metadata-only".to_string(),
            first_seen: effective_window_start - Duration::days(1),
            last_seen: effective_window_start,
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-metadata-only".to_string(),
            window_start: effective_window_start,
            pnl: 1.0,
            win_rate: 0.8,
            trades: 4,
            closed_trades: 4,
            hold_median_seconds: 90,
            score: 0.8,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &[],
        false,
        false,
        now,
        "metadata-only-test",
        Some(&snapshot_write),
    )?;

    let status = store.startup_trusted_selection_gate_status()?;
    assert!(!status.bootstrap_required);
    assert_eq!(
        status.selection_state,
        Some(TrustedSelectionState::TrustedCurrent)
    );
    assert!(!status.startup_fail_closed);
    assert_eq!(status.reason, None);
    assert_eq!(
        status.active_snapshot_id,
        Some(snapshot_write.snapshot_id.clone())
    );
    assert_eq!(
        status.active_snapshot_window_start,
        Some(effective_window_start)
    );
    assert_eq!(
        status.last_bootstrap_source_kind,
        Some(TrustedSnapshotSourceKind::DiscoveryRefresh)
    );
    assert_eq!(
        status.source_snapshot_window_start,
        Some(effective_window_start)
    );
    assert!(!status.legacy_bool_fallback_used);
    Ok(())
}

#[test]
fn startup_trusted_selection_gate_status_prefers_latest_snapshot_metadata_over_legacy_bool_row(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("startup-trusted-selection-gate-status-legacy-bool-with-metadata.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.set_discovery_trusted_selection_bootstrap_required(false, "legacy_bool_false")?;

    let effective_window_start = DateTime::parse_from_rfc3339("2026-03-16T12:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let now = effective_window_start + Duration::minutes(1);
    let snapshot_write = TrustedWalletMetricsSnapshotWrite {
        snapshot_id: "wallet_metrics:discovery_refresh:2026-03-16T12:00:00+00:00".to_string(),
        source_snapshot_id: None,
        source_window_start: Some(effective_window_start),
        effective_window_start,
        created_at: now,
        source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
        row_count: 1,
        trust_state: TrustedSelectionState::TrustedCurrent,
    };
    store.persist_discovery_cycle_with_snapshot_metadata(
        &[WalletUpsertRow {
            wallet_id: "wallet-metadata-overrides-legacy".to_string(),
            first_seen: effective_window_start - Duration::days(1),
            last_seen: effective_window_start,
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-metadata-overrides-legacy".to_string(),
            window_start: effective_window_start,
            pnl: 1.0,
            win_rate: 0.8,
            trades: 4,
            closed_trades: 4,
            hold_median_seconds: 90,
            score: 0.8,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &[],
        false,
        false,
        now,
        "metadata-overrides-legacy",
        Some(&snapshot_write),
    )?;

    let status = store.startup_trusted_selection_gate_status()?;
    assert!(!status.bootstrap_required);
    assert_eq!(
        status.selection_state,
        Some(TrustedSelectionState::TrustedCurrent)
    );
    assert!(!status.startup_fail_closed);
    assert_eq!(status.reason.as_deref(), Some("legacy_bool_false"));
    assert_eq!(
        status.active_snapshot_id,
        Some(snapshot_write.snapshot_id.clone())
    );
    assert_eq!(
        status.active_snapshot_window_start,
        Some(effective_window_start)
    );
    assert_eq!(
        status.last_bootstrap_source_kind,
        Some(TrustedSnapshotSourceKind::DiscoveryRefresh)
    );
    assert_eq!(
        status.source_snapshot_window_start,
        Some(effective_window_start)
    );
    assert!(
            !status.legacy_bool_fallback_used,
            "snapshot metadata should keep startup on the typed path even when the row was first created by the old bool setter"
        );
    Ok(())
}

#[test]
fn startup_trusted_selection_gate_status_prefers_typed_invalid_state() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("startup-trusted-selection-gate-status-typed-invalid.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
        bootstrap_required: false,
        reason: "typed_invalid".to_string(),
        selection_state: TrustedSelectionState::Invalid,
        active_snapshot_id: None,
        active_snapshot_window_start: None,
        last_bootstrap_source_kind: None,
        last_bootstrap_at: Some(now),
    })?;

    let status = store.startup_trusted_selection_gate_status()?;
    assert!(!status.bootstrap_required);
    assert_eq!(status.selection_state, Some(TrustedSelectionState::Invalid));
    assert!(status.startup_fail_closed);
    assert!(!status.legacy_bool_fallback_used);
    assert_eq!(status.reason.as_deref(), Some("typed_invalid"));
    Ok(())
}

#[test]
fn removed_bridged_selection_values_parse_fail_closed() -> Result<()> {
    assert_eq!(
        TrustedSelectionState::parse("trusted_bridged")?,
        TrustedSelectionState::Invalid
    );
    assert_eq!(
        TrustedSelectionState::parse("trusted_bridged_stale")?,
        TrustedSelectionState::Invalid
    );
    assert_eq!(
        TrustedSnapshotSourceKind::parse("clone_latest_bridge")?,
        TrustedSnapshotSourceKind::Legacy
    );
    assert_eq!(
        TrustedSnapshotSourceKind::parse("admin_materialization")?,
        TrustedSnapshotSourceKind::Legacy
    );
    Ok(())
}
