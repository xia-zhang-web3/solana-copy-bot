use super::*;

#[test]
fn history_report_validates_current_selection_across_multiple_captures() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-history-validated.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T12:00:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-beta"],
            &["wallet-gamma"],
        ),
    )?;
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T11:50:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-gamma"],
            &["wallet-beta"],
        ),
    )?;
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T11:40:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
            &["wallet-beta", "wallet-gamma"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-alpha"],
            &["wallet-beta"],
        ),
    )?;

    let report =
        discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;

    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::ValidatedCurrent
    );
    assert_eq!(report.captures_loaded, 3);
    assert_eq!(report.captures_considered, 3);
    assert_eq!(report.captures_within_recent_horizon, 3);
    assert!(!report.stale_captures_excluded_from_verdict);
    assert_eq!(report.fresh_capture_count, 3);
    assert!(report.active_follow_change_count > 0);
    assert!(report.current_raw_change_count > 0);
    assert!(report.shadow_signal_present_capture_count >= 1);
    Ok(())
}

#[test]
fn history_report_detects_publication_drift_across_recent_captures() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-history-drift.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T12:00:00Z"),
            WalletFreshnessVerdict::StalePublicationTruth,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-legacy", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha"],
            &[],
        ),
    )?;
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T11:50:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha"],
            &[],
        ),
    )?;
    let report =
        discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::PublicationDrifting
    );
    assert_eq!(report.stale_capture_count, 1);
    Ok(())
}

#[test]
fn history_report_flags_low_rotation_despite_fresh_truth() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-history-low-rotation.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    for captured_at in [
        ts("2026-03-25T12:00:00Z"),
        ts("2026-03-25T11:50:00Z"),
        ts("2026-03-25T11:40:00Z"),
    ] {
        persist_capture(
            &store,
            sample_capture_snapshot(
                captured_at,
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &[],
                &[],
            ),
        )?;
    }

    let report =
        discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::PartiallyValidatedButLowRotation
    );
    assert_eq!(report.captures_within_recent_horizon, 3);
    assert_eq!(report.rotation_evidence_capture_count, 0);
    Ok(())
}

#[test]
fn history_report_requires_shadow_signal_evidence() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-freshness-history-shadow-missing.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    for captured_at in [
        ts("2026-03-25T12:00:00Z"),
        ts("2026-03-25T11:50:00Z"),
        ts("2026-03-25T11:40:00Z"),
    ] {
        persist_capture(
            &store,
            sample_capture_snapshot(
                captured_at,
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals,
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-beta"],
                &[],
            ),
        )?;
    }

    let report =
        discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::InsufficientEvidence
    );
    assert_eq!(report.shadow_signal_present_capture_count, 0);
    Ok(())
}

#[test]
fn history_report_marks_raw_truth_insufficient_across_recent_captures() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-freshness-history-raw-insufficient.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T12:00:00Z"),
            WalletFreshnessVerdict::InsufficientRawTruth,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
            &["wallet-alpha"],
            &["wallet-alpha"],
            &["wallet-alpha"],
            &["wallet-alpha"],
            &["wallet-alpha"],
            &[],
            &[],
        ),
    )?;
    let report =
        discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::RawTruthInsufficient
    );
    assert_eq!(report.insufficient_raw_capture_count, 1);
    Ok(())
}

#[test]
fn fail_closed_preserved_publication_truth_stays_auditable_through_history_path() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-history-fail-closed.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_ranked_wallet_window(
        &store,
        now,
        &[
            ("wallet-alpha", "mint-a", 4, 0),
            ("wallet-beta", "mint-b", 3, 10),
        ],
    )?;
    seed_publication_truth(
        &store,
        &config,
        now - Duration::seconds(60),
        now,
        DiscoveryRuntimeMode::FailClosed,
        &["wallet-alpha", "wallet-beta"],
    )?;
    store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
    store.insert_copy_signal(&CopySignalRow {
        signal_id: "shadow:sig:wallet-alpha".to_string(),
        wallet_id: "wallet-alpha".to_string(),
        side: "buy".to_string(),
        token: "mint-a".to_string(),
        notional_sol: 0.2,
        notional_lamports: None,
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts: now - Duration::seconds(30),
        status: "shadow_recorded".to_string(),
    })?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let capture =
        discovery.wallet_freshness_capture_snapshot(&store, now, DEFAULT_RECENT_CYCLES)?;
    persist_capture(&store, capture.clone())?;
    let report = discovery.wallet_freshness_history_report(&store, now, 5)?;

    assert!(capture.audit.publication_truth_available);
    assert_eq!(
        capture.audit.publication_runtime_mode.as_deref(),
        Some("fail_closed")
    );
    assert_eq!(report.no_publication_truth_capture_count, 0);
    Ok(())
}

#[test]
fn stale_historical_captures_cannot_validate_current_selection() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-history-stale-only.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    for captured_at in [
        ts("2026-03-24T09:00:00Z"),
        ts("2026-03-24T08:50:00Z"),
        ts("2026-03-24T08:40:00Z"),
    ] {
        persist_capture(
            &store,
            sample_capture_snapshot(
                captured_at,
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-gamma"],
                &["wallet-beta"],
            ),
        )?;
    }

    let report = discovery.wallet_freshness_history_report_with_horizon(
        &store,
        ts("2026-03-25T12:00:00Z"),
        5,
        3_600,
    )?;

    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::InsufficientEvidence
    );
    assert_eq!(
        report.reason,
        "no_recent_wallet_freshness_captures_within_horizon"
    );
    assert_eq!(report.captures_loaded, 3);
    assert_eq!(report.captures_considered, 0);
    assert_eq!(report.captures_within_recent_horizon, 0);
    assert!(report.stale_captures_excluded_from_verdict);
    assert_eq!(report.stale_captures_excluded_count, 3);
    Ok(())
}

#[test]
fn mixed_recent_and_stale_captures_only_validate_from_recent_subset() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-freshness-history-mixed-recency.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T12:00:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-gamma"],
            &["wallet-beta"],
        ),
    )?;
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-25T11:50:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-beta"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-alpha", "wallet-gamma"],
            &["wallet-beta"],
            &["wallet-alpha"],
        ),
    )?;
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-24T08:40:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
            &["wallet-beta", "wallet-gamma"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-beta", "wallet-gamma"],
            &["wallet-alpha"],
            &["wallet-beta"],
        ),
    )?;

    let report = discovery.wallet_freshness_history_report_with_horizon(
        &store,
        ts("2026-03-25T12:05:00Z"),
        5,
        1_800,
    )?;

    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::InsufficientEvidence
    );
    assert_eq!(
        report.reason,
        "fewer_than_three_recent_wallet_freshness_captures"
    );
    assert_eq!(report.captures_loaded, 3);
    assert_eq!(report.captures_considered, 2);
    assert_eq!(report.captures_within_recent_horizon, 2);
    assert_eq!(report.stale_captures_excluded_count, 1);
    Ok(())
}

#[test]
fn recent_valid_captures_can_still_validate_with_stale_history_present() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-freshness-history-recent-subset-valid.db");
    let store = open_store(&db_path)?;
    let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
    for captured_at in [
        ts("2026-03-25T12:00:00Z"),
        ts("2026-03-25T11:50:00Z"),
        ts("2026-03-25T11:40:00Z"),
    ] {
        persist_capture(
            &store,
            sample_capture_snapshot(
                captured_at,
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-gamma"],
                &["wallet-beta"],
            ),
        )?;
    }
    persist_capture(
        &store,
        sample_capture_snapshot(
            ts("2026-03-24T08:40:00Z"),
            WalletFreshnessVerdict::FreshCurrent,
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
            &["wallet-old-a", "wallet-old-b"],
            &["wallet-old-a", "wallet-old-b"],
            &["wallet-old-a", "wallet-old-b"],
            &["wallet-old-a", "wallet-old-b"],
            &["wallet-old-a", "wallet-old-b"],
            &["wallet-old-c"],
            &["wallet-old-b"],
        ),
    )?;

    let report = discovery.wallet_freshness_history_report_with_horizon(
        &store,
        ts("2026-03-25T12:05:00Z"),
        5,
        3_600,
    )?;

    assert_eq!(
        report.verdict,
        WalletFreshnessHistoryVerdict::ValidatedCurrent
    );
    assert_eq!(report.captures_loaded, 4);
    assert_eq!(report.captures_considered, 3);
    assert_eq!(report.captures_within_recent_horizon, 3);
    assert_eq!(report.stale_captures_excluded_count, 1);
    assert!(report.stale_captures_excluded_from_verdict);
    Ok(())
}
