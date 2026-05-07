use super::*;

#[test]
fn fresh_exact_published_universe_matches_current_raw_truth() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-fresh.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_ranked_wallet_window(
        &store,
        now,
        &[
            ("wallet-alpha", "mint-a", 4, 0),
            ("wallet-beta", "mint-b", 3, 10),
            ("wallet-gamma", "mint-c", 1, 20),
        ],
    )?;
    seed_publication_truth(
        &store,
        &config,
        now - Duration::seconds(60),
        now,
        DiscoveryRuntimeMode::Healthy,
        &["wallet-alpha", "wallet-beta"],
    )?;
    store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(report.verdict, WalletFreshnessVerdict::FreshCurrent);
    assert!(report.raw_truth.sufficient);
    assert_eq!(
        report.current_raw_top_wallet_ids,
        vec!["wallet-alpha".to_string(), "wallet-beta".to_string()]
    );
    assert!(report.published_vs_current_raw.exact_match);
    assert!(report.active_follow_vs_published.exact_match);
    Ok(())
}

#[test]
fn stale_published_universe_with_meaningful_drift_is_detected() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-stale.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_ranked_wallet_window(
        &store,
        now,
        &[
            ("wallet-alpha", "mint-a", 4, 0),
            ("wallet-beta", "mint-b", 3, 10),
            ("wallet-gamma", "mint-c", 1, 20),
        ],
    )?;
    seed_publication_truth(
        &store,
        &config,
        now - Duration::hours(3),
        now - Duration::hours(3),
        DiscoveryRuntimeMode::BootstrapDegraded,
        &["wallet-beta", "wallet-legacy"],
    )?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
    store.activate_follow_wallet("wallet-legacy", now, "test-follow")?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(
        report.verdict,
        WalletFreshnessVerdict::StalePublicationTruth
    );
    assert!(report.raw_truth.sufficient);
    assert_eq!(report.reason, "publication_truth_not_recent_under_gate");
    assert_eq!(
        report.published_vs_current_raw.only_left,
        vec!["wallet-legacy".to_string()]
    );
    assert_eq!(
        report.published_vs_current_raw.only_right,
        vec!["wallet-alpha".to_string()]
    );
    Ok(())
}

#[test]
fn fail_closed_publication_state_with_preserved_exact_set_is_audited_as_truth() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-freshness-fail-closed-preserved.db");
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

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(report.verdict, WalletFreshnessVerdict::FreshCurrent);
    assert!(report.publication_truth_available);
    assert_eq!(
        report.publication_runtime_mode.as_deref(),
        Some("fail_closed")
    );
    Ok(())
}

#[test]
fn no_publication_truth_returns_fail_closed_verdict() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-no-publication.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_ranked_wallet_window(&store, now, &[("wallet-alpha", "mint-a", 3, 0)])?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(
        report.verdict,
        WalletFreshnessVerdict::FailClosedNoPublicationTruth
    );
    assert_eq!(report.reason, "no_complete_publication_truth");
    Ok(())
}

#[test]
fn insufficient_recent_raw_truth_is_reported_explicitly() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-insufficient.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_recent_only_ranked_wallet_window(&store, now, &[("wallet-alpha", "mint-a", 3, 5)])?;
    seed_publication_truth(
        &store,
        &config,
        now - Duration::seconds(60),
        now,
        DiscoveryRuntimeMode::Healthy,
        &["wallet-alpha"],
    )?;
    store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(report.verdict, WalletFreshnessVerdict::InsufficientRawTruth);
    assert_eq!(
        report.raw_truth.reason,
        "observed_swaps_coverage_starts_after_scoring_window_start"
    );
    assert!(!report.raw_truth.sufficient);
    assert!(report.raw_truth.tail_fresh_within_runtime_lag);
    Ok(())
}

#[test]
fn stale_raw_tail_freshness_is_reported_as_insufficient_raw_truth() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-stale-tail.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    let refresh_seconds = config.refresh_seconds;
    seed_ranked_wallet_window(
        &store,
        now - Duration::minutes(25),
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
        DiscoveryRuntimeMode::Healthy,
        &["wallet-alpha", "wallet-beta"],
    )?;
    store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(report.verdict, WalletFreshnessVerdict::InsufficientRawTruth);
    assert_eq!(
        report.raw_truth.reason,
        "observed_swaps_coverage_ends_before_freshness_gate"
    );
    assert!(!report.raw_truth.tail_fresh_within_runtime_lag);
    assert!(report
        .raw_truth
        .covered_through_lag_seconds
        .is_some_and(|lag| lag > refresh_seconds));
    Ok(())
}

#[test]
fn active_follow_residue_does_not_masquerade_as_fresh_publication_truth() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-active-residue.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_ranked_wallet_window(
        &store,
        now,
        &[
            ("wallet-alpha", "mint-a", 3, 0),
            ("wallet-beta", "mint-b", 2, 10),
        ],
    )?;
    store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(
        report.verdict,
        WalletFreshnessVerdict::FailClosedNoPublicationTruth
    );
    assert!(report.active_follow_vs_current_raw.exact_match);
    assert!(!report.publication_truth_available);
    Ok(())
}

#[test]
fn fail_closed_preserved_set_with_raw_drift_is_not_reported_as_missing_publication_truth(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-fail-closed-drift.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    seed_ranked_wallet_window(
        &store,
        now,
        &[
            ("wallet-alpha", "mint-a", 4, 0),
            ("wallet-beta", "mint-b", 3, 10),
            ("wallet-gamma", "mint-c", 1, 20),
        ],
    )?;
    seed_publication_truth(
        &store,
        &config,
        now - Duration::hours(3),
        now - Duration::hours(3),
        DiscoveryRuntimeMode::FailClosed,
        &["wallet-beta", "wallet-legacy"],
    )?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
    store.activate_follow_wallet("wallet-legacy", now, "test-follow")?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(
        report.verdict,
        WalletFreshnessVerdict::StalePublicationTruth
    );
    assert!(report.publication_truth_available);
    assert_eq!(
        report.publication_runtime_mode.as_deref(),
        Some("fail_closed")
    );
    assert_ne!(report.reason, "no_complete_publication_truth");
    Ok(())
}

#[test]
fn aggregate_recovery_state_does_not_affect_verdict() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-aggregate-ignored.db");
    let store = open_store(&db_path)?;
    let now = ts("2026-03-25T12:00:00Z");
    let config = freshness_test_config();
    let window_start = now - Duration::days(config.scoring_window_days as i64);
    seed_ranked_wallet_window(
        &store,
        now,
        &[
            ("wallet-alpha", "mint-a", 3, 0),
            ("wallet-beta", "mint-b", 2, 10),
        ],
    )?;
    seed_publication_truth(
        &store,
        &config,
        now - Duration::seconds(60),
        now,
        DiscoveryRuntimeMode::Healthy,
        &["wallet-alpha", "wallet-beta"],
    )?;
    store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
    store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
    store.set_discovery_scoring_covered_since(window_start + Duration::hours(12))?;
    store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now,
        slot: 99,
        signature: "aggregate-sig".to_string(),
    })?;
    store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now - Duration::hours(1),
        slot: 88,
        signature: "aggregate-gap".to_string(),
    })?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(report.verdict, WalletFreshnessVerdict::FreshCurrent);
    Ok(())
}

#[test]
fn capture_snapshot_includes_shadow_signal_evidence_for_selected_wallets() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-freshness-shadow-present.db");
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
        DiscoveryRuntimeMode::Healthy,
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
        ts: now - Duration::seconds(45),
        status: "shadow_recorded".to_string(),
    })?;

    let discovery = DiscoveryService::new(config, ShadowConfig::default());
    let capture =
        discovery.wallet_freshness_capture_snapshot(&store, now, DEFAULT_RECENT_CYCLES)?;

    assert_eq!(
        capture.shadow_signal.verdict,
        WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated
    );
    assert_eq!(capture.shadow_signal.selected_wallet_count, 2);
    assert_eq!(
        capture
            .shadow_signal
            .selected_wallets_with_recent_raw_activity,
        2
    );
    assert_eq!(
        capture
            .shadow_signal
            .selected_wallets_with_recent_shadow_signal,
        1
    );
    assert_eq!(
        capture.shadow_signal.recent_shadow_signal_wallet_ids,
        vec!["wallet-alpha".to_string()]
    );
    Ok(())
}
