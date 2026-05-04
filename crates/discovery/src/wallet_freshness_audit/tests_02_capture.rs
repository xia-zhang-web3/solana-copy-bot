    #[test]
    fn capture_snapshot_reports_missing_shadow_signal_evidence() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-shadow-missing.db");
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

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture =
            discovery.wallet_freshness_capture_snapshot(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            capture.shadow_signal.verdict,
            WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals
        );
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
            0
        );
        Ok(())
    }

    #[test]
    fn measured_capture_snapshot_keeps_exact_stage_three_semantics_for_scheduled_mode() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-scheduled-capture.db");
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
        let capture = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
            &store,
            now,
            1,
            Some(960),
        )?;

        assert_eq!(
            capture.snapshot.audit.verdict,
            WalletFreshnessVerdict::FreshCurrent
        );
        assert_eq!(
            capture.snapshot.shadow_signal.verdict,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated
        );
        assert_eq!(capture.snapshot.recent_cycles, 1);
        assert!(!capture.snapshot.audit.rotation.signal_available);
        assert_eq!(
            capture.snapshot.shadow_signal.evidence_lookback_seconds,
            Some(960)
        );
        assert_eq!(capture.snapshot.shadow_signal.selected_wallet_count, 2);
        assert_eq!(
            capture
                .snapshot
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        Ok(())
    }

    #[test]
    fn explicit_shadow_evidence_lookback_covers_scheduled_timer_gap_without_blind_spot(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-scheduled-gap-free.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:15:00Z");
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
            signal_id: "shadow:sig:wallet-alpha-gap".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            // This lands in the 5 minute blind interval that existed between
            // a 15 minute timer cadence and the old 10 minute evidence window.
            ts: now - Duration::minutes(11),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
            &store,
            now,
            1,
            Some(960),
        )?;

        assert_eq!(
            capture.snapshot.shadow_signal.evidence_lookback_seconds,
            Some(960)
        );
        assert_eq!(
            capture.snapshot.shadow_signal.recent_window_start,
            now - Duration::seconds(960)
        );
        assert_eq!(
            capture
                .snapshot
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        assert_eq!(
            capture
                .snapshot
                .shadow_signal
                .recent_shadow_signal_wallet_ids,
            vec!["wallet-alpha".to_string()]
        );
        Ok(())
    }

    #[test]
    fn persisted_capture_with_gap_free_lookback_remains_visible_to_history_report() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-scheduled-gap-history.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:15:00Z");
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
            signal_id: "shadow:sig:wallet-alpha-gap-history".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now - Duration::minutes(11),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
            &store,
            now,
            1,
            Some(960),
        )?;
        persist_capture(&store, capture.snapshot)?;

        let report =
            discovery.wallet_freshness_history_report_with_horizon(&store, now, 5, 3_600)?;

        assert_eq!(report.captures_loaded, 1);
        assert_eq!(report.shadow_signal_present_capture_count, 1);
        assert_eq!(
            report.captures[0].shadow_signal.evidence_lookback_seconds,
            Some(960)
        );
        assert_eq!(
            report.captures[0]
                .shadow_signal
                .recent_shadow_signal_wallet_ids,
            vec!["wallet-alpha".to_string()]
        );
        Ok(())
    }
