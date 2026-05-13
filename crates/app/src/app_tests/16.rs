    #[test]
    fn startup_recent_v2_published_universe_ignores_stale_followlist_residue() -> Result<()> {
        let (store, db_path) =
            make_test_store("startup-published-universe-ignores-stale-followlist")?;
        let now = DateTime::parse_from_rfc3339("2026-03-17T13:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let last_published_at = now - chrono::Duration::minutes(42);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.follow_top_n = 1;
        config.min_score = 0.1;

        let metrics_window_start = last_published_at
            - chrono::Duration::days(config.scoring_window_days.max(1) as i64);

        store.upsert_wallet(
            "wallet_ranked_now",
            now - chrono::Duration::days(2),
            now - chrono::Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_ranked_now".to_string(),
            window_start: metrics_window_start,
            pnl: 2.4,
            win_rate: 0.8,
            trades: 6,
            closed_trades: 6,
            hold_median_seconds: 120,
            score: 0.9,
            buy_total: 6,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet(
            "wallet_stale_residue",
            now - chrono::Duration::minutes(20),
            "legacy_bootstrap_residue",
        )?;
        let expected_published_wallets = vec![
            "wallet_published_exact".to_string(),
            "wallet_published_second".to_string(),
        ];
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let publication_policy_fingerprint =
            discovery.discovery_v2_publication_policy_fingerprint(false);
        let publication_cursor = DiscoveryRuntimeCursor {
            ts_utc: last_published_at - chrono::Duration::seconds(2),
            slot: 42,
            signature: "startup-v2-publication-cursor".to_string(),
        };
        let advanced_runtime_cursor = DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::minutes(1),
            slot: 84,
            signature: "startup-v2-advanced-runtime-cursor".to_string(),
        };
        store.upsert_discovery_runtime_cursor(&advanced_runtime_cursor)?;
        store.set_discovery_publication_state_with_identity(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "startup_recent_published_universe".to_string(),
                last_published_at: Some(last_published_at),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                published_wallet_ids: Some(expected_published_wallets.clone()),
            },
            false,
            Some(publication_policy_fingerprint.as_str()),
            Some(&publication_cursor),
        )?;

        let startup_published_truth = startup_runtime_publication_truth(
            &discovery,
            db_path.to_str().expect("utf8 db path"),
            now,
        )?
        .expect("startup published universe should be available from publication truth");
        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                initial_active_wallets,
                Some(&startup_published_truth),
            );

        assert_eq!(recovered_active_wallets, 0);
        assert!(!shadow_strategy_fail_closed);
        assert_eq!(
            snapshot.active,
            HashSet::from([
                "wallet_published_exact".to_string(),
                "wallet_published_second".to_string(),
            ]),
            "startup runtime snapshot must come from the exact published universe control plane, not from stale followlist residue or current ranking output"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_v2_publication_truth_requires_valid_v2_schema() -> Result<()> {
        let (store, db_path) = make_test_store("startup-v2-schema-validation")?;
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.metric_snapshot_interval_seconds = 30 * 60;
        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let runtime_cursor = DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::minutes(1),
            slot: 44,
            signature: "startup-v2-schema-cursor".to_string(),
        };
        store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
        store.set_discovery_publication_state_with_identity(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "startup_v2_schema_validation".to_string(),
                last_published_at: Some(now - chrono::Duration::minutes(10)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_published_exact".to_string()]),
            },
            false,
            Some(
                discovery
                    .discovery_v2_publication_policy_fingerprint(false)
                    .as_str(),
            ),
            Some(&runtime_cursor),
        )?;
        drop(store);

        rusqlite::Connection::open(&db_path)?.execute(
            "DROP INDEX IF EXISTS idx_observed_swaps_token_in_ts",
            [],
        )?;
        let truth = startup_runtime_publication_truth(
            &discovery,
            db_path.to_str().expect("utf8 db path"),
            now,
        )
        .expect("schema validation failure must fail closed, not abort startup");
        assert!(
            truth.is_none(),
            "valid V2 publication identity must still require V2 schema validation"
        );
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_v2_publication_truth_without_cursor_identity_keeps_runtime_fail_closed() -> Result<()>
    {
        let (store, db_path) = make_test_store("startup-v2-missing-cursor-identity")?;
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.metric_snapshot_interval_seconds = 30 * 60;
        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        store.activate_follow_wallet(
            "wallet_stale_residue",
            now - chrono::Duration::minutes(20),
            "legacy_bootstrap_residue",
        )?;
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "startup_v2_missing_cursor_identity".to_string(),
                last_published_at: Some(now - chrono::Duration::minutes(10)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_published_exact".to_string()]),
            },
            false,
            Some(discovery.discovery_v2_publication_policy_fingerprint(false).as_str()),
        )?;

        let startup_published_truth = startup_runtime_publication_truth(
            &discovery,
            db_path.to_str().expect("utf8 db path"),
            now,
        )?;
        assert!(startup_published_truth.is_none());
        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (_snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(initial_active_wallets, None);

        assert_eq!(recovered_active_wallets, 1);
        assert!(shadow_strategy_fail_closed);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_future_dated_v2_publication_truth_keeps_runtime_fail_closed() -> Result<()> {
        let (store, db_path) = make_test_store("startup-v2-future-dated-publication")?;
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.metric_snapshot_interval_seconds = 30 * 60;
        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);
        let future_published_at = now + chrono::Duration::minutes(5);
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let runtime_cursor = DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::minutes(1),
            slot: 43,
            signature: "startup-v2-future-cursor".to_string(),
        };
        store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
        store.set_discovery_publication_state_with_identity(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "startup_v2_future_dated_publication".to_string(),
                last_published_at: Some(future_published_at),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_published_exact".to_string()]),
            },
            false,
            Some(
                discovery
                    .discovery_v2_publication_policy_fingerprint(false)
                    .as_str(),
            ),
            Some(&runtime_cursor),
        )?;

        let startup_published_truth = startup_runtime_publication_truth(
            &discovery,
            db_path.to_str().expect("utf8 db path"),
            now,
        )?;

        assert!(startup_published_truth.is_none());
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_legacy_publication_truth_keeps_runtime_fail_closed() -> Result<()> {
        let (store, db_path) =
            make_test_store("startup-bootstrap-degraded-ignores-stale-followlist")?;
        let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.refresh_seconds = 600;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.follow_top_n = 1;
        config.min_score = 0.1;

        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);

        store.activate_follow_wallet(
            "wallet_stale_residue",
            now - chrono::Duration::minutes(20),
            "legacy_bootstrap_residue",
        )?;
        let publication_policy_fingerprint = test_publication_policy_fingerprint(&config);
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "stale_imported_runtime_artifact".to_string(),
                last_published_at: Some(now - chrono::Duration::minutes(61)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("legacy_publication_source".to_string()),
                published_wallet_ids: Some(vec![
                    "wallet_bootstrap_exact".to_string(),
                    "wallet_bootstrap_second".to_string(),
                ]),
            },
            false,
            Some(publication_policy_fingerprint.as_str()),
        )?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(now - chrono::Duration::minutes(5)),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let startup_published_truth = startup_runtime_publication_truth(
            &discovery,
            db_path.to_str().expect("utf8 db path"),
            now,
        )?;
        assert!(startup_published_truth.is_none());
        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                initial_active_wallets,
                startup_published_truth.as_ref(),
            );

        assert_eq!(recovered_active_wallets, 1);
        assert!(shadow_strategy_fail_closed);
        assert_eq!(
            snapshot.active,
            HashSet::new(),
            "explicit bootstrap-degraded startup must stay fail-closed instead of promoting stale imported publication truth into the runtime follow snapshot"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_v2_identity_rejects_unhealthy_missing_fingerprint_and_stale_cursor() -> Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let cases = [
            ("unhealthy", DiscoveryRuntimeMode::FailClosed, true, 60),
            (
                "missing-fingerprint",
                DiscoveryRuntimeMode::Healthy,
                false,
                60,
            ),
            (
                "stale-cursor",
                DiscoveryRuntimeMode::Healthy,
                true,
                5401,
            ),
            (
                "future-cursor",
                DiscoveryRuntimeMode::Healthy,
                true,
                -1,
            ),
        ];

        for (case_name, runtime_mode, include_fingerprint, cursor_age_seconds) in cases {
            let (store, db_path) = make_test_store(case_name)?;
            let mut config = copybot_config::DiscoveryConfig::default();
            config.scoring_window_days = 2;
            config.metric_snapshot_interval_seconds = 30 * 60;
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            let metrics_window_start =
                bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);
            let discovery = DiscoveryService::new(config, permissive_shadow_quality());
            let current_cursor = DiscoveryRuntimeCursor {
                ts_utc: now - chrono::Duration::seconds(cursor_age_seconds),
                slot: 101,
                signature: format!("{case_name}-current-cursor"),
            };
            let fingerprint = include_fingerprint
                .then(|| discovery.discovery_v2_publication_policy_fingerprint(false));
            store.upsert_discovery_runtime_cursor(&current_cursor)?;
            store.set_discovery_publication_state_with_identity(
                &DiscoveryPublicationStateUpdate {
                    runtime_mode,
                    reason: format!("startup_v2_identity_{case_name}"),
                    last_published_at: Some(now - chrono::Duration::minutes(10)),
                    last_published_window_start: Some(metrics_window_start),
                    published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                    published_wallet_ids: Some(vec!["wallet_published_exact".to_string()]),
                },
                false,
                fingerprint.as_deref(),
                Some(&current_cursor),
            )?;

            let startup_published_truth = startup_runtime_publication_truth(
                &discovery,
                db_path.to_str().expect("utf8 db path"),
                now,
        )?;

            assert!(startup_published_truth.is_none(), "{case_name}");
            let _ = std::fs::remove_file(db_path);
        }
        Ok(())
    }

    #[test]
    fn startup_trusted_selection_gate_status_falls_back_to_legacy_bool_when_typed_row_absent(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("trusted-selection-startup-legacy-bool-fallback")?;
        store.set_discovery_trusted_selection_bootstrap_required(true, "legacy_bool_only")?;

        let status = store.startup_trusted_selection_gate_status()?;
        assert!(status.bootstrap_required);
        assert!(status.startup_fail_closed);
        assert_eq!(status.selection_state, None);
        assert_eq!(status.reason.as_deref(), Some("legacy_bool_only"));
        assert!(status.legacy_bool_fallback_used);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_without_recent_published_universe_does_not_recover_open_shadow_lots() -> Result<()>
    {
        let (store, db_path) = make_test_store("trusted-selection-startup-shadow-fail-close")?;
        let now = Utc::now();
        store.insert_shadow_lot("wallet-sell", "token-a", 5.0, 1.2, now)?;
        store.set_discovery_trusted_selection_bootstrap_required(false, "legacy_false")?;
        store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
            bootstrap_required: false,
            reason: "typed_invalid_selection".to_string(),
            selection_state: TrustedSelectionState::Invalid,
            active_snapshot_id: Some("snapshot-invalid".to_string()),
            active_snapshot_window_start: Some(now),
            last_bootstrap_source_kind: Some(TrustedSnapshotSourceKind::Legacy),
            last_bootstrap_at: Some(now),
        })?;

        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (follow_snapshot, _recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(initial_active_wallets, None);
        let stored_open_shadow_lots = store.list_shadow_open_pairs()?;

        let mut swap = test_swap("sig-sell-bootstrap-invalid");
        swap.wallet = "wallet-sell".to_string();
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();

        assert!(shadow_strategy_fail_closed);
        assert_eq!(
            stored_open_shadow_lots,
            HashSet::from([("wallet-sell".to_string(), "token-a".to_string())])
        );
        let startup_open_shadow_lots = if shadow_strategy_fail_closed {
            HashSet::new()
        } else {
            stored_open_shadow_lots
        };
        assert!(
            startup_open_shadow_lots.is_empty(),
            "fail-closed startup must not recover historical open shadow lots"
        );
        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &startup_open_shadow_lots,
            ),
            ObservedSwapShadowRelevance::IrrelevantNotFollowed(ShadowSwapSide::Sell)
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
