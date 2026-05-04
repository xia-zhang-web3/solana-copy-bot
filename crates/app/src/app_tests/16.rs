    #[test]
    fn startup_recent_published_universe_ignores_stale_followlist_residue() -> Result<()> {
        let (store, db_path) =
            make_test_store("startup-published-universe-ignores-stale-followlist")?;
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.follow_top_n = 1;
        config.min_score = 0.1;

        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);

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
        let publication_policy_fingerprint = test_publication_policy_fingerprint(&config);
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "startup_recent_published_universe".to_string(),
                last_published_at: Some(now - chrono::Duration::minutes(10)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(expected_published_wallets.clone()),
            },
            false,
            Some(publication_policy_fingerprint.as_str()),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let startup_published_truth = startup_runtime_publication_truth(&discovery, &store, now)?
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
    fn startup_bootstrap_degraded_publication_truth_keeps_runtime_fail_closed() -> Result<()> {
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
                published_scoring_source: Some("raw_window".to_string()),
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
        let startup_published_truth = startup_runtime_publication_truth(&discovery, &store, now)?
            .expect("bootstrap-degraded publication truth should survive startup");
        assert!(matches!(
            startup_published_truth,
            RuntimePublicationTruthResolution::BootstrapDegraded(_)
        ));
        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                initial_active_wallets,
                Some(&startup_published_truth),
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
    fn startup_without_recent_published_universe_keeps_historical_open_shadow_lots_loaded(
    ) -> Result<()> {
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
        let open_shadow_lots = if shadow_strategy_fail_closed {
            HashSet::new()
        } else {
            store.list_shadow_open_pairs()?
        };

        let mut swap = test_swap("sig-sell-bootstrap-invalid");
        swap.wallet = "wallet-sell".to_string();
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();

        assert!(!shadow_strategy_fail_closed);
        assert!(
            !open_shadow_lots.is_empty(),
            "legacy trusted-selection recovery bookkeeping must not clear historical open shadow lots before discovery decides the runtime contract"
        );
        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &open_shadow_lots,
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn select_role_helius_http_url_prefers_role_specific() {
        let selected = select_role_helius_http_url(
            "https://role.endpoint/?api-key=abc",
            "https://fallback.endpoint/?api-key=def",
        );
        assert_eq!(
            selected.as_deref(),
            Some("https://role.endpoint/?api-key=abc")
        );
    }

    #[test]
    fn select_role_helius_http_url_falls_back_and_rejects_placeholders() {
        let selected = select_role_helius_http_url(
            "https://role.endpoint/?api-key=REPLACE_ME",
            "https://fallback.endpoint/?api-key=def",
        );
        assert_eq!(
            selected.as_deref(),
            Some("https://fallback.endpoint/?api-key=def")
        );

        let selected_none = select_role_helius_http_url("", "https://x/?api-key=REPLACE_ME");
        assert!(selected_none.is_none());

        let selected_lowercase = select_role_helius_http_url(
            "https://role.endpoint/?api-key=replace_me",
            "https://fallback.endpoint/?api-key=def",
        );
        assert_eq!(
            selected_lowercase.as_deref(),
            Some("https://fallback.endpoint/?api-key=def")
        );
    }

    #[test]
    fn follow_event_retention_spans_discovery_publish_cadence() {
        let retention = follow_event_retention_duration(30, 600);
        assert_eq!(retention, Duration::from_secs(1200));

        let retention = follow_event_retention_duration(2500, 600);
        assert_eq!(retention, Duration::from_secs(2500));
    }

    #[test]
    fn enforce_quality_gate_http_url_requires_endpoint_for_paper_prod() {
        let result = enforce_quality_gate_http_url("shadow", "paper", true, None);
        assert!(result.is_err());

        let result = enforce_quality_gate_http_url("shadow", "paper-canary", true, None);
        assert!(result.is_err());

        let result = enforce_quality_gate_http_url(
            "shadow",
            "prod",
            true,
            Some("https://endpoint".to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn enforce_quality_gate_http_url_is_lenient_for_dev_or_disabled_gates() {
        let result = enforce_quality_gate_http_url("shadow", "dev", true, None);
        assert!(result.is_ok());

        let result = enforce_quality_gate_http_url("shadow", "paper", false, None);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_shadow_quality_gate_contract_rejects_all_zero_thresholds_when_enabled() {
        let mut cfg = ShadowConfig::default();
        cfg.quality_gates_enabled = true;
        cfg.min_token_age_seconds = 0;
        cfg.min_holders = 0;
        cfg.min_liquidity_sol = 0.0;
        cfg.min_volume_5m_sol = 0.0;
        cfg.min_unique_traders_5m = 0;
        let err = validate_shadow_quality_gate_contract(&cfg, "prod")
            .expect_err("all-zero quality thresholds must fail when gates are enabled")
            .to_string();
        assert!(err.contains("all quality thresholds are zero"));
    }

    #[test]
    fn validate_shadow_quality_gate_contract_allows_non_zero_thresholds_when_enabled() {
        let mut cfg = ShadowConfig::default();
        cfg.quality_gates_enabled = true;
        cfg.min_token_age_seconds = 30;
        cfg.min_holders = 5;
        cfg.min_liquidity_sol = 1.0;
        cfg.min_volume_5m_sol = 0.5;
        cfg.min_unique_traders_5m = 2;
        validate_shadow_quality_gate_contract(&cfg, "prod")
            .expect("non-zero thresholds should pass quality gate validation");
    }

    #[test]
    fn parse_ingestion_source_override_reads_supported_source_key() {
        let content = r#"
# comment
SOLANA_COPY_BOT_INGESTION_SOURCE=yellowstone_grpc
"#;
        assert_eq!(
            parse_ingestion_source_override(content)
                .expect("valid override should parse")
                .as_deref(),
            Some("yellowstone_grpc")
        );
    }

    #[test]
    fn parse_ingestion_source_override_ignores_invalid_lines() {
        let content = r#"
FOO=bar
SOLANA_COPY_BOT_INGESTION_SOURCE=
this-is-not-a-valid-line
"#;
        assert!(parse_ingestion_source_override(content)
            .expect("invalid lines without source value should be ignored")
            .is_none());
    }

    #[test]
    fn parse_ingestion_source_override_rejects_invalid_source_value() {
        let content = r#"
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws
"#;
        let error = parse_ingestion_source_override(content)
            .expect_err("unsupported override source must warn and reject");
        assert!(
            error.to_string().contains("helius_ws"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_ingestion_source_override_normalizes_alias() {
        let content = r#"
SOLANA_COPY_BOT_INGESTION_SOURCE=yellowstone
"#;
        assert_eq!(
            parse_ingestion_source_override(content)
                .expect("yellowstone alias should normalize")
                .as_deref(),
            Some("yellowstone_grpc")
        );
    }

    #[test]
    fn observed_swap_writer_closed_channel_error_requires_restart() {
        let error = anyhow!("observed swap writer channel closed");
        assert!(observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_writer_closed_reply_error_requires_restart() {
        let error = anyhow!("observed swap writer reply channel closed");
        assert!(observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_writer_terminal_failure_error_requires_restart() {
        let error = anyhow!("forced async observed swap failure")
            .context(OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT);
        assert!(observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn startup_sqlite_wal_checkpoint_error_requires_abort_on_xshmmap_io_failure() {
        let primary = anyhow!("database is locked");
        let fallback =
            anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(!startup_sqlite_wal_checkpoint_error_requires_abort(
            Some(&primary),
            None
        ));
        assert!(startup_sqlite_wal_checkpoint_error_requires_abort(
            Some(&primary),
            Some(&fallback)
        ));
    }

    #[test]
    fn startup_sqlite_wal_checkpoint_error_does_not_abort_on_busy_only_failures() {
        let primary = anyhow!("database is locked");
        let fallback = anyhow!("database is busy");
        assert!(!startup_sqlite_wal_checkpoint_error_requires_abort(
            Some(&primary),
            Some(&fallback)
        ));
    }

    #[test]
    fn inline_startup_step_reports_started_and_completed() -> Result<()> {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let value = run_inline_startup_step(
            &reporter,
            "startup_inline_complete",
            Some(StdDuration::from_millis(50)),
            || Ok::<u64, anyhow::Error>(7),
        )?;
        assert_eq!(value, 7);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_complete"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "inline startup step must emit a started outcome"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_complete"
                    && event.outcome == StartupStepOutcome::Completed
            }),
            "inline startup step must emit a completed outcome"
        );
        Ok(())
    }

    #[test]
    fn inline_startup_step_reports_failed_outcome() {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let error = run_inline_startup_step(
            &reporter,
            "startup_inline_failure",
            Some(StdDuration::from_millis(50)),
            || -> Result<()> { Err(anyhow!("inline startup failure")) },
        )
        .expect_err("failed inline startup step must surface an explicit error");
        assert!(
            error
                .to_string()
                .contains("startup step startup_inline_failure failed"),
            "failed inline startup step must be wrapped with the stage name"
        );

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_failure"
                    && event.outcome == StartupStepOutcome::Failed
                    && event
                        .detail
                        .as_deref()
                        .is_some_and(|detail| detail.contains("inline startup failure"))
            }),
            "failed inline startup step must emit an explicit failed outcome with detail"
        );
    }

    #[test]
    fn skipped_inline_startup_step_reports_started_and_skipped() {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        skip_inline_startup_step(&reporter, "startup_inline_skipped", "not_required_for_test");

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_skipped"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "skipped inline startup step must emit a started outcome"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_skipped"
                    && event.outcome == StartupStepOutcome::Skipped
                    && event.detail.as_deref() == Some("not_required_for_test")
            }),
            "skipped inline startup step must emit an explicit skipped outcome with detail"
        );
    }

    #[test]
    fn startup_step_policy_uses_fatal_timeout_behavior() {
        let policy = startup_step_policy(StdDuration::from_secs(30));
        assert_eq!(
            policy.timeout_behavior,
            StartupStepTimeoutBehavior::AbortProcess
        );
    }

    #[test]
    fn startup_large_wal_checkpoint_policy_uses_longer_budget_than_sqlite_pragmas_stage1() {
        let policy = sqlite_startup_policy();
        assert!(
            policy.large_wal_checkpoint_step.timeout > policy.pragma_step.timeout,
            "large WAL checkpoint must not reuse the short pragma timeout"
        );
        assert_eq!(
            policy.large_wal_checkpoint_step.timeout,
            Some(STARTUP_LARGE_WAL_CHECKPOINT_TIMEOUT)
        );
        assert_eq!(
            policy.pragma_step.timeout,
            Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT)
        );
    }
