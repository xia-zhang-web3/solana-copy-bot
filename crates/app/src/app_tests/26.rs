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
    fn validate_live_ingestion_source_contract_rejects_mock_for_prod_live() {
        let err = validate_live_ingestion_source_contract("mock", "prod-live")
            .expect_err("prod-live must not accept mock ingestion")
            .to_string();
        assert!(
            err.contains("ingestion.source=mock"),
            "unexpected error: {err}"
        );
        validate_live_ingestion_source_contract("yellowstone_grpc", "prod-live")
            .expect("yellowstone_grpc remains valid in prod-live");
        validate_live_ingestion_source_contract("mock", "dev")
            .expect("mock remains valid outside production envs");
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
    fn load_ingestion_source_override_reports_unreadable_override_file() {
        let _guard = APP_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = env::var_os("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE");
        let override_dir = std::env::temp_dir().join(format!(
            "copybot-ingestion-override-dir-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        ));
        std::fs::create_dir(&override_dir).expect("test override dir should be created");
        env::set_var("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE", &override_dir);

        let error = load_ingestion_source_override()
            .expect_err("unreadable override path must be reported");
        assert!(
            error.to_string().contains("failed reading ingestion source override file"),
            "unexpected error: {error:#}"
        );

        if let Some(previous) = previous {
            env::set_var("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE", previous);
        } else {
            env::remove_var("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE");
        }
        let _ = std::fs::remove_dir(override_dir);
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
