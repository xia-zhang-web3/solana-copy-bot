#[test]
fn ingestion_yellowstone_defaults_are_applied() {
    let ingestion = IngestionConfig::default();
    assert_eq!(ingestion.yellowstone_grpc_url, "REPLACE_ME");
    assert_eq!(ingestion.yellowstone_x_token, "REPLACE_ME");
    assert_eq!(ingestion.yellowstone_connect_timeout_ms, 5_000);
    assert_eq!(ingestion.yellowstone_subscribe_timeout_ms, 15_000);
    assert_eq!(ingestion.yellowstone_stream_buffer_capacity, 2_048);
    assert_eq!(ingestion.yellowstone_reconnect_initial_ms, 500);
    assert_eq!(ingestion.yellowstone_reconnect_max_ms, 8_000);
    assert!(ingestion.yellowstone_program_ids.is_empty());
}

#[test]
fn shadow_defaults_use_conservative_min_holders_floor() {
    let shadow = ShadowConfig::default();
    assert_eq!(shadow.min_holders, 5);
}

#[test]
fn discovery_defaults_use_storage_mitigation_limits() {
    let discovery = DiscoveryConfig::default();
    assert_eq!(discovery.fetch_refresh_seconds, 60);
    assert_eq!(discovery.refresh_seconds, 600);
    assert_eq!(discovery.status_scan_window_minutes, 0);
    assert_eq!(discovery.maturity_window_days, 0);
    assert_eq!(discovery.maturity_min_active_days, 0);
    assert_eq!(discovery.maturity_score_bonus, 0.0);
    assert_eq!(discovery.metric_snapshot_interval_seconds, 1_800);
    assert_eq!(discovery.max_bootstrap_snapshot_age_seconds, 43_200);
    assert_eq!(discovery.max_window_swaps_in_memory, 60_000);
    assert_eq!(discovery.max_fetch_swaps_per_cycle, 20_000);
    assert_eq!(discovery.max_fetch_pages_per_cycle, 5);
    assert_eq!(discovery.fetch_time_budget_ms, 15_000);
    assert_eq!(discovery.observed_swaps_retention_days, 7);
    assert!(!discovery.scoring_aggregates_write_enabled);
    assert!(!discovery.scoring_aggregates_enabled);
}

#[test]
fn recent_raw_journal_defaults_are_explicit_and_bounded() {
    let journal = RecentRawJournalConfig::default();
    assert_eq!(journal.path, "state/discovery_recent_raw.db");
    assert_eq!(journal.retention_safety_buffer_days, 2);
    assert_eq!(journal.writer_queue_capacity_batches, 64);
    assert_eq!(journal.replay_batch_size, 1_024);
}

#[test]
fn runtime_restore_ops_defaults_are_explicit_and_operational() {
    let ops = RuntimeRestoreOpsConfig::default();
    assert_eq!(ops.artifact_dir, "state/discovery_restore/artifacts");
    assert_eq!(ops.artifact_retention, 288);
    assert_eq!(ops.artifact_cadence_minutes, 10);
    assert_eq!(
        ops.journal_snapshot_dir,
        "state/discovery_restore/recent_raw"
    );
    assert_eq!(ops.journal_snapshot_retention, 144);
    assert_eq!(ops.journal_snapshot_cadence_minutes, 10);
    assert_eq!(ops.drill_workspace_dir, "state/discovery_restore/drills");
}

#[test]
fn history_retention_defaults_are_explicit_and_safe() {
    let retention = HistoryRetentionConfig::default();
    assert!(!retention.enabled);
    assert_eq!(retention.sweep_seconds, 3_600);
    assert_eq!(retention.protected_history_days, 30);
    assert_eq!(retention.risk_events_days, 30);
    assert_eq!(retention.copy_signals_days, 30);
    assert_eq!(retention.orders_days, 30);
    assert_eq!(retention.fills_days, 30);
    assert_eq!(retention.shadow_closed_trades_days, 90);
}

#[test]
fn execution_defaults_are_fail_closed_and_canary_dry_run_only() {
    let execution = ExecutionConfig::default();
    assert!(!execution.enabled);
    assert!(!execution.canary_enabled);
    assert!(execution.canary_dry_run);
    assert_eq!(execution.canary_route, "dry_run");
    assert_eq!(execution.canary_interval_seconds, 15);
    assert_eq!(execution.canary_batch_limit, 1);
    assert_eq!(execution.canary_max_signal_age_seconds, 120);
    assert_eq!(execution.canary_buy_size_sol, 0.01);
    assert_eq!(execution.canary_max_open_positions, 1);
    assert_eq!(execution.canary_max_daily_loss_sol, 0.02);
    assert_eq!(
        execution.canary_kill_switch_path,
        "state/execution_canary.stop"
    );
    assert!(execution.canary_wallet_pubkey.is_empty());
}

#[test]
fn parse_from_path_uses_disabled_history_retention_for_legacy_config_without_block() {
    with_temp_config_file("", |config_path| {
        let config = load_from_path(config_path).expect("legacy config without block must parse");
        assert!(!config.history_retention.enabled);
    });
}

#[test]
fn parse_from_path_reads_runtime_restore_ops_block() {
    with_temp_config_file(
        r#"
[runtime_restore_ops]
artifact_dir = "restore/artifacts"
artifact_retention = 32
artifact_cadence_minutes = 5
journal_snapshot_dir = "restore/journal"
journal_snapshot_retention = 24
journal_snapshot_cadence_minutes = 10
drill_workspace_dir = "restore/drills"

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let config = load_from_path(config_path).expect("config must parse");
            assert_eq!(config.runtime_restore_ops.artifact_dir, "restore/artifacts");
            assert_eq!(config.runtime_restore_ops.artifact_retention, 32);
            assert_eq!(config.runtime_restore_ops.artifact_cadence_minutes, 5);
            assert_eq!(
                config.runtime_restore_ops.journal_snapshot_dir,
                "restore/journal"
            );
            assert_eq!(config.runtime_restore_ops.journal_snapshot_retention, 24);
            assert_eq!(
                config.runtime_restore_ops.journal_snapshot_cadence_minutes,
                10
            );
            assert_eq!(
                config.runtime_restore_ops.drill_workspace_dir,
                "restore/drills"
            );
        },
    );
}

#[test]
fn live_server_template_locks_publication_quality_gates() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let template_path = manifest_dir.join("../../ops/server_templates/live.server.toml.example");
    let live_path = manifest_dir.join("../../configs/live.toml");
    let prod_path = manifest_dir.join("../../configs/prod.toml");

    let template = load_from_path(&template_path).expect("live server template must parse");
    let live = load_from_path(&live_path).expect("live config must parse");
    let prod = load_from_path(&prod_path).expect("prod config must parse");

    assert!(template.discovery.require_open_positions_for_publication);
    assert_eq!(template.discovery.max_rug_ratio, 0.60);
    assert_eq!(template.discovery.thin_market_min_volume_sol, 3.0);
    assert_eq!(template.discovery.thin_market_min_unique_traders, 10);
    assert!(!template.execution.enabled);

    assert_eq!(
        template.discovery.require_open_positions_for_publication,
        live.discovery.require_open_positions_for_publication
    );
    assert_eq!(
        template.discovery.max_rug_ratio,
        live.discovery.max_rug_ratio
    );
    assert_eq!(
        template.discovery.thin_market_min_volume_sol,
        live.discovery.thin_market_min_volume_sol
    );
    assert_eq!(
        template.discovery.thin_market_min_unique_traders,
        live.discovery.thin_market_min_unique_traders
    );

    assert_eq!(
        template.discovery.require_open_positions_for_publication,
        prod.discovery.require_open_positions_for_publication
    );
    assert_eq!(
        template.discovery.max_rug_ratio,
        prod.discovery.max_rug_ratio
    );
    assert_eq!(
        template.discovery.thin_market_min_volume_sol,
        prod.discovery.thin_market_min_volume_sol
    );
    assert_eq!(
        template.discovery.thin_market_min_unique_traders,
        prod.discovery.thin_market_min_unique_traders
    );
}

#[test]
fn load_from_path_rejects_runtime_restore_ops_with_cadence_slower_than_freshness_gate() {
    with_temp_config_file(
        r#"
[runtime_restore_ops]
artifact_cadence_minutes = 30

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("artifact cadence at freshness gate boundary must fail")
                .to_string();
            assert!(
                err.contains("runtime_restore_ops.artifact_cadence_minutes (30) must be < freshness gate bucket minutes (30)"),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_non_finite_discovery_v2_float_gates() {
    with_temp_config_file(
        r#"
[discovery]
min_score = nan
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("non-finite discovery min_score must fail config load")
                .to_string();
            assert!(
                err.contains("discovery.min_score must be finite"),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_non_finite_shadow_quality_gates() {
    with_temp_config_file(
        r#"
[shadow]
min_liquidity_sol = inf
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("non-finite shadow min_liquidity_sol must fail config load")
                .to_string();
            assert!(
                err.contains("shadow.min_liquidity_sol must be finite"),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_env_rejects_empty_yellowstone_program_ids_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS",
        "\"\", '' ,",
        "must contain at least one non-empty value",
    );
}

#[test]
fn load_from_env_rejects_duplicate_helius_http_urls_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS",
        "https://rpc.example.com, https://rpc.example.com",
        "duplicate value after normalization",
    );
}

#[test]
fn load_from_env_rejects_duplicate_program_ids_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_PROGRAM_IDS",
        "program-a, program-a",
        "duplicate value after normalization",
    );
}

#[test]
fn load_from_env_applies_risk_and_shadow_quality_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL", "0.99", || {
                with_env_var(
                    "SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS",
                    "60",
                    || {
                        with_env_var(
                            "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL",
                            "9.5",
                            || {
                                with_env_var(
                                    "SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED",
                                    "false",
                                    || {
                                        with_env_var(
                                            "SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS",
                                            "42",
                                            || {
                                                let (cfg, _) = load_from_env_or_default(
                                                    config_path,
                                                )
                                                .expect(
                                                    "load config with risk/shadow env overrides",
                                                );
                                                assert!(
                                                    (cfg.risk.max_position_sol - 0.99).abs()
                                                        <= f64::EPSILON
                                                );
                                                assert_eq!(
                                                    cfg.risk.execution_buy_cooldown_seconds,
                                                    60
                                                );
                                                assert!(
                                                    (cfg.risk
                                                        .shadow_soft_exposure_resume_below_sol
                                                        - 9.5)
                                                        .abs()
                                                        <= f64::EPSILON
                                                );
                                                assert!(!cfg.risk.shadow_killswitch_enabled);
                                                assert_eq!(cfg.shadow.min_holders, 42);
                                            },
                                        );
                                    },
                                );
                            },
                        );
                    },
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_shadow_min_liquidity_sol_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_SHADOW_MIN_LIQUIDITY_SOL", "high", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid shadow min_liquidity_sol override must fail config load")
                    .to_string();
                assert!(
                    err.contains("SOLANA_COPY_BOT_SHADOW_MIN_LIQUIDITY_SOL"),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_max_concurrent_positions_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS",
                "-1",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk max_concurrent_positions override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_execution_buy_cooldown_seconds_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS",
                "cooldown",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk execution buy cooldown override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}
