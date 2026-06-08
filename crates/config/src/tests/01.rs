#[test]
fn load_from_env_rejects_invalid_risk_shadow_soft_exposure_cap_sol_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL",
                "cap",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk shadow soft exposure cap override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_soft_exposure_resume_below_sol_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL",
                "resume",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk shadow soft exposure resume threshold override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_risk_shadow_stale_close_recovery_zero_price_enabled_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED",
                "true",
                || {
                    let (cfg, _) = load_from_env_or_default(config_path)
                        .expect("load config with recovery override");
                    assert!(cfg.risk.shadow_stale_close_recovery_zero_price_enabled);
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_stale_close_recovery_zero_price_enabled_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED",
                "sometimes",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk shadow stale close recovery override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains(
                            "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED"
                        ),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_universe_breach_cycles_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES",
                "many",
                || {
                    let err = load_from_env_or_default(config_path)
                    .expect_err(
                        "invalid risk shadow universe breach cycles override must fail config load",
                    )
                    .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_path_rejects_invalid_ingestion_queue_overflow_policy() {
    with_temp_config_file(
        r#"
[ingestion]
queue_overflow_policy = "drop_newest"
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("invalid ingestion queue_overflow_policy in config must fail")
                    .to_string();
                assert!(
                    err.contains("ingestion.queue_overflow_policy"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_path_rejects_invalid_ingestion_source() {
    with_temp_config_file(
        r#"
[ingestion]
source = "laserstream"
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("invalid ingestion.source in config must fail")
                    .to_string();
                assert!(err.contains("ingestion.source"), "unexpected error: {err}");
            });
        },
    );
}

#[test]
fn load_from_path_rejects_mock_ingestion_source_for_production_env() {
    with_temp_config_file(
        r#"
[system]
env = "prod-live"

[ingestion]
source = "mock"
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("mock ingestion must not load in production")
                    .to_string();
                assert!(
                    err.contains("ingestion.source=mock"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_env_rejects_mock_ingestion_source_override_for_production_env() {
    with_temp_config_file(
        r#"
[system]
env = "prod-live"

[ingestion]
source = "yellowstone_grpc"
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                with_env_var("SOLANA_COPY_BOT_INGESTION_SOURCE", "mock", || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("mock ingestion env override must not load in production")
                        .to_string();
                    assert!(
                        err.contains("ingestion.source=mock"),
                        "unexpected error: {err}"
                    );
                });
            });
        },
    );
}

#[test]
fn load_from_path_rejects_unsatisfiable_discovery_active_days() {
    with_temp_config_file(
        r#"
[discovery]
scoring_window_days = 1
min_active_days = 3
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("unsatisfiable V2 discovery policy must fail config load")
                    .to_string();
                assert!(
                    err.contains("discovery.min_active_days"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_path_rejects_invalid_discovery_maturity_window() {
    with_temp_config_file(
        r#"
[discovery]
maturity_window_days = 2
maturity_min_active_days = 3
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("unsatisfiable V2 maturity policy must fail config load")
                    .to_string();
                assert!(
                    err.contains("discovery.maturity_min_active_days"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_path_rejects_non_finite_discovery_maturity_bonus() {
    with_temp_config_file(
        r#"
[discovery]
maturity_window_days = 3
maturity_min_active_days = 3
maturity_score_bonus = nan
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("non-finite V2 maturity bonus must fail config load")
                    .to_string();
                assert!(
                    err.contains("discovery.maturity_score_bonus"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_path_rejects_legacy_discovery_aggregate_activation() {
    with_temp_config_file(
        r#"
[discovery]
scoring_aggregates_write_enabled = true
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("legacy aggregate activation must fail config load")
                    .to_string();
                assert!(
                    err.contains("legacy discovery aggregate scoring"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_env_normalizes_ingestion_source_alias() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_INGESTION_SOURCE", "yellowstone", || {
                let (cfg, _) = load_from_env_or_default(config_path)
                    .expect("known ingestion source alias should load");
                assert_eq!(cfg.ingestion.source, "yellowstone_grpc");
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_source_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_INGESTION_SOURCE", "laserstream", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid ingestion.source override must fail config load")
                    .to_string();
                assert!(err.contains("ingestion.source"), "unexpected error: {err}");
            });
        });
    });
}

#[test]
fn load_from_env_normalizes_ingestion_queue_overflow_policy_alias() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY",
                "drop-oldest",
                || {
                    let (cfg, _) = load_from_env_or_default(config_path)
                        .expect("known queue overflow alias should load");
                    assert_eq!(cfg.ingestion.queue_overflow_policy, "drop_oldest");
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_queue_overflow_policy_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY",
                "drop_newest",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("invalid queue overflow policy override must fail config load")
                        .to_string();
                    assert!(
                        err.contains("ingestion.queue_overflow_policy"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_enabled_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_ENABLED",
        "tru",
        "invalid execution.enabled env override must fail config load",
    );
}

#[test]
fn load_from_env_rejects_invalid_execution_canary_bool_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_CANARY_ENABLED",
        "yesplease",
        "invalid execution.canary_enabled env override must fail config load",
    );
}

#[test]
fn load_from_env_applies_execution_canary_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            for (key, value) in [
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_ENABLED", "true"),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_DRY_RUN", "true"),
                (
                    "SOLANA_COPY_BOT_EXECUTION_CANARY_TINY_SUBMIT_ENABLED",
                    "true",
                ),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_ROUTE", "metis-dry-run"),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_INTERVAL_SECONDS", "7"),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_BATCH_LIMIT", "3"),
                (
                    "SOLANA_COPY_BOT_EXECUTION_CANARY_MAX_SIGNAL_AGE_SECONDS",
                    "45",
                ),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_BUY_SIZE_SOL", "0.02"),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_MAX_OPEN_POSITIONS", "1"),
                ("SOLANA_COPY_BOT_EXECUTION_CANARY_MAX_DAILY_LOSS_SOL", "0.03"),
                (
                    "SOLANA_COPY_BOT_EXECUTION_CANARY_KILL_SWITCH_PATH",
                    "state/test-stop",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_CANARY_WALLET_PUBKEY",
                    "wallet-pubkey",
                ),
                ("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_ENABLED", "true"),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_BASE_URL",
                    "https://example.com/swap/v1",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_API_KEY",
                    "quote-key",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_TIMEOUT_MS",
                    "900",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_PUBLIC_PARALLEL_ENABLED",
                    "true",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_PUBLIC_BASE_URL",
                    "https://public.example.com",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_PUMP_FUN_PARALLEL_ENABLED",
                    "true",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_SWAP_INSTRUCTIONS_DRY_RUN_ENABLED",
                    "true",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_SWAP_TRANSACTION_DRY_RUN_ENABLED",
                    "true",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_BUY_SIZE_SOL",
                    "0.2",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_SLIPPAGE_BPS",
                    "120",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_BUY_SLIPPAGE_BPS",
                    "150",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_SELL_SLIPPAGE_BPS",
                    "500",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_ENABLED",
                    "true",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_RPC_URL",
                    "https://example.com/rpc",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_TIMEOUT_MS",
                    "800",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_ACCOUNT",
                    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
                ),
                (
                    "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_LAST_N_BLOCKS",
                    "80",
                ),
            ] {
                std::env::set_var(key, value);
            }
            let (config, _) = load_from_env_or_default(config_path)
                .expect("execution canary env overrides must parse");
            assert!(config.execution.canary_enabled);
            assert!(config.execution.canary_dry_run);
            assert!(config.execution.canary_tiny_submit_enabled);
            assert_eq!(config.execution.canary_route, "metis-dry-run");
            assert_eq!(config.execution.canary_interval_seconds, 7);
            assert_eq!(config.execution.canary_batch_limit, 3);
            assert_eq!(config.execution.canary_max_signal_age_seconds, 45);
            assert_eq!(config.execution.canary_buy_size_sol, 0.02);
            assert_eq!(config.execution.canary_max_open_positions, 1);
            assert_eq!(config.execution.canary_max_daily_loss_sol, 0.03);
            assert_eq!(
                config.execution.canary_kill_switch_path,
                "state/test-stop"
            );
            assert_eq!(config.execution.canary_wallet_pubkey, "wallet-pubkey");
            assert!(config.execution.quote_canary_enabled);
            assert_eq!(
                config.execution.quote_canary_base_url,
                "https://example.com/swap/v1"
            );
            assert_eq!(config.execution.quote_canary_api_key, "quote-key");
            assert_eq!(config.execution.quote_canary_timeout_ms, 900);
            assert!(config.execution.quote_canary_public_parallel_enabled);
            assert_eq!(
                config.execution.quote_canary_public_base_url,
                "https://public.example.com"
            );
            assert!(config.execution.quote_canary_pump_fun_parallel_enabled);
            assert!(config.execution.swap_instructions_dry_run_enabled);
            assert!(config.execution.swap_transaction_dry_run_enabled);
            assert_eq!(config.execution.quote_canary_buy_size_sol, 0.2);
            assert_eq!(config.execution.quote_canary_slippage_bps, 120);
            assert_eq!(config.execution.quote_canary_buy_slippage_bps, 150);
            assert_eq!(config.execution.quote_canary_sell_slippage_bps, 500);
            assert!(config.execution.priority_fee_canary_enabled);
            assert_eq!(
                config.execution.priority_fee_canary_rpc_url,
                "https://example.com/rpc"
            );
            assert_eq!(config.execution.priority_fee_canary_timeout_ms, 800);
            assert_eq!(
                config.execution.priority_fee_canary_account,
                "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
            );
            assert_eq!(config.execution.priority_fee_canary_last_n_blocks, 80);
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_shadow_quality_gates_enabled_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_SHADOW_QUALITY_GATES_ENABLED",
        "enabled",
        "invalid shadow quality_gates_enabled env override must fail config load",
    );
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_killswitch_enabled_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED",
        "maybe",
        "invalid risk shadow_killswitch env override must fail config load",
    );
}

#[test]
fn load_from_env_rejects_sub_lamport_risk_cap() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL",
                "0.0000000001",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("sub-lamport risk cap must fail exact sizing validation")
                        .to_string();
                    assert!(
                        err.contains(
                            "risk.max_position_sol must be representable as at least 1 lamport"
                        ),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_discovery_window_memory_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY",
                "222222",
                || {
                    with_env_var(
                        "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_SWAPS_PER_CYCLE",
                        "111111",
                        || {
                            let (cfg, _) = load_from_env_or_default(config_path)
                                .expect("load config with discovery memory env overrides");
                            assert_eq!(cfg.discovery.max_window_swaps_in_memory, 222_222);
                            assert_eq!(cfg.discovery.max_fetch_swaps_per_cycle, 111_111);
                        },
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_follow_top_n_below_shadow_min_active_wallets() {
    with_temp_config_file(
        r#"
[discovery]
follow_top_n = 10

[risk]
shadow_universe_min_active_follow_wallets = 15
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_env_or_default(config_path)
                    .expect_err("impossible shadow universe config must fail")
                    .to_string();
                assert!(
                    err.contains("discovery.follow_top_n (10) must be >= risk.shadow_universe_min_active_follow_wallets (15)"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_env_rejects_publish_min_candidate_wallets_above_follow_top_n() {
    with_temp_config_file(
        r#"
[discovery]
follow_top_n = 15
publish_min_candidate_wallets = 16
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_env_or_default(config_path)
                    .expect_err("publish floor above follow max must fail")
                    .to_string();
                assert!(
                    err.contains(
                        "discovery.publish_min_candidate_wallets (16) must be <= discovery.follow_top_n (15)"
                    ),
                    "unexpected error: {err}"
                );
            });
        },
    );
}
