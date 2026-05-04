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
