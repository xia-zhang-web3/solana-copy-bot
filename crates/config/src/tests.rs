
use super::*;
use std::ffi::OsString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());
static TEMP_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

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
fn load_from_env_rejects_duplicate_normalized_route_max_slippage_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS",
        "rpc:50,RPC:75",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_tip_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
        "rpc:100,RPC:200",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_cu_limit_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT",
        "rpc:300000,RPC:350000",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_cu_price_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS",
        "rpc:1000,RPC:2000",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_submit_allowed_routes() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES",
        "rpc,RPC",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_submit_route_order() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER",
        "jito,JITO",
    );
}

#[test]
fn load_from_env_rejects_malformed_route_map_token() {
    assert_route_map_env_rejected_contains(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS",
        "rpc:50,jito",
        "malformed token",
    );
}

#[test]
fn load_from_env_rejects_invalid_route_map_numeric_value() {
    assert_route_map_env_rejected_contains(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
        "rpc:not-a-number",
        "invalid numeric value",
    );
}

#[test]
fn load_from_env_rejects_incomplete_route_policy_for_allowed_routes() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_EXECUTION_ENABLED", "true", || {
                with_env_var(
                    "SOLANA_COPY_BOT_EXECUTION_MODE",
                    "adapter_submit_confirm",
                    || {
                        with_env_var(
                            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES",
                            "paper,rpc",
                            || {
                                let err = load_from_env_or_default(config_path)
                                    .expect_err(
                                        "missing route policy for allowed route must fail at config load",
                                    )
                                    .to_string();
                                assert!(
                                    err.contains(
                                        "execution.submit_route_max_slippage_bps is missing cap for allowed route=rpc",
                                    ),
                                    "unexpected error: {err}"
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
fn load_from_env_applies_risk_and_shadow_quality_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL", "0.99", || {
                with_env_var(
                    "SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED",
                    "false",
                    || {
                        with_env_var("SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS", "42", || {
                            with_env_var(
                                "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_MICRO_LAMPORTS",
                                "12345",
                                || {
                                    let (cfg, _) = load_from_env_or_default(config_path)
                                        .expect("load config with env overrides");
                                    assert!((cfg.risk.max_position_sol - 0.99).abs() <= f64::EPSILON);
                                    assert!(!cfg.risk.shadow_killswitch_enabled);
                                    assert_eq!(cfg.shadow.min_holders, 42);
                                    assert_eq!(
                                        cfg.execution.pretrade_max_priority_fee_lamports,
                                        12_345
                                    );
                                },
                            );
                        });
                    },
                );
            });
        });
    });
}

fn assert_duplicate_normalized_route_env_rejected(env_name: &'static str, env_value: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("duplicate normalized route keys should fail")
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains("duplicate route after normalization"),
                    "error should describe duplicate normalization, got: {err}"
                );
            });
        });
    });
}

fn assert_route_map_env_rejected_contains(env_name: &'static str, env_value: &str, needle: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid route map env should fail")
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains(needle),
                    "error should contain '{needle}', got: {err}"
                );
            });
        });
    });
}

fn with_env_var<T>(key: &'static str, value: &str, run: impl FnOnce() -> T) -> T {
    let previous = std::env::var_os(key);
    std::env::set_var(key, value);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    restore_env_var(key, previous);
    match outcome {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn restore_env_var(key: &'static str, previous: Option<OsString>) {
    match previous {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
}

fn with_clean_copybot_env<T>(run: impl FnOnce() -> T) -> T {
    // Serialize all SOLANA_COPY_BOT_* env mutations in this test module.
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let saved: Vec<(OsString, OsString)> = std::env::vars_os()
        .filter(|(key, _)| key.to_string_lossy().starts_with("SOLANA_COPY_BOT_"))
        .collect();
    for (key, _) in &saved {
        std::env::remove_var(key);
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    for (key, value) in saved {
        std::env::set_var(key, value);
    }
    match outcome {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn with_temp_config_file<T>(contents: &str, run: impl FnOnce(&Path) -> T) -> T {
    let path = unique_temp_path();
    fs::write(&path, contents).expect("write temp config");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&path)));
    let _ = fs::remove_file(&path);
    match outcome {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn unique_temp_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let seq = TEMP_CONFIG_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("copybot-config-test-{pid}-{nanos}-{seq}.toml"))
}
