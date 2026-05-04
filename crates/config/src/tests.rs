use super::*;
use std::ffi::OsString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());
static TEMP_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

include!("tests/00.rs");
include!("tests/01.rs");
include!("tests/02.rs");

fn assert_string_list_env_rejected_contains(env_name: &'static str, env_value: &str, needle: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid string-list env should fail")
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

fn assert_bool_env_rejected(env_name: &'static str, env_value: &str, context: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err(context)
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains("must be a valid bool"),
                    "error should describe bool parse failure, got: {err}"
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
