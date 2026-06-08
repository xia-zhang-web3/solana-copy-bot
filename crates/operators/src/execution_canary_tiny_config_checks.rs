use copybot_config::ExecutionConfig;
use std::path::{Path, PathBuf};

use crate::execution_canary_quote_pnl_gate::TinyExecutionGateCheck;
use crate::execution_signer_keypair_preflight::push_signer_guards;

const TINY_MAX_DAILY_LOSS_SOL: f64 = 0.05;
const TINY_MIN_RESERVE_SOL: f64 = 0.05;
const TINY_MAX_PRIORITY_FEE_LAMPORTS: u64 = 10_000_000;
const TINY_MAX_SLIPPAGE_BPS: f64 = 1_000.0;

pub(crate) fn push_config_checks(
    checks: &mut Vec<TinyExecutionGateCheck>,
    config: Option<&ExecutionConfig>,
    runtime_root: Option<&Path>,
) {
    let Some(config) = config else {
        push_check(
            checks,
            "config_loaded",
            false,
            "not_loaded".to_string(),
            "--config".to_string(),
            "tiny execution preflight requires the live config, not only a db path",
        );
        return;
    };

    push_runtime_guards(checks, config);
    push_canary_guards(checks, config, runtime_root);
    push_signer_guards(checks, config, runtime_root);
    push_submit_guards(checks, config, runtime_root);
    push_pretrade_guards(checks, config);
    push_observability_guards(checks, config);
}

fn push_runtime_guards(checks: &mut Vec<TinyExecutionGateCheck>, config: &ExecutionConfig) {
    push_check(
        checks,
        "execution_disabled",
        !config.enabled,
        config.enabled.to_string(),
        "false".to_string(),
        "real execution must remain disabled until a separate explicit rollout",
    );
    push_check(
        checks,
        "canary_enabled",
        config.canary_enabled,
        config.canary_enabled.to_string(),
        "true".to_string(),
        "canary path must be enabled for tiny preflight",
    );
    push_check(
        checks,
        "canary_dry_run",
        config.canary_dry_run,
        config.canary_dry_run.to_string(),
        "true".to_string(),
        "current tiny readiness must remain dry-run only",
    );
    push_check(
        checks,
        "canary_tiny_submit_enabled",
        config.canary_tiny_submit_enabled,
        config.canary_tiny_submit_enabled.to_string(),
        "true".to_string(),
        "tiny submit gate must be explicitly enabled for rollout",
    );
}

fn push_canary_guards(
    checks: &mut Vec<TinyExecutionGateCheck>,
    config: &ExecutionConfig,
    runtime_root: Option<&Path>,
) {
    push_check(
        checks,
        "canary_wallet_pubkey",
        valid_wallet_pubkey(&config.canary_wallet_pubkey),
        redact_wallet(&config.canary_wallet_pubkey),
        "valid non-system pubkey".to_string(),
        "Metis/Jupiter builders need a valid user public key",
    );
    push_check(
        checks,
        "canary_buy_size",
        config.canary_buy_size_sol.is_finite()
            && (0.01..=0.02).contains(&config.canary_buy_size_sol),
        format!("{:.4} SOL", config.canary_buy_size_sol),
        "0.01..=0.02 SOL".to_string(),
        "tiny canary size must stay small before real execution",
    );
    push_check(
        checks,
        "canary_max_open_positions",
        (1..=20).contains(&config.canary_max_open_positions),
        config.canary_max_open_positions.to_string(),
        "1..=20".to_string(),
        "tiny test must keep aggregate open-position exposure bounded",
    );
    push_check(
        checks,
        "canary_daily_loss_cap",
        config.canary_max_daily_loss_sol.is_finite()
            && config.canary_max_daily_loss_sol > 0.0
            && config.canary_max_daily_loss_sol <= TINY_MAX_DAILY_LOSS_SOL,
        format!("{:.4} SOL", config.canary_max_daily_loss_sol),
        format!("0 < cap <= {TINY_MAX_DAILY_LOSS_SOL:.2} SOL"),
        "daily loss cap must be finite and small",
    );
    push_check(
        checks,
        "canary_kill_switch_path",
        !config.canary_kill_switch_path.trim().is_empty(),
        config.canary_kill_switch_path.clone(),
        "configured".to_string(),
        "kill switch path must exist in config",
    );
    push_check(
        checks,
        "canary_kill_switch_inactive",
        !resolve_runtime_path(&config.canary_kill_switch_path, runtime_root).exists(),
        resolve_runtime_path(&config.canary_kill_switch_path, runtime_root)
            .display()
            .to_string(),
        "file absent".to_string(),
        "kill switch file must not be present",
    );
}

fn push_submit_guards(
    checks: &mut Vec<TinyExecutionGateCheck>,
    config: &ExecutionConfig,
    runtime_root: Option<&Path>,
) {
    let token_path = resolve_runtime_path(&config.submit_adapter_auth_token_file, runtime_root);
    let token_path_configured = !config.submit_adapter_auth_token_file.trim().is_empty();
    push_check(
        checks,
        "submit_adapter_http_url",
        valid_http_url(&config.submit_adapter_http_url),
        redact_url(&config.submit_adapter_http_url),
        "http(s) url".to_string(),
        "real submit needs an explicit submit adapter endpoint",
    );
    push_check(
        checks,
        "submit_adapter_auth_token_file",
        token_path_configured,
        redact_path(&config.submit_adapter_auth_token_file),
        "configured".to_string(),
        "submit adapter must be authenticated before tiny real execution",
    );
    push_check(
        checks,
        "submit_adapter_auth_token_file_exists",
        token_path_configured && token_path.is_file(),
        redact_path(&config.submit_adapter_auth_token_file),
        "file exists".to_string(),
        "configured submit adapter auth token file must be present",
    );
    push_check(
        checks,
        "submit_timeout_ms",
        (500..=10_000).contains(&config.submit_timeout_ms),
        config.submit_timeout_ms.to_string(),
        "500..=10000".to_string(),
        "submit timeout must be bounded before real submit",
    );
    push_check(
        checks,
        "max_confirm_seconds",
        (5..=60).contains(&config.max_confirm_seconds),
        config.max_confirm_seconds.to_string(),
        "5..=60".to_string(),
        "confirmation timeout must be bounded before real submit",
    );
    push_check(
        checks,
        "max_submit_attempts",
        (1..=3).contains(&config.max_submit_attempts),
        config.max_submit_attempts.to_string(),
        "1..=3".to_string(),
        "retry budget must prevent unbounded duplicate submit risk",
    );
    push_check(
        checks,
        "simulate_before_submit",
        config.simulate_before_submit,
        config.simulate_before_submit.to_string(),
        "true".to_string(),
        "tiny rollout must simulate before any real submit",
    );
}

fn push_pretrade_guards(checks: &mut Vec<TinyExecutionGateCheck>, config: &ExecutionConfig) {
    push_check(
        checks,
        "pretrade_min_sol_reserve",
        config.pretrade_min_sol_reserve.is_finite()
            && config.pretrade_min_sol_reserve >= TINY_MIN_RESERVE_SOL,
        format!("{:.4} SOL", config.pretrade_min_sol_reserve),
        format!(">={TINY_MIN_RESERVE_SOL:.2} SOL"),
        "wallet must keep a SOL reserve for fees and recovery",
    );
    push_check(
        checks,
        "pretrade_max_priority_fee_lamports",
        (1..=TINY_MAX_PRIORITY_FEE_LAMPORTS).contains(&config.pretrade_max_priority_fee_lamports),
        config.pretrade_max_priority_fee_lamports.to_string(),
        format!("1..={TINY_MAX_PRIORITY_FEE_LAMPORTS}"),
        "priority fee cap must be finite and explicitly configured",
    );
    push_check(
        checks,
        "execution_slippage_bps",
        config.slippage_bps.is_finite()
            && config.slippage_bps > 0.0
            && config.slippage_bps <= TINY_MAX_SLIPPAGE_BPS,
        format!("{:.1}", config.slippage_bps),
        format!("0 < bps <= {TINY_MAX_SLIPPAGE_BPS:.0}"),
        "real execution slippage must be explicitly bounded",
    );
}

fn push_observability_guards(checks: &mut Vec<TinyExecutionGateCheck>, config: &ExecutionConfig) {
    push_check(
        checks,
        "quote_canary_enabled",
        config.quote_canary_enabled,
        config.quote_canary_enabled.to_string(),
        "true".to_string(),
        "quote canary metadata is required for tiny readiness",
    );
    push_check(
        checks,
        "swap_instructions_dry_run_enabled",
        config.swap_instructions_dry_run_enabled,
        config.swap_instructions_dry_run_enabled.to_string(),
        "true".to_string(),
        "swap-instructions dry-run must prove the builder path",
    );
    push_check(
        checks,
        "swap_transaction_dry_run_enabled",
        config.swap_transaction_dry_run_enabled,
        config.swap_transaction_dry_run_enabled.to_string(),
        "true".to_string(),
        "swap transaction dry-run must prove serialized transaction build",
    );
    push_check(
        checks,
        "priority_fee_canary_enabled",
        config.priority_fee_canary_enabled,
        config.priority_fee_canary_enabled.to_string(),
        "true".to_string(),
        "priority fee data must be available before tiny tests",
    );
}

fn valid_wallet_pubkey(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed != "11111111111111111111111111111111"
        && (32..=44).contains(&trimmed.len())
        && trimmed.bytes().all(|byte| {
            matches!(
                byte,
                b'1'..=b'9'
                    | b'A'..=b'H'
                    | b'J'..=b'N'
                    | b'P'..=b'Z'
                    | b'a'..=b'k'
                    | b'm'..=b'z'
            )
        })
}

fn valid_http_url(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .is_some_and(|suffix| !suffix.trim().is_empty())
}

fn redact_wallet(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.chars().count() <= 12 {
        return trimmed.to_string();
    }
    let prefix: String = trimmed.chars().take(6).collect();
    let suffix: String = trimmed
        .chars()
        .rev()
        .take(6)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{prefix}...{suffix}")
}

fn redact_url(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "missing".to_string();
    }
    match trimmed.split_once('?') {
        Some((prefix, _)) => format!("{prefix}?<redacted>"),
        None => trimmed.to_string(),
    }
}

fn redact_path(value: &str) -> String {
    if value.trim().is_empty() {
        "missing".to_string()
    } else {
        "[REDACTED_PATH]".to_string()
    }
}

fn resolve_runtime_path(raw: &str, runtime_root: Option<&Path>) -> PathBuf {
    let path = Path::new(raw.trim());
    if path.is_absolute() {
        return path.to_path_buf();
    }
    runtime_root
        .map(|root| root.join(path))
        .unwrap_or_else(|| path.to_path_buf())
}

fn push_check(
    checks: &mut Vec<TinyExecutionGateCheck>,
    name: &str,
    ok: bool,
    value: String,
    threshold: String,
    reason: impl Into<String>,
) {
    checks.push(TinyExecutionGateCheck {
        name: name.to_string(),
        status: if ok { "pass" } else { "block" }.to_string(),
        value,
        threshold,
        reason: reason.into(),
    });
}
