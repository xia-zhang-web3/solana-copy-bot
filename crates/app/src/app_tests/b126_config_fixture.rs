//! TOML inputs also parse on accepted125 (whose schema ignores the new subsection).
use anyhow::Result;
use copybot_config::ExecutionConfig;

pub(super) fn activated(c: &ExecutionConfig) -> Result<ExecutionConfig> {
    let path = std::env::temp_dir().join(format!(
        "b126-config-{}-{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let q = |v: &str| serde_json::to_string(v).unwrap();
    let raw = format!(
        r#"[execution]
canary_enabled = true
canary_dry_run = true
canary_tiny_submit_enabled = true
canary_route = {}
canary_wallet_pubkey = {}
execution_signer_pubkey = {}
execution_signer_keypair_path = {}
quote_canary_base_url = {}
submit_adapter_http_url = {}
pretrade_max_priority_fee_lamports = {}
pretrade_min_sol_reserve = {}
canary_max_open_positions = {}
canary_max_daily_loss_sol = {}
canary_batch_limit = {}
max_confirm_seconds = {}
max_submit_attempts = {}
submit_timeout_ms = {}
quote_canary_timeout_ms = {}
swap_instructions_dry_run_enabled = true
swap_transaction_dry_run_enabled = true
[execution.tiny_experiment]
id = "local-variant-a"
activate = true
"#,
        q(&c.canary_route),
        q(&c.canary_wallet_pubkey),
        q(&c.execution_signer_pubkey),
        q(&c.execution_signer_keypair_path),
        q(&c.quote_canary_base_url),
        q(&c.submit_adapter_http_url),
        c.pretrade_max_priority_fee_lamports,
        c.pretrade_min_sol_reserve,
        c.canary_max_open_positions,
        c.canary_max_daily_loss_sol,
        c.canary_batch_limit,
        c.max_confirm_seconds,
        c.max_submit_attempts,
        c.submit_timeout_ms,
        c.quote_canary_timeout_ms
    );
    std::fs::write(&path, raw)?;
    let result = copybot_config::load_from_path(&path).map(|v| v.execution);
    std::fs::remove_file(path)?;
    result
}
