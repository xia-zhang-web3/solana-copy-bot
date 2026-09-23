//! Explicit owned SELL authority; unsigned preparation remains the default.
use anyhow::{ensure, Result};
use serde::Deserialize;
pub const RPC_FINALIZED_OWNED_SELL_V1: &str = "rpc_finalized_cross_slot_owned_sell_v1";
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedSellPreparationConfig {
    pub policy: String,
    #[serde(default)]
    pub tiny_dispatch: bool,
    /// Explicit opt-in; never changes the old full-owned SELL contract.
    #[serde(default)]
    pub fractional_inventory: Option<String>,
    pub rpc_url: String,
    pub genesis_hash: String,
    pub identity: String,
}
pub fn validate_owned_sell_preparation(
    e: &crate::ExecutionConfig,
    i: &crate::IngestionConfig,
) -> Result<()> {
    let Some(p) = &e.owned_sell_preparation else {
        return Ok(());
    };
    ensure!(
        p.fractional_inventory
            .as_deref()
            .is_none_or(|v| v == "whole_wallet_parent_program_fraction_v1"),
        "owned_sell_fractional_contract"
    );
    ensure!(
        p.policy == RPC_FINALIZED_OWNED_SELL_V1,
        "owned_sell_policy_unsupported"
    );
    ensure!(
        i.yellowstone_delivery_mode == "durable_association_v1" && i.source == "yellowstone_grpc",
        "owned_sell_delivery_mode"
    );
    ensure!(
        owned_sell_flags(e) && e.canary_enabled && e.quote_canary_enabled,
        "owned_sell_unsigned_only_flags"
    );
    ensure!(
        e.swap_instructions_dry_run_enabled && e.swap_transaction_dry_run_enabled,
        "owned_sell_preparation_disabled"
    );
    ensure!(
        !p.identity.is_empty()
            && p.identity.len() <= 128
            && p.identity
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
        "owned_sell_identity"
    );
    ensure!(
        !p.rpc_url.is_empty() && !p.genesis_hash.is_empty(),
        "owned_sell_rpc_authority_missing"
    );
    ensure!(
        !e.canary_wallet_pubkey.is_empty() && e.canary_wallet_pubkey == e.execution_signer_pubkey,
        "owned_sell_wallet_identity"
    );
    ensure!(
        e.pretrade_max_priority_fee_lamports > 0,
        "owned_sell_priority_cap_required"
    );
    if p.tiny_dispatch {
        ensure!(
            e.submit_adapter_http_url == p.rpc_url,
            "owned_sell_submit_rpc_binding"
        );
        ensure!(
            !e.execution_signer_keypair_path.trim().is_empty(),
            "owned_sell_signer_required"
        );
        ensure!(
            e.submit_timeout_ms > 0 && e.submit_timeout_ms <= 30_000,
            "owned_sell_submit_timeout"
        );
    }
    e.tiny_experiment.validate(&e.canary_wallet_pubkey)?;
    ensure!(
        e.tiny_experiment.id.is_some() && !e.tiny_experiment.activate,
        "owned_sell_existing_experiment_required"
    );
    Ok(())
}

/// The only durable-source execution arm. No BUY or broader execution authority.
pub fn owned_sell_flags(e: &crate::ExecutionConfig) -> bool {
    !e.enabled
        && (!e.canary_tiny_submit_enabled && !owned_sell_dispatch(e)
            || e.canary_tiny_submit_enabled && owned_sell_dispatch(e))
}
pub fn owned_sell_dispatch(e: &crate::ExecutionConfig) -> bool {
    e.owned_sell_preparation
        .as_ref()
        .is_some_and(|p| p.policy == RPC_FINALIZED_OWNED_SELL_V1 && p.tiny_dispatch)
}
