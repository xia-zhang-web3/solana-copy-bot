//! SOURCE owns its trades; BOT BUY is a read-only prerequisite for its exit.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::owner_source_copy_gate::SourceCopyGateBinding;
use copybot_storage_core::{owner_technical_buy_order_id, SqliteStore};

/// Reuse the accepted owner wire, while the optional SOURCE scenario only emits
/// the Jupiter V1 RaydiumV2 ABI that its external SELL proof can recognize.
pub(crate) fn verify_exit_wire(
    c: &ExecutionConfig,
    r: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    payload: &str,
) -> Result<crate::execution_owner_exit_wire::OwnerExitWireProof> {
    let proof = crate::execution_owner_exit_wire::verify(r, payload)?;
    if c.owner_technical_buy
        .as_ref()
        .is_some_and(|p| p.source_copy_exit_gate.is_some())
    {
        let message = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
        let jup = crate::execution_pumpswap_accounts::parse_pubkey(
            crate::execution_owner_buy_wire::JUPITER,
            "source_copy_sell_jupiter",
        )?;
        let route = message
            .instructions
            .iter()
            .find(|i| i.program.pubkey == jup)
            .context("source_copy_sell_route_missing")?;
        ensure!(
            route.data.get(12) == Some(&105)
                && route.accounts.len() == 18
                && (route.accounts[4]
                    == crate::execution_solana_tx::SolanaAccountMeta::readonly(jup)
                    || (c.controlled_receipt_lot.is_some()
                        && route.accounts[4] == route.accounts[3])),
            "source_copy_sell_profile_unsupported"
        );
    }
    Ok(proof)
}

pub(crate) fn ready(c: &ExecutionConfig, s: &SqliteStore, now: DateTime<Utc>) -> Result<bool> {
    let Some(p) = c.owner_technical_buy.as_ref() else {
        return Ok(true);
    };
    if p.policy != copybot_config::OWNER_EXECUTION_ROUNDTRIP_V1 {
        return Ok(true);
    }
    // Even removal of the optional gate changes the immutable parent authority.
    // activate is deliberately absent from that identity, allowing STOP recovery.
    let parent = crate::execution_owner_buy_authority::parent_identity(c)?;
    ensure!(
        s.load_owner_technical_buy_intent(&p.intent_id)?.as_ref() == Some(&parent),
        "source_copy_gate_parent_changed"
    );
    let Some(g) = &p.source_copy_exit_gate else {
        return Ok(true);
    };
    copybot_config::validate_owner_source_copy_gate(p)?;
    crate::execution_pumpswap_accounts::parse_pubkey(
        &g.bot_wallet_pubkey,
        "source_copy_gate_bot_wallet",
    )?;
    if std::path::Path::new(&g.bot_kill_switch_path).exists()
        || std::path::Path::new(&c.canary_kill_switch_path).exists()
    {
        return Ok(false);
    }
    let id = owner_technical_buy_order_id(&p.intent_id);
    let Some(order) = s.load_execution_canary_order(&id)? else {
        return Ok(false);
    };
    let Some(proof) = s.load_execution_canary_receipt_proof(&id)? else {
        return Ok(false);
    };
    let Some(facts) = s.load_execution_canary_receipt_facts(&id)? else {
        return Ok(false);
    };
    if order.status != copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
        || proof.reason != "accounting_complete"
        || !s.execution_canary_fill_exists(&id)?
        || order.tx_signature.as_deref() != Some(facts.tx_signature.as_str())
        || proof.tx_signature != facts.tx_signature
        || proof.wallet_pubkey != p.wallet_pubkey
        || facts.wallet_pubkey != p.wallet_pubkey
        || facts.token != p.mint
        || facts.side != "buy"
    {
        return Ok(false);
    }
    let raw = facts
        .token_delta
        .context("source_copy_gate_source_quantity")?;
    ensure!(raw.raw > 0, "source_copy_gate_source_quantity");
    let b = SourceCopyGateBinding {
        source_wallet: &p.wallet_pubkey,
        source_signature: &facts.tx_signature,
        source_slot: facts.slot,
        source_mint: &p.mint,
        source_input_lamports: p.amount_lamports,
        source_output_raw: u64::try_from(raw.raw)?,
        source_decimals: raw.decimals,
        genesis_hash: &p.genesis_hash,
        bot_wallet: &g.bot_wallet_pubkey,
        bot_run_id: &g.bot_run_id,
        bot_policy_identity: &g.bot_policy_identity,
        activated_at: parent.activated_at,
        expires_at: parent.expires_at,
    };
    let bot = SqliteStore::open_read_only(&g.bot_state_db_path)?;
    let confirmed = bot.confirmed_native_bot_buy_for_source(&b, now)?.is_some();
    // STOP appearing during the read cannot release a new SOURCE SELL.
    Ok(confirmed
        && !std::path::Path::new(&g.bot_kill_switch_path).exists()
        && !std::path::Path::new(&c.canary_kill_switch_path).exists())
}
