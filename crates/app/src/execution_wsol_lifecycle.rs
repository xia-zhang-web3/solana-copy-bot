//! Controlled transactions own their WSOL account. Canonical WSOL is never a route operand.
mod funding;
mod profile;
mod receipt;
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaInstruction};
use crate::execution_submit_adapter::{ExecutionSubmitRequest, ExecutionTransactionPlan};
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::ExecutionConfig;
use copybot_storage_core::rpc_owned_sell_snapshot::OwnedSellSnapshot;
pub(crate) use funding::{check_initial, collect as collect_funding};
pub(crate) use profile::normalize as normalize_instructions;
pub(crate) use profile::{create, initialize, parse_create, temporary};
pub(crate) use receipt::{verify as verify_receipt, verify_failed as verify_failed_receipt};
use sha2::{Digest, Sha256};

/// Runtime-only authority, never accepted from JSON/config alone. Constructed after
/// canonical receipt snapshot validation by the owned SELL collector.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Authority {
    order: String,
    wallet: String,
    amount: u64,
    proof: String,
}
pub(crate) fn authorize_owned(
    c: &ExecutionConfig,
    s: &OwnedSellSnapshot,
    r: &mut ExecutionSubmitRequest,
) -> Result<()> {
    if c.controlled_receipt_lot.is_none() {
        return Ok(());
    }
    copybot_config::validate_controlled_receipt_lot(c)?;
    let f = s.quote.fractional.as_ref().context("owned_wsol_fraction")?;
    let proof = f
        .controlled_receipt
        .as_ref()
        .context("owned_wsol_receipt")?;
    ensure!(
        r.side == "sell"
            && r.wallet_pubkey == proof.bot_wallet
            && r.token == s.quote.mint
            && r.metadata.quote_in_amount_raw.as_deref() == Some(s.quote.raw.to_string().as_str())
            && s.quote.raw == f.selected_raw
            && s.quote.raw > 0
            && s.quote.raw <= f.owned_raw,
        "owned_wsol_authority_binding"
    );
    r.metadata.transaction_owned_wsol = Some(Box::new(Authority {
        order: order(&r.order_id).into(),
        wallet: r.wallet_pubkey.clone(),
        amount: s.quote.raw,
        proof: format!("{:x}", Sha256::digest(serde_json::to_vec(proof)?)),
    }));
    Ok(())
}
/// Body-profile binding is inserted before ContextProof captures its immutable request.
/// It does not authorize a BUY; the existing canonical signal/owner checks follow.
pub(crate) fn authorize_buy(c: &ExecutionConfig, r: &mut ExecutionSubmitRequest) -> Result<()> {
    if c.controlled_receipt_lot.is_none() {
        return Ok(());
    }
    copybot_config::validate_controlled_receipt_lot(c)?;
    ensure!(
        r.side == "buy"
            && r.wallet_pubkey == c.canary_wallet_pubkey
            && (r.signal_id.starts_with("owner-buy:") || r.signal_id.starts_with("native-buy-v1:")),
        "owned_wsol_buy_profile"
    );
    let amount = r
        .metadata
        .quote_in_amount_raw
        .as_deref()
        .context("owned_wsol_input")?
        .parse()?;
    r.metadata.transaction_owned_wsol = Some(Box::new(Authority {
        order: order(&r.order_id).into(),
        wallet: r.wallet_pubkey.clone(),
        amount,
        proof: copybot_config::TRANSACTION_OWNED_WSOL_V1.into(),
    }));
    Ok(())
}
fn order(id: &str) -> &str {
    id.strip_prefix("exec-canary:").unwrap_or(id)
}
pub(crate) fn scoped(r: &ExecutionSubmitRequest) -> bool {
    r.metadata
        .protected_capital
        .as_deref()
        .is_some_and(|p| p.receipt_scoped())
        || r.metadata
            .controlled_exit_wire
            .as_deref()
            .is_some_and(|p| p.matches(r))
        || r.metadata
            .transaction_owned_wsol
            .as_deref()
            .is_some_and(|p| {
                p.order == order(&r.order_id)
                    && p.wallet == r.wallet_pubkey
                    && r.side == "sell"
                    && r.metadata.quote_in_amount_raw.as_deref()
                        == Some(p.amount.to_string().as_str())
            })
        || r.metadata.rpc_owned_sell.as_deref().is_some_and(|p| {
            p.order_id() == r.order_id
                && p.handoff.wallet == r.wallet_pubkey
                && p.handoff
                    .snapshot
                    .quote
                    .fractional
                    .as_ref()
                    .is_some_and(|f| f.controlled_receipt.is_some())
        })
}
fn bound_request(plan: &ExecutionTransactionPlan) -> Result<ExecutionSubmitRequest> {
    let b = crate::execution_instruction_bundle_binding::BundleRequest::capture(plan)?;
    let mut r = b.request().clone();
    r.wallet_id = r.wallet_pubkey.clone();
    r.metadata.protected_capital = plan.metadata.protected_capital.clone();
    Ok(r)
}
/// Original provider wire is fully verified before any instruction is rewritten.
pub(crate) fn normalize(
    c: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    payload: &str,
    rent: u64,
) -> Result<String> {
    if c.controlled_receipt_lot.is_none() {
        return Ok(payload.into());
    }
    copybot_config::validate_controlled_receipt_lot(c)?;
    let r = bound_request(plan)?;
    ensure!(scoped(&r), "owned_wsol_private_authority_missing");
    let m = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    let mut original = m
        .instructions
        .iter()
        .map(|i| SolanaInstruction {
            program_id: i.program.pubkey,
            accounts: i.accounts.clone(),
            data: i.data.clone(),
        })
        .collect::<Vec<_>>();
    // Provider may omit cleanup when its snapshot sees an existing canonical ATA.
    // The local isolated lifecycle always closes its own account, independent of that snapshot.
    let canonical = associated_token_address(&wallet, &wsol_mint(), &token_program_id());
    if !original
        .iter()
        .any(|i| i.program_id == token_program_id() && i.data == [9])
    {
        let close = SolanaInstruction {
            program_id: token_program_id(),
            accounts: vec![
                crate::execution_solana_tx::SolanaAccountMeta::writable(canonical),
                crate::execution_solana_tx::SolanaAccountMeta::signer_writable(wallet),
                crate::execution_solana_tx::SolanaAccountMeta::signer_writable(wallet),
            ],
            data: vec![9],
        };
        let at = if r.side == "buy" || r.signal_id.starts_with("owner-exit:") {
            original
                .len()
                .checked_sub(1)
                .context("owned_wsol_floor_missing")?
        } else {
            original.len()
        };
        original.insert(at, close);
    }
    let canonical_payload = STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet,
            m.recent_blockhash,
            &original,
        )?,
    );
    verify_canonical(&r, &canonical_payload)?;
    let out = profile::normalize(&r, &original, rent)?;
    let wire = STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet,
            m.recent_blockhash,
            &out,
        )?,
    );
    let view = canonical_view_scoped(&r, &wire)?;
    verify_canonical(&r, &view)?;
    Ok(wire)
}
fn verify_canonical(r: &ExecutionSubmitRequest, payload: &str) -> Result<()> {
    if r.side == "buy" {
        if r.signal_id.starts_with("owner-buy:") {
            crate::execution_owner_buy_wire::verify_canonical(r, payload)?;
        } else {
            crate::execution_owner_buy_wire::verify_cohort_canonical(r, payload)?;
        }
    } else {
        let n = r
            .metadata
            .quote_in_amount_raw
            .as_deref()
            .context("owned_wsol_input")?
            .parse()?;
        crate::execution_usdc_sell_wire::verify_canonical(
            r,
            payload,
            n,
            r.signal_id.starts_with("owner-exit:"),
        )?;
    }
    Ok(())
}
pub(crate) fn canonical_view(r: &ExecutionSubmitRequest, payload: &str) -> Result<String> {
    if scoped(r) {
        canonical_view_scoped(r, payload)
    } else {
        Ok(payload.into())
    }
}
pub(crate) fn canonical_view_scoped(r: &ExecutionSubmitRequest, payload: &str) -> Result<String> {
    let m = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    let original = m
        .instructions
        .iter()
        .map(|i| SolanaInstruction {
            program_id: i.program.pubkey,
            accounts: i.accounts.clone(),
            data: i.data.clone(),
        })
        .collect::<Vec<_>>();
    let out = profile::canonical_view(r, &original)?;
    Ok(STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet,
            m.recent_blockhash,
            &out,
        )?,
    ))
}
pub(crate) fn address(r: &ExecutionSubmitRequest) -> Result<PubkeyBytes> {
    temporary(r).map(|(_, a)| a)
}
pub(crate) async fn rent(
    http: &reqwest::Client,
    c: &ExecutionConfig,
    check: &mut impl FnMut() -> Result<()>,
) -> Result<u64> {
    copybot_config::validate_controlled_receipt_lot(c)?;
    let url = if c.owned_sell_preparation.is_some() {
        crate::execution_owned_sell_rpc::endpoint(c)?
    } else {
        reqwest::Url::parse(&c.submit_adapter_http_url)?
    };
    ensure!(
        url.scheme() == "https"
            || url.scheme() == "http"
                && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")),
        "owned_wsol_rpc_endpoint"
    );
    let e=crate::execution_owned_sell_rpc::exchange(http,c,&url,serde_json::json!({"jsonrpc":"2.0","id":"transaction-owned-wsol-rent","method":"getMinimumBalanceForRentExemption","params":[165,{"commitment":"confirmed"}]}),check).await?;
    let rent = e.value()["result"].as_u64().context("owned_wsol_rent")?;
    ensure!(
        rent > 0 && rent <= copybot_storage_core::TINY_NATIVE_ALLOWANCE,
        "owned_wsol_rent_cap"
    );
    Ok(rent)
}
