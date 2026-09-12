//! Tiny BUY policy only. No balance observation, funding estimate or economic debit.
use crate::execution_native_floor::{verify_final_native_floor, VerifiedNativeFloor};
use crate::execution_pumpswap_accounts::parse_pubkey;
use crate::execution_submit_adapter::{ExecutionSubmitRequest, ExecutionTransactionPlan};
use anyhow::{ensure, Result};
use copybot_config::ExecutionConfig;
#[path = "execution_protected_capital.rs"]
pub(crate) mod protected;
#[path = "execution_protected_assembly.rs"]
pub(crate) mod protected_assembly;

/// ceil(exact binary f64 * 1e9), without rounded floating multiplication or saturation.
pub(crate) fn reserve_lamports(sol: f64) -> Result<u64> {
    ensure!(sol.is_finite() && sol > 0.0, "native_floor_invalid_policy");
    let bits = sol.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (mantissa, power) = if exponent == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1_u64 << 52), exponent - 1023 - 52)
    };
    let numerator = u128::from(mantissa)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| anyhow::anyhow!("native_floor_invalid_policy"))?;
    let rounded = if power >= 0 {
        let shift = power as u32;
        ensure!(
            shift < 64 && numerator <= (u128::from(u64::MAX) >> shift),
            "native_floor_invalid_policy"
        );
        numerator << shift
    } else {
        let shift = (-power) as u32;
        if shift >= 128 {
            1 // strictly positive numerator < 2^128, including subnormal inputs
        } else {
            let quotient = numerator >> shift;
            let remainder = numerator & ((1_u128 << shift) - 1);
            quotient + u128::from(remainder != 0)
        }
    };
    u64::try_from(rounded).map_err(|_| anyhow::anyhow!("native_floor_invalid_policy"))
}

pub(crate) fn required_for_plan(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<u64>> {
    if plan.side.eq_ignore_ascii_case("buy") && plan.metadata.protected_capital.is_some() {
        ensure!(
            config.canary_tiny_submit_enabled && protected::enabled(config),
            "tiny_capital_mode_conflict"
        );
    }
    if !config.canary_tiny_submit_enabled || !plan.side.eq_ignore_ascii_case("buy") {
        return Ok(None);
    }
    let reserve = if protected::enabled(config) {
        plan.metadata
            .protected_capital
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("tiny_capital_context_missing"))?
            .for_plan(config, plan)?
    } else {
        ensure!(
            plan.metadata.protected_capital.is_none(),
            "tiny_capital_mode_conflict"
        );
        reserve_lamports(config.pretrade_min_sol_reserve)?
    };
    ensure!(
        !config.execution_signer_pubkey.is_empty()
            && config.execution_signer_pubkey == config.canary_wallet_pubkey
            && plan.wallet_pubkey == config.execution_signer_pubkey,
        "native_floor_wallet_identity_mismatch"
    );
    Ok(Some(reserve))
}

pub(crate) fn verify_signing_payload(
    config: &ExecutionConfig,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    payload: &str,
) -> Result<Option<VerifiedNativeFloor>> {
    ensure!(
        request.side.eq_ignore_ascii_case(&plan.side)
            && request.wallet_pubkey == plan.wallet_pubkey,
        "native_floor_plan_identity_mismatch"
    );
    let Some(reserve) = required_for_plan(config, plan)? else {
        return Ok(None);
    };
    let wallet = parse_pubkey(&request.wallet_pubkey, "native_floor_wallet")?;
    verify_final_native_floor(payload, wallet, reserve).map(Some)
}

pub(crate) fn verify_after_signing(
    before: Option<&VerifiedNativeFloor>,
    payload: &str,
) -> Result<()> {
    if let Some(before) = before {
        let after = verify_final_native_floor(payload, before.wallet(), before.reserve_lamports())?;
        ensure!(
            before.binding().message_bytes == after.binding().message_bytes,
            "native_floor_message_changed_after_signing"
        );
    }
    Ok(())
}

/// Always required for an allowed tiny BUY; no adapter metadata/optional proof can disable this.
pub(crate) fn verify_submit_payload(
    request: &ExecutionSubmitRequest,
    payload: &str,
    current_reserve_sol: f64,
    current_wallet: &str,
) -> Result<()> {
    if !request.side.eq_ignore_ascii_case("buy") {
        return Ok(());
    }
    let reserve = match request.metadata.protected_capital.as_deref() {
        Some(proof) => {
            proof.verify_request(request)?;
            proof.floor(current_wallet, current_reserve_sol)?
        }
        None => reserve_lamports(current_reserve_sol)?,
    };
    ensure!(
        !current_wallet.is_empty() && current_wallet == request.wallet_pubkey,
        "native_floor_wallet_identity_mismatch"
    );
    let wallet = parse_pubkey(current_wallet, "native_floor_wallet")?;
    verify_final_native_floor(payload, wallet, reserve)?;
    Ok(())
}
