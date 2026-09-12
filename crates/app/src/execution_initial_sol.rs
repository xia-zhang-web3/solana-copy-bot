//! Selected explicit start funding, not full CPI/peak debit or a pending reservation.
use crate::execution_native_ata_funding::{plan_supported_ata_funding, types::ExplicitAtaCoverage};
use crate::execution_native_funding::types::{FundingOperation, UnsupportedFundingInstruction};
use crate::execution_native_rpc::{
    rent_types::ClassicAtaFundingFacts, types::AccountObservation, NativeFundingRpcClient,
};
use crate::execution_pumpswap_accounts::system_program_id;
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{anyhow, ensure, Result};
use std::{sync::OnceLock, time::Duration};

// One closed client/pool across attempts. Construction failure is a BUY error, never a panic.
static CLIENT: OnceLock<Result<NativeFundingRpcClient, ()>> = OnceLock::new();

pub(crate) async fn collect_and_check(
    endpoint: &str,
    timeout_ms: u64,
    payload: &str,
    wallet: PubkeyBytes,
    reserve: u64,
) -> Result<InitialSolRequirement> {
    collect_with_policy(endpoint, timeout_ms, payload, wallet, reserve, None).await
}

pub(crate) async fn collect_with_policy(
    endpoint: &str,
    timeout_ms: u64,
    payload: &str,
    wallet: PubkeyBytes,
    reserve: u64,
    protected: Option<&crate::execution_native_floor_policy::protected::ContextProof>,
) -> Result<InitialSolRequirement> {
    let client = CLIENT.get_or_init(|| NativeFundingRpcClient::new().map_err(|_| ()));
    let client = client
        .as_ref()
        .map_err(|_| anyhow!("initial_sol_client_unavailable"))?;
    let facts = client
        .collect_with_supported_ata_rent(
            endpoint,
            Duration::from_millis(timeout_ms),
            payload,
            wallet,
            None,
        )
        .await
        .map_err(|error| anyhow!("initial_sol_observations_unavailable:{error:#}"))?;
    check_with_policy(payload, wallet, reserve, &facts, protected)
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InitialSolRequirement {
    pub(crate) reserve: u64,
    pub(crate) total_fee: u64,
    pub(crate) outgoing_transfers: u128,
    pub(crate) classic_ata: u128,
    pub(crate) token2022_ata: u128,
    pub(crate) required: u128,
    pub(crate) observed: u64,
}

pub(crate) fn check(
    payload: &str,
    wallet: PubkeyBytes,
    reserve: u64,
    facts: &ClassicAtaFundingFacts,
) -> Result<InitialSolRequirement> {
    check_with_policy(payload, wallet, reserve, facts, None)
}
pub(crate) fn check_with_policy(
    payload: &str,
    wallet: PubkeyBytes,
    reserve: u64,
    facts: &ClassicAtaFundingFacts,
    protected: Option<&crate::execution_native_floor_policy::protected::ContextProof>,
) -> Result<InitialSolRequirement> {
    ensure!(reserve > 0, "initial_sol_invalid_reserve");
    if let Some(proof) = protected {
        proof.verify_floor(payload, wallet, reserve)?;
    }
    // Planner independently verifies full signed payload, wallet, roles and keyed observations.
    let plan = plan_supported_ata_funding(payload, wallet, facts)
        .map_err(|_| anyhow!("initial_sol_observations_binding"))?;
    let payer = facts
        .native()
        .accounts()
        .value
        .first()
        .ok_or_else(|| anyhow!("initial_sol_payer_unavailable"))?;
    ensure!(payer.pubkey == wallet, "initial_sol_payer_identity");
    let observed = match &payer.account {
        AccountObservation::Present {
            lamports,
            owner_program,
            executable,
            data,
        } if *owner_program == system_program_id() && !executable && data.is_empty() => *lamports,
        _ => return Err(anyhow!("initial_sol_payer_unavailable")),
    };
    let total_fee = facts
        .native()
        .fee()
        .value
        .ok_or_else(|| anyhow!("initial_sol_fee_unavailable"))?;
    ensure!(
        plan.explicit_ata_coverage == ExplicitAtaCoverage::Complete,
        "initial_sol_unsupported_setup"
    );
    let mut outgoing_transfers = 0_u128;
    let token2022 = crate::execution_pumpswap_accounts::parse_pubkey(
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        "initial_sol_token_program",
    )?;
    for row in &facts.native().requirements().requirements {
        ensure!(
            row.instruction.program.pubkey != token2022,
            "initial_sol_unsupported_setup"
        );
        match &row.operation {
            FundingOperation::SystemTransfer {
                from, to, lamports, ..
            } if *from == wallet && *to != wallet => {
                outgoing_transfers = outgoing_transfers
                    .checked_add(u128::from(*lamports))
                    .ok_or_else(|| anyhow!("initial_sol_overflow"))?;
            }
            // Unknown explicit system/token setup is not a proven zero. Other opaque
            // CPI remains outside this budget; Jupiter calls are refused below.
            FundingOperation::Unresolved { reason }
                if matches!(
                    reason,
                    UnsupportedFundingInstruction::SystemOpcode
                        | UnsupportedFundingInstruction::AssociatedTokenOpcode
                        | UnsupportedFundingInstruction::ClassicTokenOpcode
                ) =>
            {
                return Err(anyhow!("initial_sol_unsupported_setup"));
            }
            _ => {}
        }
    }
    let required = [
        u128::from(reserve),
        u128::from(total_fee),
        outgoing_transfers,
        plan.known_wallet_payer_lamports,
    ]
    .into_iter()
    .try_fold(0_u128, |sum, v| {
        sum.checked_add(v)
            .ok_or_else(|| anyhow!("initial_sol_overflow"))
    })?;
    ensure!(
        u128::from(observed) >= required,
        "initial_sol_insufficient:observed={observed}:required={required}:shortfall={}",
        required - u128::from(observed)
    );
    // Exact resolved outer programs, after existing identity/binding and budget checks.
    // The alternative proves an absolute final native floor only. Remaining CPI
    // coverage stays Unknown; no accepted decoded Jupiter funding profile exists.
    let jupiter = crate::execution_pumpswap_accounts::parse_pubkey(
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        "initial_sol_jupiter_program",
    )?;
    ensure!(
        protected.is_some()
            || !facts
                .native()
                .requirements()
                .requirements
                .iter()
                .any(|row| row.instruction.program.pubkey == jupiter),
        "initial_sol_jupiter_funding_unproven"
    );
    Ok(InitialSolRequirement {
        reserve,
        total_fee,
        outgoing_transfers,
        classic_ata: plan
            .known_wallet_payer_lamports
            .checked_sub(plan.known_wallet_token2022_payer_lamports)
            .ok_or_else(|| anyhow!("initial_sol_overflow"))?,
        token2022_ata: plan.known_wallet_token2022_payer_lamports,
        required,
        observed,
    })
}
