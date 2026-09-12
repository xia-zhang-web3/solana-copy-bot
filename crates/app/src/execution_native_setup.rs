//! Synchronous interpretation of initial setup observations, never transaction execution.
mod classify;
mod token;
pub(crate) mod types;

use self::types::*;
use crate::execution_native_funding::{
    decode_native_funding_requirements, types::FundingOperation,
};
use crate::execution_native_rpc::types::NativeFundingRpcFacts;
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{anyhow, ensure, Result};

pub(crate) fn interpret_native_setup<'a>(
    payload: &str,
    expected_wallet: PubkeyBytes,
    facts: &'a NativeFundingRpcFacts,
) -> Result<NativeSetupInterpretation<'a>> {
    // Same pre-allocation contract as B19; do not decode an unbounded base64 string.
    ensure!(payload.len() <= 1644, "native_setup_payload_too_large");
    let actual = decode_native_funding_requirements(payload, expected_wallet)
        .map_err(|_| anyhow!("native_setup_invalid_requirements"))?;
    // Full equality includes transaction AND message identities, roles, operands,
    // CU facts, coverage and all nine unavailable budget fields, not just a hash/key.
    ensure!(
        &actual == facts.requirements(),
        "native_setup_requirements_mismatch"
    );
    let keys = &actual.binding.accounts;
    ensure!(
        keys.len() <= 100
            && facts.requested_keys().len() == keys.len()
            && facts.accounts().value.len() == keys.len(),
        "native_setup_account_shape"
    );
    ensure!(
        keys.iter()
            .enumerate()
            .all(|(i, key)| key.pubkey == facts.requested_keys()[i]
                && key.pubkey == facts.accounts().value[i].pubkey),
        "native_setup_account_binding"
    );
    ensure!(facts.commitment() == "confirmed", "native_setup_commitment");
    if let Some(floor) = facts.min_context_slot() {
        ensure!(
            facts.fee().slot >= floor && facts.accounts().slot >= floor,
            "native_setup_context_floor"
        );
    }
    // Timings are retained as observed: wall clocks can move, and no new age/clock
    // threshold is inferred here. Independent slots do not form an atomic snapshot.
    let mut result = NativeSetupInterpretation {
        facts,
        initial_accounts: Vec::new(),
        instructions: Vec::new(),
    };
    for (index, requirement) in actual.requirements.iter().enumerate() {
        let key = match &requirement.operation {
            FundingOperation::AssociatedTokenCreateIdempotent {
                associated_account, ..
            } => Some(*associated_account),
            FundingOperation::SyncNative { account, .. }
            | FundingOperation::CloseTokenAccount { account, .. } => Some(*account),
            _ => None,
        };
        let initial_account_index = key.map(|key| {
            if let Some(i) = result.initial_accounts.iter().position(|a| a.pubkey == key) {
                return i;
            }
            // Reader resolved every operand, and the full row shape was checked above.
            let i = facts
                .requested_keys()
                .iter()
                .position(|k| *k == key)
                .expect("resolved operand");
            result
                .initial_accounts
                .push(token::initial_account(&facts.accounts().value[i], i));
            result.initial_accounts.len() - 1
        });
        let interpretation = if let Some(i) = initial_account_index {
            let account = &result.initial_accounts[i];
            match &requirement.operation {
                FundingOperation::AssociatedTokenCreateIdempotent {
                    owner,
                    mint,
                    token_program,
                    ..
                } => SetupOperation::Associated(classify::associated(
                    account,
                    owner,
                    mint,
                    token_program,
                )),
                FundingOperation::SyncNative { .. } => {
                    SetupOperation::Sync(classify::sync(account))
                }
                FundingOperation::CloseTokenAccount {
                    destination,
                    authority,
                    ..
                } => SetupOperation::Close(classify::close(
                    account,
                    *destination,
                    *authority,
                    expected_wallet,
                )),
                _ => unreachable!("setup operand"),
            }
        } else {
            SetupOperation::NotSetup
        };
        result.instructions.push(SetupInstruction {
            requirement_index: index,
            initial_account_index,
            interpretation,
        });
    }
    Ok(result)
}
