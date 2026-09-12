//! Ordered initial-state funding for classic and opt-in metadata-only Token2022 ATAs.
mod metadata;
mod profile;
pub(crate) mod token2022;
pub(crate) mod types;

use self::types::*;
use crate::execution_native_funding::types::FundingOperation;
use crate::execution_native_rpc::rent_types::ClassicAtaFundingFacts;
use crate::execution_native_setup::{
    interpret_native_setup,
    types::{AssociatedInitialState as State, SetupOperation},
};
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{anyhow, Result};
use std::collections::HashSet;

pub(crate) fn plan_classic_ata_funding<'a>(
    payload: &str,
    expected_wallet: PubkeyBytes,
    facts: &'a ClassicAtaFundingFacts,
) -> Result<ClassicAtaFundingPlan<'a>> {
    plan(payload, expected_wallet, facts, false)
}

/// Opt-in supported Token2022 ATA rows; classic callers retain their old coverage.
pub(crate) fn plan_supported_ata_funding<'a>(
    payload: &str,
    expected_wallet: PubkeyBytes,
    facts: &'a ClassicAtaFundingFacts,
) -> Result<ClassicAtaFundingPlan<'a>> {
    plan(payload, expected_wallet, facts, true)
}

fn plan<'a>(
    payload: &str,
    expected_wallet: PubkeyBytes,
    facts: &'a ClassicAtaFundingFacts,
    with_token2022: bool,
) -> Result<ClassicAtaFundingPlan<'a>> {
    // The planner binds the full payload/wallet itself. No caller-supplied partial interpretation.
    let setup = interpret_native_setup(payload, expected_wallet, facts.native())?;
    let mut rows = Vec::new();
    let mut changed = HashSet::new();
    let mut opaque_before = false;
    let mut known_wallet_payer_lamports = 0_u128;
    let mut known_wallet_token2022_payer_lamports = 0_u128;
    let mut coverage = ExplicitAtaCoverage::Complete;
    for (index, requirement) in facts
        .native()
        .requirements()
        .requirements
        .iter()
        .enumerate()
    {
        match &requirement.operation {
            FundingOperation::AssociatedTokenCreateIdempotent {
                payer,
                associated_account,
                token_program,
                mint,
                ..
            } => {
                let amount = if opaque_before {
                    AtaFundingAmount::Unresolved(AtaFundingIssue::PriorOpaqueInstruction)
                } else if changed.contains(associated_account)
                    || (with_token2022
                        && *token_program == token2022::program_id()
                        && changed.contains(mint))
                {
                    AtaFundingAmount::Unresolved(AtaFundingIssue::PriorAccountWrite)
                } else if with_token2022 && *token_program == token2022::program_id() {
                    token2022::funding(facts, index)
                } else {
                    let row = &setup.instructions[index];
                    let SetupOperation::Associated(state) = row.interpretation else {
                        unreachable!("ATA interpretation")
                    };
                    let account =
                        &setup.initial_accounts[row.initial_account_index.expect("ATA account")];
                    // spl-associated-token-account 4.0.0 tools/account.rs create_pda_account:
                    // max(rent.minimum_balance(space), 1), less observed prefunding if present.
                    // This does not establish mint initialization or successful CPI/swap execution.
                    match state {
                        State::CreationRequiredAbsent => {
                            AtaFundingAmount::Known(facts.rent().lamports().max(1))
                        }
                        State::CreationCandidateSystemPrefunded => {
                            AtaFundingAmount::Known(facts.rent().lamports().max(1).saturating_sub(
                                account.observed_lamports.expect("present system account"),
                            ))
                        }
                        State::ExistingIdentityMatch => AtaFundingAmount::Known(0),
                        other => AtaFundingAmount::Unresolved(AtaFundingIssue::InitialState(other)),
                    }
                };
                match amount {
                    AtaFundingAmount::Known(value) if *payer == expected_wallet => {
                        known_wallet_payer_lamports = known_wallet_payer_lamports
                            .checked_add(u128::from(value))
                            .ok_or_else(|| anyhow!("native_ata_funding_sum_overflow"))?;
                        if *token_program == token2022::program_id() {
                            known_wallet_token2022_payer_lamports =
                                known_wallet_token2022_payer_lamports
                                    .checked_add(u128::from(value))
                                    .ok_or_else(|| anyhow!("native_ata_funding_sum_overflow"))?;
                        }
                    }
                    AtaFundingAmount::Unresolved(_) => coverage = ExplicitAtaCoverage::Partial,
                    _ => {}
                }
                rows.push(AtaFundingRow {
                    requirement_index: index,
                    payer: *payer,
                    account: *associated_account,
                    amount,
                });
                // No projection of the post-create identity; even an initial zero must be revisited.
                changed.extend([*payer, *associated_account]);
            }
            FundingOperation::SystemTransfer { from, to, .. } => {
                changed.extend([*from, *to]);
            }
            FundingOperation::SyncNative { account, .. } => {
                changed.insert(*account);
            }
            FundingOperation::CloseTokenAccount {
                account,
                destination,
                ..
            } => {
                changed.extend([*account, *destination]);
            }
            FundingOperation::Unresolved { .. } => opaque_before = true,
            FundingOperation::ComputeBudget => {}
        }
    }
    Ok(ClassicAtaFundingPlan {
        facts,
        setup,
        rows,
        known_wallet_payer_lamports,
        known_wallet_token2022_payer_lamports,
        explicit_ata_coverage: coverage,
    })
}
