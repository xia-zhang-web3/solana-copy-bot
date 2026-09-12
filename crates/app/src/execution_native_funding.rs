//! Offline prerequisite for the next exact-message RPC budget stage. No runtime gate.
//! Programs/CPI and account state can spend beyond these operands; no refund is assumed.
pub(crate) mod types;
use self::types::*;
use crate::execution_priority_fee_wire::decode_priority_fee_message;
use crate::execution_pumpswap_accounts::{
    associated_token_program_id, compute_budget_program_id, system_program_id, token_program_id,
};
use crate::execution_solana_tx::PubkeyBytes;
use crate::execution_transaction_wire::DecodedInstruction;
use anyhow::{ensure, Context, Result};

pub(crate) fn decode_native_funding_requirements(
    payload: &str,
    expected_wallet: PubkeyBytes,
) -> Result<NativeFundingRequirements> {
    let (message, encoded_priority_fee) = decode_priority_fee_message(payload)?;
    let payer = &message.binding.accounts[0]; // shared header proves a first writable signer
    ensure!(
        payer.pubkey == expected_wallet,
        "native_funding_expected_wallet_mismatch"
    );
    ensure!(
        payer.is_signer && payer.is_writable,
        "native_funding_invalid_fee_payer_roles"
    );
    let requirements: Vec<_> = message
        .instructions
        .into_iter()
        .map(|instruction| {
            let operation = classify(&instruction, expected_wallet).with_context(|| {
                format!("native_funding_instruction_index={}", instruction.index)
            })?;
            Ok(FundingRequirement {
                instruction,
                operation,
            })
        })
        .collect::<Result<_>>()?;
    let nominal_wallet_source_transfer_operands_lamports =
        checked_operand_sum(requirements.iter().filter_map(|r| match &r.operation {
            FundingOperation::SystemTransfer { from, lamports, .. } if *from == expected_wallet => {
                Some(u128::from(*lamports))
            }
            _ => None,
        }))?;
    let coverage = if requirements.iter().all(|r| {
        matches!(
            r.operation,
            FundingOperation::ComputeBudget | FundingOperation::SystemTransfer { .. }
        )
    }) {
        FundingCoverage::ExplicitOperandsOnly
    } else {
        FundingCoverage::PartialWithStateOrUnsupported
    };
    Ok(NativeFundingRequirements {
        binding: message.binding,
        expected_wallet,
        requirements,
        nominal_wallet_source_transfer_operands_lamports,
        encoded_priority_fee,
        coverage,
        unavailable_budget: UnavailableNativeBudget::default(),
    })
}

pub(crate) fn checked_operand_sum(values: impl IntoIterator<Item = u128>) -> Result<u128> {
    values.into_iter().try_fold(0_u128, |sum, value| {
        sum.checked_add(value)
            .context("native_funding_operand_sum_overflow")
    })
}

fn classify(i: &DecodedInstruction, wallet: PubkeyBytes) -> Result<FundingOperation> {
    let program = i.program.pubkey;
    if program == compute_budget_program_id() {
        // The existing CU consumer validated data and the empty operand list.
        program_roles(i)?;
        return Ok(FundingOperation::ComputeBudget);
    }
    if program == system_program_id() {
        ensure!(
            i.data.len() >= 4,
            "native_funding_malformed_system_discriminator"
        );
        let opcode = u32::from_le_bytes(i.data[..4].try_into()?);
        if opcode != 2 {
            return Ok(unsupported(UnsupportedFundingInstruction::SystemOpcode));
        }
        program_roles(i)?;
        ensure!(
            i.data.len() == 12 && i.accounts.len() == 2,
            "native_funding_malformed_system_transfer"
        );
        let (from, to) = (i.accounts[0], i.accounts[1]);
        ensure!(
            from.is_signer && from.is_writable && to.is_writable,
            "native_funding_invalid_transfer_roles"
        );
        let relation = match (from.pubkey == wallet, to.pubkey == wallet) {
            (true, true) => TransferRelation::WalletToSelf,
            (true, false) => TransferRelation::WalletToOther,
            (false, true) => TransferRelation::OtherToWallet,
            (false, false) => TransferRelation::OtherToOther,
        };
        return Ok(FundingOperation::SystemTransfer {
            from: from.pubkey,
            to: to.pubkey,
            lamports: u64::from_le_bytes(i.data[4..].try_into()?),
            relation,
        });
    }
    if program == associated_token_program_id() {
        if i.data.first() != Some(&1) {
            return Ok(unsupported(
                UnsupportedFundingInstruction::AssociatedTokenOpcode,
            ));
        }
        program_roles(i)?;
        ensure!(
            i.data.len() == 1 && i.accounts.len() == 6,
            "native_funding_malformed_ata_create_idempotent"
        );
        let a = &i.accounts;
        ensure!(
            a[0].is_signer
                && a[0].is_writable
                && a[1].is_writable
                && a[4].pubkey == system_program_id()
                && !a[4].is_signer
                && !a[4].is_writable
                && !a[5].is_signer
                && !a[5].is_writable,
            "native_funding_invalid_ata_roles"
        );
        return Ok(FundingOperation::AssociatedTokenCreateIdempotent {
            payer: a[0].pubkey,
            associated_account: a[1].pubkey,
            owner: a[2].pubkey,
            mint: a[3].pubkey,
            token_program: a[5].pubkey,
            unresolved:
                AccountFactsRequired::AssociatedAddressExistenceOwnerMintTokenProgramRentAndPayer,
        });
    }
    if program == token_program_id() {
        match i.data.first() {
            Some(17) => {
                program_roles(i)?;
                ensure!(
                    i.data.len() == 1 && i.accounts.len() == 1,
                    "native_funding_malformed_sync_native"
                );
                ensure!(
                    i.accounts[0].is_writable,
                    "native_funding_invalid_sync_native_roles"
                );
                return Ok(FundingOperation::SyncNative {
                    account: i.accounts[0].pubkey,
                    unresolved: AccountFactsRequired::TokenProgramOwnerMintNativeReserveAndLamports,
                });
            }
            Some(9) => {
                program_roles(i)?;
                ensure!(
                    i.data.len() == 1 && i.accounts.len() == 3,
                    "native_funding_malformed_close_account"
                );
                ensure!(
                    i.accounts[0].is_writable
                        && i.accounts[1].is_writable
                        && i.accounts[2].is_signer,
                    "native_funding_invalid_close_roles"
                );
                return Ok(FundingOperation::CloseTokenAccount {
                    account: i.accounts[0].pubkey,
                    destination: i.accounts[1].pubkey,
                    authority: i.accounts[2].pubkey,
                    unresolved:
                        AccountFactsRequired::TokenProgramOwnerMintBalanceAuthorityAndDestination,
                });
            }
            _ => {
                return Ok(unsupported(
                    UnsupportedFundingInstruction::ClassicTokenOpcode,
                ))
            }
        }
    }
    Ok(unsupported(
        UnsupportedFundingInstruction::ProgramSemanticsAndCpiExpenses,
    ))
}

fn unsupported(reason: UnsupportedFundingInstruction) -> FundingOperation {
    FundingOperation::Unresolved { reason }
}

fn program_roles(i: &DecodedInstruction) -> Result<()> {
    ensure!(
        !i.program.is_signer && !i.program.is_writable,
        "native_funding_invalid_program_roles"
    );
    Ok(())
}
