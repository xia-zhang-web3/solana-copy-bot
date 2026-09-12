use super::native_funding_fixture::*;
use crate::execution_native_funding::{decode_native_funding_requirements as decode, types::*};
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{SolanaAccountMeta as M, SolanaInstruction};
use anyhow::Result;

#[test]
fn native_funding_ata_existence_rent_derivation_and_token_program_need_state() -> Result<()> {
    let token_2022 = parse_pubkey("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "fixture")?;
    let mut ata = direct(true, false, 7)?
        .into_iter()
        .find(|i| i.program_id == associated_token_program_id())
        .unwrap();
    // Arbitrary candidate ATA and Token-2022 operand: offline parsing proves neither
    // derivation nor account existence/layout; it cannot price idempotent creation.
    ata.accounts[1].pubkey = PEER;
    ata.accounts[5].pubkey = token_2022;
    let mut source = budget();
    source.push(ata);
    let value = decode(&payload(&source)?, WALLET)?;
    assert_eq!(
        value.requirements[2].operation,
        FundingOperation::AssociatedTokenCreateIdempotent {
            payer: WALLET,
            associated_account: PEER,
            owner: WALLET,
            mint: wsol_mint(),
            token_program: token_2022,
            unresolved:
                AccountFactsRequired::AssociatedAddressExistenceOwnerMintTokenProgramRentAndPayer,
        }
    );
    assert_eq!(value.unavailable_budget.rent_lamports, None);
    assert_eq!(value.unavailable_budget.available_sol_lamports, None);
    assert_eq!(
        value.coverage,
        FundingCoverage::PartialWithStateOrUnsupported
    );
    Ok(())
}

#[test]
fn native_funding_non_wsol_close_and_sync_cannot_prove_mint_or_refund() -> Result<()> {
    let mut source = budget();
    source.push(SolanaInstruction {
        program_id: token_program_id(),
        accounts: vec![M::writable(PEER)],
        data: vec![17],
    });
    source.push(SolanaInstruction {
        program_id: token_program_id(),
        accounts: vec![
            M::writable(PEER),
            M::writable(WALLET),
            M::signer_writable(WALLET),
        ],
        data: vec![9],
    });
    source.push(transfer(WALLET, PEER, 5));
    let value = decode(&payload(&source)?, WALLET)?;
    assert_eq!(
        value.requirements[2].operation,
        FundingOperation::SyncNative {
            account: PEER,
            unresolved: AccountFactsRequired::TokenProgramOwnerMintNativeReserveAndLamports
        }
    );
    assert_eq!(
        value.requirements[3].operation,
        FundingOperation::CloseTokenAccount {
            account: PEER,
            destination: WALLET,
            authority: WALLET,
            unresolved: AccountFactsRequired::TokenProgramOwnerMintBalanceAuthorityAndDestination
        }
    );
    assert_eq!(value.nominal_wallet_source_transfer_operands_lamports, 5);
    assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
    Ok(())
}

#[test]
fn native_funding_unsupported_programs_opcodes_and_token_2022_preserve_all_operands() -> Result<()>
{
    let token_2022 = parse_pubkey("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "fixture")?;
    for (program, data, reason) in [
        (
            system_program_id(),
            99_u32.to_le_bytes().to_vec(),
            UnsupportedFundingInstruction::SystemOpcode,
        ),
        (
            associated_token_program_id(),
            vec![],
            UnsupportedFundingInstruction::AssociatedTokenOpcode,
        ),
        (
            token_program_id(),
            vec![3, 7, 0, 0, 0, 0, 0, 0, 0],
            UnsupportedFundingInstruction::ClassicTokenOpcode,
        ),
        (
            token_2022,
            vec![9],
            UnsupportedFundingInstruction::ProgramSemanticsAndCpiExpenses,
        ),
        (
            [91; 32],
            vec![2, 8, 7, 6],
            UnsupportedFundingInstruction::ProgramSemanticsAndCpiExpenses,
        ),
    ] {
        let mut source = budget();
        source.push(SolanaInstruction {
            program_id: program,
            accounts: vec![M::writable(PEER), M::signer_writable(WALLET)],
            data: data.clone(),
        });
        let v = decode(&payload(&source)?, WALLET)?;
        let last = v.requirements.last().unwrap();
        assert_eq!(last.instruction.index, 2);
        assert_eq!(last.instruction.program.pubkey, program);
        assert_eq!(last.instruction.data, data);
        assert_eq!(last.instruction.accounts[0].pubkey, PEER);
        assert!(last.instruction.accounts[1].is_signer);
        assert_eq!(last.operation, FundingOperation::Unresolved { reason });
        assert_eq!(v.coverage, FundingCoverage::PartialWithStateOrUnsupported);
        assert_eq!(v.unavailable_budget, UnavailableNativeBudget::default());
    }
    Ok(())
}
