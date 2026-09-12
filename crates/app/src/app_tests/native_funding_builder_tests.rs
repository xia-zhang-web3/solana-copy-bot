use super::native_funding_fixture::*;
use crate::execution_native_funding::{decode_native_funding_requirements as decode, types::*};
use crate::execution_pumpswap_accounts::*;
use anyhow::Result;

fn label(operation: &FundingOperation) -> &'static str {
    match operation {
        FundingOperation::ComputeBudget => "cu",
        FundingOperation::SystemTransfer { .. } => "transfer",
        FundingOperation::AssociatedTokenCreateIdempotent { .. } => "ata",
        FundingOperation::SyncNative { .. } => "sync",
        FundingOperation::CloseTokenAccount { .. } => "close",
        FundingOperation::Unresolved { .. } => "unresolved",
    }
}

#[test]
fn native_funding_actual_direct_buy_sell_preserve_order_extension_and_unresolved() -> Result<()> {
    let base_ata = associated_token_address(&WALLET, &wsol_mint(), &token_program_id());
    let token = parse_pubkey(TOKEN, "fixture mint")?;
    let quote_ata = associated_token_address(&WALLET, &token, &token_program_id());
    for buy in [true, false] {
        for extension in [true, false] {
            let source = direct(buy, extension, 10_000_000)?;
            let value = decode(&payload(&source)?, WALLET)?;
            let mut expected = vec!["cu", "cu"];
            if extension {
                expected.push("unresolved");
            }
            expected.extend(if buy {
                vec!["ata", "transfer", "sync", "ata", "unresolved", "close"]
            } else {
                vec!["ata", "unresolved", "close"]
            });
            assert_eq!(
                value
                    .requirements
                    .iter()
                    .map(|r| label(&r.operation))
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                value.nominal_wallet_source_transfer_operands_lamports,
                if buy { 10_000_000 } else { 0 }
            );
            assert_eq!(value.encoded_priority_fee.limit.get(), 1_400_000);
            assert_eq!(value.encoded_priority_fee.price, 10_000);
            assert_eq!(value.encoded_priority_fee.total, 14_000);
            assert_eq!(
                value.coverage,
                FundingCoverage::PartialWithStateOrUnsupported
            );
            assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
            for (index, r) in value.requirements.iter().enumerate() {
                assert_eq!(r.instruction.index, index);
                assert_eq!(r.instruction.program.pubkey, source[index].program_id);
                assert_eq!(r.instruction.data, source[index].data);
                assert_eq!(
                    r.instruction
                        .accounts
                        .iter()
                        .map(|a| a.pubkey)
                        .collect::<Vec<_>>(),
                    source[index]
                        .accounts
                        .iter()
                        .map(|a| a.pubkey)
                        .collect::<Vec<_>>()
                );
                for (key_index, resolved) in r
                    .instruction
                    .account_indices
                    .iter()
                    .zip(&r.instruction.accounts)
                {
                    assert_eq!(*resolved, value.binding.accounts[usize::from(*key_index)]);
                }
                match &r.operation {
                    FundingOperation::SystemTransfer {
                        from,
                        to,
                        lamports,
                        relation,
                    } => {
                        assert_eq!(
                            (*from, *to, *lamports, *relation),
                            (
                                WALLET,
                                base_ata,
                                10_000_000,
                                TransferRelation::WalletToOther
                            )
                        );
                    }
                    FundingOperation::AssociatedTokenCreateIdempotent {
                        payer,
                        associated_account,
                        owner,
                        mint,
                        token_program,
                        unresolved,
                    } => {
                        assert_eq!(
                            (*payer, *owner, *token_program),
                            (WALLET, WALLET, token_program_id())
                        );
                        assert!(matches!(unresolved,AccountFactsRequired::AssociatedAddressExistenceOwnerMintTokenProgramRentAndPayer));
                        if *mint == wsol_mint() {
                            assert_eq!(*associated_account, base_ata);
                        } else {
                            assert_eq!((*mint, *associated_account), (token, quote_ata));
                        }
                    }
                    FundingOperation::SyncNative {
                        account,
                        unresolved,
                    } => {
                        assert_eq!(*account, base_ata);
                        assert_eq!(
                            *unresolved,
                            AccountFactsRequired::TokenProgramOwnerMintNativeReserveAndLamports
                        );
                    }
                    FundingOperation::CloseTokenAccount {
                        account,
                        destination,
                        authority,
                        unresolved,
                    } => {
                        assert_eq!(
                            (*account, *destination, *authority),
                            (base_ata, WALLET, WALLET)
                        );
                        assert_eq!(*unresolved,AccountFactsRequired::TokenProgramOwnerMintBalanceAuthorityAndDestination);
                    }
                    FundingOperation::Unresolved { reason } => {
                        assert_eq!(
                            *reason,
                            UnsupportedFundingInstruction::ProgramSemanticsAndCpiExpenses
                        );
                        assert_eq!(r.instruction.program.pubkey, pump_amm_program_id());
                        assert!(r
                            .instruction
                            .accounts
                            .iter()
                            .any(|a| a.pubkey == WALLET && a.is_signer && a.is_writable));
                    }
                    FundingOperation::ComputeBudget => {}
                }
            }
        }
    }
    Ok(())
}

#[test]
fn native_funding_follows_transfer_bytes_even_when_quote_and_cpi_amount_differ() -> Result<()> {
    let quoted_input = 10_000_000;
    let mut source = direct(true, false, quoted_input)?;
    let transfer_index = source
        .iter()
        .position(|i| i.program_id == system_program_id())
        .unwrap();
    // A provider's actual payload can disagree with its quote and the swap CPI operand.
    source[transfer_index].data[4..].copy_from_slice(&77_u64.to_le_bytes());
    source.push(transfer(WALLET, PEER, 13)); // after CPI/close: cannot stop at the first swap
    let value = decode(&payload(&source)?, WALLET)?;
    assert_eq!(value.nominal_wallet_source_transfer_operands_lamports, 90);
    assert_ne!(
        value.nominal_wallet_source_transfer_operands_lamports,
        u128::from(quoted_input)
    );
    assert!(matches!(
        value.requirements[transfer_index].operation,
        FundingOperation::SystemTransfer { lamports: 77, .. }
    ));
    assert!(matches!(
        value.requirements.last().unwrap().operation,
        FundingOperation::SystemTransfer {
            to: PEER,
            lamports: 13,
            ..
        }
    ));
    assert_eq!(
        value.requirements.last().unwrap().instruction.index,
        source.len() - 1
    );
    assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
    Ok(())
}
