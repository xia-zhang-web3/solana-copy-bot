use super::final_floor_fixture::*;
use crate::execution_native_funding::{decode_native_funding_requirements, types::*};
use crate::execution_pumpswap_accounts::system_program_id;
use crate::execution_transaction_wire::decode_message;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

#[test]
fn final_floor_actual_buy_sell_layout_alias_size_and_exact_operands() -> Result<()> {
    for buy in [false, true] {
        for extension in [false, true] {
            let original = direct(buy, extension, 10_000_000)?;
            let old_payload = payload(&original)?;
            let old = decode_native_funding_requirements(&old_payload, WALLET)?;
            assert!(matches!(
                old.requirements.last().unwrap().operation,
                FundingOperation::CloseTokenAccount { .. }
            ));
            for reserve in [0, 2_000_000, 9_007_199_254_740_993, u64::MAX] {
                let mut guarded = original.clone();
                guarded.push(transfer(WALLET, WALLET, reserve));
                assert_eq!(guarded[..original.len()], original);
                let next_payload = payload(&guarded)?;
                let wire = STANDARD.decode(&next_payload)?;
                let next = decode_native_funding_requirements(&next_payload, WALLET)?;
                let decoded = decode_message(&next_payload, |_| Ok(()))?;
                assert!(wire.len() <= 1232);
                assert_eq!(wire.len(), STANDARD.decode(&old_payload)?.len() + 17);
                assert_eq!(&wire[1..65], &[0; 64]); // No signing in layout tests.
                assert_eq!(decoded.instructions.len(), original.len() + 1);
                assert_eq!(next.binding.accounts, old.binding.accounts);
                assert_eq!(next.requirements[..original.len()], old.requirements);
                // Compare each original operand and its effective compiled account metadata.
                for (source, compiled) in original.iter().zip(&decoded.instructions) {
                    assert_eq!(compiled.program.pubkey, source.program_id);
                    assert_eq!(compiled.data, source.data);
                    assert_eq!(
                        compiled
                            .accounts
                            .iter()
                            .map(|a| a.pubkey)
                            .collect::<Vec<_>>(),
                        source.accounts.iter().map(|a| a.pubkey).collect::<Vec<_>>()
                    );
                    for (requested, actual) in source.accounts.iter().zip(&compiled.accounts) {
                        assert!(!requested.is_signer || actual.is_signer);
                        assert!(!requested.is_writable || actual.is_writable);
                    }
                }
                let last = decoded.instructions.last().unwrap();
                assert_eq!(last.index, original.len());
                assert_eq!(last.program.pubkey, system_program_id());
                assert_eq!(last.account_indices, [0, 0]);
                assert_eq!(
                    next.binding
                        .accounts
                        .iter()
                        .filter(|a| a.pubkey == WALLET)
                        .count(),
                    1
                );
                assert!(last
                    .accounts
                    .iter()
                    .all(|a| a.pubkey == WALLET && a.is_signer && a.is_writable));
                assert_eq!(
                    next.requirements.last().unwrap().operation,
                    FundingOperation::SystemTransfer {
                        from: WALLET,
                        to: WALLET,
                        lamports: reserve,
                        relation: TransferRelation::WalletToSelf,
                    }
                );
                assert_ne!(old.binding.message_bytes, next.binding.message_bytes);
                assert_ne!(old.binding.message_sha256, next.binding.message_sha256);
                assert_ne!(
                    old.binding.transaction_sha256,
                    next.binding.transaction_sha256
                );
                assert_eq!(
                    next.encoded_priority_fee.total,
                    old.encoded_priority_fee.total
                );
                assert_eq!(
                    next.nominal_wallet_source_transfer_operands_lamports,
                    old.nominal_wallet_source_transfer_operands_lamports + u128::from(reserve)
                );
                // Nominal self-transfer is an operand, not extra economic or net spending.
                assert_eq!(next.unavailable_budget, UnavailableNativeBudget::default());
                assert_eq!(next.coverage, old.coverage);
                eprintln!("B23_LAYOUT buy={buy} extension={extension} R={reserve} old_bytes={} guarded_bytes={} index={} operands={} unavailable=all9",
                          STANDARD.decode(&old_payload)?.len(), wire.len(), last.index,
                          next.nominal_wallet_source_transfer_operands_lamports);
            }
        }
    }
    Ok(())
}
