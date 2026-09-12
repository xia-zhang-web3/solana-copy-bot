use super::native_funding_fixture::{direct, payload, transfer, WALLET};
use crate::execution_native_floor::{
    prepare_final_native_floor as prepare, verify_final_native_floor as verify,
};
use crate::execution_native_funding::{decode_native_funding_requirements, types::*};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

#[test]
fn native_floor_prepare_actual_direct_layout_and_integer_reserves() -> Result<()> {
    for buy in [false, true] {
        for extension in [false, true] {
            let original = direct(buy, extension, 10_000_000)?;
            let saved = original.clone();
            let before = decode_native_funding_requirements(&payload(&original)?, WALLET)?;
            assert!(matches!(
                before.requirements.last().unwrap().operation,
                FundingOperation::CloseTokenAccount { .. }
            ));
            for reserve in [0, 2_000_000, 9_007_199_254_740_993, u64::MAX] {
                let value = prepare(WALLET, [9; 32], &original, reserve)?;
                assert_eq!(original, saved);
                assert_eq!(verify(value.payload(), WALLET, reserve)?, value);
                assert_eq!(value.wallet(), WALLET);
                assert_eq!(value.reserve_lamports(), reserve);
                assert_eq!(value.final_instruction_index(), original.len());
                let mut expected = original.clone();
                expected.push(transfer(WALLET, WALLET, reserve));
                assert_eq!(value.payload(), payload(&expected)?);
                let wire = STANDARD.decode(value.payload())?;
                assert!(wire.len() <= 1232);
                assert_eq!(&wire[1..65], &[0; 64]);
                let after = decode_native_funding_requirements(value.payload(), WALLET)?;
                assert_eq!(after.requirements[..original.len()], before.requirements);
                assert_eq!(after.binding.accounts, before.binding.accounts);
                assert_eq!(
                    after
                        .requirements
                        .last()
                        .unwrap()
                        .instruction
                        .account_indices,
                    [0, 0]
                );
                assert_eq!(
                    after.requirements.last().unwrap().operation,
                    FundingOperation::SystemTransfer {
                        from: WALLET,
                        to: WALLET,
                        lamports: reserve,
                        relation: TransferRelation::WalletToSelf,
                    }
                );
                assert_eq!(after.unavailable_budget, UnavailableNativeBudget::default());
                assert_eq!(
                    after.nominal_wallet_source_transfer_operands_lamports,
                    before.nominal_wallet_source_transfer_operands_lamports + u128::from(reserve)
                );
                eprintln!(
                    "B24_LAYOUT buy={buy} extension={extension} R={reserve} bytes={} index={}",
                    wire.len(),
                    original.len()
                );
            }
        }
    }
    Ok(())
}

#[test]
fn native_floor_preparation_rejects_existing_self_transfer_without_mutating() -> Result<()> {
    for existing in [0, 2_000_000, u64::MAX] {
        for early in [false, true] {
            let mut original = direct(true, false, 17)?;
            original.insert(
                if early { 0 } else { original.len() },
                transfer(WALLET, WALLET, existing),
            );
            let before = original.clone();
            let error = prepare(WALLET, [9; 32], &original, 7).unwrap_err();
            assert_eq!(
                error.to_string(),
                "native_floor_already_guarded_or_ambiguous"
            );
            assert_eq!(original, before);
        }
    }
    Ok(())
}

#[test]
fn native_floor_verified_binding_changes_without_implying_crypto() -> Result<()> {
    let a = prepare(WALLET, [9; 32], &direct(true, false, 17)?, 2_000_000)?;
    let b = prepare(WALLET, [9; 32], &direct(true, false, 18)?, 2_000_000)?;
    assert_eq!(a.binding().accounts, b.binding().accounts);
    assert_eq!(
        (a.wallet(), a.reserve_lamports()),
        (b.wallet(), b.reserve_lamports())
    );
    assert_ne!(a.binding().message_bytes, b.binding().message_bytes);
    assert_ne!(a.binding().message_sha256, b.binding().message_sha256);
    assert_ne!(
        a.binding().transaction_sha256,
        b.binding().transaction_sha256
    );
    assert_ne!(a, b);
    assert_eq!(a.clone(), a);
    let mut signed_shaped = STANDARD.decode(a.payload())?;
    signed_shaped[1..65].fill(127); // deliberately NOT a valid cryptographic signature
    let c = verify(&STANDARD.encode(signed_shaped), WALLET, 2_000_000)?;
    assert_eq!(c.binding().message_sha256, a.binding().message_sha256);
    assert_ne!(
        c.binding().transaction_sha256,
        a.binding().transaction_sha256
    );
    assert_ne!(c, a);
    Ok(())
}
