use super::native_funding_fixture::*;
use crate::execution_native_funding::{
    checked_operand_sum, decode_native_funding_requirements as decode, types::*,
};
use anyhow::Result;

#[test]
fn native_funding_exact_zero_max_large_sums_and_self_are_only_nominal_operands() -> Result<()> {
    let mut instructions = budget();
    instructions.extend([
        transfer(WALLET, PEER, 0),
        transfer(WALLET, PEER, u64::MAX),
        transfer(WALLET, PEER, u64::MAX),
        transfer(WALLET, WALLET, 7),
    ]);
    let value = decode(&payload(&instructions)?, WALLET)?;
    assert_eq!(
        value.nominal_wallet_source_transfer_operands_lamports,
        2 * u128::from(u64::MAX) + 7
    );
    assert!(value.nominal_wallet_source_transfer_operands_lamports > (1_u128 << 64));
    let amounts: Vec<_> = value
        .requirements
        .iter()
        .filter_map(|r| match r.operation {
            FundingOperation::SystemTransfer { lamports, .. } => Some(lamports),
            _ => None,
        })
        .collect();
    assert_eq!(amounts, [0, u64::MAX, u64::MAX, 7]);
    assert!(matches!(
        value.requirements.last().unwrap().operation,
        FundingOperation::SystemTransfer {
            relation: TransferRelation::WalletToSelf,
            ..
        }
    ));
    assert_eq!(value.coverage, FundingCoverage::ExplicitOperandsOnly);
    assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
    Ok(())
}

#[test]
fn native_funding_foreign_signer_incoming_never_becomes_wallet_debit_or_guaranteed_credit(
) -> Result<()> {
    for to in [WALLET, [53; 32]] {
        let value = decode(&foreign_signer_transfer(to, u64::MAX)?, WALLET)?;
        assert_eq!(value.nominal_wallet_source_transfer_operands_lamports, 0);
        assert_eq!(value.binding.signature_count, 2);
        assert_eq!(value.binding.required_signatures, 2);
        assert!(value.binding.accounts[1].is_signer && value.binding.accounts[1].is_writable);
        assert!(matches!(
            value.requirements[2].operation,
            FundingOperation::SystemTransfer {
                from: PEER,
                lamports: u64::MAX,
                ..
            }
        ));
        let relation = if to == WALLET {
            TransferRelation::OtherToWallet
        } else {
            TransferRelation::OtherToOther
        };
        if let FundingOperation::SystemTransfer { relation: r, .. } =
            value.requirements[2].operation
        {
            assert_eq!(r, relation);
        }
        assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
    }
    Ok(())
}

#[test]
fn native_funding_checked_sum_overflow_is_an_error_never_zero() -> Result<()> {
    assert_eq!(checked_operand_sum([])?, 0);
    assert_eq!(checked_operand_sum([u128::MAX, 0])?, u128::MAX);
    assert_eq!(
        checked_operand_sum([u128::MAX, 1]).unwrap_err().to_string(),
        "native_funding_operand_sum_overflow"
    );
    // A valid 1232-byte message cannot contain enough u64 operands to reach u128::MAX.
    // The actual summation helper is exercised at its arithmetic boundary separately.
    Ok(())
}

#[test]
fn native_funding_zero_priority_and_no_transfer_do_not_prove_zero_full_fee() -> Result<()> {
    let value = decode(
        &payload(&super::priority_fee_fixture::budget(1, 0))?,
        WALLET,
    )?;
    assert_eq!(value.encoded_priority_fee.total, 0);
    assert_eq!(value.nominal_wallet_source_transfer_operands_lamports, 0);
    assert_eq!(value.coverage, FundingCoverage::ExplicitOperandsOnly);
    assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
    Ok(())
}
