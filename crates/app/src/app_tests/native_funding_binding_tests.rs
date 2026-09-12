use super::native_funding_fixture::*;
use crate::execution_native_funding::{decode_native_funding_requirements as decode, types::*};
use crate::execution_solana_tx::serialize_unsigned_legacy_transaction;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};

#[test]
fn native_funding_signed_placeholder_and_unsigned_bind_to_same_exact_message() -> Result<()> {
    let mut source = budget();
    source.push(transfer(WALLET, PEER, 101));
    for v0 in [false, true] {
        let p = payload(&source)?;
        let p = if v0 { version_zero(&p)? } else { p };
        let mut wire = STANDARD.decode(&p)?;
        let unsigned = decode(&p, WALLET)?;
        assert_eq!(unsigned.binding.message_bytes, &wire[65..]);
        assert_eq!(
            unsigned.binding.message_sha256,
            format!("{:x}", Sha256::digest(&wire[65..]))
        );
        assert_eq!(
            unsigned.binding.transaction_sha256,
            format!("{:x}", Sha256::digest(&wire))
        );
        assert_eq!(unsigned.binding.signature_count, 1);
        assert_eq!(unsigned.binding.required_signatures, 1);
        assert_eq!(unsigned.binding.readonly_signed, 0);
        assert_eq!(unsigned.binding.readonly_unsigned, 2); // System and CU programs
        assert_eq!(unsigned.binding.accounts[0].pubkey, WALLET);
        assert!(unsigned.binding.accounts[0].is_signer && unsigned.binding.accounts[0].is_writable);
        wire[1..65].fill(0xab); // Deliberately not a valid signature; parser makes no crypto claim.
        let replaced = decode(&STANDARD.encode(wire), WALLET)?;
        assert_eq!(
            unsigned.binding.message_bytes,
            replaced.binding.message_bytes
        );
        assert_eq!(
            unsigned.binding.message_sha256,
            replaced.binding.message_sha256
        );
        assert_ne!(
            unsigned.binding.transaction_sha256,
            replaced.binding.transaction_sha256
        );
        assert_eq!(unsigned.requirements, replaced.requirements);
        assert_eq!(unsigned.unavailable_budget, replaced.unavailable_budget);
        assert_eq!(
            unsigned.nominal_wallet_source_transfer_operands_lamports,
            replaced.nominal_wallet_source_transfer_operands_lamports
        );
        assert_eq!(
            unsigned.encoded_priority_fee.total,
            replaced.encoded_priority_fee.total
        );
        assert_eq!(
            replaced.encoded_priority_fee.message_sha256,
            replaced.binding.message_sha256
        );
        assert_eq!(
            replaced.encoded_priority_fee.transaction_sha256,
            replaced.binding.transaction_sha256
        );
    }
    Ok(())
}

#[test]
fn native_funding_instruction_account_and_blockhash_changes_rebind_requirements() -> Result<()> {
    let mut source = budget();
    source.push(transfer(WALLET, PEER, 101));
    let old = decode(&payload(&source)?, WALLET)?;
    let mut variants = Vec::new();
    let mut changed = source.clone();
    changed[2].data[4..].copy_from_slice(&102_u64.to_le_bytes());
    variants.push(payload(&changed)?);
    let mut changed = source.clone();
    changed[2].accounts[1].pubkey = [53; 32];
    variants.push(payload(&changed)?);
    variants.push(STANDARD.encode(serialize_unsigned_legacy_transaction(
        WALLET, [8; 32], &source,
    )?));
    for changed in variants {
        let new = decode(&changed, WALLET)?;
        assert_ne!(old.binding.message_sha256, new.binding.message_sha256);
        assert_ne!(old.binding.message_bytes, new.binding.message_bytes);
        assert_ne!(
            old.binding.transaction_sha256,
            new.binding.transaction_sha256
        );
        assert_eq!(new.unavailable_budget, UnavailableNativeBudget::default());
    }
    assert_eq!(
        decode(&payload(&source)?, PEER).unwrap_err().to_string(),
        "native_funding_expected_wallet_mismatch"
    );
    Ok(())
}

#[test]
fn native_funding_ordered_mixed_flows_do_not_net_or_assume_peak() -> Result<()> {
    let mut source = budget();
    let mut incoming = transfer(PEER, WALLET, 17);
    incoming.accounts[0].is_signer = false;
    source.extend([
        incoming,
        transfer(WALLET, PEER, 11),
        transfer(WALLET, WALLET, 3),
        transfer(WALLET, PEER, 7),
    ]);
    let mut wire = STANDARD.decode(payload(&source)?)?;
    assert_eq!(&wire[101..133], &PEER);
    wire[65] = 2;
    wire[0] = 2;
    wire.splice(1..1, [0; 64]);
    let value = decode(&STANDARD.encode(wire), WALLET)?;
    assert_eq!(value.nominal_wallet_source_transfer_operands_lamports, 21);
    let flows: Vec<_> = value
        .requirements
        .iter()
        .filter_map(|r| match r.operation {
            FundingOperation::SystemTransfer {
                relation, lamports, ..
            } => Some((r.instruction.index, relation, lamports)),
            _ => None,
        })
        .collect();
    assert_eq!(
        flows,
        [
            (2, TransferRelation::OtherToWallet, 17),
            (3, TransferRelation::WalletToOther, 11),
            (4, TransferRelation::WalletToSelf, 3),
            (5, TransferRelation::WalletToOther, 7)
        ]
    );
    assert_eq!(value.unavailable_budget, UnavailableNativeBudget::default());
    Ok(())
}
