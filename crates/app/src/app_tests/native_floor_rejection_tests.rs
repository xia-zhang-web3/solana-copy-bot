use super::native_funding_fixture::{budget, payload, transfer, version_zero, PEER, WALLET};
use crate::execution_native_floor::{
    prepare_final_native_floor as prepare, verify_final_native_floor as verify,
};
use crate::execution_solana_tx::{
    serialize_unsigned_legacy_transaction, SolanaAccountMeta, SolanaInstruction,
};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

#[test]
fn native_floor_verifier_rejects_precise_contract_failures() -> Result<()> {
    let guard = transfer(WALLET, WALLET, 2_000_000);
    let mut cases: Vec<(&str, Vec<SolanaInstruction>, &str)> = vec![
        ("missing", vec![], "native_floor_missing_guard"),
        (
            "early before compute",
            vec![guard.clone(), budget()[0].clone()],
            "native_floor_program",
        ),
        (
            "early before debit",
            vec![guard.clone(), transfer(WALLET, PEER, 1)],
            "native_floor_wallet_operands",
        ),
        (
            "to",
            vec![transfer(WALLET, PEER, 2_000_000)],
            "native_floor_wallet_operands",
        ),
    ];
    let mut from = transfer(PEER, WALLET, 2_000_000);
    from.accounts[0].is_signer = false; // Reach operand authority check with a valid single-signer header.
    cases.push(("from", vec![from], "native_floor_wallet_operands"));
    let mut program = guard.clone();
    program.program_id = PEER;
    cases.push(("program", vec![program], "native_floor_program"));
    let writer = SolanaInstruction {
        program_id: PEER,
        accounts: vec![SolanaAccountMeta::writable([0; 32])],
        data: vec![],
    };
    cases.push((
        "writable program",
        vec![writer, guard.clone()],
        "native_floor_program_flags",
    ));
    for count in [0, 1, 3] {
        let mut ix = guard.clone();
        ix.accounts
            .resize(count, SolanaAccountMeta::writable(WALLET));
        cases.push(("operands", vec![ix], "native_floor_operands"));
    }
    for opcode in [0_u32, 4, 11] {
        let mut ix = guard.clone();
        ix.data[..4].copy_from_slice(&opcode.to_le_bytes());
        cases.push(("opcode", vec![ix], "native_floor_transfer_encoding"));
    }
    for length in [0, 4, 11, 13] {
        let mut ix = guard.clone();
        ix.data.resize(length, 0);
        cases.push(("length", vec![ix], "native_floor_transfer_encoding"));
    }
    for (case, instructions, expected) in cases {
        let p = payload(&instructions)?;
        assert_eq!(
            verify(&p, WALLET, 2_000_000).unwrap_err().to_string(),
            expected,
            "{case}"
        );
    }
    let correct = payload(&[guard])?;
    assert_eq!(
        verify(&correct, PEER, 2_000_000).unwrap_err().to_string(),
        "native_floor_wallet_payer"
    );
    assert_eq!(
        verify(&correct, WALLET, 2_000_001).unwrap_err().to_string(),
        "native_floor_reserve_mismatch"
    );
    let wrong_payer = STANDARD.encode(serialize_unsigned_legacy_transaction(
        PEER,
        [9; 32],
        &[transfer(PEER, PEER, 2_000_000)],
    )?);
    assert_eq!(
        verify(&wrong_payer, WALLET, 2_000_000)
            .unwrap_err()
            .to_string(),
        "native_floor_wallet_payer"
    );
    Ok(())
}

#[test]
fn native_floor_verifier_preserves_wire_rejections_and_packet_bound() -> Result<()> {
    let valid = payload(&[transfer(WALLET, WALLET, 2_000_000)])?;
    let wire = STANDARD.decode(&valid)?;
    let mut cases: Vec<(Vec<u8>, &str)> = Vec::new();
    let mut duplicate = wire.clone();
    duplicate[101..133].copy_from_slice(&WALLET);
    cases.push((duplicate, "priority_fee_duplicate_account_key"));
    let mut trailing = wire.clone();
    trailing.push(0);
    cases.push((trailing, "priority_fee_trailing_wire_data"));
    let mut truncated = wire.clone();
    truncated.pop();
    cases.push((truncated, "priority_fee_truncated_wire"));
    let mut index = wire.clone();
    let last_operand = index.len() - 14;
    index[last_operand] = 255;
    cases.push((index, "priority_fee_unresolved_account_index"));
    let mut readonly = wire.clone();
    readonly[66] = 1;
    cases.push((readonly, "priority_fee_invalid_header")); // single read-only signer is rejected by the parser
    let mut shortvec = wire.clone();
    shortvec.splice(0..1, [0x81, 0]);
    cases.push((shortvec, "priority_fee_noncanonical_shortvec"));
    let mut oversized = wire.clone();
    oversized.resize(1233, 0);
    cases.push((oversized, "priority_fee_transaction_too_large"));
    let mut multi = STANDARD.decode(payload(&[
        transfer(WALLET, PEER, 1),
        transfer(WALLET, WALLET, 2_000_000),
    ])?)?;
    multi[65] = 2;
    multi[0] = 2;
    multi.splice(1..1, [0; 64]);
    cases.push((multi, "native_floor_single_signer"));
    let v0 = version_zero(&valid)?;
    cases.push((STANDARD.decode(&v0)?, "native_floor_unsupported_version"));
    let mut alt = STANDARD.decode(v0)?;
    *alt.last_mut().unwrap() = 1;
    cases.push((alt, "priority_fee_unresolved_address_lookup_table"));
    for (bytes, expected) in cases {
        assert_eq!(
            verify(&STANDARD.encode(bytes), WALLET, 2_000_000)
                .unwrap_err()
                .to_string(),
            expected
        );
    }
    assert_eq!(
        verify(&"A".repeat(1645), WALLET, 0)
            .unwrap_err()
            .to_string(),
        "native_floor_payload_too_large"
    );
    assert_eq!(
        verify("!", WALLET, 0).unwrap_err().to_string(),
        "priority_fee_invalid_base64"
    );
    let huge = SolanaInstruction {
        program_id: PEER,
        accounts: vec![],
        data: vec![1; 1232],
    };
    assert_eq!(
        prepare(WALLET, [9; 32], &[huge], 0)
            .unwrap_err()
            .to_string(),
        "native_floor_payload_too_large"
    );
    Ok(())
}
