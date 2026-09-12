use super::native_funding_fixture::*;
use crate::execution_native_funding::decode_native_funding_requirements as decode;
use crate::execution_priority_fee_wire::decode_priority_fee;
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{SolanaAccountMeta as M, SolanaInstruction};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

fn malformed(i: SolanaInstruction, expected: &str) -> Result<()> {
    let mut source = budget();
    source.push(i);
    let p = payload(&source)?;
    assert_eq!(
        decode_priority_fee(&p)?.total,
        120_000,
        "native classification must not add a priority business gate"
    );
    let error = format!("{:#}", decode(&p, WALLET).unwrap_err());
    assert!(
        error.contains("native_funding_instruction_index="),
        "{error}"
    );
    assert!(error.contains(expected), "{error}");
    Ok(())
}

#[test]
fn native_funding_transfer_roles_and_exact_lengths_reject_without_verified_amounts() -> Result<()> {
    for len in [0, 1, 3, 4, 11, 13] {
        let mut t = transfer(WALLET, PEER, 7);
        t.data.resize(len, 0);
        malformed(
            t,
            if len < 4 {
                "malformed_system_discriminator"
            } else {
                "malformed_system_transfer"
            },
        )?;
    }
    for count in [0, 1, 3] {
        let mut t = transfer(WALLET, PEER, 7);
        t.accounts.resize(count, M::readonly([53; 32]));
        malformed(t, "malformed_system_transfer")?;
    }
    let mut t = transfer(PEER, WALLET, 7);
    t.accounts[0].is_signer = false;
    malformed(t, "invalid_transfer_roles")?;
    let mut t = transfer(WALLET, PEER, 7);
    t.accounts[1].is_writable = false;
    malformed(t, "invalid_transfer_roles")?;
    let mut bytes = STANDARD.decode(foreign_signer_transfer(WALLET, 7)?)?;
    bytes[130] = 1;
    let p = STANDARD.encode(bytes);
    assert!(decode_priority_fee(&p).is_ok());
    assert!(format!("{:#}", decode(&p, WALLET).unwrap_err()).contains("invalid_transfer_roles"));
    // A program promoted to writable is not certified by native operand classification.
    malformed(
        transfer(WALLET, compute_budget_program_id(), 7),
        "invalid_program_roles",
    )?;
    Ok(())
}

#[test]
fn native_funding_ata_sync_and_close_require_canonical_data_and_minimum_roles() -> Result<()> {
    let direct = direct(true, false, 7)?;
    let samples = [
        (
            direct
                .iter()
                .find(|i| i.program_id == associated_token_program_id())
                .unwrap()
                .clone(),
            "ata",
        ),
        (
            direct
                .iter()
                .find(|i| i.program_id == token_program_id() && i.data == [17])
                .unwrap()
                .clone(),
            "sync_native",
        ),
        (
            direct
                .iter()
                .find(|i| i.program_id == token_program_id() && i.data == [9])
                .unwrap()
                .clone(),
            "close",
        ),
    ];
    for (original, kind) in samples {
        let mut bad = original.clone();
        bad.data.push(0);
        malformed(bad, "malformed_")?;
        let mut bad = original.clone();
        bad.accounts.pop();
        malformed(bad, "malformed_")?;
        let mut bad = original.clone();
        bad.accounts.push(M::readonly([53; 32]));
        malformed(bad, "malformed_")?;
        let mut bad = original.clone();
        if kind == "ata" {
            bad.accounts[0] = M::writable(PEER);
        } else {
            bad.accounts[0].is_writable = false;
        }
        malformed(bad, "invalid_")?;
        if kind == "ata" {
            let mut bad = original.clone();
            bad.accounts[4].pubkey = PEER;
            malformed(bad, "invalid_ata_roles")?;
            let mut bad = original.clone();
            bad.accounts[1].is_writable = false;
            malformed(bad, "invalid_ata_roles")?;
        }
        if kind == "close" {
            let mut bad = original.clone();
            bad.accounts[1] = M::readonly([53; 32]);
            malformed(bad, "invalid_close_roles")?;
            let mut bad = original;
            bad.accounts[2] = M::readonly(PEER);
            malformed(bad, "invalid_close_roles")?;
        }
    }
    Ok(())
}

#[test]
fn native_funding_shared_wire_rejects_every_invalid_old_oracle_input_first() -> Result<()> {
    let mut rejected = 0;
    for (name, p) in super::wire_extraction_oracle_tests::corpus()? {
        if let Err(old) = decode_priority_fee(&p) {
            let new = decode(&p, WALLET).expect_err(&name);
            assert_eq!(format!("{new:#}"), format!("{old:#}"), "{name}");
            rejected += 1;
        }
    }
    assert_eq!(
        rejected, 489,
        "captured accepted-base oracle rejection count"
    );
    eprintln!("B18 native reader retained {rejected} priority/wire rejections");
    Ok(())
}
