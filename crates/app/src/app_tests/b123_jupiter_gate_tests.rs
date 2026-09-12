use super::b123_jupiter_fixture_tests::*;
use super::token2022_ata_boundary_tests::Boundary;
use super::token2022_ata_collector_tests::facts;
use super::token2022_ata_inputs_tests::{key, output, Spec, RESERVE};
use crate::execution_solana_tx::{SolanaAccountMeta as Meta, SolanaInstruction};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;

#[tokio::test]
async fn b123_jupiter_unknown_data_and_role_shapes_refuse_at_before_send() -> Result<()> {
    for case in [
        "empty-data",
        "unknown-data",
        "missing-roles",
        "no-uva",
        "aliased",
        "reordered",
    ] {
        let spec = Spec::sufficient("absent")?;
        let mut ix = instructions(&spec)?;
        let jup = ix
            .iter_mut()
            .find(|i| i.program_id == key(JUPITER))
            .unwrap();
        match case {
            "empty-data" => jup.data.clear(),
            "unknown-data" => jup.data = vec![0xff, 7, 0, 42, 1],
            "missing-roles" => jup.accounts.clear(),
            "no-uva" => jup.accounts.retain(|a| a.pubkey != key(UVA)),
            "aliased" => jup.accounts.fill(Meta::signer_writable(spec.wallet)),
            "reordered" => jup.accounts.reverse(),
            _ => unreachable!(),
        }
        let changed = serialize(&spec, spec.wallet, &ix)?;
        assert_ne!(changed.payload, spec.payload);
        if ["missing-roles", "no-uva", "aliased"].contains(&case) {
            assert!(!changed.keys.as_array().unwrap().contains(&json!(UVA)));
        }
        refused(&changed, case).await?;
    }
    Ok(())
}

#[tokio::test]
async fn b123_jupiter_uva_observations_never_upgrade_unknown_funding() -> Result<()> {
    for state in ["absent", "existing137", "invalid137", "prefunded"] {
        let mut spec = Spec::sufficient("absent")?;
        let mut data137 = vec![0; 137];
        data137[..8].copy_from_slice(&[0x56, 0xff, 0x70, 0x0e, 0x66, 0x35, 0x9a, 0xfa]);
        data137[8..40].copy_from_slice(&spec.wallet);
        spec.set(UVA, match state {
            "absent" => serde_json::Value::Null,
            // Synthetic supplied account data; no claim that this is a valid
            // program account, rent contract, or CPI creation profile.
            "existing137" => json!({"lamports":1_844_400,"owner":"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
                "executable":false,"data":[STANDARD.encode(data137),"base64"]}),
            "invalid137" => json!({"lamports":1_844_400,"owner":JUPITER,
                "executable":false,"data":[STANDARD.encode([0u8;137]),"base64"]}),
            "prefunded" => system(100_000_000),
            _ => unreachable!(),
        });
        spec.rows[0] = system(u64::MAX);
        refused(&spec, state).await?;
    }
    Ok(())
}

#[tokio::test]
async fn b123_jupiter_other_payer_and_amount_have_fresh_exact_binding() -> Result<()> {
    let original = non_jupiter()?;
    let wallet = [91; 32];
    let amount = 21_000_000;
    let mut ix = super::priority_fee_fixture::budget(200_000, 10_000);
    ix.push(super::native_funding_fixture::transfer(
        wallet, [92; 32], amount,
    ));
    ix.push(SolanaInstruction {
        program_id: key(JUPITER),
        accounts: vec![Meta::signer_writable(wallet), Meta::writable(key(UVA))],
        data: vec![231, 123, 0, 1],
    });
    ix.push(super::native_funding_fixture::transfer(
        wallet, wallet, RESERVE,
    ));
    let spec = serialize(&original, wallet, &ix)?;
    assert_eq!(spec.wallet, wallet);
    let dir = output("b123-other-payer-amount-facts");
    let (native, count) = facts(&spec, &dir).await?;
    assert_eq!(count, 3);
    assert_eq!(native.native().requirements().expected_wallet, wallet);
    assert_eq!(
        native
            .native()
            .requirements()
            .nominal_wallet_source_transfer_operands_lamports,
        u128::from(amount + RESERVE)
    );
    assert_eq!(
        crate::execution_initial_sol::check(&spec.payload, wallet, RESERVE, &native)
            .unwrap_err()
            .to_string(),
        UNPROVEN
    );
    refused(&spec, "other-payer-amount").await?;
    Ok(())
}

#[tokio::test]
async fn b123_non_jupiter_and_jupiter_key_only_keep_selected_positive() -> Result<()> {
    let original = Spec::synthetic(false)?;
    let (native, count) = facts(&original, &output("b123-original-nonjup-positive")).await?;
    assert_eq!(count, 3);
    assert_eq!(
        crate::execution_initial_sol::check(&original.payload, original.wallet, RESERVE, &native)?
            .outgoing_transfers,
        10_000_000
    );
    for case in ["existing-positive", "operand", "unused-key"] {
        let spec = non_jupiter()?;
        let changed = if case == "operand" {
            let mut ix = instructions(&spec)?;
            // A genuine System transfer to this key is not a Jupiter invocation.
            ix.insert(
                ix.len() - 1,
                super::native_funding_fixture::transfer(spec.wallet, key(JUPITER), 1),
            );
            serialize(&spec, spec.wallet, &ix)?
        } else if case == "unused-key" {
            let mut wire = STANDARD.decode(&spec.payload)?;
            assert_eq!(wire[65], 1); // Legacy one-signer fixture, one-byte key count.
            let count = usize::from(wire[68]);
            assert!(count < 127);
            wire[67] += 1; // One extra unsigned readonly key, no instruction operand.
            wire[68] += 1;
            wire.splice(69 + 32 * count..69 + 32 * count, key(JUPITER));
            rebind(&spec, STANDARD.encode(wire))?
        } else {
            spec
        };
        let decoded =
            crate::execution_transaction_wire::decode_message(&changed.payload, |_| Ok(()))?;
        assert!(decoded
            .instructions
            .iter()
            .all(|i| i.program.pubkey != key(JUPITER)));
        if case != "existing-positive" {
            assert!(decoded
                .binding
                .accounts
                .iter()
                .any(|a| a.pubkey == key(JUPITER)));
        }
        let dir = output(&format!("b123-nonjup-{case}"));
        let (native, count) = facts(&changed, &dir.join("facts")).await?;
        assert_eq!(count, 3);
        let selected = crate::execution_initial_sol::check(
            &changed.payload,
            changed.wallet,
            RESERVE,
            &native,
        )?;
        assert_eq!(
            selected.outgoing_transfers,
            if case == "operand" {
                10_000_001
            } else {
                10_000_000
            }
        );
        let boundary = Boundary::new(&changed, "buy", &dir)?;
        let before = boundary.sql()?;
        assert!(invoke(&boundary, &changed, &dir, None).await?.is_none());
        assert_eq!(boundary.sql()?, before);
    }
    Ok(())
}

#[tokio::test]
async fn b123_binding_and_malformed_errors_precede_jupiter_refusal() -> Result<()> {
    let spec = Spec::sufficient("absent")?;
    let (native, _) = facts(&spec, &output("b123-binding-native")).await?;
    let mut signature_changed = STANDARD.decode(&spec.payload)?;
    signature_changed[1] = 1;
    let mut ix = instructions(&spec)?;
    ix.iter_mut()
        .find(|i| i.program_id == key(JUPITER))
        .unwrap()
        .data
        .push(0);
    let other_message = serialize(&spec, spec.wallet, &ix)?;
    for (payload, wallet) in [
        (STANDARD.encode(signature_changed), spec.wallet),
        (other_message.payload, spec.wallet),
        (spec.payload.clone(), [91; 32]),
        ("not base64!".into(), spec.wallet),
    ] {
        assert_eq!(
            crate::execution_initial_sol::check(&payload, wallet, RESERVE, &native)
                .unwrap_err()
                .to_string(),
            "initial_sol_observations_binding"
        );
    }
    for case in ["malformed", "wallet-mismatch"] {
        let dir = output(&format!("b123-before-send-{case}"));
        let mut boundary = Boundary::new(&spec, "buy", &dir)?;
        if case == "malformed" {
            boundary.intent.signed_transaction_base64 = "not base64!".into();
        } else {
            boundary.gate.execution_wallet_pubkey = bs58::encode([91; 32]).into_string();
        }
        let out = invoke(&boundary, &spec, &dir, None).await?.unwrap();
        assert_eq!(out.failed, 1);
        assert!(out
            .error
            .as_deref()
            .unwrap()
            .starts_with("initial_sol_observations_unavailable:"));
    }
    Ok(())
}
