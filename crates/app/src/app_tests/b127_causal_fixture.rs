//! Synthetic JUP6 instruction fixture: no decoded route or live Jupiter simulation proof.
use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_solana_tx::{SolanaAccountMeta as Meta, SolanaInstruction as Ix};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
pub(super) const B: u64 = 102_324_740;
pub(super) const F: u64 = 87_324_740;
pub(super) const JUP: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub(super) async fn fixture(route: Route) -> Result<Fixture> {
    let mut f = Fixture::new(route, 10_000, 1_400_000).await?;
    f.config = super::b126_config_fixture::activated(&f.config)?;
    f.config.pretrade_min_sol_reserve = 0.05;
    f.funding.lock().unwrap().balance = B;
    // Direct wraps 10m and may create two ATAs; fixed allowance covers both here.
    f.funding.lock().unwrap().rent = 2_039_280;
    let wallet =
        crate::execution_pumpswap_accounts::parse_pubkey(&f.request.wallet_pubkey, "test-wallet")?;
    let bundle = bundle(wallet);
    f.wire.lock().unwrap().bundle = Some(bundle);
    f.wire.lock().unwrap().transaction = Some(STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet,
            [9; 32],
            &instructions(wallet),
        )?,
    ));
    f.sync_config();
    Ok(f)
}
pub(super) fn instructions(wallet: [u8; 32]) -> Vec<Ix> {
    let mut i = super::priority_fee_fixture::budget(1_400_000, 10_000);
    i.push(Ix {
        program_id: [0; 32],
        accounts: vec![Meta::signer_writable(wallet), Meta::writable([31; 32])],
        data: [
            2_u32.to_le_bytes().to_vec(),
            10_000_000_u64.to_le_bytes().to_vec(),
        ]
        .concat(),
    });
    i.push(Ix {
        program_id: crate::execution_pumpswap_accounts::parse_pubkey(JUP, "test-program").unwrap(),
        accounts: vec![Meta::signer_writable(wallet)],
        data: vec![99],
    });
    i
}
pub(super) fn bundle(wallet: [u8; 32]) -> Value {
    let mut b = super::generic_sell_synthetic_fixture::bundle(wallet, 1_400_000, 10_000);
    let i = instructions(wallet);
    let wire = |i: &Ix| {
        json!({"programId":bs58::encode(i.program_id).into_string(), "accounts":i.accounts.iter().map(|a|
        json!({"pubkey":bs58::encode(a.pubkey).into_string(),"isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(), "data":STANDARD.encode(&i.data)})
    };
    b["setupInstructions"] = json!([wire(&i[i.len() - 2])]);
    b["swapInstruction"] = wire(i.last().unwrap());
    b
}
pub(super) fn trace(f: &Fixture, signed: &str, floor: u64) -> Result<()> {
    let calls = f.calls.lock().unwrap();
    let simulated = calls
        .iter()
        .rev()
        .find(|(_, v)| v["method"] == "simulateTransaction")
        .unwrap()
        .1["params"][0]
        .as_str()
        .unwrap();
    let sent = calls
        .iter()
        .rev()
        .find(|(_, v)| v["method"] == "sendTransaction")
        .unwrap()
        .1["params"][0]
        .as_str()
        .unwrap();
    let wallet =
        crate::execution_pumpswap_accounts::parse_pubkey(&f.request.wallet_pubkey, "test-wallet")?;
    let before =
        crate::execution_native_floor::verify_final_native_floor(simulated, wallet, floor)?;
    let after = crate::execution_native_floor::verify_final_native_floor(signed, wallet, floor)?;
    let send = crate::execution_native_floor::verify_final_native_floor(sent, wallet, floor)?;
    assert_eq!(
        before.binding().message_bytes,
        after.binding().message_bytes
    );
    assert_eq!(after.binding().message_bytes, send.binding().message_bytes);
    assert_eq!(signed, sent);
    let fees: Vec<_> = calls
        .iter()
        .filter(|(_, v)| v["method"] == "getFeeForMessage")
        .map(|(_, v)| v["params"][0].as_str().unwrap().to_owned())
        .collect();
    assert!(fees.len() >= 2);
    assert!(fees
        .iter()
        .all(|v| v == &STANDARD.encode(&send.binding().message_bytes)));
    println!(
        "B127_WIRE {}",
        json!({"floor":floor,"simulated":simulated,"signed":signed,"sent":sent,
        "message_sha256":send.binding().message_sha256,"fee_messages":fees})
    );
    Ok(())
}
