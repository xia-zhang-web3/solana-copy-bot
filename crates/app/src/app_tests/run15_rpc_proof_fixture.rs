//! Saved source controls and a separately labelled model follower receipt.
use anyhow::Result;
use copybot_core_types::association_delivery::{
    AdmissionFacts, CheckedFacts, InfoIdentity, MessageTime,
};
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::{association_sell_preparation::ReceiptAnchor, *};
use serde_json::{json, Value};
pub(crate) const MINT: &str = "DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX";
pub(crate) const LEADER: &str = "7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF";
pub(crate) const SOL: &str = "So11111111111111111111111111111111111111112";

pub(crate) fn source_sell() -> Value {
    let mut v: Value = serde_json::from_str(include_str!(
        "../../../storage-core/tests/fixtures/run15-source-sell-amm-v4.json"
    ))
    .unwrap();
    v["slot"] = json!(451313058);
    v
}
pub(crate) fn source_buy() -> Value {
    serde_json::from_str(include_str!("run15_fixtures/source-buy.json")).unwrap()
}
pub(crate) fn sell_admission(v: &Value) -> AdmissionFacts {
    AdmissionFacts {
        facts: CheckedFacts {
            signature: v["transaction"]["signatures"][0].as_str().unwrap().into(),
            slot: 451313058,
            wallet: LEADER.into(),
            token_in: MINT.into(),
            token_out: SOL.into(),
            amount_in_bits: 0.264731434_f64.to_bits(),
            amount_out_bits: 0.009832310_f64.to_bits(),
            exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
                amount_in_raw: "264731434".into(),
                amount_in_decimals: 9,
                amount_out_raw: "9832310".into(),
                amount_out_decimals: 9,
            }),
            programs: vec![crate::execution_owner_buy_wire::RAYDIUM.into()],
            dex: "Raydium".into(),
            program_fallback: false,
        },
        info: InfoIdentity {
            encoded: vec![],
            float_bits: vec![],
        },
        message_time: MessageTime::Missing,
    }
}
pub(crate) fn anchor(v: &Value, raw: u64) -> (ReceiptAnchor, ExecutionCanaryReceiptFacts) {
    let signature = v["transaction"]["signatures"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let wallet = v["transaction"]["message"]["accountKeys"][0]
        .as_str()
        .or_else(|| v["transaction"]["message"]["accountKeys"][0]["pubkey"].as_str())
        .unwrap()
        .to_owned();
    let slot = v["slot"].as_u64().unwrap();
    let pre = v["meta"]["preBalances"][0].as_u64().unwrap();
    let post = v["meta"]["postBalances"][0].as_u64().unwrap();
    (
        ReceiptAnchor {
            contributor: ProvenBuyContributor {
                fill_id: 1,
                order_id: "model-or-control".into(),
                signal_id: "model-or-control".into(),
                source_wallet: LEADER.into(),
                tx_signature: signature.clone(),
            },
            slot,
            wallet: wallet.clone(),
            token: MINT.into(),
            raw: raw.to_string(),
            decimals: 9,
            receipt_fingerprint: "test-control".into(),
        },
        ExecutionCanaryReceiptFacts {
            order_id: "model-or-control".into(),
            tx_signature: signature,
            wallet_pubkey: wallet.clone(),
            token: MINT.into(),
            side: "buy".into(),
            slot,
            wallet_native_pre: Lamports::new(pre),
            wallet_native_post: Lamports::new(post),
            wallet_native_delta: SignedLamports::new(i128::from(post) - i128::from(pre)),
            transaction_fee: Some(Lamports::new(v["meta"]["fee"].as_u64().unwrap())),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(wallet.clone()),
            token_delta: Some(ReceiptTokenDelta {
                raw: i128::from(raw),
                decimals: 9,
            }),
            token_coverage: if v["meta"]["preTokenBalances"].as_array().unwrap().iter()
                .any(|r| r["owner"] == wallet && r["mint"] == MINT) {
                ReceiptTokenCoverage::PairedBalances
            } else { ReceiptTokenCoverage::ProvenLifecycle },
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: None,
            decomposition: ReceiptDecomposition::Unresolved,
        },
    )
}

pub(crate) fn model_follower() -> Result<(String, Value)> {
    model_follower_with_output(100_000_000, 160_200_031)
}
pub(crate) fn model_follower_with_output(output: u64, floor: u64) -> Result<(String, Value)> {
    model_follower_with_output_and_target(output, floor, false)
}
pub(crate) fn model_follower_with_output_and_target(
    output: u64,
    floor: u64,
    fresh: bool,
) -> Result<(String, Value)> {
    let (payload, signature, _) =
        super::run15_buy_wire_fixture::signed(MINT, 10_000_000, output, floor)?;
    let wire = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    let keys = wire
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect::<Vec<_>>();
    let outer = wire
        .instructions
        .iter()
        .map(|i| {
            json!({"programIdIndex":i.program_index,
        "accounts":i.account_indices,"data":bs58::encode(&i.data).into_string(),"stackHeight":1})
        })
        .collect::<Vec<_>>();
    let route = wire
        .instructions
        .iter()
        .find(|i| {
            bs58::encode(i.program.pubkey).into_string() == crate::execution_owner_buy_wire::JUPITER
        })
        .unwrap();
    let remaining = &route.account_indices[9..];
    let source = remaining[15];
    let destination = remaining[16];
    let coin = remaining[5];
    let pc = remaining[6];
    let authority = remaining[3];
    let token = remaining[1];
    let amm = remaining[0];
    let token_row = |index: u8, mint: &str, owner: &str, raw: u64| {
        json!({"accountIndex":index,
        "mint":mint,"owner":owner,"programId":keys[token as usize],
        "uiTokenAmount":{"amount":raw.to_string(),"decimals":9,
            "uiAmount":raw as f64 / 1e9,"uiAmountString":(raw as f64 / 1e9).to_string()}})
    };
    let wallet = &keys[0];
    let auth = &keys[authority as usize];
    let transfer = |from: u8, to: u8, owner: u8, amount: u64| {
        json!({"programIdIndex":token,
        "accounts":[from,to,owner],"data":bs58::encode([vec![3],amount.to_le_bytes().to_vec()].concat()).into_string(),"stackHeight":3})
    };
    let mut amm_data = vec![9];
    amm_data.extend(10_000_000_u64.to_le_bytes());
    amm_data.extend((output * 9950 / 10_000).to_le_bytes());
    let init_index = wire
        .instructions
        .iter()
        .find(|i| {
            i.accounts
                .get(1)
                .is_some_and(|a| a.pubkey == wire.binding.accounts[source as usize].pubkey)
                && i.program.pubkey
                    == crate::execution_pumpswap_accounts::associated_token_program_id()
        })
        .unwrap()
        .index;
    let mut pre = vec![0_u64; keys.len()];
    let mut post = pre.clone();
    pre[0] = 1_000_000_000;
    post[0] = 989_981_000 - if fresh { 2_039_280 } else { 0 };
    pre[pc as usize] = 1_002_039_280;
    post[pc as usize] = 1_012_039_280;
    pre[coin as usize] = 2_039_280;
    post[coin as usize] = 2_039_280;
    pre[destination as usize] = if fresh { 0 } else { 2_039_280 };
    post[destination as usize] = 2_039_280;
    let system = keys
        .iter()
        .position(|k| k == "11111111111111111111111111111111")
        .unwrap();
    let sol_mint = keys.iter().position(|k| k == SOL).unwrap();
    let mut create = 0_u32.to_le_bytes().to_vec();
    create.extend(2_039_280_u64.to_le_bytes());
    create.extend(165_u64.to_le_bytes());
    create.extend(bs58::decode(&keys[token as usize]).into_vec()?);
    let setup = |program: usize, accounts: Vec<usize>, data: Vec<u8>| {
        json!({
        "programIdIndex":program,"accounts":accounts,"data":bs58::encode(data).into_string(),"stackHeight":2})
    };
    let creation_group = |account: u8, mint: usize, index: usize| {
        json!({"index":index,"instructions":[
            setup(token as usize,vec![mint],vec![21,7,0]),
            setup(system,vec![0,account as usize],create.clone()),
            setup(token as usize,vec![account as usize],vec![22]),
            {"programIdIndex":token,"stackHeight":2,
            "parsed":{"type":"initializeAccount3","info":{"account":keys[account as usize],"owner":wallet,"mint":keys[mint]}}}]})
    };
    let mut event = vec![
        228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226,
    ];
    event.extend(bs58::decode(&keys[remaining[2] as usize]).into_vec()?);
    event.extend(bs58::decode(SOL).into_vec()?);
    event.extend(10_000_000_u64.to_le_bytes());
    event.extend(bs58::decode(MINT).into_vec()?);
    event.extend(output.to_le_bytes());
    let mut groups = vec![creation_group(source, sol_mint, init_index)];
    if fresh {
        let target_init =
            wire.instructions
                .iter()
                .find(|i| {
                    i.accounts.get(1).is_some_and(|a| {
                        a.pubkey == wire.binding.accounts[destination as usize].pubkey
                    }) && i.program.pubkey
                        == crate::execution_pumpswap_accounts::associated_token_program_id()
                })
                .unwrap()
                .index;
        groups.push(creation_group(
            destination,
            route.account_indices[5] as usize,
            target_init,
        ));
    }
    groups.push(
        json!({"index":route.index,"instructions":[{"programIdIndex":amm,"accounts":remaining[1..],
            "data":bs58::encode(amm_data).into_string(),"stackHeight":2},
            transfer(source,pc,0,10_000_000),transfer(coin,destination,authority,output),
            {"programIdIndex":route.program_index,"accounts":[route.account_indices[7]],
                "data":bs58::encode(event).into_string(),"stackHeight":2}]}),
    );
    let mut before = vec![
        token_row(coin, MINT, auth, 500_000_000),
        token_row(pc, SOL, auth, 1_000_000_000),
    ];
    if !fresh {
        before.insert(0, token_row(destination, MINT, wallet, 0));
    }
    let tx = json!({"slot":451313057,"version":"legacy","transaction":{"signatures":[signature],
        "message":{"header":{"numRequiredSignatures":1,"numReadonlySignedAccounts":0,
            "numReadonlyUnsignedAccounts":wire.binding.readonly_unsigned},"accountKeys":keys,"instructions":outer}},
        "meta":{"err":null,"fee":19000,"preBalances":pre,"postBalances":post,
        "preTokenBalances":before,
        "postTokenBalances":[token_row(destination,MINT,wallet,output),token_row(coin,MINT,auth,500_000_000-output),token_row(pc,SOL,auth,1_010_000_000)],
        "innerInstructions":groups}});
    Ok((payload, tx))
}
