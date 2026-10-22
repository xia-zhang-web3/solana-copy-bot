//! Normalize genuine decoded operands into the shared closed USDC SELL witness.
//! No freshness, signature, slot, authority or wallet state is manufactured here.
use super::{ata, Attribution, Instruction, Row, Trade, View, SOL_MINT};
use copybot_storage_core::ordered_sell_quote::fractional::inventory as proof;
use serde_json::{json, Value};
pub(super) const JUPITER: &str = proof::JUPITER;
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub(super) fn attribute(v: &View, signer: &str) -> Attribution {
    prove(v, signer)
        .map(Attribution::Known)
        .unwrap_or(Attribution::Unknown)
}
fn ix(i: &Instruction, keys: &[&str]) -> Option<Value> {
    let program = keys.iter().position(|p| Some(*p) == i.program.as_deref())?;
    let a = i.accounts.as_ref()?;
    if a.iter().any(|n| *n >= keys.len()) {
        return None;
    }
    Some(
        json!({"programIdIndex":program,"accounts":a,"data":bs58::encode(i.data.as_ref()?).into_string(),
        "stackHeight":i.depth}),
    )
}
fn rows(r: &[Row]) -> Value {
    json!(r.iter().map(|r|json!({"accountIndex":r.index,"mint":r.mint,
    "owner":r.owner,"programId":r.program,"uiTokenAmount":{"amount":r.raw.to_string(),"decimals":r.decimals}}))
    .collect::<Vec<_>>())
}
pub(super) fn prove(v: &View, signer: &str) -> Option<Trade> {
    if !v.successful || v.required_signatures != Some(1) || v.first_signer.as_deref()? != signer {
        return None;
    }
    let keys = v
        .keys
        .iter()
        .map(Option::as_deref)
        .collect::<Option<Vec<_>>>()?;
    if keys.len() > 256 {
        return None;
    }
    let top = v.top.as_ref()?;
    let parent = top.iter().find(|i| i.program.as_deref() == Some(JUPITER))?;
    let a = parent.accounts.as_ref()?;
    let d = parent.data.as_ref()?;
    let input = u64::from_le_bytes(d.get(16..24)?.try_into().ok()?);
    // View contains effective keys after lookup expansion; retain the original
    // legacy/v0 version and do not append loaded addresses a second time.
    let version = if v.versioned? {
        json!(0)
    } else {
        json!("legacy")
    };
    let t = json!({"version":version,"transaction":{"message":{"accountKeys":keys,
        "header":{"numRequiredSignatures":v.required_signatures},
        "instructions":top.iter().map(|i|ix(i,&keys)).collect::<Option<Vec<_>>>()?}},
        "meta":{"err":null,"fee":v.fee?,"loadedAddresses":{"writable":[],"readonly":[]},
        "preBalances":v.pre.as_ref()?,"postBalances":v.post.as_ref()?,
        "preTokenBalances":rows(v.pre_tokens.as_ref()?),"postTokenBalances":rows(v.post_tokens.as_ref()?),
        "innerInstructions":v.inner.as_ref()?.iter().map(|(n,c)|Some(json!({"index":n,
            "instructions":c.iter().map(|i|ix(i,&keys)).collect::<Option<Vec<_>>>()?}))).collect::<Option<Vec<_>>>()?}});
    let proved = proof::prove_jupiter_source_sell(&t, signer, USDC, 6, input).ok()?;
    // Reuse the exact independent canonical-ATA/PDA witness, including creation
    // rent and close authority. The financial proof separately checks cash/CPI.
    let mut roles = [0; 13];
    roles[1] = *a.get(1)?;
    roles[3] = keys.iter().position(|k| *k == SOL_MINT)?;
    // SELL creates only WSOL. The target mint need not be a message account;
    // its identity/decimals are proved from paired custody rows above. ATA's
    // target-mint role is used only by its BUY target-creation branch.
    roles[5] = *a.get(3)?;
    roles[6] = *a.get(2)?;
    roles[7] = *a.get(13)?;
    roles[8] = *a.get(14)?;
    if proved.seeded {
        super::lifecycle::prove(v, proved.route_index, &roles, false, proved.sol_raw)?;
    } else if ata::prove(v, proved.route_index, &roles, false, proved.sol_raw)?
        != proved.setup_index
    {
        return None;
    }
    Some(Trade {
        buy: false,
        target: USDC.into(),
        sol_raw: proved.sol_raw,
        target_raw: proved.target_raw,
        target_decimals: 6,
    })
}
