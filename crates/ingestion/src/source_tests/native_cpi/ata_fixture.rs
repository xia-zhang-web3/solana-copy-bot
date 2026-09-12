//! Small synthetic execution-builder body; cached ATA 4.0.0 / classic Token 6.0.0.
use super::{harness::fixture, parsed};
use curve25519_dalek::edwards::CompressedEdwardsY;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(super) const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub(super) const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const SYSTEM: &str = "11111111111111111111111111111111";
pub(super) const SOL: &str = "So11111111111111111111111111111111111111112";

// Fixture-only reference derived from cached Pubkey::find_program_address and
// the existing builder seeds. Production helper is never used to build inputs.
pub(super) fn address(owner: &str, mint: &str, skip: usize) -> (String, u8) {
    let bytes = |s: &str| bs58::decode(s).into_vec().unwrap();
    let mut valid = Vec::new();
    for bump in (1..=255u8).rev() {
        let mut h = Sha256::new();
        for s in [
            bytes(owner),
            bytes(TOKEN),
            bytes(mint),
            vec![bump],
            bytes(ATA),
            b"ProgramDerivedAddress".to_vec(),
        ] {
            h.update(s);
        }
        let raw: [u8; 32] = h.finalize().into();
        if CompressedEdwardsY(raw).decompress().is_none() {
            valid.push((bs58::encode(raw).into_string(), bump));
            if valid.len() > skip {
                return valid[skip].clone();
            }
        }
    }
    panic!("fixture PDA unavailable");
}

pub(super) fn replace(v: &mut Value, from: &str, to: &str) {
    match v {
        Value::String(s) if s == from => *s = to.into(),
        Value::Array(a) => a.iter_mut().for_each(|v| replace(v, from, to)),
        Value::Object(m) => m.values_mut().for_each(|v| replace(v, from, to)),
        _ => (),
    }
}

pub(super) fn ix(program: &str, accounts: Vec<Value>, data: Vec<u8>, inner: bool) -> Value {
    let mut v =
        json!({"programId":program,"accounts":accounts,"data":bs58::encode(data).into_string()});
    if inner {
        v["stackHeight"] = json!(2);
    }
    v
}

pub(super) fn built(name: &str, buy: bool) -> Value {
    let mut f = fixture(name, if buy { "buy" } else { "sell" });
    let owner = f["roles"]["user"].as_str().unwrap().to_owned();
    let (base, bump) = address(&owner, SOL, 0);
    let (quote, quote_bump) = address(&owner, f["roles"]["quote_mint"].as_str().unwrap(), 0);
    for (role, key) in [("user_base", &base), ("user_quote", &quote)] {
        let old = f["roles"][role].as_str().unwrap().to_owned();
        replace(&mut f, &old, key);
    }
    f["audit"]["ata_bump"] = json!(bump);
    f["audit"]["target_ata_bump"] = json!(quote_bump);
    f["audit"]["fixture_contract"] =
        json!("ATA4.0.0 zero-lamport classic creation; builder body; target already exists");
    let r = f["roles"].clone();
    let ata = |account: Value, mint: Value| {
        ix(
            ATA,
            vec![
                r["user"].clone(),
                account,
                r["user"].clone(),
                mint,
                json!(SYSTEM),
                json!(TOKEN),
            ],
            vec![1],
            false,
        )
    };
    let top = f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap();
    top[0] = ata(r["user_base"].clone(), json!(SOL));
    top.remove(1); // InitializeAccount3 is now a CPI of ATA CreateIdempotent.
    let parent = if buy { 4 } else { 1 };
    if buy {
        top.insert(3, ata(r["user_quote"].clone(), r["quote_mint"].clone()));
    }
    f["result"]["meta"]["innerInstructions"][0]["index"] = json!(parent);
    let mut create = 0u32.to_le_bytes().to_vec();
    create.extend(2_039_280u64.to_le_bytes());
    create.extend(165u64.to_le_bytes());
    create.extend(bs58::decode(TOKEN).into_vec().unwrap());
    let mut init = vec![18];
    init.extend(bs58::decode(&owner).into_vec().unwrap());
    let group = json!({"index":0,"instructions":[
        ix(TOKEN,vec![json!(SOL)],vec![21,7,0],true),
        ix(SYSTEM,vec![r["user"].clone(),r["user_base"].clone()],create,true),
        ix(TOKEN,vec![r["user_base"].clone()],vec![22],true),
        ix(TOKEN,vec![r["user_base"].clone(),json!(SOL)],init,true)]});
    f["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
        .insert(0, group);
    f
}

pub(super) fn as_parsed(f: &Value) -> Value {
    let mut f = parsed::converted(f);
    for group in f["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
    {
        for i in group["instructions"].as_array_mut().unwrap() {
            if !i["data"].is_string() {
                continue;
            }
            let d = bs58::decode(i["data"].as_str().unwrap())
                .into_vec()
                .unwrap();
            let a = &i["accounts"];
            let (kind, info) = match (i["programId"].as_str().unwrap(), d[0]) {
                (SYSTEM, 0) => (
                    "createAccount",
                    json!({"source":a[0],"newAccount":a[1],"lamports":u64::from_le_bytes(d[4..12].try_into().unwrap()),"space":u64::from_le_bytes(d[12..20].try_into().unwrap()),"owner":bs58::encode(&d[20..52]).into_string()}),
                ),
                (TOKEN, 21) => (
                    "getAccountDataSize",
                    json!({"mint":a[0],"extensionTypes":["immutableOwner"]}),
                ),
                (TOKEN, 22) => ("initializeImmutableOwner", json!({"account":a[0]})),
                _ => continue,
            };
            *i = json!({"programId":i["programId"],"parsed":{"type":kind,"info":info},"stackHeight":2});
        }
    }
    f
}
