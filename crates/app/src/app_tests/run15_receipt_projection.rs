//! Strict test-only projection of modeled compiled bytes into RPC jsonParsed.
//! Custody, amounts, metadata and source samples stay unchanged.
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};

const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SYSTEM: &str = "11111111111111111111111111111111";
const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

fn instruction(i: &Value, keys: &[String]) -> Result<Value> {
    let program = keys
        .get(
            i["programIdIndex"]
                .as_u64()
                .context("model program index")? as usize,
        )
        .context("model program outside keys")?;
    let mut out = json!({"programId":program,"stackHeight":i["stackHeight"]});
    if i.get("parsed").is_some() {
        ensure!(
            program == TOKEN && i["parsed"]["type"] == "initializeAccount3",
            "unexpected model pre-parsed instruction"
        );
        out["parsed"] = i["parsed"].clone();
        return Ok(out);
    }
    let a = i["accounts"]
        .as_array()
        .context("model instruction accounts")?
        .iter()
        .map(|a| {
            keys.get(a.as_u64().context("model account index")? as usize)
                .context("model account outside keys")
        })
        .collect::<Result<Vec<_>>>()?;
    let data = bs58::decode(i["data"].as_str().context("model instruction data")?).into_vec()?;
    let n = |offset: usize| -> Result<u64> {
        Ok(u64::from_le_bytes(
            data.get(offset..offset + 8)
                .context("truncated model operand")?
                .try_into()?,
        ))
    };
    let parsed = if program == TOKEN {
        match data.as_slice() {
            [21, 7, 0] if a.len() == 1 => json!({"type":"getAccountDataSize","info":{
                "mint":a[0],"extensionTypes":["immutableOwner"]}}),
            [22] if a.len() == 1 => {
                json!({"type":"initializeImmutableOwner","info":{"account":a[0]}})
            }
            [18, ..] if data.len() == 33 && a.len() == 2 => {
                json!({"type":"initializeAccount3","info":{
                "account":a[0],"mint":a[1],"owner":bs58::encode(&data[1..]).into_string()}})
            }
            [3, ..] if data.len() == 9 && a.len() == 3 => json!({"type":"transfer","info":{
                "source":a[0],"destination":a[1],"authority":a[2],"amount":n(1)?.to_string()}}),
            [17] if a.len() == 1 => json!({"type":"syncNative","info":{"account":a[0]}}),
            [9] if a.len() == 3 => json!({"type":"closeAccount","info":{
                "account":a[0],"destination":a[1],"owner":a[2]}}),
            _ => bail!("unsupported modeled SPL instruction"),
        }
    } else if program == SYSTEM {
        match data.get(..4) {
            Some([0, 0, 0, 0]) if data.len() == 52 && a.len() == 2 => {
                json!({"type":"createAccount","info":{
                "source":a[0],"newAccount":a[1],"lamports":n(4)?,"space":n(12)?,
                "owner":bs58::encode(&data[20..52]).into_string()}})
            }
            Some([2, 0, 0, 0]) if data.len() == 12 && a.len() == 2 => {
                json!({"type":"transfer","info":{
                "source":a[0],"destination":a[1],"lamports":n(4)?}})
            }
            _ => bail!("unsupported modeled System instruction"),
        }
    } else if program == ATA {
        ensure!(
            data == [1] && a.len() == 6,
            "unsupported modeled ATA instruction"
        );
        json!({"type":"createIdempotent","info":{"source":a[0],"account":a[1],
            "wallet":a[2],"mint":a[3],"systemProgram":a[4],"tokenProgram":a[5]}})
    } else {
        // Unknown program parsers remain partially decoded, as on real RPC.
        out["accounts"] = json!(a);
        out["data"] = i["data"].clone();
        return Ok(out);
    };
    out["parsed"] = parsed;
    Ok(out)
}

pub(crate) fn parsed(model: &Value, payload: &str) -> Result<Value> {
    let wire = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let keys = wire
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect::<Vec<_>>();
    ensure!(
        model["transaction"]["message"]["accountKeys"] == json!(keys),
        "model key binding"
    );
    let mut receipt = model.clone();
    receipt["transaction"]["message"]["accountKeys"] = json!(wire.binding.accounts.iter().map(|a| json!({
        "pubkey":bs58::encode(a.pubkey).into_string(),"signer":a.is_signer,"writable":a.is_writable})).collect::<Vec<_>>());
    for i in receipt["transaction"]["message"]["instructions"]
        .as_array_mut()
        .context("model outer instructions")?
    {
        *i = instruction(i, &keys)?;
    }
    for group in receipt["meta"]["innerInstructions"]
        .as_array_mut()
        .context("model inner instructions")?
    {
        for i in group["instructions"]
            .as_array_mut()
            .context("model CPI group")?
        {
            *i = instruction(i, &keys)?;
        }
    }
    Ok(receipt)
}
