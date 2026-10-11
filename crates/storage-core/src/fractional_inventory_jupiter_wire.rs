//! Bounded raw/jsonParsed normalization for the closed source SELL custody proof.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
pub(super) const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const SYSTEM: &str = "11111111111111111111111111111111";
pub(super) const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub(super) fn decode(s: &str) -> Result<Vec<u8>> {
    const A: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    ensure!(s.len() <= 180, "jupiter_source_data_bound");
    let mut out = [0u8; 128];
    for c in s.bytes() {
        let mut carry = A
            .iter()
            .position(|b| *b == c)
            .context("jupiter_source_base58")? as u32;
        for b in out.iter_mut().rev() {
            carry += u32::from(*b) * 58;
            *b = carry as u8;
            carry >>= 8;
        }
        ensure!(carry == 0, "jupiter_source_data_bound");
    }
    let first = out.iter().position(|b| *b != 0).unwrap_or(128);
    let zeros = s.bytes().take_while(|b| *b == b'1').count();
    ensure!(zeros + 128 - first <= 128, "jupiter_source_data_bound");
    let mut bytes = vec![0; zeros];
    bytes.extend_from_slice(&out[first..]);
    Ok(bytes)
}
pub(super) fn uint(v: &Value) -> Result<u64> {
    let s = v.as_str().context("jupiter_source_raw")?;
    let n: u64 = s.parse()?;
    ensure!(s == n.to_string(), "jupiter_source_raw");
    Ok(n)
}
pub(super) struct Ix {
    pub program: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
    pub depth: Option<u64>,
}
pub(super) fn instruction(v: &Value, keys: &[String]) -> Result<Ix> {
    let program = v["programId"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            v["programIdIndex"]
                .as_u64()
                .and_then(|n| keys.get(n as usize).cloned())
        })
        .context("jupiter_source_program")?;
    ensure!(keys.contains(&program), "jupiter_source_program");
    let (accounts, data) = if v.get("parsed").is_some() {
        parsed(v, &program)?
    } else {
        let accounts = v["accounts"]
            .as_array()
            .context("jupiter_source_accounts")?
            .iter()
            .map(|a| {
                a.as_str()
                    .map(str::to_owned)
                    .or_else(|| a.as_u64().and_then(|n| keys.get(n as usize).cloned()))
                    .context("jupiter_source_accounts")
            })
            .collect::<Result<Vec<_>>>()?;
        (
            accounts,
            decode(v["data"].as_str().context("jupiter_source_data")?)?,
        )
    };
    ensure!(
        accounts.iter().all(|a| keys.contains(a)),
        "jupiter_source_accounts"
    );
    let depth = match v.get("stackHeight") {
        None | Some(Value::Null) => None,
        Some(n) => Some(n.as_u64().context("jupiter_source_depth")?),
    };
    Ok(Ix {
        program,
        accounts,
        data,
        depth,
    })
}
fn parsed(v: &Value, program: &str) -> Result<(Vec<String>, Vec<u8>)> {
    let p = &v["parsed"];
    let i = p["info"].as_object().context("jupiter_source_parsed")?;
    ensure!(
        !["signers", "multisigAuthority", "multisigOwner"]
            .iter()
            .any(|k| i.contains_key(*k)),
        "jupiter_source_multisig"
    );
    let address = |k: &str| -> Result<String> {
        let s = i
            .get(k)
            .and_then(Value::as_str)
            .context("jupiter_source_parsed_key")?;
        ensure!(decode(s)?.len() == 32, "jupiter_source_parsed_key");
        Ok(s.into())
    };
    let number = |k: &str| {
        i.get(k)
            .and_then(Value::as_u64)
            .context("jupiter_source_parsed_number")
    };
    let mut d = vec![];
    let a = match (program, p["type"].as_str()) {
        (ATA, Some("createIdempotent")) => {
            d.push(1);
            [
                "source",
                "account",
                "wallet",
                "mint",
                "systemProgram",
                "tokenProgram",
            ]
            .iter()
            .map(|k| address(k))
            .collect::<Result<Vec<_>>>()?
        }
        (TOKEN, Some("getAccountDataSize")) => {
            ensure!(
                i.get("extensionTypes") == Some(&serde_json::json!(["immutableOwner"])),
                "jupiter_source_extensions"
            );
            d.extend([21, 7, 0]);
            vec![address("mint")?]
        }
        (TOKEN, Some("initializeImmutableOwner")) => {
            d.push(22);
            vec![address("account")?]
        }
        (TOKEN, Some("initializeAccount3")) => {
            d.push(18);
            d.extend(decode(&address("owner")?)?);
            vec![address("account")?, address("mint")?]
        }
        (TOKEN, Some("closeAccount")) => {
            d.push(9);
            vec![
                address("account")?,
                address("destination")?,
                address("owner")?,
            ]
        }
        (TOKEN, Some("transfer")) => {
            d.push(3);
            d.extend(uint(i.get("amount").context("jupiter_source_raw")?)?.to_le_bytes());
            vec![
                address("source")?,
                address("destination")?,
                address("authority")?,
            ]
        }
        (SYSTEM, Some("createAccount")) => {
            d.extend(0u32.to_le_bytes());
            d.extend(number("lamports")?.to_le_bytes());
            d.extend(number("space")?.to_le_bytes());
            d.extend(decode(&address("owner")?)?);
            vec![address("source")?, address("newAccount")?]
        }
        (SYSTEM, Some("transfer")) => {
            d.extend(2u32.to_le_bytes());
            d.extend(number("lamports")?.to_le_bytes());
            vec![address("source")?, address("destination")?]
        }
        _ => anyhow::bail!("jupiter_source_parsed_subset"),
    };
    Ok((a, d))
}
pub(super) fn exact(ix: &Ix, program: &str, accounts: &[&str], data: &[u8], depth: u64) -> bool {
    ix.program == program
        && ix
            .accounts
            .iter()
            .map(String::as_str)
            .eq(accounts.iter().copied())
        && ix.data == data
        && if depth == 1 {
            ix.depth.is_none_or(|n| n == 1)
        } else {
            ix.depth == Some(depth)
        }
}

pub(super) type Row = (String, String, String, u8, u64);
pub(super) fn rows(t: &Value, field: &str, count: usize) -> Result<BTreeMap<usize, Row>> {
    let mut out = BTreeMap::new();
    for r in t["meta"][field]
        .as_array()
        .context("jupiter_source_balances")?
    {
        let n = r["accountIndex"]
            .as_u64()
            .context("jupiter_source_token_index")? as usize;
        let text = |k: &str| {
            r[k].as_str()
                .map(str::to_owned)
                .context("jupiter_source_token_identity")
        };
        let row = (
            text("owner")?,
            text("mint")?,
            text("programId")?,
            u8::try_from(
                r["uiTokenAmount"]["decimals"]
                    .as_u64()
                    .context("jupiter_source_decimals")?,
            )?,
            uint(&r["uiTokenAmount"]["amount"])?,
        );
        ensure!(
            n < count && out.insert(n, row).is_none(),
            "jupiter_source_token_index"
        );
    }
    Ok(out)
}
pub(super) fn pair(
    pre: &BTreeMap<usize, Row>,
    post: &BTreeMap<usize, Row>,
    n: usize,
    owner: &str,
    mint: &str,
    dec: u8,
) -> Result<(u64, u64)> {
    let a = pre.get(&n).context("jupiter_source_balance_missing")?;
    let b = post.get(&n).context("jupiter_source_balance_missing")?;
    ensure!(
        [a, b]
            .iter()
            .all(|r| r.0 == owner && r.1 == mint && r.2 == TOKEN && r.3 == dec),
        "jupiter_source_balance_identity"
    );
    Ok((a.4, b.4))
}
pub(super) fn keys(t: &Value, wallet: &str) -> Result<Vec<String>> {
    let m = &t["transaction"]["message"];
    let a = m["accountKeys"].as_array().context("jupiter_source_keys")?;
    let parsed = a.first().is_some_and(Value::is_object);
    ensure!(
        !a.is_empty()
            && (t["version"] == "legacy" || t["version"] == 0)
            && m.get("transactionConfig").is_none(),
        "jupiter_source_message"
    );
    if parsed {
        ensure!(
            a.iter()
                .enumerate()
                .all(|(i, k)| k["signer"].as_bool() == Some(i == 0) && k["writable"].is_boolean()),
            "jupiter_source_signer"
        );
    } else {
        ensure!(
            m["header"]["numRequiredSignatures"] == 1,
            "jupiter_source_signer"
        );
    }
    let mut keys = a
        .iter()
        .map(|k| {
            k.as_str()
                .or_else(|| k["pubkey"].as_str())
                .map(str::to_owned)
                .context("jupiter_source_key")
        })
        .collect::<Result<Vec<_>>>()?;
    if !parsed && t["version"] == 0 {
        for f in ["writable", "readonly"] {
            for k in t["meta"]["loadedAddresses"][f]
                .as_array()
                .context("jupiter_source_loaded")?
            {
                keys.push(k.as_str().context("jupiter_source_loaded")?.into());
            }
        }
    }
    ensure!(
        keys.len() <= 256
            && keys[0] == wallet
            && keys.iter().collect::<BTreeSet<_>>().len() == keys.len()
            && keys.iter().all(|k| decode(k).is_ok_and(|d| d.len() == 32)),
        "jupiter_source_key_set"
    );
    Ok(keys)
}
