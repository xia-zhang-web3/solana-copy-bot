use super::{QuoteBinding, TOKEN, TOKEN22};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
pub(super) fn key(v: &Value) -> Result<String> {
    let s = v.as_str().context("fraction_pubkey")?;
    ensure!(
        copybot_core_types::association_parent::valid_hash(s),
        "fraction_pubkey"
    );
    Ok(s.into())
}
fn amount(v: &Value) -> Result<u64> {
    let s = v.as_str().context("fraction_raw")?;
    ensure!(
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
        "fraction_raw"
    );
    Ok(s.parse()?)
}
pub(super) fn account(
    v: &Value,
    wallet: &str,
    program: Option<&str>,
) -> Result<(String, String, u64, u8)> {
    let address = key(&v["pubkey"])?;
    let p = v["account"]["owner"]
        .as_str()
        .context("fraction_token_program")?;
    ensure!(
        matches!(p, TOKEN | TOKEN22) && program.is_none_or(|expected| p == expected),
        "fraction_token_program"
    );
    let parsed = &v["account"]["data"]["parsed"];
    ensure!(
        parsed["type"] == "account" && parsed["info"]["owner"] == wallet,
        "fraction_account_identity"
    );
    let i = &parsed["info"];
    Ok((
        address,
        key(&i["mint"])?,
        amount(&i["tokenAmount"]["amount"])?,
        u8::try_from(
            i["tokenAmount"]["decimals"]
                .as_u64()
                .context("fraction_decimals")?,
        )?,
    ))
}
type Amounts = BTreeMap<String, u64>;
pub(super) fn balances(t: &Value, b: &QuoteBinding) -> Result<(Vec<String>, Amounts, Amounts)> {
    ensure!(
        t["version"] == "legacy" || t["version"] == 0,
        "fraction_transaction_version"
    );
    let tx = &t["transaction"];
    ensure!(
        tx["signatures"].as_array().is_some_and(|s| !s.is_empty()),
        "fraction_signatures"
    );
    let msg = &tx["message"];
    let a = msg["accountKeys"].as_array().context("fraction_keys")?;
    ensure!(
        a.iter().all(Value::is_string),
        "fraction_raw_encoding_required"
    );
    let required = msg["header"]["numRequiredSignatures"]
        .as_u64()
        .context("fraction_signer_header")? as usize;
    ensure!(
        required > 0
            && required <= a.len()
            && tx["signatures"].as_array().unwrap().len() == required,
        "fraction_signer_header"
    );
    key(&msg["recentBlockhash"])?;
    let mut keys = a
        .iter()
        .map(|v| key(v.get("pubkey").unwrap_or(v)))
        .collect::<Result<Vec<_>>>()?;
    if a.first().is_some_and(Value::is_string) {
        if let Some(loaded) = t["meta"].get("loadedAddresses") {
            for field in ["writable", "readonly"] {
                for k in loaded[field].as_array().context("fraction_loaded_keys")? {
                    keys.push(key(k)?);
                }
            }
        }
    }
    ensure!(
        !keys.is_empty()
            && keys.len() <= 256
            && keys.iter().collect::<BTreeSet<_>>().len() == keys.len(),
        "fraction_key_set"
    );
    let meta = &t["meta"];
    ensure!(
        meta.get("err").is_some() && meta["fee"].as_u64().is_some(),
        "fraction_status_fee"
    );
    // This financial subset refuses failed/null-CPI rather than inferring neutrality.
    let inner = meta["innerInstructions"]
        .as_array()
        .context("fraction_cpi_missing")?;
    ensure!(
        msg["instructions"].is_array(),
        "fraction_instructions_missing"
    );
    let mut indices = BTreeSet::new();
    for i in inner {
        let n = i["index"].as_u64().context("fraction_cpi_index")? as usize;
        ensure!(
            n < msg["instructions"].as_array().unwrap().len()
                && indices.insert(n)
                && i["instructions"].is_array(),
            "fraction_cpi_index"
        );
    }
    for field in ["preBalances", "postBalances"] {
        let v = meta[field].as_array().context("fraction_native_balances")?;
        ensure!(
            v.len() == keys.len() && v.iter().all(|x| x.as_u64().is_some()),
            "fraction_native_balances"
        );
    }
    super::schema::check(t, keys.len())?;
    let mut values = vec![];
    for field in ["preTokenBalances", "postTokenBalances"] {
        let mut out = BTreeMap::new();
        let mut indices = BTreeSet::new();
        for v in meta[field].as_array().context("fraction_token_balances")? {
            let index =
                usize::try_from(v["accountIndex"].as_u64().context("fraction_token_index")?)?;
            ensure!(
                index < keys.len() && indices.insert(index),
                "fraction_token_index"
            );
            let owner = key(&v["owner"])?;
            let mint = key(&v["mint"])?;
            ensure!(
                v["programId"] == TOKEN || v["programId"] == TOKEN22,
                "fraction_balance_program"
            );
            let raw = amount(&v["uiTokenAmount"]["amount"])?;
            let dec = u8::try_from(
                v["uiTokenAmount"]["decimals"]
                    .as_u64()
                    .context("fraction_decimals")?,
            )?;
            if mint == b.mint {
                ensure!(dec == b.decimals, "fraction_balance_decimals");
                if owner == b.source_wallet {
                    out.insert(keys[index].clone(), raw);
                }
            }
        }
        values.push(out);
    }
    Ok((keys, values.remove(0), values.remove(0)))
}
pub(super) fn advance(t: &Value, b: &QuoteBinding, inventory: &mut Amounts) -> Result<()> {
    let (keys, pre, post) = balances(t, b)?;
    for field in ["preTokenBalances", "postTokenBalances"] {
        for v in t["meta"][field].as_array().unwrap() {
            let a = &keys[v["accountIndex"].as_u64().unwrap() as usize];
            if inventory.contains_key(a) {
                ensure!(
                    v["owner"] == b.source_wallet && v["mint"] == b.mint,
                    "fraction_owner_change"
                );
            }
        }
    }
    if !t["meta"]["err"].is_null() {
        ensure!(pre == post, "fraction_failed_change");
    }
    for a in pre.keys().chain(post.keys()).collect::<BTreeSet<_>>() {
        ensure!(
            inventory.get(a).copied().unwrap_or(0) == pre.get(a).copied().unwrap_or(0),
            "fraction_prefix_pre_conflict"
        );
        inventory.insert(a.clone(), post.get(a).copied().unwrap_or(0));
    }
    Ok(())
}
fn data(s: &str) -> Result<Vec<u8>> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    ensure!(s.len() <= 64, "fraction_swap_layout");
    let mut out = vec![0u8; 32];
    for c in s.bytes() {
        let mut carry = ALPHABET
            .iter()
            .position(|b| *b == c)
            .context("fraction_swap_encoding")? as u32;
        for b in out.iter_mut().rev() {
            carry += u32::from(*b) * 58;
            *b = carry as u8;
            carry >>= 8;
        }
        ensure!(carry == 0, "fraction_swap_encoding");
    }
    let first = out.iter().position(|b| *b != 0).unwrap_or(out.len());
    Ok(out[first..].to_vec())
}
pub(super) fn source(t: &Value, keys: &[String], b: &QuoteBinding, n: u64) -> Result<String> {
    const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
    let msg = &t["transaction"]["message"];
    let required = msg["header"]["numRequiredSignatures"]
        .as_u64()
        .context("fraction_signer_header")? as usize;
    ensure!(
        required <= keys.len() && keys[..required].contains(&b.source_wallet),
        "fraction_wallet_not_signer"
    );
    let instructions = msg["instructions"]
        .as_array()
        .context("fraction_source_instructions")?;
    let mut source = None;
    let mut swap_index = 0usize;
    for (outer_index, ix) in instructions.iter().enumerate() {
        let program = ix["programId"]
            .as_str()
            .or_else(|| {
                ix["programIdIndex"]
                    .as_u64()
                    .and_then(|i| keys.get(i as usize).map(String::as_str))
            })
            .context("fraction_instruction_program")?;
        if program == "ComputeBudget111111111111111111111111111111" {
            continue;
        }
        // Preserve the existing financial direct-PumpSwap supported subset. No inferred
        // owner-before-swap delta; any other top-level effect is explicitly unsupported.
        ensure!(
            program == PUMP && source.is_none(),
            "fraction_source_layout_unsupported"
        );
        let bytes = data(ix["data"].as_str().context("fraction_swap_data")?)?;
        ensure!(
            bytes.len() == 24
                && bytes[..8] == [51, 230, 133, 164, 1, 127, 131, 173]
                && u64::from_le_bytes(bytes[8..16].try_into()?) == n,
            "fraction_source_n_binding"
        );
        let accounts = ix["accounts"]
            .as_array()
            .context("fraction_swap_accounts")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .or_else(|| v.as_u64().and_then(|n| keys.get(n as usize).cloned()))
                    .context("fraction_swap_account")
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            accounts.len() >= 19
                && accounts[1] == b.source_wallet
                && accounts[3] == b.mint
                && accounts[4] == b.output_mint,
            "fraction_source_identity"
        );
        source = Some(accounts[5].clone());
        swap_index = outer_index;
    }
    let source = source.context("fraction_source_swap_missing")?;
    ensure!(
        t["meta"]["logMessages"]
            .as_array()
            .is_some_and(|v| v.iter().any(|v| v.as_str()
                == Some("Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA success"))),
        "fraction_source_success_missing"
    );
    let mut outgoing = 0u64;
    for group in t["meta"]["innerInstructions"]
        .as_array()
        .context("fraction_cpi_missing")?
    {
        for ix in group["instructions"]
            .as_array()
            .context("fraction_cpi_missing")?
        {
            ensure!(
                ix["stackHeight"].as_u64().is_some_and(|v| v >= 2),
                "fraction_cpi_depth"
            );
            let program = ix["programIdIndex"]
                .as_u64()
                .and_then(|i| keys.get(i as usize))
                .context("fraction_cpi_program")?;
            let ac = ix["accounts"]
                .as_array()
                .context("fraction_cpi_accounts")?
                .iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|n| keys.get(n as usize))
                        .context("fraction_cpi_account")
                })
                .collect::<Result<Vec<_>>>()?;
            if *program != TOKEN && *program != TOKEN22 {
                continue;
            }
            if !ac.iter().any(|a| a.as_str() == source.as_str()) {
                continue;
            }
            ensure!(
                group["index"].as_u64() == Some(swap_index as u64),
                "fraction_external_source_transfer"
            );
            let data = data(ix["data"].as_str().context("fraction_cpi_data")?)?;
            ensure!(
                *program == TOKEN && matches!(data.first(), Some(3 | 12)),
                "fraction_source_token_instruction"
            );
            let checked = data[0] == 12;
            ensure!(
                data.len() == if checked { 10 } else { 9 }
                    && ac.len() >= if checked { 4 } else { 3 },
                "fraction_transfer_layout"
            );
            let destination = ac[if checked { 2 } else { 1 }];
            ensure!(
                *ac[0] == source
                    && *destination != source
                    && *ac[if checked { 3 } else { 2 }] == b.source_wallet,
                "fraction_transfer_authority"
            );
            if checked {
                ensure!(
                    *ac[1] == b.mint && data[9] == b.decimals,
                    "fraction_transfer_mint"
                );
            }
            outgoing = outgoing
                .checked_add(u64::from_le_bytes(data[1..9].try_into()?))
                .context("fraction_transfer_overflow")?;
        }
    }
    ensure!(outgoing == n, "fraction_swap_transfer_missing_conflict");
    Ok(source)
}
