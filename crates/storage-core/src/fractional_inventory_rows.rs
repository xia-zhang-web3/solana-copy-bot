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
        t["transaction"]["message"]
            .get("transactionConfig")
            .is_none(),
        "fraction_transaction_config"
    );
    ensure!(
        t["version"] == "legacy" || t["version"] == 0,
        "fraction_transaction_version"
    );
    metadata_balances(t, b)
}
/// Prefix effects come from complete exact owned token metadata, never swap decoding.
/// v1 remains forbidden as a target signal; its finalized inventory effects are read.
fn metadata_balances(t: &Value, b: &QuoteBinding) -> Result<(Vec<String>, Amounts, Amounts)> {
    ensure!(
        t["version"] == "legacy" || t["version"] == 0 || t["version"] == 1,
        "fraction_prefix_transaction_version"
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
    // Complete raw shape is required even for a metadata-only prefix effect.
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
    let (keys, pre, post) = metadata_balances(t, b)?;
    // A writable known account without token metadata is not evidence of neutrality.
    for (index, address) in keys.iter().enumerate() {
        if inventory.contains_key(address) && writable(t, index)? {
            ensure!(
                pre.contains_key(address) || post.contains_key(address),
                "fraction_prefix_owned_metadata_missing"
            );
        }
    }
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
        let index = keys
            .iter()
            .position(|k| k == a)
            .context("fraction_token_index")?;
        ensure!(
            pre.get(a) == post.get(a) || writable(t, index)?,
            "fraction_prefix_readonly_change"
        );
        ensure!(
            inventory.get(a).copied().unwrap_or(0) == pre.get(a).copied().unwrap_or(0),
            "fraction_prefix_pre_conflict"
        );
        inventory.insert(a.clone(), post.get(a).copied().unwrap_or(0));
    }
    Ok(())
}
pub(super) fn source(t: &Value, keys: &[String], b: &QuoteBinding, n: u64) -> Result<String> {
    super::source::verify(t, keys, b, n)
}

fn writable(t: &Value, index: usize) -> Result<bool> {
    let msg = &t["transaction"]["message"];
    let static_count = msg["accountKeys"]
        .as_array()
        .context("fraction_keys")?
        .len();
    let h = &msg["header"];
    let required = usize::try_from(
        h["numRequiredSignatures"]
            .as_u64()
            .context("fraction_signer_header")?,
    )?;
    let readonly_signed = usize::try_from(
        h["numReadonlySignedAccounts"]
            .as_u64()
            .context("fraction_signer_header")?,
    )?;
    let readonly_unsigned = usize::try_from(
        h["numReadonlyUnsignedAccounts"]
            .as_u64()
            .context("fraction_signer_header")?,
    )?;
    if index < required {
        return Ok(index < required - readonly_signed);
    }
    if index < static_count {
        return Ok(index < static_count - readonly_unsigned);
    }
    let loaded_writable = t["meta"]["loadedAddresses"]["writable"]
        .as_array()
        .context("fraction_loaded_keys")?
        .len();
    Ok(index < static_count + loaded_writable)
}
