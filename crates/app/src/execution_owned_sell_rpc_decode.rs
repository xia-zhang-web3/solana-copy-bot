//! Narrow supported jsonParsed PumpSwap operands, without an event timestamp.
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::AdmissionFacts;
use copybot_storage_core::{
    association_sell_preparation::ReceiptAnchor, ExecutionCanaryReceiptFacts,
};
use serde_json::Value;
use std::collections::BTreeMap;
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const SOL: &str = "So11111111111111111111111111111111111111112";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
fn basic<'a>(v: &'a Value, sig: &str, wallet: &str, slot: u64) -> Result<Vec<&'a str>> {
    ensure!(
        v["version"] == "legacy" || v["version"] == 0,
        "owned_sell_transaction_version"
    );
    ensure!(
        v["slot"].as_u64() == Some(slot) && slot > 0,
        "owned_sell_transaction_slot"
    );
    ensure!(
        v.pointer("/meta/err") == Some(&Value::Null),
        "owned_sell_transaction_meta_error"
    );
    let signatures = v
        .pointer("/transaction/signatures")
        .and_then(Value::as_array)
        .context("owned_sell_signatures")?;
    ensure!(
        signatures.len() == 1
            && signatures[0].as_str() == Some(sig)
            && bs58::decode(sig).into_vec()?.len() == 64,
        "owned_sell_signature_binding"
    );
    let accounts = v
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .context("owned_sell_accounts")?;
    ensure!(
        !accounts.is_empty() && accounts.len() <= 256,
        "owned_sell_accounts"
    );
    let mut keys = vec![];
    for (i, a) in accounts.iter().enumerate() {
        let k = a["pubkey"].as_str().context("owned_sell_account_key")?;
        crate::execution_pumpswap_accounts::parse_pubkey(k, "owned_sell_account")?;
        ensure!(
            !keys.contains(&k)
                && a["signer"].as_bool() == Some(i == 0)
                && a["writable"].is_boolean(),
            "owned_sell_account_identity"
        );
        keys.push(k);
    }
    ensure!(keys[0] == wallet, "owned_sell_wallet_binding");
    for field in ["preBalances", "postBalances"] {
        let b = v["meta"][field]
            .as_array()
            .context("owned_sell_native_balances")?;
        ensure!(
            b.len() == keys.len() && b.iter().all(|n| n.as_u64().is_some()),
            "owned_sell_native_balances"
        );
    }
    ensure!(
        v["meta"]["fee"].as_u64().is_some(),
        "owned_sell_fee_missing"
    );
    Ok(keys)
}
fn balances(
    v: &Value,
    field: &str,
    keys: &[&str],
    wallet: &str,
) -> Result<BTreeMap<(String, String), (u64, u8)>> {
    let rows = v["meta"][field]
        .as_array()
        .context("owned_sell_token_balances")?;
    let mut out = BTreeMap::new();
    let mut seen = std::collections::BTreeSet::new();
    for r in rows {
        let i = r["accountIndex"]
            .as_u64()
            .context("owned_sell_token_index")? as usize;
        ensure!(i < keys.len() && seen.insert(i), "owned_sell_token_index");
        let owner = r["owner"].as_str().context("owned_sell_token_owner")?;
        let mint = r["mint"].as_str().context("owned_sell_token_mint")?;
        crate::execution_pumpswap_accounts::parse_pubkey(mint, "owned_sell_token_mint")?;
        let raw = r["uiTokenAmount"]["amount"]
            .as_str()
            .context("owned_sell_token_raw")?;
        let amount = raw.parse::<u64>()?;
        let dec = u8::try_from(
            r["uiTokenAmount"]["decimals"]
                .as_u64()
                .context("owned_sell_decimals")?,
        )?;
        ensure!(
            raw == amount.to_string() && dec <= 38 && r["programId"] == TOKEN,
            "owned_sell_token_encoding"
        );
        if owner == wallet {
            out.insert((keys[i].into(), mint.into()), (amount, dec));
        }
    }
    Ok(out)
}
fn delta(
    v: &Value,
    keys: &[&str],
    wallet: &str,
    account: &str,
    mint: &str,
    dec: u8,
) -> Result<i128> {
    let pre = balances(v, "preTokenBalances", keys, wallet)?;
    let post = balances(v, "postTokenBalances", keys, wallet)?;
    ensure!(pre.keys().eq(post.keys()), "owned_sell_unpaired_tokens");
    let key = (account.into(), mint.into());
    let (a, da) = pre.get(&key).context("owned_sell_token_account_missing")?;
    let (b, db) = post.get(&key).context("owned_sell_token_account_missing")?;
    ensure!(*da == dec && *db == dec, "owned_sell_decimals");
    for (k, (before, decimals)) in &pre {
        let (after, post_decimals) = post.get(k).context("owned_sell_unpaired_tokens")?;
        ensure!(decimals == post_decimals, "owned_sell_decimals");
        if k.1 == mint && k != &key {
            ensure!(before == after, "owned_sell_conflicting_token_delta");
        }
    }
    Ok(i128::from(*b) - i128::from(*a))
}
fn swap<'a>(
    v: &'a Value,
    keys: &[&str],
    wallet: &str,
    mint: &str,
    sell: bool,
    raw: u64,
) -> Result<(&'a str, &'a str)> {
    // First version intentionally supports one top-level PumpSwap swap only.
    // No logs/program-name/token-debit inference and no inner-instruction fallback.
    let ixs = v
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
        .context("owned_sell_instructions")?;
    let matches = ixs
        .iter()
        .filter(|i| i["programId"] == PUMP)
        .collect::<Vec<_>>();
    ensure!(matches.len() == 1, "owned_sell_decoded_swap_missing");
    let ix = matches[0];
    let data =
        bs58::decode(ix["data"].as_str().context("owned_sell_instruction_data")?).into_vec()?;
    let discriminator = if sell {
        [51, 230, 133, 164, 1, 127, 131, 173]
    } else {
        [198, 46, 21, 82, 180, 217, 232, 112]
    };
    ensure!(
        data.len() == if sell { 24 } else { 25 },
        "owned_sell_decoded_swap_length"
    );
    ensure!(
        data[..8] == discriminator && (sell || data[24] <= 1),
        "owned_sell_decoded_swap_side"
    );
    ensure!(
        u64::from_le_bytes(data[8..16].try_into()?) == raw && raw > 0,
        "owned_sell_decoded_swap_amount"
    );
    ensure!(
        u64::from_le_bytes(data[16..24].try_into()?) > 0,
        "owned_sell_decoded_swap_minimum"
    );
    let a = ix["accounts"]
        .as_array()
        .context("owned_sell_instruction_accounts")?;
    ensure!(
        a.len() >= 9
            && a.iter()
                .all(|a| a.as_str().is_some_and(|k| keys.contains(&k))),
        "owned_sell_instruction_accounts"
    );
    ensure!(
        a[1] == wallet && a[3] == mint && a[4] == SOL,
        "owned_sell_decoded_swap_identity"
    );
    Ok((a[5].as_str().unwrap(), a[6].as_str().unwrap()))
}
pub(super) fn buy(v: &Value, r: &ReceiptAnchor, f: &ExecutionCanaryReceiptFacts) -> Result<()> {
    ensure!(
        f.tx_signature == r.contributor.tx_signature
            && f.wallet_pubkey == r.wallet
            && f.token == r.token
            && f.side == "buy"
            && f.slot == r.slot,
        "owned_sell_buy_receipt_identity"
    );
    let keys = basic(v, &f.tx_signature, &r.wallet, r.slot)?;
    ensure!(
        v["meta"]["preBalances"][0].as_u64() == Some(f.wallet_native_pre.as_u64())
            && v["meta"]["postBalances"][0].as_u64() == Some(f.wallet_native_post.as_u64()),
        "owned_sell_buy_native_receipt"
    );
    if let Some(fee) = f.transaction_fee {
        ensure!(
            v["meta"]["fee"].as_u64() == Some(fee.as_u64()),
            "owned_sell_buy_fee_receipt"
        );
    }
    let ixs = v
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
        .context("owned_sell_buy_instructions")?;
    let ix = ixs
        .iter()
        .find(|i| i["programId"] == PUMP)
        .context("owned_sell_buy_swap_missing")?;
    let data = bs58::decode(ix["data"].as_str().context("owned_sell_buy_data")?).into_vec()?;
    ensure!(data.len() >= 16, "owned_sell_buy_data");
    let input = u64::from_le_bytes(data[8..16].try_into()?);
    ensure!(
        input <= copybot_storage_core::TINY_BUY_LAMPORTS,
        "owned_sell_buy_amount_cap"
    );
    let (base, quote) = swap(v, &keys, &r.wallet, &r.token, false, input)?;
    let raw = r.raw.parse::<i128>()?;
    ensure!(
        raw > 0
            && delta(v, &keys, &r.wallet, base, &r.token, r.decimals)? == raw
            && delta(v, &keys, &r.wallet, quote, SOL, 9)? == -i128::from(input),
        "owned_sell_buy_receipt_amount"
    );
    ensure!(
        f.token_delta
            .as_ref()
            .is_some_and(|d| d.raw == raw && d.decimals == r.decimals),
        "owned_sell_buy_receipt_delta"
    );
    Ok(())
}
pub(super) fn sell(v: &Value, s: &AdmissionFacts) -> Result<u64> {
    let f = &s.facts;
    let e = f
        .exact_amounts
        .as_ref()
        .context("owned_sell_source_amount_missing")?;
    ensure!(
        f.token_out == SOL && e.amount_out_decimals == 9,
        "owned_sell_source_side"
    );
    let keys = basic(v, &f.signature, &f.wallet, f.slot)?;
    let raw = e.amount_in_raw.parse::<u64>()?;
    let (base, quote) = swap(v, &keys, &f.wallet, &f.token_in, true, raw)?;
    ensure!(
        delta(v, &keys, &f.wallet, base, &f.token_in, e.amount_in_decimals)? == -i128::from(raw)
            && delta(v, &keys, &f.wallet, quote, SOL, 9)? == e.amount_out_raw.parse::<i128>()?,
        "owned_sell_source_decoded_amount"
    );
    Ok(f.slot)
}
