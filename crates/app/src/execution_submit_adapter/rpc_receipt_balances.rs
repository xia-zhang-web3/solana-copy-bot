use anyhow::{ensure, Result};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn wallet_native_balances(result: &Value, wallet: &str) -> Result<(u64, u64, usize)> {
    let keys = result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("receipt_account_keys_missing"))?;
    let pre = balances(result, "/meta/preBalances", keys.len())?;
    let post = balances(result, "/meta/postBalances", keys.len())?;
    let mut seen = std::collections::HashSet::new();
    let mut wallet_index = None;
    for (index, key) in keys.iter().enumerate() {
        let pubkey = key
            .get("pubkey")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow::anyhow!("receipt_parsed_account_key_missing"))?;
        ensure!(seen.insert(pubkey), "receipt_duplicate_account_key");
        if pubkey == wallet {
            ensure!(
                key.get("signer").and_then(Value::as_bool) == Some(true),
                "receipt_wallet_not_signer"
            );
            ensure!(
                key.get("writable").and_then(Value::as_bool) == Some(true),
                "receipt_wallet_not_writable"
            );
            wallet_index = Some(index);
        }
    }
    let index = wallet_index.ok_or_else(|| anyhow::anyhow!("receipt_wallet_missing"))?;
    Ok((pre[index], post[index], keys.len()))
}

fn balances(result: &Value, path: &str, len: usize) -> Result<Vec<u64>> {
    let items = result
        .pointer(path)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("receipt_sol_balances_missing"))?;
    ensure!(
        items.len() == len && len > 0,
        "receipt_sol_balances_length_mismatch"
    );
    items
        .iter()
        .map(|v| {
            v.as_u64()
                .ok_or_else(|| anyhow::anyhow!("receipt_sol_balance_invalid"))
        })
        .collect()
}

#[derive(PartialEq, Eq)]
struct TokenBalance<'a> {
    owner: &'a str,
    mint: &'a str,
    raw: u64,
    decimals: u8,
    program: Option<&'a str>,
}

pub(super) fn token_delta(
    result: &Value,
    wallet: &str,
    mint: &str,
    keys: usize,
) -> Result<(i128, u8, bool)> {
    let pre = token_balances(result, "/meta/preTokenBalances", keys)?;
    let post = token_balances(result, "/meta/postTokenBalances", keys)?;
    for (index, before) in &pre {
        if let Some(after) = post.get(index) {
            ensure!(
                before.owner == after.owner
                    && before.mint == after.mint
                    && before.decimals == after.decimals,
                "receipt_token_account_identity_changed"
            );
        }
    }
    let mut decimals = None;
    let mut sum = |rows: &BTreeMap<usize, TokenBalance>| -> Result<i128> {
        let mut total = 0_i128;
        for row in rows
            .values()
            .filter(|r| r.owner == wallet && r.mint == mint)
        {
            ensure!(
                decimals.is_none_or(|d| d == row.decimals),
                "receipt_token_decimals_mismatch"
            );
            decimals = Some(row.decimals);
            total = total
                .checked_add(i128::from(row.raw))
                .ok_or_else(|| anyhow::anyhow!("receipt_token_amount_overflow"))?;
        }
        Ok(total)
    };
    let mut used_lifecycle = false;
    // Array presence alone does not prove why a wallet account's row is absent.
    for (rows, other, creation) in [(&post, &pre, true), (&pre, &post, false)] {
        for (index, row) in rows
            .iter()
            .filter(|(_, r)| r.owner == wallet && r.mint == mint)
        {
            if !other.contains_key(index) {
                super::rpc_receipt_lifecycle::validate_missing_row(
                    result,
                    *index,
                    wallet,
                    mint,
                    row.program,
                    creation,
                )?;
                used_lifecycle = true;
            }
        }
    }
    let before = sum(&pre)?;
    let after = sum(&post)?;
    let decimals = decimals.ok_or_else(|| anyhow::anyhow!("receipt_wallet_mint_missing"))?;
    Ok((after - before, decimals, used_lifecycle))
}

fn token_balances<'a>(
    result: &'a Value,
    path: &str,
    keys: usize,
) -> Result<BTreeMap<usize, TokenBalance<'a>>> {
    let rows = result
        .pointer(path)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("receipt_token_balances_missing"))?;
    let mut balances = BTreeMap::new();
    for row in rows {
        let index = row
            .get("accountIndex")
            .and_then(Value::as_u64)
            .and_then(|i| usize::try_from(i).ok())
            .filter(|i| *i < keys)
            .ok_or_else(|| anyhow::anyhow!("receipt_token_account_index_invalid"))?;
        let owner = row
            .get("owner")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("receipt_token_owner_missing"))?;
        let mint = row
            .get("mint")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("receipt_token_mint_missing"))?;
        let raw = row
            .pointer("/uiTokenAmount/amount")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| anyhow::anyhow!("receipt_token_raw_invalid"))?;
        let decimals = row
            .pointer("/uiTokenAmount/decimals")
            .and_then(Value::as_u64)
            .and_then(|v| u8::try_from(v).ok())
            .ok_or_else(|| anyhow::anyhow!("receipt_token_decimals_invalid"))?;
        ensure!(
            balances
                .insert(
                    index,
                    TokenBalance {
                        owner,
                        mint,
                        raw,
                        decimals,
                        program: row.get("programId").and_then(Value::as_str),
                    }
                )
                .is_none(),
            "receipt_token_account_duplicate"
        );
    }
    Ok(balances)
}
