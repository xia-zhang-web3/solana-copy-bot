//! Closed finalized AMM v4 operands: the exchange is proved by two classic SPL CPIs.
//! Supports the saved direct source layout and the constrained Jupiter follower BUY.
use anyhow::{ensure, Context, Result};
use serde_json::Value;

pub(super) const AMM: &str = crate::execution_owner_buy_wire::RAYDIUM;
const TOKEN: &str = super::TOKEN;
const SOL: &str = super::SOL;
const JUP: &str = crate::execution_owner_buy_wire::JUPITER;

fn program<'a>(ix: &'a Value, keys: &[&'a str]) -> Result<&'a str> {
    if let Some(p) = ix["programId"].as_str() { return Ok(p); }
    keys.get(ix["programIdIndex"].as_u64().context("owned_sell_amm_program")? as usize)
        .copied().context("owned_sell_amm_program")
}
fn accounts<'a>(ix: &'a Value, keys: &[&'a str]) -> Result<Vec<&'a str>> {
    ix["accounts"].as_array().context("owned_sell_amm_accounts")?.iter().map(|v| {
        if let Some(k) = v.as_str() {
            ensure!(keys.contains(&k), "owned_sell_amm_account_binding");
            Ok(k)
        } else {
            keys.get(v.as_u64().context("owned_sell_amm_index")? as usize)
                .copied().context("owned_sell_amm_index")
        }
    }).collect()
}
fn data(ix: &Value) -> Result<Vec<u8>> {
    bs58::decode(ix["data"].as_str().context("owned_sell_amm_data")?)
        .into_vec().context("owned_sell_amm_data")
}
fn token_row<'a>(v: &'a Value, field: &str, keys: &[&str], account: &str) -> Result<Option<&'a Value>> {
    let index = keys.iter().position(|k| *k == account).context("owned_sell_amm_token_key")?;
    let rows = v["meta"][field].as_array().context("owned_sell_amm_balances")?;
    let found = rows.iter().filter(|r| r["accountIndex"].as_u64() == Some(index as u64)).collect::<Vec<_>>();
    ensure!(found.len() <= 1, "owned_sell_amm_duplicate_balance");
    if let Some(r) = found.first() {
        ensure!(r["programId"] == TOKEN, "owned_sell_amm_token_program");
        let raw = r["uiTokenAmount"]["amount"].as_str().context("owned_sell_amm_token_raw")?;
        ensure!(raw.parse::<u64>()?.to_string() == raw, "owned_sell_amm_token_raw");
        ensure!(r["uiTokenAmount"]["decimals"].as_u64().is_some_and(|d| d <= 38),
            "owned_sell_amm_token_decimals");
        crate::execution_pumpswap_accounts::parse_pubkey(
            r["owner"].as_str().context("owned_sell_amm_owner")?, "owned_sell_amm_owner")?;
    }
    Ok(found.first().copied())
}
fn row_amount(row: &Value) -> Result<u64> {
    Ok(row["uiTokenAmount"]["amount"].as_str().context("owned_sell_amm_token_raw")?.parse()?)
}
fn identity(row: &Value, mint: &str, owner: &str, decimals: u8) -> Result<()> {
    ensure!(row["mint"] == mint && row["owner"] == owner
        && row["uiTokenAmount"]["decimals"] == decimals, "owned_sell_amm_token_identity");
    Ok(())
}
fn transfer(ix: &Value, keys: &[&str], source: &str, destination: &str, owner: &str) -> Result<u64> {
    ensure!(program(ix, keys)? == TOKEN, "owned_sell_amm_cpi_program");
    if ix.get("parsed").is_some() {
        let p = &ix["parsed"];
        ensure!(p["type"] == "transfer" && p["info"]["source"] == source
            && p["info"]["destination"] == destination && p["info"]["authority"] == owner,
            "owned_sell_amm_cpi_transfer");
        let raw = p["info"]["amount"].as_str().context("owned_sell_amm_cpi_raw")?;
        let amount = raw.parse::<u64>()?;
        ensure!(raw == amount.to_string(), "owned_sell_amm_cpi_raw");
        Ok(amount)
    } else {
        let a = accounts(ix, keys)?;
        let d = data(ix)?;
        ensure!(a == [source, destination, owner] && d.len() == 9 && d[0] == 3,
            "owned_sell_amm_cpi_transfer");
        Ok(u64::from_le_bytes(d[1..].try_into()?))
    }
}

pub(super) fn supported(v: &Value, keys: &[&str]) -> Result<bool> {
    let outer = v.pointer("/transaction/message/instructions").and_then(Value::as_array)
        .context("owned_sell_amm_instructions")?;
    Ok(outer.iter().any(|i| program(i,keys).is_ok_and(|p| p == AMM || p == JUP)))
}
pub(super) struct Swap<'a> {
    pub base: &'a str,
    pub quote: &'a str,
    pub input: u64,
    pub output: u64,
    pub nested: bool,
}
pub(super) fn prove<'a>(v: &'a Value, keys: &[&'a str], wallet: &str, mint: &str, dec: u8, sell: bool) -> Result<Swap<'a>> {
    let outer = v.pointer("/transaction/message/instructions").and_then(Value::as_array)
        .context("owned_sell_amm_instructions")?;
    let direct = outer.iter().enumerate().filter_map(|(n, i)|
        (program(i, keys).ok() == Some(AMM)).then_some((n, i))).collect::<Vec<_>>();
    let nested = direct.is_empty();
    let (index, instruction, cpis) = if nested {
        ensure!(!sell, "owned_sell_amm_source_direct_required");
        let routes = outer.iter().enumerate().filter_map(|(n, i)|
            (program(i, keys).ok() == Some(JUP)).then_some((n, i))).collect::<Vec<_>>();
        ensure!(routes.len() == 1, "owned_sell_amm_route_missing");
        let index = routes[0].0;
        let route_data = data(routes[0].1)?;
        ensure!(route_data.len() == 35 && route_data[..8] == [229,23,203,151,122,227,173,42]
            && route_data[8..12] == 1_u32.to_le_bytes() && [7,105].contains(&route_data[12])
            && route_data[13..16] == [100,0,1] && route_data[34] == 0,
            "owned_sell_amm_route_layout");
        let group = inner(v, index)?;
        let swaps = group.iter().enumerate().filter_map(|(n, i)|
            (program(i, keys).ok() == Some(AMM)).then_some((n, i))).collect::<Vec<_>>();
        ensure!(swaps.len() == 1 && swaps[0].1["stackHeight"] == 2,
            "owned_sell_amm_cpi_missing");
        let offset = swaps[0].0 + 1;
        let cpis = group[offset..].iter().take_while(|i| i["stackHeight"].as_u64().is_some_and(|d| d > 2))
            .collect::<Vec<_>>();
        (index, swaps[0].1, cpis)
    } else {
        ensure!(direct.len() == 1 && !outer.iter().any(|i| program(i, keys).ok() == Some(JUP)),
            "owned_sell_amm_multiple_swaps");
        ensure!(outer.iter().enumerate().all(|(n,i)| n == direct[0].0 ||
            (program(i,keys).ok() == Some("ComputeBudget111111111111111111111111111111")
                && accounts(i,keys).is_ok_and(|a| a.is_empty()))),
            "owned_sell_amm_outer_layout");
        (direct[0].0, direct[0].1, inner(v, direct[0].0)?.iter().collect())
    };
    let a = accounts(instruction, keys)?;
    let d = data(instruction)?;
    ensure!(d.len() == 17 && a.first() == Some(&TOKEN), "owned_sell_amm_layout");
    let (vault_a, vault_b, source, destination, owner) = match (d[0], a.len()) {
        (9 | 11, 18) => (5,6,15,16,17),
        (9 | 11, 17) => (4,5,14,15,16),
        (16, 8) if !sell => (3,4,5,6,7),
        _ => anyhow::bail!("owned_sell_amm_layout"),
    };
    ensure!(a[owner] == wallet && (!sell || d[0] == 9), "owned_sell_amm_signer_or_side");
    if nested {
        let route = data(&outer[index])?;
        ensure!(matches!((route[12],d[0],a.len()),(7,9,17)|(105,16,8)),
            "owned_sell_amm_route_cpi_layout");
    }
    ensure!(cpis.len() == 2 && cpis.iter().all(|i| i["stackHeight"].as_u64()
        .is_none_or(|n| n == if nested {3} else {2})), "owned_sell_amm_cpi_count");
    let (input_mint, output_mint, input_dec, output_dec) = if sell {(mint,SOL,dec,9)} else {(SOL,mint,9,dec)};
    let pre_a = token_row(v, "preTokenBalances", keys, a[vault_a])?.context("owned_sell_amm_vault_missing")?;
    let pre_b = token_row(v, "preTokenBalances", keys, a[vault_b])?.context("owned_sell_amm_vault_missing")?;
    ensure!(pre_a["owner"] == a[2] && pre_b["owner"] == a[2], "owned_sell_amm_vault_owner");
    let (input_vault, output_vault) = if pre_a["mint"] == input_mint {(a[vault_a],a[vault_b])} else {(a[vault_b],a[vault_a])};
    let input = transfer(cpis[0], keys, a[source], input_vault, wallet)?;
    let output = transfer(cpis[1], keys, output_vault, a[destination], a[2])?;
    ensure!(input > 0 && output > 0, "owned_sell_amm_zero_swap");
    let first = u64::from_le_bytes(d[1..9].try_into()?);
    let second = u64::from_le_bytes(d[9..17].try_into()?);
    ensure!(if d[0] == 11 {input <= first && output == second} else {input == first && output >= second},
        "owned_sell_amm_instruction_amount");
    for (account, asset, decimals, effect) in [(input_vault,input_mint,input_dec,i128::from(input)),
        (output_vault,output_mint,output_dec,-i128::from(output))] {
        let pre = token_row(v,"preTokenBalances",keys,account)?.context("owned_sell_amm_vault_missing")?;
        let post = token_row(v,"postTokenBalances",keys,account)?.context("owned_sell_amm_vault_missing")?;
        identity(pre,asset,a[2],decimals)?; identity(post,asset,a[2],decimals)?;
        ensure!(i128::from(row_amount(post)?) - i128::from(row_amount(pre)?) == effect,
            "owned_sell_amm_vault_delta");
    }
    if nested {
        let rd = data(&outer[index])?;
        ensure!(u64::from_le_bytes(rd[16..24].try_into()?) == input,
            "owned_sell_amm_route_input");
        let quoted = u64::from_le_bytes(rd[24..32].try_into()?);
        let slippage = u16::from_le_bytes(rd[32..34].try_into()?);
        ensure!(quoted > 0 && slippage <= 50 && u128::from(output)
            >= u128::from(quoted) * u128::from(10_000 - slippage) / 10_000,
            "owned_sell_amm_route_output");
    }
    Ok(Swap {base: if sell {a[source]} else {a[destination]},
        quote: if sell {a[destination]} else {a[source]},input,output,nested})
}
fn inner(v: &Value, index: usize) -> Result<&Vec<Value>> {
    let groups = v["meta"]["innerInstructions"].as_array().context("owned_sell_amm_inner")?;
    let found = groups.iter().filter(|g| g["index"].as_u64() == Some(index as u64)).collect::<Vec<_>>();
    ensure!(found.len() == 1, "owned_sell_amm_inner");
    found[0]["instructions"].as_array().context("owned_sell_amm_inner")
}

pub(super) fn buy_target_delta(v: &Value, keys: &[&str], wallet: &str, mint: &str, dec: u8, s: &Swap<'_>) -> Result<()> {
    let post = token_row(v,"postTokenBalances",keys,s.base)?.context("owned_sell_amm_target_missing")?;
    identity(post,mint,wallet,dec)?;
    let before = if let Some(pre) = token_row(v,"preTokenBalances",keys,s.base)? {
        identity(pre,mint,wallet,dec)?; row_amount(pre)?
    } else {
        ensure!(initialized(v, keys, s.base, mint, wallet)?, "owned_sell_amm_target_creation_unproved");
        0
    };
    ensure!(row_amount(post)?.checked_sub(before) == Some(s.output), "owned_sell_amm_buy_target_delta");
    let pre = super::balances(v,"preTokenBalances",keys,wallet)?;
    let post_all = super::balances(v,"postTokenBalances",keys,wallet)?;
    for (key, row) in pre.iter().chain(post_all.iter()).filter(|(k,_)| k.1 == mint && k.0 != s.base) {
        ensure!(pre.get(key) == Some(row) && post_all.get(key) == Some(row), "owned_sell_conflicting_token_delta");
    }
    if !s.nested {
        ensure!(super::delta(v,keys,wallet,s.quote,SOL,9)? == -i128::from(s.input),
            "owned_sell_amm_buy_quote_delta");
    } else {
        let owned = token_row(v,"preTokenBalances",keys,s.quote)?.or(token_row(v,"postTokenBalances",keys,s.quote)?);
        if let Some(row) = owned {identity(row,SOL,wallet,9)?;} else {
            ensure!(initialized(v,keys,s.quote,SOL,wallet)?, "owned_sell_amm_wrap_owner_unproved");
        }
    }
    Ok(())
}
fn initialized(v: &Value, keys: &[&str], account: &str, mint: &str, wallet: &str) -> Result<bool> {
    for group in v["meta"]["innerInstructions"].as_array().context("owned_sell_amm_inner")? {
        for i in group["instructions"].as_array().context("owned_sell_amm_inner")? {
            if program(i,keys)? == TOKEN && i["parsed"]["type"].as_str().is_some_and(|t|
                ["initializeAccount","initializeAccount2","initializeAccount3"].contains(&t)) {
                let p = &i["parsed"]["info"];
                if p["account"] == account && p["mint"] == mint && p["owner"] == wallet {return Ok(true);}
            }
        }
    }
    Ok(false)
}
