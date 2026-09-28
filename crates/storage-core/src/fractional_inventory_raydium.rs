//! The saved direct AMM v4 swap-base-in subset, with both exact SPL CPI legs.
use super::{data, QuoteBinding, TOKEN};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
pub(super) const AMM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SOL: &str = "So11111111111111111111111111111111111111112";
const COMPUTE: &str = "ComputeBudget111111111111111111111111111111";

fn raw(v: &Value) -> Result<u64> {
    let s = v.as_str().context("fraction_raydium_raw")?;
    let n: u64 = s.parse()?;
    ensure!(s == n.to_string(), "fraction_raydium_raw");
    Ok(n)
}
fn balances(
    t: &Value,
    keys: &[String],
    account: &str,
    owner: &str,
    mint: &str,
    dec: u8,
) -> Result<(u64, u64)> {
    let index = keys
        .iter()
        .position(|k| k == account)
        .context("fraction_raydium_account")?;
    let mut amounts = vec![];
    for field in ["preTokenBalances", "postTokenBalances"] {
        let rows = t["meta"][field]
            .as_array()
            .context("fraction_token_balances")?;
        let v = rows
            .iter()
            .find(|v| v["accountIndex"].as_u64() == Some(index as u64))
            .context("fraction_raydium_balance_missing")?;
        ensure!(
            v["owner"] == owner
                && v["mint"] == mint
                && v["programId"] == TOKEN
                && v["uiTokenAmount"]["decimals"].as_u64() == Some(u64::from(dec)),
            "fraction_raydium_balance_identity"
        );
        amounts.push(raw(&v["uiTokenAmount"]["amount"])?);
    }
    Ok((amounts[0], amounts[1]))
}
fn accounts<'a>(ix: &Value, keys: &'a [String]) -> Result<Vec<&'a str>> {
    ix["accounts"]
        .as_array()
        .context("fraction_raydium_accounts")?
        .iter()
        .map(|v| {
            v.as_u64()
                .and_then(|n| keys.get(n as usize))
                .map(String::as_str)
                .context("fraction_raydium_account_index")
        })
        .collect()
}
pub(super) fn source(t: &Value, keys: &[String], b: &QuoteBinding, n: u64) -> Result<String> {
    let message = &t["transaction"]["message"];
    let required = usize::try_from(
        message["header"]["numRequiredSignatures"]
            .as_u64()
            .context("fraction_signer_header")?,
    )?;
    ensure!(
        required == 1 && keys.first() == Some(&b.source_wallet),
        "fraction_wallet_not_signer"
    );
    ensure!(
        b.output_mint == SOL && n > 0,
        "fraction_raydium_sell_identity"
    );
    let instructions = message["instructions"]
        .as_array()
        .context("fraction_source_instructions")?;
    let mut swap = None;
    for (index, ix) in instructions.iter().enumerate() {
        let program = ix["programIdIndex"]
            .as_u64()
            .and_then(|n| keys.get(n as usize))
            .context("fraction_instruction_program")?;
        if program == COMPUTE {
            continue;
        }
        ensure!(
            program == AMM && swap.is_none(),
            "fraction_source_layout_unsupported"
        );
        swap = Some((index, ix));
    }
    let (swap_index, ix) = swap.context("fraction_source_swap_missing")?;
    let bytes = data(ix["data"].as_str().context("fraction_swap_data")?)?;
    ensure!(
        bytes.len() == 17 && bytes[0] == 9 && u64::from_le_bytes(bytes[1..9].try_into()?) == n,
        "fraction_source_n_binding"
    );
    // A historical finalized source may have minimum zero; this is not a send policy.
    let minimum = u64::from_le_bytes(bytes[9..17].try_into()?);
    let a = accounts(ix, keys)?;
    ensure!(
        a.len() == 18 && a[0] == TOKEN && a[17] == b.source_wallet,
        "fraction_raydium_layout_identity"
    );
    let (source_pre, source_post) = balances(t, keys, a[15], a[17], &b.mint, b.decimals)?;
    let (dest_pre, dest_post) = balances(t, keys, a[16], a[17], SOL, 9)?;
    let (coin_pre, coin_post) = balances(t, keys, a[5], a[2], &b.mint, b.decimals)?;
    let (pc_pre, pc_post) = balances(t, keys, a[6], a[2], SOL, 9)?;
    let output = dest_post
        .checked_sub(dest_pre)
        .context("fraction_raydium_output")?;
    ensure!(
        source_pre.checked_sub(source_post) == Some(n)
            && coin_post.checked_sub(coin_pre) == Some(n)
            && pc_pre.checked_sub(pc_post) == Some(output)
            && output > 0
            && output >= minimum,
        "fraction_raydium_raw_conflict"
    );
    ensure!(
        t["meta"]["logMessages"]
            .as_array()
            .is_some_and(|l| l.iter().any(|v| v.as_str()
                == Some("Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success"))),
        "fraction_source_success_missing"
    );
    let groups = t["meta"]["innerInstructions"]
        .as_array()
        .context("fraction_cpi_missing")?;
    let mut legs = vec![];
    for group in groups {
        let inner = group["instructions"]
            .as_array()
            .context("fraction_cpi_missing")?;
        if inner.is_empty() {
            continue;
        }
        ensure!(
            group["index"].as_u64() == Some(swap_index as u64),
            "fraction_raydium_external_cpi"
        );
        for cpi in inner {
            ensure!(
                cpi["stackHeight"].as_u64() == Some(2)
                    && cpi["programIdIndex"]
                        .as_u64()
                        .and_then(|n| keys.get(n as usize))
                        .is_some_and(|p| p == TOKEN),
                "fraction_raydium_cpi_program"
            );
            let ac = accounts(cpi, keys)?;
            let bytes = data(cpi["data"].as_str().context("fraction_cpi_data")?)?;
            ensure!(
                ac.len() == 3 && bytes.len() == 9 && bytes[0] == 3,
                "fraction_raydium_transfer_layout"
            );
            legs.push((ac, u64::from_le_bytes(bytes[1..9].try_into()?)));
        }
    }
    ensure!(
        legs.len() == 2
            && legs[0] == (vec![a[15], a[5], a[17]], n)
            && legs[1] == (vec![a[6], a[16], a[2]], output),
        "fraction_raydium_transfer_conflict"
    );
    Ok(a[15].into())
}
