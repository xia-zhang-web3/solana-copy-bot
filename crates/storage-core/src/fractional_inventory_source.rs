//! Exact finalized source swap subset; no metadata delta alone proves a swap.
use super::{QuoteBinding, TOKEN, TOKEN22};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
#[path = "fractional_inventory_raydium.rs"]
mod raydium;

pub(super) fn verify(t: &Value, keys: &[String], b: &QuoteBinding, n: u64) -> Result<String> {
    let instructions = t["transaction"]["message"]["instructions"]
        .as_array()
        .context("fraction_source_instructions")?;
    if instructions.iter().any(|i| {
        i["programIdIndex"]
            .as_u64()
            .and_then(|n| keys.get(n as usize))
            .is_some_and(|p| p == raydium::AMM)
    }) {
        return raydium::source(t, keys, b, n);
    }
    pump(t, keys, b, n)
}
pub(super) fn data(s: &str) -> Result<Vec<u8>> {
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
fn pump(t: &Value, keys: &[String], b: &QuoteBinding, n: u64) -> Result<String> {
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
