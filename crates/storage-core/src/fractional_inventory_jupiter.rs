//! Closed USDC→temporary SOL Jupiter V1/Raydium-v2 source custody grammar.
//! A successful wrapper, token debit or native refund alone never proves a SELL.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
#[path = "fractional_inventory_jupiter_seeded.rs"]
mod seeded;
#[path = "fractional_inventory_jupiter_wire.rs"]
mod wire;
use wire::{decode, exact, instruction, keys, pair, rows, ATA, SYSTEM, TOKEN};
pub const JUPITER: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const EVENT: &str = "D8cy77BBepLMngZx6ZukaTff5hCt1HrWyKk3Hnd9oitf";
const AMM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const COMPUTE: &str = "ComputeBudget111111111111111111111111111111";
#[derive(Debug)]
pub struct JupiterSourceSellProof {
    pub source_account: String,
    pub temporary_account: String,
    pub target_raw: u64,
    pub sol_raw: u64,
    pub setup_index: usize,
    pub route_index: usize,
    pub seeded: bool,
}
/// Shared by genuine ingress attribution, finalized source receipt verification,
/// and the full parent/program/prefix fractional proof. It grants no authority.
pub fn prove_jupiter_source_sell(
    t: &Value,
    wallet: &str,
    mint: &str,
    dec: u8,
    n: u64,
) -> Result<JupiterSourceSellProof> {
    ensure!(
        mint == USDC && dec == 6 && n > 0 && t["meta"].get("err") == Some(&Value::Null),
        "jupiter_source_sell_identity"
    );
    let keys = keys(t, wallet)?;
    let outer = t["transaction"]["message"]["instructions"]
        .as_array()
        .context("jupiter_source_outer")?
        .iter()
        .map(|v| instruction(v, &keys))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        outer.len() <= 8 && outer.iter().all(|i| i.depth.is_none_or(|d| d == 1)),
        "jupiter_source_outer"
    );
    let is_seeded = outer
        .iter()
        .any(|i| i.program == SYSTEM && i.data.get(..4) == Some(&3u32.to_le_bytes()));
    let routes = outer
        .iter()
        .enumerate()
        .filter(|(_, i)| i.program == JUPITER)
        .collect::<Vec<_>>();
    ensure!(routes.len() == 1, "jupiter_source_route_count");
    let (index, r) = routes[0];
    let d = &r.data;
    let a = &r.accounts;
    ensure!(
        d.len() == 35
            && d[..8] == [229, 23, 203, 151, 122, 227, 173, 42]
            && d[8..12] == 1u32.to_le_bytes()
            && d[12..16] == [105, 100, 0, 1]
            && d[34] == 0
            && u64::from_le_bytes(d[16..24].try_into()?) == n
            && a.len() == 18
            && a[0] == TOKEN
            && a[1] == wallet
            && (a[4] == JUPITER || (is_seeded && a[4] == a[3]))
            && a[5] == SOL
            && a[6] == JUPITER
            && a[7] == EVENT
            && a[8] == JUPITER
            && a[9] == AMM,
        "jupiter_source_route_layout"
    );
    let quoted = u64::from_le_bytes(d[24..32].try_into()?);
    let slip = u16::from_le_bytes(d[32..34].try_into()?);
    ensure!(quoted > 0 && slip <= 50, "jupiter_source_slippage");
    let amm = &a[10..];
    ensure!(
        amm[0] == TOKEN
            && amm[5] == a[2]
            && amm[6] == a[3]
            && amm[7] == wallet
            && amm[1..].iter().collect::<BTreeSet<_>>().len() == 7,
        "jupiter_source_route_accounts"
    );
    let groups = t["meta"]["innerInstructions"]
        .as_array()
        .context("jupiter_source_cpi")?;
    let mut inner = BTreeMap::new();
    for g in groups {
        let idx = g["index"].as_u64().context("jupiter_source_cpi_index")? as usize;
        let c = g["instructions"]
            .as_array()
            .context("jupiter_source_cpi")?
            .iter()
            .map(|v| instruction(v, &keys))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            idx < outer.len() && c.len() <= 4 && inner.insert(idx, c).is_none(),
            "jupiter_source_cpi_index"
        );
    }
    let c = inner.get(&index).context("jupiter_source_swap_cpi")?;
    ensure!(
        [3, 4].contains(&c.len())
            && c[0].program == AMM
            && c[0].accounts == amm
            && c[0].depth == Some(2)
            && c[0].data.len() == 17
            && c[0].data[0] == 16
            && u64::from_le_bytes(c[0].data[1..9].try_into()?) == n,
        "jupiter_source_swap_cpi"
    );
    let pre = rows(t, "preTokenBalances", keys.len())?;
    let post = rows(t, "postTokenBalances", keys.len())?;
    let pos = |s: &str| {
        keys.iter()
            .position(|k| k == s)
            .context("jupiter_source_account")
    };
    let source = pos(&a[2])?;
    let temp = pos(&a[3])?;
    let coin = pos(&amm[3])?;
    let pc = pos(&amm[4])?;
    let (input_vault, output_vault) = if pre.get(&coin).is_some_and(|r| r.1 == mint) {
        (coin, pc)
    } else {
        (pc, coin)
    };
    let mut transfer = vec![3];
    transfer.extend(n.to_le_bytes());
    ensure!(
        exact(
            &c[1],
            TOKEN,
            &[&a[2], &keys[input_vault], wallet],
            &transfer,
            3
        ),
        "jupiter_source_input_cpi"
    );
    ensure!(
        c[2].data.len() == 9 && c[2].data[0] == 3,
        "jupiter_source_output_cpi"
    );
    let output = u64::from_le_bytes(c[2].data[1..9].try_into()?);
    ensure!(
        output > 0
            && exact(
                &c[2],
                TOKEN,
                &[&keys[output_vault], &a[3], &amm[2]],
                &c[2].data,
                3
            )
            && output >= u64::from_le_bytes(c[0].data[9..17].try_into()?)
            && u128::from(output) >= u128::from(quoted) * u128::from(10_000 - slip) / 10_000,
        "jupiter_source_output_cpi"
    );
    for (account, owner, asset, decimals, effect) in [
        (source, wallet, mint, dec, -i128::from(n)),
        (input_vault, amm[2].as_str(), mint, dec, i128::from(n)),
        (output_vault, amm[2].as_str(), SOL, 9, -i128::from(output)),
    ] {
        let (before, after) = pair(&pre, &post, account, owner, asset, decimals)?;
        ensure!(
            i128::from(after) - i128::from(before) == effect,
            "jupiter_source_token_delta"
        );
    }
    ensure!(
        !pre.contains_key(&temp) && !post.contains_key(&temp),
        "jupiter_source_temp_inventory"
    );
    for i in pre.keys().chain(post.keys()) {
        if ![source, input_vault, output_vault].contains(i) {
            ensure!(
                pre.get(i) == post.get(i),
                "jupiter_source_extra_token_effect"
            );
        }
    }
    if c.len() == 4 {
        let event = &c[3];
        let e = &event.data;
        ensure!(
            e.len() == 128
                && exact(event, JUPITER, &[EVENT], e, 2)
                && e[..16]
                    == [228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226]
                && e[16..48] == decode(AMM)?
                && e[48..80] == decode(mint)?
                && u64::from_le_bytes(e[80..88].try_into()?) == n
                && e[88..120] == decode(SOL)?
                && u64::from_le_bytes(e[120..128].try_into()?) == output,
            "jupiter_source_event"
        );
    }
    let (setup, init) = if is_seeded {
        let (s, i) = seeded::setup(&outer, &inner, index, wallet, &a[3])?;
        (s, Some(i))
    } else {
        let setups = outer
            .iter()
            .enumerate()
            .filter(|(_, i)| i.program == ATA)
            .collect::<Vec<_>>();
        ensure!(setups.len() == 1, "jupiter_source_setup_count");
        let setup = setups[0].0;
        ensure!(
            setup < index
                && exact(
                    setups[0].1,
                    ATA,
                    &[wallet, &a[3], wallet, SOL, SYSTEM, TOKEN],
                    &[1],
                    1
                ),
            "jupiter_source_setup_identity"
        );
        let creation = inner.get(&setup).context("jupiter_source_creation")?;
        ensure!(
            creation.len() == 4
                && exact(&creation[0], TOKEN, &[SOL], &[21, 7, 0], 2)
                && exact(&creation[2], TOKEN, &[&a[3]], &[22], 2),
            "jupiter_source_creation"
        );
        let create = &creation[1];
        let cd = &create.data;
        ensure!(
            cd.len() == 52
                && cd[..4] == [0; 4]
                && u64::from_le_bytes(cd[4..12].try_into()?) > 0
                && u64::from_le_bytes(cd[12..20].try_into()?) == 165
                && cd[20..52] == decode(TOKEN)?
                && exact(create, SYSTEM, &[wallet, &a[3]], cd, 2),
            "jupiter_source_creation"
        );
        let mut init = vec![18];
        init.extend(decode(wallet)?);
        ensure!(
            exact(&creation[3], TOKEN, &[&a[3], SOL], &init, 2),
            "jupiter_source_temp_owner"
        );
        (setup, None)
    };
    let mut closes = 0;
    let mut self_transfers = 0;
    for (i, ix) in outer.iter().enumerate() {
        if i == setup || Some(i) == init || i == index {
            continue;
        }
        if ix.program == COMPUTE {
            ensure!(
                ix.accounts.is_empty()
                    && matches!(ix.data.first(), Some(2 | 3))
                    && ix.data.len() == if ix.data[0] == 2 { 5 } else { 9 },
                "jupiter_source_compute"
            );
        } else if exact(ix, TOKEN, &[&a[3], wallet, wallet], &[9], 1) {
            ensure!(i > index, "jupiter_source_close_order");
            closes += 1;
        } else if ix.program == SYSTEM
            && ix.accounts == [wallet, wallet]
            && ix.data.len() == 12
            && ix.data[..4] == [2, 0, 0, 0]
        {
            ensure!(
                i > index && (is_seeded || u64::from_le_bytes(ix.data[4..].try_into()?) > 0),
                "jupiter_source_self_transfer"
            );
            self_transfers += 1;
        } else {
            anyhow::bail!("jupiter_source_outer_effect");
        }
    }
    ensure!(
        closes == 1
            && self_transfers <= 1
            && inner
                .iter()
                .all(|(i, c)| *i == setup || *i == index || c.is_empty()),
        "jupiter_source_lifecycle"
    );
    let before = t["meta"]["preBalances"]
        .as_array()
        .context("jupiter_source_native")?;
    let after = t["meta"]["postBalances"]
        .as_array()
        .context("jupiter_source_native")?;
    ensure!(
        before.len() == keys.len() && after.len() == keys.len(),
        "jupiter_source_native"
    );
    let b = before
        .iter()
        .map(|x| x.as_u64().context("jupiter_source_native"))
        .collect::<Result<Vec<_>>>()?;
    let p = after
        .iter()
        .map(|x| x.as_u64().context("jupiter_source_native"))
        .collect::<Result<Vec<_>>>()?;
    let fee = t["meta"]["fee"].as_u64().context("jupiter_source_fee")?;
    ensure!(
        b[temp] == 0
            && p[temp] == 0
            && b[source] > 0
            && b[source] == p[source]
            && i128::from(p[output_vault]) - i128::from(b[output_vault]) == -i128::from(output)
            && i128::from(p[0]) - i128::from(b[0]) == i128::from(output) - i128::from(fee)
            && b.iter()
                .zip(&p)
                .enumerate()
                .all(|(i, (b, p))| i == 0 || i == output_vault || b == p),
        "jupiter_source_native_effect"
    );
    Ok(JupiterSourceSellProof {
        source_account: a[2].clone(),
        temporary_account: a[3].clone(),
        target_raw: n,
        sol_raw: output,
        setup_index: setup,
        route_index: index,
        seeded: is_seeded,
    })
}
