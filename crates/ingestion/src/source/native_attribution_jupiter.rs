//! Exact temporary-WSOL BUY for the closed Jupiter V1 → classic Raydium profile.
//! The parent ABI, AMM operands, CPI amounts and custody changes must all agree.
use super::{ata, pda, wire, Attribution, Instruction, Trade, View, ATA, SOL_MINT, SYSTEM, TOKEN};
use std::collections::HashSet;
const JUP: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const AMM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ROUTE: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];

pub(super) fn attribute(v: &View, signer: &str) -> Attribution {
    let Some(top) = &v.top else {
        return Attribution::Unknown;
    };
    let parents = top
        .iter()
        .enumerate()
        .filter(|(_, i)| i.program.as_deref() == Some(JUP))
        .collect::<Vec<_>>();
    if parents.is_empty() {
        return Attribution::NotApplicable;
    }
    // Existing exact persistent WSOL inference remains authoritative for sources.
    if v.persistent_delta(signer).is_some_and(|delta| delta != 0) {
        return Attribution::NotApplicable;
    }
    if parents.len() != 1
        || !parents[0]
            .1
            .data
            .as_deref()
            .is_some_and(|d| d.starts_with(&ROUTE))
    {
        return Attribution::Unknown;
    }
    prove(v, signer, parents[0].0, parents[0].1)
        .map(Attribution::Known)
        .unwrap_or(Attribution::Unknown)
}
fn transfer(ix: &Instruction, keys: [usize; 3]) -> Option<u64> {
    if ix.program.as_deref()? != TOKEN || ix.accounts.as_deref()? != keys || ix.depth != Some(3) {
        return None;
    }
    let wire::Op::Transfer(amount) = wire::decode(TOKEN, ix.data.as_ref()?)? else {
        return None;
    };
    (amount > 0).then_some(amount)
}
fn prove(v: &View, signer: &str, index: usize, parent: &Instruction) -> Option<Trade> {
    if !v.successful || v.first_signer.as_deref()? != signer {
        return None;
    }
    let keys = v
        .keys
        .iter()
        .map(Option::as_deref)
        .collect::<Option<Vec<_>>>()?;
    if keys.iter().collect::<HashSet<_>>().len() != keys.len()
        || v.pre.as_ref()?.len() != keys.len()
        || v.post.as_ref()?.len() != keys.len()
    {
        return None;
    }
    if v.top.as_ref()?.iter().any(|i| {
        !matches!(
            i.program.as_deref(),
            Some(JUP | ATA | TOKEN | SYSTEM | "ComputeBudget111111111111111111111111111111")
        )
    }) {
        return None;
    }
    let a = parent.accounts.as_ref()?;
    let data = parent.data.as_ref()?;
    if data.len() != 35
        || data[8..12] != 1_u32.to_le_bytes()
        || data[13..16] != [100, 0, 1]
        || data[34] != 0
    {
        return None;
    }
    let tag = data[12];
    let required = match tag {
        7 => 27,
        105 => 18,
        _ => return None,
    };
    let input = wire::u64_at(data, 16)?;
    let quoted = wire::u64_at(data, 24)?;
    let slippage = u16::from_le_bytes(data[32..34].try_into().ok()?);
    // Source transactions can be larger than the follower's separately enforced
    // 0.01 SOL / 50 bps submission limits. This decoder proves custody facts.
    if a.len() != required
        || input == 0
        || quoted == 0
        || slippage > 10_000
        || v.key(a[0])? != TOKEN
        || v.key(a[1])? != signer
        || v.key(a[6])? != JUP
        || v.key(a[8])? != JUP
        || pda::event_authority(JUP).as_deref()? != v.key(a[7])?
        || (v.key(a[4])? != JUP && a[4] != a[3])
    {
        return None;
    }
    let mint = v.key(a[5])?;
    if mint == SOL_MINT {
        return None;
    }
    let r = &a[9..];
    if v.key(r[0])? != AMM || v.key(r[1])? != TOKEN {
        return None;
    }
    let groups = v.inner.as_ref()?;
    let mut indices = HashSet::new();
    if groups
        .iter()
        .any(|(n, _)| *n >= v.top.as_ref().unwrap().len() || !indices.insert(*n))
    {
        return None;
    }
    let cpis = &groups.iter().find(|(n, _)| *n == index)?.1;
    // No arbitrary CPI or additional leg can be assigned to this exchange.
    if ![3, 4].contains(&cpis.len()) {
        return None;
    }
    let swap = &cpis[0];
    if swap.program.as_deref()? != AMM
        || swap.depth != Some(2)
        || swap.accounts.as_deref()? != &r[1..]
    {
        return None;
    }
    let amm = swap.accounts.as_ref()?;
    let d = swap.data.as_ref()?;
    let (coin, pc, source, destination, owner, opcode) = if tag == 7 {
        (4, 5, 14, 15, 16, 9)
    } else {
        (3, 4, 5, 6, 7, 16)
    };
    if d.len() != 17
        || d[0] != opcode
        || wire::u64_at(d, 1)? != input
        || amm[source] != a[2]
        || amm[destination] != a[3]
        || amm[owner] != a[1]
    {
        return None;
    }
    let authority = v.key(amm[2])?;
    let pair_a = v.pair(
        amm[coin],
        authority,
        v.row(v.pre_tokens.as_ref()?, amm[coin])?.mint.as_str(),
    )?;
    let (sol_vault, target_vault) = if pair_a.0.mint == SOL_MINT {
        (amm[coin], amm[pc])
    } else {
        (amm[pc], amm[coin])
    };
    let sol = transfer(&cpis[1], [a[2], sol_vault, a[1]])?;
    let target = transfer(&cpis[2], [target_vault, a[3], amm[2]])?;
    if sol != input
        || target < wire::u64_at(d, 9)?
        || u128::from(target) < u128::from(quoted) * u128::from(10_000 - slippage) / 10_000
    {
        return None;
    }
    let (sol_pre, sol_post) = v.pair(sol_vault, authority, SOL_MINT)?;
    let (pool_pre, pool_post) = v.pair(target_vault, authority, mint)?;
    let user_post = v.row(v.post_tokens.as_ref()?, a[3])?;
    let before = if let Some(pre) = v.row(v.pre_tokens.as_ref()?, a[3]) {
        if pre.owner != signer
            || pre.program != TOKEN
            || pre.mint != mint
            || pre.decimals != user_post.decimals
        {
            return None;
        }
        pre.raw
    } else {
        // The zero is justified later by the complete target ATA creation CPI;
        // native presence or an output token delta alone never supplies it.
        if *v.pre.as_ref()?.get(a[3])? != 0 {
            return None;
        }
        0
    };
    if user_post.owner != signer
        || user_post.program != TOKEN
        || user_post.mint != mint
        || sol_pre.decimals != 9
        || user_post.decimals != pool_pre.decimals
        || i128::from(sol_post.raw) - i128::from(sol_pre.raw) != i128::from(sol)
        || i128::from(*v.post.as_ref()?.get(sol_vault)?)
            - i128::from(*v.pre.as_ref()?.get(sol_vault)?)
            != i128::from(sol)
        || i128::from(pool_post.raw) - i128::from(pool_pre.raw) != -i128::from(target)
        || i128::from(user_post.raw) - i128::from(before) != i128::from(target)
    {
        return None;
    }
    if cpis.len() == 4 && !event(v, &cpis[3], a[7], r[2], mint, sol, target)? {
        return None;
    }
    for rows in [v.pre_tokens.as_ref()?, v.post_tokens.as_ref()?] {
        let mut seen = HashSet::new();
        if rows
            .iter()
            .any(|r| r.index >= keys.len() || !seen.insert(r.index) || r.index == a[2])
        {
            return None;
        }
    }
    let owned = v
        .pre_tokens
        .as_ref()?
        .iter()
        .chain(v.post_tokens.as_ref()?)
        .filter(|r| r.owner == signer)
        .map(|r| r.index)
        .collect::<HashSet<_>>();
    for account in owned {
        if account == a[3] {
            continue;
        }
        let before = v.row(v.pre_tokens.as_ref()?, account)?;
        let after = v.row(v.post_tokens.as_ref()?, account)?;
        if before.owner != signer
            || after.owner != signer
            || before.mint != after.mint
            || before.program != TOKEN
            || after.program != TOKEN
            || before.decimals != after.decimals
            || before.raw != after.raw
        {
            return None;
        }
    }
    // Reuse the full classic ATA creation/fund/sync/close witness. Its role layout
    // is independent of the DEX parent; all mapped accounts are bound above.
    let sol_mint = keys.iter().position(|k| *k == SOL_MINT)?;
    let mut roles = [0; 13];
    roles[0] = amm[2];
    roles[1] = a[1];
    roles[3] = sol_mint;
    roles[4] = a[5];
    roles[5] = a[2];
    roles[6] = a[3];
    roles[7] = sol_vault;
    roles[8] = target_vault;
    roles[11] = a[0];
    roles[12] = a[0];
    let (setup, target_setup) = ata::prove_jupiter(v, index, &roles, sol)?;
    for (n, group) in groups {
        if *n == setup || *n == index || Some(*n) == target_setup {
            continue;
        }
        if !group.is_empty() {
            return None;
        }
    }
    Some(Trade {
        buy: true,
        target: mint.to_owned(),
        sol_raw: sol,
        target_raw: target,
        target_decimals: user_post.decimals,
    })
}

// Anchor emit_cpi is one readonly self-event account, not another trading leg.
// SwapEvent layout: pinned jup-ag/jupiter-cpi 12bc5f67... / idl.json events.
// Anchor EVENT_IX_TAG_LE is 0x1d9acb512ea545e4_u64.to_le_bytes(); event discriminator is
// sha256("event:SwapEvent")[..8]. Exact custody values remain proven by CPIs.
fn event(
    v: &View,
    ix: &Instruction,
    authority: usize,
    pool: usize,
    mint: &str,
    input: u64,
    output: u64,
) -> Option<bool> {
    let d = ix.data.as_ref()?;
    Some(
        ix.program.as_deref()? == JUP
            && ix.depth == Some(2)
            && ix.accounts.as_deref()? == [authority]
            && d.len() == 128
            && d[..16]
                == [
                    228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226,
                ]
            && bs58::encode(&d[16..48]).into_string() == v.key(pool)?
            && bs58::encode(&d[48..80]).into_string() == SOL_MINT
            && wire::u64_at(d, 80)? == input
            && bs58::encode(&d[88..120]).into_string() == mint
            && wire::u64_at(d, 120)? == output,
    )
}
