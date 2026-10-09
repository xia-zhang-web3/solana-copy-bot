//! Direct classic AMMv4: quantities are the two executed SPL CPI legs, not cash.
//! Layout/opcodes: raydium-io/raydium-amm program/src/{instruction,processor}.rs.
use super::{wire, Attribution, Instruction, Row, Trade, View, SOL_MINT, TOKEN};
use std::collections::HashSet;
#[path = "native_attribution_ammv4_lifecycle.rs"]
mod lifecycle;

pub(super) const PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

pub(super) fn attribute(v: &View, signer: &str) -> Attribution {
    let Some(top) = &v.top else {
        return Attribution::Unknown;
    };
    let parents: Vec<_> = top
        .iter()
        .enumerate()
        .filter(|(_, ix)| {
            ix.program.as_deref() == Some(PROGRAM)
                && ix
                    .data
                    .as_deref()
                    .is_none_or(|d| d.is_empty() || matches!(d[0], 9 | 11))
        })
        .collect();
    if parents.is_empty() {
        return Attribution::NotApplicable;
    }
    if parents.len() != 1 {
        return Attribution::Unknown;
    }
    let (index, parent) = parents[0];
    // A recognized but damaged parent must never escape into native cash inference.
    prove(v, signer, index, parent).unwrap_or(Attribution::Unknown)
}

fn prove(v: &View, signer: &str, index: usize, parent: &Instruction) -> Option<Attribution> {
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
        || parent.depth.is_some_and(|d| d != 1)
    {
        return None;
    }
    let a = parent.accounts.as_ref()?;
    if !matches!(a.len(), 17 | 18) || a.iter().any(|i| *i >= keys.len()) {
        return None;
    }
    let offset = a.len() - 17; // optional deprecated target-orders account
    let (coin, pc, source, destination, trader) = (
        a[4 + offset],
        a[5 + offset],
        a[14 + offset],
        a[15 + offset],
        a[16 + offset],
    );
    if v.key(a[0])? != TOKEN
        || v.key(trader)? != signer
        || [a[1], a[2], coin, pc, source, destination, trader]
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != 7
    {
        return None;
    }
    let data = parent.data.as_deref()?;
    if data.len() != 17 || !matches!(data[0], 9 | 11) {
        return None;
    }
    for rows in [v.pre_tokens.as_ref()?, v.post_tokens.as_ref()?] {
        let mut seen = HashSet::new();
        if rows
            .iter()
            .any(|r| r.index >= keys.len() || !seen.insert(r.index))
        {
            return None;
        }
    }
    let (coin_pre, coin_post) = vault(v, coin, v.key(a[2])?)?;
    let (pc_pre, pc_post) = vault(v, pc, v.key(a[2])?)?;
    if coin_pre.mint == pc_pre.mint {
        return None;
    }
    if coin_pre.mint != SOL_MINT && pc_pre.mint != SOL_MINT {
        // A proven token/token AMM has no SOL swap. Do not fabricate one from
        // the wallet's transaction fee via the legacy native cash fallback.
        return Some(Attribution::Unknown);
    }
    let (sol_vault, target_vault, target_pre, target_post) = if coin_pre.mint == SOL_MINT {
        (coin, pc, pc_pre, pc_post)
    } else {
        (pc, coin, coin_pre, coin_post)
    };
    let groups = v.inner.as_ref()?;
    let mut seen = HashSet::new();
    if groups
        .iter()
        .any(|(i, _)| *i >= v.top.as_ref().map_or(0, Vec::len) || !seen.insert(*i))
    {
        return None;
    }
    let group = &groups.iter().find(|(i, _)| *i == index)?.1;
    if group.len() != 2 || group.iter().any(|i| i.depth != Some(2)) {
        return None;
    }
    // Input transfer names its pool vault; direction is relative to SOL, never opcode.
    let input_vault = transfer_destination(&group[0])?;
    let buy = input_vault == sol_vault;
    if !buy && input_vault != target_vault {
        return None;
    }
    let (input_pool, output_pool) = if buy {
        (sol_vault, target_vault)
    } else {
        (target_vault, sol_vault)
    };
    let in_mint = if buy { SOL_MINT } else { &target_pre.mint };
    let out_mint = if buy { &target_pre.mint } else { SOL_MINT };
    let input = transfer(
        &group[0],
        [source, input_pool, trader],
        in_mint,
        if buy { 9 } else { target_pre.decimals },
        v,
    )?;
    let output = transfer(
        &group[1],
        [output_pool, destination, a[2]],
        out_mint,
        if buy { target_pre.decimals } else { 9 },
        v,
    )?;
    // Instruction exact-in/out constraints validate the witness; they supply no amount.
    let first = wire::u64_at(data, 1)?;
    let second = wire::u64_at(data, 9)?;
    if (data[0] == 9 && (input != first || output < second))
        || (data[0] == 11 && (input > first || output != second))
    {
        return None;
    }
    let (sol, target) = if buy {
        (input, output)
    } else {
        (output, input)
    };
    let target_account = if buy { destination } else { source };
    let quote = if buy { source } else { destination };
    let (user_pre, user_post) = vault(v, target_account, signer)?;
    let target_delta = if buy {
        i128::from(target)
    } else {
        -i128::from(target)
    };
    if user_pre.mint != target_pre.mint
        || user_pre.decimals != target_pre.decimals
        || delta(user_pre, user_post) != target_delta
        || delta(target_pre, target_post) != -target_delta
    {
        return None;
    }
    let (sol_pre, sol_post) = if sol_vault == coin {
        (coin_pre, coin_post)
    } else {
        (pc_pre, pc_post)
    };
    if sol_pre.decimals != 9
        || delta(sol_pre, sol_post)
            != if buy {
                i128::from(sol)
            } else {
                -i128::from(sol)
            }
    {
        return None;
    }
    unchanged_owned(v, signer, target_account, quote)?;
    lifecycle::prove(
        v,
        index,
        trader,
        quote,
        buy,
        sol,
        &[coin, pc, source, destination],
    )?;
    Some(Attribution::Known(Trade {
        buy,
        target: target_pre.mint.clone(),
        sol_raw: sol,
        target_raw: target,
        target_decimals: target_pre.decimals,
    }))
}

fn vault<'a>(v: &'a View, index: usize, owner: &str) -> Option<(&'a Row, &'a Row)> {
    let pre = v.row(v.pre_tokens.as_ref()?, index)?;
    let (pre, post) = v.pair(index, owner, &pre.mint)?;
    (pre.decimals <= 18).then_some((pre, post))
}
fn delta(pre: &Row, post: &Row) -> i128 {
    i128::from(post.raw) - i128::from(pre.raw)
}
fn transfer_destination(ix: &Instruction) -> Option<usize> {
    let a = ix.accounts.as_ref()?;
    match wire::decode(TOKEN, ix.data.as_deref()?)? {
        wire::Op::Transfer(_) if a.len() == 3 => Some(a[1]),
        wire::Op::TransferChecked { .. } if a.len() == 4 => Some(a[2]),
        _ => None,
    }
}
fn transfer(
    ix: &Instruction,
    expected: [usize; 3],
    mint: &str,
    decimals: u8,
    v: &View,
) -> Option<u64> {
    if ix.program.as_deref()? != TOKEN {
        return None;
    }
    let a = ix.accounts.as_ref()?;
    let amount = match wire::decode(TOKEN, ix.data.as_deref()?)? {
        wire::Op::Transfer(raw) if a.as_slice() == expected => raw,
        wire::Op::TransferChecked {
            raw,
            decimals: actual,
        } if a.len() == 4
            && [a[0], a[2], a[3]] == expected
            && v.key(a[1])? == mint
            && actual == decimals =>
        {
            raw
        }
        _ => return None,
    };
    (amount > 0).then_some(amount)
}
fn unchanged_owned(v: &View, signer: &str, target: usize, quote: usize) -> Option<()> {
    let owned: HashSet<_> = v
        .pre_tokens
        .as_ref()?
        .iter()
        .chain(v.post_tokens.as_ref()?)
        .filter(|r| r.owner == signer)
        .map(|r| r.index)
        .collect();
    for i in owned {
        if [target, quote].contains(&i) {
            continue;
        }
        let (a, b) = vault(v, i, signer)?;
        if delta(a, b) != 0 {
            return None;
        }
    }
    Some(())
}
