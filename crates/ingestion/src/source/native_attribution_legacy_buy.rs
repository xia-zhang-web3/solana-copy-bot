//! Direct classic PumpSwap Buy: exact base output, bounded quote debit, both SOL roles.
use super::{pda, wire, Attribution, Instruction, Trade, View, ATA, SOL_MINT, SYSTEM, TOKEN};
use std::collections::HashSet;
#[path = "native_attribution_legacy_buy_event.rs"]
mod event;
#[path = "native_attribution_legacy_buy_lifecycle.rs"]
mod lifecycle;

const FEES: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";

pub(super) fn attribute(v: &View, signer: &str, programs: &HashSet<String>) -> Attribution {
    let Some(top) = &v.top else {
        return Attribution::Unknown;
    };
    // This profile never attributes a nested legacy Buy through a wrapper.
    if v.inner.as_ref().is_some_and(|groups| {
        groups.iter().any(|(_, group)| {
            group.iter().any(|ix| {
                ix.program.as_ref().is_some_and(|p| programs.contains(p))
                    && ix
                        .data
                        .as_deref()
                        .is_some_and(|d| d.starts_with(&wire::LEGACY_BUY))
            })
        })
    }) {
        return Attribution::Unknown;
    }
    let parents: Vec<_> = top
        .iter()
        .enumerate()
        .filter(|(_, ix)| {
            ix.program.as_ref().is_some_and(|p| programs.contains(p))
                && ix
                    .data
                    .as_deref()
                    .is_some_and(|d| d.starts_with(&wire::LEGACY_BUY))
        })
        .collect();
    if parents.is_empty() {
        return Attribution::NotApplicable;
    }
    if parents.len() != 1 {
        return Attribution::Unknown;
    }
    let (index, parent) = parents[0];
    prove(v, signer, index, parent)
        .map(Attribution::Known)
        .unwrap_or(Attribution::Unknown)
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
        || parent.depth.is_some_and(|d| d != 1)
    {
        return None;
    }
    let a = parent.accounts.as_ref()?;
    if !matches!(a.len(), 25 | 26) || a.iter().any(|i| *i >= keys.len()) {
        return None;
    }
    let base_sol = v.key(a[3])? == SOL_MINT;
    let quote_sol = v.key(a[4])? == SOL_MINT;
    if base_sol == quote_sol
        || a.len() != if base_sol { 25 } else { 26 }
        || v.key(a[1])? != signer
        || v.key(a[11])? != TOKEN
        || v.key(a[12])? != TOKEN
        || v.key(a[13])? != SYSTEM
        || v.key(a[14])? != ATA
        || v.key(a[16])? != parent.program.as_deref()?
        || v.key(a[22])? != FEES
        || pda::event_authority(v.key(a[16])?)?.as_str() != v.key(a[15])?
    {
        return None;
    }
    let roles = [
        a[0],
        a[1],
        a[3],
        a[4],
        a[5],
        a[6],
        a[7],
        a[8],
        a[10],
        a[17],
        a[a.len() - 1],
    ];
    if roles.iter().collect::<HashSet<_>>().len() != roles.len() {
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
    let mut seen = HashSet::new();
    let groups = v.inner.as_ref()?;
    if groups
        .iter()
        .any(|(i, _)| *i >= v.top.as_ref().map_or(0, Vec::len) || !seen.insert(*i))
    {
        return None;
    }
    let group = &groups.iter().find(|(i, _)| *i == index)?.1;
    if group.len() != if base_sol { 6 } else { 7 } || group.iter().any(|ix| ix.depth != Some(2)) {
        return None;
    }
    let query = &group[0];
    if query.program.as_deref()? != FEES
        || query.accounts.as_deref()? != [a[21], a[16]]
        || query.data.as_ref()?.len() != 57
        || !query
            .data
            .as_ref()?
            .starts_with(&[154, 237, 138, 92, 162, 2, 162, 187])
    {
        return None;
    }
    let (base_pre, base_post) = v.pair(a[7], v.key(a[0])?, v.key(a[3])?)?;
    let (quote_pre, quote_post) = v.pair(a[8], v.key(a[0])?, v.key(a[4])?)?;
    if base_pre.decimals > 18
        || quote_pre.decimals > 18
        || (base_sol && base_pre.decimals != 9)
        || (quote_sol && quote_pre.decimals != 9)
    {
        return None;
    }
    let base = checked(&group[1], [a[7], a[3], a[5], a[0]], base_pre.decimals)?;
    let quote = checked(&group[2], [a[6], a[4], a[8], a[1]], quote_pre.decimals)?;
    let protocol = fee(v, &group[3], a, a[10], a[9], quote_pre.decimals)?;
    let creator = if base_sol {
        0
    } else {
        fee(v, &group[4], a, a[17], a[18], quote_pre.decimals)?
    };
    let buyback = fee(
        v,
        &group[group.len() - 2],
        a,
        a[a.len() - 1],
        a[a.len() - 2],
        quote_pre.decimals,
    )?;
    let debit = quote
        .checked_add(protocol)?
        .checked_add(creator)?
        .checked_add(buyback)?;
    let (output, max_quote, track) = wire::legacy_buy(parent.data.as_ref()?)?;
    if output == 0
        || base != output
        || debit > max_quote
        || max_quote == 0
        || delta(base_pre.raw, base_post.raw) != -i128::from(base)
        || delta(quote_pre.raw, quote_post.raw) != i128::from(quote)
    {
        return None;
    }
    let balances = lifecycle::prove(
        v,
        index,
        a,
        base,
        debit,
        base_sol,
        base_pre.decimals,
        quote_pre.decimals,
    )?;
    event::prove(
        v,
        a,
        group.last()?,
        base,
        quote,
        protocol,
        creator,
        buyback,
        debit,
        max_quote,
        track,
        balances,
        base_pre.raw,
        quote_pre.raw,
    )?;
    // Rows constrain the executed legs; unrelated owned flows cannot be discarded.
    let owned: HashSet<_> = v
        .pre_tokens
        .as_ref()?
        .iter()
        .chain(v.post_tokens.as_ref()?)
        .filter(|r| r.owner == signer)
        .map(|r| r.index)
        .collect();
    for account in owned {
        if [a[5], a[6]].contains(&account) {
            continue;
        }
        let row = v.row(v.pre_tokens.as_ref()?, account)?;
        let (pre, post) = v.pair(account, signer, &row.mint)?;
        if pre.raw != post.raw {
            return None;
        }
    }
    Some(Trade {
        buy: quote_sol,
        target: v.key(if base_sol { a[4] } else { a[3] })?.to_owned(),
        sol_raw: if base_sol { base } else { debit },
        target_raw: if base_sol { debit } else { base },
        target_decimals: if base_sol {
            quote_pre.decimals
        } else {
            base_pre.decimals
        },
    })
}

fn delta(pre: u64, post: u64) -> i128 {
    i128::from(post) - i128::from(pre)
}
fn checked(ix: &Instruction, accounts: [usize; 4], decimals: u8) -> Option<u64> {
    if ix.program.as_deref()? != TOKEN || ix.accounts.as_deref()? != accounts {
        return None;
    }
    let wire::Op::TransferChecked {
        raw,
        decimals: actual,
    } = wire::decode(TOKEN, ix.data.as_ref()?)?
    else {
        return None;
    };
    (raw > 0 && actual == decimals).then_some(raw)
}
fn fee(
    v: &View,
    ix: &Instruction,
    a: &[usize],
    account: usize,
    owner: usize,
    decimals: u8,
) -> Option<u64> {
    let raw = checked(ix, [a[6], a[4], account, a[1]], decimals)?;
    let (pre, post) = v.pair(account, v.key(owner)?, v.key(a[4])?)?;
    if pre.decimals != decimals
        || delta(pre.raw, post.raw) != i128::from(raw)
        || pda::associated(v.key(owner)?, v.key(a[4])?)?.as_str() != v.key(account)?
    {
        return None;
    }
    Some(raw)
}
