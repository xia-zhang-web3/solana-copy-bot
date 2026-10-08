//! Bind the user's quote account independently of its close refund/native cash.
use super::super::{pda, wire, Instruction, View, ATA, SOL_MINT, SYSTEM, TOKEN};
use std::collections::HashSet;

const COMPUTE: &str = "ComputeBudget111111111111111111111111111111";

pub(super) fn prove(
    v: &View,
    parent: usize,
    a: &[usize],
    buy: bool,
    sol: u64,
    trading: &[usize],
    programs: &HashSet<String>,
) -> Option<()> {
    let trader = a[1];
    let quote = a[6];
    let pre = v.row(v.pre_tokens.as_ref()?, quote);
    let post = v.row(v.post_tokens.as_ref()?, quote);
    let mut setup = HashSet::new();
    let mut funding = None;
    let mut sync = None;
    let mut close = None;
    for (i, ix) in v.top.as_ref()?.iter().enumerate() {
        if i == parent {
            continue;
        }
        let accounts = ix.accounts.as_ref()?;
        if accounts.iter().any(|i| v.key(*i).is_none()) || ix.depth.is_some_and(|d| d != 1) {
            return None;
        }
        match ix.program.as_deref()? {
            COMPUTE if accounts.is_empty() && ix.data.is_some() => {}
            ATA => {
                let [payer, account, owner, mint, system, token] = accounts.as_slice() else {
                    return None;
                };
                if *payer != trader
                    || i >= parent
                    || ix.data.as_deref()? != [1]
                    || v.key(*system)? != SYSTEM
                    || v.key(*token)? != TOKEN
                    || pda::associated(v.key(*owner)?, v.key(*mint)?)?.as_str()
                        != v.key(*account)?
                {
                    return None;
                }
                if *account == quote && pre.is_none() {
                    if *owner != trader || v.key(*mint)? != SOL_MINT || !setup.insert(i) {
                        return None;
                    }
                    created_quote(v, i, trader, quote, a[4])?;
                } else {
                    // Existing idempotent ATA instructions may have no CPI. Their
                    // account identity still needs token rows on each present side.
                    let row = v.row(v.pre_tokens.as_ref()?, *account)?;
                    if row.owner != v.key(*owner)?
                        || row.mint != v.key(*mint)?
                        || row.program != TOKEN
                        || *v.pre.as_ref()?.get(*account)? == 0
                        || v.inner
                            .as_ref()?
                            .iter()
                            .any(|(j, g)| *j == i && !g.is_empty())
                    {
                        return None;
                    }
                    if *account != quote {
                        v.pair(*account, v.key(*owner)?, v.key(*mint)?)?;
                    }
                }
            }
            SYSTEM => {
                let wire::Op::Fund(amount) = wire::decode(SYSTEM, ix.data.as_ref()?)? else {
                    return None;
                };
                let [source, destination] = accounts.as_slice() else {
                    return None;
                };
                if source == destination {
                    return None;
                }
                if *destination == quote {
                    if !buy
                        || *source != trader
                        || i >= parent
                        || funding.replace((i, amount)).is_some()
                    {
                        return None;
                    }
                } else if accounts.iter().any(|i| trading.contains(i)) {
                    return None;
                }
                // Other System transfers (e.g. a tip) cannot contribute a swap leg.
            }
            TOKEN => match wire::decode(TOKEN, ix.data.as_ref()?)? {
                wire::Op::Sync if accounts.as_slice() == [quote] => {
                    if i >= parent || sync.replace(i).is_some() {
                        return None;
                    }
                }
                wire::Op::Close if accounts.as_slice() == [quote, trader, trader] => {
                    if i <= parent || close.replace(i).is_some() {
                        return None;
                    }
                }
                _ => return None,
            },
            // No arbitrary top-level wrapper/second DEX gets negative attribution.
            _ => return None,
        }
    }
    for (i, group) in v.inner.as_ref()? {
        if *i != parent && !setup.contains(i) && !group.is_empty() {
            return None;
        }
        if *i != parent
            && group
                .iter()
                .any(|ix| ix.program.as_ref().is_some_and(|p| programs.contains(p)))
        {
            return None;
        }
    }
    let initial = if let Some(row) = pre {
        if row.owner != v.key(trader)?
            || row.mint != SOL_MINT
            || row.program != TOKEN
            || row.decimals != 9
            || v.pre.as_ref()?.get(quote).copied()? <= row.raw
            || !setup.is_empty()
        {
            return None;
        }
        row.raw
    } else {
        if v.pre.as_ref()?.get(quote).copied()? != 0 || setup.len() != 1 {
            return None;
        }
        0
    };
    let funded = funding.map_or(0, |(_, amount)| amount);
    if let Some((fund_i, amount)) = funding {
        if amount == 0
            || sync.is_none_or(|sync_i| fund_i >= sync_i)
            || (pre.is_none() && setup.iter().any(|i| *i >= fund_i))
        {
            return None;
        }
    } else if sync.is_some() {
        return None;
    }
    let available = initial.checked_add(funded)?;
    let remaining = if buy {
        available.checked_sub(sol)?
    } else {
        available.checked_add(sol)?
    };
    if close.is_some() {
        if post.is_some() || v.post.as_ref()?.get(quote).copied()? != 0 {
            return None;
        }
        // `remaining` is the initial WSOL balance plus swap cash minus executed
        // debits; close refunds it and rent. Neither refund is a swap amount.
        let _ = remaining;
    } else {
        let row = post?;
        if row.owner != v.key(trader)?
            || row.mint != SOL_MINT
            || row.program != TOKEN
            || row.decimals != 9
            || row.raw != remaining
            || v.post.as_ref()?.get(quote).copied()? <= row.raw
        {
            return None;
        }
    }
    Some(())
}

fn created_quote(v: &View, top: usize, trader: usize, quote: usize, mint: usize) -> Option<()> {
    let group = &v.inner.as_ref()?.iter().find(|(i, _)| *i == top)?.1;
    if group.len() != 4 || group.iter().any(|i| i.depth != Some(2)) {
        return None;
    }
    if !matches(&group[0], TOKEN, &[mint], &wire::Op::AccountSize)
        || !matches(&group[2], TOKEN, &[quote], &wire::Op::ImmutableOwner)
        || !matches(
            &group[3],
            TOKEN,
            &[quote, mint],
            &wire::Op::Init(v.key(trader)?.to_owned()),
        )
    {
        return None;
    }
    let wire::Op::CreateAccount {
        lamports,
        space,
        owner,
    } = wire::decode(SYSTEM, group[1].data.as_ref()?)?
    else {
        return None;
    };
    if lamports == 0
        || space != 165
        || owner != TOKEN
        || !matches(
            &group[1],
            SYSTEM,
            &[trader, quote],
            &wire::Op::CreateAccount {
                lamports,
                space,
                owner,
            },
        )
    {
        return None;
    }
    Some(())
}

fn matches(ix: &Instruction, program: &str, accounts: &[usize], op: &wire::Op) -> bool {
    ix.program.as_deref() == Some(program)
        && ix.accounts.as_deref() == Some(accounts)
        && ix
            .data
            .as_deref()
            .and_then(|data| wire::decode(program, data))
            .as_ref()
            == Some(op)
}
