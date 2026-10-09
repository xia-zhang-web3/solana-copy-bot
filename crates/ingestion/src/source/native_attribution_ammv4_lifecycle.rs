//! Establish quote-account identity and isolate setup/funding/close from swap legs.
use super::super::{pda, wire, Instruction, View, ATA, SOL_MINT, SYSTEM, TOKEN};
use std::collections::HashSet;
const COMPUTE: &str = "ComputeBudget111111111111111111111111111111";
const RENT: &str = "SysvarRent111111111111111111111111111111111";

pub(super) fn prove(
    v: &View,
    parent: usize,
    trader: usize,
    quote: usize,
    target: usize,
    buy: bool,
    sol: u64,
    trading: &[usize],
) -> Option<()> {
    let pre = v.row(v.pre_tokens.as_ref()?, quote);
    let post = v.row(v.post_tokens.as_ref()?, quote);
    for row in [pre, post].into_iter().flatten() {
        if row.owner != v.key(trader)?
            || row.mint != SOL_MINT
            || row.program != TOKEN
            || row.decimals != 9
        {
            return None;
        }
    }
    let mut created = None;
    let mut initialized = None;
    let mut funded = None;
    let mut sync = None;
    let mut close = None;
    let mut setup = HashSet::new();
    for (i, ix) in v.top.as_ref()?.iter().enumerate() {
        if i == parent {
            continue;
        }
        let a = ix.accounts.as_ref()?;
        if a.iter().any(|j| v.key(*j).is_none()) || ix.depth.is_some_and(|d| d != 1) {
            return None;
        }
        match ix.program.as_deref()? {
            COMPUTE if a.is_empty() && ix.data.is_some() => {}
            ATA => {
                if a.get(1) == Some(&target) {
                    // Only a proved existing target ATA may be a harmless noop.
                    // Never accept account creation or transfer CPI as this witness.
                    if !buy || i >= parent {
                        return None;
                    }
                    existing_target_ata(v, i, ix, trader, target)?;
                    continue;
                }
                if !matches!(a.len(), 6 | 7)
                    || !matches!(ix.data.as_deref()?, [] | [0] | [1])
                    || a[0] != trader
                    || a[1] != quote
                    || a[2] != trader
                    || v.key(a[3])? != SOL_MINT
                    || v.key(a[4])? != SYSTEM
                    || v.key(a[5])? != TOKEN
                    || (a.len() == 7 && v.key(a[6])? != RENT)
                    || i >= parent
                    || pda::associated(v.key(trader)?, SOL_MINT)?.as_str() != v.key(quote)?
                {
                    return None;
                }
                let group = v
                    .inner
                    .as_ref()?
                    .iter()
                    .find(|(j, _)| *j == i)
                    .map(|(_, g)| g.as_slice())
                    .unwrap_or_default();
                if pre.is_some() && group.is_empty() && ix.data.as_deref()? == [1] {
                    continue;
                }
                created_ata(v, i, trader, quote, a[3])?;
                if pre.is_some() || created.replace(i).is_some() || initialized.replace(i).is_some()
                {
                    return None;
                }
                setup.insert(i);
            }
            SYSTEM => match wire::decode(SYSTEM, ix.data.as_deref()?)? {
                wire::Op::CreateAccount {
                    lamports,
                    space,
                    owner,
                } if a.as_slice() == [trader, quote] => {
                    if lamports == 0
                        || space != 165
                        || owner != TOKEN
                        || pre.is_some()
                        || i >= parent
                        || created.replace(i).is_some()
                    {
                        return None;
                    }
                }
                wire::Op::Fund(amount) if a.len() == 2 && a[0] == trader && a[1] != trader => {
                    if a[1] == quote {
                        if i >= parent || funded.replace((i, amount)).is_some() {
                            return None;
                        }
                    } else if a.iter().any(|a| trading.contains(a)) {
                        return None;
                    }
                    // External tips remain separate. Their amount cannot enter a swap leg.
                }
                _ => return None,
            },
            TOKEN => {
                if ix.data.as_deref()? == [1] && a.len() == 4 {
                    if a[0] != quote
                        || v.key(a[1])? != SOL_MINT
                        || a[2] != trader
                        || v.key(a[3])? != RENT
                        || i >= parent
                        || created.is_none_or(|c| c >= i)
                        || initialized.replace(i).is_some()
                    {
                        return None;
                    }
                } else {
                    match wire::decode(TOKEN, ix.data.as_deref()?)? {
                        wire::Op::Sync if a.as_slice() == [quote] => {
                            if i >= parent || sync.replace(i).is_some() {
                                return None;
                            }
                        }
                        wire::Op::Close if a.as_slice() == [quote, trader, trader] => {
                            if i <= parent || close.replace(i).is_some() {
                                return None;
                            }
                        }
                        _ => return None,
                    }
                }
            }
            _ => return None,
        }
    }
    for (i, g) in v.inner.as_ref()? {
        if *i != parent && !setup.contains(i) && !g.is_empty() {
            return None;
        }
    }
    if let Some(row) = pre {
        if created.is_some() || initialized.is_some() || *v.pre.as_ref()?.get(quote)? <= row.raw {
            return None;
        }
    } else if created.is_none() || initialized.is_none() || *v.pre.as_ref()?.get(quote)? != 0 {
        return None;
    }
    if let Some((fund_i, _)) = funded {
        if created.is_some_and(|c| c > fund_i) {
            return None;
        }
        // InitializeAccount folds prior funding and the creation surplus into WSOL.
        // Funding after initialization requires SyncNative. Neither supplies swap raw.
        if initialized.is_none_or(|i| i <= fund_i) && sync.is_none_or(|i| i <= fund_i) {
            return None;
        }
    } else if sync.is_some() {
        return None;
    }
    if close.is_some() {
        if post.is_some() || *v.post.as_ref()?.get(quote)? != 0 {
            return None;
        }
        // A closed temporary account may refund rent AND setup/initial WSOL.
        // The executed CPI above is the sole amount source; no refund is summed.
    } else {
        let pre = pre?;
        let post = post?;
        let change = if buy {
            -i128::from(sol)
        } else {
            i128::from(sol)
        };
        if i128::from(post.raw) - i128::from(pre.raw)
            != change + i128::from(funded.map_or(0, |(_, n)| n))
            || *v.post.as_ref()?.get(quote)? <= post.raw
        {
            return None;
        }
    }
    Some(())
}

fn existing_target_ata(
    v: &View,
    top: usize,
    ix: &Instruction,
    trader: usize,
    target: usize,
) -> Option<()> {
    let a = ix.accounts.as_ref()?;
    if !matches!(a.len(), 6 | 7)
        || ix.data.as_deref()? != [1]
        || a[0] != trader
        || a[1] != target
        || a[2] != trader
        || v.key(a[4])? != SYSTEM
        || v.key(a[5])? != TOKEN
        || (a.len() == 7 && v.key(a[6])? != RENT)
    {
        return None;
    }
    let mint = v.key(a[3])?;
    let (pre, _) = v.pair(target, v.key(trader)?, mint)?;
    if mint == SOL_MINT
        || pre.decimals > 18
        || pda::associated(v.key(trader)?, mint)?.as_str() != v.key(target)?
        || v.inner
            .as_ref()?
            .iter()
            .any(|(i, group)| *i == top && !group.is_empty())
    {
        return None;
    }
    Some(())
}

fn created_ata(v: &View, top: usize, trader: usize, quote: usize, mint: usize) -> Option<()> {
    let group = &v.inner.as_ref()?.iter().find(|(i, _)| *i == top)?.1;
    if group.len() != 4
        || group.iter().any(|i| i.depth != Some(2))
        || !matches(&group[0], TOKEN, &[mint], &wire::Op::AccountSize)
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
    } = wire::decode(SYSTEM, group[1].data.as_deref()?)?
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
fn matches(ix: &Instruction, p: &str, a: &[usize], op: &wire::Op) -> bool {
    ix.program.as_deref() == Some(p)
        && ix.accounts.as_deref() == Some(a)
        && ix.data.as_deref().and_then(|d| wire::decode(p, d)).as_ref() == Some(op)
}
