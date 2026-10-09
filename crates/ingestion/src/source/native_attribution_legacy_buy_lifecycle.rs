//! Bound existing or newly created user accounts; never price a trade from rent/cash.
use super::super::{pda, wire, Instruction, Row, View, ATA, SOL_MINT, SYSTEM, TOKEN};
use std::collections::HashSet;
const COMPUTE: &str = "ComputeBudget111111111111111111111111111111";

#[allow(clippy::too_many_arguments)]
pub(super) fn prove(
    v: &View,
    parent: usize,
    a: &[usize],
    base: u64,
    debit: u64,
    base_sol: bool,
    base_decimals: u8,
    quote_decimals: u8,
) -> Option<(u64, u64)> {
    let trader = a[1];
    let user_base = a[5];
    let user_quote = a[6];
    let mut setup = HashSet::new();
    let mut created = HashSet::new();
    let mut funding = None;
    let mut sync = None;
    let mut close = None;
    let trading = [
        user_base,
        user_quote,
        a[7],
        a[8],
        a[10],
        a[17],
        a[a.len() - 1],
    ];
    for (i, ix) in v.top.as_ref()?.iter().enumerate() {
        if i == parent {
            continue;
        }
        let accounts = ix.accounts.as_ref()?;
        if accounts.iter().any(|j| v.key(*j).is_none()) || ix.depth.is_some_and(|d| d != 1) {
            return None;
        }
        match ix.program.as_deref()? {
            COMPUTE if accounts.is_empty() && ix.data.is_some() => {}
            ATA => {
                let [payer, account, owner, mint, system, token] = accounts.as_slice() else {
                    return None;
                };
                if *payer != trader
                    || *owner != trader
                    || i >= parent
                    || ix.data.as_deref() != Some(&[1])
                    || ![user_base, user_quote].contains(account)
                    || v.key(*system)? != SYSTEM
                    || v.key(*token)? != TOKEN
                    || v.key(*mint)? != v.key(if *account == user_base { a[3] } else { a[4] })?
                    || pda::associated(v.key(trader)?, v.key(*mint)?)?.as_str()
                        != v.key(*account)?
                {
                    return None;
                }
                if let Some(row) = v.row(v.pre_tokens.as_ref()?, *account) {
                    valid(
                        row,
                        v.key(trader)?,
                        v.key(*mint)?,
                        if *account == user_base {
                            base_decimals
                        } else {
                            quote_decimals
                        },
                    )?;
                    if v.pre.as_ref()?.get(*account).copied()? == 0
                        || v.inner
                            .as_ref()?
                            .iter()
                            .any(|(j, g)| *j == i && !g.is_empty())
                    {
                        return None;
                    }
                } else {
                    if v.pre.as_ref()?.get(*account).copied()? != 0 || !created.insert(*account) {
                        return None;
                    }
                    created_ata(v, i, trader, *account, *mint)?;
                    setup.insert(i);
                }
            }
            SYSTEM => {
                let wire::Op::Fund(amount) = wire::decode(SYSTEM, ix.data.as_deref()?)? else {
                    return None;
                };
                let [from, to] = accounts.as_slice() else {
                    return None;
                };
                if from == to {
                    return None;
                }
                if *to == user_quote {
                    if base_sol
                        || *from != trader
                        || i >= parent
                        || amount == 0
                        || funding.replace((i, amount)).is_some()
                    {
                        return None;
                    }
                } else if accounts.iter().any(|j| trading.contains(j)) {
                    return None;
                }
            }
            TOKEN => match wire::decode(TOKEN, ix.data.as_deref()?)? {
                wire::Op::Sync if !base_sol && accounts.as_slice() == [user_quote] => {
                    if i >= parent || sync.replace(i).is_some() {
                        return None;
                    }
                }
                wire::Op::Close
                    if !base_sol && accounts.as_slice() == [user_quote, trader, trader] =>
                {
                    if i <= parent || close.replace(i).is_some() {
                        return None;
                    }
                }
                _ => return None,
            },
            _ => return None,
        }
    }
    if v.inner
        .as_ref()?
        .iter()
        .any(|(i, g)| *i != parent && !setup.contains(i) && !g.is_empty())
    {
        return None;
    }
    if let Some((fi, _)) = funding {
        if sync.is_none_or(|si| si <= fi)
            || setup.iter().any(|si| {
                *si >= fi
                    && v.top.as_ref().unwrap()[*si].accounts.as_ref().unwrap()[1] == user_quote
            })
        {
            return None;
        }
    } else if sync.is_some() {
        return None;
    }
    let before = |account: usize, mint: usize, decimals: u8| -> Option<u64> {
        match v.row(v.pre_tokens.as_ref()?, account) {
            Some(row) => {
                valid(row, v.key(trader)?, v.key(mint)?, decimals)?;
                if v.key(mint)? == SOL_MINT && v.pre.as_ref()?.get(account).copied()? <= row.raw {
                    return None;
                }
                Some(row.raw)
            }
            None => created.contains(&account).then_some(0),
        }
    };
    let before_base = before(user_base, a[3], base_decimals)?;
    let before_quote =
        before(user_quote, a[4], quote_decimals)?.checked_add(funding.map_or(0, |(_, n)| n))?;
    let after_base = before_base.checked_add(base)?;
    let remaining = before_quote.checked_sub(debit)?;
    let br = v.row(v.post_tokens.as_ref()?, user_base)?;
    valid(br, v.key(trader)?, v.key(a[3])?, base_decimals)?;
    if br.raw != after_base || (base_sol && v.post.as_ref()?.get(user_base).copied()? <= br.raw) {
        return None;
    }
    if close.is_some() {
        if v.row(v.post_tokens.as_ref()?, user_quote).is_some()
            || v.post.as_ref()?.get(user_quote).copied()? != 0
        {
            return None;
        }
        // The refund may include remaining WSOL and rent. It never supplies debit.
        let _ = remaining;
    } else {
        let qr = v.row(v.post_tokens.as_ref()?, user_quote)?;
        valid(qr, v.key(trader)?, v.key(a[4])?, quote_decimals)?;
        if qr.raw != remaining
            || (!base_sol && v.post.as_ref()?.get(user_quote).copied()? <= qr.raw)
        {
            return None;
        }
    }
    Some((before_base, before_quote))
}

fn valid(row: &Row, owner: &str, mint: &str, decimals: u8) -> Option<()> {
    (row.owner == owner && row.mint == mint && row.program == TOKEN && row.decimals == decimals)
        .then_some(())
}
fn created_ata(v: &View, top: usize, trader: usize, account: usize, mint: usize) -> Option<()> {
    let group = &v.inner.as_ref()?.iter().find(|(i, _)| *i == top)?.1;
    if group.len() != 4
        || group.iter().any(|i| i.depth != Some(2))
        || !matches(&group[0], TOKEN, &[mint], &wire::Op::AccountSize)
        || !matches(&group[2], TOKEN, &[account], &wire::Op::ImmutableOwner)
        || !matches(
            &group[3],
            TOKEN,
            &[account, mint],
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
            &[trader, account],
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
fn matches(ix: &Instruction, program: &str, a: &[usize], op: &wire::Op) -> bool {
    ix.program.as_deref() == Some(program)
        && ix.accounts.as_deref() == Some(a)
        && ix
            .data
            .as_deref()
            .and_then(|d| wire::decode(program, d))
            .as_ref()
            == Some(op)
}
