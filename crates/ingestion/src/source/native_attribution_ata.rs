//! Bounded classic ATA creation proof; only its exact CPI group can be exempted.
use super::{
    lifecycle, pda,
    wire::{self, Op},
    Instruction, View, ATA, SOL_MINT, SYSTEM, TOKEN,
};

pub(super) fn prove(v: &View, parent: usize, a: &[usize], buy: bool, sol: u64) -> Option<usize> {
    profile(v, parent, a, buy, sol, false).map(|proof| proof.0)
}

/// Jupiter can create its target ATA during the first purchase. Unlike an
/// absent balance default, this requires the full classic creation witness.
pub(super) fn prove_jupiter(
    v: &View,
    parent: usize,
    a: &[usize],
    sol: u64,
) -> Option<(usize, Option<usize>)> {
    profile(v, parent, a, true, sol, true)
}

fn profile(
    v: &View,
    parent: usize,
    a: &[usize],
    buy: bool,
    sol: u64,
    new_target: bool,
) -> Option<(usize, Option<usize>)> {
    let top = v.top.as_ref()?;
    if !depth_matches(top.get(parent)?, 1) {
        return None;
    }
    let temp = a[5];
    let trader = a[1];
    let mut steps = Vec::new();
    let mut target_ata = None;
    let mut target_creation = None;
    // R1: validate binding before treating a System/SPL instruction as unrelated.
    for (index, ix) in top.iter().enumerate() {
        if index == parent {
            continue;
        }
        let keys = ix.accounts.as_ref()?;
        if !lifecycle::ata_compatible_operation(v, ix, temp) {
            return None;
        }
        if keys.iter().any(|i| [a[5], a[6], a[7], a[8]].contains(i)) {
            // Existing targets need paired rows and no creation CPI. Jupiter's
            // first target is accepted only with the full classic creation group.
            if keys.contains(&a[6]) {
                if target_ata.replace(index).is_some()
                    || !buy
                    || index >= parent
                    || !identity(v, ix, trader, a[6], a[4])
                {
                    return None;
                }
                if *v.pre.as_ref()?.get(a[6])? == 0 {
                    if !new_target || v.pre_tokens.as_ref()?.iter().any(|r| r.index == a[6]) {
                        return None;
                    }
                    let rent = creation(v, index, trader, a[6], a[4])?;
                    let post = v.row(v.post_tokens.as_ref()?, a[6])?;
                    if post.owner != v.key(trader)?
                        || post.mint != v.key(a[4])?
                        || post.program != TOKEN
                        || *v.post.as_ref()?.get(a[6])? != rent
                    {
                        return None;
                    }
                    target_creation = Some(index);
                } else {
                    if v.pre.as_ref()?.get(a[6])? != v.post.as_ref()?.get(a[6])?
                        || v.inner
                            .as_ref()?
                            .iter()
                            .any(|(i, g)| *i == index && !g.is_empty())
                    {
                        return None;
                    }
                    v.pair(a[6], v.key(trader)?, v.key(a[4])?)?;
                }
                continue;
            }
            if keys.iter().any(|i| [a[6], a[7], a[8]].contains(i)) {
                return None;
            }
            steps.push((index, ix));
        }
    }
    if steps.len() != if buy { 4 } else { 2 } {
        return None;
    }
    let (create_index, create) = steps[0];
    let (close_index, close) = *steps.last()?;
    if !(create_index < parent && parent < close_index)
        || !identity(v, create, trader, temp, a[3])
        || v.key(a[3])? != SOL_MINT
        || *v.pre.as_ref()?.get(temp)? != 0
        || *v.post.as_ref()?.get(temp)? != 0
        || !matches_ix(close, 1, TOKEN, &[temp, trader, trader], &Op::Close)
    {
        return None;
    }
    // Pump keeps its persistent target rule. Only the explicitly proven Jupiter
    // creation can supply a new target; a closed target is always unsupported.
    if target_creation.is_none()
        && (*v.pre.as_ref()?.get(a[6])? == 0
            || v.pre.as_ref()?.get(a[6])? != v.post.as_ref()?.get(a[6])?)
    {
        return None;
    }
    if buy {
        let (fund_i, fund) = steps[1];
        let (sync_i, sync) = steps[2];
        if !(create_index < fund_i && fund_i < sync_i && sync_i < parent)
            || !matches_ix(fund, 1, SYSTEM, &[trader, temp], &Op::Fund(sol))
            || !matches_ix(sync, 1, TOKEN, &[temp], &Op::Sync)
        {
            return None;
        }
    }
    creation(v, create_index, trader, temp, a[3])?;
    Some((create_index, target_creation))
}

fn creation(v: &View, index: usize, trader: usize, account: usize, mint: usize) -> Option<u64> {
    let mut groups = v.inner.as_ref()?.iter().filter(|(i, _)| *i == index);
    let (_, group) = groups.next()?;
    if groups.next().is_some() || group.len() != 4 {
        return None;
    }
    if !matches_ix(&group[0], 2, TOKEN, &[mint], &Op::AccountSize)
        || !matches_ix(&group[2], 2, TOKEN, &[account], &Op::ImmutableOwner)
        || !matches_ix(
            &group[3],
            2,
            TOKEN,
            &[account, mint],
            &Op::Init(v.key(trader)?.to_owned()),
        )
    {
        return None;
    }
    let Op::CreateAccount {
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
        || !matches_ix(
            &group[1],
            2,
            SYSTEM,
            &[trader, account],
            &Op::CreateAccount {
                lamports,
                space,
                owner,
            },
        )
    {
        return None;
    }
    Some(lamports)
}

fn identity(v: &View, ix: &Instruction, trader: usize, account: usize, mint: usize) -> bool {
    let Some(k) = &ix.accounts else {
        return false;
    };
    depth_matches(ix, 1)
        && k.len() == 6
        && k[..4] == [trader, account, trader, mint]
        && v.key(k[4]) == Some(SYSTEM)
        && v.key(k[5]) == Some(TOKEN)
        && ix.program.as_deref() == Some(ATA)
        && ix.data.as_deref() == Some(&[1])
        && v.key(trader)
            .zip(v.key(mint))
            .and_then(|(o, m)| pda::associated(o, m))
            .as_deref()
            == v.key(account)
}

// Missing/null metadata is compatible with older JSON and protobuf top-levels.
fn depth_matches(ix: &Instruction, expected: u32) -> bool {
    !ix.depth.is_some_and(|depth| depth != expected)
}

fn matches_ix(ix: &Instruction, depth: u32, program: &str, accounts: &[usize], op: &Op) -> bool {
    ix.program.as_deref() == Some(program)
        && ix.accounts.as_deref() == Some(accounts)
        && depth_matches(ix, depth)
        && ix
            .data
            .as_deref()
            .and_then(|d| wire::decode(program, d))
            .as_ref()
            == Some(op)
}
