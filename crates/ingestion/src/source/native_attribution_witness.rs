use super::{ata, lifecycle, wire, Instruction, Row, Trade, View, SOL_MINT, TOKEN};
use std::collections::HashSet;

pub(super) fn prove(
    v: &View,
    signer: &str,
    programs: &HashSet<String>,
    index: usize,
    parent: &Instruction,
) -> Option<Trade> {
    if !v.successful || v.first_signer.as_deref()? != signer {
        return None;
    }
    // Positions must be intact and unique in the transaction key table.
    let keys: Vec<_> = v.keys.iter().map(Option::as_deref).collect::<Option<_>>()?;
    if keys.iter().collect::<HashSet<_>>().len() != keys.len() {
        return None;
    }
    let pre = v.pre.as_ref()?;
    let post = v.post.as_ref()?;
    if pre.len() != keys.len() || post.len() != keys.len() {
        return None;
    }
    let a = parent.accounts.as_ref()?;
    if a.len() < 13 || a.iter().any(|i| *i >= keys.len()) {
        return None;
    }
    if v.key(a[1])? != signer
        || v.key(a[3])? != SOL_MINT
        || v.key(a[4])? == SOL_MINT
        || v.key(a[11])? != TOKEN
        || v.key(a[12])? != TOKEN
    {
        return None;
    }
    let roles = [a[0], a[1], a[3], a[4], a[5], a[6], a[7], a[8]];
    if roles.iter().collect::<HashSet<_>>().len() != roles.len() {
        return None;
    }
    if pre[a[5]] != 0 || post[a[5]] != 0 {
        return None;
    }
    let (buy, input, min_out) = wire::swap(parent.data.as_ref()?)?;
    if input == 0 {
        return None;
    }
    let groups = v.inner.as_ref()?;
    let mut group_indices = HashSet::new();
    for (parent_index, instructions) in groups {
        if *parent_index >= v.top.as_ref()?.len() || !group_indices.insert(parent_index) {
            return None;
        }
        for ix in instructions {
            ix.accounts.as_ref()?;
            if ix.program.as_ref().is_some_and(|p| programs.contains(p)) {
                return None;
            }
        }
    }
    let transfers = &groups.iter().find(|(i, _)| *i == index)?.1;
    // Fees or any third CPI are outside this first two-transfer subset.
    if transfers.len() != 2 {
        return None;
    }
    let sol = transfer(
        &transfers[0],
        if buy {
            [a[5], a[7], a[1]]
        } else {
            [a[7], a[5], a[0]]
        },
    )?;
    let target = transfer(
        &transfers[1],
        if buy {
            [a[8], a[6], a[0]]
        } else {
            [a[6], a[8], a[1]]
        },
    )?;
    if (if buy { sol } else { target }) != input || (if buy { target } else { sol }) < min_out {
        return None;
    }
    let (pool_pre, pool_post) = v.pair(a[7], v.key(a[0])?, SOL_MINT)?;
    let sol_delta = if buy {
        i128::from(sol)
    } else {
        -i128::from(sol)
    };
    if pool_pre.decimals != 9
        || delta(pool_pre, pool_post) != sol_delta
        || i128::from(post[a[7]]) - i128::from(pre[a[7]]) != sol_delta
    {
        return None;
    }
    let mint = v.key(a[4])?;
    let (user_pre, user_post) = v.pair(a[6], signer, mint)?;
    let (quote_pre, quote_post) = v.pair(a[8], v.key(a[0])?, mint)?;
    let target_delta = if buy {
        i128::from(target)
    } else {
        -i128::from(target)
    };
    if delta(user_pre, user_post) != target_delta
        || delta(quote_pre, quote_post) != -target_delta
        || user_pre.decimals != quote_pre.decimals
    {
        return None;
    }
    // Reject duplicate/inconsistent accountIndex rows, including rows hidden by mint aggregation.
    for rows in [v.pre_tokens.as_ref()?, v.post_tokens.as_ref()?] {
        let mut seen = HashSet::new();
        for row in rows {
            if row.index >= keys.len() || !seen.insert(row.index) || row.index == a[5] {
                return None;
            }
        }
    }
    // No other owned trading-token flow can be silently discarded by this attribution.
    let owned: HashSet<_> = v
        .pre_tokens
        .as_ref()?
        .iter()
        .chain(v.post_tokens.as_ref()?)
        .filter(|r| r.owner == signer)
        .map(|r| r.index)
        .collect();
    for account in owned {
        if account == a[6] {
            continue;
        }
        let pre = v.pre_tokens.as_ref()?.iter().find(|r| r.index == account);
        let post = v.post_tokens.as_ref()?.iter().find(|r| r.index == account);
        if pre.is_some_and(|r| r.raw != 0) || post.is_some_and(|r| r.raw != 0) {
            let (pre, post) = (pre?, post?);
            if pre.owner != signer
                || post.owner != signer
                || pre.mint != post.mint
                || pre.decimals != post.decimals
                || delta(pre, post) != 0
            {
                return None;
            }
        }
    }
    for (i, ix) in v.top.as_ref()?.iter().enumerate() {
        if i != index && !other_token_operation(ix) {
            return None;
        }
    }
    // A missing lifecycle proof is terminal; failed ATA decoding cannot restore
    // legacy native inference. Only the specifically proved setup group is exempt.
    let setup = lifecycle::prove(v, index, a, buy, sol)
        .map(|_| None)
        .or_else(|| ata::prove(v, index, a, buy, sol).map(Some))?;
    for (parent_index, instructions) in groups {
        if *parent_index == index || Some(*parent_index) == setup {
            continue;
        }
        for ix in instructions {
            if ix
                .accounts
                .as_ref()?
                .iter()
                .any(|i| [a[5], a[6], a[7], a[8]].contains(i))
                || !other_token_operation(ix)
            {
                return None;
            }
        }
    }
    Some(Trade {
        buy,
        target: mint.to_owned(),
        sol_raw: sol,
        target_raw: target,
        target_decimals: user_pre.decimals,
    })
}

fn delta(pre: &Row, post: &Row) -> i128 {
    i128::from(post.raw) - i128::from(pre.raw)
}

fn transfer(ix: &Instruction, expected: [usize; 3]) -> Option<u64> {
    if ix.program.as_deref()? != TOKEN
        || ix.accounts.as_ref()?.as_slice() != expected
        || ix.depth.is_some_and(|depth| depth != 2)
    {
        return None;
    }
    let wire::Op::Transfer(raw) = wire::decode(TOKEN, ix.data.as_ref()?)? else {
        return None;
    };
    (raw > 0).then_some(raw)
}

// Other rent lifecycles are allowed; further token transfers/unknown token opcodes
// require broader attribution. In particular, a fee cannot hide on another account.
fn other_token_operation(ix: &Instruction) -> bool {
    if ix.program.as_deref() != Some(TOKEN) {
        return true;
    }
    matches!(
        ix.data.as_deref().and_then(|d| wire::decode(TOKEN, d)),
        Some(wire::Op::Init(_) | wire::Op::Close | wire::Op::Sync)
    )
}
