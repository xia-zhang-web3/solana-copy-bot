//! Direct PumpSwap quote-WSOL proof. Amounts come from executed checked CPI legs.
use super::{wire, Instruction, Row, Trade, View, ATA, SOL_MINT, SYSTEM, TOKEN};
use std::collections::HashSet;

#[path = "native_attribution_quote_sol_lifecycle.rs"]
mod lifecycle;

const FEES: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";

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
    let keys = v
        .keys
        .iter()
        .map(Option::as_deref)
        .collect::<Option<Vec<_>>>()?;
    if keys.iter().collect::<HashSet<_>>().len() != keys.len()
        || v.pre.as_ref()?.len() != keys.len()
        || v.post.as_ref()?.len() != keys.len()
        || parent.depth.is_some_and(|depth| depth != 1)
    {
        return None;
    }
    let a = parent.accounts.as_ref()?;
    // The old witness's bool is a base-SOL direction. Quote-SOL reverses it.
    let (base_to_quote, input_limit, output_limit) = wire::swap(parent.data.as_ref()?)?;
    let buy = !base_to_quote;
    if a.len() != if buy { 26 } else { 24 }
        || a.iter().any(|i| *i >= keys.len())
        || v.key(a[1])? != signer
        || v.key(a[3])? == SOL_MINT
        || v.key(a[4])? != SOL_MINT
        || v.key(a[11])? != TOKEN
        || v.key(a[12])? != TOKEN
        || v.key(a[13])? != SYSTEM
        || v.key(a[14])? != ATA
        || v.key(a[16])? != parent.program.as_deref()?
    {
        return None;
    }
    let (fee_config, fee_program, creator_owner, creator_account) = if buy {
        (a[21], a[22], a[24], a[25])
    } else {
        (a[19], a[20], a[22], a[23])
    };
    if v.key(fee_program)? != FEES {
        return None;
    }
    let trading = [a[5], a[6], a[7], a[8], a[10], a[17], creator_account];
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
        creator_account,
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
    let groups = v.inner.as_ref()?;
    let mut indices = HashSet::new();
    if groups
        .iter()
        .any(|(i, _)| *i >= v.top.as_ref().map_or(0, Vec::len) || !indices.insert(i))
    {
        return None;
    }
    let group = &groups.iter().find(|(i, _)| *i == index)?.1;
    // One fee query, two swap legs, three fee legs and the event CPI. Unknown
    // children, nested wrappers and repeated legs never become an exact amount.
    if group.len() != 7 || group.iter().any(|ix| ix.depth != Some(2)) {
        return None;
    }
    let query = &group[0];
    if query.program.as_deref()? != FEES
        || query.accounts.as_deref()? != [fee_config, a[16]]
        || query.data.as_ref()?.len() != 57
        || !query
            .data
            .as_ref()?
            .starts_with(&[154, 237, 138, 92, 162, 2, 162, 187])
    {
        return None;
    }
    let event = &group[6];
    if event.program != parent.program
        || event.accounts.as_deref()? != [a[15]]
        || event.data.as_ref()?.len() != if buy { 512 } else { 449 }
        || !event
            .data
            .as_ref()?
            .starts_with(&[228, 69, 165, 46, 81, 203, 154, 29])
    {
        return None;
    }
    let mint = v.key(a[3])?;
    let (user_pre, user_post) = v.pair(a[5], signer, mint)?;
    let (base_pre, base_post) = v.pair(a[7], v.key(a[0])?, mint)?;
    if user_pre.decimals != base_pre.decimals {
        return None;
    }
    let target = checked(
        &group[1],
        if buy {
            [a[7], a[3], a[5], a[0]]
        } else {
            [a[5], a[3], a[7], a[1]]
        },
        user_pre.decimals,
    )?;
    let quote = checked(
        &group[2],
        if buy {
            [a[6], a[4], a[8], a[1]]
        } else {
            [a[8], a[4], a[6], a[0]]
        },
        9,
    )?;
    let mut fees = 0u64;
    for (ix, (account, owner)) in group[3..6].iter().zip([
        (a[10], a[9]),
        (a[17], a[18]),
        (creator_account, creator_owner),
    ]) {
        let amount = checked(
            ix,
            [
                if buy { a[6] } else { a[8] },
                a[4],
                account,
                if buy { a[1] } else { a[0] },
            ],
            9,
        )?;
        let (pre, post) = v.pair(account, v.key(owner)?, SOL_MINT)?;
        if pre.decimals != 9 || delta(pre, post) != i128::from(amount) {
            return None;
        }
        fees = fees.checked_add(amount)?;
    }
    // BUY includes the user's executed quote fees. SELL excludes pool-paid fees.
    let sol = if buy { quote.checked_add(fees)? } else { quote };
    let (pool_pre, pool_post) = v.pair(a[8], v.key(a[0])?, SOL_MINT)?;
    let pool_change = if buy {
        i128::from(quote)
    } else {
        -i128::from(quote.checked_add(fees)?)
    };
    let target_change = if buy {
        i128::from(target)
    } else {
        -i128::from(target)
    };
    // Token-balance changes are integrity checks, never the source of quantities.
    if pool_pre.decimals != 9
        || delta(pool_pre, pool_post) != pool_change
        || delta(user_pre, user_post) != target_change
        || delta(base_pre, base_post) != -target_change
        || (if buy { sol } else { target }) != input_limit
        || (if buy { target } else { sol }) < output_limit
    {
        return None;
    }
    unchanged_owned(v, signer, a[5], a[6])?;
    lifecycle::prove(v, index, a, buy, sol, &trading, programs)?;
    Some(Trade {
        buy,
        target: mint.to_owned(),
        sol_raw: sol,
        target_raw: target,
        target_decimals: user_pre.decimals,
    })
}

fn checked(ix: &Instruction, expected: [usize; 4], decimals: u8) -> Option<u64> {
    if ix.program.as_deref()? != TOKEN || ix.accounts.as_deref()? != expected {
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

fn delta(pre: &Row, post: &Row) -> i128 {
    i128::from(post.raw) - i128::from(pre.raw)
}

fn unchanged_owned(v: &View, signer: &str, base: usize, quote: usize) -> Option<()> {
    let owned: HashSet<_> = v
        .pre_tokens
        .as_ref()?
        .iter()
        .chain(v.post_tokens.as_ref()?)
        .filter(|r| r.owner == signer)
        .map(|r| r.index)
        .collect();
    for index in owned {
        if [base, quote].contains(&index) {
            continue;
        }
        let pre = v.row(v.pre_tokens.as_ref()?, index)?;
        let post = v.row(v.post_tokens.as_ref()?, index)?;
        if pre.owner != signer
            || post.owner != signer
            || pre.program != TOKEN
            || post.program != TOKEN
            || pre.mint != post.mint
            || pre.decimals != post.decimals
            || delta(pre, post) != 0
        {
            return None;
        }
    }
    Some(())
}
