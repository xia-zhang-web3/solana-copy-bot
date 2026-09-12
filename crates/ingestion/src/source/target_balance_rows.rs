//! Completeness gate for owned non-WSOL rows, before mint aggregation or fallback.
use super::SOL_MINT;
use serde_json::Value;
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::{TokenBalance, TransactionStatusMeta};

struct Row<'a> {
    index: Option<usize>,
    owner: Option<&'a str>,
    mint: Option<&'a str>,
}

pub(super) fn json(meta: &Value, signer: &str) -> Option<()> {
    let rows = |field| {
        meta.get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|r| Row {
                index: r
                    .get("accountIndex")
                    .and_then(Value::as_u64)
                    .and_then(|i| usize::try_from(i).ok()),
                owner: r.get("owner").and_then(Value::as_str),
                mint: r.get("mint").and_then(Value::as_str),
            })
            .collect::<Vec<_>>()
    };
    check(
        &rows("preTokenBalances"),
        &rows("postTokenBalances"),
        signer,
        |index| {
            let balance = |field| meta.get(field)?.as_array()?.get(index)?.as_u64();
            Some((balance("preBalances")?, balance("postBalances")?))
        },
    )
}

pub(super) fn protobuf(meta: &TransactionStatusMeta, signer: &str) -> Option<()> {
    fn rows(items: &[TokenBalance]) -> Vec<Row<'_>> {
        items
            .iter()
            .map(|r| Row {
                index: usize::try_from(r.account_index).ok(),
                owner: Some(r.owner.as_str()),
                mint: Some(r.mint.as_str()),
            })
            .collect::<Vec<_>>()
    }
    check(
        &rows(&meta.pre_token_balances),
        &rows(&meta.post_token_balances),
        signer,
        |index| {
            Some((
                *meta.pre_balances.get(index)?,
                *meta.post_balances.get(index)?,
            ))
        },
    )
}

fn check(
    pre: &[Row<'_>],
    post: &[Row<'_>],
    signer: &str,
    native: impl Fn(usize) -> Option<(u64, u64)>,
) -> Option<()> {
    let mut needed = HashMap::new();
    for row in pre.iter().chain(post) {
        if row.owner == Some(signer) && row.mint != Some(SOL_MINT) {
            let index = row.index?;
            let mint = row.mint.filter(|m| !m.is_empty())?;
            if let Some((other, _)) = needed.insert(index, (mint, [false; 2])) {
                if other != mint {
                    return None;
                }
            }
        }
    }
    // Only inspect identities at required indexes. Unrelated foreign rows do not
    // become global prerequisites; duplicates/conflicts at an owned index do.
    for (side, rows) in [pre, post].into_iter().enumerate() {
        for row in rows {
            let Some(pair) = row.index.and_then(|i| needed.get_mut(&i)) else {
                continue;
            };
            if row.owner != Some(signer) || row.mint != Some(pair.0) || pair.1[side] {
                return None;
            }
            pair.1[side] = true;
        }
    }
    for (index, (_, seen)) in needed {
        if seen != [true; 2] {
            // A missing row is not zero inventory. A native zero endpoint only
            // preserves the previous policy; it does not prove a lifecycle.
            let (before, after) = native(index)?;
            if before > 0 && after > 0 {
                return None;
            }
        }
    }
    Some(())
}
