//! BuyEvent497: cached pump_amm IDL fields cross-check the executed CPI witness.
//! The final eight extension bytes are opaque telemetry, never amounts or authority.
use super::super::{pda, wire, Instruction, View, SYSTEM};

#[allow(clippy::too_many_arguments)]
pub(super) fn prove(
    v: &View,
    a: &[usize],
    ix: &Instruction,
    base: u64,
    quote: u64,
    protocol: u64,
    creator: u64,
    buyback: u64,
    debit: u64,
    max_quote: u64,
    track: Option<bool>,
    balances: (u64, u64),
    pool_base: u64,
    pool_quote: u64,
) -> Option<()> {
    let data = ix.data.as_deref()?;
    if ix.program.as_deref()? != v.key(a[16])?
        || ix.accounts.as_deref()? != [a[15]]
        || data.len() != 497
        || !data.starts_with(&[
            228, 69, 165, 46, 81, 203, 154, 29, 103, 244, 82, 31, 44, 245, 119, 119,
        ])
        || data[368] > 1
        || track.is_some_and(|b| b != (data[368] == 1))
        || data.get(409..416)? != [3, 0, 0, 0, b'b', b'u', b'y']
        || data[464] > 1
    {
        return None;
    }
    for (offset, role) in [(128, 0), (160, 1), (192, 5), (224, 6), (256, 9), (288, 10)] {
        if key(data, offset)? != v.key(a[role])? {
            return None;
        }
    }
    let coin_creator = key(data, 320)?;
    if creator == 0 {
        if coin_creator != SYSTEM {
            return None;
        }
    } else if pda::creator_vault(v.key(a[16])?, &coin_creator)?.as_str() != v.key(a[18])? {
        return None;
    }
    for (offset, expected) in [
        (24, base),
        (32, max_quote),
        (40, balances.0),
        (48, balances.1),
        (56, pool_base),
        (64, pool_quote),
        (104, protocol.checked_add(buyback)?),
        (112, quote),
        (120, debit),
        (360, creator),
        (401, base),
        (424, 0),
        (440, buyback),
    ] {
        if wire::u64_at(data, offset)? != expected {
            return None;
        }
    }
    if wire::u64_at(data, 72)?.checked_add(wire::u64_at(data, 88)?)? != quote {
        return None;
    }
    Some(())
}

fn key(data: &[u8], offset: usize) -> Option<String> {
    Some(bs58::encode(data.get(offset..offset.checked_add(32)?)?).into_string())
}
