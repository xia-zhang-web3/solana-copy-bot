//! Wire subset from cached Solana SystemInstruction and classic SPL TokenInstruction.
use super::{SYSTEM, TOKEN};

#[derive(Debug, PartialEq)]
pub(super) enum Op {
    Create {
        base: String,
        seed: String,
        lamports: u64,
        space: u64,
        owner: String,
    },
    CreateAccount {
        lamports: u64,
        space: u64,
        owner: String,
    },
    AccountSize,
    ImmutableOwner,
    Fund(u64),
    Init(String),
    Sync,
    Close,
    Transfer(u64),
}

pub(super) fn u64_at(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset.checked_add(8)?)?.try_into().ok()?,
    ))
}

pub(super) fn swap(data: &[u8]) -> Option<(bool, u64, u64)> {
    let buy = if data.len() == 24 && data.starts_with(&[51, 230, 133, 164, 1, 127, 131, 173]) {
        true
    } else if data.len() == 25
        && data.starts_with(&[198, 46, 21, 82, 180, 217, 232, 112])
        && data[24] <= 1
    {
        false
    } else {
        return None;
    };
    Some((buy, u64_at(data, 8)?, u64_at(data, 16)?))
}

pub(super) fn decode(program: &str, data: &[u8]) -> Option<Op> {
    if program == SYSTEM {
        match data.get(..4)? {
            [0, 0, 0, 0] if data.len() == 52 => Some(Op::CreateAccount {
                lamports: u64_at(data, 4)?,
                space: u64_at(data, 12)?,
                owner: bs58::encode(&data[20..52]).into_string(),
            }),
            [2, 0, 0, 0] if data.len() == 12 => Some(Op::Fund(u64_at(data, 4)?)),
            [3, 0, 0, 0] => {
                let n = usize::try_from(u64_at(data, 36)?).ok()?;
                if n > 32 || data.len() != 92 + n {
                    return None;
                }
                Some(Op::Create {
                    base: bs58::encode(data.get(4..36)?).into_string(),
                    seed: std::str::from_utf8(data.get(44..44 + n)?).ok()?.to_owned(),
                    lamports: u64_at(data, 44 + n)?,
                    space: u64_at(data, 52 + n)?,
                    owner: bs58::encode(data.get(60 + n..92 + n)?).into_string(),
                })
            }
            _ => None,
        }
    } else if program == TOKEN {
        match data {
            [21, 7, 0] => Some(Op::AccountSize),
            [22] => Some(Op::ImmutableOwner),
            [3, ..] if data.len() == 9 => Some(Op::Transfer(u64_at(data, 1)?)),
            [18, ..] if data.len() == 33 => Some(Op::Init(bs58::encode(&data[1..]).into_string())),
            [17] => Some(Op::Sync),
            [9] => Some(Op::Close),
            _ => None,
        }
    } else {
        None
    }
}

pub(super) fn amount(
    raw: &str,
    decimals: u64,
    ui_string: Option<&str>,
    ui: Option<f64>,
) -> Option<(u64, u8)> {
    if raw.is_empty() || !raw.bytes().all(|c| c.is_ascii_digit()) || decimals > 18 {
        return None;
    }
    let raw = raw.parse::<u64>().ok()?;
    let expected = raw as f64 / 10f64.powi(decimals as i32);
    let consistent = |v: f64| {
        v.is_finite() && v >= 0.0 && (v - expected).abs() <= expected.abs().max(1.0) * 1e-12
    };
    if let Some(s) = ui_string {
        if !consistent(s.parse().ok()?) {
            return None;
        }
    }
    if ui.is_some_and(|n| !consistent(n)) {
        return None;
    }
    Some((raw, decimals as u8))
}
