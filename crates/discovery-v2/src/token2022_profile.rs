//! Pure, bounded metadata-only Token-2022 valuation profile; no RPC or app imports.
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt};

pub const PROFILE_VERSION: u8 = 1;
pub const MAX_PROFILE_ACCOUNTS: usize = 128;
pub const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const MAX_MINT_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Token2022RawAccount {
    pub program_owner: String,
    pub context_slot: u64,
    pub executable: bool,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Token2022BoundAccount {
    pub pubkey: String,
    pub raw: Token2022RawAccount,
    pub expected_raw_amount: u64,
    pub expected_decimals: u8,
    pub expected_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Token2022PositionProfile {
    pub contract_version: u8,
    pub wallet: String,
    pub mint: String,
    pub mint_account: Token2022RawAccount,
    pub accounts: Vec<Token2022BoundAccount>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ProfileAmount {
    pub raw_total: u128,
    pub decimals: u8,
    pub amount: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileFailure {
    Missing,
    Malformed,
    Unsupported,
    Frozen,
    Binding,
}

impl fmt::Display for ProfileFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Missing => "token2022_profile_missing",
            Self::Malformed => "token2022_profile_malformed",
            Self::Unsupported => "token2022_profile_unsupported",
            Self::Frozen => "token2022_profile_frozen",
            Self::Binding => "token2022_profile_binding",
        })
    }
}
impl std::error::Error for ProfileFailure {}

/// Reparse saved raw bytes on every call; expected JSON quantities are never sufficient.
/// Collector/revalidator owns observation freshness and RPC account-address ordering.
pub fn validate_profile(
    profile: &Token2022PositionProfile,
    expected_wallet: &str,
    expected_mint: &str,
) -> Result<ProfileAmount, ProfileFailure> {
    use ProfileFailure as F;
    if profile.contract_version != PROFILE_VERSION {
        return Err(F::Unsupported);
    }
    let wallet = decode_key(&profile.wallet)?;
    let mint = decode_key(&profile.mint)?;
    if wallet != decode_key(expected_wallet)? || mint != decode_key(expected_mint)? {
        return Err(F::Binding);
    }
    if profile.accounts.is_empty() {
        return Err(F::Missing);
    }
    if profile.accounts.len() > MAX_PROFILE_ACCOUNTS {
        return Err(F::Unsupported);
    }
    raw_binding(&profile.mint_account)?;
    let decimals = validate_mint(&profile.mint_account.data, &mint)?;
    let mut seen = HashSet::new();
    let mut raw_total = 0u128;
    for account in &profile.accounts {
        if !seen.insert(decode_key(&account.pubkey)?) {
            return Err(F::Binding);
        }
        raw_binding(&account.raw)?;
        let raw = validate_account(&account.raw.data, &mint, &wallet)?;
        if raw != account.expected_raw_amount
            || decimals != account.expected_decimals
            || account.expected_state != "initialized"
        {
            return Err(F::Binding);
        }
        raw_total = raw_total.checked_add(u128::from(raw)).ok_or(F::Malformed)?;
    }
    let amount = raw_total as f64 / 10_f64.powi(i32::from(decimals));
    if raw_total == 0 || !amount.is_finite() || amount <= 0.0 {
        return Err(F::Malformed);
    }
    Ok(ProfileAmount {
        raw_total,
        decimals,
        amount,
    })
}

fn raw_binding(raw: &Token2022RawAccount) -> Result<(), ProfileFailure> {
    if raw.data.is_empty() || raw.context_slot == 0 {
        return Err(ProfileFailure::Missing);
    }
    if raw.program_owner != TOKEN_2022_PROGRAM || raw.executable {
        return Err(ProfileFailure::Binding);
    }
    Ok(())
}

// SPL Pack COption tags are four bytes; unused None payload bytes are ignored by SPL.
fn coption(data: &[u8], at: usize) -> bool {
    matches!(data.get(at..at + 4), Some([0, 0, 0, 0] | [1, 0, 0, 0]))
}

fn validate_mint(data: &[u8], mint: &[u8; 32]) -> Result<u8, ProfileFailure> {
    use ProfileFailure as F;
    if data.len() > MAX_MINT_BYTES || data.len() == 355 {
        return Err(F::Unsupported);
    }
    if data.len() < 82 || !coption(data, 0) || !coption(data, 46) || data[45] != 1 {
        return Err(F::Malformed);
    }
    if data.len() == 82 {
        return Ok(data[44]);
    }
    if data.len() < 167 {
        return Err(F::Malformed);
    }
    if data[82..165].iter().any(|byte| *byte != 0) || data[165] != 1 {
        return Err(F::Malformed);
    }
    let mut offset = 166;
    let (mut pointer, mut metadata) = (false, false);
    while offset < data.len() {
        let header = data.get(offset..offset + 4).ok_or(F::Malformed)?;
        let kind = u16::from_le_bytes([header[0], header[1]]);
        let size = usize::from(u16::from_le_bytes([header[2], header[3]]));
        let body = data
            .get(offset + 4..offset + 4 + size)
            .ok_or(F::Malformed)?;
        match kind {
            18 if !pointer => {
                if size != 64 {
                    return Err(F::Malformed);
                }
                if body[32..] != mint[..] {
                    return Err(F::Binding);
                }
                pointer = true;
            }
            19 if !metadata => {
                crate::token2022_profile_metadata::validate(body, mint)?;
                metadata = true;
            }
            18 | 19 => return Err(F::Malformed),
            _ => return Err(F::Unsupported),
        }
        offset += 4 + size;
    }
    if pointer != metadata {
        return Err(F::Unsupported);
    }
    Ok(data[44])
}

fn validate_account(
    data: &[u8],
    mint: &[u8; 32],
    wallet: &[u8; 32],
) -> Result<u64, ProfileFailure> {
    use ProfileFailure as F;
    if data.len() > 170 {
        return Err(F::Unsupported);
    }
    if data.len() < 165 || !coption(data, 72) || !coption(data, 129) || !coption(data, 109) {
        return Err(F::Malformed);
    }
    if data[..32] != mint[..] || data[32..64] != wallet[..] {
        return Err(F::Binding);
    }
    match data[108] {
        1 => {}
        2 => return Err(F::Frozen),
        _ => return Err(F::Malformed),
    }
    if data[109..113] != [0, 0, 0, 0] {
        return Err(F::Unsupported);
    }
    if data.len() > 165 {
        if data[165] != 2 {
            return Err(F::Malformed);
        }
        let header = data.get(166..170).ok_or(F::Malformed)?;
        let kind = u16::from_le_bytes([header[0], header[1]]);
        let size = usize::from(u16::from_le_bytes([header[2], header[3]]));
        if 170 + size != data.len() {
            return Err(F::Malformed);
        }
        if kind != 7 || size != 0 {
            return Err(F::Unsupported);
        }
    }
    Ok(u64::from_le_bytes(
        data[64..72].try_into().map_err(|_| F::Malformed)?,
    ))
}

/// Strict canonical base58 identity, exactly 32 bytes; no curve or signature claim.
fn decode_key(text: &str) -> Result<[u8; 32], ProfileFailure> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if !(32..=44).contains(&text.len()) {
        return Err(ProfileFailure::Malformed);
    }
    let mut decoded = [0u8; 32];
    for byte in text.bytes() {
        let value = ALPHABET
            .iter()
            .position(|cell| *cell == byte)
            .ok_or(ProfileFailure::Malformed)?;
        let mut carry = value as u16;
        for cell in decoded.iter_mut().rev() {
            carry += u16::from(*cell) * 58;
            *cell = carry as u8;
            carry >>= 8;
        }
        if carry != 0 {
            return Err(ProfileFailure::Malformed);
        }
    }
    let zeros = text.bytes().take_while(|byte| *byte == b'1').count();
    if zeros + decoded.iter().skip_while(|byte| **byte == 0).count() != 32 {
        return Err(ProfileFailure::Malformed);
    }
    Ok(decoded)
}
