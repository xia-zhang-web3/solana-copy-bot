//! Deliberately limited metadata-only Token2022 profile, not an extension engine.
//! SPL4 local layout/size semantics; no claim about the deployed program version.
use crate::execution_solana_tx::PubkeyBytes;

pub(crate) const ACCOUNT_LENGTH: usize = 165 + 1 + 4; // AccountType + ImmutableOwner7/0
const MAX_MINT_LENGTH: usize = 4096;

pub(crate) fn program_id() -> PubkeyBytes {
    crate::execution_pumpswap_accounts::parse_pubkey(
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        "token2022_program",
    )
    .expect("constant program id")
}

// Pack COption has a four-byte tag; SPL ignores the unused None payload.
fn coption(data: &[u8], at: usize) -> bool {
    matches!(data.get(at..at + 4), Some([0, 0, 0, 0] | [1, 0, 0, 0]))
}

pub(super) fn mint(data: &[u8], key: &PubkeyBytes) -> Option<()> {
    if !(166..=MAX_MINT_LENGTH).contains(&data.len())
        || data.len() == 355 // SPL multisig length is not an extension account.
        || !coption(data, 0) || !coption(data, 46)
        || data[44] != 6 || data[45] != 1
        || data[82..165].iter().any(|b| *b != 0) || data[165] != 1
    {
        return None;
    }
    let mut offset = 166usize;
    let (mut pointer, mut metadata) = (false, false);
    while offset < data.len() {
        let header_end = offset.checked_add(4)?;
        let header = data.get(offset..header_end)?;
        let kind = u16::from_le_bytes(header[..2].try_into().ok()?);
        let length = usize::from(u16::from_le_bytes(header[2..].try_into().ok()?));
        let end = header_end.checked_add(length)?;
        let body = data.get(header_end..end)?;
        match kind {
            18 if !pointer && length == 64 && body[32..] == key[..] => pointer = true,
            19 if !metadata => {
                super::metadata::valid(body, key)?;
                metadata = true;
            }
            _ => return None, // Unknown, duplicate, padding/trailing TLV: never size165/zero.
        }
        offset = end;
    }
    (pointer && metadata).then_some(())
}

pub(super) fn account(data: &[u8], mint: &PubkeyBytes, owner: &PubkeyBytes) -> bool {
    data.len() == ACCOUNT_LENGTH
        && data[..32] == mint[..] && data[32..64] == owner[..]
        && data[108] == 1 // initialized, not uninitialized/frozen
        && coption(data, 72) && coption(data, 129)
        && data[109..113] == [0,0,0,0] // non-native COption, never WSOL/refund credit
        && data[165..] == [2,7,0,0,0] // AccountType=Account, only ImmutableOwner7/0
}
