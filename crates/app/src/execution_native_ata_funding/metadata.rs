//! Bounded Borsh TokenMetadata body (SPL token-metadata-interface 0.4.0).
use crate::execution_solana_tx::PubkeyBytes;
use std::{collections::HashSet, str};

pub(super) fn valid(data: &[u8], mint: &PubkeyBytes) -> Option<()> {
    let mut cursor = Cursor { data, offset: 0 };
    // OptionalNonZeroPubkey is exactly 32 bytes: zero=None, all other bytes=Some.
    cursor.take(32)?;
    if cursor.take(32)? != mint {
        return None;
    }
    for _ in 0..3 {
        cursor.string()?;
    }
    let entries = cursor.u32()?;
    // Every pair requires two u32 lengths, even when both strings are empty.
    if entries > (data.len() - cursor.offset) / 8 {
        return None;
    }
    let mut keys = HashSet::new();
    for _ in 0..entries {
        if !keys.insert(cursor.string()?) {
            return None;
        }
        cursor.string()?;
    }
    (cursor.offset == data.len()).then_some(())
}

struct Cursor<'a> {
    data: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, size: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(size)?;
        let bytes = self.data.get(self.offset..end)?;
        self.offset = end;
        Some(bytes)
    }
    fn u32(&mut self) -> Option<usize> {
        usize::try_from(u32::from_le_bytes(self.take(4)?.try_into().ok()?)).ok()
    }
    fn string(&mut self) -> Option<&'a str> {
        let size = self.u32()?;
        str::from_utf8(self.take(size)?).ok()
    }
}
