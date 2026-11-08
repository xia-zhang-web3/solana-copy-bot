//! Bounded Borsh TokenMetadata subset; reference: SPL token-metadata-interface 0.4.0.
use crate::token2022_profile::ProfileFailure;
use std::{collections::HashSet, str};

pub(crate) fn validate(data: &[u8], mint: &[u8; 32]) -> Result<(), ProfileFailure> {
    use ProfileFailure as F;
    let mut cursor = Cursor { data, offset: 0 };
    // OptionalNonZeroPubkey: fixed 32 bytes; zero=None, all other byte patterns=Some.
    cursor.take(32).ok_or(F::Malformed)?;
    if cursor.take(32).ok_or(F::Malformed)? != mint {
        return Err(F::Binding);
    }
    for _ in 0..3 {
        cursor.string().ok_or(F::Malformed)?;
    }
    let entries = cursor.u32().ok_or(F::Malformed)?;
    if entries > (data.len() - cursor.offset) / 8 {
        return Err(F::Malformed);
    }
    let mut keys = HashSet::new();
    for _ in 0..entries {
        if !keys.insert(cursor.string().ok_or(F::Malformed)?) {
            return Err(F::Malformed);
        }
        cursor.string().ok_or(F::Malformed)?;
    }
    if cursor.offset != data.len() {
        return Err(F::Malformed);
    }
    Ok(())
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
