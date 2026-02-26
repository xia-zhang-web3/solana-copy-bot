use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub(crate) fn compute_hmac_signature_hex(key: &[u8], payload: &[u8]) -> Result<String> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).context("invalid HMAC key")?;
    mac.update(payload);
    Ok(to_hex_lower(mac.finalize().into_bytes().as_slice()))
}

pub(crate) fn to_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // Do not early-return on length mismatch to avoid a fast-fail branch.
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for idx in 0..max_len {
        let lhs = *left.get(idx).unwrap_or(&0);
        let rhs = *right.get(idx).unwrap_or(&0);
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::{constant_time_eq, to_hex_lower};

    #[test]
    fn constant_time_eq_checks_content() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }

    #[test]
    fn constant_time_eq_rejects_length_mismatch() {
        assert!(!constant_time_eq(b"", b"a"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn to_hex_lower_matches_expected() {
        assert_eq!(to_hex_lower(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }
}
