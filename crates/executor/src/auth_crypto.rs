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
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (lhs, rhs) in left.iter().zip(right.iter()) {
        diff |= lhs ^ rhs;
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
    fn to_hex_lower_matches_expected() {
        assert_eq!(to_hex_lower(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }
}
