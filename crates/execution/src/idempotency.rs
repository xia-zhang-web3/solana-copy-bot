const CLIENT_ORDER_ID_MAX_LEN: usize = 120;
const HASH_SUFFIX_HEX_BYTES: usize = 6;
const HASH_PREFIX: &str = "_h";

use sha2::{Digest, Sha256};

fn sanitize_for_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn stable_hash_suffix(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut out = String::with_capacity(HASH_PREFIX.len() + (HASH_SUFFIX_HEX_BYTES * 2));
    out.push_str(HASH_PREFIX);
    for byte in digest.iter().take(HASH_SUFFIX_HEX_BYTES) {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", byte);
    }
    out
}

fn needs_hash_suffix(signal_id: &str, legacy_id_len: usize) -> bool {
    legacy_id_len > CLIENT_ORDER_ID_MAX_LEN
        || signal_id
            .chars()
            .any(|ch| !ch.is_ascii_alphanumeric() && ch != ':')
}

pub fn client_order_id(signal_id: &str, attempt: u32) -> String {
    let attempt = attempt.max(1);
    let sanitized = sanitize_for_id(signal_id);
    let legacy = format!("cb_{}_a{}", sanitized, attempt);

    if !needs_hash_suffix(signal_id, legacy.len()) {
        return legacy;
    }

    let suffix = stable_hash_suffix(signal_id);
    let attempt_part = format!("_a{}", attempt);
    let available_for_sanitized = CLIENT_ORDER_ID_MAX_LEN
        .saturating_sub("cb_".len())
        .saturating_sub(attempt_part.len())
        .saturating_sub(suffix.len());
    let mut truncated_sanitized = sanitized;
    if truncated_sanitized.len() > available_for_sanitized {
        truncated_sanitized.truncate(available_for_sanitized);
    }

    let mut id = format!("cb_{}{}{}", truncated_sanitized, attempt_part, suffix);
    if id.len() > CLIENT_ORDER_ID_MAX_LEN {
        id.truncate(CLIENT_ORDER_ID_MAX_LEN);
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_id_is_stable_for_same_input() {
        let a = client_order_id("shadow:sig-x:wallet-a:buy:token-x", 1);
        let b = client_order_id("shadow:sig-x:wallet-a:buy:token-x", 1);
        assert_eq!(a, b);
    }

    #[test]
    fn client_order_id_keeps_legacy_format_for_colon_delimited_signal() {
        let id = client_order_id("shadow:s2:w:buy:t1", 1);
        assert_eq!(id, "cb_shadow_s2_w_buy_t1_a1");
    }

    #[test]
    fn client_order_id_avoids_collision_for_non_colon_delimiters() {
        let a = client_order_id("shadow:test-wallet:buy:token", 1);
        let b = client_order_id("shadow:test/wallet:buy:token", 1);
        assert_ne!(a, b);
        assert!(a.contains("_h"));
        assert!(b.contains("_h"));
    }

    #[test]
    fn client_order_id_with_hash_keeps_attempt_and_bound() {
        let long_signal = format!("shadow:{}:wallet:buy:token", "path/".repeat(128));
        let id = client_order_id(&long_signal, 3);
        assert!(id.len() <= CLIENT_ORDER_ID_MAX_LEN);
        assert!(id.contains("_a3_h"));
    }

    #[test]
    fn client_order_id_is_bounded() {
        let long_signal = "x".repeat(512);
        let id = client_order_id(&long_signal, 1);
        assert!(id.len() <= 120);
    }
}
