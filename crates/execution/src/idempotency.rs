const CLIENT_ORDER_ID_MAX_LEN: usize = 120;

fn sanitize_for_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

pub fn client_order_id(signal_id: &str, attempt: u32) -> String {
    let mut id = format!("cb_{}_a{}", sanitize_for_id(signal_id), attempt.max(1));
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
    fn client_order_id_is_bounded() {
        let long_signal = "x".repeat(512);
        let id = client_order_id(&long_signal, 1);
        assert!(id.len() <= 120);
    }
}
