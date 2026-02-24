pub(crate) fn is_valid_contract_version_token(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
}

#[cfg(test)]
mod tests {
    use super::is_valid_contract_version_token;

    #[test]
    fn contract_version_token_validation() {
        assert!(is_valid_contract_version_token("v1"));
        assert!(is_valid_contract_version_token("v1.2.3-prod"));
        assert!(!is_valid_contract_version_token("v1 beta"));
        assert!(!is_valid_contract_version_token("v1/beta"));
    }
}
