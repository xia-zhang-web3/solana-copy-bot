use anyhow::{anyhow, Result};

pub(crate) fn is_valid_contract_version_token(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
}

pub(crate) fn parse_contract_version(value: &str) -> Result<String> {
    let contract_version = value.trim();
    if contract_version.is_empty()
        || contract_version.len() > 64
        || !is_valid_contract_version_token(contract_version)
    {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_CONTRACT_VERSION must be non-empty token [A-Za-z0-9._-], len<=64"
        ));
    }
    Ok(contract_version.to_string())
}

#[cfg(test)]
mod tests {
    use super::{is_valid_contract_version_token, parse_contract_version};

    #[test]
    fn contract_version_token_validation() {
        assert!(is_valid_contract_version_token("v1"));
        assert!(is_valid_contract_version_token("v1.2.3-prod"));
        assert!(!is_valid_contract_version_token("v1 beta"));
        assert!(!is_valid_contract_version_token("v1/beta"));
    }

    #[test]
    fn parse_contract_version_accepts_trimmed_valid_value() {
        let version = parse_contract_version("  v1.2.3-prod  ").expect("valid version");
        assert_eq!(version, "v1.2.3-prod");
    }

    #[test]
    fn parse_contract_version_rejects_invalid_value() {
        let error = parse_contract_version("v1 beta").expect_err("invalid token must reject");
        assert!(
            error
                .to_string()
                .contains("COPYBOT_EXECUTOR_CONTRACT_VERSION"),
            "error={}",
            error
        );
    }
}
