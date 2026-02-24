use anyhow::{anyhow, Result};

pub(crate) fn require_authenticated_mode(
    bearer_token: Option<&str>,
    allow_unauthenticated: bool,
) -> Result<()> {
    if allow_unauthenticated {
        return Ok(());
    }
    if bearer_token.is_none() {
        return Err(anyhow!(
            "executor auth is required: set COPYBOT_EXECUTOR_BEARER_TOKEN (or *_FILE); optionally add HMAC pair for dual auth. For controlled local setups only, set COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true"
        ));
    }
    Ok(())
}

pub(crate) fn validate_hmac_auth_config(
    hmac_key_id: Option<&str>,
    hmac_secret: Option<&str>,
    hmac_ttl_sec: u64,
) -> Result<()> {
    if (hmac_key_id.is_some() && hmac_secret.is_none())
        || (hmac_key_id.is_none() && hmac_secret.is_some())
    {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_HMAC_KEY_ID and COPYBOT_EXECUTOR_HMAC_SECRET must be set together"
        ));
    }
    if hmac_key_id.is_some() && !(5..=300).contains(&hmac_ttl_sec) {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_HMAC_TTL_SEC must be in 5..=300 when HMAC auth is enabled"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_hmac_auth_config, require_authenticated_mode};

    #[test]
    fn require_authenticated_mode_fails_without_bearer_when_auth_required() {
        let error = require_authenticated_mode(None, false).expect_err("must fail closed");
        assert!(error.to_string().contains("COPYBOT_EXECUTOR_BEARER_TOKEN"));
    }

    #[test]
    fn validate_hmac_auth_config_rejects_partial_pair() {
        let error =
            validate_hmac_auth_config(Some("key-id"), None, 30).expect_err("partial pair fails");
        assert!(
            error
                .to_string()
                .contains("must be set together"),
            "error={}",
            error
        );
    }

    #[test]
    fn validate_hmac_auth_config_rejects_ttl_out_of_range_when_enabled() {
        let error = validate_hmac_auth_config(Some("key-id"), Some("secret"), 301)
            .expect_err("ttl out of range must fail");
        assert!(error.to_string().contains("must be in 5..=300"));
    }

    #[test]
    fn validate_hmac_auth_config_accepts_disabled_hmac_with_any_ttl() {
        validate_hmac_auth_config(None, None, 1_000).expect("disabled hmac should skip ttl gate");
    }
}
