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
