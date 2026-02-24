use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::env;

pub(crate) fn non_empty_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{} is required", name))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must be non-empty", name));
    }
    Ok(trimmed.to_string())
}

pub(crate) fn optional_non_empty_env(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(crate) fn parse_u64_env(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|error| anyhow!("{} must be u64: {}", name, error)),
        Err(_) => Ok(default),
    }
}

pub(crate) fn parse_f64_env(name: &str, default: f64) -> Result<f64> {
    match env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<f64>()
            .map_err(|error| anyhow!("{} must be f64: {}", name, error)),
        Err(_) => Ok(default),
    }
}

pub(crate) fn parse_bool_token(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

pub(crate) fn parse_bool_env(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(raw) => parse_bool_token(raw.as_str()),
        Err(_) => default,
    }
}

pub(crate) fn parse_socket_addr_str(name: &str, value: &str) -> Result<SocketAddr> {
    value
        .trim()
        .parse::<SocketAddr>()
        .map_err(|error| anyhow!("invalid {}: {}", name, error))
}

#[cfg(test)]
mod tests {
    use super::{parse_bool_token, parse_socket_addr_str};

    #[test]
    fn parse_bool_token_accepts_true_forms() {
        assert!(parse_bool_token("1"));
        assert!(parse_bool_token("true"));
        assert!(parse_bool_token("TRUE"));
        assert!(parse_bool_token("yes"));
        assert!(parse_bool_token("on"));
    }

    #[test]
    fn parse_bool_token_rejects_false_forms() {
        assert!(!parse_bool_token("0"));
        assert!(!parse_bool_token("false"));
        assert!(!parse_bool_token("off"));
        assert!(!parse_bool_token("no"));
        assert!(!parse_bool_token("random"));
    }

    #[test]
    fn parse_socket_addr_str_accepts_valid_socket_addr() {
        let addr = parse_socket_addr_str("COPYBOT_EXECUTOR_BIND_ADDR", "127.0.0.1:8090")
            .expect("valid socket addr must parse");
        assert_eq!(addr.to_string(), "127.0.0.1:8090");
    }

    #[test]
    fn parse_socket_addr_str_rejects_invalid_socket_addr() {
        let error = parse_socket_addr_str("COPYBOT_EXECUTOR_BIND_ADDR", "not-an-addr")
            .expect_err("invalid socket addr must reject");
        assert!(
            error
                .to_string()
                .contains("invalid COPYBOT_EXECUTOR_BIND_ADDR"),
            "error={}",
            error
        );
    }
}
