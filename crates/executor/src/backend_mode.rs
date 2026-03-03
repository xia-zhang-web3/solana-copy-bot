use anyhow::{anyhow, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutorBackendMode {
    Upstream,
    Mock,
}

impl ExecutorBackendMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Upstream => "upstream",
            Self::Mock => "mock",
        }
    }

    pub(crate) fn parse_env(raw: &str) -> Result<Self> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "upstream" => Ok(Self::Upstream),
            "mock" => Ok(Self::Mock),
            _ => Err(anyhow!("must be one of: upstream,mock (got {})", raw)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutorBackendMode;

    #[test]
    fn parse_env_accepts_known_values_case_insensitive() {
        assert_eq!(
            ExecutorBackendMode::parse_env("upstream").expect("upstream must parse"),
            ExecutorBackendMode::Upstream
        );
        assert_eq!(
            ExecutorBackendMode::parse_env(" MOCK ").expect("mock must parse"),
            ExecutorBackendMode::Mock
        );
    }

    #[test]
    fn parse_env_rejects_unknown_value() {
        let error = ExecutorBackendMode::parse_env("invalid").expect_err("must reject");
        assert!(
            error.to_string().contains("must be one of: upstream,mock"),
            "unexpected error: {}",
            error
        );
    }
}
