use std::fmt;
use zeroize::Zeroizing;

#[derive(Clone)]
pub(crate) struct SecretValue(Zeroizing<String>);

impl SecretValue {
    pub(crate) fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue([REDACTED])")
    }
}

impl From<String> for SecretValue {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretValue {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::SecretValue;

    #[test]
    fn secret_value_debug_redacts_plaintext() {
        let secret = SecretValue::from("super-secret-token");
        let debug = format!("{:?}", secret);
        assert!(
            !debug.contains("super-secret-token"),
            "debug output must not expose plaintext secret"
        );
        assert!(
            debug.contains("REDACTED"),
            "debug output should indicate redaction"
        );
    }
}
