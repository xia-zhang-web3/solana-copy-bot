use std::{fmt, sync::Arc};
use zeroize::Zeroizing;

#[derive(Clone)]
pub(crate) struct SecretValue(Arc<Zeroizing<String>>);

impl SecretValue {
    pub(crate) fn new(value: String) -> Self {
        Self(Arc::new(Zeroizing::new(value)))
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
    use std::sync::Arc;

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

    #[test]
    fn secret_value_clone_shares_backing_allocation() {
        let secret = SecretValue::from("shared-secret-token");
        let cloned = secret.clone();
        assert!(
            Arc::ptr_eq(&secret.0, &cloned.0),
            "cloned secret values must share backing allocation"
        );
    }
}
