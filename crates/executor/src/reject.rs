#[derive(Debug, Clone)]
pub(crate) struct Reject {
    pub(crate) retryable: bool,
    pub(crate) code: String,
    pub(crate) detail: String,
}

impl Reject {
    pub(crate) fn terminal(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            retryable: false,
            code: code.into(),
            detail: detail.into(),
        }
    }

    pub(crate) fn retryable(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            retryable: true,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Reject;

    #[test]
    fn reject_builders_set_retryable_flag() {
        let terminal = Reject::terminal("terminal_code", "terminal_detail");
        assert!(!terminal.retryable);
        assert_eq!(terminal.code, "terminal_code");
        assert_eq!(terminal.detail, "terminal_detail");

        let retryable = Reject::retryable("retry_code", "retry_detail");
        assert!(retryable.retryable);
        assert_eq!(retryable.code, "retry_code");
        assert_eq!(retryable.detail, "retry_detail");
    }
}
