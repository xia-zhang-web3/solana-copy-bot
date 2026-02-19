use chrono::{DateTime, Utc};
use std::fmt;
use uuid::Uuid;

use crate::intent::ExecutionIntent;

#[derive(Debug, Clone)]
pub struct SubmitResult {
    pub route: String,
    pub tx_signature: String,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitErrorKind {
    Retryable,
    Terminal,
}

#[derive(Debug, Clone)]
pub struct SubmitError {
    pub kind: SubmitErrorKind,
    pub code: String,
    pub detail: String,
}

impl SubmitError {
    pub fn retryable(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind: SubmitErrorKind::Retryable,
            code: code.into(),
            detail: detail.into(),
        }
    }

    pub fn terminal(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind: SubmitErrorKind::Terminal,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "submit_error kind={:?} code={} detail={}",
            self.kind, self.code, self.detail
        )
    }
}

impl std::error::Error for SubmitError {}

pub trait OrderSubmitter {
    fn submit(
        &self,
        intent: &ExecutionIntent,
        client_order_id: &str,
        route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperOrderSubmitter;

impl OrderSubmitter for PaperOrderSubmitter {
    fn submit(
        &self,
        intent: &ExecutionIntent,
        client_order_id: &str,
        route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        let sig = format!(
            "paper:{}:{}:{}",
            intent.side.as_str(),
            client_order_id,
            Uuid::new_v4().simple()
        );
        Ok(SubmitResult {
            route: route.to_string(),
            tx_signature: sig,
            submitted_at: Utc::now(),
        })
    }
}
