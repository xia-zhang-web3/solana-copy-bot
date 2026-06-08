use crate::execution_signing_envelope::{
    validate_signed_transaction_base64, ExecutionSigningEnvelope,
};
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExecutionSubmitPlan {
    SubmitDisabled {
        idempotency_key: String,
        reason: String,
    },
    SubmitReady(ExecutionSubmitIntent),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionSubmitIntent {
    pub(crate) idempotency_key: String,
    pub(crate) submit_route: String,
    pub(crate) signed_transaction_base64: String,
    pub(crate) tx_signature_hint: Option<String>,
}

impl ExecutionSubmitPlan {
    pub(crate) fn submit_disabled(request: &ExecutionSubmitRequest, reason: String) -> Self {
        Self::SubmitDisabled {
            idempotency_key: execution_submit_idempotency_key(request),
            reason,
        }
    }

    pub(crate) fn idempotency_key(&self) -> &str {
        match self {
            Self::SubmitDisabled {
                idempotency_key, ..
            } => idempotency_key,
            Self::SubmitReady(intent) => &intent.idempotency_key,
        }
    }

    pub(crate) fn validate_for_request(&self, request: &ExecutionSubmitRequest) -> Result<()> {
        let expected = execution_submit_idempotency_key(request);
        let actual = self.idempotency_key();
        if actual != expected {
            anyhow::bail!("submit idempotency key mismatch: actual={actual} expected={expected}");
        }
        if let Self::SubmitReady(intent) = self {
            validate_submit_intent(intent)?;
        }
        Ok(())
    }
}

pub(crate) fn execution_submit_idempotency_key(request: &ExecutionSubmitRequest) -> String {
    format!(
        "copybot:submit:{}:attempt:{}",
        request.client_order_id, request.attempt
    )
}

pub(crate) fn execution_submit_intent_from_signed_envelope(
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    submit_route: String,
) -> Result<ExecutionSubmitIntent> {
    let expected_key = execution_submit_idempotency_key(request);
    if envelope.idempotency_key != expected_key {
        anyhow::bail!(
            "signed envelope idempotency key mismatch: actual={} expected={}",
            envelope.idempotency_key,
            expected_key
        );
    }
    let Some(signed_transaction_base64) = envelope.signed_transaction_base64.clone() else {
        anyhow::bail!("signed envelope missing signed transaction payload");
    };
    let intent = ExecutionSubmitIntent {
        idempotency_key: expected_key,
        submit_route,
        signed_transaction_base64,
        tx_signature_hint: envelope.tx_signature_hint.clone(),
    };
    validate_submit_intent(&intent)?;
    Ok(intent)
}

fn validate_submit_intent(intent: &ExecutionSubmitIntent) -> Result<()> {
    if intent.submit_route.trim().is_empty() {
        anyhow::bail!("submit intent route must be non-empty");
    }
    validate_signed_transaction_base64(&intent.signed_transaction_base64)?;
    if intent
        .tx_signature_hint
        .as_deref()
        .is_some_and(|hint| hint.trim().is_empty())
    {
        anyhow::bail!("submit intent transaction signature hint must be non-empty");
    }
    Ok(())
}
