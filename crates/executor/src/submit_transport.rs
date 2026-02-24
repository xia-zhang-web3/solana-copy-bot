use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmitTransportArtifact {
    UpstreamSignature(String),
    SignedTransactionBase64(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmitTransportArtifactError {
    InvalidUpstreamSignature { error: String },
    MissingSubmitArtifact,
}

pub(crate) fn extract_submit_transport_artifact(
    backend_response: &Value,
) -> Result<SubmitTransportArtifact, SubmitTransportArtifactError> {
    let upstream_tx_signature = backend_response
        .get("tx_signature")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let signed_tx_base64 = backend_response
        .get("signed_tx_base64")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(value) = upstream_tx_signature {
        crate::validate_signature_like(value).map_err(|error| {
            SubmitTransportArtifactError::InvalidUpstreamSignature {
                error: error.to_string(),
            }
        })?;
        return Ok(SubmitTransportArtifact::UpstreamSignature(value.to_string()));
    }
    if let Some(value) = signed_tx_base64 {
        return Ok(SubmitTransportArtifact::SignedTransactionBase64(value.to_string()));
    }

    Err(SubmitTransportArtifactError::MissingSubmitArtifact)
}

#[cfg(test)]
mod tests {
    use super::{
        extract_submit_transport_artifact, SubmitTransportArtifact, SubmitTransportArtifactError,
    };
    use serde_json::json;

    #[test]
    fn submit_transport_extract_prefers_upstream_signature_when_present() {
        let signature = bs58::encode([42u8; 64]).into_string();
        let body = json!({
            "tx_signature": signature,
            "signed_tx_base64": "AQID",
        });
        let artifact = extract_submit_transport_artifact(&body).expect("must parse transport");
        assert!(matches!(
            artifact,
            SubmitTransportArtifact::UpstreamSignature(_)
        ));
    }

    #[test]
    fn submit_transport_extract_uses_signed_tx_when_signature_missing() {
        let body = json!({
            "signed_tx_base64": "AQID",
        });
        let artifact = extract_submit_transport_artifact(&body).expect("must parse transport");
        assert_eq!(
            artifact,
            SubmitTransportArtifact::SignedTransactionBase64("AQID".to_string())
        );
    }

    #[test]
    fn submit_transport_extract_rejects_invalid_signature() {
        let body = json!({
            "tx_signature": "not-base58",
        });
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::InvalidUpstreamSignature { .. }
        ));
    }

    #[test]
    fn submit_transport_extract_rejects_missing_artifacts() {
        let body = json!({});
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert_eq!(error, SubmitTransportArtifactError::MissingSubmitArtifact);
    }
}
