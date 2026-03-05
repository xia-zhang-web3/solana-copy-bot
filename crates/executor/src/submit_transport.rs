use crate::key_validation::validate_signature_like;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmitTransportArtifact {
    UpstreamSignature(String),
    SignedTransactionBase64(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmitTransportArtifactError {
    InvalidSubmitArtifactType { field_name: String },
    InvalidUpstreamSignature { error: String },
    ConflictingSubmitArtifacts,
    MissingSubmitArtifact,
}

pub(crate) fn extract_submit_transport_artifact(
    backend_response: &Value,
) -> Result<SubmitTransportArtifact, SubmitTransportArtifactError> {
    let upstream_tx_signature =
        parse_optional_non_empty_submit_transport_field(backend_response, "tx_signature")?;
    let signed_tx_base64 =
        parse_optional_non_empty_submit_transport_field(backend_response, "signed_tx_base64")?;

    if upstream_tx_signature.is_some() && signed_tx_base64.is_some() {
        return Err(SubmitTransportArtifactError::ConflictingSubmitArtifacts);
    }

    if let Some(value) = upstream_tx_signature {
        validate_signature_like(value.as_str()).map_err(|error| {
            SubmitTransportArtifactError::InvalidUpstreamSignature {
                error: error.to_string(),
            }
        })?;
        return Ok(SubmitTransportArtifact::UpstreamSignature(value));
    }
    if let Some(value) = signed_tx_base64 {
        return Ok(SubmitTransportArtifact::SignedTransactionBase64(
            value.to_string(),
        ));
    }

    Err(SubmitTransportArtifactError::MissingSubmitArtifact)
}

fn parse_optional_non_empty_submit_transport_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<String>, SubmitTransportArtifactError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(SubmitTransportArtifactError::InvalidSubmitArtifactType {
            field_name: field_name.to_string(),
        });
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(SubmitTransportArtifactError::InvalidSubmitArtifactType {
            field_name: field_name.to_string(),
        });
    }
    Ok(Some(normalized.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        extract_submit_transport_artifact, SubmitTransportArtifact, SubmitTransportArtifactError,
    };
    use serde_json::json;

    #[test]
    fn submit_transport_extract_rejects_conflicting_artifacts_when_both_present() {
        let signature = bs58::encode([42u8; 64]).into_string();
        let body = json!({
            "tx_signature": signature,
            "signed_tx_base64": "AQID",
        });
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::ConflictingSubmitArtifacts
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

    #[test]
    fn submit_transport_extract_rejects_non_string_tx_signature_type() {
        let body = json!({
            "tx_signature": 123,
        });
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name }
            if field_name == "tx_signature"
        ));
    }

    #[test]
    fn submit_transport_extract_rejects_non_string_signed_tx_base64_type() {
        let body = json!({
            "signed_tx_base64": 123,
        });
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name }
            if field_name == "signed_tx_base64"
        ));
    }

    #[test]
    fn submit_transport_extract_rejects_null_tx_signature() {
        let body = json!({
            "tx_signature": null,
        });
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name }
            if field_name == "tx_signature"
        ));
    }

    #[test]
    fn submit_transport_extract_rejects_empty_signed_tx_base64() {
        let body = json!({
            "signed_tx_base64": "   ",
        });
        let error = extract_submit_transport_artifact(&body).expect_err("must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name }
            if field_name == "signed_tx_base64"
        ));
    }
}
