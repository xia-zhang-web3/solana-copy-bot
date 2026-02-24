use serde_json::{json, Value};

use crate::submit_verify::SubmitSignatureVerification;

pub(crate) fn submit_signature_verification_to_json(value: &SubmitSignatureVerification) -> Value {
    match value {
        SubmitSignatureVerification::Skipped => json!({
            "enabled": false,
        }),
        SubmitSignatureVerification::Seen {
            confirmation_status,
        } => json!({
            "enabled": true,
            "seen": true,
            "confirmation_status": confirmation_status,
        }),
        SubmitSignatureVerification::Unseen { reason } => json!({
            "enabled": true,
            "seen": false,
            "reason": reason,
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::submit_signature_verification_to_json;
    use crate::submit_verify::SubmitSignatureVerification;

    #[test]
    fn submit_verify_payload_serializes_seen_shape() {
        let value = submit_signature_verification_to_json(&SubmitSignatureVerification::Seen {
            confirmation_status: "confirmed".to_string(),
        });
        assert_eq!(value.get("enabled").and_then(Value::as_bool), Some(true));
        assert_eq!(value.get("seen").and_then(Value::as_bool), Some(true));
        assert_eq!(
            value.get("confirmation_status").and_then(Value::as_str),
            Some("confirmed")
        );
    }

    #[test]
    fn submit_verify_payload_serializes_unseen_shape() {
        let value = submit_signature_verification_to_json(&SubmitSignatureVerification::Unseen {
            reason: "rpc timeout".to_string(),
        });
        assert_eq!(value.get("enabled").and_then(Value::as_bool), Some(true));
        assert_eq!(value.get("seen").and_then(Value::as_bool), Some(false));
        assert_eq!(value.get("reason").and_then(Value::as_str), Some("rpc timeout"));
    }
}
