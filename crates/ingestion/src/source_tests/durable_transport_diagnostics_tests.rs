use crate::source::durable::transport_diagnostics::ErrorDetails;
use crate::TransportClass;

#[test]
fn diagnostics_preserve_internal_and_data_loss_with_bounded_redacted_text() {
    let endpoint = "https://private.provider.example/key-secret?api-key=value";
    let token = "private-token-123";
    for (code, class) in [
        (tonic::Code::Internal, TransportClass::Internal),
        (tonic::Code::DataLoss, TransportClass::DataLoss),
    ] {
        let status = tonic::Status::new(code, format!("h2 stream reset PROTOCOL_ERROR endpoint={endpoint}\nAuthorization: Bearer {token}\nx_token={token}\n{}", "a".repeat(1000)));
        let details = ErrorDetails::status(&status, &[endpoint, token]);
        assert_eq!(details.code, Some(code));
        assert_eq!(details.class, class);
        assert!(details.message.contains("PROTOCOL_ERROR"));
        assert!(details.message.chars().count() <= 240);
        assert!(!details.message.contains("private"));
        assert!(!details.message.contains("Bearer"));
        assert!(!details.message.contains("Authorization"));
        assert!(!details.message.contains("x_token"));
        assert!(!details.message.contains("https://"));
    }
}

#[test]
fn nested_causes_are_bounded_and_do_not_expose_unknown_urls_or_headers() {
    let error = anyhow::Error::new(std::io::Error::other(
        "h2 peer stream reset https://other.example/secret",
    ))
    .context("headers { custom: credential }")
    .context("transport connection error");
    let details = ErrorDetails::error(&error, &[]);
    assert_eq!(details.message, "transport connection error");
    assert_eq!(details.causes.len(), 2);
    assert!(details.causes.iter().all(|v| v.chars().count() <= 160));
    let printable = format!("{details:?}");
    assert!(!printable.contains("other.example"));
    assert!(!printable.contains("credential"));
    assert!(printable.contains("h2 peer stream reset"));
}

#[test]
fn typed_status_through_anyhow_preserves_exact_code() {
    let error = anyhow::Error::new(tonic::Status::data_loss(
        "protobuf failed to decode field 7",
    ))
    .context("connection transport");
    let details = ErrorDetails::error(&error, &[]);
    assert_eq!(details.code, Some(tonic::Code::DataLoss));
    assert_eq!(details.class, TransportClass::DataLoss);
    assert_eq!(details.message, "protobuf failed to decode field 7");
}
