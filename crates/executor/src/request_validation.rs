use crate::rfc3339_time::parse_rfc3339_utc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestValidationError {
    InvalidAction,
    InvalidDryRun,
    InvalidSignalTs,
    InvalidRequestId,
    InvalidSignalId,
    InvalidClientOrderId,
}

pub(crate) fn validate_simulate_action(action: Option<&str>) -> Result<(), RequestValidationError> {
    if action
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case("simulate"))
        == Some(true)
    {
        return Ok(());
    }
    Err(RequestValidationError::InvalidAction)
}

pub(crate) fn validate_simulate_dry_run(
    dry_run: Option<bool>,
) -> Result<(), RequestValidationError> {
    if dry_run == Some(true) {
        return Ok(());
    }
    Err(RequestValidationError::InvalidDryRun)
}

pub(crate) fn validate_signal_ts_rfc3339(signal_ts: &str) -> Result<(), RequestValidationError> {
    if parse_rfc3339_utc(signal_ts).is_some() {
        return Ok(());
    }
    Err(RequestValidationError::InvalidSignalTs)
}

pub(crate) fn validate_non_empty_request_id(
    request_id: &str,
) -> Result<(), RequestValidationError> {
    if request_id.trim().is_empty() {
        return Err(RequestValidationError::InvalidRequestId);
    }
    Ok(())
}

pub(crate) fn validate_non_empty_signal_id(signal_id: &str) -> Result<(), RequestValidationError> {
    if signal_id.trim().is_empty() {
        return Err(RequestValidationError::InvalidSignalId);
    }
    Ok(())
}

pub(crate) fn validate_non_empty_client_order_id(
    client_order_id: &str,
) -> Result<(), RequestValidationError> {
    if client_order_id.trim().is_empty() {
        return Err(RequestValidationError::InvalidClientOrderId);
    }
    Ok(())
}

pub(crate) fn validate_simulate_request_basics(
    action: Option<&str>,
    dry_run: Option<bool>,
    signal_ts: &str,
    request_id: &str,
    signal_id: &str,
) -> Result<(), RequestValidationError> {
    validate_simulate_action(action)?;
    validate_simulate_dry_run(dry_run)?;
    validate_signal_ts_rfc3339(signal_ts)?;
    validate_non_empty_request_id(request_id)?;
    validate_non_empty_signal_id(signal_id)?;
    Ok(())
}

pub(crate) fn validate_submit_request_identity(
    signal_ts: &str,
    client_order_id: &str,
    request_id: &str,
    signal_id: &str,
) -> Result<(), RequestValidationError> {
    validate_signal_ts_rfc3339(signal_ts)?;
    validate_non_empty_client_order_id(client_order_id)?;
    validate_non_empty_request_id(request_id)?;
    validate_non_empty_signal_id(signal_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_non_empty_client_order_id, validate_non_empty_request_id,
        validate_non_empty_signal_id, validate_signal_ts_rfc3339, validate_simulate_action,
        validate_simulate_dry_run, validate_simulate_request_basics,
        validate_submit_request_identity, RequestValidationError,
    };

    #[test]
    fn request_validation_rejects_invalid_simulate_action() {
        let error = validate_simulate_action(Some("submit")).expect_err("must reject non-simulate");
        assert_eq!(error, RequestValidationError::InvalidAction);
    }

    #[test]
    fn request_validation_rejects_invalid_simulate_dry_run() {
        let error = validate_simulate_dry_run(Some(false)).expect_err("must reject dry_run=false");
        assert_eq!(error, RequestValidationError::InvalidDryRun);
    }

    #[test]
    fn request_validation_rejects_invalid_signal_ts() {
        let error =
            validate_signal_ts_rfc3339("not-a-time").expect_err("must reject invalid signal_ts");
        assert_eq!(error, RequestValidationError::InvalidSignalTs);
    }

    #[test]
    fn request_validation_rejects_empty_request_id() {
        let error = validate_non_empty_request_id("  ").expect_err("must reject empty request_id");
        assert_eq!(error, RequestValidationError::InvalidRequestId);
    }

    #[test]
    fn request_validation_rejects_empty_signal_id() {
        let error = validate_non_empty_signal_id("").expect_err("must reject empty signal_id");
        assert_eq!(error, RequestValidationError::InvalidSignalId);
    }

    #[test]
    fn request_validation_rejects_empty_client_order_id() {
        let error = validate_non_empty_client_order_id("\t")
            .expect_err("must reject empty client_order_id");
        assert_eq!(error, RequestValidationError::InvalidClientOrderId);
    }

    #[test]
    fn request_validation_validate_simulate_request_basics_accepts_valid_inputs() {
        validate_simulate_request_basics(
            Some("simulate"),
            Some(true),
            "2026-02-24T00:00:00Z",
            "request-1",
            "signal-1",
        )
        .expect("valid simulate basics must pass");
    }

    #[test]
    fn request_validation_validate_simulate_request_basics_rejects_invalid_action_first() {
        let error = validate_simulate_request_basics(
            Some("submit"),
            Some(true),
            "2026-02-24T00:00:00Z",
            "request-1",
            "signal-1",
        )
        .expect_err("invalid action must reject");
        assert_eq!(error, RequestValidationError::InvalidAction);
    }

    #[test]
    fn request_validation_validate_submit_request_identity_accepts_valid_inputs() {
        validate_submit_request_identity(
            "2026-02-24T00:00:00Z",
            "client-1",
            "request-1",
            "signal-1",
        )
        .expect("valid submit identity fields must pass");
    }

    #[test]
    fn request_validation_validate_submit_request_identity_rejects_empty_client_order_id() {
        let error = validate_submit_request_identity(
            "2026-02-24T00:00:00Z",
            " ",
            "request-1",
            "signal-1",
        )
        .expect_err("empty client_order_id must reject");
        assert_eq!(error, RequestValidationError::InvalidClientOrderId);
    }
}
