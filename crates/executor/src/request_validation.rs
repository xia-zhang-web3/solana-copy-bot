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

#[cfg(test)]
mod tests {
    use super::{
        validate_non_empty_client_order_id, validate_non_empty_request_id,
        validate_non_empty_signal_id, validate_signal_ts_rfc3339, validate_simulate_action,
        validate_simulate_dry_run, RequestValidationError,
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
}
