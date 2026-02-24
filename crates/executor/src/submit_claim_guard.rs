use std::sync::Arc;

use tracing::warn;

use crate::idempotency::SubmitIdempotencyStore;

pub(crate) struct SubmitClaimGuard {
    idempotency: Arc<SubmitIdempotencyStore>,
    client_order_id: String,
    request_id: String,
}

impl SubmitClaimGuard {
    pub(crate) fn new(
        idempotency: Arc<SubmitIdempotencyStore>,
        client_order_id: &str,
        request_id: &str,
    ) -> Self {
        Self {
            idempotency,
            client_order_id: client_order_id.trim().to_string(),
            request_id: request_id.trim().to_string(),
        }
    }
}

impl Drop for SubmitClaimGuard {
    fn drop(&mut self) {
        match self
            .idempotency
            .release_submit_claim(self.client_order_id.as_str(), self.request_id.as_str())
        {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    client_order_id = %self.client_order_id,
                    request_id = %self.request_id,
                    "idempotency submit claim release had no owner-match row"
                );
            }
            Err(error) => {
                warn!(
                    client_order_id = %self.client_order_id,
                    request_id = %self.request_id,
                    error = %error,
                    "failed to release idempotency submit claim"
                );
            }
        }
    }
}
