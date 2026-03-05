use std::sync::Arc;

use tracing::warn;

use crate::idempotency::SubmitIdempotencyStore;

pub(crate) struct SubmitClaimGuard {
    idempotency: Arc<SubmitIdempotencyStore>,
    client_order_id: String,
    request_id: String,
    release_on_drop: bool,
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
            release_on_drop: true,
        }
    }

    pub(crate) fn retain_claim_on_drop(&mut self) {
        self.release_on_drop = false;
    }

    pub(crate) fn release_claim_on_drop(&mut self) {
        self.release_on_drop = true;
    }
}

impl Drop for SubmitClaimGuard {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::idempotency::{SubmitClaimOutcome, SubmitIdempotencyStore};

    use super::SubmitClaimGuard;

    #[test]
    fn submit_claim_guard_releases_claim_by_default() {
        let store = Arc::new(SubmitIdempotencyStore::open(":memory:").expect("open store"));
        let claim = store
            .load_cached_or_claim_submit("order-1", "req-1", 30)
            .expect("claim");
        assert!(matches!(claim, SubmitClaimOutcome::Claimed));

        {
            let _guard = SubmitClaimGuard::new(store.clone(), "order-1", "req-1");
        }

        let next = store
            .load_cached_or_claim_submit("order-1", "req-2", 30)
            .expect("claim after drop");
        assert!(matches!(next, SubmitClaimOutcome::Claimed));
    }

    #[test]
    fn submit_claim_guard_can_retain_claim_on_drop() {
        let store = Arc::new(SubmitIdempotencyStore::open(":memory:").expect("open store"));
        let claim = store
            .load_cached_or_claim_submit("order-2", "req-1", 30)
            .expect("claim");
        assert!(matches!(claim, SubmitClaimOutcome::Claimed));

        {
            let mut guard = SubmitClaimGuard::new(store.clone(), "order-2", "req-1");
            guard.retain_claim_on_drop();
        }

        let next = store
            .load_cached_or_claim_submit("order-2", "req-2", 30)
            .expect("claim after retained drop");
        assert!(matches!(next, SubmitClaimOutcome::InFlight));
    }
}
