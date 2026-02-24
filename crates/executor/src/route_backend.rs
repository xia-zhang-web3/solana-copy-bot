#[derive(Clone, Debug)]
pub(crate) struct RouteBackend {
    pub(crate) submit_url: String,
    pub(crate) submit_fallback_url: Option<String>,
    pub(crate) simulate_url: String,
    pub(crate) simulate_fallback_url: Option<String>,
    pub(crate) primary_auth_token: Option<String>,
    pub(crate) fallback_auth_token: Option<String>,
    pub(crate) send_rpc_url: Option<String>,
    pub(crate) send_rpc_fallback_url: Option<String>,
    pub(crate) send_rpc_primary_auth_token: Option<String>,
    pub(crate) send_rpc_fallback_auth_token: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendRpcEndpointChainError {
    FallbackWithoutPrimary,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum UpstreamAction {
    Simulate,
    Submit,
}

impl UpstreamAction {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Simulate => "simulate",
            Self::Submit => "submit",
        }
    }
}

impl RouteBackend {
    pub(crate) fn endpoint_chain(&self, action: UpstreamAction) -> Vec<&str> {
        let mut endpoints = Vec::with_capacity(2);
        match action {
            UpstreamAction::Simulate => {
                endpoints.push(self.simulate_url.as_str());
                if let Some(url) = self.simulate_fallback_url.as_deref() {
                    endpoints.push(url);
                }
            }
            UpstreamAction::Submit => {
                endpoints.push(self.submit_url.as_str());
                if let Some(url) = self.submit_fallback_url.as_deref() {
                    endpoints.push(url);
                }
            }
        }
        endpoints
    }

    pub(crate) fn auth_token_for_attempt(
        &self,
        _action: UpstreamAction,
        attempt_idx: usize,
    ) -> Option<&str> {
        if attempt_idx == 0 {
            return self.primary_auth_token.as_deref();
        }
        self.fallback_auth_token.as_deref()
    }

    pub(crate) fn send_rpc_endpoint_chain(&self) -> Vec<(&str, Option<&str>)> {
        let mut endpoints = Vec::with_capacity(2);
        if let Some(url) = self.send_rpc_url.as_deref() {
            endpoints.push((url, self.send_rpc_primary_auth_token.as_deref()));
        }
        if let Some(url) = self.send_rpc_fallback_url.as_deref() {
            endpoints.push((url, self.send_rpc_fallback_auth_token.as_deref()));
        }
        endpoints
    }

    pub(crate) fn send_rpc_endpoint_chain_checked(
        &self,
    ) -> Result<Vec<(&str, Option<&str>)>, SendRpcEndpointChainError> {
        if self.send_rpc_url.is_none() && self.send_rpc_fallback_url.is_some() {
            return Err(SendRpcEndpointChainError::FallbackWithoutPrimary);
        }
        Ok(self.send_rpc_endpoint_chain())
    }
}

#[cfg(test)]
mod tests {
    use super::{RouteBackend, SendRpcEndpointChainError, UpstreamAction};

    fn sample_backend() -> RouteBackend {
        RouteBackend {
            submit_url: "https://submit.primary".to_string(),
            submit_fallback_url: Some("https://submit.fallback".to_string()),
            simulate_url: "https://simulate.primary".to_string(),
            simulate_fallback_url: Some("https://simulate.fallback".to_string()),
            primary_auth_token: Some("primary-token".to_string()),
            fallback_auth_token: Some("fallback-token".to_string()),
            send_rpc_url: None,
            send_rpc_fallback_url: None,
            send_rpc_primary_auth_token: None,
            send_rpc_fallback_auth_token: None,
        }
    }

    #[test]
    fn endpoint_chain_respects_action_and_fallback() {
        let backend = sample_backend();
        let submit_chain = backend.endpoint_chain(UpstreamAction::Submit);
        assert_eq!(
            submit_chain,
            vec!["https://submit.primary", "https://submit.fallback"]
        );

        let simulate_chain = backend.endpoint_chain(UpstreamAction::Simulate);
        assert_eq!(
            simulate_chain,
            vec!["https://simulate.primary", "https://simulate.fallback"]
        );
    }

    #[test]
    fn auth_token_for_attempt_uses_primary_then_fallback() {
        let backend = sample_backend();
        assert_eq!(
            backend.auth_token_for_attempt(UpstreamAction::Submit, 0),
            Some("primary-token")
        );
        assert_eq!(
            backend.auth_token_for_attempt(UpstreamAction::Submit, 1),
            Some("fallback-token")
        );
        assert_eq!(
            backend.auth_token_for_attempt(UpstreamAction::Simulate, 3),
            Some("fallback-token")
        );
    }

    #[test]
    fn send_rpc_endpoint_chain_preserves_primary_fallback_and_auth() {
        let mut backend = sample_backend();
        backend.send_rpc_url = Some("https://send-rpc.primary".to_string());
        backend.send_rpc_fallback_url = Some("https://send-rpc.fallback".to_string());
        backend.send_rpc_primary_auth_token = Some("send-primary-token".to_string());
        backend.send_rpc_fallback_auth_token = Some("send-fallback-token".to_string());

        let chain = backend.send_rpc_endpoint_chain();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].0, "https://send-rpc.primary");
        assert_eq!(chain[0].1, Some("send-primary-token"));
        assert_eq!(chain[1].0, "https://send-rpc.fallback");
        assert_eq!(chain[1].1, Some("send-fallback-token"));
    }

    #[test]
    fn route_backend_send_rpc_endpoint_chain_checked_rejects_fallback_without_primary() {
        let mut backend = sample_backend();
        backend.send_rpc_url = None;
        backend.send_rpc_fallback_url = Some("https://send-rpc.fallback".to_string());

        let error = backend
            .send_rpc_endpoint_chain_checked()
            .expect_err("fallback without primary must reject");
        assert_eq!(error, SendRpcEndpointChainError::FallbackWithoutPrimary);
    }
}
