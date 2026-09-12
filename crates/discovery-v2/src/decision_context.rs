use crate::DiscoveryV2BuildOptions;
use copybot_config::{DiscoveryConfig, ShadowConfig};

/// Explicit current policy and decision clock. Callers obtain now after loading
/// or preparing data; it is never inferred from the persisted status.now.
#[derive(Debug, Clone, Copy)]
pub struct DiscoveryV2DecisionContext<'a> {
    pub discovery: &'a DiscoveryConfig,
    pub shadow: &'a ShadowConfig,
    pub options: &'a DiscoveryV2BuildOptions,
}

impl<'a> DiscoveryV2DecisionContext<'a> {
    pub fn new(
        discovery: &'a DiscoveryConfig,
        shadow: &'a ShadowConfig,
        options: &'a DiscoveryV2BuildOptions,
    ) -> Self {
        Self {
            discovery,
            shadow,
            options,
        }
    }
}
