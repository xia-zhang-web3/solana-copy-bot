//! Variant A has fixed financial limits. Configuration only selects/activates identity.
use anyhow::{ensure, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct TinyExperimentConfig {
    pub policy_mode: TinyPolicyMode,
    pub id: Option<String>,
    pub activate: bool,
}
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TinyPolicyMode {
    #[default]
    DecodedAmount,
    ProtectedNativeCapital,
}
impl TinyExperimentConfig {
    pub fn validate(&self, wallet: &str) -> Result<()> {
        if let Some(id) = &self.id {
            ensure!(
                !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
                "tiny_budget_invalid_id"
            );
        }
        ensure!(
            !self.activate || self.id.is_some(),
            "tiny_budget_activation_id_missing"
        );
        ensure!(
            self.id.is_none() || (!wallet.is_empty() && wallet.trim() == wallet),
            "tiny_budget_wallet_missing"
        );
        Ok(())
    }
}
