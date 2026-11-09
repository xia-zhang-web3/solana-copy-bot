//! Explicit observation-only protected complete-block delivery contract.
use anyhow::{ensure, Result};
use serde::Deserialize;

pub const PROTECTED_COMPLETE_RECOVERY_V1: &str = "protected_complete_recovery_v1";
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectedRecoveryConfig {
    pub policy: String,
    pub checkpoint_directory: String,
    pub migrations_directory: String,
    pub max_scope_wallets: usize,
    pub max_scope_revisions: usize,
    pub provenance_policy: String,
}
impl ProtectedRecoveryConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.policy == PROTECTED_COMPLETE_RECOVERY_V1,
            "protected_recovery_policy"
        );
        ensure!(
            self.provenance_policy == "historical_missed_no_execution_v1",
            "protected_recovery_provenance_policy"
        );
        ensure!(
            std::path::Path::new(&self.checkpoint_directory).is_absolute(),
            "protected_recovery_absolute_checkpoint_directory"
        );
        ensure!(
            std::path::Path::new(&self.migrations_directory).is_absolute(),
            "protected_recovery_absolute_migrations_directory"
        );
        ensure!(
            (1..=128).contains(&self.max_scope_wallets)
                && (1..=128).contains(&self.max_scope_revisions),
            "protected_recovery_explicit_scope_bounds"
        );
        Ok(())
    }
}
