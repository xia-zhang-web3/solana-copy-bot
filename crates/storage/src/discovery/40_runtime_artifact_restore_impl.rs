use super::validate_runtime_artifact_snapshot_shape;
use crate::{DiscoveryRuntimeArtifact, SqliteStore};
use anyhow::Result;
use chrono::{DateTime, Utc};

impl SqliteStore {
    pub fn restore_discovery_runtime_artifact(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        _restored_at: DateTime<Utc>,
        _bootstrap_degraded: bool,
    ) -> Result<()> {
        // Legacy restore remains quarantined before schema or row mutation.
        // The strict discovery restore verdict is currently a tested contract
        // for a future V2 restore operator, not an active legacy write path.
        validate_runtime_artifact_snapshot_shape(artifact)?;
        Err(anyhow::anyhow!(
            "legacy copybot-storage runtime artifact restore is quarantined; use the discovery v2 storage-core restore path"
        ))
    }
}
