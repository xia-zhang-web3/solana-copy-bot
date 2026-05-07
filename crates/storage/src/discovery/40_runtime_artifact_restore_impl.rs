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
        validate_runtime_artifact_snapshot_shape(artifact)?;
        Err(anyhow::anyhow!(
            "legacy copybot-storage runtime artifact restore is quarantined; use the discovery v2 storage-core restore path"
        ))
    }
}
