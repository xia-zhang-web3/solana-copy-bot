fn validate_runtime_artifact_snapshot_shape(artifact: &DiscoveryRuntimeArtifact) -> Result<()> {
    if artifact.format_version != DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION {
        return Err(anyhow::anyhow!(
            "unsupported discovery runtime artifact format_version={} expected={}",
            artifact.format_version,
            DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION
        ));
    }
    if !artifact.publication_state.has_complete_publication_truth() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication truth is incomplete"
        ));
    }
    if artifact.publication_state.runtime_mode != DiscoveryRuntimeMode::Healthy {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact restore requires healthy publication state"
        ));
    }
    if artifact.publication_state.published_scoring_source.is_none()
        || artifact
            .publication_state
            .publication_policy_fingerprint
            .is_none()
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact restore requires complete publication identity"
        ));
    }
    let published_window_start = artifact
        .publication_state
        .last_published_window_start
        .expect("validated complete publication truth above");
    if artifact.published_wallet_metrics_snapshot.is_empty() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact is missing published wallet_metrics snapshot rows"
        ));
    }
    if artifact
        .published_wallet_metrics_snapshot
        .iter()
        .any(|row| row.window_start != published_window_start)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot rows do not match publication window_start={}",
            published_window_start.to_rfc3339()
        ));
    }
    let snapshot_wallet_ids: Vec<String> = artifact
        .published_wallet_metrics_snapshot
        .iter()
        .map(|row| row.wallet_id.clone())
        .collect();
    let snapshot_unique_wallet_ids: HashSet<String> =
        snapshot_wallet_ids.iter().cloned().collect();
    if snapshot_unique_wallet_ids.len() != snapshot_wallet_ids.len() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot contains duplicate wallet rows"
        ));
    }
    let published_wallet_ids = artifact
        .publication_state
        .published_wallet_ids
        .as_ref()
        .expect("validated complete publication truth above");
    let published_unique_wallet_ids: HashSet<String> =
        published_wallet_ids.iter().cloned().collect();
    if published_unique_wallet_ids.len() != published_wallet_ids.len() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication truth contains duplicate published wallet ids"
        ));
    }
    if snapshot_wallet_ids.len() != published_wallet_ids.len()
        || snapshot_unique_wallet_ids != published_unique_wallet_ids
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot must exactly match published wallet ids"
        ));
    }
    Ok(())
}
