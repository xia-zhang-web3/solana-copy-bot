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
    let snapshot_wallet_ids: HashSet<String> = artifact
        .published_wallet_metrics_snapshot
        .iter()
        .map(|row| row.wallet_id.clone())
        .collect();
    let published_wallet_ids = artifact
        .publication_state
        .published_wallet_ids
        .as_ref()
        .expect("validated complete publication truth above");
    if published_wallet_ids
        .iter()
        .any(|wallet_id| !snapshot_wallet_ids.contains(wallet_id))
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication wallet ids are not fully covered by the published wallet_metrics snapshot"
        ));
    }
    Ok(())
}
