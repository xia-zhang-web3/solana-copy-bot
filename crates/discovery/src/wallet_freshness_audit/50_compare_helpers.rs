fn publication_truth_for_audit(
    publication_state: Option<&DiscoveryPublicationStateRow>,
) -> Option<RuntimePublishedUniverseTruth> {
    let publication_state = publication_state?;
    DiscoveryService::runtime_publication_truth_from_state(publication_state.clone())
}

fn compare_wallet_universes(left: &[String], right: &[String]) -> WalletUniverseComparison {
    let left_set: BTreeSet<_> = left.iter().cloned().collect();
    let right_set: BTreeSet<_> = right.iter().cloned().collect();
    WalletUniverseComparison {
        left_count: left_set.len(),
        right_count: right_set.len(),
        overlap_count: left_set.intersection(&right_set).count(),
        exact_match: left_set == right_set,
        only_left: left_set.difference(&right_set).cloned().collect(),
        only_right: right_set.difference(&left_set).cloned().collect(),
    }
}

fn sorted_wallets_from_iter<I>(wallets: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut wallets: Vec<String> = wallets.into_iter().collect();
    wallets.sort();
    wallets.dedup();
    wallets
}

fn stable_wallets(samples: &[WalletFreshnessRawCycleSample]) -> Vec<String> {
    let mut intersection = samples
        .first()
        .map(|sample| {
            sample
                .top_wallet_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for sample in &samples[1..] {
        let sample_set: BTreeSet<_> = sample.top_wallet_ids.iter().cloned().collect();
        intersection = intersection
            .intersection(&sample_set)
            .cloned()
            .collect::<BTreeSet<_>>();
    }
    intersection.into_iter().collect()
}

fn compare_wallet_recent_activity_rows(
    left: &WalletRecentActivityCountRow,
    right: &WalletRecentActivityCountRow,
) -> std::cmp::Ordering {
    right
        .row_count
        .cmp(&left.row_count)
        .then_with(|| right.latest_ts.cmp(&left.latest_ts))
        .then_with(|| left.wallet_id.cmp(&right.wallet_id))
}

fn dominant_wallet_share(
    counts: &[WalletRecentActivityCountRow],
    total_count: usize,
) -> Option<f64> {
    if counts.is_empty() || total_count == 0 {
        return None;
    }
    let dominant = counts.iter().map(|row| row.row_count).max().unwrap_or(0);
    Some(dominant as f64 / total_count as f64)
}

fn activity_broadly_distributed(
    selected_wallet_count: usize,
    active_wallet_count: usize,
    dominant_share: Option<f64>,
) -> bool {
    if active_wallet_count == 0 {
        return false;
    }
    if selected_wallet_count <= 1 {
        return active_wallet_count == 1;
    }
    active_wallet_count >= selected_wallet_count.min(2)
        && dominant_share.is_some_and(|share| share <= 0.80)
}
