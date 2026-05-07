use super::*;

pub(super) fn open_store(path: &Path) -> Result<SqliteStore> {
    let mut store = SqliteStore::open(path)?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;
    Ok(store)
}

pub(super) fn freshness_test_config() -> DiscoveryConfig {
    let mut config = DiscoveryConfig::default();
    config.scoring_window_days = 2;
    config.decay_window_days = 2;
    config.observed_swaps_retention_days = 14;
    config.refresh_seconds = 600;
    config.metric_snapshot_interval_seconds = 60;
    config.follow_top_n = 2;
    config.min_score = 0.0;
    config.min_trades = 1;
    config.min_active_days = 1;
    config.min_leader_notional_sol = 0.0;
    config.min_buy_count = 1;
    config.min_tradable_ratio = 0.0;
    config.max_rug_ratio = 1.0;
    config.max_window_swaps_in_memory = 256;
    config.max_fetch_swaps_per_cycle = 256;
    config.max_fetch_pages_per_cycle = 8;
    config.fetch_time_budget_ms = 1_000;
    config.thin_market_min_unique_traders = 1;
    config
}

pub(super) fn expected_metrics_window_start(
    config: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
}

pub(super) fn seed_publication_truth(
    store: &SqliteStore,
    config: &DiscoveryConfig,
    last_published_at: DateTime<Utc>,
    now: DateTime<Utc>,
    runtime_mode: DiscoveryRuntimeMode,
    wallet_ids: &[&str],
) -> Result<()> {
    store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
        runtime_mode,
        reason: "test-publication".to_string(),
        last_published_at: Some(last_published_at),
        last_published_window_start: Some(expected_metrics_window_start(config, now)),
        published_scoring_source: Some("discovery_v2_operational_window".to_string()),
        published_wallet_ids: Some(
            wallet_ids
                .iter()
                .map(|wallet| (*wallet).to_string())
                .collect(),
        ),
    })
}

pub(super) fn seed_ranked_wallet_window(
    store: &SqliteStore,
    now: DateTime<Utc>,
    wallets: &[(&str, &str, usize, i64)],
) -> Result<()> {
    let coverage_start = now - Duration::days(2);
    for (wallet_idx, (wallet_id, mint, trades, offset_minutes)) in wallets.iter().enumerate() {
        if *trades == 0 {
            continue;
        }
        store.insert_observed_swap(&buy_swap(
            &format!("{wallet_id}-head"),
            wallet_id,
            mint,
            10_000 + wallet_idx as u64 * 100,
            coverage_start + Duration::minutes(wallet_idx as i64),
        ))?;
        for trade_idx in 1..*trades {
            let ts = now
                - Duration::minutes(*offset_minutes)
                - Duration::minutes(trade_idx as i64)
                - Duration::minutes((wallet_idx as i64) * 2);
            store.insert_observed_swap(&buy_swap(
                &format!("{wallet_id}-{trade_idx}"),
                wallet_id,
                mint,
                10_000 + wallet_idx as u64 * 100 + trade_idx as u64,
                ts,
            ))?;
        }
    }
    Ok(())
}

pub(super) fn seed_recent_only_ranked_wallet_window(
    store: &SqliteStore,
    now: DateTime<Utc>,
    wallets: &[(&str, &str, usize, i64)],
) -> Result<()> {
    for (wallet_idx, (wallet_id, mint, trades, offset_minutes)) in wallets.iter().enumerate() {
        for trade_idx in 0..*trades {
            let ts = now
                - Duration::minutes(*offset_minutes)
                - Duration::minutes(trade_idx as i64)
                - Duration::minutes((wallet_idx as i64) * 2);
            store.insert_observed_swap(&buy_swap(
                &format!("{wallet_id}-recent-{trade_idx}"),
                wallet_id,
                mint,
                20_000 + wallet_idx as u64 * 100 + trade_idx as u64,
                ts,
            ))?;
        }
    }
    Ok(())
}

pub(super) fn persist_capture(store: &SqliteStore, capture: WalletFreshnessCaptureSnapshot) -> Result<()> {
    store.append_discovery_wallet_freshness_capture(&capture.to_storage_write()?)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn sample_capture_snapshot(
    captured_at: DateTime<Utc>,
    audit_verdict: WalletFreshnessVerdict,
    shadow_verdict: WalletShadowSignalVerdict,
    published_wallet_ids: &[&str],
    active_follow_wallet_ids: &[&str],
    current_raw_top_wallet_ids: &[&str],
    selected_raw_wallet_ids: &[&str],
    selected_shadow_wallet_ids: &[&str],
    rotation_entered: &[&str],
    rotation_left: &[&str],
) -> WalletFreshnessCaptureSnapshot {
    let published_wallet_ids = published_wallet_ids
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let active_follow_wallet_ids = active_follow_wallet_ids
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let current_raw_top_wallet_ids = current_raw_top_wallet_ids
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let selected_raw_wallet_ids = selected_raw_wallet_ids
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let selected_shadow_wallet_ids = selected_shadow_wallet_ids
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let rotation_entered = rotation_entered
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let rotation_left = rotation_left
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let raw_counts = selected_raw_wallet_ids
        .iter()
        .enumerate()
        .map(|(idx, wallet_id)| WalletRecentActivityCountRow {
            wallet_id: wallet_id.clone(),
            row_count: (selected_raw_wallet_ids.len().saturating_sub(idx)).max(1),
            latest_ts: captured_at - Duration::seconds(idx as i64),
        })
        .collect::<Vec<_>>();
    let shadow_counts = selected_shadow_wallet_ids
        .iter()
        .enumerate()
        .map(|(idx, wallet_id)| WalletRecentActivityCountRow {
            wallet_id: wallet_id.clone(),
            row_count: (selected_shadow_wallet_ids.len().saturating_sub(idx)).max(1),
            latest_ts: captured_at - Duration::seconds(idx as i64),
        })
        .collect::<Vec<_>>();
    let selected_wallet_count = active_follow_wallet_ids.len();

    WalletFreshnessCaptureSnapshot {
        capture_id: None,
        captured_at,
        capture_age_seconds: None,
        within_recent_horizon: None,
        recent_cycles: 3,
        audit: WalletFreshnessAuditReport {
            now: captured_at,
            window_start: captured_at - Duration::days(2),
            verdict: audit_verdict,
            reason: audit_reason(audit_verdict).to_string(),
            follow_top_n: current_raw_top_wallet_ids.len(),
            publication_truth_available: true,
            publication_runtime_mode: Some("healthy".to_string()),
            publication_recent_under_gate: true,
            latest_publication_ts: Some(captured_at - Duration::seconds(60)),
            publication_age_seconds: Some(60),
            latest_publication_window_start: Some(captured_at - Duration::days(2)),
            published_scoring_source: Some("discovery_v2_operational_window".to_string()),
            published_wallet_ids: published_wallet_ids.clone(),
            active_follow_wallet_ids: active_follow_wallet_ids.clone(),
            current_raw_top_wallet_ids: current_raw_top_wallet_ids.clone(),
            published_vs_current_raw: WalletUniverseComparison {
                left_count: published_wallet_ids.len(),
                right_count: current_raw_top_wallet_ids.len(),
                overlap_count: published_wallet_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .intersection(
                        &current_raw_top_wallet_ids
                            .iter()
                            .collect::<std::collections::BTreeSet<_>>(),
                    )
                    .count(),
                exact_match: compare_wallet_universes(
                    &published_wallet_ids,
                    &current_raw_top_wallet_ids,
                )
                .exact_match,
                only_left: compare_wallet_universes(
                    &published_wallet_ids,
                    &current_raw_top_wallet_ids,
                )
                .only_left,
                only_right: compare_wallet_universes(
                    &published_wallet_ids,
                    &current_raw_top_wallet_ids,
                )
                .only_right,
            },
            active_follow_vs_current_raw: compare_wallet_universes(
                &active_follow_wallet_ids,
                &current_raw_top_wallet_ids,
            ),
            active_follow_vs_published: compare_wallet_universes(
                &active_follow_wallet_ids,
                &published_wallet_ids,
            ),
            raw_truth: WalletFreshnessRawTruthStatus {
                sufficient: audit_verdict != WalletFreshnessVerdict::InsufficientRawTruth,
                reason: if audit_verdict == WalletFreshnessVerdict::InsufficientRawTruth {
                    "observed_swaps_coverage_ends_before_freshness_gate".to_string()
                } else {
                    "full_scoring_window_raw_truth_available".to_string()
                },
                observed_swaps_loaded: 10,
                eligible_wallet_count: current_raw_top_wallet_ids.len(),
                top_wallet_count: current_raw_top_wallet_ids.len(),
                short_retention_configured: false,
                covered_since: Some(captured_at - Duration::days(2)),
                covered_through_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: captured_at,
                    slot: 1,
                    signature: format!("sig-{}", captured_at.timestamp()),
                }),
                covered_through_lag_seconds: Some(30),
                tail_fresh_within_runtime_lag: audit_verdict
                    != WalletFreshnessVerdict::InsufficientRawTruth,
                runtime_freshness_lag_seconds: 600,
                total_observed_swaps_rows: 10,
            },
            rotation: WalletFreshnessRotationSignal {
                signal_available: true,
                reason: None,
                cycles_requested: 3,
                cycles_completed: 3,
                sample_interval_seconds: 600,
                overlap_with_previous_cycle: Some(current_raw_top_wallet_ids.len()),
                entered_since_previous_cycle: rotation_entered.clone(),
                left_since_previous_cycle: rotation_left.clone(),
                stable_wallets_across_cycles: current_raw_top_wallet_ids.clone(),
                unique_wallet_count_across_cycles: current_raw_top_wallet_ids.len()
                    + rotation_entered.len(),
                samples: vec![WalletFreshnessRawCycleSample {
                    sample_now: captured_at,
                    window_start: captured_at - Duration::days(2),
                    observed_swaps_loaded: 10,
                    eligible_wallet_count: current_raw_top_wallet_ids.len(),
                    top_wallet_ids: current_raw_top_wallet_ids.clone(),
                }],
            },
        },
        shadow_signal: WalletFreshnessShadowSignalEvidence {
            recent_window_start: captured_at - Duration::minutes(30),
            recent_window_end: captured_at,
            evidence_lookback_seconds: Some(1_800),
            selected_wallet_ids: active_follow_wallet_ids,
            selected_wallet_count,
            selected_wallets_with_recent_raw_activity: selected_raw_wallet_ids.len(),
            selected_wallets_with_recent_shadow_signal: selected_shadow_wallet_ids.len(),
            recent_raw_swap_count: raw_counts.iter().map(|row| row.row_count).sum(),
            recent_shadow_signal_count: shadow_counts.iter().map(|row| row.row_count).sum(),
            recent_raw_activity_wallet_ids: selected_raw_wallet_ids,
            recent_shadow_signal_wallet_ids: selected_shadow_wallet_ids,
            recent_raw_activity_by_wallet: raw_counts,
            recent_shadow_signal_by_wallet: shadow_counts,
            raw_activity_top_wallet_share: Some(1.0),
            shadow_signal_top_wallet_share: Some(1.0),
            raw_activity_broadly_distributed: false,
            shadow_signal_broadly_distributed: matches!(
                shadow_verdict,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed
            ),
            verdict: shadow_verdict,
            reason: shadow_reason(shadow_verdict).to_string(),
        },
    }
}

pub(super) fn audit_reason(verdict: WalletFreshnessVerdict) -> &'static str {
    match verdict {
        WalletFreshnessVerdict::FreshCurrent => "published_and_active_match_current_raw_top_n",
        WalletFreshnessVerdict::DriftingButAcceptable => {
            "publication_recent_under_gate_but_current_raw_top_n_has_rotated"
        }
        WalletFreshnessVerdict::StalePublicationTruth => {
            "published_universe_drifted_from_current_raw_top_n"
        }
        WalletFreshnessVerdict::InsufficientRawTruth => {
            "observed_swaps_coverage_ends_before_freshness_gate"
        }
        WalletFreshnessVerdict::FailClosedNoPublicationTruth => "no_complete_publication_truth",
    }
}

pub(super) fn shadow_reason(verdict: WalletShadowSignalVerdict) -> &'static str {
    match verdict {
        WalletShadowSignalVerdict::NoSelectedWallets => "no_active_follow_wallets_selected",
        WalletShadowSignalVerdict::NoRecentSelectedRawActivity => {
            "no_recent_observed_swaps_from_selected_wallets"
        }
        WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals => {
            "selected_wallets_emit_recent_raw_activity_but_no_shadow_signals"
        }
        WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated => {
            "recent_shadow_signals_present_but_concentrated_in_few_selected_wallets"
        }
        WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed => {
            "recent_shadow_signals_present_across_multiple_selected_wallets"
        }
    }
}

pub(super) fn buy_swap(
    signature: &str,
    wallet: &str,
    mint: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        signature: signature.to_string(),
        wallet: wallet.to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: mint.to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

pub(super) fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("valid rfc3339")
        .with_timezone(&Utc)
}
