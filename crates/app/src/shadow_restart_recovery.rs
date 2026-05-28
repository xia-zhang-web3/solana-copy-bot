use crate::*;
use copybot_ingestion::fetch_recent_swaps_for_wallets;
use copybot_shadow::ShadowProcessOutcome;

#[derive(Debug, Clone, Default)]
pub(crate) struct ShadowRestartRecoverySummary {
    pub(crate) enabled: bool,
    pub(crate) skipped_reason: Option<&'static str>,
    pub(crate) wallets_scanned: usize,
    pub(crate) signatures_seen: usize,
    pub(crate) transactions_fetched: usize,
    pub(crate) rpc_errors: usize,
    pub(crate) swaps_fetched: usize,
    pub(crate) sell_candidates: usize,
    pub(crate) recovered_sells: usize,
    pub(crate) skipped_without_open_lot: usize,
    pub(crate) skipped_non_sell: usize,
    pub(crate) duplicate_recent: usize,
    pub(crate) observed_persist_errors: usize,
    pub(crate) realized_pnl_sol: f64,
}

pub(crate) async fn recover_shadow_restart_gap(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    shadow: &ShadowService,
    ingestion_config: &IngestionConfig,
    shadow_config: &ShadowConfig,
    open_shadow_lots: &mut HashSet<(String, String)>,
    recent_swap_signatures: &mut HashSet<String>,
    recent_swap_signature_order: &mut VecDeque<String>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
) -> Result<ShadowRestartRecoverySummary> {
    if !shadow_config.restart_recovery_enabled {
        return Ok(ShadowRestartRecoverySummary {
            enabled: false,
            skipped_reason: Some("disabled"),
            ..ShadowRestartRecoverySummary::default()
        });
    }
    if open_shadow_lots.is_empty() {
        return Ok(ShadowRestartRecoverySummary {
            enabled: true,
            skipped_reason: Some("no_open_shadow_lots"),
            ..ShadowRestartRecoverySummary::default()
        });
    }

    let now = Utc::now();
    let window_seconds = shadow_config.restart_recovery_window_seconds.max(1);
    let since = now - chrono::Duration::seconds(window_seconds.min(i64::MAX as u64) as i64);
    let mut wallets = open_shadow_lots
        .iter()
        .map(|(wallet, _)| wallet.clone())
        .collect::<Vec<_>>();
    wallets.sort();
    wallets.dedup();

    let backfill = fetch_recent_swaps_for_wallets(
        ingestion_config,
        wallets,
        since,
        shadow_config.restart_recovery_signature_limit,
    )
    .await
    .context("failed fetching restart recovery swaps")?;

    let mut summary = ShadowRestartRecoverySummary {
        enabled: true,
        wallets_scanned: backfill.wallets_scanned,
        signatures_seen: backfill.signatures_seen,
        transactions_fetched: backfill.transactions_fetched,
        rpc_errors: backfill.rpc_errors,
        swaps_fetched: backfill.swaps.len(),
        ..ShadowRestartRecoverySummary::default()
    };

    apply_shadow_restart_recovery_swaps(
        store,
        observed_swap_writer,
        shadow,
        backfill.swaps,
        open_shadow_lots,
        recent_swap_signatures,
        recent_swap_signature_order,
        shadow_drop_reason_counts,
        shadow_drop_stage_counts,
        &mut summary,
    )
    .await?;

    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
async fn apply_shadow_restart_recovery_swaps(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    shadow: &ShadowService,
    swaps: Vec<SwapEvent>,
    open_shadow_lots: &mut HashSet<(String, String)>,
    recent_swap_signatures: &mut HashSet<String>,
    recent_swap_signature_order: &mut VecDeque<String>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    summary: &mut ShadowRestartRecoverySummary,
) -> Result<()> {
    for swap in swaps {
        let Some(side) = classify_swap_side(&swap) else {
            summary.skipped_non_sell = summary.skipped_non_sell.saturating_add(1);
            continue;
        };
        if !matches!(side, ShadowSwapSide::Sell) {
            summary.skipped_non_sell = summary.skipped_non_sell.saturating_add(1);
            continue;
        }
        summary.sell_candidates = summary.sell_candidates.saturating_add(1);

        let key = shadow_task_key_for_swap(&swap, side);
        let key_tuple = (key.wallet.clone(), key.token.clone());
        if !open_shadow_lots.contains(&key_tuple) {
            summary.skipped_without_open_lot = summary.skipped_without_open_lot.saturating_add(1);
            continue;
        }

        if !note_recent_swap_signature(
            recent_swap_signatures,
            recent_swap_signature_order,
            &swap.signature,
        ) {
            summary.duplicate_recent = summary.duplicate_recent.saturating_add(1);
            continue;
        }

        if let Err(error) = persist_relevant_observed_swap(
            observed_swap_writer,
            recent_swap_signatures,
            recent_swap_signature_order,
            &swap,
        )
        .await
        {
            if observed_swap_writer_error_requires_restart(&error) {
                return Err(error)
                    .context("observed swap writer is no longer running during restart recovery");
            }
            summary.observed_persist_errors = summary.observed_persist_errors.saturating_add(1);
            warn!(
                error = %error,
                signature = %swap.signature,
                "failed persisting restart recovery observed swap; continuing sell recovery"
            );
        }

        let outcome = shadow.process_restart_recovery_sell(store, &swap, Utc::now());
        if let Ok(ShadowProcessOutcome::Recorded(result)) = &outcome {
            if result.side == "sell" && result.closed_qty > 0.0 {
                summary.recovered_sells = summary.recovered_sells.saturating_add(1);
                summary.realized_pnl_sol += result.realized_pnl_sol;
            }
        }
        handle_shadow_task_output(
            ShadowTaskOutput {
                signature: swap.signature,
                key,
                outcome,
            },
            open_shadow_lots,
            shadow_drop_reason_counts,
            shadow_drop_stage_counts,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn make_test_store(name: &str) -> Result<(SqliteStore, String)> {
        let unique = format!(
            "{}-{}-{}",
            name,
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("copybot-app-{unique}.db"));
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((store, db_path.to_string_lossy().into_owned()))
    }

    #[tokio::test]
    async fn restart_recovery_swaps_close_open_sell_and_persist_observed_swap() -> Result<()> {
        let (store, db_path) = make_test_store("restart-recovery-apply")?;
        let writer = ObservedSwapWriter::start_for_test(db_path.clone(), 8, 8)?;

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.max_signal_lag_seconds = 30;
        cfg.quality_gates_enabled = false;
        let shadow = ShadowService::new(cfg);

        let opened_ts = DateTime::parse_from_rfc3339("2026-05-28T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = opened_ts + chrono::Duration::minutes(5);
        store.insert_shadow_lot("leader-wallet", "TokenMint", 500.0, 0.5, opened_ts)?;
        let mut open_shadow_lots = store.list_shadow_open_pairs()?;

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 1000.0,
            amount_out: 1.0,
            signature: "sig-restart-recovery-sell".to_string(),
            slot: 10,
            ts_utc: sell_ts,
            exact_amounts: None,
        };

        let mut recent = HashSet::new();
        let mut recent_order = VecDeque::new();
        let mut reason_counts = BTreeMap::new();
        let mut stage_counts = BTreeMap::new();
        let mut summary = ShadowRestartRecoverySummary::default();
        apply_shadow_restart_recovery_swaps(
            &store,
            &writer,
            &shadow,
            vec![sell.clone()],
            &mut open_shadow_lots,
            &mut recent,
            &mut recent_order,
            &mut reason_counts,
            &mut stage_counts,
            &mut summary,
        )
        .await?;

        assert_eq!(summary.sell_candidates, 1);
        assert_eq!(summary.recovered_sells, 1);
        assert_eq!(store.shadow_open_lots_count()?, 0);
        assert!(open_shadow_lots.is_empty());
        let persisted = store.load_observed_swaps_since(sell_ts - chrono::Duration::seconds(1))?;
        assert!(
            persisted
                .iter()
                .any(|swap| swap.signature == "sig-restart-recovery-sell"),
            "recovered sell should also be persisted into observed swaps"
        );

        writer.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
}
