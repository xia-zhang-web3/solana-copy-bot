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
pub(crate) async fn apply_shadow_restart_recovery_swaps(
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
