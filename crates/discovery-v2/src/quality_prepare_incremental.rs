use crate::quality_prepare::{
    finish_quality_prepare_from_evidence, DiscoveryV2PrepareQualityOptions,
    DiscoveryV2PrepareQualityReport,
};
use crate::token_market::{is_sol_buy, sol_leg_token_and_notional};
use anyhow::{Context, Result};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{
    DiscoveryRuntimeCursor, DiscoveryV2QualityPrepareUpsert, SqliteDiscoveryStore,
};
use std::time::{Duration as StdDuration, Instant};

pub(crate) fn prepare_discovery_v2_quality_incremental(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: DiscoveryV2PrepareQualityOptions,
) -> Result<DiscoveryV2PrepareQualityReport> {
    anyhow::ensure!(
        options.commit,
        "incremental discovery v2 quality prepare requires --commit on a writable DB"
    );
    let window_start = options.window_start();
    let deadline = Instant::now() + StdDuration::from_millis(options.time_budget_ms);
    let state = store.discovery_v2_quality_prepare_state()?;
    let state_reset = state.as_ref().is_none_or(|state| {
        state.covered_from_ts > window_start || state.cursor.ts_utc > options.now
    });
    let cursor = state
        .as_ref()
        .filter(|_| !state_reset)
        .map(|state| state.cursor.clone());

    store.begin_discovery_v2_quality_prepare_update()?;
    let mut transaction_open = true;
    let result = (|| {
        if state_reset {
            store.clear_discovery_v2_quality_prepare_state()?;
        }
        let evidence_rows_pruned =
            store.prune_discovery_v2_quality_observed_evidence(window_start)?;
        let mut rows_seen = 0usize;
        let mut inserted = 0usize;
        let mut last_cursor = cursor.clone();
        let page = store
            .for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
                window_start,
                options.now,
                cursor.as_ref(),
                options.max_rows.saturating_add(1),
                deadline,
                |swap| {
                    rows_seen = rows_seen.saturating_add(1);
                    if rows_seen > options.max_rows {
                        return Ok(());
                    }
                    last_cursor = Some(DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    });
                    if let Some(upsert) = quality_prepare_upsert_from_swap(&swap) {
                        if store.insert_discovery_v2_quality_observed_evidence(&upsert)? {
                            inserted = inserted.saturating_add(1);
                        }
                    }
                    Ok(())
                },
            )
            .context("failed streaming incremental discovery v2 quality prepare window")?;
        let evidence_rows_available =
            store.discovery_v2_quality_observed_evidence_count(window_start, options.now)?;
        let aggregates =
            store.discovery_v2_quality_observed_evidence_aggregates(window_start, options.now)?;
        let window_truncated =
            rows_seen > options.max_rows || evidence_rows_available > options.max_rows;
        let mut report = finish_quality_prepare_from_evidence(
            store,
            discovery,
            shadow,
            &options,
            window_start,
            rows_seen,
            evidence_rows_available,
            evidence_rows_pruned,
            !state_reset && state.is_some(),
            state_reset,
            page.time_budget_exhausted,
            window_truncated,
            aggregates,
        )?;
        if evidence_rows_available == 0 {
            report
                .blockers
                .push("discovery_v2_quality_prepare_observed_window_empty".to_string());
        }
        if inserted == 0 && last_cursor.is_none() && evidence_rows_available > 0 {
            report
                .blockers
                .push("discovery_v2_quality_prepare_missing_cursor".to_string());
        }
        if report.blockers.is_empty() {
            if let Some(cursor) = last_cursor.as_ref() {
                store.persist_discovery_v2_quality_prepare_state(
                    window_start,
                    cursor,
                    options.now,
                )?;
            }
            report.committed = true;
            store.commit_discovery_v2_quality_prepare_update()?;
            transaction_open = false;
        } else {
            store.rollback_discovery_v2_quality_prepare_update();
            transaction_open = false;
            report.committed = false;
            report.upserted = 0;
        }
        Ok(report)
    })();
    if transaction_open {
        store.rollback_discovery_v2_quality_prepare_update();
    }
    result
}

fn quality_prepare_upsert_from_swap(swap: &SwapEvent) -> Option<DiscoveryV2QualityPrepareUpsert> {
    let (mint, sol_notional) = sol_leg_token_and_notional(swap)?;
    if !sol_notional.is_finite() || sol_notional <= 0.0 {
        return None;
    }
    Some(DiscoveryV2QualityPrepareUpsert {
        signature: swap.signature.clone(),
        mint: mint.to_string(),
        wallet_id: swap.wallet.clone(),
        ts_utc: swap.ts_utc,
        slot: swap.slot,
        sol_notional,
        is_buy: is_sol_buy(swap),
    })
}
