use crate::live_portfolio::{
    evaluate_live_portfolio_snapshot, load_live_token_prices, load_live_token_quality,
    reject_metric, DiscoveryV2LivePortfolioStatus, LivePortfolioSnapshot,
    LIVE_PORTFOLIO_RPC_BATCH_SIZE,
};
use crate::metric::DiscoveryV2WalletMetric;
use crate::status::DiscoveryV2CandidateWalletSource;
use anyhow::Result;
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::SqliteDiscoveryStore;
use std::thread;

pub(super) fn process_live_portfolio_candidate_rows(
    store: &SqliteDiscoveryStore,
    client: &crate::live_portfolio_rpc::LivePortfolioRpcClient,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: &crate::policy::DiscoveryV2BuildOptions,
    metrics: &mut [DiscoveryV2WalletMetric],
    rows: &[(usize, String, &'static str)],
    accept_limit: usize,
    source_cohort: &'static str,
    status: &mut DiscoveryV2LivePortfolioStatus,
    candidates: &mut Vec<String>,
    candidate_sources: &mut Vec<DiscoveryV2CandidateWalletSource>,
    live_reject_reasons: &mut Vec<String>,
) -> Result<()> {
    let mut accepted = 0usize;
    for batch in rows.chunks(LIVE_PORTFOLIO_RPC_BATCH_SIZE) {
        let snapshots = fetch_live_portfolio_batch(client, batch);
        for (index, snapshot) in snapshots {
            if accepted >= accept_limit {
                break;
            }
            let metric = &mut metrics[index];
            status.checked_wallets += 1;
            let snapshot = match snapshot {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    let reason = "live_portfolio_rpc_unavailable";
                    reject_metric(metric, reason);
                    live_reject_reasons.push(reason.to_string());
                    status.rpc_failures += 1;
                    status.rejected_wallets += 1;
                    continue;
                }
            };
            let quality_cache = load_live_token_quality(store, &snapshot)?;
            let price_cache = load_live_token_prices(store, &snapshot, options.now)?;
            let evaluation = evaluate_live_portfolio_snapshot(
                &snapshot,
                discovery,
                shadow,
                &price_cache,
                &quality_cache,
                options.now,
            );
            metric.live_sol_balance = Some(evaluation.sol_balance);
            metric.live_token_value_sol = Some(evaluation.token_value_sol);
            metric.live_token_positions = Some(evaluation.token_positions);
            metric.live_tradable_token_positions = Some(evaluation.tradable_token_positions);
            if evaluation.accepted {
                accepted += 1;
                status.accepted_wallets += 1;
                candidates.push(metric.wallet_id.clone());
                candidate_sources.push(DiscoveryV2CandidateWalletSource {
                    wallet_id: metric.wallet_id.clone(),
                    source_cohort: source_cohort.to_string(),
                });
            } else {
                let reason = evaluation
                    .reject_reason
                    .unwrap_or("live_portfolio_rejected");
                reject_metric(metric, reason);
                live_reject_reasons.push(reason.to_string());
                status.rejected_wallets += 1;
            }
        }
        if accepted >= accept_limit {
            break;
        }
    }
    Ok(())
}

fn fetch_live_portfolio_batch(
    client: &crate::live_portfolio_rpc::LivePortfolioRpcClient,
    batch: &[(usize, String, &'static str)],
) -> Vec<(usize, Result<LivePortfolioSnapshot>)> {
    thread::scope(|scope| {
        let handles = batch
            .iter()
            .map(|(index, wallet_id, _)| {
                let client = client.clone();
                let wallet_id = wallet_id.clone();
                scope.spawn(move || (*index, client.fetch_snapshot(&wallet_id)))
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("live portfolio RPC worker panicked"))
            .collect()
    })
}
