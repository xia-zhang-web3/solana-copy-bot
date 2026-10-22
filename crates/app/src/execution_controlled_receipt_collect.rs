//! Native controlled SELL collection. Pending daemon confirmation waits only
//! inside the original quote lease; no event clock or admission is replaced.
use super::{acquire, readiness, transport};
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::InboxLimits, ordered_sell_quote::QuoteClaim, SqliteStore,
};
use serde_json::json;
pub(super) async fn bind<T: transport::Transport, C: readiness::Clock + Sync>(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: QuoteClaim,
    l: InboxLimits,
    rpc: &mut T,
    clock: &mut C,
) -> Result<QuoteClaim> {
    let scope =
        crate::execution_controlled_receipt_lot::scope(c)?.context("receipt_lot_scope_required")?;
    let producer = super::super::identity(c)?;
    if let Some(d) = &claim.binding.fractional {
        ensure!(
            d.controlled_receipt.is_some() && d.producer_identity == producer,
            "receipt_lot_producer_changed"
        );
        ensure!(
            store.recheck_strict_sell_quote(&claim, l, clock.now())?,
            "receipt_lot_claim_changed"
        );
        return Ok(claim);
    }
    let _permit = super::COLLECTION
        .try_acquire()
        .context("fraction_collection_capacity")?;
    let snapshot = store.begin_fractional_sell(&claim, l, clock.now(), &producer)?;
    let event = &snapshot.sell.facts;
    for observation in 0..readiness::OBSERVATIONS {
        readiness::guard(c, &claim, clock)?;
        if SqliteStore::controlled_source_receipts(&scope, &event.signature, event.slot)?.is_some()
        {
            break;
        }
        ensure!(
            observation + 1 < readiness::OBSERVATIONS,
            "receipt_lot_source_receipt_observations_exhausted"
        );
        readiness::wait(store, c, &claim, l, clock).await?;
    }
    let mut reader = acquire::Reader {
        store,
        config: c,
        claim: &claim,
        rpc,
        clock,
    };
    let genesis = reader.read("getGenesisHash", json!([])).await?;
    ensure!(
        genesis
            == c.owned_sell_preparation
                .as_ref()
                .context("receipt_lot_policy")?
                .genesis_hash,
        "receipt_lot_genesis"
    );
    let accounts=reader.read("getTokenAccountsByOwner",json!([c.canary_wallet_pubkey,{"mint":claim.binding.mint},{"encoding":"jsonParsed","commitment":"finalized","minContextSlot":event.slot}])).await?;
    let canonical = crate::execution_controlled_receipt_lot::canonical(c)?;
    let out =
        store.complete_controlled_receipt_sell(&claim, &accounts, &canonical, l, clock.now())?;
    readiness::guard(c, &out, clock)?;
    Ok(out)
}
