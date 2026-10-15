//! Fractional inventory producer adapter. Only raw request-bound responses enter
//! the verifier; no quote is requested until complete parent/program/prefix proof.
use super::endpoint;
#[path = "execution_fractional_acquire.rs"]
mod acquire;
#[path = "execution_fractional_readiness.rs"]
pub(crate) mod readiness;
#[path = "execution_fractional_transport.rs"]
pub(crate) mod transport;
static COLLECTION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::InboxLimits,
    ordered_sell_quote::{
        fractional::{inventory::CONTRACT, CollectionStep},
        QuoteClaim,
    },
    SqliteStore,
};
use serde_json::{json, Value};
use std::future::Future;
pub(crate) fn enabled(c: &ExecutionConfig) -> bool {
    c.owned_sell_preparation
        .as_ref()
        .is_some_and(|p| p.fractional_inventory.as_deref() == Some(CONTRACT))
}
/// Shared actual native entry for production and in-process mocked transport tests.
/// A claim is pinned and consumed before first read; errors cannot silently refresh it.
pub(crate) async fn bind<F, Fut>(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: QuoteClaim,
    limits: InboxLimits,
    rpc: F,
) -> Result<QuoteClaim>
where
    F: FnMut(Value) -> Fut + Send,
    Fut: Future<Output = Result<Value>> + Send,
{
    bind_transport(store, c, claim, limits, transport::Parsed(rpc)).await
}
pub(crate) async fn bind_transport(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: QuoteClaim,
    limits: InboxLimits,
    mut rpc: impl transport::Transport,
) -> Result<QuoteClaim> {
    bind_transport_with_clock(store, c, claim, limits, &mut rpc, readiness::WallClock).await
}
pub(crate) async fn bind_transport_with_clock(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: QuoteClaim,
    limits: InboxLimits,
    rpc: &mut impl transport::Transport,
    mut clock: impl readiness::Clock + Sync,
) -> Result<QuoteClaim> {
    ensure!(
        enabled(c) && copybot_config::owned_sell_flags(c),
        "fraction_mode_required"
    );
    let producer_identity = super::identity(c)?;
    let baseline = crate::execution_protected_token_baseline::configured(c)?;
    if let Some(d) = &claim.binding.fractional {
        ensure!(
            d.producer_identity == producer_identity,
            "fraction_producer_changed"
        );
        ensure!(
            d.protected_execution_baseline == baseline,
            "fraction_protected_baseline_changed"
        );
        store.recheck_strict_sell_quote(&claim, limits, clock.now())?;
        return Ok(claim);
    }
    // No lease extension and at most one large collector retained in-process.
    let _permit = COLLECTION
        .try_acquire()
        .map_err(|_| anyhow::anyhow!("fraction_collection_capacity"))?;
    let snapshot = store.begin_fractional_sell(&claim, limits, clock.now(), &producer_identity)?;
    let p = c
        .owned_sell_preparation
        .as_ref()
        .context("fraction_policy")?;
    readiness::guard(c, &claim, &clock)?;
    let mut reader = acquire::Reader {
        store,
        config: c,
        claim: &claim,
        rpc,
        clock: &clock,
    };
    ensure!(
        reader.read("getGenesisHash", json!([])).await? == p.genesis_hash,
        "fraction_genesis"
    );
    let slot = snapshot.sell.facts.slot;
    let mut block = reader.read("getBlock", json!([slot,{"encoding":"json","commitment":"finalized","maxSupportedTransactionVersion":1,"transactionDetails":"full","rewards":false}])).await?;
    let parent_slot = block["parentSlot"]
        .as_u64()
        .context("fraction_parent_missing")?;
    ensure!(parent_slot < slot, "fraction_parent_order");
    let mut parent = reader.read("getBlock", json!([parent_slot,{"encoding":"json","commitment":"finalized","maxSupportedTransactionVersion":1,"transactionDetails":"none","rewards":false}])).await?;
    for observation in 0..readiness::OBSERVATIONS {
        store.recheck_fractional_collection(&claim, limits, clock.now())?;
        let evidence = acquire::Reader {
            store,
            config: c,
            claim: &claim,
            rpc,
            clock: &clock,
        }
        .pages(slot, block, parent)
        .await?;
        match store.try_complete_fractional_sell_with_baseline(
            &claim,
            &evidence,
            limits,
            clock.now(),
            baseline.as_ref(),
        )? {
            CollectionStep::Proven(proven) => {
                readiness::guard(c, &claim, &clock)?;
                return Ok(proven);
            }
            CollectionStep::MissingTargetParentAccount => {
                ensure!(
                    observation + 1 < readiness::OBSERVATIONS,
                    "fraction_readiness_observations_exhausted"
                );
                // Blocks remain pinned. Only program pages and current BOT inventory are reread.
                block = evidence.block;
                parent = evidence.parent;
                readiness::wait(store, c, &claim, limits, &mut clock).await?;
            }
        }
    }
    unreachable!("bounded inventory observations return or refuse")
}

pub(crate) async fn collect(
    http: &reqwest::Client,
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: QuoteClaim,
    limits: InboxLimits,
) -> Result<QuoteClaim> {
    let url = endpoint(c)?;
    bind_transport(
        store,
        c,
        claim,
        limits,
        transport::Http {
            http,
            config: c,
            url,
            budget: Default::default(),
        },
    )
    .await
}
