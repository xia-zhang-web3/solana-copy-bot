//! Fractional inventory producer adapter. Only raw request-bound responses enter
//! the verifier; no quote is requested until complete parent/program/prefix proof.
use super::endpoint;
#[path = "execution_fractional_transport.rs"]
pub(crate) mod transport;
static COLLECTION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::InboxLimits,
    ordered_sell_quote::{
        fractional::inventory::{Evidence, Page, CONTRACT, TOKEN, TOKEN22},
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
    ensure!(
        enabled(c) && copybot_config::owned_sell_flags(c),
        "fraction_mode_required"
    );
    let producer_identity = super::identity(c)?;
    if let Some(d) = &claim.binding.fractional {
        ensure!(
            d.producer_identity == producer_identity,
            "fraction_producer_changed"
        );
        store.recheck_strict_sell_quote(&claim, limits, Utc::now())?;
        return Ok(claim);
    }
    // No waiting/lease extension and at most one large collector retained in-process.
    let _permit = COLLECTION
        .try_acquire()
        .map_err(|_| anyhow::anyhow!("fraction_collection_capacity"))?;
    let snapshot = store.begin_fractional_sell(&claim, limits, Utc::now(), &producer_identity)?;
    let p = c
        .owned_sell_preparation
        .as_ref()
        .context("fraction_policy")?;
    let mut read = async |method: &str, params: Value| -> Result<Value> {
        let store = &mut *store;
        // Preserve full provenance/generation checks around the logical response.
        // Per-chunk progress checks must not rerun the whole SQL proof graph hundreds
        // of times: stop/lease/deadline are checked during acquisition, and a changed
        // generation is refused after body completion, before verifier or quote.
        store.recheck_fractional_collection(&claim, limits, Utc::now())?;
        let mut check = || {
            ensure!(
                !std::path::Path::new(&c.canary_kill_switch_path).exists(),
                "kill_switch_active"
            );
            ensure!(
                Utc::now() < claim.lease_until,
                "fraction_collection_deadline"
            );
            Ok(())
        };
        check()?;
        let request =
            json!({"jsonrpc":"2.0","id":"fractional-inventory-v1","method":method,"params":params});
        let mut response = rpc.read(request.clone(), &mut check).await?;
        check()?;
        store.recheck_fractional_collection(&claim, limits, Utc::now())?;
        ensure!(
            response["jsonrpc"] == "2.0"
                && response["id"] == request["id"]
                && response.get("error").is_none()
                && response.get("result").is_some_and(|v| !v.is_null()),
            "fraction_rpc_binding"
        );
        Ok(response["result"].take())
    };
    ensure!(
        read("getGenesisHash", json!([])).await? == p.genesis_hash,
        "fraction_genesis"
    );
    let slot = snapshot.sell.facts.slot;
    let block = read("getBlock", json!([slot,{"encoding":"json","commitment":"finalized","maxSupportedTransactionVersion":1,"transactionDetails":"full","rewards":false}])).await?;
    let parent_slot = block["parentSlot"]
        .as_u64()
        .context("fraction_parent_missing")?;
    ensure!(parent_slot < slot, "fraction_parent_order");
    let parent = read("getBlock", json!([parent_slot,{"encoding":"json","commitment":"finalized","maxSupportedTransactionVersion":1,"transactionDetails":"none","rewards":false}])).await?;
    let mut pages = vec![];
    for program in [TOKEN22, TOKEN] {
        let mut cursor: Option<String> = None;
        let mut seen = std::collections::BTreeSet::new();
        loop {
            ensure!(pages.len() < 32, "fraction_pages_bound");
            let mut cfg = json!({"slot":parent_slot,"pageLimit":1000});
            if let Some(key) = &cursor {
                cfg["pageKey"] = json!(key);
            }
            let response = read(
                "getTokenAccountsByOwnerAtSlot",
                json!([claim.binding.source_wallet,{"programId":program},cfg]),
            )
            .await?;
            let next = response
                .get("pageKey")
                .context("fraction_page_terminal_missing")?;
            let next = if next.is_null() {
                None
            } else {
                Some(
                    next.as_str()
                        .filter(|s| !s.is_empty() && s.len() <= 1024)
                        .context("fraction_cursor")?
                        .to_owned(),
                )
            };
            if let Some(key) = &next {
                ensure!(seen.insert(key.clone()), "fraction_cursor_cycle");
            }
            pages.push(Page {
                program: program.into(),
                cursor,
                response,
            });
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
    }
    let execution_accounts = read("getTokenAccountsByOwner", json!([c.canary_wallet_pubkey,{"mint":claim.binding.mint},{"encoding":"jsonParsed","commitment":"finalized"}])).await?;
    store.complete_fractional_sell(
        &claim,
        &Evidence {
            version: 1,
            slot,
            block,
            parent,
            pages,
            execution_accounts,
        },
        limits,
        Utc::now(),
    )
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
