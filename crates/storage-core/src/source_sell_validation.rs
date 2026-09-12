use crate::{
    execution_canary_buy_attribution::read_on_conn,
    execution_canary_position_open::load_open_position_by_token, source_sell_event as event,
    ExecutionCanaryBuyAttribution as Attribution, ExecutionSourceSellIntent,
    ExecutionSourceSellReject as Reject, ProvenBuyContributor, SqliteDiscoveryStore,
};
use anyhow::{ensure, Result};
use copybot_core_types::SwapEvent;
use rusqlite::Connection;

// All callers hold a transaction on store.conn, also passed as conn below.
pub(crate) fn position(
    store: &SqliteDiscoveryStore,
    conn: &Connection,
    swap: &SwapEvent,
    expected_position_id: &str,
) -> Result<Option<Reject>> {
    use Reject::*;
    let reject = |reason| Ok(Some(reason));
    let Some(position) = load_open_position_by_token(conn, &swap.token_in)? else {
        return reject(NoOwnedPosition);
    };
    if position.position_id != expected_position_id {
        return reject(GenerationMismatch);
    }
    if !position.qty.is_finite()
        || position.qty <= 1e-12
        || position.qty_exact.is_some_and(|q| q.raw() == 0)
    {
        return reject(NoOwnedPosition);
    }
    if swap.ts_utc < position.opened_ts {
        return reject(SellBeforePosition);
    }
    // These legacy reads use store.conn, inside the caller's existing snapshot.
    if store
        .latest_live_execution_canary_buy_signal_ts(&swap.token_in)?
        .is_some_and(|ts| swap.ts_utc < ts)
    {
        return reject(SellBeforeLatestBuy);
    }
    if store.has_shadow_lots_at(&swap.wallet, &swap.token_in, swap.ts_utc)? {
        return reject(ShadowRiskPresent);
    }
    Ok(None)
}

pub(crate) fn witness(
    conn: &Connection,
    swap: &SwapEvent,
    expected_position_id: &str,
    existing: Option<&ExecutionSourceSellIntent>,
) -> Result<std::result::Result<(ProvenBuyContributor, String), Reject>> {
    use Reject::*;
    let reject = |reason| Ok(Err(reason));
    let Attribution::Open(attribution) = read_on_conn(conn, &swap.token_in)? else {
        return reject(NoOwnedPosition);
    };
    ensure!(
        attribution.position_id == expected_position_id,
        "staged SELL snapshot generation changed"
    );
    let mut witnesses = attribution
        .proven_contributors
        .into_iter()
        .filter(|w| w.source_wallet == swap.wallet);
    let witness = if let Some(old) = &existing {
        let Some(witness) = witnesses.find(|w| w == &old.buy_witness) else {
            return reject(WitnessNoLongerProven);
        };
        witness
    } else {
        // Stable smallest fill ID, with textual identities as deterministic tie breakers.
        let Some(witness) = witnesses.min_by(|a, b| {
            (a.fill_id, &a.order_id, &a.signal_id, &a.tx_signature).cmp(&(
                b.fill_id,
                &b.order_id,
                &b.signal_id,
                &b.tx_signature,
            ))
        }) else {
            return reject(SourceNotProven);
        };
        witness
    };
    let execution_wallet: String = conn.query_row(
        "SELECT wallet_pubkey FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&witness.order_id],
        |r| r.get(0),
    )?;
    if existing.is_some_and(|old| old.buy_execution_wallet != execution_wallet) {
        return reject(WitnessNoLongerProven);
    }
    Ok(Ok((witness, execution_wallet)))
}

pub(crate) fn revalidate(
    store: &SqliteDiscoveryStore,
    conn: &Connection,
    staged: &ExecutionSourceSellIntent,
) -> Result<Option<Reject>> {
    if !event::valid(&staged.event) {
        return Ok(Some(Reject::InvalidSell));
    }
    if !event::matches_observed(conn, &staged.event)? {
        return Ok(Some(Reject::ObservedEventMismatch));
    }
    if let Some(reason) = position(store, conn, &staged.event, &staged.position_id)? {
        return Ok(Some(reason));
    }
    Ok(witness(conn, &staged.event, &staged.position_id, Some(staged))?.err())
}
