//! Current selector/proof API for synthetic owned inventory in legacy boundary fixtures.
use crate::execution_source_sell_guard::{self as guard, amount::Selection};
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::Result;
use copybot_storage_core::SqliteStore;

pub(super) fn bind(
    store: &SqliteStore,
    request: &mut ExecutionSubmitRequest,
    wallet_raw: u64,
    decimals: u8,
) -> Result<()> {
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .unwrap();
    let source = guard::order(store, &order.order_id, &[order.status.as_str()])?;
    let position = store
        .load_execution_canary_open_position(&request.token)?
        .unwrap();
    let selection = Selection::new(&position)?;
    let selected = selection.amount(wallet_raw, decimals)?;
    selection.recheck(store)?;
    request.metadata = selection.finish(
        request.metadata.clone(),
        source.as_ref(),
        &request.wallet_pubkey,
        wallet_raw,
        selected,
    )?;
    assert!(guard::request(store, request, &[order.status.as_str()])?.is_some());
    Ok(())
}
