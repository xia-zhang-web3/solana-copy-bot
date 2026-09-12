use super::{
    association_parent_fixture as p, association_sell_fixture as s, b93_fixture as f,
    b93_fixture::config, b93_http_fixture::Server,
};
use anyhow::Result;
use serde_json::json;
#[tokio::test]
async fn b93_preparation_partial_receipt_and_replay_same_generation() -> Result<()> {
    let (db, m) = f::seeded("b93-preparation").await?;
    let before = f::state(&db, &m)?;
    let first = p::read(&db, &m)?.first;
    // Unknown-time durable delivery creates preparation only, no legacy bridge.
    assert_eq!(
        db.sql.query_row::<i64, _, _>(
            "SELECT count(*) FROM execution_source_sell_intents",
            [],
            |r| r.get(0)
        )?,
        0
    );
    let facts = f::receipt(&db, &m, "partial-preparation", 3000)?;
    let result = f::settle(&db, &facts)?;
    assert_eq!(result.settlement.remaining_quantity.raw(), 4000);
    assert_eq!(f::raw(&db, &m)?, 4000);
    let frozen = s::snapshot(&db)?;
    let repeat = db
        .store
        .apply_execution_canary_sell_settlement(&facts, f::at())?;
    assert!(repeat.already_accounted);
    assert_eq!(s::snapshot(&db)?, frozen);
    p::stage_at(
        &db,
        super::b93_local_fixture::inputs(),
        &m,
        p::frames(&m),
        "repeat-after-partial",
        true,
    )
    .await?;
    let after = f::state(&db, &m)?;
    assert_eq!(p::read(&db, &m)?.first, first);
    assert_eq!(before["generation"], after["generation"]);
    assert_eq!(after["original_buy_raw"], "7000");
    assert_eq!(
        serde_json::to_value(p::read(&db, &m)?.current.selected_chain)?,
        json!("ProviderOrderedAcrossBlocks")
    );
    f::write(
        "preparation-before-after",
        json!({"before":before,"after":after,"replay":format!("{repeat:?}"),"delivery_revalidation_financial_delta":0}),
    )
}

#[tokio::test]
async fn b93_owned_quote_after_partial_during_quote_and_refresh() -> Result<()> {
    let (db, m) = f::seeded("b93-owned-quote").await?;
    let signal = f::legacy(&db, &m)?;
    let mut rpc = Server::new(db.path.clone(), f::at(), [94; 32]).await?;
    {
        let path = db.path.clone();
        let m = m.clone();
        let mut c = rpc.state.lock().unwrap();
        c.mutate_at = Some("quote");
        c.after_wallet_raw = Some(4000);
        c.mutation = Some(Box::new(move || {
            let db = f::open(&path)?;
            let facts = f::receipt(&db, &m, "owned-quote-race", 3000)?;
            f::settle(&db, &facts)?;
            Ok(())
        }));
    }
    let shadow = copybot_shadow::ShadowSignalResult {
        signal_id: signal.signal_id.clone(),
        wallet_id: signal.wallet_id.clone(),
        side: signal.side.clone(),
        token: signal.token.clone(),
        notional_sol: signal.notional_sol,
        latency_ms: 0,
        closed_qty: 0.0,
        realized_pnl_sol: 0.0,
        has_open_lots_after_signal: Some(false),
    };
    let c = config(&rpc.url);
    let out = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(c.clone())
        .process_recorded_shadow_signal(&db.store, &shadow, f::at())
        .await?;
    let event_id = format!("quote:owned-close:{}", signal.signal_id);
    let event = db
        .store
        .load_execution_quote_canary_event_by_id(&event_id)?
        .unwrap();
    assert_eq!(event.quote_in_amount_raw.as_deref(), Some("7000"));
    assert_eq!(f::raw(&db, &m)?, 4000);
    let tiny = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &c,
        &db.store,
        &event_id,
        f::at(),
    )
    .await?;
    rpc.finish().await?;
    f::write(
        "owned-quote-refresh",
        json!({"quote_summary":format!("{out:?}"),"old_quote":format!("{event:?}"),"tiny":format!("{tiny:?}"),"calls":rpc.state.lock().unwrap().calls,"after":f::state(&db,&m)?}),
    )?;
    let amounts: Vec<String> = db
        .sql
        .prepare("SELECT quote_in_amount_raw FROM execution_canary_build_plan_metadata")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(amounts, vec!["4000"]);
    assert_eq!(rpc.count("sendTransaction"), 0);
    Ok(())
}
