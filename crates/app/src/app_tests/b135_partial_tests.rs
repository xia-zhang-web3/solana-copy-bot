#[path = "b135_held_delivery_fixture.rs"]
mod held_delivery;
use super::{
    b135_fixture::Fixture, b135_server::Server, b93_fixture as receipt, strict_quote_fixture as q,
};
use anyhow::Result;
use std::time::Duration;
#[tokio::test]
async fn b135_held_quote_partial_receipt_refreshes_exact_remaining_4000() -> Result<()> {
    let f = Fixture::new().await?;
    let server = Server::new().await?;
    let c = f.config(&server.url)?;
    f.ingress(&c).await?;
    // Full replay/recovery is already proved. Preload captures and ACK the first
    // two admissions before holding the quote; the critical delivery is SELL.
    let setup = std::time::Instant::now();
    let (mut consumer, tx) =
        super::association_fixture::start(&f.db, &c, held_delivery::SESSION).await?;
    let mut delivery = held_delivery::Replay::prepare(&f, &mut consumer, tx).await?;
    let setup_ms = setup.elapsed().as_millis();
    let first = q::first(&f.db, &f.meta)?;
    let r = f.runner(&c)?;
    let (reached, release) = server.hold("quote");
    q::tick(&r, &f.db).await?;
    tokio::time::timeout(Duration::from_secs(3), reached).await??;
    let replay = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(2),
        delivery.acknowledge_sell(&f, &mut consumer),
    )
    .await??;
    assert!(f
        .db
        .store
        .load_strict_sell_quote(
            &q::id(&f.meta),
            super::association_parent_fixture::limits(),
            chrono::Utc::now()
        )?
        .is_none());
    assert!(
        !release.is_closed(),
        "held quote peer must still await release"
    );
    println!(
        "B135_HELD_DELIVERY setup_ms={setup_ms} sell_ack_ms={} quote_pending=true end_gated=true",
        replay.elapsed().as_millis()
    );
    let facts = receipt::receipt(&f.db, &f.meta, "b135-partial", 3000)?;
    // Explicit prior partial execution fixture, with matching durable budget operands.
    f.db.sql.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,message_sha256,transaction_sha256,claimed_at) SELECT order_id,signal_id,client_order_id,route,attempt,?1,?2,'sell',tx_signature,?3,?4,submit_ts FROM orders WHERE order_id=?5",rusqlite::params![facts.wallet_pubkey,facts.token,"c".repeat(64),"d".repeat(64),facts.order_id])?;
    f.db.sql.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) SELECT order_id,'b135',tx_signature,?1,'sell',?2,?3,0,100000,10000,'140',submit_ts FROM orders WHERE order_id=?4",rusqlite::params![facts.wallet_pubkey,"c".repeat(64),"d".repeat(64),facts.order_id])?;
    receipt::settle(&f.db, &facts)?;
    assert_eq!(receipt::raw(&f.db, &f.meta)?, 4000);
    let before = f.rows("positions")?;
    release.send(()).unwrap();
    let stale = q::result(&f.db, &f.meta).await?;
    assert_eq!(
        stale.outcome,
        copybot_storage_core::ordered_sell_quote::QuoteOutcome::Stale
    );
    // The old aggregate boundary still times out deterministically: all frames
    // may commit, but EOF cannot arrive until this explicit test gate is released.
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            super::association_fixture::drain(&mut consumer, &f.db),
        )
        .await
        .is_err(),
        "EOF-gated full drain must not masquerade as delivery ACK"
    );
    delivery.finish(&f, &mut consumer).await?;
    f.drive(&r).await?;
    let snapshot: String =
        f.db.sql
            .query_row("SELECT snapshot FROM rpc_owned_sell_handoffs", [], |r| {
                r.get(0)
            })?;
    let snapshot: serde_json::Value = serde_json::from_str(&snapshot)?;
    assert_eq!(snapshot["quote"]["raw"], 4000);
    assert_eq!(snapshot["receipts"][0]["raw"], "7000");
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(q::first(&f.db, &f.meta)?, first);
    let amounts = server
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|v| v["method"] == "quote")
        .map(|v| v["params"]["amount"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        amounts,
        vec![serde_json::json!("7000"), serde_json::json!("4000")]
    );
    server.healthy();
    println!("B135_PARTIAL quote=7000->stale->4000 origin_buy=7000 handoff=4000 inventory_unchanged_after_receipt");
    Ok(())
}
