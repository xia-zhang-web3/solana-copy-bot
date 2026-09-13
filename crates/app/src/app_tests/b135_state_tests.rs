use super::{b135_fixture::Fixture, b135_server::Server, strict_quote_fixture as q};
use anyhow::Result;
use std::time::Duration;
#[tokio::test]
async fn b135_held_rpc_rechecks_generation_receipt_contributors_amount() -> Result<()> {
    for (case, sql) in [
        (
            "generation",
            "UPDATE positions SET opened_ts='2026-09-10T00:00:00Z'",
        ),
        (
            "receipt",
            "UPDATE execution_canary_receipt_facts SET wallet_native_post='42'",
        ),
        ("contributor", "UPDATE copy_signals SET wallet_id='other'"),
        ("amount", "UPDATE positions SET qty_raw='4000',qty=4"),
    ] {
        let f = Fixture::new().await?;
        let server = Server::new().await?;
        let c = f.config(&server.url)?;
        f.ingress(&c).await?;
        let r = f.runner(&c)?;
        let (reached, release) = server.hold("getTransaction");
        q::tick(&r, &f.db).await?;
        tokio::time::timeout(Duration::from_secs(3), reached).await??;
        // Actual runner/consumer remain responsive while the RPC is held.
        q::tick(&r, &f.db).await?;
        f.db.sql.execute(sql, [])?;
        release.send(()).unwrap();
        let error = f.drive(&r).await.unwrap_err().to_string();
        assert!(
            error.contains("changed")
                || error.contains("snapshot")
                || (case == "receipt" && error.contains("receipt facts native delta mismatch")),
            "{case}:{error}"
        );
        assert_eq!(f.handoffs()?, 0);
        assert!(f.rows("rpc_owned_sell_handoffs")?.is_empty());
        server.healthy();
        println!("B135_HELD {case}: {error}");
    }
    Ok(())
}
#[tokio::test]
async fn b135_unsigned_owner_duplicate_reopen_and_unknown_obligations() -> Result<()> {
    let f = Fixture::new().await?;
    let server = Server::new().await?;
    let c = f.config(&server.url)?;
    f.ingress(&c).await?;
    let before = f.rows("positions")?;
    let reservations = f.rows("execution_tiny_reservations")?;
    f.drive(&f.runner(&c)?).await?;
    let handoff = f.rows("rpc_owned_sell_handoffs")?;
    let orders = f.rows("orders")?;
    assert_eq!(handoff.len(), 1);
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("execution_tiny_reservations")?, reservations);
    for _ in 0..3 {
        let r = f.runner(&c)?;
        q::tick(&r, &f.db).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    f.ingress(&c).await?;
    assert_eq!(f.rows("rpc_owned_sell_handoffs")?, handoff);
    assert_eq!(f.rows("orders")?, orders);
    let fee: u64 =
        f.db.sql
            .query_row("SELECT fee_reserve FROM rpc_owned_sell_handoffs", [], |r| {
                r.get(0)
            })?;
    assert_eq!(fee, 100000);
    let pending: u64 = f.db.sql.query_row(
        "SELECT fee_bound FROM execution_tiny_reservations WHERE side='buy' AND actual_fee IS NULL",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(pending, 100000);
    let h: (String, String, String) = f.db.sql.query_row(
        "SELECT order_id,unsigned_payload,authority FROM rpc_owned_sell_handoffs",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD.decode(&h.1)?;
    assert_eq!(bytes[0], 1);
    assert_eq!(&bytes[1..65], &[0; 64]);
    let (_, wire_priority) = crate::execution_priority_fee_wire::decode_priority_fee_message(&h.1)?;
    let (total_fee, priority_fee): (u64, u64) = f.db.sql.query_row(
        "SELECT total_fee,priority_fee FROM rpc_owned_sell_handoffs",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(priority_fee, wire_priority.total);
    // This fixture has one signature and the standard 5,000-lamport base fee.
    assert_eq!(total_fee, wire_priority.total + 5_000);
    assert_eq!(super::b135_hooks::count(&h.0), 0);
    assert_eq!(
        super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
        0
    );
    let a: serde_json::Value = serde_json::from_str(&h.2)?;
    assert!(a["order"]["RpcFinalizedCrossSlot"]["source_utc"].is_null());
    assert!(a["order"]["RpcFinalizedCrossSlot"]["absolute_age_ms"].is_null());
    let calls = server.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|v| v.get("quoteResponse").is_some())
            .count(),
        1
    );
    assert!(calls.iter().all(|v| v["method"] != "sendTransaction"));
    for call in calls.iter().filter(|v| v["method"] == "getTransaction") {
        assert_eq!(call["params"][1]["commitment"], "finalized");
        assert_eq!(call["params"][1]["maxSupportedTransactionVersion"], 0);
    }
    println!(
        "B135_PROOF {}",
        serde_json::json!({"handoff_rows":handoff,"authority":a,"unsigned_payload":h.1,"order_id":h.0,"requests":*calls,"keyloader":0,"signing":0,"send":0,"source_utc":null,"absolute_age_ms":null,"owned_raw":"7000","buy_unknown_reserve":pending,"unsigned_reserve":fee})
    );
    server.healthy();
    println!("B135_OWNERSHIP order={} raw=7000 keyloader=0 signing=0 send=0 buy_unknown_reserve=100000 unsigned_reserve=100000",h.0);
    Ok(())
}
#[tokio::test]
async fn b135_interrupted_preparation_cannot_rearm_on_reopen() -> Result<()> {
    let f = Fixture::new().await?;
    let server = Server::new().await?;
    let c = f.config(&server.url)?;
    f.ingress(&c).await?;
    let r = f.runner(&c)?;
    let (reached, release) = server.hold("instructions");
    q::tick(&r, &f.db).await?;
    tokio::time::timeout(Duration::from_secs(3), reached).await??;
    let owned = f.rows("rpc_owned_sell_handoffs")?;
    assert_eq!(owned.len(), 1);
    assert_eq!(f.handoffs()?, 0);
    drop(r);
    release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    for _ in 0..3 {
        q::tick(&f.runner(&c)?, &f.db).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(f.rows("rpc_owned_sell_handoffs")?, owned);
    assert_eq!(f.handoffs()?, 0);
    assert_eq!(
        server
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v.get("quoteResponse").is_some())
            .count(),
        1
    );
    Ok(())
}
