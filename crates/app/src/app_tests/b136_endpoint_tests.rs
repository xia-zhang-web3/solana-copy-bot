use super::{b136_config, b136_fixture::Fixture, b136_server::Server, strict_quote_fixture as q};
use anyhow::Result;
use std::time::Duration;

pub(super) async fn finish(
    f: &Fixture,
    r: &crate::execution_canary::ExecutionCanaryRunner,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            q::tick(r,&f.db).await?;
            let done:bool=f.db.sql.query_row("SELECT EXISTS(SELECT 1 FROM fills f JOIN execution_canary_dispatch d ON d.order_id=f.order_id WHERE d.side='sell')",[],|r|r.get(0))?;
            if done {return Ok::<_,anyhow::Error>(());}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await?
}
pub(super) fn sends(s: &Server) -> usize {
    s.calls
        .lock()
        .unwrap()
        .iter()
        .filter(|v| v["method"] == "sendTransaction")
        .count()
}
#[tokio::test]
async fn b136_actual_source_to_receipt_settlement_once() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = b136_config::load(&f, &s.url, true)?;
    f.ingress(&c).await?;
    let before = f.rows("positions")?;
    let basis: i64 =
        f.db.sql
            .query_row("SELECT cost_lamports FROM positions", [], |r| r.get(0))?;
    let r = f.runner(&c)?;
    let (reached, release) = s.hold("sendTransaction");
    q::tick(&r, &f.db).await?;
    if tokio::time::timeout(Duration::from_secs(4), reached)
        .await
        .is_err()
    {
        println!(
            "B136_REFUSAL calls={:?}",
            s.calls
                .lock()
                .unwrap()
                .iter()
                .map(|v| v["method"].clone())
                .collect::<Vec<_>>()
        );
        q::tick(&r, &f.db).await?;
        anyhow::bail!("owned endpoint did not reach transport");
    }
    assert_eq!(
        f.rows("positions")?,
        before,
        "no inventory/basis/cash before receipt"
    );
    let (slots, fee): (u64, u64) = f.db.sql.query_row(
        "SELECT count(*),sum(fee_bound) FROM execution_tiny_reservations WHERE side='sell'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!((slots, fee), (1, 100000));
    assert_eq!(f.rows("rpc_owned_sell_handoffs")?.len(), 1);
    assert_eq!(f.rows("rpc_owned_sell_dispatches")?.len(), 1);
    assert_eq!(f.rows("copy_signals")?.len(), 1, "no technical copy signal");
    release.send(()).unwrap();
    finish(&f, &r).await?;
    let id: String =
        f.db.sql
            .query_row("SELECT order_id FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?;
    let settled =
        f.db.store
            .load_execution_canary_cash_settlement(&id)?
            .unwrap();
    assert_eq!(settled.sold_quantity.raw(), 7000);
    assert_eq!(settled.remaining_quantity.raw(), 0);
    assert_eq!(settled.allocated_entry_basis.as_u64(), basis as u64);
    assert_eq!(settled.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(
        settled.cash_result_delta.as_i128(),
        981000 - i128::from(basis)
    );
    assert_eq!(
        f.db.sql.query_row(
            "SELECT actual_fee FROM execution_tiny_reservations WHERE side='sell'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        19000
    );
    assert_eq!(f.db.sql.query_row("SELECT fee_bound FROM execution_tiny_reservations WHERE side='buy' AND actual_fee IS NULL",[],|r|r.get::<_,u64>(0))?,100000);
    let after = f.rows("positions")?;
    let fills = f.rows("fills")?;
    drop(r);
    f.ingress(&c).await?;
    for _ in 0..3 {
        q::tick(&f.runner(&c)?, &f.db).await?;
    }
    assert_eq!(sends(&s), 1);
    assert_eq!(f.rows("positions")?, after);
    assert_eq!(f.rows("fills")?, fills);
    let facts =
        f.db.store
            .load_execution_canary_receipt_facts(&id)?
            .unwrap();
    assert!(
        f.db.store
            .apply_execution_canary_sell_settlement(&facts, chrono::Utc::now())?
            .already_accounted
    );
    println!("B136_ENDPOINT synthetic source/DEX/receipt actual consumer+runner dispatch=1 verified_test_signature=1 send=1 settlement=1 raw=7000 native_delta=981000 fee=19000 buy_unknown_hold=100000 source_utc=Unknown basis={basis}");
    s.healthy();
    Ok(())
}
