use super::{b136_fixture::Fixture, b136_server::Server};
use anyhow::Result;
#[tokio::test]
async fn b136_baseline_unsigned_actual_runner_has_no_dispatch() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = f.config(&s.url)?;
    f.ingress(&c).await?;
    let before = f.rows("positions")?;
    f.drive(&f.runner(&c)?).await?;
    let dispatches: u64 = f.db.sql.query_row(
        "SELECT count(*) FROM execution_canary_dispatch WHERE side='sell'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(f.handoffs()?, 1);
    assert_eq!(dispatches, 0);
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("copy_signals")?.len(), 1);
    println!("B136_BASELINE actual ingress/recovery/runner; unsigned=1 canonical_sell_dispatch=0 signer=0 send=0 owned_raw=7000 source_utc=Unknown");
    Ok(())
}
