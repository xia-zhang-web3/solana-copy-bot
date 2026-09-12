#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
#[test]
fn b95_fully_applied_buy_has_zero_actual_pending() -> Result<()> {
    let mut f = f::F::new()?;
    f.anchors()?;
    f.sell()?;
    f.drain()?;
    let p = f.read()?;
    assert_eq!(p.current.current_contributors.len(), 1);
    if let Ok(path) = std::env::var("B95_EXPORT_PRE_FIX") {
        f.db.conn()?.execute("VACUUM INTO ?1", [&path])?;
        std::fs::write(format!("{path}.json"), serde_json::to_vec_pretty(&p.first)?)?;
    }
    assert_eq!(
        p.current.pending_buys.len(),
        0,
        "fully applied canonical BUY is not pending"
    );
    Ok(())
}
