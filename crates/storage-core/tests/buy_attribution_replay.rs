#[path = "common/buy_attribution_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::Db;

#[test]
fn actual_merged_buy_persists_each_destination() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    let first = db.buy(&a)?;
    let b = db.seed("b", "source-b", "buy")?;
    assert_eq!(db.buy(&b)?.position.position_id, first.position.position_id);
    db.reopen()?;
    assert_eq!(
        db.links()?,
        vec![
            (a, Some(first.position.position_id.clone())),
            (b, Some(first.position.position_id))
        ]
    );
    Ok(())
}

#[test]
fn old_merged_buy_replay_returns_its_closed_generation() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    let first = db.buy(&a)?;
    let b = db.seed("b", "source-b", "buy")?;
    db.buy(&b)?;
    db.close()?;
    let c = db.seed("c", "source-c", "buy")?;
    let new = db.buy(&c)?;
    assert_ne!(first.position.position_id, new.position.position_id);
    db.reopen()?;
    let before = db.snapshot()?;
    let replay = db.buy(&b)?;
    assert_eq!(db.snapshot()?, before);
    assert_eq!(replay.position.position_id, first.position.position_id);
    assert_eq!(replay.position.state, "closed");
    Ok(())
}
