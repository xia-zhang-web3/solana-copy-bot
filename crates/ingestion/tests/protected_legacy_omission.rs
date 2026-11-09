//! Causal RED baseline: a complete block cannot repair an omitted frame in legacy capture.
#[path = "protected_recovery/support.rs"]
mod support;
use anyhow::Result;
use copybot_ingestion::capture_replay::CaptureReplay;

#[tokio::test]
async fn legacy_protected_capture_does_not_recover_a_silently_omitted_transaction() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("capture.db");
    let mut config = support::config(&path)?;
    config.capture_recovery = None;
    config.yellowstone_http_recovery = None;
    config.yellowstone_delivery_mode = "legacy".into();
    let row = support::rows().remove(0);
    let db = rusqlite::Connection::open(&path)?;
    db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('LOCAL-legacy','{}',1e100)",[])?;
    db.execute("INSERT INTO capture_members VALUES(1,?)",[row["wallet"].as_str().unwrap()])?;
    let legacy = CaptureReplay::open(&config).await?;
    legacy.accept_pending().await?;
    // Same explicitly synthetic full block as the candidate's successful causal
    // fixture. The transaction frame is deliberately absent; there is no reconnect.
    assert!(legacy.push(&support::update(support::block(&row))).await?.is_none());
    assert_eq!(db.query_row("SELECT count(*) FROM capture_events",[],|r|r.get::<_,i64>(0))?,0);
    assert_eq!(db.query_row("SELECT count(*) FROM capture_recovery",[],|r|r.get::<_,i64>(0))?,0);
    Ok(())
}
