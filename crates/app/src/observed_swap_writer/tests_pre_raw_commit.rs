use super::*;
use serde_json::json;

pub async fn commit_cases() -> Result<Value> {
    let c = Case::new(30)?;
    let writer = ObservedSwapWriter::start_for_test(c.path.to_string_lossy().into(), 8, 8)?;
    let row = chosen(30);
    note(&row);
    assert!(writer.write(&row).await?);
    note(&row);
    assert!(!writer.write(&row).await?);
    writer.shutdown()?;
    let committed = c.rows()?;

    let retry = Case::new(31)?;
    let control = Connection::open(&retry.path)?;
    control.execute_batch(
        "CREATE TABLE membership_lock_gate(locked INTEGER NOT NULL);
        INSERT INTO membership_lock_gate VALUES(1);
        CREATE TRIGGER membership_retry BEFORE INSERT ON observed_swaps
        WHEN (SELECT locked FROM membership_lock_gate)=1
        BEGIN SELECT RAISE(FAIL,'database is locked'); END;",
    )?;
    let writer = ObservedSwapWriter::start_for_test(retry.path.to_string_lossy().into(), 8, 8)?;
    let row = chosen(31);
    note(&row);
    writer.enqueue(&row).await?;
    timeout(Duration::from_secs(3), async {
        while writer.snapshot().raw_batch_write_ms_p95 == 0 {
            writer.ensure_running()?;
            sleep(Duration::from_millis(2)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    writer.ensure_running()?;
    assert_eq!(retry.rows()?.len(), 0);
    let before: Value = serde_json::from_str(&membership::snapshot_json())?;
    assert_eq!(count(&before, 31, Disposition::RawInserted), 0);
    assert_eq!(count(&before, 31, Disposition::RawUnknown), 0);
    control.execute("UPDATE membership_lock_gate SET locked=0", [])?;
    writer.shutdown()?;
    let retried = retry.rows()?;
    assert_eq!(retried.len(), 1);

    let refused = Case::new(32)?;
    Connection::open(&refused.path)?.execute_batch(
        "CREATE TRIGGER membership_abort
        BEFORE INSERT ON observed_swaps BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;",
    )?;
    let writer = ObservedSwapWriter::start_for_test(refused.path.to_string_lossy().into(), 8, 8)?;
    let row = chosen(32);
    note(&row);
    assert!(writer.write(&row).await.is_err());
    assert!(writer.shutdown().is_err());
    assert!(refused.rows()?.is_empty());
    Ok(json!({"committed":committed,"retry_after_attempt":retried,"rollback_rows":0}))
}
