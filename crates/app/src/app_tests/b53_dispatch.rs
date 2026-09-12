use super::{b53_fixture::Fixture, b53_http::Send};
use anyhow::Result;
use copybot_storage_core::*;
use std::time::Duration;

#[tokio::test]
async fn b53_cancel_after_complete_request_reopen_keeps_reconcilable_identity() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.rpc.state.lock().unwrap().mode = Send::Hold;
    let received = f.rpc.received.clone();
    let mut tick = Box::pin(f.tick(0));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            biased;
            _ = received.notified() => {},
            result = &mut tick => panic!("tick finished before request barrier: {result:?}"),
        }
    })
    .await?;
    let before = f.emit("complete_request_before_reply", "audit-a")?;
    assert_eq!(before["status"], EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(before["signature"], f.sends()[0]["signature"]);
    assert!(!before["first_signature_locations"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!before["first_transaction_hash_locations"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(f.sends().len(), 1);
    drop(tick); // Cancel actual Runner future, with no SQL crash-state substitution.
    f.rpc.release.notify_one();
    f.reopen()?;
    let a = f.emit("cancelled_tick_reopened", "audit-a")?;
    assert_eq!(a["status"], EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(a["selected"], true);
    assert_eq!(a["accounting_pending"], false);
    assert!(!a["first_signature_locations"]
        .as_array()
        .unwrap()
        .is_empty());
    let request = crate::execution_submit_adapter::build_tiny_submit_reconciliation_request(
        &f.store,
        &f.config,
        &f.order("audit-a")?,
    )?;
    assert!(crate::execution_tiny_submit_state::eligible(&f.store, &request).is_err());
    f.tick(2).await?;
    assert_eq!(
        f.sends().len(),
        1,
        "ordinary simulated A is not picked by actual retry selector"
    );
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("audit-b", 3)?;
    f.tick(3).await?;
    assert_eq!(f.sends().len(), 1);
    f.emit("cancelled_a_new_b_sent", "audit-a")?;
    f.finish().await
}

#[tokio::test]
async fn b53_send_timeout_preserves_hint_but_blocks_new_buy() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.rpc.state.lock().unwrap().mode = Send::Hold;
    f.tick(0).await?;
    f.rpc.release.notify_one();
    f.reopen()?;
    let a = f.emit("request_timeout_hint_recorded", "audit-a")?;
    assert_eq!(a["status"], EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(a["signature"], f.sends()[0]["signature"]);
    assert_eq!(a["selected"], true);
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("audit-b", 2)?;
    f.tick(2).await?;
    assert_eq!(f.sends().len(), 1);
    f.emit("send_timeout_new_b", "audit-a")?;
    f.finish().await
}

#[tokio::test]
async fn b53_body_and_json_failure_after_request_keep_signature_and_block_b() -> Result<()> {
    for mode in [Send::BadBody, Send::BadJson] {
        let mut f = Fixture::new().await?;
        f.seed("audit-a", 0)?;
        f.rpc.state.lock().unwrap().mode = mode;
        f.tick(0).await?;
        assert_eq!(f.sends().len(), 1);
        f.reopen()?;
        let a = f.emit(&format!("{mode:?}_reopen"), "audit-a")?;
        assert_eq!(a["status"], EXECUTION_STATUS_CANARY_SUBMITTED);
        assert_eq!(a["signature"], f.sends()[0]["signature"]);
        assert!(!a["first_signature_locations"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(a["selected"], true);
        assert_eq!(a["failed_expenses"], 0);
        f.tick(2).await?;
        assert_eq!(
            f.sends().len(),
            1,
            "failed submit plan A is not a daemon retry"
        );
        f.backend.wire.lock().unwrap().blockhash = 17;
        f.seed("audit-b", 3)?;
        f.tick(3).await?;
        assert_eq!(f.sends().len(), 1);
        f.emit(&format!("{mode:?}_new_b"), "audit-a")?;
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn b53_not_sent_classification_never_refreshes_an_unknown_signed_message() -> Result<()> {
    for mode in [
        Send::HttpError,
        Send::MissingResult,
        Send::RpcError,
        Send::Disconnect,
        Send::EmptyResult,
        Send::Mismatch,
    ] {
        let mut f = Fixture::new().await?;
        f.seed("audit-a", 0)?;
        f.rpc.state.lock().unwrap().mode = mode;
        f.tick(0).await?;
        assert_eq!(f.sends().len(), 1);
        f.reopen()?;
        let a = f.emit(&format!("{mode:?}_classified_not_sent"), "audit-a")?;
        assert_eq!(a["status"], EXECUTION_STATUS_CANARY_SUBMITTED);
        assert_eq!(a["signature"], f.sends()[0]["signature"]);
        assert_eq!(a["attempt"], 1);
        assert_eq!(a["selected"], true);
        f.backend.wire.lock().unwrap().blockhash = 17;
        f.tick(2).await?;
        assert_eq!(f.sends().len(), 1);
        f.emit(&format!("{mode:?}_new_message_retry"), "audit-a")?;
        f.finish().await?;
    }
    Ok(())
}
