use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::source_sell_staging::{StageCompletion, StageNotice};
use anyhow::{Context, Result};
use copybot_core_types::SwapEvent;
use copybot_storage_core::ExecutionSourceSellReject as Reject;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(super) fn new_unfollowed() -> Result<Ingress> {
    let mut f = Ingress::new()?;
    f.buy("batch57-buy", "source-b")?;
    f.follow_source("source-b")?;
    f.store
        .deactivate_follow_wallet("source-b", f.now, "unfollowed")?;
    Arc::make_mut(&mut f.follow).active.clear();
    assert!(f.store.list_active_follow_wallets()?.is_empty());
    assert!(f.follow.active.is_empty() && f.lots.is_empty());
    assert!(f.money()?["shadow_lots"].is_empty());
    Ok(f)
}

pub(super) fn event_reason(events: &[BTreeMap<String, String>], signature: &str, reason: &str) {
    assert!(
        events.iter().any(
            |e| e.get("signature").map(String::as_str) == Some(signature)
                && e.get("reason").map(String::as_str) == Some(reason)
        ),
        "{events:?}"
    );
    eprintln!("B57_EVENTS {}", serde_json::json!(events));
}

pub(super) async fn capacity_cut(f: &mut Ingress, b: &SwapEvent) -> Result<()> {
    let a = f.sell("batch57-foreign-busy-a", "unrelated-source");
    let release = f.pause_worker();
    f.send(&a, true).await?;
    let (result, events) = capture(f.send(b, true)).await;
    release.send(())?;
    let completion = f.stage_completion().await?;
    result?;
    assert_eq!(completion.signature, a.signature);
    assert_eq!(
        completion.notice,
        StageNotice::Rejected(Reject::SourceNotProven)
    );
    event_reason(&events, &b.signature, "worker_capacity");
    assert!(f.staged(&b.signature)?.is_none());
    assert!(f.scheduler.source_sells.is_empty());
    assert_eq!(
        f.store
            .load_observed_swaps_since(f.now)?
            .iter()
            .filter(|e| e.signature == b.signature)
            .count(),
        1
    );
    assert_eq!(
        f.store
            .load_source_sell_handoff(&b.signature)?
            .unwrap()
            .disposition,
        "pending"
    );
    Ok(()) // B provably never spawned; all A effects have completed.
}

pub(super) async fn recover(f: &mut Ingress, signature: &str) -> Result<Vec<StageCompletion>> {
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut outputs = Vec::new();
        loop {
            // Exact same scheduler method as app_loop startup/completion/periodic tick.
            f.scheduler
                .source_sells
                .recover(&f.store, &f.path.to_string_lossy())?;
            if !f.scheduler.source_sells.is_empty() {
                outputs.push(f.stage_completion().await?);
            }
            if f.store
                .load_source_sell_handoff(signature)?
                .is_some_and(|h| h.disposition != "pending")
            {
                return Ok(outputs);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("bounded autonomous recovery deadline")?
}

pub(super) fn money_unchanged_except_signal(
    f: &Ingress,
    before: &BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let after = f.money()?;
    for (table, rows) in before {
        if table != "copy_signals" {
            assert_eq!(&after[table], rows, "{table}");
        }
    }
    Ok(())
}

pub(super) fn produce(f: &Ingress, signature: &str) -> Result<()> {
    let mut inserted = 0;
    for _ in 0..3 {
        let out = crate::execution_source_sell_producer::produce(&f.store)?;
        assert!(out.wrapped);
        inserted += out.inserted;
    }
    assert_eq!(inserted, 1);
    assert!(f
        .store
        .load_copy_signal_by_signal_id(&format!("shadow:{signature}:source-b:sell:mint"))?
        .is_some());
    Ok(())
}
