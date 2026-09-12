use super::{
    source_sell_event_capture::capture,
    source_sell_handoff_fixture::{event_reason, new_unfollowed, recover},
    source_sell_ingress_fixture::Ingress,
};
use anyhow::{Context, Result};
use std::{
    future::Future,
    sync::{mpsc::Sender, Arc},
    task::{Context as TaskContext, Poll, Wake, Waker},
    time::Duration,
};

struct JoinReadyWake(Arc<tokio::sync::Notify>);
impl Wake for JoinReadyWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
}

// Register the same pending finish_next future used in app_loop::select, then
// cancel it while the worker is provably held. The wake proves JoinSet readiness;
// nobody joins/reaps the result before the next actual ingress handler does so.
async fn release_to_join_ready(f: &mut Ingress, release: Sender<()>) -> Result<()> {
    let ready = Arc::new(tokio::sync::Notify::new());
    let waker = Waker::from(Arc::new(JoinReadyWake(Arc::clone(&ready))));
    {
        let mut context = TaskContext::from_waker(&waker);
        let mut pending = Box::pin(f.scheduler.source_sells.finish_next());
        assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
    }
    release.send(()).context("release held worker")?;
    tokio::time::timeout(Duration::from_secs(5), ready.notified())
        .await
        .context("held worker must notify actual JoinSet readiness")?;
    assert!(
        !f.scheduler.source_sells.is_empty(),
        "ready result remains unjoined"
    );
    Ok(())
}

#[tokio::test]
async fn root_b57_ingress_ready_completion_cannot_let_new_arrivals_bypass_old_pending() -> Result<()>
{
    let mut f = new_unfollowed()?;
    let b = f.sell("root-old-pending-b", "source-b");
    let money = f.money()?;
    // Reachable initial state: A remains held throughout B's single delivery.
    // No join, reap, or empty-slot scheduling shortcut occurs before loop round 1.
    let mut current = f.sell("root-hot-0", "unrelated-source");
    let mut release = f.pause_worker();
    f.send(&current, true).await?;
    let (result, events) = capture(f.send(&b, true)).await;
    result?;
    event_reason(&events, &b.signature, "worker_capacity");
    assert_eq!(
        f.store
            .load_source_sell_handoff(&b.signature)?
            .unwrap()
            .disposition,
        "pending"
    );
    assert!(f.staged(&b.signature)?.is_none());
    assert!(
        !f.scheduler.source_sells.is_empty(),
        "A is still held, not joined"
    );
    // Each fresh C is delivered through real ACK/ingress. A cursor wrap can
    // start the production 1s cooldown; arrivals continue throughout that time.
    let started = std::time::Instant::now();
    let original_position = f.position()?;
    let mut arrivals = vec![current.signature.clone()];
    let mut pending_each_round = Vec::new();
    let mut scheduled_b = 0;
    let mut elapsed_at_progress = None;
    let mut wrap_ready_by = None;
    let mut ready_rounds = 0;
    for round in 1..=128 {
        f.scheduler
            .source_sells
            .recover(&f.store, &f.path.to_string_lossy())?;
        assert!(!f.scheduler.source_sells.is_empty());
        let next = f.sell(&format!("root-hot-{round}"), "unrelated-source");
        let next_release = f.pause_worker();
        release_to_join_ready(&mut f, release).await?;
        if wrap_ready_by.is_some_and(|at| std::time::Instant::now() >= at) {
            ready_rounds += 1;
        }
        let (result, events) = capture(f.send(&next, true)).await;
        result?;
        if round == 1 {
            // The first post-A visit wrapped; after this upper bound the real
            // interval is certainly due, without inspecting/mutating its clock.
            wrap_ready_by = Some(
                std::time::Instant::now()
                    + crate::source_sell_staging::SOURCE_SELL_RECOVERY_INTERVAL,
            );
        }
        event_reason(
            &events,
            &current.signature,
            if current.signature == b.signature {
                "staged"
            } else {
                "source_not_proven"
            },
        );
        let scheduled: Vec<_> = events
            .iter()
            .filter(|e| e["reason"] == "scheduled")
            .collect();
        assert_eq!(
            scheduled.len(),
            1,
            "one active worker and its actual ID: {events:?}"
        );
        let selected = &scheduled[0]["signature"];
        if selected == &b.signature {
            scheduled_b += 1;
            event_reason(&events, &next.signature, "worker_capacity");
        }
        // Follow the actual selected job, never assume that ingress C won.
        current = if selected == &b.signature {
            b.clone()
        } else {
            f.sell(selected, "unrelated-source")
        };
        assert!(!f.scheduler.source_sells.is_empty());
        let pending = f.staged(&b.signature)?.is_none();
        pending_each_round.push(pending);
        arrivals.push(next.signature.clone());
        release = next_release;
        if !pending {
            elapsed_at_progress = Some(started.elapsed());
            break;
        }
        // Pace an uninterrupted stream across cooldown; no empty recovery pass
        // or quiet interval is used to get B a slot. The next worker stays held.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    release.send(())?;
    let last = f.stage_completion().await?;
    assert_eq!(last.signature, current.signature);
    // Positive control without competition, retained even if the hot-stream
    // assertion fails: the same B must be valid and recoverable in an empty slot.
    recover(&mut f, &b.signature).await?;
    let staged = f.staged(&b.signature)?.unwrap();
    assert_eq!(staged.position_id, original_position);
    let observed_b: i64 = f.conn()?.query_row(
        "SELECT COUNT(*) FROM observed_swaps WHERE signature=?1",
        [&b.signature],
        |r| r.get(0),
    )?;
    assert_eq!(observed_b, 1, "B was delivered once, never replayed");
    for signature in arrivals {
        recover(&mut f, &signature).await?;
        assert_eq!(
            f.store
                .load_source_sell_handoff(&signature)?
                .unwrap()
                .disposition,
            "refused"
        );
    }
    assert_eq!(f.money()?, money);
    f.finish().await?;
    eprintln!("ROOT_B57_ARRIVAL_FAIRNESS pending={pending_each_round:?} elapsed_at_progress={elapsed_at_progress:?} ready_rounds={ready_rounds} scheduled_b={scheduled_b} empty_slot_control=true all_workers_joined=true");
    assert!(
        elapsed_at_progress.is_some(),
        "continuous new arrivals starved the single B"
    );
    assert_eq!(
        scheduled_b, 1,
        "exactly one B worker under competing ingress"
    );
    assert!(
        (1..=3).contains(&ready_rounds),
        "bounded progress after recovery is due: {ready_rounds}"
    );
    Ok(())
}

#[tokio::test]
async fn root_b57_completion_during_ack_enters_the_same_durable_cursor() -> Result<()> {
    let mut f = new_unfollowed()?;
    // Explicit startup wrap; this separate ACK-boundary control does not stand
    // in for the uninterrupted-arrival fairness test above.
    f.scheduler
        .source_sells
        .recover(&f.store, &f.path.to_string_lossy())?;
    let a = f.sell("ack-held-a", "unrelated-source");
    let b = f.sell("ack-pending-b", "source-b");
    let c = f.sell("ack-incoming-c", "unrelated-source");
    let d = f.sell("ack-incoming-d", "unrelated-source");
    let original = f.position()?;
    let money = f.money()?;
    let release_a = f.pause_worker();
    f.send(&a, true).await?;
    f.send(&b, true).await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    f.scheduler
        .source_sells
        .recover(&f.store, &f.path.to_string_lossy())?;
    let ready = Arc::new(tokio::sync::Notify::new());
    let waker = Waker::from(Arc::new(JoinReadyWake(Arc::clone(&ready))));
    {
        let mut context = TaskContext::from_waker(&waker);
        let mut pending = Box::pin(f.scheduler.source_sells.finish_next());
        assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
    }
    let release_c = f.pause_worker();
    let conn = f.conn()?;
    conn.execute_batch("BEGIN IMMEDIATE")?; // Real writer cannot ACK yet.
    let mut ingress = Box::pin(capture(f.send(&c, true)));
    {
        let mut context = TaskContext::from_waker(Waker::noop());
        assert!(matches!(ingress.as_mut().poll(&mut context), Poll::Pending));
    }
    conn.execute_batch("COMMIT")?;
    release_a.send(())?;
    tokio::time::timeout(Duration::from_secs(5), ready.notified()).await?;
    // A is now join-ready, but ingress is still suspended at its writer await.
    let (result, events) = ingress.await;
    result?;
    event_reason(&events, &a.signature, "source_not_proven");
    event_reason(&events, &c.signature, "scheduled");
    let selected: String = conn.query_row(
        "SELECT signature FROM source_sell_handoffs h JOIN source_sell_handoff_cursor c ON h.sequence=c.last_sequence", [], |r| r.get(0))?;
    assert_eq!(
        selected, c.signature,
        "ACK continuation must checkpoint its selection"
    );
    release_to_join_ready(&mut f, release_c).await?;
    let (result, events) = capture(f.send(&d, true)).await;
    result?;
    event_reason(&events, &c.signature, "source_not_proven");
    event_reason(&events, &b.signature, "scheduled");
    event_reason(&events, &d.signature, "worker_capacity");
    assert_eq!(f.stage_completion().await?.signature, b.signature);
    assert_eq!(f.staged(&b.signature)?.unwrap().position_id, original);
    recover(&mut f, &d.signature).await?;
    assert_eq!(f.money()?, money);
    f.finish().await?;
    Ok(())
}
