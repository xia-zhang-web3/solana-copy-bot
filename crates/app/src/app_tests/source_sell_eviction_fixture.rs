//! Hold the real A worker before proof while real ingress evicts its delivery hint.
use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::source_sell_ingress::RecentSwapDelivery;
use crate::source_sell_staging::{
    SourceSellAdmission, SourceSellStaging, StageNotice, SOURCE_SELL_BINDING_CAPACITY,
};
use anyhow::{Context, Result};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ExecutionSourceSellReject as Reject, SourceSellHandoff, SqliteStore};
use std::{
    collections::{HashSet, VecDeque},
    sync::mpsc,
    time::Duration,
};

pub(super) struct HeldEviction {
    release: Option<mpsc::Sender<()>>,
    original: SourceSellHandoff,
    fillers: HashSet<String>,
    empty_store: SqliteStore,
}

impl Drop for HeldEviction {
    fn drop(&mut self) {
        // An assertion failure must also release the blocking worker.
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

fn hint(f: &mut Ingress, store: &SqliteStore, a: &SwapEvent) -> Result<SourceSellAdmission> {
    // Read-only probe of the actual hint cache without the durable lookup masking
    // it. A known-repeat capture cannot insert a hint or schedule any work.
    let mut recent = HashSet::from([a.signature.clone()]);
    let mut order = VecDeque::from([a.signature.clone()]);
    f.scheduler
        .source_sells
        .capture(store, &RecentSwapDelivery::note(&mut recent, &mut order, a))
}

impl HeldEviction {
    pub(super) async fn enter(f: &mut Ingress, a: &SwapEvent) -> Result<Self> {
        assert!(f.scheduler.source_sells.is_empty());
        let mut empty_store = SqliteStore::open(":memory:")?;
        empty_store.run_migrations(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"),
        )?;
        assert!(
            matches!(hint(f, &empty_store, a)?, SourceSellAdmission::Captured(_)),
            "A hint present before actual eviction"
        );
        let original = f.store.load_source_sell_handoff(&a.signature)?.unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release, wait) = mpsc::channel();
        f.scheduler.source_sells.before_proof = Some(Box::new(move || {
            entered_tx.send(()).expect("fixture awaiting proof entry");
            wait.recv_timeout(Duration::from_secs(20))
                .expect("bounded A proof release");
        }));
        let mut held = Self {
            release: Some(release),
            original,
            fillers: HashSet::new(),
            empty_store,
        };
        f.send(a, true).await?;
        tokio::time::timeout(Duration::from_secs(3), entered_rx)
            .await
            .context("A proof entered deadline")??;
        held.assert_pending(f, a, "entered")?;
        let before = f.money()?;
        tokio::time::timeout(Duration::from_secs(8), async {
            for i in 0..SOURCE_SELL_BINDING_CAPACITY {
                let filler = f.sell(&format!("b133-eviction-{i}"), "foreign-source");
                assert!(held.fillers.insert(filler.signature.clone()));
                let (result, events) = capture(f.send(&filler, true)).await;
                result?;
                assert!(events.iter().any(
                    |e| e["signature"] == filler.signature && e["reason"] == "worker_capacity"
                ));
                let job = f
                    .store
                    .load_source_sell_handoff(&filler.signature)?
                    .unwrap();
                assert_eq!(job.original_position_id, held.original.original_position_id);
                assert_eq!(job.disposition, "pending");
                assert!(f.staged(&filler.signature)?.is_none());
                // ACTIVE_LIMIT1: no filler completion can occur while A is held.
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("128 actual ingress ACK deadline")??;
        assert_eq!(f.money()?, before);
        held.assert_pending(f, a, "after_128_ack")?;
        Ok(held)
    }

    pub(super) fn assert_pending(&self, f: &Ingress, a: &SwapEvent, boundary: &str) -> Result<()> {
        let job = f.store.load_source_sell_handoff(&a.signature)?.unwrap();
        assert_eq!(format!("{job:?}"), format!("{:?}", self.original));
        assert_eq!(job.disposition, "pending");
        assert!(f.staged(&a.signature)?.is_none());
        assert!(!f.scheduler.source_sells.is_empty());
        let observed: i64 = f.conn()?.query_row(
            "SELECT count(*) FROM observed_swaps WHERE signature=?1",
            [&a.signature],
            |r| r.get(0),
        )?;
        assert_eq!(observed, 1, "pending A remains retention-pinned");
        eprintln!("B133_PENDING boundary={boundary} signature={} original={:?} current={} ack={} observed={observed} stage=none", a.signature, job.original_position_id, f.position()?, self.fillers.len());
        Ok(())
    }

    pub(super) async fn release_and_drain(mut self, f: &mut Ingress, a: &SwapEvent) -> Result<()> {
        self.assert_pending(f, a, "B_before_release")?;
        assert_ne!(Some(f.position()?), self.original.original_position_id);
        let before = f.money()?;
        self.release.take().unwrap().send(())?;
        let (result, events) = capture(f.stage_completion()).await;
        let completion = result?;
        assert_eq!(completion.signature, a.signature);
        assert_eq!(
            completion.notice,
            StageNotice::Rejected(Reject::GenerationMismatch)
        );
        assert!(events
            .iter()
            .any(|e| e["signature"] == a.signature && e["reason"] == "generation_mismatch"));
        assert!(f.scheduler.source_sells.is_empty());
        assert!(
            matches!(
                hint(f, &self.empty_store, a)?,
                SourceSellAdmission::Refused(StageNotice::GenerationUnknown)
            ),
            "actual A hint evicted; active worker and durable lookup cannot mask probe"
        );
        let mut timer = SourceSellStaging::recovery_interval();
        tokio::time::timeout(Duration::from_secs(15), async {
            for _ in 0..SOURCE_SELL_BINDING_CAPACITY + 4 {
                if self.fillers.is_empty() {
                    return Ok::<_, anyhow::Error>(());
                }
                f.scheduler
                    .source_sells
                    .recover(&f.store, &f.path.to_string_lossy())?;
                if f.scheduler.source_sells.is_empty() {
                    timer.tick().await; // Real recovery cadence; no sleep to win a race.
                    continue;
                }
                let done = f.stage_completion().await?;
                assert!(
                    self.fillers.remove(&done.signature),
                    "unexpected recovery completion {done:?}"
                );
                assert_eq!(
                    done.notice,
                    StageNotice::Rejected(Reject::GenerationMismatch)
                );
            }
            anyhow::ensure!(self.fillers.is_empty(), "bounded recovery visits exhausted");
            Ok(())
        })
        .await
        .context("signature-aware handoff drain deadline")??;
        let pending: i64 = f.conn()?.query_row(
            "SELECT count(*) FROM source_sell_handoffs WHERE disposition='pending'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(pending, 0);
        assert!(f.scheduler.source_sells.is_empty());
        assert!(f.staged(&a.signature)?.is_none());
        assert_eq!(f.money()?, before);
        eprintln!("B133_DRAIN signature={} hint=evicted fillers={} pending=0 A=GenerationMismatch money=unchanged", a.signature, SOURCE_SELL_BINDING_CAPACITY);
        Ok(())
    }
}
