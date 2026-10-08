//! Actual scheduler with a logical service clock. This is not network/live proof.
use super::OrderedPipeline;
use futures_util::{stream, StreamExt};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

const PROFILES: [&str; 2] = [
    include_str!("../../tests/fixtures/recovery_06_profile_465.json"),
    include_str!("../../tests/fixtures/recovery_06_profile_659.json"),
];
#[derive(Clone)]
struct Record {
    slot: u64,
    service_ms: u64,
    bytes: u64,
}
fn records(profile: &str) -> Vec<Record> {
    let profile: Value = serde_json::from_str(profile).unwrap();
    let rows = profile["records"].as_array().unwrap();
    assert!(rows.last().unwrap()["is_live_anchor"].as_bool().unwrap());
    rows.iter()
        .map(|row| Record {
            slot: row["slot"].as_u64().unwrap(),
            // Failed frontend elapsed time is unknown. These add the captured
            // nonzero backend lower bound and accepted minimum retry backoff.
            service_ms: row["observed_attempt_service_lower_bound_ms"]
                .as_u64()
                .unwrap()
                + row["modeled_minimum_retry_backoff_ms"].as_u64().unwrap(),
            bytes: row["body_bytes"].as_u64().unwrap(),
        })
        .collect()
}
#[derive(Default)]
struct State {
    now: u64,
    timers: BTreeMap<u64, (u64, Waker)>,
    network: usize,
    peak_network: usize,
    charged: usize,
    peak_charged: usize,
    raw_bytes: u64,
    peak_raw_bytes: u64,
}
#[derive(Default)]
struct Clock {
    state: Mutex<State>,
    sequence: AtomicU64,
}
impl Clock {
    fn wait(self: &Arc<Self>, milliseconds: u64) -> Timer {
        Timer {
            clock: self.clone(),
            id: self.sequence.fetch_add(1, Ordering::SeqCst),
            milliseconds,
            deadline: None,
        }
    }
    fn advance(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(next) = state
            .timers
            .values()
            .map(|(time, _)| *time)
            .filter(|time| *time > state.now)
            .min()
        else {
            return false;
        };
        state.now = next;
        let wake: Vec<_> = state
            .timers
            .values()
            .filter(|(time, _)| *time <= next)
            .map(|(_, waker)| waker.clone())
            .collect();
        drop(state);
        for waker in wake {
            waker.wake();
        }
        true
    }
}
struct Timer {
    clock: Arc<Clock>,
    id: u64,
    milliseconds: u64,
    deadline: Option<u64>,
}
impl Future for Timer {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let timer = self.get_mut();
        let mut state = timer.clock.state.lock().unwrap();
        let deadline = *timer.deadline.get_or_insert(state.now + timer.milliseconds);
        if state.now >= deadline {
            state.timers.remove(&timer.id);
            Poll::Ready(())
        } else {
            state
                .timers
                .insert(timer.id, (deadline, cx.waker().clone()));
            Poll::Pending
        }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.clock.state.lock().unwrap().timers.remove(&self.id);
    }
}
struct ModelRaw {
    slot: u64,
    bytes: u64,
    clock: Arc<Clock>,
    completed: bool,
}
impl Drop for ModelRaw {
    fn drop(&mut self) {
        let mut state = self.clock.state.lock().unwrap();
        state.charged -= 1;
        if self.completed {
            state.raw_bytes -= self.bytes;
        } else {
            state.network -= 1;
        }
    }
}
async fn fetch(record: Record, clock: Arc<Clock>) -> anyhow::Result<ModelRaw> {
    {
        let mut state = clock.state.lock().unwrap();
        state.charged += 1;
        state.peak_charged = state.peak_charged.max(state.charged);
        state.network += 1;
        state.peak_network = state.peak_network.max(state.network);
    }
    let mut raw = ModelRaw {
        slot: record.slot,
        bytes: record.bytes,
        clock: clock.clone(),
        completed: false,
    };
    clock.wait(record.service_ms).await;
    {
        let mut state = clock.state.lock().unwrap();
        state.network -= 1;
        state.raw_bytes += record.bytes;
        state.peak_raw_bytes = state.peak_raw_bytes.max(state.raw_bytes);
    }
    raw.completed = true;
    Ok(raw)
}
struct Report {
    milliseconds: u64,
    peak_network: usize,
    peak_charged: usize,
    peak_raw_bytes: u64,
}
async fn run(rows: Vec<Record>, width: usize, window: usize, lazy: bool, apply_ms: u64) -> Report {
    let clock = Arc::new(Clock::default());
    let consumer_clock = clock.clone();
    let expected: Vec<_> = rows.iter().map(|row| row.slot).collect();
    let consumer = tokio::spawn(async move {
        let mut applied = Vec::new();
        if lazy {
            // Accepted buffered fetches include the final live anchor.
            let service = consumer_clock.clone();
            let blocks = stream::iter(rows)
                .map(move |row| fetch(row, service.clone()))
                .buffered(width);
            tokio::pin!(blocks);
            while let Some(raw) = blocks.next().await {
                let raw = raw.unwrap();
                if apply_ms > 0 {
                    consumer_clock.wait(apply_ms).await;
                }
                applied.push(raw.slot);
                drop(raw);
            }
        } else {
            let mut rows = rows;
            let anchor = rows.pop().unwrap();
            let service = consumer_clock.clone();
            let lookup = Arc::new(
                rows.iter()
                    .cloned()
                    .map(|row| (row.slot, row))
                    .collect::<BTreeMap<_, _>>(),
            );
            let mut blocks = OrderedPipeline::start(
                rows.iter().map(|row| row.slot).collect(),
                width,
                window,
                move |slot| fetch(lookup[&slot].clone(), service.clone()),
            )
            .unwrap();
            while let Some(raw) = blocks.next().await.unwrap() {
                let (raw, permit) = raw.into_parts();
                if apply_ms > 0 {
                    consumer_clock.wait(apply_ms).await;
                }
                applied.push(raw.slot);
                drop(raw);
                drop(permit);
            }
            let raw = fetch(anchor, consumer_clock.clone()).await.unwrap();
            if apply_ms > 0 {
                consumer_clock.wait(apply_ms).await;
            }
            applied.push(raw.slot);
            drop(raw);
        }
        assert_eq!(applied, expected);
    });
    let mut idle = 0;
    // Let runnable fetch/application tasks settle at each logical instant.
    // There are no real-time sleeps or network operations in this harness.
    for _ in 0..100_000 {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        if consumer.is_finished() {
            break;
        }
        if clock.advance() {
            idle = 0;
        } else {
            idle += 1;
            assert!(
                idle < 100,
                "logical pipeline stalled without a service timer"
            );
        }
    }
    assert!(consumer.is_finished(), "logical pipeline did not finish");
    consumer.await.unwrap();
    let state = clock.state.lock().unwrap();
    assert_eq!(state.network, 0);
    assert_eq!(state.charged, 0);
    assert_eq!(state.raw_bytes, 0);
    assert!(state.peak_network <= width);
    assert!(state.peak_charged <= window);
    Report {
        milliseconds: state.now,
        peak_network: state.peak_network,
        peak_charged: state.peak_charged,
        peak_raw_bytes: state.peak_raw_bytes,
    }
}

#[tokio::test]
async fn saved_profiles_actual_pipeline_keeps_width4_blocker_and_tests_unapproved_window32_proposals(
) {
    for profile in PROFILES {
        let rows = records(profile);
        let count = rows.len();
        let archive_bytes: u64 = rows.iter().map(|row| row.bytes).sum();
        let service_sum: u64 = rows.iter().map(|row| row.service_ms).sum();
        // Even ideal width4 service sharing cannot sustain the observed3.8/s.
        assert!((count as f64) * 4000.0 / (service_sum as f64) < 3.8);
        for apply_ms in [0, 9] {
            let old = run(rows.clone(), 4, 4, true, apply_ms).await;
            let changed = run(rows.clone(), 4, 4, false, apply_ms).await;
            let proposal8 = run(rows.clone(), 8, 32, false, apply_ms).await;
            let proposal9 = run(rows.clone(), 9, 32, false, apply_ms).await;
            let proposal10 = run(rows.clone(), 10, 32, false, apply_ms).await;
            let mut proposed_widths_under_modeled_count = Vec::new();
            for (mode, report, width, window) in [
                ("accepted_lazy4", old, 4, 4),
                ("changed4", changed, 4, 4),
                ("UNAPPROVED_proposal8_window32", proposal8, 8, 32),
                ("UNAPPROVED_proposal9_window32", proposal9, 9, 32),
                ("UNAPPROVED_proposal10_window32", proposal10, 10, 32),
            ] {
                let arrivals = report.milliseconds as f64 * 3.8 / 1000.0;
                let modeled_count_exceeds_384 = arrivals > 384.0;
                eprintln!("SAVED_PROFILE_SCHEDULING count={count} mode={mode} modeled_apply_ms={apply_ms} logical_ms={} modeled_arrivals={arrivals:.3} modeled_count_exceeds_384={modeled_count_exceeds_384} minimum_modeled_block_capture_count={} historical_capture_count_failure_claim=false original_archive_bytes={archive_bytes} peak_network={} peak_charged={} peak_completed_raw_bytes={} window_worst_case_bytes={} final_anchor_service_included=true failure_service=CAPTURED_LOWER_BOUND not_live_proof=true",
                    report.milliseconds, arrivals.ceil() as u64, report.peak_network, report.peak_charged, report.peak_raw_bytes, window * (16 << 20));
                if mode == "changed4" {
                    assert!(arrivals > count as f64);
                }
                if width > 4 && !modeled_count_exceeds_384 {
                    proposed_widths_under_modeled_count.push(width);
                }
                // Proposal numbers are scheduling evidence, not authority or
                // catchup acceptance: normalization/getBlocks/ACK costs omitted.
            }
            assert!(!proposed_widths_under_modeled_count.is_empty());
            eprintln!("SAVED_PROFILE_BOUNDARY_PROPOSAL count={count} modeled_apply_ms={apply_ms} tested_widths_under_modeled_capture_count384={proposed_widths_under_modeled_count:?} total_window32=true owner_decision_required=true live_capture_may_also_contain_other_update_kinds=true");
        }
    }
}
