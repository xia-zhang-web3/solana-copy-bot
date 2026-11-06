use super::*;
use std::cell::Cell;

#[test]
fn fake_monotonic_slow_refresh_process_and_output_remain_separate() {
    let ledger = StageLedger::default();
    for (stage, millis) in [
        (Stage::RefreshTotal, 80),
        (Stage::NextNetworkDecodeSchedulingWait, 7),
        (Stage::ScopedProcessTotal, 400),
        (Stage::OutputPushWait, 250),
    ] {
        ledger.record_span(
            stage,
            Duration::from_millis(100),
            Duration::from_millis(100 + millis),
        );
    }
    let snapshot = ledger.snapshot().unwrap();
    assert_eq!(
        snapshot.stages[Stage::RefreshTotal as usize].sum,
        80_000_000
    );
    assert_eq!(
        snapshot.stages[Stage::ScopedProcessTotal as usize].max,
        400_000_000
    );
    assert_eq!(
        snapshot.stages[Stage::OutputPushWait as usize].sum,
        250_000_000
    );
    assert_eq!(
        snapshot.stages[Stage::NextNetworkDecodeSchedulingWait as usize].count,
        1
    );
    assert_eq!(snapshot.stages[Stage::NextPollExecution as usize].count, 0);
    assert_eq!(
        Stage::ScopedProcessTotal.parent(),
        Some(Stage::SerialLoopTurnTotal)
    );
    assert_eq!(
        Stage::PayloadEncodeHash.parent(),
        Some(Stage::ScopedProcessTotal)
    );
}

#[test]
fn delegated_poll_is_exactly_once_and_preserves_pending_ready_and_error() {
    let ledger = StageLedger::default();
    let calls = Cell::new(0);
    for expected in [
        Poll::Pending,
        Poll::Ready(Ok(42)),
        Poll::Ready(Err("same error")),
    ] {
        let tick = Cell::new(0);
        let result = ledger.measure_poll_with(
            || {
                let value = tick.get();
                tick.set(value + 1);
                Duration::from_nanos(100 + value * 25)
            },
            || {
                calls.set(calls.get() + 1);
                expected
            },
        );
        assert_eq!(result, expected);
        assert_eq!(tick.get(), 2);
    }
    assert_eq!(calls.get(), 3);
    let snapshot = ledger.snapshot().unwrap();
    let poll = snapshot.stages[Stage::NextPollExecution as usize];
    assert_eq!((poll.count, poll.sum, poll.max), (3, 75, 25));
    assert_eq!((snapshot.poll_pending, snapshot.poll_ready), (1, 2));
    assert!(Stage::NextNetworkDecodeSchedulingWait
        .name()
        .contains("network_decode_scheduling_wait"));
    assert!(Stage::NextPollExecution.name().contains("wall_not_cpu"));
    assert_eq!(
        Stage::NextPollExecution.parent(),
        Some(Stage::NextNetworkDecodeSchedulingWait)
    );
    assert_eq!(ledger.measure_poll(|| Poll::Ready(11)), Poll::Ready(11));
}

#[test]
fn created_at_absent_invalid_future_and_available_are_explicit_provider_metadata() {
    let now = 1_800_000_000_i128 * 1_000_000_000;
    assert_eq!(ProviderAge::at(None, now), ProviderAge::Absent);
    assert_eq!(
        ProviderAge::at(Some((1_800_000_000, -1)), now),
        ProviderAge::Invalid
    );
    assert_eq!(
        ProviderAge::at(Some((1_800_000_000, 1_000_000_000)), now),
        ProviderAge::Invalid
    );
    assert_eq!(
        ProviderAge::at(Some((253_402_300_800, 0)), now),
        ProviderAge::Invalid
    );
    assert_eq!(
        ProviderAge::at(Some((1_800_000_000, 1)), now),
        ProviderAge::Future(0)
    );
    assert_eq!(
        ProviderAge::at(Some((1_800_000_001, 0)), now),
        ProviderAge::Future(1_000)
    );
    assert_eq!(
        ProviderAge::at(Some((1_799_999_728, 0)), now),
        ProviderAge::Available(272_000)
    );
    let ledger = StageLedger::default();
    ledger.decoded_age(false, ProviderAge::Absent);
    ledger.decoded_age(true, ProviderAge::Invalid);
    ledger.decoded_age(true, ProviderAge::Future(1_000));
    ledger.decoded_age(true, ProviderAge::Available(272_000));
    let s = ledger.snapshot().unwrap();
    assert_eq!(s.decoded_updates_age.absent, 1);
    assert_eq!(s.decoded_transactions_age.absent, 0);
    assert_eq!(s.decoded_transactions_age.invalid, 1);
    assert_eq!(s.decoded_transactions_age.future, 1);
    assert_eq!(s.decoded_transactions_age.available_ms.max, 272_000);
}

#[test]
fn paired_output_cohort_is_separate_from_all_decoded_frames() {
    let ledger = StageLedger::default();
    for age in [2, 30, 900] {
        ledger.decoded_age(true, ProviderAge::Available(age));
    }
    ledger.enqueued_age_pair(ProviderAge::Available(30), ProviderAge::Available(430));
    let s = ledger.snapshot().unwrap();
    assert_eq!(s.decoded_transactions_age.available_ms.count, 3);
    assert_eq!(s.enqueued_pairs, 1);
    assert_eq!(s.enqueued_before_process_age.available_ms.sum, 30);
    assert_eq!(s.enqueued_after_process_age.available_ms.sum, 430);
    ledger.enqueued_age_pair(ProviderAge::Future(20), ProviderAge::Available(10));
    let s = ledger.snapshot().unwrap();
    assert_eq!(s.enqueued_pairs, 2);
    assert_eq!(s.enqueued_before_process_age.future, 1);
    assert_eq!(s.enqueued_after_process_age.available_ms.count, 2);
    // A future→available transition stays visible; separate quantiles cannot be subtracted.
}

#[test]
fn fixed_histogram_memory_and_saturating_sums_stay_bounded() {
    let ledger = StageLedger::default();
    for _ in 0..100_000 {
        ledger.record(Stage::DomainParse, Duration::from_micros(12));
    }
    ledger.record(Stage::CaptureFinish, Duration::from_nanos(u64::MAX));
    ledger.record(Stage::CaptureFinish, Duration::from_nanos(u64::MAX));
    let s = ledger.snapshot().unwrap();
    let parse = s.stages[Stage::DomainParse as usize];
    assert_eq!(parse.count, 100_000);
    assert_eq!(parse.histogram.iter().sum::<u64>(), 100_000);
    assert_eq!(parse.histogram.len(), 8);
    assert_eq!(s.stages[Stage::CaptureFinish as usize].sum, u64::MAX);
    assert_eq!(s.stages.len(), STAGE_COUNT);
    assert!(std::mem::size_of::<StageLedger>() < 8_192);
    assert_eq!(
        Stage::ALL.map(|x| x as usize),
        std::array::from_fn::<_, STAGE_COUNT, _>(|i| i)
    );
}

#[test]
fn raii_turns_include_continue_break_and_expose_active_parent() {
    let ledger = StageLedger::default();
    for i in 0..3 {
        let _turn = ledger.timer(Stage::SerialLoopTurnTotal);
        assert_eq!(
            ledger.snapshot().unwrap().active[Stage::SerialLoopTurnTotal as usize],
            1
        );
        if i == 0 {
            continue;
        }
        if i == 2 {
            break;
        }
    }
    let s = ledger.snapshot().unwrap();
    assert_eq!(s.stages[Stage::SerialLoopTurnTotal as usize].count, 3);
    assert_eq!(s.active[Stage::SerialLoopTurnTotal as usize], 0);
}

#[test]
fn poisoned_measurement_is_unavailable_without_changing_poll_result() {
    let ledger = StageLedger::default();
    std::thread::scope(|scope| {
        let job = scope.spawn(|| {
            let _lock = ledger.state.lock().unwrap();
            panic!("synthetic poison");
        });
        assert!(job.join().is_err());
    });
    assert!(ledger.snapshot().is_none());
    ledger.record(Stage::DomainParse, Duration::from_secs(1));
    let calls = Cell::new(0);
    let result = ledger.measure_poll_with(
        || Duration::ZERO,
        || {
            calls.set(calls.get() + 1);
            Poll::Ready(77)
        },
    );
    assert_eq!(result, Poll::Ready(77));
    assert_eq!(calls.get(), 1);
    assert!(ledger.snapshot().is_none());
}
