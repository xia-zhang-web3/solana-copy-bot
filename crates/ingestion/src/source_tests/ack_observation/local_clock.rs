//! Explicit LOCAL test clock; never built into runtime or presented as Linux proof.
use crate::source::durable::diagnostic_clock::{ClockIdentity, ClockStamp};
use std::cell::Cell;
thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
pub(in crate::source) struct LocalClock;
impl LocalClock {
    pub(in crate::source) fn enter() -> Self {
        ENABLED.with(|v| v.set(true));
        Self
    }
}
impl Drop for LocalClock {
    fn drop(&mut self) {
        ENABLED.with(|v| v.set(false));
    }
}
pub(in crate::source) fn enabled() -> bool {
    ENABLED.with(Cell::get)
}
pub(in crate::source) fn observe(sample: impl FnOnce() -> u64) -> (u64, ClockStamp) {
    let elapsed = sample();
    (
        elapsed,
        ClockStamp {
            schema: "linux_monotonic_elapsed_bracket_v1",
            clock_id: "CLOCK_MONOTONIC",
            clock_code: 1,
            before_ns: Some(1_000_000_000 + elapsed),
            elapsed_ns: elapsed,
            after_ns: Some(1_000_000_001 + elapsed),
            resolution_ns: Some(1),
            status: "PRESENT_NOT_ORDER_QUALIFIED",
            identity: ClockIdentity {
                boot_id: Some("LOCAL_TEST_BOOT_NO_LINUX_PROOF".into()),
                time_namespace: Some("LOCAL_TEST_time:[1]".into()),
                timens_offsets: Some("LOCAL_TEST_monotonic0_boottime0".into()),
                pid_namespace: Some("LOCAL_TEST_pid:[1]".into()),
                pid: std::process::id(),
                process_start_ticks: Some(1),
                process_origin_monotonic_ns: Some(1_000_000_000),
                run_id: Some("copybot-alpenglow-recovery-readonly-20261021-01".into()),
            },
        },
    )
}
