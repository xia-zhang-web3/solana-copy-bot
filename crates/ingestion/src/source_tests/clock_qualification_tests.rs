//! Explicit isolated clock qualification. No transport, provider or SQLite action.
use super::{capture_scope::CaptureScope, continuity_telemetry::*, reader_diagnostics};
use serde_json::json;
use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
#[test]
#[ignore = "explicit isolated Linux clock qualification env only"]
fn clock_qualification_emit_linux_witness() {
    assert!(cfg!(target_os = "linux"), "Linux qualification only");
    let out = std::path::PathBuf::from(std::env::var("COPY49_CLOCK_OUT").unwrap());
    assert!(out.is_absolute() && !out.exists());
    let run = std::env::var("COPY49_CLOCK_RUN_ID").unwrap();
    super::diagnostic_clock::initialize();
    let telemetry = Arc::new(ContinuityTelemetry::default());
    let observation = telemetry.reader("SYNTHETIC_CLOCK_HOOK_FIXTURE".into(), 0);
    let identity = observation.identity();
    let scope = CaptureScope::new(
        &[],
        std::iter::empty(),
        HashSet::new(),
        Some(Arc::new(AtomicBool::new(true))),
    )
    .observed(observation);
    let initial = telemetry.snapshot();
    std::thread::sleep(Duration::from_millis(5));
    scope.frontier(2020);
    telemetry.acknowledged(CheckpointWitness {
        session: identity.session.clone(),
        sequence: 1,
        slot: 2020,
        observed_ns: 0,
        reader_id: None,
    });
    let during = telemetry.snapshot();
    scope.clear_verified();
    let cleared = telemetry.snapshot();
    std::thread::sleep(Duration::from_millis(5));
    scope.reader_dropped();
    let after = telemetry.snapshot();
    let terminal = reader_diagnostics::event(
        Some(identity),
        "synthetic_terminal_clock_hook",
        "NO_READER_NETWORK_STARTED",
    );
    let mut direct = Vec::new();
    for _ in 0..16 {
        let (elapsed, stamp) = diagnostic_time();
        assert_eq!(stamp.elapsed_ns, elapsed);
        assert_eq!(stamp.identity.run_id.as_deref(), Some(run.as_str()));
        assert_eq!(stamp.status, "PRESENT_NOT_ORDER_QUALIFIED");
        assert!(stamp.before_ns.unwrap() <= stamp.after_ns.unwrap());
        direct.push(stamp);
        std::thread::sleep(Duration::from_millis(2));
    }
    let witness = json!({"schema":"actual_linux_rust_clock_witness_v1","run_id":run,"pid":std::process::id(),
        "initial":initial,"during":during,"cleared":cleared,"after":after,"terminal":terminal,"samples":direct,
        "fixture":"SYNTHETIC_CLOCK_HOOKS_NOT_REAL_READER_OR_SQLITE_ACK","provider_network_financial_actions":0,"wire_order_qualified":false});
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&out)
        .unwrap();
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(&witness).unwrap())
        .unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
    println!("CLOCK_QUALIFICATION_RUST_WITNESS_COMPLETE samples=16 synthetic_hooks=1 provider=0");
}

#[test]
fn clock_diagnostic_sampler_keeps_elapsed_and_missing_identity_unqualified() {
    let mut calls = 0;
    let (elapsed, stamp) = super::diagnostic_clock::observe(|| {
        calls += 1;
        123456
    });
    assert_eq!(calls, 1);
    assert_eq!(elapsed, 123456);
    assert_eq!(stamp.elapsed_ns, elapsed);
    // No startup initialization has occurred in this exact isolated test process.
    assert_eq!(stamp.status, "UNAVAILABLE_OR_INCOMPLETE");
    assert!(stamp.identity.boot_id.is_none());
    assert!(stamp.identity.run_id.is_none());
    assert!(stamp.resolution_ns.is_none());
}

// New ACK fixtures are registered from this existing external test root.
#[path = "ack_observation/ack_hook.rs"]
pub(in crate::source) mod ack_hook;
#[path = "ack_observation/local_clock.rs"]
pub(in crate::source) mod local_clock;
#[path = "ack_observation/tracker_impl.rs"]
mod tracker_impl;
