//! Measurement-only fixtures. Status/ACK inputs are synthetic, no live/provider proof.
use super::*;
use crate::source::durable::{
    continuity_telemetry::monotonic_ns,
    reader_diagnostics::{event, ReaderDiagnostics},
    reader_error_witness::ErrorWitness,
};
use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicBool, Ordering},
};
fn scope(
    t: &Arc<DurableIngressTelemetry>,
    hold: Arc<AtomicBool>,
    generation: u64,
) -> Arc<CaptureScope> {
    CaptureScope::new(&[], std::iter::empty(), Default::default(), Some(hold)).observed(
        t.continuity
            .reader(format!("synthetic-diagnostic:{generation}"), generation),
    )
}
fn reader<S>(
    t: &Arc<DurableIngressTelemetry>,
    hold: Arc<AtomicBool>,
    stream: S,
    generation: u64,
) -> Reader
where
    S: Stream<Item = std::result::Result<SubscribeUpdate, tonic::Status>> + Unpin + Send + 'static,
{
    Reader::start_diagnosed(
        stream,
        futures_util::sink::drain(),
        2,
        1 << 20,
        1 << 20,
        t.clone(),
        scope(t, hold, generation),
        vec!["fixture-credential-do-not-export".into()],
    )
    .unwrap()
}
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
fn bundle(name: &str, value: &impl serde::Serialize) {
    if let Ok(root) = std::env::var("READER_DIAGNOSTIC_BUNDLE") {
        std::fs::write(
            std::path::Path::new(&root).join(name),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .unwrap();
    }
}
#[tokio::test]
async fn reader_diag_status_retained_before_unread_join_and_later_fatal_drop() {
    let t = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(false));
    let input = futures_util::stream::iter(vec![Err(tonic::Status::internal(
        "h2 protocol error: error reading a body from connection",
    ))]);
    let r = reader(&t, hold.clone(), input, 4);
    settle().await;
    assert!(r.task.as_ref().unwrap().is_finished()); // Do not consume End: mimics fatal catch-up before finish().
    let before = t.reader_diagnostics.snapshot();
    let terminal = before
        .events
        .iter()
        .find(|e| e.kind == "terminal_end")
        .unwrap();
    assert_eq!(terminal.outcome, "STATUS");
    assert_eq!(
        terminal.error.as_ref().unwrap().code.as_deref(),
        Some("Internal")
    );
    assert_eq!(terminal.reader.as_ref().unwrap().generation, 4);
    assert_eq!(
        terminal.reader.as_ref().unwrap().session,
        "synthetic-diagnostic:4"
    );
    assert!(hold.load(Ordering::Acquire));
    let fatal_observed = monotonic_ns();
    assert!(terminal.observed_ns <= fatal_observed);
    drop(r);
    settle().await;
    let after = t.reader_diagnostics.snapshot();
    assert_eq!(
        after
            .events
            .iter()
            .filter(|e| e.kind == "terminal_end")
            .count(),
        1
    );
    assert!(!after.events.iter().any(|e| e.kind == "task_future_dropped"));
    assert!(after.events.iter().any(
        |e| e.kind == "owner_abort_requested" && e.task_finished_at_abort_request == Some(true)
    ));
    assert_eq!(after.export_pending, 0);
    assert_eq!(after.retention_lost, 0);
    assert_eq!(after.export_completed, after.attempted);
    bundle(
        "actual-task-synthetic-status-before-unread-join.json",
        &after,
    );
}
#[tokio::test]
async fn reader_diag_abort_requested_future_drop_and_cancel_join_are_distinct() {
    let t = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(false));
    let r = reader(&t, hold.clone(), futures_util::stream::pending(), 5);
    settle().await;
    drop(r);
    settle().await;
    let snapshot = t.reader_diagnostics.snapshot();
    assert!(snapshot
        .events
        .iter()
        .any(|e| e.kind == "owner_abort_requested"));
    assert!(snapshot
        .events
        .iter()
        .any(|e| e.kind == "task_future_dropped" && e.outcome == "UNAVAILABLE_NO_END_RETURNED"));
    assert!(!snapshot.events.iter().any(|e| e.kind == "terminal_end"));
    assert!(hold.load(Ordering::Acquire));
    let mut r = reader(&t, hold, futures_util::stream::pending(), 6);
    settle().await;
    r.task.as_ref().unwrap().abort();
    assert!(r.end().await.is_err());
    settle().await;
    assert!(t
        .reader_diagnostics
        .snapshot()
        .events
        .iter()
        .any(|e| e.kind == "join_cancelled"));
    bundle(
        "actual-task-synthetic-cancellation.json",
        &t.reader_diagnostics.snapshot(),
    );
}
#[tokio::test]
async fn reader_diag_panic_preserves_join_error_without_fabricating_end_status() {
    let t = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(false));
    let stream = futures_util::stream::poll_fn(
        |_| -> std::task::Poll<Option<std::result::Result<SubscribeUpdate, tonic::Status>>> {
            panic!("synthetic-stream-panic")
        },
    );
    let mut r = reader(&t, hold.clone(), stream, 7);
    assert!(r.end().await.is_err());
    settle().await;
    let snapshot = t.reader_diagnostics.snapshot();
    assert!(snapshot
        .events
        .iter()
        .any(|e| e.kind == "task_panicked" && e.error.is_none()));
    assert!(snapshot.events.iter().any(|e| e.kind == "join_panicked"));
    assert!(!snapshot.events.iter().any(|e| e.kind == "terminal_end"));
    assert!(hold.load(Ordering::Acquire));
    bundle("actual-task-synthetic-panic.json", &snapshot);
}
#[derive(Debug)]
struct Cause {
    index: usize,
    next: Option<Box<Cause>>,
}
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.index == 0 {
            write!(f, "TCP reset")
        } else {
            write!(f, "h2 cause {}", self.index)
        }
    }
}
impl Error for Cause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.next.as_deref().map(|e| e as _)
    }
}
fn chain(n: usize) -> Cause {
    Cause {
        index: n,
        next: (n > 0).then(|| Box::new(chain(n - 1))),
    }
}
#[test]
fn reader_diag_safe_error_keeps_lengths_redaction_caps_and_unknown_provenance_explicit() {
    let message = format!("h2 issue fixture-credential-do-not-export https://private.invalid/path\nauthorization: Bearer hidden\n{}", "ordinary words ".repeat(90));
    let status = tonic::Status::internal(message.clone());
    let witness = ErrorWitness::status(&status, &["fixture-credential-do-not-export".into()]);
    let json = serde_json::to_string(&witness).unwrap();
    for secret in [
        "fixture-credential-do-not-export",
        "private.invalid",
        "Bearer hidden",
    ] {
        assert!(!json.contains(secret));
    }
    assert_eq!(witness.message.original_bytes, Some(message.len()));
    assert!(witness.message.redacted);
    assert!(witness.message.retained_chars <= 8192);
    assert!(witness.typed_h2_reason.starts_with("UNAVAILABLE"));
    assert_eq!(witness.causes_total, Some(0));
    let e = anyhow::Error::new(chain(5));
    let bounded = ErrorWitness::error(&e, &[]);
    assert_eq!(bounded.causes_total, Some(5));
    assert_eq!(bounded.causes_retained, 5);
    assert_eq!(bounded.causes_dropped, Some(0));
    assert!(bounded.causes.last().unwrap().text.contains("TCP reset"));
    let e = anyhow::Error::new(chain(20));
    let unknown = ErrorWitness::error(&e, &[]);
    assert_eq!(unknown.causes_total, None);
    assert_eq!(unknown.causes_observed_minimum, 16);
    assert_eq!(unknown.causes_dropped, None);
    let huge = ErrorWitness::status(&tonic::Status::internal("文".repeat(9000)), &[]);
    assert_eq!(huge.message.original_bytes, Some(27000));
    assert_eq!(huge.message.original_chars, None);
    assert!(huge.message.truncated);
    bundle(
        "synthetic-safe-error-and-cause-bounds.json",
        &serde_json::json!({"status":witness,"bounded_causes":bounded,"unknown_total":unknown,"oversized":huge}),
    );
}
#[test]
fn reader_diag_journal_contention_overflow_and_missing_export_are_visible_and_nonblocking() {
    let journal = ReaderDiagnostics::default();
    for _ in 0..40 {
        journal.record(event(None, "terminal_end", "EOF"));
    }
    let snapshot = journal.snapshot();
    assert_eq!(snapshot.attempted, 40);
    assert_eq!(snapshot.events.len(), 32);
    assert_eq!(snapshot.retention_lost, 8);
    assert_eq!(snapshot.export_queue_dropped, 40);
    assert_eq!(snapshot.export_enqueued, 0);
    // Exercise the actual try_lock branch under a held lock, not a timing assumption.
    let locked = journal.events.lock().unwrap();
    journal.record(event(None, "terminal_end", "EOF"));
    assert!(!journal.snapshot().journal_available);
    drop(locked);
    assert_eq!(journal.snapshot().retention_lost, 9);
    bundle(
        "synthetic-journal-overflow-contention-missing-export.json",
        &journal.snapshot(),
    );
}
#[derive(Clone)]
struct PanicExporter;
impl tracing::Subscriber for PanicExporter {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().name().contains("reader_diagnostics") {
            panic!("synthetic-exporter-failure");
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
#[tokio::test]
async fn reader_diag_export_callback_panic_does_not_change_end_or_hold() {
    let dispatch = tracing::Dispatch::new(PanicExporter);
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let t = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(false));
    let mut r = reader(&t, hold.clone(), futures_util::stream::empty(), 8);
    assert!(matches!(r.end().await.unwrap(), End::Eof));
    settle().await;
    let snapshot = t.reader_diagnostics.snapshot();
    assert!(snapshot.export_failed > 0);
    assert_eq!(snapshot.observer_panics, 0);
    assert!(hold.load(Ordering::Acquire));
    assert!(snapshot.events.iter().any(|e| e.outcome == "EOF"));
    bundle("actual-task-synthetic-export-failure.json", &snapshot);
}

#[derive(Debug)]
struct Cyclic;
impl fmt::Display for Cyclic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cyclic h2 cause")
    }
}
impl Error for Cyclic {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self)
    }
}
#[derive(Debug)]
struct Wide {
    next: Option<Box<Wide>>,
}
impl fmt::Display for Wide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..900 {
            write!(f, "\"\\ word ")?;
        }
        Ok(())
    }
}
impl Error for Wide {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.next.as_deref().map(|v| v as _)
    }
}
#[test]
fn reader_diag_safe_error_extended_tail_total_cap_and_cycle_are_explicit() {
    let tail = ErrorWitness::error(&anyhow::Error::new(chain(6)), &[]);
    assert_eq!(tail.causes_total, Some(6));
    assert_eq!(tail.causes.len(), 6);
    assert!(tail.causes[5].text.contains("TCP reset"));
    let cycle = ErrorWitness::error(&anyhow::Error::new(Cyclic), &[]);
    assert_eq!(cycle.causes_total, None);
    assert_eq!(cycle.cause_count_availability, "UNAVAILABLE_CYCLE");
    let mut wide = Wide { next: None };
    for _ in 0..16 {
        wide = Wide {
            next: Some(Box::new(wide)),
        };
    }
    let capped = ErrorWitness::error(&anyhow::Error::new(wide), &[]);
    let bytes = serde_json::to_vec(&capped).unwrap();
    assert!(bytes.len() <= 64 * 1024);
    assert!(capped
        .causes
        .iter()
        .any(|v| v.necessary_detail == "UNAVAILABLE_TOTAL_DIAGNOSTIC_CAP"));
    bundle(
        "extended-safe-error-tail-cap-cycle.json",
        &serde_json::json!({"tail":tail,"cycle":cycle,"capped":capped,"encoded_error_bytes":bytes.len()}),
    );
}
#[tokio::test]
#[ignore = "explicit isolated file-export fixture; process environment scope"]
async fn reader_diag_actual_file_export_closure_and_cap_preserve_reader_end() {
    let root = tempfile::tempdir().unwrap();
    std::env::set_var("COPY49_MEASUREMENT_OUT", root.path());
    std::env::remove_var("COPY49_TERMINAL_FILE_CAP");
    let t = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(false));
    let mut status = tonic::Status::data_loss("SYNTHETIC file boundary status");
    status.set_source(Arc::new(chain(6)));
    let mut r = reader(
        &t,
        hold.clone(),
        futures_util::stream::iter(vec![Err(status)]),
        10,
    );
    assert!(matches!(r.end().await.unwrap(),End::Status(s)if s.code()==tonic::Code::DataLoss));
    for _ in 0..100 {
        if t.reader_diagnostics.snapshot().export_pending == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let snapshot = t.reader_diagnostics.snapshot();
    assert_eq!(snapshot.export_failed, 0);
    assert_eq!(snapshot.export_pending, 0);
    assert_eq!(snapshot.export_completed, 2);
    let saved = std::fs::read_to_string(root.path().join("reader-terminal-events.jsonl")).unwrap();
    let lines = saved
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[1]["sequence"], 2);
    assert_eq!(lines[1]["error"]["code"], "DataLoss");
    assert!(saved.contains("TCP reset"));
    assert!(hold.load(Ordering::Acquire));
    std::env::set_var("COPY49_TERMINAL_FILE_CAP", "1");
    let failed = Arc::new(DurableIngressTelemetry::default());
    let failed_hold = Arc::new(AtomicBool::new(false));
    let mut r = reader(
        &failed,
        failed_hold.clone(),
        futures_util::stream::empty(),
        11,
    );
    assert!(matches!(r.end().await.unwrap(), End::Eof));
    for _ in 0..100 {
        let s = failed.reader_diagnostics.snapshot();
        if s.export_pending == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let refusal = failed.reader_diagnostics.snapshot();
    assert_eq!(refusal.export_failed, 2);
    assert_eq!(refusal.export_completed, 0);
    assert_eq!(refusal.export_pending, 0);
    assert!(failed_hold.load(Ordering::Acquire));
    assert_eq!(
        std::fs::read_to_string(root.path().join("reader-terminal-events.jsonl")).unwrap(),
        saved
    );
    std::env::remove_var("COPY49_TERMINAL_FILE_CAP");
    std::env::remove_var("COPY49_MEASUREMENT_OUT");
    bundle(
        "actual-file-export-closure-and-cap.json",
        &serde_json::json!({"persisted":lines,"normal_snapshot":snapshot,"cap_failure_snapshot":refusal,"trading":false}),
    );
}
