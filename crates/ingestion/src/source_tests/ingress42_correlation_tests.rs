//! New correlation fields only; no repeat of decoder/collector/financial suites.
use super::*;
use crate::source::durable::continuity_telemetry::{
    CheckpointWitness, ContinuityTelemetry, PreviousRelation, TransitionKind,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

type Source = mpsc::UnboundedSender<std::result::Result<SubscribeUpdate, tonic::Status>>;
fn start(
    telemetry: &Arc<DurableIngressTelemetry>,
    hold: Arc<AtomicBool>,
    generation: u64,
) -> (Source, Reader) {
    let scope = CaptureScope::new(&[], std::iter::empty(), Default::default(), Some(hold))
        .observed(
            telemetry
                .continuity
                .reader(format!("diagnostic42:{generation}"), generation),
        );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let stream = futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx));
    let reader = Reader::start(
        stream,
        futures_util::sink::drain(),
        2,
        1 << 20,
        1 << 20,
        telemetry.clone(),
        scope,
    )
    .unwrap();
    (tx, reader)
}
async fn block_and_ack_metadata(
    source: &Source,
    reader: &mut Reader,
    telemetry: &DurableIngressTelemetry,
    generation: u64,
) {
    source
        .send(Ok(SubscribeUpdate {
            update_oneof: Some(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
                slot: 501,
                ..Default::default()
            })),
            ..Default::default()
        }))
        .unwrap();
    let mut block = reader.next().await.unwrap();
    block.dequeue();
    // Synthetic metadata for testing observation correlation only. The accepted
    // app/SQLite ACK proof is reused; this is not a new durability proof.
    telemetry.continuity.acknowledged(CheckpointWitness {
        session: format!("diagnostic42:{generation}"),
        sequence: 17,
        slot: 501,
        observed_ns: 0,
        reader_id: None,
    });
}
#[tokio::test]
async fn ingress42_actual_reader_ends_before_clear_records_same_generation_refusal() {
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(true));
    let (source, mut reader) = start(&telemetry, hold.clone(), 4);
    block_and_ack_metadata(&source, &mut reader, &telemetry, 4).await;
    drop(source);
    assert!(matches!(reader.end().await.unwrap(), End::Eof));
    reader.clear_verified();
    let state = telemetry.continuity.snapshot();
    let ended = state.last_task_guard_drop.unwrap();
    let clear = state.last_clear.unwrap();
    assert_eq!(ended.reader, clear.reader);
    assert_eq!(clear.reader.generation, 4);
    assert!(ended.ended_ns <= clear.began_ns);
    assert_eq!(clear.previous_relation, PreviousRelation::After);
    assert_eq!(
        (clear.hold_before, clear.hold_after),
        (Some(true), Some(true))
    );
    assert!(clear.interrupted_before && clear.interrupted_after);
    let ack = clear.context_before.checkpoint.unwrap();
    let frontier = clear.context_before.live_frontier.unwrap();
    assert_eq!(ack.reader_id, Some(clear.reader.reader_id));
    assert_eq!(ack.session, clear.reader.session);
    assert_eq!((ack.slot, ack.sequence), (501, 17));
    assert_eq!(frontier.reader, clear.reader);
    assert_eq!(frontier.slot, 501);
    assert!(ack.observed_ns <= clear.began_ns && frontier.observed_ns <= clear.began_ns);
    assert!(clear.ended_ns <= clear.recorded_ns);
    assert!(hold.load(Ordering::Acquire));
}
#[tokio::test]
async fn ingress42_healthy_clear_then_actual_reader_end_records_reclosed_hold() {
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(true));
    let (source, mut reader) = start(&telemetry, hold.clone(), 5);
    block_and_ack_metadata(&source, &mut reader, &telemetry, 5).await;
    reader.clear_verified();
    let clear = telemetry.continuity.snapshot().last_clear.unwrap();
    assert_eq!(
        (clear.hold_before, clear.hold_after),
        (Some(true), Some(false))
    );
    assert!(!clear.interrupted_after && !hold.load(Ordering::Acquire));
    drop(source);
    assert!(matches!(reader.end().await.unwrap(), End::Eof));
    let ended = telemetry
        .continuity
        .snapshot()
        .last_task_guard_drop
        .unwrap();
    assert_eq!(clear.reader, ended.reader);
    assert!(clear.ended_ns <= ended.began_ns);
    assert_eq!(
        (ended.hold_before, ended.hold_after),
        (Some(false), Some(true))
    );
    assert!(ended.interrupted_after && hold.load(Ordering::Acquire));
}
#[tokio::test]
async fn ingress42_old_task_drop_after_new_clear_is_retained_with_both_generations() {
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let hold = Arc::new(AtomicBool::new(true));
    let (old_source, mut old) = start(&telemetry, hold.clone(), 6);
    block_and_ack_metadata(&old_source, &mut old, &telemetry, 6).await;
    let (_current_source, current) = start(&telemetry, hold.clone(), 7);
    current.clear_verified();
    assert!(!hold.load(Ordering::Acquire));
    drop(old_source);
    assert!(matches!(old.end().await.unwrap(), End::Eof));
    let snapshot = telemetry.continuity.snapshot();
    let ended = snapshot.last_task_guard_drop.unwrap();
    let clear = snapshot.last_clear.unwrap();
    assert_eq!(ended.reader.generation, 6);
    assert_eq!(clear.reader.generation, 7);
    assert_eq!(
        ended
            .context_at_record
            .active_reader
            .as_ref()
            .unwrap()
            .generation,
        7
    );
    assert_ne!(ended.reader.reader_id, clear.reader.reader_id);
    assert!(hold.load(Ordering::Acquire));
    drop(current); // Synchronous close is a separate witness from task guard Drop.
    let dropped = telemetry.continuity.snapshot().last_reader_drop.unwrap();
    assert_eq!(dropped.reader.generation, 7);
    assert_eq!(dropped.kind, TransitionKind::ReaderDrop);
    assert!(hold.load(Ordering::Acquire));
}
#[test]
fn ingress42_overlapping_and_late_emission_brackets_are_not_false_ordering() {
    let telemetry = Arc::new(ContinuityTelemetry::default());
    let observation = telemetry.reader("synthetic-brackets:1".into(), 1);
    // Synthetic timestamp-only cases exercise the new classification, not guards.
    let first = observation.begin(10, Some(false), false);
    observation.finish(TransitionKind::VerifiedClear, first, 20, Some(false), false);
    let overlap = observation.begin(15, Some(false), false);
    observation.finish(
        TransitionKind::TaskGuardDrop,
        overlap,
        25,
        Some(false),
        false,
    );
    assert_eq!(
        telemetry
            .snapshot()
            .last_task_guard_drop
            .unwrap()
            .previous_relation,
        PreviousRelation::Overlap
    );
    let delayed = observation.begin(1, Some(false), false);
    observation.finish(
        TransitionKind::VerifiedClear,
        delayed,
        5,
        Some(false),
        false,
    );
    let snapshot = telemetry.snapshot();
    assert_eq!(
        snapshot.last_clear.unwrap().previous_relation,
        PreviousRelation::Before
    );
    assert_eq!(snapshot.transition_count, 3);
    assert_eq!(snapshot.overwritten_observations, 1);
}

#[test]
fn ingress42_wire_late_ack_cannot_be_attributed_to_current_reader_generation() {
    let telemetry = Arc::new(ContinuityTelemetry::default());
    let _old = telemetry.reader("same-base:6".into(), 6);
    let _current = telemetry.reader("same-base:7".into(), 7);
    let ack = |session: &str| CheckpointWitness {
        session: session.into(),
        sequence: 19,
        slot: 501,
        observed_ns: 0,
        reader_id: None,
    };
    telemetry.acknowledged(ack("same-base:6"));
    let old = telemetry.snapshot().context.checkpoint.unwrap();
    assert_eq!(old.session, "same-base:6");
    assert_eq!(old.reader_id, None);
    telemetry.acknowledged(ack("same-base:7"));
    let current = telemetry.snapshot();
    assert_eq!(
        current.context.checkpoint.as_ref().unwrap().reader_id,
        current.context.active_reader.as_ref().map(|r| r.reader_id)
    );
    assert_eq!(current.context.active_reader.unwrap().generation, 7);
}

#[tokio::test]
async fn ingress42_wire_json_preserves_actual_source_brackets_and_identity() {
    for interrupted_first in [true, false] {
        let telemetry = Arc::new(DurableIngressTelemetry::default());
        let hold = Arc::new(AtomicBool::new(true));
        let (source, mut reader) = start(&telemetry, hold.clone(), 9);
        block_and_ack_metadata(&source, &mut reader, &telemetry, 9).await;
        if interrupted_first {
            drop(source);
            assert!(matches!(reader.end().await.unwrap(), End::Eof));
            reader.clear_verified();
        } else {
            reader.clear_verified();
            drop(source);
            assert!(matches!(reader.end().await.unwrap(), End::Eof));
        }
        let snapshot = telemetry.continuity.snapshot();
        let encoded = serde_json::to_string(&snapshot).unwrap();
        let wire: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(wire["last_task_guard_drop"]["kind"], "task_guard_drop");
        assert_eq!(wire["last_clear"]["kind"], "verified_clear");
        assert_eq!(
            wire["last_task_guard_drop"]["reader"]["reader_id"],
            wire["last_clear"]["reader"]["reader_id"]
        );
        assert_eq!(
            wire["last_clear"]["context_before"]["checkpoint"]["reader_id"],
            wire["last_clear"]["reader"]["reader_id"]
        );
        assert_eq!(wire["last_clear"]["hold_after"], interrupted_first);
        let ns = |kind: &str, field: &str| wire[kind][field].as_u64().unwrap();
        if interrupted_first {
            assert!(ns("last_task_guard_drop", "ended_ns") <= ns("last_clear", "began_ns"));
        } else {
            assert!(ns("last_clear", "ended_ns") <= ns("last_task_guard_drop", "began_ns"));
        }
        if let Ok(directory) = std::env::var("INGRESS42_WIRE_DIR") {
            let path = std::path::PathBuf::from(directory);
            std::fs::create_dir_all(&path).unwrap();
            let name = if interrupted_first {
                "actual-reader-before-clear.json"
            } else {
                "actual-clear-before-reader.json"
            };
            std::fs::write(path.join(name), serde_json::to_vec_pretty(&wire).unwrap()).unwrap();
        }
    }
}
