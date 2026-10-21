use super::*;

struct Local {
    _tmp: tempfile::TempDir,
    path: PathBuf,
    scope: ReplayScope,
    db: AssociationInbox,
    receiver: DeliveryReceiver,
    bridge: Bridge<'static>,
    reader: Reader,
    capture: Arc<CaptureScope>,
}
impl Local {
    async fn new(clear: bool) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let config = config();
        let scope = crate::replay_scope(&config, &wallets()).unwrap();
        let path = tmp.path().join("negative.db");
        let mut db = database(&path, &scope);
        let (mut receiver, mut bridge, reader, capture) = runtime(&config, &scope).await;
        initial(&mut bridge, &mut receiver, &mut db, &scope, &capture).await;
        capture.frontier(104);
        let error = bridge.update(120, &update(104)).await.unwrap_err();
        let gap = error
            .downcast_ref::<crate::source::durable::recovery::LiveParentGap>()
            .unwrap();
        assert_eq!(
            bridge
                .restart_live_gap(121, gap, std::time::Duration::from_millis(100))
                .await
                .unwrap(),
            99
        );
        drain_sessions(&mut receiver, &mut db).await;
        for slot in 99..=104 {
            bridge.http_block(200 + slot, &block(slot)).await.unwrap();
            persist_next(&mut receiver, &mut db, &scope).await;
        }
        if clear {
            reader.clear_verified();
        }
        Self {
            _tmp: tmp,
            path,
            scope,
            db,
            receiver,
            bridge,
            reader,
            capture,
        }
    }
    async fn committed(&mut self, slot: u64) -> DurableCheckpoint {
        self.capture.frontier(slot);
        self.bridge.update(400 + slot, &update(slot)).await.unwrap();
        let envelope = self.receiver.next().await.unwrap().unwrap();
        self.db
            .persist(&envelope.delivery, &CandidateGeneration::Unknown)
            .unwrap();
        self.db.replay_checkpoint(&self.scope).unwrap().unwrap()
    }
    async fn ack(&mut self, slot: u64) -> DurableCheckpoint {
        let head = self.committed(slot).await;
        self.receiver.acknowledge_checkpoint(head.clone()).unwrap();
        head
    }
    fn rejected(&self) {
        let (current, completed) = self.receiver.telemetry.continuity.ack_snapshot();
        assert!(completed.is_none());
        assert!(current.unwrap().diagnostic_invalid);
    }
    fn cursor(&self) -> RecoveryCursor {
        self.receiver.recovery.clone().unwrap()
    }
}

#[tokio::test]
async fn ack_observation_interrupted_flag_before_business_hold_store_cannot_certify_ack_pair() {
    let _clock = LocalClock::enter();
    let mut local = Local::new(true).await;
    let first = local.committed(105).await;
    let second = local.committed(106).await;
    let report = local.receiver.telemetry.clone();
    let flag = local.cursor().http_hold.unwrap();
    let capture = local.capture.clone();
    let receiver = local.receiver;
    let before_store = flag.clone();
    let _hook =
        crate::source::durable::clock_qualification_tests::ack_hook::Hook::interrupted(move || {
            assert!(
                !before_store.load(Ordering::Acquire),
                "paused before original business hold store"
            );
            receiver.acknowledge_checkpoint(first.clone()).unwrap();
            receiver.acknowledge_checkpoint(second.clone()).unwrap();
        });
    capture.interrupted();
    assert!(
        flag.load(Ordering::Acquire),
        "original fail-closed store still executes"
    );
    let (current, completed) = report.continuity.ack_snapshot();
    assert!(completed.is_none());
    assert!(current.unwrap().diagnostic_invalid);
}

#[tokio::test]
async fn ack_observation_duplicate_does_not_replace_second_distinct_and_conflict_refuses() {
    let _clock = LocalClock::enter();
    let mut local = Local::new(true).await;
    let first = local.ack(105).await;
    local
        .receiver
        .acknowledge_checkpoint(first.clone())
        .unwrap();
    assert_eq!(
        local
            .receiver
            .telemetry
            .continuity
            .ack_snapshot()
            .0
            .unwrap()
            .samples
            .len(),
        1
    );
    local.ack(106).await;
    assert!(
        local
            .receiver
            .telemetry
            .continuity
            .ack_snapshot()
            .1
            .unwrap()
            .completed
    );
    let mut conflict = Local::new(true).await;
    let mut first = conflict.ack(105).await;
    first.block.observation.child.hash = bs58::encode([199; 32]).into_string();
    assert!(conflict.receiver.acknowledge_checkpoint(first).is_err());
    conflict.ack(106).await;
    conflict.rejected();
}

#[tokio::test]
async fn ack_observation_cursor_visible_before_continuity_clear_is_invalid() {
    let _clock = LocalClock::enter();
    let mut local = Local::new(false).await;
    let cursor = local.cursor();
    let scope = local.capture.clone();
    let report = local.receiver.telemetry.clone();
    let _hook = crate::source::durable::clock_qualification_tests::ack_hook::Hook::set(move || {
        assert_eq!(cursor.committed_slot().unwrap(), Some(105));
        assert_eq!(
            report
                .continuity
                .snapshot()
                .context
                .checkpoint
                .unwrap()
                .slot,
            104
        );
        scope.clear_verified();
    });
    local.ack(105).await;
    assert!(
        !local.cursor().http_hold.unwrap().load(Ordering::Acquire),
        "business clear remains unchanged"
    );
    local.ack(106).await;
    local.rejected();
}

#[tokio::test]
async fn ack_observation_tail_writer_and_hidden_assertions_cannot_complete_pair() {
    let _clock = LocalClock::enter();
    for mode in ["tail", "true-false", "true-true-false"] {
        let mut local = Local::new(true).await;
        local.ack(105).await;
        let cursor = local.cursor();
        if mode == "tail" {
            let _hook = crate::source::durable::clock_qualification_tests::ack_hook::Hook::tail(
                move || {
                    cursor.set_http_hold(true);
                    cursor.set_http_hold(false);
                },
            );
            local.ack(106).await;
        } else {
            cursor.set_http_hold(true);
            if mode == "true-true-false" {
                cursor.set_http_hold(true);
            }
            cursor.set_http_hold(false);
            local.ack(106).await;
        }
        local.rejected();
    }
}

#[tokio::test]
async fn ack_observation_shared_clone_writer_paused_between_epoch_and_flag_is_invalid() {
    let _clock = LocalClock::enter();
    let mut local = Local::new(true).await;
    let tracker = local.receiver.telemetry.continuity.hold_tracker();
    let cursor = local.cursor();
    assert!(Arc::ptr_eq(cursor.observation.as_ref().unwrap(), &tracker));
    let flag = cursor.http_hold.clone().unwrap();
    let ready = Arc::new(std::sync::Barrier::new(2));
    let release = ready.clone();
    let thread = std::thread::spawn(move || {
        tracker.test_paused_assertion(&flag, || {
            ready.wait();
            ready.wait();
        });
    });
    release.wait();
    local.ack(105).await;
    release.wait();
    thread.join().unwrap();
    local.ack(106).await;
    local.rejected();
}

#[tokio::test]
async fn ack_observation_capture_guard_drop_reassert_stop_and_context_changes_refuse() {
    let _clock = LocalClock::enter();
    for mode in [
        "scope",
        "task",
        "reader",
        "reassert",
        "stop",
        "reset",
        "reader-context",
        "association-context",
    ] {
        let mut local = Local::new(true).await;
        let mut late_head = None;
        match mode {
            "scope" => local.capture.interrupted(),
            "task" => {
                tokio::task::yield_now().await;
                let task = local.reader.task.take().unwrap();
                task.abort();
                let _ = task.await;
            }
            "reader" => local.capture.reader_dropped(),
            "reassert" => {
                local.cursor().set_http_hold(true);
                let before = local
                    .receiver
                    .telemetry
                    .continuity
                    .hold_tracker()
                    .sample(local.cursor().http_hold.as_ref())
                    .epoch;
                let capture = local.capture.clone();
                let _hook =
                    crate::source::durable::clock_qualification_tests::ack_hook::Hook::clear(
                        move || capture.interrupted(),
                    );
                local.reader.clear_verified();
                let after = local
                    .receiver
                    .telemetry
                    .continuity
                    .hold_tracker()
                    .sample(local.cursor().http_hold.as_ref());
                assert!(after.hold.unwrap());
                assert_eq!(
                    after.epoch,
                    before + 2,
                    "interruption and synchronous restore are both assertions"
                );
            }
            "stop" => {
                late_head = Some(local.committed(105).await);
                let before = local
                    .receiver
                    .telemetry
                    .continuity
                    .hold_tracker()
                    .sample(local.cursor().http_hold.as_ref())
                    .epoch;
                local.receiver.stop();
                local.receiver.stop();
                let after = local
                    .receiver
                    .telemetry
                    .continuity
                    .hold_tracker()
                    .sample(local.cursor().http_hold.as_ref());
                assert_eq!(after.epoch, before + 2);
            }
            "reset" => {
                local.bridge.reset(350).await.unwrap();
                drain_sessions(&mut local.receiver, &mut local.db).await;
            }
            "reader-context" => {
                local
                    .receiver
                    .telemetry
                    .continuity
                    .reader("foreign-reader:9".into(), 9);
            }
            "association-context" => local
                .receiver
                .telemetry
                .continuity
                .association_session("foreign-association:9".into()),
            _ => unreachable!(),
        }
        if mode == "stop" {
            local
                .receiver
                .acknowledge_checkpoint(late_head.unwrap())
                .unwrap();
        } else {
            local.ack(105).await;
        }
        local.rejected();
    }
}

#[tokio::test]
async fn ack_observation_overflow_missing_tracker_and_new_clear_do_not_substitute_samples() {
    let _clock = LocalClock::enter();
    for mode in ["overflow", "missing", "new-clear"] {
        let mut local = Local::new(true).await;
        local.ack(105).await;
        if mode == "overflow" {
            local
                .receiver
                .telemetry
                .continuity
                .hold_tracker()
                .test_overflow();
        } else if mode == "missing" {
            local.receiver.recovery.as_mut().unwrap().observation = None;
        } else {
            let prior = local
                .receiver
                .telemetry
                .continuity
                .ack_snapshot()
                .0
                .unwrap()
                .eligible_clear
                .record_sequence;
            local.cursor().set_http_hold(true);
            local.reader.clear_verified();
            let current = local
                .receiver
                .telemetry
                .continuity
                .ack_snapshot()
                .0
                .unwrap();
            assert!(current.eligible_clear.record_sequence > prior);
            assert!(current.samples.is_empty());
        }
        local.ack(106).await;
        if mode == "new-clear" {
            assert!(local
                .receiver
                .telemetry
                .continuity
                .ack_snapshot()
                .1
                .is_none());
            assert_eq!(
                local
                    .receiver
                    .telemetry
                    .continuity
                    .ack_snapshot()
                    .0
                    .unwrap()
                    .samples
                    .len(),
                1
            );
        } else {
            local.rejected();
        }
    }
}

#[tokio::test]
async fn ack_observation_sqlite_commit_or_readback_failure_never_records_ack() {
    let _clock = LocalClock::enter();
    for mode in ["commit", "readback"] {
        let mut local = Local::new(true).await;
        let sql = rusqlite::Connection::open(&local.path).unwrap();
        if mode == "commit" {
            sql.execute_batch("CREATE TRIGGER local_ack_fail BEFORE UPDATE ON association_replay_cursor BEGIN SELECT RAISE(ABORT,'LOCAL injected commit failure'); END;").unwrap();
        }
        local.capture.frontier(105);
        local.bridge.update(505, &update(105)).await.unwrap();
        let envelope = local.receiver.next().await.unwrap().unwrap();
        let persisted = local
            .db
            .persist(&envelope.delivery, &CandidateGeneration::Unknown);
        if mode == "commit" {
            assert!(persisted.is_err());
        } else {
            persisted.unwrap();
            sql.execute("UPDATE association_replay_cursor SET head='{}'", [])
                .unwrap();
            assert!(local.db.replay_checkpoint(&local.scope).is_err());
        }
        // Mirrors the unchanged consumer contract: no successful readback, no ACK call.
        assert_eq!(local.cursor().committed_slot().unwrap(), Some(104));
        let (current, completed) = local.receiver.telemetry.continuity.ack_snapshot();
        assert!(current.unwrap().samples.is_empty());
        assert!(completed.is_none());
    }
}

#[test]
fn ack_observation_hold_epoch_counts_repeated_assertions_and_inflight_pause() {
    let tracker = crate::source::durable::hold_observation::HoldTracker::default();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    tracker.write(&flag, true, Ordering::Release);
    tracker.write(&flag, true, Ordering::Release);
    assert_eq!(tracker.sample(Some(&flag)).epoch, 2);
    tracker.test_paused_assertion(&flag, || {
        let stamp = tracker.sample(Some(&flag));
        assert!(!stamp.coherent && stamp.inflight > 0);
    });
    assert_eq!(tracker.sample(Some(&flag)).epoch, 3);
    tracker.test_overflow();
    tracker.write(&flag, false, Ordering::SeqCst);
    assert!(
        !flag.load(Ordering::Acquire),
        "diagnostic overflow never suppresses business clear"
    );
    assert!(!tracker.sample(Some(&flag)).coherent);
}

#[tokio::test]
async fn ack_observation_export_panic_cannot_change_successful_sqlite_ack() {
    let _clock = LocalClock::enter();
    let mut local = Local::new(true).await;
    let trace = trace::Trace::default();
    trace.1.store(true, Ordering::Release);
    let dispatch = tracing::Dispatch::new(trace.clone());
    let _trace = tracing::dispatcher::set_default(&dispatch);
    let flag = local.cursor().http_hold.unwrap();
    assert!(!flag.load(Ordering::Acquire));
    let head = local.committed(105).await;
    assert_eq!(
        local.db.replay_checkpoint(&local.scope).unwrap(),
        Some(head.clone())
    );
    // This ACK follows a real SQLite commit/readback. Only its diagnostic
    // subscriber panics; cursor/consumer errors are never caught or replaced.
    assert!(local.receiver.acknowledge_checkpoint(head).is_ok());
    assert_eq!(local.cursor().committed_slot().unwrap(), Some(105));
    assert!(!flag.load(Ordering::Acquire));
    assert!(trace.0.lock().unwrap().is_empty());
    let (current, completed) = local.receiver.telemetry.continuity.ack_snapshot();
    assert_eq!(current.unwrap().samples.len(), 1);
    assert!(
        completed.is_none(),
        "a missing first log is no completed proof"
    );

    // A later bounded complete event can export both real distinct samples.
    trace.1.store(false, Ordering::Release);
    let head = local.committed(106).await;
    assert!(local.receiver.acknowledge_checkpoint(head).is_ok());
    assert_eq!(local.cursor().committed_slot().unwrap(), Some(106));
    assert!(!flag.load(Ordering::Acquire));
    let proof = local
        .receiver
        .telemetry
        .continuity
        .ack_snapshot()
        .1
        .unwrap();
    let events = trace.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&events[0]).unwrap(),
        json!(proof)
    );
    assert!(proof.completed && !proof.diagnostic_invalid);
    assert_eq!(proof.samples[0].commit_identity.slot, 105);
    assert_eq!(proof.samples[1].commit_identity.slot, 106);
}
