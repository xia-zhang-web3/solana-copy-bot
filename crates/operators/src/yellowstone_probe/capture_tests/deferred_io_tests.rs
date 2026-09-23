use super::super::super::{
    capture_files::write_new,
    capture_persist::{persist_with, TestDeadline},
};
use super::deferred_tests::files;
use super::*;
use std::sync::mpsc;

#[tokio::test]
async fn writer_failure_preserves_partial_file_and_never_green_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    for i in 1..=3 {
        files.record(&transaction(), i).unwrap();
    }
    let output = files.config.output.clone();
    let outcome = persist_with(
        files,
        "stream_closed",
        json!({}),
        4,
        Duration::from_secs(2),
        |p, b| {
            if p.file_name().unwrap() == "000002.pb" {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(p)?;
                f.write_all(&b[..1])?;
                return Err(std::io::Error::other("synthetic partial write"));
            }
            write_new(p, b)
        },
    )
    .await;
    let m = &outcome.capture["manifest"];
    assert_eq!(m["stop_reason"], "output_io_failure");
    assert_eq!(m["complete"], false);
    assert_eq!(m["messages_received"], 3);
    assert_eq!(m["envelopes_written"], 1);
    assert_eq!(m["payload_bytes"], 2);
    assert_eq!(m["messages"][1]["refused"], "output_io_failure");
    assert_eq!(std::fs::read(output.join("000002.pb")).unwrap(), [34]);
    assert!(!output.join("000003.pb").exists());
    assert_eq!(outcome.capture["manifest_persisted"], true);
}

#[tokio::test]
async fn writer_deadline_returns_before_release_and_cannot_publish_late() {
    held_writer(false).await;
}
#[tokio::test]
async fn pending_manifest_deadline_cannot_publish_complete_late() {
    pending_manifest_order(false).await;
}
#[tokio::test]
async fn pending_manifest_early_deadline_control_misses_writer() {
    pending_manifest_order(true).await;
}
#[tokio::test]
async fn cancelled_owner_cannot_publish_or_start_next_file_after_release() {
    held_writer(true).await;
}
async fn held_writer(cancel: bool) {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    for i in 1..=2 {
        files.record(&transaction(), i).unwrap();
    }
    let output = files.config.output.clone();
    let (entered, wait) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let mut gate = Some(gate);
    struct Finished(mpsc::Sender<()>);
    impl Drop for Finished {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let guard = Finished(done);
    let write = move |p: &std::path::Path, b: &[u8]| {
        let _keep = &guard;
        if p.file_name().unwrap() == "000001.pb" {
            if let Some(gate) = gate.take() {
                entered.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        }
        write_new(p, b)
    };
    let (mut task, arm) = if cancel {
        (
            tokio::spawn(persist_with(
                files,
                "stream_closed",
                json!({}),
                3,
                Duration::from_secs(10),
                write,
            )),
            None,
        )
    } else {
        let (deadline, arm) = TestDeadline::pair();
        (
            tokio::spawn(deadline.persist_with(
                files,
                "stream_closed",
                json!({}),
                3,
                Duration::from_millis(200),
                write,
            )),
            Some(arm),
        )
    };
    let entry = tokio::task::spawn_blocking(move || wait.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap();
    if let Err(error) = entry {
        if !task.is_finished() {
            if let Some(arm) = arm {
                arm.arm();
            }
        }
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
        panic!(
            "stage=payload_writer_entry error={error:?} owner_outcome={:?}",
            outcome
                .as_ref()
                .map(|joined| joined.as_ref().map(|value| value.reason))
        );
    }
    if cancel {
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
    } else {
        arm.unwrap().arm();
        let outcome = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.reason, "persistence_deadline");
        assert_eq!(outcome.capture["manifest_persisted"], false);
        assert_eq!(outcome.capture["manifest"]["complete"], false);
        assert_eq!(outcome.capture["manifest"]["durable_counts_known"], false);
        assert!(outcome.capture["manifest"]["payload_bytes"].is_null());
    }
    assert!(!output.join("manifest.json").exists());
    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || finished.recv_timeout(Duration::from_secs(10)).unwrap())
        .await
        .unwrap();
    assert!(!output.join("manifest.json").exists());
    assert!(!output.join("000002.pb").exists());
}

async fn pending_manifest_order(arm_before_manifest: bool) {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    for i in 1..=2 {
        files.record(&transaction(), i).unwrap();
    }
    let output = files.config.output.clone();
    let (setup_entered, setup_wait) = mpsc::channel();
    let (setup_release, setup_gate) = mpsc::channel();
    let (manifest_entered, manifest_wait) = mpsc::channel();
    let (manifest_release, manifest_gate) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let mut setup_gate = Some(setup_gate);
    let mut manifest_gate = Some(manifest_gate);
    struct Finished(mpsc::Sender<()>);
    impl Drop for Finished {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let guard = Finished(done);
    let (deadline, arm) = TestDeadline::pair();
    let mut arm = Some(arm);
    let mut setup_release = Some(setup_release);
    let mut manifest_release = Some(manifest_release);
    let mut manifest_wait = Some(manifest_wait);
    let mut task = tokio::spawn(deadline.persist_with(
        files,
        "stream_closed",
        json!({}),
        3,
        Duration::from_millis(200),
        move |p, b| {
            let _keep = &guard;
            match p.file_name().unwrap().to_str().unwrap() {
                "000001.pb" => {
                    if let Some(gate) = setup_gate.take() {
                        setup_entered.send(()).unwrap();
                        gate.recv_timeout(Duration::from_secs(10)).unwrap();
                    }
                }
                "manifest.pending.json" => {
                    if let Some(gate) = manifest_gate.take() {
                        manifest_entered.send(()).unwrap();
                        gate.recv_timeout(Duration::from_secs(10)).unwrap();
                    }
                }
                _ => {}
            }
            write_new(p, b)
        },
    ));

    let setup =
        tokio::task::spawn_blocking(move || setup_wait.recv_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    if let Err(error) = setup {
        if !task.is_finished() {
            arm.take().unwrap().arm();
        }
        let _ = setup_release.take().unwrap().send(());
        let _ = manifest_release.take().unwrap().send(());
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
        panic!(
            "stage=first_payload_writer_entry error={error:?} owner_outcome={:?}",
            outcome
                .as_ref()
                .map(|joined| joined.as_ref().map(|value| value.reason))
        );
    }

    let outcome = if arm_before_manifest {
        // Old ordering: the deadline expires while the first payload is held.
        assert_eq!(
            manifest_wait.as_ref().unwrap().try_recv(),
            Err(mpsc::TryRecvError::Empty)
        );
        arm.take().unwrap().arm();
        tokio::time::timeout(Duration::from_secs(2), &mut task)
            .await
            .expect("stage=early_owner_deadline")
            .expect("stage=early_owner_join")
    } else {
        // Delay setup beyond the old 200 ms without starting the owner deadline.
        if let Ok(early) = tokio::time::timeout(Duration::from_millis(300), &mut task).await {
            panic!(
                "stage=delayed_setup owner returned before deadline was armed; owner_outcome={:?}",
                early.as_ref().map(|value| value.reason)
            );
        }
        assert_eq!(
            manifest_wait.as_ref().unwrap().try_recv(),
            Err(mpsc::TryRecvError::Empty)
        );
        setup_release.take().unwrap().send(()).unwrap();
        let manifest_entry = manifest_wait.take().unwrap();
        let entry = tokio::task::spawn_blocking(move || {
            manifest_entry.recv_timeout(Duration::from_secs(10))
        })
        .await
        .unwrap();
        if let Err(error) = entry {
            if !task.is_finished() {
                arm.take().unwrap().arm();
            }
            let _ = manifest_release.take().unwrap().send(());
            let early = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
            panic!(
                "stage=pending_manifest_writer_entry error={error:?} owner_outcome={:?}",
                early
                    .as_ref()
                    .map(|joined| joined.as_ref().map(|value| value.reason))
            );
        }
        if task.is_finished() {
            let early = task.await;
            panic!(
                "stage=pending_manifest_entered owner returned before deadline arm; owner_outcome={:?}",
                early.as_ref().map(|value| value.reason)
            );
        }
        arm.take().unwrap().arm();
        tokio::time::timeout(Duration::from_secs(2), &mut task)
            .await
            .expect("stage=manifest_owner_deadline")
            .expect("stage=manifest_owner_join")
    };

    assert_eq!(outcome.reason, "persistence_deadline");
    assert_eq!(outcome.capture["manifest_persisted"], false);
    assert_eq!(outcome.capture["manifest"]["complete"], false);
    assert_eq!(outcome.capture["manifest"]["durable_counts_known"], false);
    assert!(outcome.capture["manifest"]["payload_bytes"].is_null());
    assert!(!output.join("manifest.json").exists());

    if arm_before_manifest {
        setup_release.take().unwrap().send(()).unwrap();
    } else {
        manifest_release.take().unwrap().send(()).unwrap();
    }
    tokio::task::spawn_blocking(move || finished.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .expect("stage=worker_finished_after_release");
    if arm_before_manifest {
        assert_eq!(
            manifest_wait
                .take()
                .unwrap()
                .recv_timeout(Duration::from_secs(1)),
            Err(mpsc::RecvTimeoutError::Disconnected),
            "stage=old_order_pending_manifest_callback_missed; owner_reason={}",
            outcome.reason
        );
    }
    assert!(!output.join("manifest.json").exists());
}

#[tokio::test]
async fn deferred_writer_never_overwrites_payload_or_manifest() {
    for target in ["000001.pb", "manifest.json", "manifest.pending.json"] {
        let temp = tempfile::tempdir().unwrap();
        let mut files = files(&temp);
        files.record(&transaction(), 1).unwrap();
        let output = files.config.output.clone();
        std::fs::write(output.join(target), b"prior evidence").unwrap();
        let result = persist_with(
            files,
            "stream_closed",
            json!({}),
            2,
            Duration::from_secs(2),
            write_new,
        )
        .await;
        assert_ne!(result.reason, "capture_complete");
        assert_eq!(result.capture["manifest"]["complete"], false);
        assert_eq!(
            std::fs::read(output.join(target)).unwrap(),
            b"prior evidence"
        );
    }
}
