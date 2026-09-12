use super::super::super::{capture_files::write_new, capture_persist::persist_with};
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
    held_writer(false, false).await;
}
#[tokio::test]
async fn pending_manifest_deadline_cannot_publish_complete_late() {
    held_writer(false, true).await;
}
#[tokio::test]
async fn cancelled_owner_cannot_publish_or_start_next_file_after_release() {
    held_writer(true, false).await;
}
async fn held_writer(cancel: bool, manifest: bool) {
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
    let limit = if cancel {
        Duration::from_secs(10)
    } else {
        Duration::from_millis(200)
    };
    let task = tokio::spawn(persist_with(
        files,
        "stream_closed",
        json!({}),
        3,
        limit,
        move |p, b| {
            let _keep = &guard;
            let target = if manifest {
                "manifest.pending.json"
            } else {
                "000001.pb"
            };
            if p.file_name().unwrap() == target {
                if let Some(gate) = gate.take() {
                    entered.send(()).unwrap();
                    gate.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            }
            write_new(p, b)
        },
    ));
    tokio::task::spawn_blocking(move || wait.recv_timeout(Duration::from_secs(2)).unwrap())
        .await
        .unwrap();
    if cancel {
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
    } else {
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
    tokio::task::spawn_blocking(move || finished.recv_timeout(Duration::from_secs(2)).unwrap())
        .await
        .unwrap();
    assert!(!output.join("manifest.json").exists());
    if !manifest {
        assert!(!output.join("000002.pb").exists());
    }
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
