//! Receive retains bounded encoded envelopes; this phase owns all SHA/durable I/O.
use super::capture_files::{write_new, CaptureFiles};
use super::capture_hash::sha256;
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub(super) const PERSIST_LIMIT: Duration = Duration::from_secs(60);
const PENDING_MANIFEST: &str = "manifest.pending.json";

pub(super) struct Outcome {
    pub reason: &'static str,
    pub capture: Value,
}
struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub(super) async fn persist(
    files: CaptureFiles,
    reason: &'static str,
    request: Value,
    receive_ns: u64,
) -> Outcome {
    persist_with(files, reason, request, receive_ns, PERSIST_LIMIT, write_new).await
}

// Injectable I/O stays on the same persistence path in dedicated boundary tests.
pub(super) async fn persist_with<W>(
    mut files: CaptureFiles,
    reason: &'static str,
    request: Value,
    receive_ns: u64,
    limit: Duration,
    mut write: W,
) -> Outcome
where
    W: FnMut(&Path, &[u8]) -> std::io::Result<()> + Send + 'static,
{
    let started = Instant::now();
    let deadline = started + limit;
    let output = files.config.output.clone();
    let buffered_bytes = files.buffered_bytes;
    let buffered_count = files.pending.len();
    let mut fallback = files.manifest("persistence_deadline", request.clone(), receive_ns);
    // If waiting is interrupted, disk progress is unknown, never a saved prefix.
    fallback["envelopes_written"] = Value::Null;
    fallback["payload_bytes"] = Value::Null;
    fallback["durable_counts_known"] = json!(false);
    let cancel = Cancel(Arc::new(AtomicBool::new(false)));
    let stopped = cancel.0.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let worker = std::thread::Builder::new()
        .name("capture-persist".into())
        .spawn(move || {
            let check = || {
                if stopped.load(Ordering::SeqCst) {
                    Err("persistence_cancelled")
                } else if Instant::now() >= deadline {
                    Err("persistence_deadline")
                } else {
                    Ok(())
                }
            };
            let mut stop = reason;
            let pending = std::mem::take(&mut files.pending);
            for (index, bytes) in pending {
                let row = &mut files.rows[index];
                let result = (|| {
                    check()?;
                    let digest = sha256(&bytes);
                    check()?;
                    let name = format!("{:06}.pb", index + 1);
                    row["file"] = json!(name);
                    row["sha256"] = json!(digest);
                    write(&files.config.output.join(name), &bytes)
                        .map_err(|_| "output_io_failure")?;
                    row["saved"] = json!(true);
                    files.payload_bytes += bytes.len() as u64;
                    files.written += 1;
                    check()
                })();
                if let Err(error) = result {
                    row["refused"] = json!(error);
                    stop = error;
                    break;
                }
            }
            let mut manifest = files.manifest(stop, request, receive_ns);
            annotate(
                &mut manifest,
                reason,
                buffered_count,
                buffered_bytes,
                started,
                limit,
            );
            // This worker NEVER publishes manifest.json. A timed-out/cancelled worker
            // may finish an admitted file operation, but cannot publish readable evidence.
            let persisted = check().is_ok()
                && (|| {
                    let raw = serde_json::to_vec(&manifest)?;
                    if raw.len() as u64 > files.config.metadata_reserve() {
                        return Err(std::io::Error::other("metadata bound"));
                    }
                    write(&files.config.output.join(PENDING_MANIFEST), &raw)
                })()
                .is_ok();
            let _ = sender.send((manifest, stop, persisted));
        });
    let result = if worker.is_err() {
        None
    } else {
        // Detached std thread, not spawn_blocking: runtime shutdown never joins
        // an uninterruptible filesystem syscall. Drop cancels subsequent work.
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receiver)
            .await
            .ok()
            .and_then(Result::ok)
    };
    let (mut manifest, mut stop, ready) = match result {
        Some(result) if Instant::now() < deadline => result,
        _ => {
            let stop = if worker.is_err() {
                "persistence_worker_start_failed"
            } else if Instant::now() >= deadline {
                "persistence_deadline"
            } else {
                "persistence_worker_failed"
            };
            fallback["stop_reason"] = json!(stop);
            annotate(
                &mut fallback,
                reason,
                buffered_count,
                buffered_bytes,
                started,
                limit,
            );
            (fallback, stop, false)
        }
    };
    // Only the awaiting owner can publish, after all durable writes completed
    // within the persistence deadline. create_new semantics via hard_link.
    // These two local namespace syscalls cannot be preempted by a Rust timeout.
    let published = ready && publish(&output).is_ok();
    if ready && !published || (!ready && !stop.starts_with("persistence_")) {
        manifest["prior_stop_reason"] = json!(stop);
        stop = "manifest_io_failure";
        manifest["stop_reason"] = json!(stop);
        manifest["complete"] = json!(false);
    }
    let report_reason = if stop == "manifest_io_failure" {
        "capture_manifest_io_failure"
    } else if published
        && stop == "stream_closed"
        && manifest["envelopes_written"].as_u64().unwrap_or(0) > 0
    {
        "capture_complete"
    } else {
        stop
    };
    Outcome {
        reason: report_reason,
        capture: json!({
            "manifest_persisted":published,"manifest":manifest,
            "persistence_wall_elapsed_ns":ns(started),
            "persistence_deadline_scope":"hash, payload and pending-manifest durable writes; final local namespace publication follows",
        }),
    }
}

fn annotate(
    m: &mut Value,
    receive_reason: &str,
    count: usize,
    bytes: u64,
    start: Instant,
    limit: Duration,
) {
    m["capture_pipeline"] = json!("buffer-then-persist-v1");
    m["receive_stop_reason"] = json!(receive_reason);
    m["receive_elapsed_ns"] = m["elapsed_ns"].clone();
    m["persistence_elapsed_ns"] = json!(ns(start));
    m["persistence_limit_ms"] = json!(limit.as_millis() as u64);
    m["buffered_envelopes"] = json!(count);
    m["buffered_encoded_bytes"] = json!(bytes);
    m["buffer_limits"] = json!({
        "count":m["limits"]["messages"],
        "encoded_bytes":m["limits"]["total_bytes"].as_u64().unwrap() - m["metadata_reserve_bytes"].as_u64().unwrap(),
        "metadata_rows":m["limits"]["messages"],
        "in_flight_decoded_envelopes":1,
        "in_flight_encoding_bytes":m["limits"]["message_bytes"],
        "scope":"encoded capacity plus bounded metadata; decoded protobuf, allocator and transport overhead are not measured RSS",
    });
}
fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}
fn publish(output: &Path) -> std::io::Result<()> {
    std::fs::hard_link(output.join(PENDING_MANIFEST), output.join("manifest.json"))?;
    // If removal fails the undeclared pending file makes the reader refuse.
    std::fs::remove_file(output.join(PENDING_MANIFEST))
}
