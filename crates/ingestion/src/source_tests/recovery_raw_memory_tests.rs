//! Linux-only simultaneous changed raw window + existing live capture boundary.
//! Oversized synthetic blockhash is wire-pressure input, never semantic evidence.
use super::*;
use crate::source::http_recovery::{
    blocking_normalization::BlockingNormalization, ordered_pipeline::OrderedPipeline,
    ConfirmedHttpRecovery, RawRecoveredBlock,
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn linux_sample(label: &str) {
    let cgroup = |name: &str| std::fs::read_to_string(format!("/sys/fs/cgroup/{name}")).unwrap();
    let rss = std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|s| s.starts_with("VmHWM:"))
        .unwrap()
        .to_string();
    eprintln!(
        "RAW_MEMORY label={label} {} peak={} current={} max={} swap={}",
        rss,
        cgroup("memory.peak").trim(),
        cgroup("memory.current").trim(),
        cgroup("memory.max").trim(),
        cgroup("memory.swap.current").trim()
    );
    assert_eq!(cgroup("memory.max").trim(), (3_u64 << 30).to_string());
    assert_eq!(cgroup("memory.swap.current").trim(), "0");
    assert!(cgroup("memory.peak").trim().parse::<u64>().unwrap() < 3_u64 << 30);
}

#[tokio::test]
#[ignore = "explicit Linux cgroup3GiB pressure; no provider or operational app"]
async fn linux_raw32_and_live1664mib_keep_actual_capture_bounds() {
    assert_eq!(std::env::consts::OS, "linux");
    let full = 16 << 20;
    let normalization = BlockingNormalization::default();
    let (owner, window) = normalization.begin(32).await.unwrap();
    let done = Arc::new(AtomicUsize::new(0));
    let fetched = done.clone();
    let mut pipeline = OrderedPipeline::start_in_window((0..64).collect(), 10, window, move |_| {
        let done = fetched.clone();
        async move {
            // Same bounded exact capacity as the changed HTTP attempt buffer.
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(full).unwrap();
            assert_eq!(bytes.capacity(), full);
            bytes.resize(full, b' '); // touch every page, not a virtual allocation claim.
            done.fetch_add(1, Ordering::SeqCst);
            Ok(bytes)
        }
    })
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while done.load(Ordering::SeqCst) < 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(done.load(Ordering::SeqCst), 32);
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let live_cap = 1664 << 20;
    let wire_payload = live_cap / 384 - 512 - 64;
    let stream = futures_util::stream::iter(0..385_u64).map(move |slot| {
        Ok(SubscribeUpdate {
            update_oneof: Some(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
                slot,
                blockhash: "x".repeat(wire_payload),
                ..Default::default()
            })),
            ..Default::default()
        })
    });
    let mut reader = Reader::start(
        stream,
        futures_util::sink::drain(),
        384,
        live_cap,
        full,
        telemetry.clone(),
        CaptureScope::new(&[], std::iter::empty(), Default::default(), None),
    )
    .unwrap();
    assert!(matches!(
        reader.end().await.unwrap(),
        // Bytes are acquired before count: the385th full-sized update crosses
        // the unchanged1664MiB byte ceiling before attempting its count permit.
        End::Budget("LiveCaptureBytes")
    ));
    let captured = telemetry.snapshot().processing;
    assert_eq!(captured.input_queue_count_max, 384);
    assert!(captured.input_queue_bytes_max <= live_cap as u64);
    assert!(captured.input_queue_bytes_max > 1663_u64 << 20);
    linux_sample("raw512MiB_plus_live1664MiB");

    // Replace ONE charged raw buffer with the verified real largest response;
    // the owning blocking job, result and application share that same permit.
    // The other31 raw buffers and actual1664MiB Reader stay resident throughout.
    let dir = std::env::var("COPYBOT_RECOVERY_06_HTTP_EVIDENCE_DIR").unwrap();
    let profile: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/recovery_06_profile_659.json"
    ))
    .unwrap();
    let row = profile["records"]
        .as_array()
        .unwrap()
        .iter()
        .max_by_key(|r| r["body_bytes"].as_u64().unwrap())
        .unwrap();
    let bytes =
        std::fs::read(std::path::Path::new(&dir).join(row["body_file"].as_str().unwrap())).unwrap();
    use sha2::{Digest, Sha256};
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        row["body_sha256"].as_str().unwrap()
    );
    let slot = row["slot"].as_u64().unwrap();
    let id = row["rpc_id"].as_u64().unwrap();
    let raw = pipeline.next().await.unwrap().unwrap().map(|space| {
        drop(space);
        let mut body = Vec::new();
        body.try_reserve_exact(full).unwrap();
        assert_eq!(body.capacity(), full);
        body.extend_from_slice(&bytes);
        RawRecoveredBlock::from_response(slot, 200, id, body)
    });
    let client = ConfirmedHttpRecovery::new(
        "http://127.0.0.1:1",
        None,
        1024,
        full,
        std::time::Duration::from_secs(1),
    )
    .unwrap();
    let (transformed, owner) = owner
        .run(raw, move |raw| client.normalize_raw_block(raw))
        .await
        .unwrap();
    let (transformed, permit) = transformed.into_parts();
    eprintln!(
        "RAW_MEMORY_NORMALIZATION execution_us={} waiting_us={} charged_active=1 charged_queued=31",
        transformed.execution.as_micros(),
        transformed.waiting.as_micros()
    );
    let recovered = transformed.result.unwrap();
    assert_eq!(
        recovered.block.transactions.len() as u64,
        row["transaction_count"].as_u64().unwrap()
    );
    assert_eq!(done.load(Ordering::SeqCst), 32);
    assert_eq!(recovered.raw_response.capacity(), full);
    linux_sample("plus_actual_owning_blocking_normalization");
    // Dropping a charged producer cancels every further future. The rejected
    // live update never enlarges count/bytes; existing queued values drain.
    drop(pipeline);
    tokio::task::yield_now().await;
    let stopped = done.load(Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(done.load(Ordering::SeqCst), stopped);
    drop((recovered, permit, owner, bytes));
    while let Some(mut captured) = reader.next().await {
        captured.dequeue();
    }
    assert_eq!(telemetry.snapshot().processing.input_queue_bytes, 0);
    assert_eq!(telemetry.snapshot().processing.input_queue_count, 0);
    linux_sample("after_STOP_and_release");
    eprintln!("RAW_MEMORY_RESULT raw_window=32 width=10 live_max=384 live_bytes_cap={} provider_calls=0 operational_app=0", live_cap);
}
