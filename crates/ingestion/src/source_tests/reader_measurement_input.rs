//! Test-only encoded block witness. Bounded try_send; hashing/file sync never run on Reader.
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        OnceLock,
    },
};
use tokio::sync::mpsc;
struct Packet {
    bytes: Vec<u8>,
    slot: u64,
    available: (u64, crate::source::durable::diagnostic_clock::ClockStamp),
    encoded_ns: u64,
}
static SENDER: OnceLock<mpsc::Sender<Packet>> = OnceLock::new();
static ATTEMPTED: AtomicU64 = AtomicU64::new(0);
static ENQUEUED: AtomicU64 = AtomicU64::new(0);
static COMPLETED: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);
pub(super) fn errors() -> u64 {
    ERRORS.load(Relaxed)
}
pub(super) fn attempted() -> u64 {
    ATTEMPTED.load(Relaxed)
}
pub(super) fn snapshot() -> serde_json::Value {
    json!({"attempted":attempted(),"enqueued":ENQUEUED.load(Relaxed),"completed":COMPLETED.load(Relaxed),"loss":errors(),"pending":ENQUEUED.load(Relaxed).saturating_sub(COMPLETED.load(Relaxed))})
}
pub(super) fn observe(bytes: &[u8], slot: u64, available: (u64, crate::source::durable::diagnostic_clock::ClockStamp)) {
    let Ok(root) = std::env::var("COPY49_MEASUREMENT_OUT") else {
        return;
    };
    if ATTEMPTED.fetch_add(1, Relaxed) >= 512 {
        ERRORS.fetch_add(1, Relaxed);
        return;
    }
    let sender=SENDER.get_or_init(||{
        let(tx,mut rx)=mpsc::channel::<Packet>(8);
        tokio::spawn(async move {while let Some(packet)=rx.recv().await{
            let root=root.clone();
            let result=tokio::task::spawn_blocking(move||->std::io::Result<()>{
                let value=json!({"typed_update_available_ns":packet.available.0,"typed_update_clock":packet.available.1,"encoded_ns":packet.encoded_ns,"slot":packet.slot,"bytes":packet.bytes.len(),"typed_reencode_sha256":format!("{:x}",Sha256::digest(&packet.bytes))});
                let mut f=OpenOptions::new().create(true).append(true).open(std::path::Path::new(&root).join("actual-reader-typed-input.jsonl"))?;
                let encoded=serde_json::to_vec(&value)?;
                if f.metadata()?.len().saturating_add(encoded.len()as u64).saturating_add(1)>256*1024{return Err(std::io::Error::other("typed input file cap"));}
                f.write_all(&encoded)?;f.write_all(b"\n")?;f.sync_data()
            }).await;
            if matches!(result,Ok(Ok(()))){COMPLETED.fetch_add(1,Relaxed);}else{ERRORS.fetch_add(1,Relaxed);}
        }});tx
    });
    let packet = Packet {
        bytes: bytes.to_vec(),
        slot,
        available,
        encoded_ns: crate::source::durable::diagnostic_monotonic_ns(),
    };
    if sender.try_send(packet).is_ok() {
        ENQUEUED.fetch_add(1, Relaxed);
    } else {
        ERRORS.fetch_add(1, Relaxed);
    }
}
