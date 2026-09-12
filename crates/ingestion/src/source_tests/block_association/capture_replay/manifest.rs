use super::*;
use anyhow::{ensure, Context};
use std::io::Read;

pub(super) const RESERVE: u64 = 262_144;
pub(super) const MAX_RESERVE: u64 = 2_097_152;
pub(super) const TRANSPORT: u64 = 1_048_576;
pub(super) struct Limits {
    pub messages: u64,
    pub message_bytes: u64,
    pub total_bytes: u64,
    pub metadata_reserve: u64,
}
pub(super) fn number(v: &Value, key: &str) -> Result<u64> {
    v[key].as_u64().with_context(|| format!("invalid {key}"))
}
pub(super) fn bounded_file(path: &Path, cap: u64) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path).context("missing capture file")?;
    ensure!(
        metadata.is_file() && metadata.len() <= cap,
        "capture file type/size bound"
    );
    let file = std::fs::File::open(path).context("capture file open")?;
    ensure!(file.metadata()?.is_file(), "capture file is not regular");
    let mut raw = Vec::new();
    file.take(cap + 1).read_to_end(&mut raw)?;
    ensure!(raw.len() as u64 <= cap, "capture read size bound");
    Ok(raw)
}
pub(super) fn validate(m: &Value) -> Result<Limits> {
    ensure!(
        m["schema"] == 1 && m["mode"] == "association-capture" && m["production_green"] == false,
        "unsupported capture schema/mode/green flag"
    );
    let stop = m["stop_reason"].as_str().context("missing stop reason")?;
    ensure!(
        matches!(
            stop,
            "stream_closed" | "message_count_limit" | "stream_deadline"
        ),
        "unsupported terminal capture reason; diagnostic prefix refused"
    );
    ensure!(
        m["complete"] == (stop == "stream_closed"),
        "inconsistent completeness"
    );
    let (diagnostic, count_cap, metadata_reserve) = match m.get("capture_profile") {
        None => (false, 256, RESERVE),
        Some(v) if v == "diagnostic-v1" => (true, 256, RESERVE),
        Some(v) if v == "window-v1" => (true, 4096, MAX_RESERVE),
        Some(_) => anyhow::bail!("unsupported capture profile"),
    };
    let l = &m["limits"];
    let duration = number(l, "duration_ms")?;
    let limits = Limits {
        messages: number(l, "messages")?,
        message_bytes: number(l, "message_bytes")?,
        total_bytes: number(l, "total_bytes")?,
        metadata_reserve,
    };
    let (message_cap, total_cap, transport) = if diagnostic {
        (8_388_608, 67_108_864, limits.message_bytes)
    } else {
        (TRANSPORT, 16_777_216, TRANSPORT)
    };
    ensure!(
        m["transport_decode_bytes"] == transport && m["metadata_reserve_bytes"] == metadata_reserve,
        "unsupported transport/metadata bounds"
    );
    ensure!(
        (1..=60_000).contains(&duration)
            && (1..=count_cap).contains(&limits.messages)
            && (1..=message_cap).contains(&limits.message_bytes)
            && limits.total_bytes > metadata_reserve
            && limits.total_bytes <= total_cap,
        "capture limits out of bounds"
    );
    let session = m["session_id"].as_str().context("missing session ID")?;
    ensure!(
        session.len() == 32 && session.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid session ID"
    );
    let count = number(m, "messages_received")?;
    ensure!(
        count <= limits.messages && number(m, "elapsed_ns")? <= i64::MAX as u64,
        "capture count/elapsed bound"
    );
    if stop == "message_count_limit" {
        ensure!(count == limits.messages, "count cutoff mismatch");
    }
    if stop == "stream_closed" {
        ensure!(count < limits.messages, "closed at count cutoff");
    }
    ensure!(
        m["provider_coverage"] == "unmeasured" && m["association_verdict"] == "not_evaluated",
        "capture overstates coverage/verdict"
    );
    Ok(limits)
}
