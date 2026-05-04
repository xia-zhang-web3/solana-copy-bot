#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveryRuntimeMemoryPressureGate {
    pressure_reason: Option<&'static str>,
    process_rss_bytes: Option<u64>,
    process_rss_threshold_bytes: u64,
    system_available_bytes: Option<u64>,
    system_available_threshold_bytes: u64,
    system_total_bytes: Option<u64>,
    system_available_bps: Option<u64>,
    system_available_min_bps: u64,
    error: Option<String>,
}

fn discovery_runtime_memory_pressure_gate_from_snapshot(
    process_rss_bytes: Option<u64>,
    system_available_bytes: Option<u64>,
    system_total_bytes: Option<u64>,
    error: Option<String>,
) -> DiscoveryRuntimeMemoryPressureGate {
    let system_available_bps = match (system_available_bytes, system_total_bytes) {
        (Some(available), Some(total)) if total > 0 => {
            Some(available.saturating_mul(10_000) / total)
        }
        _ => None,
    };
    let pressure_reason = if process_rss_bytes
        .is_some_and(|rss| rss >= DISCOVERY_RUNTIME_MEMORY_PRESSURE_PROCESS_RSS_THRESHOLD_BYTES)
    {
        Some("process_rss_over_threshold")
    } else if system_available_bytes.is_some_and(|available| {
        available <= DISCOVERY_RUNTIME_MEMORY_PRESSURE_SYSTEM_AVAILABLE_THRESHOLD_BYTES
    }) {
        Some("system_available_below_threshold")
    } else if system_available_bps
        .is_some_and(|bps| bps <= DISCOVERY_RUNTIME_MEMORY_PRESSURE_SYSTEM_AVAILABLE_MIN_BPS)
    {
        Some("system_available_fraction_below_threshold")
    } else {
        None
    };

    DiscoveryRuntimeMemoryPressureGate {
        pressure_reason,
        process_rss_bytes,
        process_rss_threshold_bytes: DISCOVERY_RUNTIME_MEMORY_PRESSURE_PROCESS_RSS_THRESHOLD_BYTES,
        system_available_bytes,
        system_available_threshold_bytes:
            DISCOVERY_RUNTIME_MEMORY_PRESSURE_SYSTEM_AVAILABLE_THRESHOLD_BYTES,
        system_total_bytes,
        system_available_bps,
        system_available_min_bps: DISCOVERY_RUNTIME_MEMORY_PRESSURE_SYSTEM_AVAILABLE_MIN_BPS,
        error,
    }
}

fn parse_proc_status_vmrss_bytes(contents: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?.trim();
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(kb.saturating_mul(1024))
    })
}

fn parse_proc_meminfo_bytes(contents: &str, key: &str) -> Option<u64> {
    let prefix = format!("{key}:");
    contents.lines().find_map(|line| {
        let rest = line.strip_prefix(&prefix)?.trim();
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(kb.saturating_mul(1024))
    })
}
