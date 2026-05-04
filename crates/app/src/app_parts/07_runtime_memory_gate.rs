fn read_proc_file_if_present(path: &str) -> Result<Option<String>> {
    let path = Path::new(path);
    if !path.exists() {
        return Ok(None);
    }
    fs::read_to_string(path)
        .map(Some)
        .with_context(|| format!("failed reading {}", path.display()))
}

fn load_discovery_runtime_memory_pressure_gate() -> DiscoveryRuntimeMemoryPressureGate {
    let mut errors = Vec::new();
    let process_rss_bytes = match read_proc_file_if_present("/proc/self/status") {
        Ok(Some(contents)) => parse_proc_status_vmrss_bytes(&contents),
        Ok(None) => None,
        Err(error) => {
            errors.push(format!("{error:#}"));
            None
        }
    };
    let (system_available_bytes, system_total_bytes) =
        match read_proc_file_if_present("/proc/meminfo") {
            Ok(Some(contents)) => (
                parse_proc_meminfo_bytes(&contents, "MemAvailable"),
                parse_proc_meminfo_bytes(&contents, "MemTotal"),
            ),
            Ok(None) => (None, None),
            Err(error) => {
                errors.push(format!("{error:#}"));
                (None, None)
            }
        };
    discovery_runtime_memory_pressure_gate_from_snapshot(
        process_rss_bytes,
        system_available_bytes,
        system_total_bytes,
        (!errors.is_empty()).then(|| errors.join("; ")),
    )
}

fn discovery_runtime_memory_pressure_defer_reason(
    gate: &DiscoveryRuntimeMemoryPressureGate,
) -> Option<&'static str> {
    gate.pressure_reason
        .map(|_| DISCOVERY_CYCLE_DEFERRED_DUE_TO_RUNTIME_MEMORY_PRESSURE)
}

fn discovery_runtime_memory_pressure_abort_reason(
    gate: &DiscoveryRuntimeMemoryPressureGate,
) -> Option<&'static str> {
    gate.pressure_reason
        .map(|_| DISCOVERY_CYCLE_ABORTED_DUE_TO_RUNTIME_MEMORY_PRESSURE)
}

fn discovery_task_abort_preserves_catch_up_pending(trigger: &'static str) -> bool {
    trigger == "catch_up_retrigger"
}

fn discovery_task_output_should_be_ignored_due_to_abort_reason(
    abort_reason: Option<&'static str>,
) -> bool {
    abort_reason.is_some()
}
