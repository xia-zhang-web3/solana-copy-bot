//! Diagnostic Linux clock brackets only. Never supplies runtime time or authority.
use serde::Serialize;
use std::sync::OnceLock;
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClockIdentity {
    pub boot_id: Option<String>,
    pub time_namespace: Option<String>,
    pub timens_offsets: Option<String>,
    pub pid_namespace: Option<String>,
    pub pid: u32,
    pub process_start_ticks: Option<u64>,
    pub process_origin_monotonic_ns: Option<u64>,
    pub run_id: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClockStamp {
    pub schema: &'static str,
    pub clock_id: &'static str,
    pub clock_code: i32,
    pub before_ns: Option<u64>,
    pub elapsed_ns: u64,
    pub after_ns: Option<u64>,
    pub resolution_ns: Option<u64>,
    pub status: &'static str,
    pub identity: ClockIdentity,
}
struct Origin {
    identity: ClockIdentity,
    resolution: Option<u64>,
}
static ORIGIN: OnceLock<Origin> = OnceLock::new();
pub(super) fn observe(sample: impl FnOnce() -> u64) -> (u64, ClockStamp) {
    #[cfg(test)]
    if super::clock_qualification_tests::local_clock::enabled() {
        return super::clock_qualification_tests::local_clock::observe(sample);
    }
    // Metadata must have been initialized before spawning intake. Never filesystem I/O here.
    let origin = ORIGIN.get();
    let identity = origin.map(|o| o.identity.clone()).unwrap_or_default();
    let resolution = origin.and_then(|o| o.resolution);
    let before = linux::read(false);
    let elapsed = sample();
    let after = linux::read(false);
    let complete = before.zip(after).is_some_and(|(a, b)| a <= b)
        && resolution.is_some()
        && identity.complete();
    (
        elapsed,
        ClockStamp {
            schema: "linux_monotonic_elapsed_bracket_v1",
            clock_id: "CLOCK_MONOTONIC",
            clock_code: 1,
            before_ns: before,
            elapsed_ns: elapsed,
            after_ns: after,
            resolution_ns: resolution,
            status: if complete {
                "PRESENT_NOT_ORDER_QUALIFIED"
            } else {
                "UNAVAILABLE_OR_INCOMPLETE"
            },
            identity,
        },
    )
}
impl ClockIdentity {
    fn complete(&self) -> bool {
        self.boot_id.is_some()
            && self.time_namespace.is_some()
            && self.timens_offsets.is_some()
            && self.pid_namespace.is_some()
            && self.pid > 0
            && self.process_start_ticks.is_some()
            && self.process_origin_monotonic_ns.is_some()
            && self.run_id.is_some()
    }
}
pub(super) fn initialize() {
    ORIGIN.get_or_init(|| Origin {
        identity: ClockIdentity {
            boot_id: read_small("/proc/sys/kernel/random/boot_id", 64),
            time_namespace: namespace("/proc/self/ns/time"),
            timens_offsets: read_small("/proc/self/timens_offsets", 256),
            pid_namespace: namespace("/proc/self/ns/pid"),
            pid: std::process::id(),
            process_start_ticks: read_small("/proc/self/stat", 4096).and_then(|s| {
                s.rsplit_once(')')
                    .and_then(|(_, rest)| rest.split_whitespace().nth(19)?.parse().ok())
            }),
            process_origin_monotonic_ns: linux::read(false),
            run_id: std::env::var("COPY49_CLOCK_RUN_ID").ok().filter(|s| {
                !s.is_empty()
                    && s.len() <= 96
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
            }),
        },
        resolution: linux::read(true),
    });
}
fn read_small(path: &str, cap: u64) -> Option<String> {
    use std::io::Read;
    let mut value = String::new();
    std::fs::File::open(path)
        .ok()?
        .take(cap + 1)
        .read_to_string(&mut value)
        .ok()?;
    if value.len() as u64 > cap {
        return None;
    }
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}
fn namespace(path: &str) -> Option<String> {
    let value = std::fs::read_link(path).ok()?.to_str()?.to_owned();
    (value.len() <= 64 && value.is_ascii()).then_some(value)
}
#[cfg(target_os = "linux")]
mod linux {
    use std::os::raw::{c_int, c_long};
    #[repr(C)]
    struct Timespec {
        sec: c_long,
        nsec: c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: c_int, value: *mut Timespec) -> c_int;
        fn clock_getres(clock: c_int, value: *mut Timespec) -> c_int;
    }
    pub(super) fn read(resolution: bool) -> Option<u64> {
        let mut value = Timespec { sec: 0, nsec: 0 };
        // Linux ABI CLOCK_MONOTONIC=1; no Instant representation or wall clock used.
        let rc = unsafe {
            if resolution {
                clock_getres(1, &mut value)
            } else {
                clock_gettime(1, &mut value)
            }
        };
        if rc != 0 || value.sec < 0 || !(0..1_000_000_000).contains(&value.nsec) {
            return None;
        }
        (value.sec as u64)
            .checked_mul(1_000_000_000)?
            .checked_add(value.nsec as u64)
    }
}
#[cfg(not(target_os = "linux"))]
mod linux {
    pub(super) fn read(_: bool) -> Option<u64> {
        None
    }
}
