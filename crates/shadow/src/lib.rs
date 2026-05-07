mod candidate;
use self::candidate::to_shadow_candidate;
mod quality_gates;
mod signals;
mod snapshots;
use self::signals::log_gate_drop;

#[path = "lib_parts/02_service_constructors.rs"]
mod service_constructors;
#[path = "lib_parts/02_service_process_swap.rs"]
mod service_process_swap;
#[path = "lib_parts/01_types.rs"]
mod types;

pub use self::types::{
    FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService, ShadowSignalResult,
    ShadowSnapshot,
};

#[cfg(test)]
mod tests;
