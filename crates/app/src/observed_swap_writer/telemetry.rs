use super::*;

#[path = "telemetry_counters.rs"]
mod telemetry_counters;
#[path = "telemetry_snapshot.rs"]
mod telemetry_snapshot;
#[path = "telemetry_struct.rs"]
mod telemetry_struct;

pub(in crate::observed_swap_writer) use telemetry_struct::ObservedSwapWriterTelemetry;
