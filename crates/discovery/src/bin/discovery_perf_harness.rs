//! Deterministic local discovery perf harness for Package C.
//!
//! Standard command:
//! `RUSTFLAGS='-Awarnings' cargo run -p copybot-discovery --quiet --bin discovery_perf_harness`
//!
//! The binary prints a stable JSON report for the current narrow bottlenecks:
//! stale `CollectBuyMints` convergence and bounded `Replay`.

use anyhow::Result;
use copybot_discovery::perf_harness::run_standard_harness;

fn main() -> Result<()> {
    if std::env::args()
        .skip(1)
        .any(|arg| arg == "--help" || arg == "-h")
    {
        println!(
            "Usage:\n  RUSTFLAGS='-Awarnings' cargo run -p copybot-discovery --quiet --bin discovery_perf_harness"
        );
        return Ok(());
    }

    let report = run_standard_harness()?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
