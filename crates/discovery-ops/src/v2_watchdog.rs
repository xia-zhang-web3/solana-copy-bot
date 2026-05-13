#[path = "v2_watchdog/cli.rs"]
mod cli;
#[path = "v2_watchdog/render.rs"]
mod render;
#[path = "v2_watchdog/run.rs"]
mod run;
#[path = "v2_watchdog/types.rs"]
mod types;

pub use cli::main_entry;
pub use run::run_watchdog;
pub use types::{WatchdogConfig, WatchdogOutput, WatchdogState};
