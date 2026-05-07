#[path = "parts/06_archive.rs"]
mod archive;
#[path = "parts/03_cli.rs"]
mod cli;
#[path = "parts/05_fresh_write.rs"]
mod fresh_write;
#[path = "parts/07_hooks.rs"]
mod hooks;
#[path = "parts/02_policy.rs"]
mod policy;
#[path = "parts/10_render.rs"]
mod render;
#[path = "parts/04_scheduled.rs"]
mod scheduled;
#[path = "parts/08_staged.rs"]
mod staged;
#[path = "parts/01_types.rs"]
mod types;
#[path = "parts/09_write.rs"]
mod write;

pub(crate) use self::archive::*;
pub(crate) use self::cli::*;
pub(crate) use self::fresh_write::*;
pub(crate) use self::hooks::*;
pub(crate) use self::policy::*;
pub(crate) use self::render::*;
pub(crate) use self::scheduled::*;
pub(crate) use self::staged::*;
pub use self::types::main_entry;
pub(crate) use self::types::*;
pub(crate) use self::write::*;

#[cfg(test)]
mod tests;
