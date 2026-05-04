include!("parts/01_types.rs");
include!("parts/02_policy.rs");
include!("parts/03_cli.rs");
include!("parts/04_scheduled.rs");
include!("parts/05_fresh_write.rs");
include!("parts/06_archive.rs");
include!("parts/07_hooks.rs");
include!("parts/08_staged.rs");
include!("parts/09_write.rs");
include!("parts/10_render.rs");

#[cfg(test)]
mod tests;
