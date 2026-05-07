#[path = "10_render_human.rs"]
mod human;
#[path = "10_render_overview.rs"]
mod overview;
#[path = "10_render_staged_progress.rs"]
mod staged_progress;
#[path = "10_render_staged_write.rs"]
mod staged_write;
#[path = "10_render_snapshot_attempt.rs"]
mod snapshot_attempt;
#[path = "10_render_manifest.rs"]
mod manifest;

use self::human::render_human;
use self::manifest::append_render_manifest;
use self::overview::append_render_overview;
use self::snapshot_attempt::append_render_snapshot_attempt;
use self::staged_progress::append_render_staged_progress;
use self::staged_write::append_render_staged_write;
