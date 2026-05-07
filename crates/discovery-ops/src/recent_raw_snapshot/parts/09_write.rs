#[path = "09_write_files.rs"]
mod files;
#[path = "09_write_render_output.rs"]
mod render;

use self::files::{link_or_copy_atomic, snapshot_manifest, write_snapshot_with_policy};
use self::render::render_output;
