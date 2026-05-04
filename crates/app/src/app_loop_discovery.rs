use super::*;

#[path = "app_loop_discovery_join.rs"]
mod join;
#[path = "app_loop_discovery_start.rs"]
mod start;

pub(super) use join::handle_discovery_join_result;
pub(super) use start::{
    try_start_discovery_catch_up, try_start_scheduled_discovery, watch_running_discovery_safety,
};
