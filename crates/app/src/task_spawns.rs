use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_shadow::{ShadowService, ShadowSnapshot};
use copybot_storage::SqliteStore;
use std::path::Path;

include!("task_spawns_shadow.rs");
