use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_discovery::DiscoveryService;
use copybot_shadow::{ShadowService, ShadowSnapshot};
use copybot_storage::SqliteStore;
use std::path::Path;
use std::time::Instant;
use tracing::{info, warn};

include!("task_spawns_discovery.rs");
include!("task_spawns_shadow.rs");
