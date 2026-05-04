use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
mod freshness;

use copybot_config::load_from_path;
use copybot_runtime_artifacts::{
    artifact_archive_path, artifact_latest_path, copy_atomic, load_json, prune_rotated_archives,
    resolve_db_path, resolve_relative_to_config, write_json_atomic, ARTIFACT_ARCHIVE_PREFIX,
    ARTIFACT_ARCHIVE_SUFFIX,
};
use copybot_storage_core::{
    DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact, SqliteStore,
};
use freshness::{assess_runtime_artifact_freshness, publication_freshness_gate};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

include!("runtime_export/types.rs");
include!("runtime_export/cli.rs");
include!("runtime_export/run.rs");
include!("runtime_export/render.rs");
