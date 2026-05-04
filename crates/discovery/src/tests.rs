use super::*;
use crate::wallet_freshness_audit::{
    current_raw_truth_sample_call_count_for_tests,
    reset_current_raw_truth_sample_call_count_for_tests, wallet_freshness_capture_from_row,
    WalletFreshnessHistoryVerdict,
};
use anyhow::{anyhow, Context};
use copybot_config::ShadowConfig;
use copybot_storage::{
    CopySignalRow, DiscoveryAggregateWriteConfig, DiscoveryPersistedRebuildPhase,
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
    WalletActivityDayRow, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
};
use rusqlite::Connection;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

include!("discovery_tests/00.rs");
include!("discovery_tests/01.rs");
include!("discovery_tests/02.rs");
include!("discovery_tests/03.rs");
include!("discovery_tests/04.rs");
include!("discovery_tests/05.rs");
include!("discovery_tests/06.rs");
include!("discovery_tests/07.rs");
include!("discovery_tests/08.rs");
include!("discovery_tests/09.rs");
include!("discovery_tests/10.rs");
include!("discovery_tests/11.rs");
include!("discovery_tests/12.rs");
include!("discovery_tests/13.rs");
include!("discovery_tests/14.rs");
include!("discovery_tests/15.rs");
include!("discovery_tests/16.rs");
include!("discovery_tests/17.rs");
include!("discovery_tests/18.rs");
include!("discovery_tests/19.rs");
include!("discovery_tests/20.rs");
include!("discovery_tests/21.rs");
include!("discovery_tests/22.rs");
include!("discovery_tests/23.rs");
include!("discovery_tests/24.rs");
include!("discovery_tests/25.rs");
include!("discovery_tests/26.rs");
include!("discovery_tests/27.rs");
include!("discovery_tests/28.rs");
include!("discovery_tests/29.rs");
include!("discovery_tests/30.rs");
include!("discovery_tests/31.rs");
include!("discovery_tests/32.rs");
include!("discovery_tests/33.rs");
include!("discovery_tests/34.rs");
include!("discovery_tests/35.rs");
include!("discovery_tests/36.rs");
include!("discovery_tests/37.rs");
include!("discovery_tests/38.rs");
include!("discovery_tests/39.rs");
include!("discovery_tests/40.rs");
include!("discovery_tests/41.rs");
include!("discovery_tests/42.rs");
include!("discovery_tests/43.rs");
include!("discovery_tests/44.rs");
include!("discovery_tests/45.rs");
include!("discovery_tests/46.rs");
