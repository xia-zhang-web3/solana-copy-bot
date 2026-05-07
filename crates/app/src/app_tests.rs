use super::*;
use copybot_core_types::WalletMetricRow;
use copybot_storage_core::{
    DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, DiscoveryTrustedSelectionStateUpdate, TrustedSelectionState,
    TrustedSnapshotSourceKind,
};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

include!("app_tests/00.rs");
include!("app_tests/01.rs");
include!("app_tests/02.rs");
include!("app_tests/03.rs");
include!("app_tests/04.rs");
include!("app_tests/05.rs");
include!("app_tests/07.rs");
include!("app_tests/08.rs");
include!("app_tests/09.rs");
include!("app_tests/10.rs");
include!("app_tests/11.rs");
include!("app_tests/12.rs");
include!("app_tests/13.rs");
include!("app_tests/14.rs");
include!("app_tests/15.rs");
include!("app_tests/16.rs");
include!("app_tests/17.rs");
include!("app_tests/18.rs");
include!("app_tests/19.rs");
include!("app_tests/20.rs");
include!("app_tests/21.rs");
include!("app_tests/22.rs");
include!("app_tests/23.rs");
include!("app_tests/24.rs");
include!("app_tests/25.rs");
include!("app_tests/26.rs");
