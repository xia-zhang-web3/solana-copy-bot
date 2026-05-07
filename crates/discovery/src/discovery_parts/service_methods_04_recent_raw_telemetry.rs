use super::*;

impl DiscoveryService {
    pub(crate) fn recent_raw_snapshot_dir_for_state_root(state_root: &Path) -> PathBuf {
        let direct_candidate = state_root.to_path_buf();
        let nested_candidate = state_root.join("discovery_restore/recent_raw");
        let direct_has_recent_raw_artifacts = [
            runtime_artifacts::journal_snapshot_latest_path(&direct_candidate),
            runtime_artifacts::journal_snapshot_latest_metadata_path(&direct_candidate),
            direct_candidate.join(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME),
            direct_candidate.join(RECENT_RAW_STAGED_METADATA_FILE_NAME),
        ]
        .iter()
        .any(|path| path.exists());
        if direct_has_recent_raw_artifacts {
            direct_candidate
        } else {
            nested_candidate
        }
    }

    pub(crate) fn recent_raw_staged_snapshot_path(snapshot_dir: &Path) -> PathBuf {
        snapshot_dir.join(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME)
    }

    pub(crate) fn recent_raw_staged_metadata_path(snapshot_dir: &Path) -> PathBuf {
        snapshot_dir.join(RECENT_RAW_STAGED_METADATA_FILE_NAME)
    }

    pub(crate) fn recent_raw_staged_candidate_reads_result(
        snapshot_dir: &Path,
    ) -> Result<Vec<RecentRawStagedCandidateRead>> {
        let mut candidates: BTreeMap<String, (Option<PathBuf>, Option<PathBuf>)> = BTreeMap::new();
        let snapshot_prefix = format!("{RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME}.");
        let entries = fs::read_dir(snapshot_dir)
            .with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let name = name.to_string();
            if name.contains(".tmp-") {
                continue;
            }
            if !name.ends_with(".json")
                && (name == RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME
                    || name.starts_with(&snapshot_prefix))
            {
                candidates.entry(name).or_default().0 = Some(path);
                continue;
            }
            if name.ends_with(".json") {
                let Some(base_name) = name.strip_suffix(".json") else {
                    continue;
                };
                if base_name != RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME
                    && !base_name.starts_with(&snapshot_prefix)
                {
                    continue;
                }
                candidates.entry(base_name.to_string()).or_default().1 = Some(path);
            }
        }
        Ok(candidates
            .into_iter()
            .map(|(base_name, (snapshot_path, metadata_path))| {
                let snapshot_path = snapshot_path.unwrap_or_else(|| snapshot_dir.join(&base_name));
                let metadata_path =
                    metadata_path.unwrap_or_else(|| snapshot_dir.join(format!("{base_name}.json")));
                let surface =
                    Self::read_recent_raw_surface_manifest(&snapshot_path, &metadata_path);
                RecentRawStagedCandidateRead {
                    snapshot_path,
                    metadata_path,
                    surface,
                }
            })
            .collect())
    }

    pub(crate) fn recent_raw_staged_candidate_reads(snapshot_dir: &Path) -> Vec<RecentRawStagedCandidateRead> {
        Self::recent_raw_staged_candidate_reads_result(snapshot_dir).unwrap_or_default()
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_candidate_dirs(
        state_root: &Path,
        snapshot_dir: &Path,
    ) -> Vec<PathBuf> {
        let candidates = [
            snapshot_dir.to_path_buf(),
            state_root.join("discovery_restore/artifacts"),
            state_root.join("artifacts"),
        ];
        let mut seen = BTreeSet::new();
        candidates
            .into_iter()
            .filter(|path| seen.insert(path.display().to_string()))
            .collect()
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_explicit_paths(
        state_root: &Path,
        snapshot_dir: &Path,
    ) -> Vec<PathBuf> {
        let candidates = [
            state_root.join("discovery_restore/artifacts/latest.json"),
            state_root.join("artifacts/latest.json"),
            state_root.join("discovery_restore/recent_raw/latest.json"),
            snapshot_dir.join("latest.json"),
            Self::recent_raw_replacement_attempt_telemetry_latest_path(snapshot_dir),
            snapshot_dir.join("recent_raw_snapshot_attempt_latest.json"),
            snapshot_dir.join("snapshot_attempt_latest.json"),
        ];
        let mut seen = BTreeSet::new();
        candidates
            .into_iter()
            .filter(|path| seen.insert(path.display().to_string()))
            .collect()
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_latest_path(snapshot_dir: &Path) -> PathBuf {
        snapshot_dir.join(RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME)
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_filename_candidate(name: &str) -> bool {
        name.contains("discovery_recent_raw_snapshot")
            || name.contains("recent_raw_snapshot")
            || name.contains("snapshot_attempt")
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_modified_at(path: &Path) -> Option<DateTime<Utc>> {
        fs::metadata(path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .map(DateTime::<Utc>::from)
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_timestamp(
        read: &RecentRawSnapshotAttemptTelemetryRead,
    ) -> Option<DateTime<Utc>> {
        read.telemetry
            .as_ref()
            .and_then(|telemetry| {
                telemetry
                    .last_batch_completed_at
                    .or(telemetry.created_at)
                    .or(read.modified_at)
            })
            .or(read.modified_at)
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_read_path(
        path: &Path,
    ) -> Option<RecentRawSnapshotAttemptTelemetryRead> {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let filename_candidate =
            Self::recent_raw_replacement_attempt_telemetry_filename_candidate(name);
        let modified_at = Self::recent_raw_replacement_attempt_telemetry_modified_at(path);
        let raw = match fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) => {
                return (filename_candidate && error.kind() != std::io::ErrorKind::NotFound).then(
                    || RecentRawSnapshotAttemptTelemetryRead {
                        path: path.to_path_buf(),
                        modified_at,
                        telemetry: None,
                    },
                );
            }
        };
        let value = match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(value) => value,
            Err(_) => {
                return filename_candidate.then(|| RecentRawSnapshotAttemptTelemetryRead {
                    path: path.to_path_buf(),
                    modified_at,
                    telemetry: None,
                });
            }
        };
        let event_matches = value.get("event").and_then(serde_json::Value::as_str)
            == Some("discovery_recent_raw_snapshot");
        if !event_matches && !filename_candidate {
            return None;
        }
        let telemetry = serde_json::from_value::<RecentRawSnapshotAttemptTelemetryArtifact>(value);
        Some(match telemetry {
            Ok(telemetry)
                if telemetry.event.as_deref() == Some("discovery_recent_raw_snapshot") =>
            {
                RecentRawSnapshotAttemptTelemetryRead {
                    path: path.to_path_buf(),
                    modified_at,
                    telemetry: Some(telemetry),
                }
            }
            Ok(_) | Err(_) => RecentRawSnapshotAttemptTelemetryRead {
                path: path.to_path_buf(),
                modified_at,
                telemetry: None,
            },
        })
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_explicit_reads(
        state_root: &Path,
        snapshot_dir: &Path,
    ) -> (Vec<String>, Vec<RecentRawSnapshotAttemptTelemetryRead>) {
        let paths =
            Self::recent_raw_replacement_attempt_telemetry_explicit_paths(state_root, snapshot_dir);
        let reads = paths
            .iter()
            .filter_map(|path| Self::recent_raw_replacement_attempt_telemetry_read_path(path))
            .collect();
        (
            paths
                .into_iter()
                .map(|path| path.display().to_string())
                .collect(),
            reads,
        )
    }

    pub(crate) fn recent_raw_replacement_attempt_telemetry_deep_scan_reads(
        state_root: &Path,
        snapshot_dir: &Path,
    ) -> (
        Vec<String>,
        Vec<RecentRawSnapshotAttemptTelemetryRead>,
        bool,
    ) {
        let dirs =
            Self::recent_raw_replacement_attempt_telemetry_candidate_dirs(state_root, snapshot_dir);
        let mut reads = Vec::new();
        let mut scan_truncated = false;
        for dir in &dirs {
            let Ok(entries) = fs::read_dir(dir) else {
                continue;
            };
            let mut json_files_seen = 0usize;
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if !name.ends_with(".json") {
                    continue;
                }
                json_files_seen = json_files_seen.saturating_add(1);
                if json_files_seen > RECENT_RAW_ATTEMPT_TELEMETRY_SCAN_FILE_LIMIT {
                    scan_truncated = true;
                    continue;
                }

                if let Some(read) = Self::recent_raw_replacement_attempt_telemetry_read_path(&path)
                {
                    reads.push(read);
                }
            }
        }
        (
            dirs.into_iter()
                .map(|path| path.display().to_string())
                .collect(),
            reads,
            scan_truncated,
        )
    }
}
