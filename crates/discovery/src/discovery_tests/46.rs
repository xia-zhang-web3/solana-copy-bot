    impl RecentRawPromotionFixture {
        fn write_promoted_surface(
            &self,
            snapshot_file_name: &str,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            self.write_promoted_surface_with_covered_since(
                snapshot_file_name,
                parse_ts("2026-04-14T07:55:00Z")?,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_promoted_surface_with_covered_since(
            &self,
            snapshot_file_name: &str,
            covered_since: DateTime<Utc>,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            let snapshot_path = self.recent_raw_dir.join(snapshot_file_name);
            let metadata_path = self.recent_raw_dir.join("latest.json");
            self.write_surface_with_source_path_and_covered_since(
                &snapshot_path,
                &metadata_path,
                &self.runtime_db_path,
                covered_since,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_staged_surface(
            &self,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            self.write_staged_surface_with_source_path_and_covered_since(
                &self.runtime_db_path,
                parse_ts("2026-04-14T07:55:00Z")?,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_staged_surface_with_source_path(
            &self,
            source_db_path: &Path,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            self.write_staged_surface_with_source_path_and_covered_since(
                source_db_path,
                parse_ts("2026-04-14T07:55:00Z")?,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_staged_surface_with_source_path_and_covered_since(
            &self,
            source_db_path: &Path,
            covered_since: DateTime<Utc>,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            let snapshot_path = self
                .recent_raw_dir
                .join(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME);
            let metadata_path = self
                .recent_raw_dir
                .join(RECENT_RAW_STAGED_METADATA_FILE_NAME);
            self.write_surface_with_source_path_and_covered_since(
                &snapshot_path,
                &metadata_path,
                source_db_path,
                covered_since,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_named_staged_candidate_surface(
            &self,
            snapshot_file_name: &str,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            self.write_named_staged_candidate_surface_with_source_path_and_covered_since(
                snapshot_file_name,
                &self.runtime_db_path,
                parse_ts("2026-04-14T07:55:00Z")?,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_named_staged_candidate_surface_with_source_path_and_covered_since(
            &self,
            snapshot_file_name: &str,
            source_db_path: &Path,
            covered_since: DateTime<Utc>,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            let snapshot_path = self.recent_raw_dir.join(snapshot_file_name);
            let metadata_path = self
                .recent_raw_dir
                .join(format!("{snapshot_file_name}.json"));
            self.write_surface_with_source_path_and_covered_since(
                &snapshot_path,
                &metadata_path,
                source_db_path,
                covered_since,
                row_count,
                cursor_ts,
                signature,
                created_at,
            )
        }

        fn write_attempt_telemetry_json(
            &self,
            file_name: &str,
            value: serde_json::Value,
        ) -> Result<PathBuf> {
            let path = self.recent_raw_dir.join(file_name);
            runtime_artifacts::write_json_atomic(&path, &value)
                .with_context(|| format!("failed writing {}", path.display()))?;
            Ok(path)
        }

        fn rewrite_selected_staged_last_batch_completed_at(
            &self,
            last_batch_completed_at: DateTime<Utc>,
        ) -> Result<()> {
            let metadata_path = self
                .recent_raw_dir
                .join(RECENT_RAW_STAGED_METADATA_FILE_NAME);
            let mut manifest =
                runtime_artifacts::load_json::<RecentRawPromotionSnapshotManifest>(&metadata_path)
                    .with_context(|| {
                        format!("failed loading staged metadata {}", metadata_path.display())
                    })?;
            manifest.last_batch_completed_at = Some(last_batch_completed_at);
            manifest.updated_at = Some(last_batch_completed_at);
            runtime_artifacts::write_json_atomic(&metadata_path, &manifest).with_context(|| {
                format!("failed writing staged metadata {}", metadata_path.display())
            })?;
            Ok(())
        }

        fn write_selected_staged_snapshot_sqlite_content(
            &self,
            swaps: &[SwapEvent],
            completed_at: DateTime<Utc>,
        ) -> Result<()> {
            let snapshot_path = self
                .recent_raw_dir
                .join(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME);
            if snapshot_path.exists() {
                fs::remove_file(&snapshot_path)
                    .with_context(|| format!("failed removing {}", snapshot_path.display()))?;
            }
            let store = SqliteStore::open(&snapshot_path)
                .with_context(|| format!("failed opening {}", snapshot_path.display()))?;
            store.insert_recent_raw_journal_batch(swaps, completed_at)?;
            Ok(())
        }

        fn rewrite_source_state(
            &self,
            swaps: &[SwapEvent],
            completed_at: DateTime<Utc>,
        ) -> Result<()> {
            for path in [
                self.runtime_db_path.clone(),
                PathBuf::from(format!("{}-wal", self.runtime_db_path.display())),
                PathBuf::from(format!("{}-shm", self.runtime_db_path.display())),
            ] {
                if path.exists() {
                    fs::remove_file(&path)
                        .with_context(|| format!("failed removing {}", path.display()))?;
                }
            }
            let store = SqliteStore::open(&self.runtime_db_path)?;
            store.insert_recent_raw_journal_batch(swaps, completed_at)?;
            Ok(())
        }

        fn prune_source_state_before(
            &self,
            cutoff: DateTime<Utc>,
            batch_size: usize,
            pruned_at: DateTime<Utc>,
        ) -> Result<usize> {
            let store = SqliteStore::open(&self.runtime_db_path)?;
            store.prune_recent_raw_journal_before_batch(cutoff, batch_size, pruned_at)
        }

        fn write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &self,
            source_db_path: &Path,
            swaps: &[SwapEvent],
            completed_at: DateTime<Utc>,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            let snapshot_path = self
                .recent_raw_dir
                .join(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME);
            let metadata_path = self
                .recent_raw_dir
                .join(RECENT_RAW_STAGED_METADATA_FILE_NAME);
            for path in [
                snapshot_path.clone(),
                metadata_path.clone(),
                PathBuf::from(format!("{}-wal", snapshot_path.display())),
                PathBuf::from(format!("{}-shm", snapshot_path.display())),
            ] {
                if path.exists() {
                    fs::remove_file(&path)
                        .with_context(|| format!("failed removing {}", path.display()))?;
                }
            }

            let store = SqliteStore::open(&snapshot_path)?;
            store.insert_recent_raw_journal_batch(swaps, completed_at)?;
            let state = store.recent_raw_journal_state_cached()?;
            let snapshot_bytes = fs::metadata(&snapshot_path)
                .with_context(|| format!("failed stat {}", snapshot_path.display()))?
                .len();
            let manifest = RecentRawPromotionSnapshotManifest {
                created_at,
                source_db_path: source_db_path.display().to_string(),
                snapshot_path: snapshot_path.display().to_string(),
                row_count: state.row_count,
                covered_since: state.covered_since,
                covered_through_cursor: state.covered_through_cursor,
                last_batch_completed_at: state.last_batch_completed_at,
                updated_at: state.updated_at,
                snapshot_bytes,
            };
            runtime_artifacts::write_json_atomic(&metadata_path, &manifest)
                .with_context(|| format!("failed writing {}", metadata_path.display()))?;
            Ok(())
        }

        fn write_surface_with_source_path_and_covered_since(
            &self,
            snapshot_path: &Path,
            metadata_path: &Path,
            source_db_path: &Path,
            covered_since: DateTime<Utc>,
            row_count: usize,
            cursor_ts: DateTime<Utc>,
            signature: &str,
            created_at: DateTime<Utc>,
        ) -> Result<()> {
            fs::write(snapshot_path, b"snapshot")
                .with_context(|| format!("failed writing {}", snapshot_path.display()))?;
            let manifest = RecentRawPromotionSnapshotManifest {
                created_at,
                source_db_path: source_db_path.display().to_string(),
                snapshot_path: snapshot_path.display().to_string(),
                row_count,
                covered_since: Some(covered_since),
                covered_through_cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: cursor_ts,
                    slot: cursor_ts.timestamp().max(0) as u64,
                    signature: signature.to_string(),
                }),
                last_batch_completed_at: Some(created_at),
                updated_at: Some(created_at),
                snapshot_bytes: 8,
            };
            runtime_artifacts::write_json_atomic(metadata_path, &manifest)
                .with_context(|| format!("failed writing {}", metadata_path.display()))?;
            Ok(())
        }

        fn capture_bytes(&self) -> Result<Vec<(String, Vec<u8>)>> {
            let mut entries = Vec::new();
            for path in [
                self.runtime_db_path.clone(),
                self.recent_raw_dir.join("latest.sqlite"),
                self.recent_raw_dir.join("latest.json"),
                self.recent_raw_dir
                    .join(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME),
                self.recent_raw_dir
                    .join(RECENT_RAW_STAGED_METADATA_FILE_NAME),
                self.recent_raw_dir
                    .join(RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME),
            ] {
                if path.exists() {
                    entries.push((
                        path.display().to_string(),
                        fs::read(&path)
                            .with_context(|| format!("failed reading {}", path.display()))?,
                    ));
                }
            }
            Ok(entries)
        }
    }

    fn make_recent_raw_promotion_fixture(
        name: &str,
        source_seed: SourceStateSeed,
    ) -> Result<RecentRawPromotionFixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let state_root = temp.path().join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)
            .with_context(|| format!("failed creating {}", recent_raw_dir.display()))?;
        let runtime_db_path = temp.path().join(format!("{name}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&runtime_db_path)?;
        store.run_migrations(&migration_dir)?;
        seed_recent_raw_source_state_for_fixture(&store, source_seed)?;
        Ok(RecentRawPromotionFixture {
            _temp: temp,
            state_root,
            recent_raw_dir,
            runtime_db_path,
        })
    }

    fn seed_recent_raw_source_state_for_fixture(
        store: &SqliteStore,
        source_seed: SourceStateSeed,
    ) -> Result<()> {
        match source_seed {
            SourceStateSeed::StagedCurrent => {
                store.insert_recent_raw_journal_batch(
                    &[
                        swap(
                            "wallet-raw",
                            "sig-promoted",
                            parse_ts("2026-04-14T07:55:00Z")?,
                            SOL_MINT,
                            "TokenRaw111111111111111111111111111111111",
                            1.0,
                            10.0,
                        ),
                        swap(
                            "wallet-raw",
                            "sig-staged",
                            parse_ts("2026-04-14T07:56:00Z")?,
                            SOL_MINT,
                            "TokenRaw111111111111111111111111111111111",
                            1.0,
                            11.0,
                        ),
                    ],
                    parse_ts("2026-04-14T08:06:00Z")?,
                )?;
            }
            SourceStateSeed::SourceAheadOfStaged => {
                store.insert_recent_raw_journal_batch(
                    &[
                        swap(
                            "wallet-raw",
                            "sig-promoted",
                            parse_ts("2026-04-14T07:55:00Z")?,
                            SOL_MINT,
                            "TokenRaw111111111111111111111111111111111",
                            1.0,
                            10.0,
                        ),
                        swap(
                            "wallet-raw",
                            "sig-staged",
                            parse_ts("2026-04-14T07:56:00Z")?,
                            SOL_MINT,
                            "TokenRaw111111111111111111111111111111111",
                            1.0,
                            11.0,
                        ),
                        swap(
                            "wallet-raw",
                            "sig-source",
                            parse_ts("2026-04-14T07:57:00Z")?,
                            SOL_MINT,
                            "TokenRaw111111111111111111111111111111111",
                            1.0,
                            12.0,
                        ),
                    ],
                    parse_ts("2026-04-14T08:07:00Z")?,
                )?;
            }
            SourceStateSeed::Missing => {}
        }
        Ok(())
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn insert_recent_profitable_pair(
        store: &SqliteStore,
        wallet_id: &str,
        now: DateTime<Utc>,
        signature_prefix: &str,
    ) -> Result<()> {
        let token = format!("Token{signature_prefix}111111111111111111111111");
        store.insert_observed_swap(&swap(
            wallet_id,
            &format!("{signature_prefix}-buy"),
            now - Duration::minutes(2),
            SOL_MINT,
            token.as_str(),
            1.0,
            100.0,
        ))?;
        store.insert_observed_swap(&swap(
            wallet_id,
            &format!("{signature_prefix}-sell"),
            now - Duration::minutes(1),
            token.as_str(),
            SOL_MINT,
            100.0,
            1.2,
        ))?;
        Ok(())
    }

    fn swap(
        wallet: &str,
        signature: &str,
        ts_utc: DateTime<Utc>,
        token_in: &str,
        token_out: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> SwapEvent {
        SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in,
            amount_out,
            signature: signature.to_string(),
            slot: ts_utc.timestamp().max(0) as u64,
            ts_utc,
            exact_amounts: None,
        }
    }
