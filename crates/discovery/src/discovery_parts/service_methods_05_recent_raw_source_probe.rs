impl DiscoveryService {
    fn load_recent_raw_source_observed_swaps_bounded_probe_read_only(
        runtime_db_path: &Path,
    ) -> RecentRawObservedSwapsBoundedProbeRead {
        let conn =
            match Connection::open_with_flags(runtime_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .with_context(|| format!("failed opening {}", runtime_db_path.display()))
            {
                Ok(conn) => conn,
                Err(error) => {
                    return RecentRawObservedSwapsBoundedProbeRead {
                        covered_since: None,
                        covered_through_cursor: None,
                        error: Some(format!("{error:#}")),
                    }
                }
            };
        let table_exists = match conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'observed_swaps'
                 LIMIT 1",
                [],
                |_| Ok(true),
            )
            .optional()
            .context("failed checking observed_swaps table")
        {
            Ok(value) => value.unwrap_or(false),
            Err(error) => {
                return RecentRawObservedSwapsBoundedProbeRead {
                    covered_since: None,
                    covered_through_cursor: None,
                    error: Some(format!("{error:#}")),
                }
            }
        };
        if !table_exists {
            return RecentRawObservedSwapsBoundedProbeRead {
                covered_since: None,
                covered_through_cursor: None,
                error: Some("observed_swaps table is missing".to_string()),
            };
        }

        let oldest_raw = conn
            .query_row(
                "SELECT ts
                 FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed probing oldest observed_swaps row");
        let newest_raw = conn
            .query_row(
                "SELECT ts, slot, signature
                 FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .context("failed probing newest observed_swaps row");

        match (oldest_raw, newest_raw) {
            (Ok(oldest_raw), Ok(newest_raw)) => {
                let covered_since = match oldest_raw {
                    Some(oldest_raw) => match Self::parse_recent_raw_optional_rfc3339_utc(
                        Some(oldest_raw),
                        "observed_swaps.ts",
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            return RecentRawObservedSwapsBoundedProbeRead {
                                covered_since: None,
                                covered_through_cursor: None,
                                error: Some(format!("{error:#}")),
                            }
                        }
                    },
                    None => None,
                };
                let covered_through_cursor = match newest_raw {
                    Some((ts_raw, slot_raw, signature)) => {
                        match DateTime::parse_from_rfc3339(&ts_raw)
                            .map(|dt| dt.with_timezone(&Utc))
                            .with_context(|| {
                                format!("invalid observed_swaps.ts timestamp value: {ts_raw}")
                            }) {
                            Ok(ts_utc) => Some(DiscoveryRuntimeCursor {
                                ts_utc,
                                slot: slot_raw.max(0) as u64,
                                signature,
                            }),
                            Err(error) => {
                                return RecentRawObservedSwapsBoundedProbeRead {
                                    covered_since: None,
                                    covered_through_cursor: None,
                                    error: Some(format!("{error:#}")),
                                }
                            }
                        }
                    }
                    None => None,
                };
                RecentRawObservedSwapsBoundedProbeRead {
                    covered_since,
                    covered_through_cursor,
                    error: None,
                }
            }
            (Err(error), _) | (_, Err(error)) => RecentRawObservedSwapsBoundedProbeRead {
                covered_since: None,
                covered_through_cursor: None,
                error: Some(format!("{error:#}")),
            },
        }
    }

    fn recent_raw_source_state_matches_observed_swaps_bounded_probe(
        source_state: &RecentRawJournalStateRow,
        source_bounded_probe: &RecentRawObservedSwapsBoundedProbeRead,
    ) -> bool {
        source_state.covered_since == source_bounded_probe.covered_since
            && Self::recent_raw_optional_cursor_equal(
                source_state.covered_through_cursor.as_ref(),
                source_bounded_probe.covered_through_cursor.as_ref(),
            )
    }
}
