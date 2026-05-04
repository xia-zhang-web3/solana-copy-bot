fn load_observed_swaps_tail_cursor(sqlite_path: &str) -> Result<Option<DiscoveryRuntimeCursor>> {
    let conn = Connection::open_with_flags(sqlite_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| {
            format!("failed to open sqlite db read-only for observed_swaps tail: {sqlite_path}")
        })?;
    conn.busy_timeout(StdDuration::from_millis(250))
        .context("failed setting observed_swaps tail read-only busy timeout")?;
    conn.pragma_update(None, "query_only", true)
        .context("failed enabling observed_swaps tail read-only query_only mode")?;
    let cursor_raw = conn
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
        .context("failed loading observed_swaps bounded tail cursor")?;
    cursor_raw
        .map(
            |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                Ok(DiscoveryRuntimeCursor {
                    ts_utc: DateTime::parse_from_rfc3339(&ts_raw)
                        .with_context(|| {
                            format!("invalid observed_swaps tail ts rfc3339 value: {ts_raw}")
                        })?
                        .with_timezone(&Utc),
                    slot: slot_raw.max(0) as u64,
                    signature,
                })
            },
        )
        .transpose()
}

#[derive(Default)]
struct AggregateReplayProgress {
    page_count: usize,
    last_page_rows: usize,
    gap_cursor_loaded: bool,
    gap_cursor_observed: bool,
    caught_up_to_tail: bool,
    reached_repair_target: bool,
    last_replay_cursor: Option<DiscoveryRuntimeCursor>,
}

fn compare_discovery_runtime_cursors(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn aggregate_replay_cursor_before(cursor: &DiscoveryRuntimeCursor) -> DiscoveryRuntimeCursor {
    if cursor.slot > 0 {
        return DiscoveryRuntimeCursor {
            ts_utc: cursor.ts_utc,
            slot: cursor.slot - 1,
            signature: String::new(),
        };
    }

    DiscoveryRuntimeCursor {
        ts_utc: cursor
            .ts_utc
            .checked_sub_signed(ChronoDuration::nanoseconds(1))
            .unwrap_or(cursor.ts_utc),
        slot: u64::MAX,
        signature: String::new(),
    }
}
