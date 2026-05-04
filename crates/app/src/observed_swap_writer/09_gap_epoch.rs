#[derive(Default)]
struct DiscoveryAggregateGapRepairEpoch {
    gap_cursor: Option<DiscoveryRuntimeCursor>,
    repair_target_cursor: Option<DiscoveryRuntimeCursor>,
    gap_cursor_observed: bool,
    resume_after_cursor: Option<DiscoveryRuntimeCursor>,
    repair_resume_source: Option<&'static str>,
    reconstructed_gap_row_observed: bool,
    persisted_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
}

impl DiscoveryAggregateGapRepairEpoch {
    fn reset(&mut self) {
        self.gap_cursor = None;
        self.repair_target_cursor = None;
        self.gap_cursor_observed = false;
        self.resume_after_cursor = None;
        self.repair_resume_source = None;
        self.reconstructed_gap_row_observed = false;
        self.persisted_covered_through_cursor = None;
    }

    fn reset_for_gap(
        &mut self,
        gap_cursor: DiscoveryRuntimeCursor,
        repair_target_cursor: Option<DiscoveryRuntimeCursor>,
        resume_decision: DiscoveryAggregateRepairResumeDecision,
    ) {
        self.gap_cursor = Some(gap_cursor);
        self.repair_target_cursor = repair_target_cursor;
        self.gap_cursor_observed = resume_decision.gap_cursor_observed;
        self.resume_after_cursor = resume_decision.resume_after_cursor;
        self.repair_resume_source = Some(resume_decision.repair_resume_source);
        self.reconstructed_gap_row_observed = resume_decision.reconstructed_gap_row_observed;
        self.persisted_covered_through_cursor = resume_decision.persisted_covered_through_cursor;
    }

    fn matches_gap(&self, gap_cursor: &DiscoveryRuntimeCursor) -> bool {
        self.gap_cursor.as_ref().is_some_and(|current| {
            compare_discovery_runtime_cursors(current, gap_cursor) == std::cmp::Ordering::Equal
        })
    }
}

struct DiscoveryAggregateRepairResumeDecision {
    resume_after_cursor: Option<DiscoveryRuntimeCursor>,
    gap_cursor_observed: bool,
    repair_resume_source: &'static str,
    reconstructed_gap_row_observed: bool,
    persisted_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
}

fn discovery_aggregate_repair_resume_decision(
    sqlite_path: &str,
    store: &SqliteStore,
    gap_cursor: Option<&DiscoveryRuntimeCursor>,
) -> Result<DiscoveryAggregateRepairResumeDecision> {
    let persisted_covered_through_cursor = store.load_discovery_scoring_covered_through_cursor()?;
    if let (Some(gap_cursor), Some(covered_through_cursor)) =
        (gap_cursor, persisted_covered_through_cursor.as_ref())
    {
        if compare_discovery_runtime_cursors(covered_through_cursor, gap_cursor)
            == std::cmp::Ordering::Greater
        {
            if observed_swap_exact_cursor_exists(sqlite_path, gap_cursor)? {
                return Ok(DiscoveryAggregateRepairResumeDecision {
                    resume_after_cursor: Some(covered_through_cursor.clone()),
                    gap_cursor_observed: true,
                    repair_resume_source:
                        DISCOVERY_AGGREGATE_REPAIR_RESUME_SOURCE_PERSISTED_COVERED_THROUGH,
                    reconstructed_gap_row_observed: true,
                    persisted_covered_through_cursor,
                });
            }
            warn!(
                materialization_gap_ts = %gap_cursor.ts_utc,
                materialization_gap_slot = gap_cursor.slot,
                materialization_gap_signature = %gap_cursor.signature,
                persisted_covered_through_ts = %covered_through_cursor.ts_utc,
                persisted_covered_through_slot = covered_through_cursor.slot,
                persisted_covered_through_signature = %covered_through_cursor.signature,
                "discovery aggregate repair found persisted coverage after materialization gap but exact gap row is absent; not reconstructing gap proof",
            );
        }
    }

    Ok(DiscoveryAggregateRepairResumeDecision {
        resume_after_cursor: None,
        gap_cursor_observed: false,
        repair_resume_source: DISCOVERY_AGGREGATE_REPAIR_RESUME_SOURCE_GAP_CURSOR,
        reconstructed_gap_row_observed: false,
        persisted_covered_through_cursor,
    })
}

fn discovery_aggregate_repair_target_for_gap(
    sqlite_path: &str,
    store: &SqliteStore,
    gap_cursor: Option<&DiscoveryRuntimeCursor>,
) -> Result<(Option<DiscoveryRuntimeCursor>, Option<&'static str>)> {
    let Some(gap_cursor) = gap_cursor else {
        return Ok((None, None));
    };
    if let Some((target_gap_cursor, target_cursor)) =
        store.load_discovery_scoring_materialization_gap_repair_target()?
    {
        if compare_discovery_runtime_cursors(&target_gap_cursor, gap_cursor)
            == std::cmp::Ordering::Equal
        {
            return Ok((
                Some(target_cursor),
                Some(DISCOVERY_AGGREGATE_REPAIR_TARGET_SOURCE_PERSISTED),
            ));
        }
        store.clear_discovery_scoring_materialization_gap_repair_target()?;
    }

    let target_cursor = load_observed_swaps_tail_cursor(sqlite_path)?.ok_or_else(|| {
        anyhow!(
            "cannot freeze aggregate materialization repair target because observed_swaps tail is unavailable"
        )
    })?;
    store.set_discovery_scoring_materialization_gap_repair_target(gap_cursor, &target_cursor)?;
    Ok((
        Some(target_cursor),
        Some(DISCOVERY_AGGREGATE_REPAIR_TARGET_SOURCE_NEWLY_FROZEN),
    ))
}

fn observed_swap_exact_cursor_exists(
    sqlite_path: &str,
    cursor: &DiscoveryRuntimeCursor,
) -> Result<bool> {
    let conn = Connection::open_with_flags(sqlite_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| {
            format!(
                "failed to open sqlite db read-only for observed_swaps exact cursor: {sqlite_path}"
            )
        })?;
    conn.busy_timeout(StdDuration::from_millis(250))
        .context("failed setting observed_swaps exact cursor read-only busy timeout")?;
    conn.pragma_update(None, "query_only", true)
        .context("failed enabling observed_swaps exact cursor read-only query_only mode")?;
    let slot = i64::try_from(cursor.slot).with_context(|| {
        format!(
            "observed_swaps exact cursor slot overflows i64: {}",
            cursor.slot
        )
    })?;
    let found = conn
        .query_row(
            "SELECT 1
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             WHERE ts = ?1 AND slot = ?2 AND signature = ?3
             LIMIT 1",
            rusqlite::params![cursor.ts_utc.to_rfc3339(), slot, cursor.signature.as_str()],
            |_| Ok(()),
        )
        .optional()
        .context("failed loading observed_swaps exact materialization gap row")?
        .is_some();
    Ok(found)
}
