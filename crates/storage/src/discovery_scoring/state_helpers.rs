fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn cmp_swap_order(a: &SwapEvent, b: &SwapEvent) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn cmp_cursor_order(a: &DiscoveryRuntimeCursor, b: &DiscoveryRuntimeCursor) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn upsert_discovery_scoring_state_value_on_conn(
    conn: &Connection,
    state_key: &str,
    state_value: &str,
    updated_at: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at",
        params![state_key, state_value, updated_at],
    )
    .with_context(|| format!("failed upserting discovery_scoring_state.{state_key}"))?;
    Ok(())
}

fn upsert_discovery_scoring_cursor_state_on_conn(
    conn: &Connection,
    ts_key: &str,
    slot_key: &str,
    signature_key: &str,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        ts_key,
        &cursor.ts_utc.to_rfc3339(),
        updated_at,
    )?;
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        slot_key,
        &cursor.slot.to_string(),
        updated_at,
    )?;
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        signature_key,
        &cursor.signature,
        updated_at,
    )?;
    Ok(())
}

pub(crate) fn upsert_discovery_scoring_backfill_progress_on_conn(
    conn: &Connection,
    start_ts: DateTime<Utc>,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        "backfill_progress_start_ts",
        &start_ts.to_rfc3339(),
        updated_at,
    )?;
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "backfill_progress_cursor_ts",
        "backfill_progress_cursor_slot",
        "backfill_progress_cursor_signature",
        cursor,
        updated_at,
    )?;
    Ok(())
}

fn upsert_discovery_scoring_seed_boundary_install_marker_on_conn(
    conn: &Connection,
    start_ts: DateTime<Utc>,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        "seed_boundary_install_start_ts",
        &start_ts.to_rfc3339(),
        updated_at,
    )?;
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "seed_boundary_install_cursor_ts",
        "seed_boundary_install_cursor_slot",
        "seed_boundary_install_cursor_signature",
        cursor,
        updated_at,
    )?;
    Ok(())
}

fn clear_discovery_scoring_seed_boundary_install_marker_on_conn(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM discovery_scoring_state
         WHERE state_key IN (
            'seed_boundary_install_start_ts',
            'seed_boundary_install_cursor_ts',
            'seed_boundary_install_cursor_slot',
            'seed_boundary_install_cursor_signature'
         )",
        [],
    )
    .context("failed clearing discovery_scoring_state.seed_boundary_install marker")?;
    Ok(())
}

fn upsert_discovery_scoring_materialization_gap_cursor_on_conn(
    conn: &Connection,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "materialization_gap_since_ts",
        "materialization_gap_since_slot",
        "materialization_gap_since_signature",
        cursor,
        updated_at,
    )?;
    Ok(())
}

fn clear_discovery_scoring_materialization_gap_repair_target_on_conn(
    conn: &Connection,
) -> Result<()> {
    conn.execute(
        "DELETE FROM discovery_scoring_state
         WHERE state_key IN (
            'materialization_gap_repair_gap_ts',
            'materialization_gap_repair_gap_slot',
            'materialization_gap_repair_gap_signature',
            'materialization_gap_repair_target_ts',
            'materialization_gap_repair_target_slot',
            'materialization_gap_repair_target_signature'
         )",
        [],
    )
    .context("failed clearing discovery_scoring_state.materialization_gap repair target")?;
    Ok(())
}

fn load_discovery_scoring_state_value_on_conn(
    conn: &Connection,
    state_key: &str,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state_value
         FROM discovery_scoring_state
         WHERE state_key = ?1",
        params![state_key],
        |row| row.get(0),
    )
    .optional()
    .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))
}

fn load_discovery_scoring_cursor_state_exact_on_conn(
    conn: &Connection,
    ts_key: &str,
    slot_key: &str,
    signature_key: &str,
    label: &str,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    let ts_raw = load_discovery_scoring_state_value_on_conn(conn, ts_key)?;
    let slot_raw = load_discovery_scoring_state_value_on_conn(conn, slot_key)?;
    let signature = load_discovery_scoring_state_value_on_conn(conn, signature_key)?;
    match (ts_raw, slot_raw, signature) {
        (None, None, None) => Ok(None),
        (Some(ts_raw), Some(slot_raw), Some(signature)) => {
            let ts_utc = parse_ts(&ts_raw, &format!("discovery_scoring_state.{ts_key}"))?;
            let slot = slot_raw.parse::<u64>().with_context(|| {
                format!("invalid discovery_scoring_state.{slot_key} value: {slot_raw}")
            })?;
            Ok(Some(DiscoveryRuntimeCursor {
                ts_utc,
                slot,
                signature,
            }))
        }
        _ => Err(anyhow!(
            "discovery_scoring_state.{label} cursor is partially populated"
        )),
    }
}

fn load_discovery_scoring_materialization_gap_cursor_on_conn(
    conn: &Connection,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    load_discovery_scoring_cursor_state_exact_on_conn(
        conn,
        "materialization_gap_since_ts",
        "materialization_gap_since_slot",
        "materialization_gap_since_signature",
        "materialization_gap_since",
    )
}

fn load_discovery_scoring_materialization_gap_repair_target_on_conn(
    conn: &Connection,
) -> Result<Option<(DiscoveryRuntimeCursor, DiscoveryRuntimeCursor)>> {
    let gap_cursor = load_discovery_scoring_cursor_state_exact_on_conn(
        conn,
        "materialization_gap_repair_gap_ts",
        "materialization_gap_repair_gap_slot",
        "materialization_gap_repair_gap_signature",
        "materialization_gap_repair_gap",
    )?;
    let target_cursor = load_discovery_scoring_cursor_state_exact_on_conn(
        conn,
        "materialization_gap_repair_target_ts",
        "materialization_gap_repair_target_slot",
        "materialization_gap_repair_target_signature",
        "materialization_gap_repair_target",
    )?;
    match (gap_cursor, target_cursor) {
        (None, None) => Ok(None),
        (Some(gap_cursor), Some(target_cursor)) => Ok(Some((gap_cursor, target_cursor))),
        _ => Err(anyhow!(
            "discovery_scoring_state.materialization_gap_repair target is partially populated"
        )),
    }
}
