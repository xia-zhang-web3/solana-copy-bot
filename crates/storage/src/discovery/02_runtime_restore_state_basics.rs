fn discovery_bootstrap_degraded_state_query(
    conn: &Connection,
) -> Result<DiscoveryBootstrapDegradedStateRow> {
    let row = conn
        .query_row(
            "SELECT
                bootstrap_degraded_active,
                bootstrap_degraded_reason,
                bootstrap_degraded_armed_at
             FROM discovery_strategy_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .context("failed reading discovery bootstrap-degraded state")?;
    let Some((active, reason, armed_at_raw)) = row else {
        return Ok(DiscoveryBootstrapDegradedStateRow::default());
    };
    Ok(DiscoveryBootstrapDegradedStateRow {
        active: active != 0,
        reason,
        armed_at: parse_optional_rfc3339_utc(
            armed_at_raw,
            "discovery_strategy_state.bootstrap_degraded_armed_at",
        )?,
    })
}

fn parse_optional_runtime_cursor(
    ts_raw: Option<String>,
    slot_raw: Option<i64>,
    signature: Option<String>,
    field_prefix: &str,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    match (ts_raw, slot_raw, signature) {
        (None, None, None) => Ok(None),
        (Some(ts_raw), Some(slot_raw), Some(signature)) => Ok(Some(DiscoveryRuntimeCursor {
            ts_utc: parse_rfc3339_utc(&ts_raw, &format!("{field_prefix}_ts"))?,
            slot: slot_raw.max(0) as u64,
            signature,
        })),
        _ => Err(anyhow::anyhow!(
            "incomplete discovery runtime cursor payload for {field_prefix}"
        )),
    }
}
