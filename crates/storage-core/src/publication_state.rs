fn publication_state_query(
    conn: &rusqlite::Connection,
) -> Result<Option<DiscoveryPublicationStateRow>> {
    let raw = conn
        .query_row(
            "SELECT publication_runtime_mode, publication_reason,
                publication_last_published_at, publication_last_published_window_start,
                publication_scoring_source, publication_wallet_ids_json,
                publication_policy_fingerprint, updated_at
             FROM discovery_strategy_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()?;
    raw.map(row_to_publication_state).transpose()
}

fn row_to_publication_state(
    row: (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    ),
) -> Result<DiscoveryPublicationStateRow> {
    Ok(DiscoveryPublicationStateRow {
        runtime_mode: DiscoveryRuntimeMode::parse(&row.0)?,
        reason: row.1,
        last_published_at: parse_optional_ts(row.2, "publication_last_published_at")?,
        last_published_window_start: parse_optional_ts(
            row.3,
            "publication_last_published_window_start",
        )?,
        published_scoring_source: row.4,
        published_wallet_ids: row
            .5
            .map(|raw| serde_json::from_str(&raw).context("invalid publication wallet ids json"))
            .transpose()?,
        publication_policy_fingerprint: row.6,
        updated_at: parse_rfc3339_utc(&row.7, "discovery_strategy_state.updated_at")?,
    })
}

fn parse_optional_ts(raw: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    raw.map(|value| parse_rfc3339_utc(&value, field))
        .transpose()
}

fn canonical_wallet_ids_json(wallet_ids: &[String]) -> Result<String> {
    let mut ids = wallet_ids.to_vec();
    ids.sort();
    ids.dedup();
    serde_json::to_string(&ids).context("failed serializing publication wallet ids")
}
