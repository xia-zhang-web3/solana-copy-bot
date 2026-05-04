fn read_discovery_wallet_freshness_capture_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DiscoveryWalletFreshnessCaptureRow> {
    let capture_id: i64 = row.get(0)?;
    let captured_at_raw: String = row.get(1)?;
    let recent_cycles_raw: i64 = row.get(2)?;
    let verdict: String = row.get(3)?;
    let reason: String = row.get(4)?;
    let publication_age_seconds_raw: Option<i64> = row.get(5)?;
    let raw_truth_sufficient: i64 = row.get(6)?;
    let raw_truth_reason: String = row.get(7)?;
    let shadow_signal_verdict: String = row.get(8)?;
    let shadow_signal_reason: String = row.get(9)?;
    let published_wallet_ids_json: String = row.get(10)?;
    let active_follow_wallet_ids_json: String = row.get(11)?;
    let current_raw_top_wallet_ids_json: String = row.get(12)?;
    let audit_json: String = row.get(13)?;
    let shadow_signal_json: String = row.get(14)?;
    let captured_at = parse_rfc3339_utc(
        &captured_at_raw,
        "discovery_wallet_freshness_history.captured_at",
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
        )
    })?;
    let published_wallet_ids =
        parse_wallet_ids_json(published_wallet_ids_json, "published_wallet_ids_json").map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Text,
                    Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
                )
            },
        )?;
    let active_follow_wallet_ids = parse_wallet_ids_json(
        active_follow_wallet_ids_json,
        "active_follow_wallet_ids_json",
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            11,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
        )
    })?;
    let current_raw_top_wallet_ids = parse_wallet_ids_json(
        current_raw_top_wallet_ids_json,
        "current_raw_top_wallet_ids_json",
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
        )
    })?;
    Ok(DiscoveryWalletFreshnessCaptureRow {
        capture_id,
        captured_at,
        recent_cycles: recent_cycles_raw.max(1) as usize,
        verdict,
        reason,
        publication_age_seconds: publication_age_seconds_raw.map(|value| value.max(0) as u64),
        raw_truth_sufficient: raw_truth_sufficient != 0,
        raw_truth_reason,
        shadow_signal_verdict,
        shadow_signal_reason,
        published_wallet_ids,
        active_follow_wallet_ids,
        current_raw_top_wallet_ids,
        audit_json,
        shadow_signal_json,
    })
}
