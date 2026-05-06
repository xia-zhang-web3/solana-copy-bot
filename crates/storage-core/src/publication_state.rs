use crate::observed::parse_rfc3339_utc;
use crate::{
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

pub(super) fn publication_state_query(
    conn: &rusqlite::Connection,
) -> Result<Option<DiscoveryPublicationStateRow>> {
    let raw = conn
        .query_row(
            "SELECT publication_runtime_mode, publication_reason,
                publication_last_published_at, publication_last_published_window_start,
                publication_scoring_source, publication_wallet_ids_json,
                publication_policy_fingerprint, publication_runtime_cursor_ts,
                publication_runtime_cursor_slot, publication_runtime_cursor_signature,
                updated_at
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
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
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
        Option<String>,
        Option<i64>,
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
        publication_runtime_cursor: parse_optional_publication_cursor(row.7, row.8, row.9)?,
        updated_at: parse_rfc3339_utc(&row.10, "discovery_strategy_state.updated_at")?,
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

fn parse_optional_publication_cursor(
    ts: Option<String>,
    slot: Option<i64>,
    signature: Option<String>,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    match (ts, slot, signature) {
        (None, None, None) => Ok(None),
        (Some(ts), Some(slot), Some(signature)) => {
            if slot < 0 {
                return Err(anyhow!(
                    "invalid negative publication_runtime_cursor_slot: {slot}"
                ));
            }
            Ok(Some(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts, "publication_runtime_cursor_ts")?,
                slot: slot as u64,
                signature,
            }))
        }
        _ => Err(anyhow!(
            "publication runtime cursor columns must be all set or all empty"
        )),
    }
}

fn publication_cursor_values(
    cursor: Option<&DiscoveryRuntimeCursor>,
) -> (Option<String>, Option<i64>, Option<&str>) {
    match cursor {
        Some(cursor) => (
            Some(cursor.ts_utc.to_rfc3339()),
            Some(cursor.slot as i64),
            Some(cursor.signature.as_str()),
        ),
        None => (None, None, None),
    }
}

pub(super) fn write_publication_state_on_conn(
    conn: &rusqlite::Connection,
    update: &DiscoveryPublicationStateUpdate,
    clear_published_truth: bool,
    policy_fingerprint: Option<&str>,
    publication_runtime_cursor: Option<&DiscoveryRuntimeCursor>,
) -> Result<()> {
    let wallet_ids_json = update
        .published_wallet_ids
        .as_deref()
        .map(canonical_wallet_ids_json)
        .transpose()?;
    let (cursor_ts, cursor_slot, cursor_signature) =
        publication_cursor_values(publication_runtime_cursor);
    conn.execute(
        "INSERT INTO discovery_strategy_state(
            id, publication_runtime_mode, publication_reason,
            publication_last_published_at, publication_last_published_window_start,
            publication_scoring_source, publication_wallet_ids_json,
            publication_policy_fingerprint, publication_runtime_cursor_ts,
            publication_runtime_cursor_slot, publication_runtime_cursor_signature, updated_at
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(id) DO UPDATE SET
            publication_runtime_mode = excluded.publication_runtime_mode,
            publication_reason = excluded.publication_reason,
            publication_last_published_at =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_last_published_at END,
            publication_last_published_window_start =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_last_published_window_start END,
            publication_scoring_source = excluded.publication_scoring_source,
            publication_wallet_ids_json =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_wallet_ids_json END,
            publication_policy_fingerprint =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_policy_fingerprint END,
            publication_runtime_cursor_ts =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_runtime_cursor_ts END,
            publication_runtime_cursor_slot =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_runtime_cursor_slot END,
            publication_runtime_cursor_signature =
                CASE WHEN ?12 THEN NULL ELSE excluded.publication_runtime_cursor_signature END,
            updated_at = excluded.updated_at",
        params![
            update.runtime_mode.as_str(),
            &update.reason,
            update.last_published_at.map(|ts| ts.to_rfc3339()),
            update.last_published_window_start.map(|ts| ts.to_rfc3339()),
            update.published_scoring_source.as_deref(),
            wallet_ids_json.as_deref(),
            policy_fingerprint,
            cursor_ts.as_deref(),
            cursor_slot,
            cursor_signature,
            Utc::now().to_rfc3339(),
            clear_published_truth,
        ],
    )?;
    Ok(())
}

pub(super) fn write_discovery_runtime_cursor_on_conn(
    conn: &rusqlite::Connection,
    cursor: &DiscoveryRuntimeCursor,
) -> Result<()> {
    conn.execute(
        "INSERT INTO discovery_runtime_state(
            id, cursor_ts, cursor_slot, cursor_signature, updated_at
         ) VALUES (1, ?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
            cursor_ts = excluded.cursor_ts,
            cursor_slot = excluded.cursor_slot,
            cursor_signature = excluded.cursor_signature,
            updated_at = excluded.updated_at",
        params![
            cursor.ts_utc.to_rfc3339(),
            cursor.slot as i64,
            &cursor.signature,
            Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}
