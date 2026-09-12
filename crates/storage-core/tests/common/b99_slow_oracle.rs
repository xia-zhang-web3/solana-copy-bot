// Original accepted97 SQL, frozen before implementation. Do not use candidate queries.
use anyhow::Result;
use rusqlite::Connection;
pub fn usage(c: &Connection, strict: bool) -> Result<(usize, usize)> {
    let mut total = (0usize, 0usize);
    for sql in &QUERIES[..if strict { 3 } else { 2 }] {
        let (n, b): (i64, i64) =
            c.query_row(&table_scan(sql), [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        total.0 = total.0.checked_add(n.try_into()?).unwrap();
        total.1 = total.1.checked_add(b.try_into()?).unwrap();
    }
    Ok(total)
}
pub const QUERIES: [&str; 3] = [
    r#"SELECT count(*),coalesce(sum(bytes),0) FROM (
      SELECT 512+length(CAST(signature AS BLOB))+length(CAST(admission AS BLOB))+length(CAST(candidate AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(terminal AS BLOB)),0) AS bytes FROM association_inbox_identities
      UNION ALL SELECT 512+length(CAST(session AS BLOB))+length(CAST(delivery AS BLOB)) FROM association_inbox_events
      UNION ALL SELECT 512+length(CAST(signature AS BLOB))+length(CAST(first_binding AS BLOB))+length(CAST(initial_evaluation AS BLOB))+length(CAST(latest_evaluation AS BLOB))+length(CAST(authority AS BLOB)) FROM association_sell_preparations
      UNION ALL SELECT 512+2*length(CAST(sell_signature AS BLOB))+2*length(CAST(anchor_signature AS BLOB))+coalesce(length(CAST(first_identity AS BLOB)),0) FROM association_sell_dependencies
      UNION ALL SELECT 512+length(CAST(anchor_signature AS BLOB))+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT max(length(CAST(d.sell_signature AS BLOB))) FROM association_sell_dependencies d WHERE d.anchor_signature=w.anchor_signature),0)) FROM association_sell_work w
      UNION ALL SELECT 512+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT max(length(CAST(signature AS BLOB))) FROM association_inbox_identities),0)) FROM association_sell_bootstrap)"#,
    r#"SELECT count(*),coalesce(sum(bytes),0) FROM (
      SELECT 512+length(CAST(block_key AS BLOB))+length(CAST(first_observation AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(contradiction AS BLOB)),0) AS bytes FROM association_parent_blocks
      UNION ALL SELECT 512+length(CAST(block_hash AS BLOB))+length(CAST(first_slot AS BLOB))+coalesce(length(CAST(contradiction_slot AS BLOB)),0) FROM association_parent_hashes
      UNION ALL SELECT 512+2*length(CAST(sell_signature AS BLOB))+2*length(CAST(block_hash AS BLOB)) FROM association_parent_dependencies
      UNION ALL SELECT 512+length(CAST(block_hash AS BLOB))+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT max(length(CAST(d.sell_signature AS BLOB))) FROM association_parent_dependencies d WHERE d.block_hash=w.block_hash),0)) FROM association_parent_work w)"#,
    r#"SELECT count(*),coalesce(sum(bytes),0) FROM (
         SELECT 512+length(CAST(intent_id AS BLOB))+length(CAST(signature AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(record AS BLOB)) AS bytes FROM ordered_source_sell_intents
         UNION ALL SELECT 512+length(CAST(signature AS BLOB))+length(CAST(owner AS BLOB))+length(CAST(intent_id AS BLOB)) FROM source_sell_signature_claims WHERE owner='provider_order_strict_v1')"#,
];

// Force source rows, so an optimizer cannot make the oracle reuse candidate indexes.
fn table_scan(sql: &str) -> String {
    let mut result = sql.to_owned();
    for table in [
        "association_inbox_identities",
        "association_inbox_events",
        "association_sell_preparations",
        "association_sell_dependencies",
        "association_sell_work",
        "association_sell_bootstrap",
        "association_parent_blocks",
        "association_parent_hashes",
        "association_parent_dependencies",
        "association_parent_work",
        "ordered_source_sell_intents",
        "source_sell_signature_claims",
    ] {
        result = result.replace(
            &format!("FROM {table} d WHERE"),
            &format!("FROM {table} d NOT INDEXED WHERE"),
        );
        result = result.replace(
            &format!("FROM {table} w"),
            &format!("FROM {table} w NOT INDEXED"),
        );
        // Only unaliased occurrences remain; aliases start with a space and letter.
        for suffix in ["\n", ")", " WHERE"] {
            result = result.replace(
                &format!("FROM {table}{suffix}"),
                &format!("FROM {table} NOT INDEXED{suffix}"),
            );
        }
    }
    result
}
