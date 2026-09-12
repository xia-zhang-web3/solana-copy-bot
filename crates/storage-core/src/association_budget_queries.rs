//! Frozen logical formulas, with optional SQLite expression-index access paths.
pub(super) const SLOW: [&str; 3] = [
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
pub(super) const INDEXED: [&str; 3] = [
    r#"SELECT count(*),coalesce(sum(bytes),0) FROM (
      SELECT 512+length(CAST(signature AS BLOB))+length(CAST(admission AS BLOB))+length(CAST(candidate AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(terminal AS BLOB)),0) AS bytes FROM association_inbox_identities INDEXED BY b99_association_inbox_identities
      UNION ALL SELECT 512+length(CAST(session AS BLOB))+length(CAST(delivery AS BLOB)) FROM association_inbox_events INDEXED BY b99_association_inbox_events
      UNION ALL SELECT 512+length(CAST(signature AS BLOB))+length(CAST(first_binding AS BLOB))+length(CAST(initial_evaluation AS BLOB))+length(CAST(latest_evaluation AS BLOB))+length(CAST(authority AS BLOB)) FROM association_sell_preparations INDEXED BY b99_association_sell_preparations
      UNION ALL SELECT 512+2*length(CAST(sell_signature AS BLOB))+2*length(CAST(anchor_signature AS BLOB))+coalesce(length(CAST(first_identity AS BLOB)),0) FROM association_sell_dependencies INDEXED BY b99_association_sell_dependencies
      UNION ALL SELECT 512+length(CAST(anchor_signature AS BLOB))+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT max(length(CAST(d.sell_signature AS BLOB))) FROM association_sell_dependencies d INDEXED BY b99_sell_dependency_bytes WHERE d.anchor_signature=w.anchor_signature),0)) FROM association_sell_work w INDEXED BY b99_association_sell_work
      UNION ALL SELECT 512+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT max(length(CAST(signature AS BLOB))) FROM association_inbox_identities INDEXED BY b99_identity_signature_bytes),0)) FROM association_sell_bootstrap INDEXED BY b99_association_sell_bootstrap)"#,
    r#"SELECT count(*),coalesce(sum(bytes),0) FROM (
      SELECT 512+length(CAST(block_key AS BLOB))+length(CAST(first_observation AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(contradiction AS BLOB)),0) AS bytes FROM association_parent_blocks INDEXED BY b99_association_parent_blocks
      UNION ALL SELECT 512+length(CAST(block_hash AS BLOB))+length(CAST(first_slot AS BLOB))+coalesce(length(CAST(contradiction_slot AS BLOB)),0) FROM association_parent_hashes INDEXED BY b99_association_parent_hashes
      UNION ALL SELECT 512+2*length(CAST(sell_signature AS BLOB))+2*length(CAST(block_hash AS BLOB)) FROM association_parent_dependencies INDEXED BY b99_association_parent_dependencies
      UNION ALL SELECT 512+length(CAST(block_hash AS BLOB))+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT max(length(CAST(d.sell_signature AS BLOB))) FROM association_parent_dependencies d INDEXED BY b99_parent_dependency_bytes WHERE d.block_hash=w.block_hash),0)) FROM association_parent_work w INDEXED BY b99_association_parent_work)"#,
    r#"SELECT count(*),coalesce(sum(bytes),0) FROM (
         SELECT 512+length(CAST(intent_id AS BLOB))+length(CAST(signature AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(record AS BLOB)) AS bytes FROM ordered_source_sell_intents INDEXED BY b99_ordered_source_sell_intents
         UNION ALL SELECT 512+length(CAST(signature AS BLOB))+length(CAST(owner AS BLOB))+length(CAST(intent_id AS BLOB)) FROM source_sell_signature_claims INDEXED BY b99_source_sell_signature_claims WHERE owner='provider_order_strict_v1')"#,
];
