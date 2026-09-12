-- object b99_association_inbox_identities
CREATE INDEX b99_association_inbox_identities ON association_inbox_identities(512+length(CAST(signature AS BLOB))+length(CAST(admission AS BLOB))+length(CAST(candidate AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(terminal AS BLOB)),0));
-- object b99_association_inbox_events
CREATE INDEX b99_association_inbox_events ON association_inbox_events(512+length(CAST(session AS BLOB))+length(CAST(delivery AS BLOB)));
-- object b99_association_sell_preparations
CREATE INDEX b99_association_sell_preparations ON association_sell_preparations(512+length(CAST(signature AS BLOB))+length(CAST(first_binding AS BLOB))+length(CAST(initial_evaluation AS BLOB))+length(CAST(latest_evaluation AS BLOB))+length(CAST(authority AS BLOB)));
-- object b99_association_sell_dependencies
CREATE INDEX b99_association_sell_dependencies ON association_sell_dependencies(512+2*length(CAST(sell_signature AS BLOB))+2*length(CAST(anchor_signature AS BLOB))+coalesce(length(CAST(first_identity AS BLOB)),0));
-- object b99_association_parent_blocks
CREATE INDEX b99_association_parent_blocks ON association_parent_blocks(512+length(CAST(block_key AS BLOB))+length(CAST(first_observation AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(contradiction AS BLOB)),0));
-- object b99_association_parent_hashes
CREATE INDEX b99_association_parent_hashes ON association_parent_hashes(512+length(CAST(block_hash AS BLOB))+length(CAST(first_slot AS BLOB))+coalesce(length(CAST(contradiction_slot AS BLOB)),0));
-- object b99_association_parent_dependencies
CREATE INDEX b99_association_parent_dependencies ON association_parent_dependencies(512+2*length(CAST(sell_signature AS BLOB))+2*length(CAST(block_hash AS BLOB)));
-- object b99_ordered_source_sell_intents
CREATE INDEX b99_ordered_source_sell_intents ON ordered_source_sell_intents(512+length(CAST(intent_id AS BLOB))+length(CAST(signature AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(record AS BLOB)));
-- object b99_source_sell_signature_claims
CREATE INDEX b99_source_sell_signature_claims ON source_sell_signature_claims(512+length(CAST(signature AS BLOB))+length(CAST(owner AS BLOB))+length(CAST(intent_id AS BLOB))) WHERE owner='provider_order_strict_v1';
-- object b99_association_sell_work
CREATE INDEX b99_association_sell_work ON association_sell_work(length(CAST(anchor_signature AS BLOB)),length(CAST(after_signature AS BLOB)),anchor_signature);
-- object b99_association_parent_work
CREATE INDEX b99_association_parent_work ON association_parent_work(length(CAST(block_hash AS BLOB)),length(CAST(after_signature AS BLOB)),block_hash);
-- object b99_association_sell_bootstrap
CREATE INDEX b99_association_sell_bootstrap ON association_sell_bootstrap(length(CAST(after_signature AS BLOB)));
-- object b99_identity_signature_bytes
CREATE INDEX b99_identity_signature_bytes ON association_inbox_identities(length(CAST(signature AS BLOB)));
-- object b99_sell_dependency_bytes
CREATE INDEX b99_sell_dependency_bytes ON association_sell_dependencies(anchor_signature,length(CAST(sell_signature AS BLOB)));
-- object b99_parent_dependency_bytes
CREATE INDEX b99_parent_dependency_bytes ON association_parent_dependencies(block_hash,length(CAST(sell_signature AS BLOB)));
