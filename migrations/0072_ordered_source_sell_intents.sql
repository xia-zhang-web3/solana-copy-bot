-- Versioned timeless intents are deliberately outside all legacy loaders/queues.
-- Durable claims survive intent retention. No historical backfill or rebinding.
-- Canonical identity is shadow:<signature>:<wallet>:sell:<token>, five nonempty
-- colon-separated components. Insert claims cover both storage facades. Delete
-- claims also preserve pre-0072 orphans during normal retention, without backfill.
-- The readback after each claim insert rejects even trigger-injected IGNORE.
-- object source_sell_signature_claims
CREATE TABLE source_sell_signature_claims (
    signature TEXT PRIMARY KEY NOT NULL CHECK(length(trim(signature))>0),
    owner TEXT NOT NULL CHECK(owner IN ('legacy','provider_order_strict_v1')),
    intent_id TEXT NOT NULL UNIQUE CHECK(intent_id='source-sell:'||signature)
);
-- object source_sell_claim_no_update
CREATE TRIGGER source_sell_claim_no_update BEFORE UPDATE ON source_sell_signature_claims
BEGIN SELECT RAISE(ABORT,'immutable SELL claim'); END;
-- object source_sell_claim_no_delete
CREATE TRIGGER source_sell_claim_no_delete BEFORE DELETE ON source_sell_signature_claims
BEGIN SELECT RAISE(ABORT,'durable SELL claim'); END;
-- object source_sell_claim_no_replace
CREATE TRIGGER source_sell_claim_no_replace BEFORE INSERT ON source_sell_signature_claims
WHEN EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=NEW.signature OR intent_id=NEW.intent_id)
BEGIN SELECT RAISE(ABORT,'SELL claim already exists'); END;
-- object ordered_source_sell_intents
CREATE TABLE ordered_source_sell_intents (
    intent_id TEXT PRIMARY KEY NOT NULL,
    signature TEXT NOT NULL UNIQUE CHECK(intent_id='source-sell:'||signature),
    version INTEGER NOT NULL CHECK(version=1),
    policy TEXT NOT NULL CHECK(policy='provider_order_strict_v1'),
    record TEXT NOT NULL CHECK(json_valid(record))
);
-- object ordered_source_sell_no_update
CREATE TRIGGER ordered_source_sell_no_update BEFORE UPDATE ON ordered_source_sell_intents
BEGIN SELECT RAISE(ABORT,'immutable ordered SELL intent'); END;
-- object ordered_source_sell_no_delete
CREATE TRIGGER ordered_source_sell_no_delete BEFORE DELETE ON ordered_source_sell_intents
BEGIN SELECT RAISE(ABORT,'durable ordered SELL intent'); END;
-- object ordered_source_sell_no_replace
CREATE TRIGGER ordered_source_sell_no_replace BEFORE INSERT ON ordered_source_sell_intents
WHEN EXISTS(SELECT 1 FROM ordered_source_sell_intents WHERE signature=NEW.signature OR intent_id=NEW.intent_id)
BEGIN SELECT RAISE(ABORT,'ordered SELL intent already exists'); END;
-- object source_sell_legacy_claim_insert
CREATE TRIGGER source_sell_legacy_claim_insert AFTER INSERT ON execution_source_sell_intents
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT NEW.event_signature,'legacy','source-sell:'||NEW.event_signature
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=NEW.event_signature);
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=NEW.event_signature AND owner='legacy' AND intent_id=NEW.intent_id)
        THEN RAISE(ABORT,'legacy SELL claim conflict or ignored') END;
END;
-- object source_sell_legacy_claim_delete
CREATE TRIGGER source_sell_legacy_claim_delete BEFORE DELETE ON execution_source_sell_intents
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT OLD.event_signature,'legacy','source-sell:'||OLD.event_signature
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=OLD.event_signature);
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=OLD.event_signature AND owner='legacy' AND intent_id=OLD.intent_id)
        THEN RAISE(ABORT,'legacy SELL retention claim conflict or ignored') END;
END;
-- object source_sell_signal_claim_insert
CREATE TRIGGER source_sell_signal_claim_insert AFTER INSERT ON copy_signals
WHEN NEW.signal_id GLOB 'shadow:?*:?*:sell:?*'
    AND length(NEW.signal_id)-length(replace(NEW.signal_id,':',''))=4
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1),'legacy','source-sell:'||substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1)
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1));
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1) AND owner='legacy')
        THEN RAISE(ABORT,'canonical SELL claim conflict or ignored') END;
END;
-- object source_sell_signal_claim_delete
CREATE TRIGGER source_sell_signal_claim_delete BEFORE DELETE ON copy_signals
WHEN OLD.signal_id GLOB 'shadow:?*:?*:sell:?*'
    AND length(OLD.signal_id)-length(replace(OLD.signal_id,':',''))=4
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1),'legacy','source-sell:'||substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1)
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1));
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1) AND owner='legacy')
        THEN RAISE(ABORT,'canonical SELL claim conflict or ignored') END;
END;
-- object source_sell_order_claim_insert
CREATE TRIGGER source_sell_order_claim_insert AFTER INSERT ON orders
WHEN NEW.signal_id GLOB 'shadow:?*:?*:sell:?*'
    AND length(NEW.signal_id)-length(replace(NEW.signal_id,':',''))=4
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1),'legacy','source-sell:'||substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1)
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1));
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1) AND owner='legacy')
        THEN RAISE(ABORT,'canonical SELL claim conflict or ignored') END;
END;
-- object source_sell_order_claim_delete
CREATE TRIGGER source_sell_order_claim_delete BEFORE DELETE ON orders
WHEN OLD.signal_id GLOB 'shadow:?*:?*:sell:?*'
    AND length(OLD.signal_id)-length(replace(OLD.signal_id,':',''))=4
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1),'legacy','source-sell:'||substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1)
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1));
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1) AND owner='legacy')
        THEN RAISE(ABORT,'canonical SELL claim conflict or ignored') END;
END;
-- object source_sell_promotion_claim_insert
CREATE TRIGGER source_sell_promotion_claim_insert AFTER INSERT ON execution_source_sell_promotions
WHEN NEW.signal_id GLOB 'shadow:?*:?*:sell:?*'
    AND length(NEW.signal_id)-length(replace(NEW.signal_id,':',''))=4
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1),'legacy','source-sell:'||substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1)
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1));
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=substr(NEW.signal_id,8,instr(substr(NEW.signal_id,8),':')-1) AND owner='legacy')
        THEN RAISE(ABORT,'canonical SELL claim conflict or ignored') END;
END;
-- object source_sell_promotion_claim_delete
CREATE TRIGGER source_sell_promotion_claim_delete BEFORE DELETE ON execution_source_sell_promotions
WHEN OLD.signal_id GLOB 'shadow:?*:?*:sell:?*'
    AND length(OLD.signal_id)-length(replace(OLD.signal_id,':',''))=4
BEGIN
    INSERT INTO source_sell_signature_claims(signature,owner,intent_id)
        SELECT substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1),'legacy','source-sell:'||substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1)
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1));
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_signature_claims
        WHERE signature=substr(OLD.signal_id,8,instr(substr(OLD.signal_id,8),':')-1) AND owner='legacy')
        THEN RAISE(ABORT,'canonical SELL claim conflict or ignored') END;
END;
