-- object association_parent_blocks
CREATE TABLE association_parent_blocks (
    block_key TEXT PRIMARY KEY NOT NULL,
    first_observation TEXT NOT NULL,
    first_session TEXT NOT NULL,
    first_sequence INTEGER NOT NULL,
    contradiction TEXT
);
-- object association_parent_hashes
CREATE TABLE association_parent_hashes (
    block_hash TEXT PRIMARY KEY NOT NULL,
    first_slot TEXT NOT NULL,
    contradiction_slot TEXT
);
-- object association_parent_dependencies
CREATE TABLE association_parent_dependencies (
    sell_signature TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    PRIMARY KEY(sell_signature,block_hash)
);
-- object association_parent_dependency_hash
CREATE INDEX association_parent_dependency_hash ON association_parent_dependencies(block_hash,sell_signature);
-- object association_parent_work
CREATE TABLE association_parent_work (
    block_hash TEXT PRIMARY KEY NOT NULL,
    after_signature TEXT NOT NULL,
    pending INTEGER NOT NULL CHECK(pending IN (0,1))
);
-- object association_parent_first_immutable
CREATE TRIGGER association_parent_first_immutable BEFORE UPDATE ON association_parent_blocks
WHEN NEW.block_key!=OLD.block_key OR NEW.first_observation!=OLD.first_observation
 OR NEW.first_session!=OLD.first_session OR NEW.first_sequence!=OLD.first_sequence
 OR (OLD.contradiction IS NOT NULL AND NEW.contradiction IS NOT OLD.contradiction)
BEGIN SELECT RAISE(ABORT,'immutable parent observation/conflict'); END;
-- object association_parent_hash_immutable
CREATE TRIGGER association_parent_hash_immutable BEFORE UPDATE ON association_parent_hashes
WHEN NEW.block_hash!=OLD.block_hash OR NEW.first_slot!=OLD.first_slot
 OR (OLD.contradiction_slot IS NOT NULL AND NEW.contradiction_slot IS NOT OLD.contradiction_slot)
BEGIN SELECT RAISE(ABORT,'immutable parent hash slot/conflict'); END;
-- object association_parent_block_no_delete
CREATE TRIGGER association_parent_block_no_delete BEFORE DELETE ON association_parent_blocks
BEGIN SELECT RAISE(ABORT,'parent evidence cannot be deleted'); END;
-- object association_parent_hash_no_delete
CREATE TRIGGER association_parent_hash_no_delete BEFORE DELETE ON association_parent_hashes
BEGIN SELECT RAISE(ABORT,'parent hash evidence cannot be deleted'); END;
-- object association_parent_dependency_no_delete
CREATE TRIGGER association_parent_dependency_no_delete BEFORE DELETE ON association_parent_dependencies
BEGIN SELECT RAISE(ABORT,'parent dependency cannot be deleted'); END;
-- object association_parent_dependency_immutable
CREATE TRIGGER association_parent_dependency_immutable BEFORE UPDATE ON association_parent_dependencies
BEGIN SELECT RAISE(ABORT,'parent dependency immutable'); END;
-- object association_parent_work_no_delete
CREATE TRIGGER association_parent_work_no_delete BEFORE DELETE ON association_parent_work
BEGIN SELECT RAISE(ABORT,'reserved parent work cannot be deleted'); END;
-- object association_parent_pending
CREATE INDEX association_parent_pending ON association_parent_work(pending,block_hash);
