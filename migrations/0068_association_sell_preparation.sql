-- object association_sell_preparations
CREATE TABLE association_sell_preparations (
    signature TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL CHECK(version=1),
    first_binding TEXT NOT NULL,
    initial_evaluation TEXT NOT NULL,
    latest_evaluation TEXT NOT NULL,
    authority TEXT NOT NULL DEFAULT 'trade_authority_none' CHECK(authority='trade_authority_none')
);
-- object association_sell_dependencies
CREATE TABLE association_sell_dependencies (
    sell_signature TEXT NOT NULL,
    anchor_signature TEXT NOT NULL,
    first_identity TEXT,
    PRIMARY KEY(sell_signature,anchor_signature)
);
-- object association_sell_dependency_anchor
CREATE INDEX association_sell_dependency_anchor ON association_sell_dependencies(anchor_signature,sell_signature);
-- object association_sell_work
CREATE TABLE association_sell_work (
    anchor_signature TEXT PRIMARY KEY NOT NULL,
    after_signature TEXT NOT NULL
);
-- object association_sell_first_immutable
CREATE TRIGGER association_sell_first_immutable BEFORE UPDATE ON association_sell_preparations
WHEN NEW.signature!=OLD.signature OR NEW.version!=OLD.version
 OR NEW.first_binding!=OLD.first_binding OR NEW.initial_evaluation!=OLD.initial_evaluation
 OR NEW.authority!=OLD.authority
BEGIN SELECT RAISE(ABORT,'immutable first SELL preparation'); END;
-- object association_sell_anchor_immutable
CREATE TRIGGER association_sell_anchor_immutable BEFORE UPDATE ON association_sell_dependencies
WHEN NEW.sell_signature!=OLD.sell_signature OR NEW.anchor_signature!=OLD.anchor_signature
 OR (OLD.first_identity IS NOT NULL AND NEW.first_identity IS NOT OLD.first_identity)
BEGIN SELECT RAISE(ABORT,'immutable first SELL anchor'); END;
-- object association_sell_bootstrap
CREATE TABLE association_sell_bootstrap (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    after_signature TEXT NOT NULL,
    complete INTEGER NOT NULL CHECK(complete IN (0,1))
);
-- object association_sell_preparation_no_delete
CREATE TRIGGER association_sell_preparation_no_delete BEFORE DELETE ON association_sell_preparations
BEGIN SELECT RAISE(ABORT,'immutable SELL preparation cannot be deleted'); END;
-- object association_sell_dependency_no_delete
CREATE TRIGGER association_sell_dependency_no_delete BEFORE DELETE ON association_sell_dependencies
BEGIN SELECT RAISE(ABORT,'immutable SELL dependency cannot be deleted'); END;
