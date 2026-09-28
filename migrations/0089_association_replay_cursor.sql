-- object association_replay_cursor
CREATE TABLE association_replay_cursor (
    id INTEGER PRIMARY KEY NOT NULL CHECK(id=1),
    scope TEXT NOT NULL,
    head TEXT
);
-- object association_replay_scope_immutable
CREATE TRIGGER association_replay_scope_immutable BEFORE UPDATE ON association_replay_cursor
WHEN NEW.id!=OLD.id OR NEW.scope!=OLD.scope
 OR (OLD.head IS NOT NULL AND NEW.head IS NULL)
 OR (OLD.head IS NOT NULL AND json_extract(NEW.head,'$.block.observation.child.slot') < json_extract(OLD.head,'$.block.observation.child.slot'))
 OR (OLD.head IS NOT NULL AND json_extract(NEW.head,'$.block.observation.child.slot') = json_extract(OLD.head,'$.block.observation.child.slot') AND json_extract(NEW.head,'$.block.observation.child.hash') != json_extract(OLD.head,'$.block.observation.child.hash'))
BEGIN SELECT RAISE(ABORT,'immutable replay scope and nonregressing anchor'); END;
-- object association_replay_cursor_no_delete
CREATE TRIGGER association_replay_cursor_no_delete BEFORE DELETE ON association_replay_cursor
BEGIN SELECT RAISE(ABORT,'replay cursor cannot be deleted'); END;
