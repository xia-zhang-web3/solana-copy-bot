-- Traversal only: checkpoint before decoding/proof, never trade authorization.
CREATE TABLE execution_source_sell_staging_cursor (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    last_rowid INTEGER CHECK(last_rowid IS NULL OR typeof(last_rowid) = 'integer')
);
