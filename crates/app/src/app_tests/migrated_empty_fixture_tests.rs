use super::*;
use rusqlite::{types::Value, Connection};
use std::{collections::BTreeMap, time::Instant};

fn logical(path: &Path) -> Result<BTreeMap<String, Vec<String>>> {
    let conn = Connection::open(path)?;
    let mut state = BTreeMap::new();
    let schema: Vec<String> = conn
        .prepare("SELECT coalesce(sql,'') FROM sqlite_master ORDER BY type,name")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    state.insert("schema".into(), schema);
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for table in tables {
        let mut query =
            conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let columns: Vec<_> = query.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = query
            .query_map([], |row| {
                let values = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, name)| table != "schema_migrations" || *name != "applied_at")
                    .map(|(i, _)| row.get::<_, Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(format!("{values:?}"))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.sort();
        state.insert(table, rows);
    }
    Ok(state)
}

#[test]
fn migrated_empty_fixture_matches_fresh_seed_and_is_independent() -> Result<()> {
    let root =
        super::super::super::temporary_output_fixture::OutputRoot::new("empty-template-check")?;
    let direct = root.path().join("direct.db");
    let mut baseline = SqliteStore::open(&direct)?;
    baseline.run_migrations(migrations())?;
    drop(baseline);
    let left = root.path().join("left.db");
    let right = root.path().join("right.db");
    let a = open(&left)?;
    let b = open(&right)?;
    assert_eq!(logical(&left)?, logical(&direct)?);
    assert_eq!(logical(&right)?, logical(&direct)?);
    for path in [&left, &right] {
        let conn = Connection::open(path)?;
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?,
            "ok"
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM schema_migrations WHERE applied_at IS NULL OR applied_at=''",
                [],
                |r| r.get::<_, u64>(0)
            )?,
            0
        );
    }
    Connection::open(&left)?.execute_batch(
        "CREATE TABLE fixture_probe(value TEXT); INSERT INTO fixture_probe VALUES('left-only');",
    )?;
    assert!(!logical(&right)?.contains_key("fixture_probe"));
    drop(a);
    drop(b);
    let reopened = SqliteStore::open(&left)?;
    drop(reopened);
    assert_eq!(
        Connection::open(&left)?.query_row("SELECT value FROM fixture_probe", [], |r| r
            .get::<_, String>(0))?,
        "left-only"
    );
    assert!(open(&right).is_err()); // Never overwrite an existing case.
    let third = root.path().join("third.db");
    let c = open(&third)?;
    drop(c);
    assert_eq!(logical(&third)?, logical(&direct)?);
    Ok(())
}

#[test]
fn migrated_empty_fixture_measures_setup_in_opt_in_matrix() -> Result<()> {
    let cases = match std::env::var("COPYBOT_FIXTURE_TIMING_CASES").as_deref() {
        Ok("32") => 32,
        _ => 1,
    };
    let root =
        super::super::super::temporary_output_fixture::OutputRoot::new("empty-template-timing")?;
    let started = Instant::now();
    for n in 0..cases {
        let path = root.path().join(format!("fresh-{n}.db"));
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(migrations())?;
        drop(store);
    }
    let direct = started.elapsed();
    let cold = Instant::now();
    let store = open(&root.path().join("first-template-copy.db"))?;
    drop(store);
    let first = cold.elapsed();
    let started = Instant::now();
    for n in 0..cases {
        let store = open(&root.path().join(format!("copy-{n}.db")))?;
        drop(store);
    }
    eprintln!(
        "fixture_setup_budget cases={cases} direct_ms={} first_template_copy_ms={} copy_ms={}",
        direct.as_millis(),
        first.as_millis(),
        started.elapsed().as_millis()
    );
    Ok(())
}
