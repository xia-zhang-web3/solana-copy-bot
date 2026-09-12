use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use rusqlite::{params, Connection};

const SOL: &str = "So11111111111111111111111111111111111111112";
const TOKEN: &str = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";

fn fixture() -> Result<(
    tempfile::TempDir,
    Connection,
    SqliteDiscoveryStore,
    DateTime<Utc>,
)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prices.db");
    let store = SqliteDiscoveryStore::open(&path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);
    let conn = Connection::open(&path)?;
    let reader = SqliteDiscoveryStore::open_read_only(&path)?;
    let now = DateTime::parse_from_rfc3339("2026-09-07T12:00:00+00:00")?.with_timezone(&Utc);
    Ok((dir, conn, reader, now))
}

fn insert(
    conn: &Connection,
    signature: &str,
    slot: i64,
    ts: &str,
    buy: bool,
    sol: f64,
    token: f64,
) -> Result<()> {
    let (input, output, qty_in, qty_out) = if buy {
        (SOL, TOKEN, sol, token)
    } else {
        (TOKEN, SOL, token, sol)
    };
    conn.execute("INSERT INTO observed_swaps(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts)
        VALUES (?1,'wallet','fixture',?2,?3,?4,?5,?6,?7)", params![signature,input,output,qty_in,qty_out,slot,ts])?;
    Ok(())
}

#[test]
fn b35_reader_preserves_both_sol_orientations_and_scalar_results() -> Result<()> {
    for buy in [true, false] {
        let (_dir, conn, reader, now) = fixture()?;
        let observed = now - Duration::minutes(186);
        insert(
            &conn,
            "old-observation",
            42,
            &observed.to_rfc3339(),
            buy,
            2.0,
            8.0,
        )?;
        let value = reader
            .latest_token_sol_price_observation(TOKEN, now)?
            .unwrap();
        assert_eq!(value.price_sol, 0.25);
        assert_eq!(value.observed_at, observed);
        assert_eq!(value.signature, "old-observation");
        assert_eq!(value.slot, 42);
        assert_eq!(reader.latest_token_sol_price(TOKEN, now)?, Some(0.25));
        assert_eq!(
            reader.latest_token_sol_price_observation(TOKEN, now + Duration::minutes(10))?,
            Some(value.clone())
        );
        println!(
            "B35 storage orientation_buy={buy} observation={}",
            serde_json::to_string(&value)?
        );
    }
    Ok(())
}

#[test]
fn b35_reader_stable_ties_do_not_depend_on_insertion_order() -> Result<()> {
    for reverse in [false, true] {
        let (_dir, conn, reader, now) = fixture()?;
        let mut rows = vec![
            ("z", 9, false, 0.2),
            ("a", 10, true, 0.3),
            ("z-high", 10, false, 0.4),
        ];
        if reverse {
            rows.reverse();
        }
        for (sig, slot, buy, sol) in rows {
            insert(&conn, sig, slot, &now.to_rfc3339(), buy, sol, 1.0)?;
        }
        for _ in 0..3 {
            let price = reader
                .latest_token_sol_price_observation(TOKEN, now)?
                .unwrap();
            assert_eq!(
                (price.signature.as_str(), price.slot, price.price_sol),
                ("z-high", 10, 0.4)
            );
            assert_eq!(price.observed_at, now);
        }
    }
    Ok(())
}

#[test]
fn b35_reader_exact_as_of_nanoseconds_and_future_exclusion() -> Result<()> {
    for millis in [0, 120] {
        let (_dir, conn, reader, base) = fixture()?;
        let now = base + Duration::milliseconds(millis);
        insert(&conn, "boundary", 1, &now.to_rfc3339(), true, 0.1, 1.0)?;
        insert(
            &conn,
            "future",
            2,
            &(now + Duration::nanoseconds(1)).to_rfc3339(),
            false,
            0.9,
            1.0,
        )?;
        let price = reader
            .latest_token_sol_price_observation(TOKEN, now)?
            .unwrap();
        assert_eq!(price.signature, "boundary");
        assert_eq!(price.observed_at, now);
        assert!(reader
            .latest_token_sol_price_observation(TOKEN, now - Duration::nanoseconds(1))?
            .is_none());
    }
    Ok(())
}

#[test]
fn b35_reader_missing_invalid_timestamp_identity_and_nonfinite_are_not_prices() -> Result<()> {
    let (_dir, _conn, reader, now) = fixture()?;
    assert!(reader
        .latest_token_sol_price_observation(TOKEN, now)?
        .is_none());
    assert!(reader.latest_token_sol_price(TOKEN, now)?.is_none());
    for ts in [
        "",
        "not-a-timestamp",
        "2026-09-31T12:00:00+00:00",
        "2026-09-07T12:00:00Z",
    ] {
        let (_dir, conn, reader, now) = fixture()?;
        insert(&conn, "invalid", 1, ts, true, 1.0, 10.0)?;
        assert!(
            reader
                .latest_token_sol_price_observation(TOKEN, now)
                .is_err(),
            "{ts}"
        );
        assert!(reader.latest_token_sol_price(TOKEN, now).is_err());
    }
    for (signature, slot) in [("", 1), ("negative-slot", -1)] {
        let (_dir, conn, reader, now) = fixture()?;
        insert(&conn, signature, slot, &now.to_rfc3339(), true, 1.0, 10.0)?;
        assert!(reader
            .latest_token_sol_price_observation(TOKEN, now)
            .is_err());
        assert_eq!(
            reader.latest_token_sol_price(TOKEN, now)?,
            Some(0.1),
            "scalar identity policy is unchanged"
        );
    }
    for sol in [0.0, -1.0, f64::INFINITY] {
        let (_dir, conn, reader, now) = fixture()?;
        insert(
            &conn,
            "invalid-price",
            1,
            &now.to_rfc3339(),
            true,
            sol,
            10.0,
        )?;
        assert!(reader
            .latest_token_sol_price_observation(TOKEN, now)?
            .is_none());
        assert!(reader.latest_token_sol_price(TOKEN, now)?.is_none());
    }
    Ok(())
}

#[test]
fn b35_reader_query_uses_existing_pair_indexes_without_mutation() -> Result<()> {
    let (dir, conn, reader, now) = fixture()?;
    insert(&conn, "row", 1, &now.to_rfc3339(), true, 1.0, 10.0)?;
    let source = include_str!("../../src/market_price.rs");
    let sql = source
        .split("const LATEST_PRICE_SQL: &str = r#\"")
        .nth(1)
        .unwrap()
        .split("\"#;")
        .next()
        .unwrap();
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
    let plan = stmt
        .query_map(params![SOL, TOKEN, now.to_rfc3339()], |row| {
            row.get::<_, String>(3)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let searches = plan
        .iter()
        .filter(|line| {
            line.contains("SEARCH observed_swaps USING INDEX")
                && line.contains("token_in")
                && line.contains("token_out")
                && line.contains("ts")
        })
        .count();
    assert_eq!(searches, 2, "{plan:?}");
    assert!(
        !plan.iter().any(|line| line.contains("SCAN observed_swaps")),
        "{plan:?}"
    );
    let before = std::fs::read(dir.path().join("prices.db"))?;
    assert!(reader
        .latest_token_sol_price_observation(TOKEN, now)?
        .is_some());
    assert_eq!(before, std::fs::read(dir.path().join("prices.db"))?);
    println!("B35 PRICE_QUERY_PLAN {}", serde_json::to_string(&plan)?);
    Ok(())
}
