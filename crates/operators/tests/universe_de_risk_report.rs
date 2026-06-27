use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use copybot_operators::universe_de_risk_report::{build_report, Cli};
use rusqlite::{params, Connection};
use std::path::Path;
use tempfile::NamedTempFile;

const SOL: &str = "So11111111111111111111111111111111111111112";

#[test]
fn reports_slow_survivable_copyable_population_by_dex() -> Result<()> {
    let db = setup_db()?;
    let since = ts(2026, 6, 27, 8, 0, 0);
    let until = ts(2026, 6, 27, 12, 0, 0);
    seed_round_trips(
        db.path(),
        "raydium",
        "slow_wallet",
        "slow_token",
        since,
        5,
        1_200,
        1.0,
        1.2,
    )?;
    seed_round_trips(
        db.path(),
        "pumpswap",
        "fast_wallet",
        "fast_token",
        since,
        5,
        60,
        1.0,
        1.1,
    )?;

    let report = build_report(
        Cli {
            db_path: Some(db.path().to_path_buf()),
            json: true,
            since: Some(since),
            until: Some(until),
            max_rows: 100,
            min_wallet_trades: 5,
            min_wallet_hold_seconds: 900,
            fresh_hours: 2,
            ..Cli::default()
        },
        until,
    );

    assert_eq!(report.reason_class, "universe_de_risk_report_loaded");
    let safety = report.safety.as_ref().expect("safety must be present");
    assert!(safety.read_only);
    assert!(safety.query_only);
    assert_eq!(safety.rows_seen, 20);
    assert!(
        safety
            .query_plan
            .iter()
            .any(|line| line.contains("idx_observed_swaps_sol_leg_ts_slot_signature")),
        "query plan should use SOL-leg partial index: {:?}",
        safety.query_plan
    );

    let summary = report.summary.as_ref().expect("summary must be present");
    assert_eq!(summary.totals.tokens, 2);
    assert_eq!(summary.totals.copyable_wallets, 1);
    assert_eq!(summary.totals.unmatched_sell_events, 0);
    assert_eq!(summary.concentration.eligible_wallets, 1);
    assert_eq!(summary.concentration.copyable_token_count, 1);
    assert_eq!(
        summary.concentration.top_wallet_share_of_positive_pnl,
        Some(1.0)
    );
    assert!(summary.verdict.contains("SOL-leg slice only"));
    let raydium = summary
        .by_dex
        .iter()
        .find(|row| row.dex == "raydium")
        .expect("raydium dex summary");
    assert_eq!(raydium.copyable_wallets, 1);
    assert_eq!(raydium.round_trips, 5);
    assert_eq!(raydium.buys, 5);
    assert_eq!(raydium.sells, 5);
    assert_eq!(raydium.median_hold_seconds, Some(1_200));
    assert!(raydium.leader_pnl_sol > 0.9);
    assert_eq!(raydium.survival_proxy_token_rate, Some(1.0));

    let pumpswap = summary
        .by_dex
        .iter()
        .find(|row| row.dex == "pumpswap")
        .expect("pumpswap dex summary");
    assert_eq!(pumpswap.copyable_wallets, 0);
    assert_eq!(pumpswap.median_hold_seconds, Some(60));
    assert!(summary
        .caveats
        .iter()
        .any(|line| line.contains("USDC-paired")));
    assert!(summary
        .caveats
        .iter()
        .any(|line| line.contains("unmatched_sell_events")));
    Ok(())
}

#[test]
fn marks_max_row_cap_without_writing() -> Result<()> {
    let db = setup_db()?;
    let since = ts(2026, 6, 27, 8, 0, 0);
    let until = ts(2026, 6, 27, 9, 0, 0);
    insert_swap(
        db.path(),
        "raydium",
        "sig-buy",
        "wallet",
        SOL,
        "token",
        1.0,
        100.0,
        1,
        since,
    )?;
    insert_swap(
        db.path(),
        "raydium",
        "sig-sell",
        "wallet",
        "token",
        SOL,
        100.0,
        1.1,
        2,
        since + chrono::Duration::minutes(20),
    )?;

    let report = build_report(
        Cli {
            db_path: Some(db.path().to_path_buf()),
            json: true,
            since: Some(since),
            until: Some(until),
            max_rows: 1,
            ..Cli::default()
        },
        until,
    );

    let safety = report.safety.as_ref().expect("safety must be present");
    assert!(safety.max_rows_exhausted);
    assert_eq!(safety.rows_seen, 1);
    assert_eq!(
        count_swaps(db.path())?,
        2,
        "read-only report must not mutate observed_swaps"
    );
    Ok(())
}

#[test]
fn exposes_left_truncated_sells_as_unmatched() -> Result<()> {
    let db = setup_db()?;
    let since = ts(2026, 6, 27, 8, 0, 0);
    let until = ts(2026, 6, 27, 9, 0, 0);
    insert_swap(
        db.path(),
        "raydium",
        "sig-left-truncated-sell",
        "wallet",
        "token",
        SOL,
        100.0,
        1.1,
        2,
        since + chrono::Duration::minutes(20),
    )?;

    let report = build_report(
        Cli {
            db_path: Some(db.path().to_path_buf()),
            json: true,
            since: Some(since),
            until: Some(until),
            max_rows: 10,
            ..Cli::default()
        },
        until,
    );

    let summary = report.summary.as_ref().expect("summary must be present");
    assert_eq!(summary.totals.unmatched_sell_events, 1);
    assert_eq!(summary.totals.round_trips, 0);
    assert_eq!(summary.totals.leader_pnl_sol, 0.0);
    Ok(())
}

#[test]
fn token_concentration_share_uses_distinct_copyable_wallets() -> Result<()> {
    let db = setup_db()?;
    let since = ts(2026, 6, 27, 8, 0, 0);
    let until = ts(2026, 6, 27, 14, 0, 0);
    seed_round_trips(
        db.path(),
        "raydium",
        "multi_token_wallet",
        "token_a",
        since,
        5,
        1_200,
        1.0,
        1.2,
    )?;
    seed_round_trips(
        db.path(),
        "raydium",
        "multi_token_wallet",
        "token_b",
        since + chrono::Duration::minutes(5),
        5,
        1_200,
        1.0,
        1.2,
    )?;

    let report = build_report(
        Cli {
            db_path: Some(db.path().to_path_buf()),
            json: true,
            since: Some(since),
            until: Some(until),
            max_rows: 100,
            min_wallet_trades: 5,
            min_wallet_hold_seconds: 900,
            ..Cli::default()
        },
        until,
    );

    let summary = report.summary.as_ref().expect("summary must be present");
    assert_eq!(summary.totals.copyable_wallets, 1);
    assert_eq!(summary.concentration.copyable_token_count, 2);
    assert_eq!(summary.concentration.top_token_copyable_wallets, Some(1));
    assert_eq!(
        summary.concentration.top_token_copyable_wallet_share,
        Some(1.0),
        "one wallet spread across tokens must not dilute top-token concentration"
    );
    Ok(())
}

#[test]
fn token_concentration_share_uses_per_token_copyable_wallet_population() -> Result<()> {
    let db = setup_db()?;
    let since = ts(2026, 6, 27, 8, 0, 0);
    let until = ts(2026, 6, 27, 14, 0, 0);
    seed_round_trips(
        db.path(),
        "raydium",
        "mixed_wallet",
        "winning_token",
        since,
        5,
        1_200,
        1.0,
        1.2,
    )?;
    seed_round_trips(
        db.path(),
        "raydium",
        "mixed_wallet",
        "losing_token",
        since + chrono::Duration::minutes(5),
        5,
        1_200,
        1.0,
        0.4,
    )?;

    let report = build_report(
        Cli {
            db_path: Some(db.path().to_path_buf()),
            json: true,
            since: Some(since),
            until: Some(until),
            max_rows: 100,
            min_wallet_trades: 5,
            min_wallet_hold_seconds: 900,
            ..Cli::default()
        },
        until,
    );

    let summary = report.summary.as_ref().expect("summary must be present");
    assert_eq!(
        summary.totals.copyable_wallets, 0,
        "wallet is globally negative and must not be globally copyable"
    );
    assert_eq!(summary.concentration.copyable_token_count, 1);
    assert_eq!(summary.concentration.top_token_copyable_wallets, Some(1));
    assert_eq!(
        summary.concentration.top_token_copyable_wallet_share,
        Some(1.0),
        "token share denominator must use per-token-copyable wallet population"
    );
    Ok(())
}

fn setup_db() -> Result<NamedTempFile> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    conn.execute_batch(
        "
        CREATE TABLE observed_swaps (
            signature TEXT NOT NULL,
            wallet_id TEXT NOT NULL,
            dex TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            qty_in REAL NOT NULL,
            qty_out REAL NOT NULL,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX idx_observed_swaps_sol_leg_ts_slot_signature
        ON observed_swaps(ts, slot, signature)
        WHERE token_in = 'So11111111111111111111111111111111111111112'
           OR token_out = 'So11111111111111111111111111111111111111112';
        ",
    )?;
    Ok(db)
}

fn seed_round_trips(
    path: &Path,
    dex: &str,
    wallet: &str,
    token: &str,
    start: DateTime<Utc>,
    count: usize,
    hold_seconds: i64,
    buy_cost: f64,
    sell_proceeds: f64,
) -> Result<()> {
    for index in 0..count {
        let buy_ts = start + chrono::Duration::minutes((index * 30) as i64);
        let sell_ts = buy_ts + chrono::Duration::seconds(hold_seconds);
        insert_swap(
            path,
            dex,
            &format!("{dex}-{wallet}-{index}-buy"),
            wallet,
            SOL,
            token,
            buy_cost,
            100.0,
            index as i64 * 2 + 1,
            buy_ts,
        )?;
        insert_swap(
            path,
            dex,
            &format!("{dex}-{wallet}-{index}-sell"),
            wallet,
            token,
            SOL,
            100.0,
            sell_proceeds,
            index as i64 * 2 + 2,
            sell_ts,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_swap(
    path: &Path,
    dex: &str,
    signature: &str,
    wallet: &str,
    token_in: &str,
    token_out: &str,
    qty_in: f64,
    qty_out: f64,
    slot: i64,
    ts: DateTime<Utc>,
) -> Result<()> {
    let conn = Connection::open(path)?;
    conn.execute(
        "INSERT INTO observed_swaps (
            signature, wallet_id, dex, token_in, token_out,
            qty_in, qty_out, slot, ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            signature,
            wallet,
            dex,
            token_in,
            token_out,
            qty_in,
            qty_out,
            slot,
            ts.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn count_swaps(path: &Path) -> Result<i64> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))?)
}

fn ts(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid timestamp")
}
