use anyhow::Result;
use chrono::{TimeZone, Utc};
use copybot_operators::leader_copyability_report::{build_report, Cli};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn leader_copyability_report_marks_small_samples_underpowered() -> Result<()> {
    let db = seed_db(&[
        ("w01", 10.0, 1.0, 5, 5),
        ("w02", 8.0, 0.5, 5, 5),
        ("w03", 6.0, -0.2, 5, 5),
        ("w04", 4.0, 0.1, 5, 5),
    ])?;
    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");

    assert_eq!(summary.correlation.eligible_wallets, 4);
    assert!(summary.correlation.verdict.contains("underpowered"));
    assert!(summary
        .caveats
        .iter()
        .any(|note| note.contains("partly tautological")));
    Ok(())
}

#[test]
fn candidate_lists_require_trade_eligibility() -> Result<()> {
    let db = seed_db(&[
        ("w01", 10.0, -1.0, 5, 5),
        ("w02", 9.0, -0.5, 5, 5),
        ("w03", 8.0, 0.4, 5, 5),
        ("w04", 7.0, 0.3, 5, 5),
        ("w05", 6.0, 0.2, 5, 5),
        ("w06", 5.0, 0.1, 5, 5),
        ("w07", 4.0, 0.2, 5, 5),
        ("w08", 3.0, 0.1, 5, 5),
        ("w09", 2.0, 0.4, 5, 5),
        ("w10", 1.0, 0.3, 5, 5),
        ("thin", 11.0, -5.0, 5, 1),
    ])?;
    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");

    assert!(summary.correlation.eligible_wallets >= 8);
    assert!(summary
        .high_leader_low_copyability
        .iter()
        .any(|row| row.wallet_id == "w01"));
    assert!(!summary
        .high_leader_low_copyability
        .iter()
        .any(|row| row.wallet_id == "thin"));
    Ok(())
}

#[test]
fn leader_pnl_uses_observed_swaps_fifo_when_close_facts_are_empty() -> Result<()> {
    let db = seed_db(&[
        ("w01", 10.0, 1.0, 5, 5),
        ("w02", 8.0, 0.5, 5, 5),
        ("w03", 6.0, -0.2, 5, 5),
        ("w04", 4.0, 0.1, 5, 5),
        ("w05", 3.0, 0.2, 5, 5),
        ("w06", 2.0, 0.1, 5, 5),
        ("w07", 1.0, 0.2, 5, 5),
        ("w08", 0.5, 0.1, 5, 5),
    ])?;
    let conn = Connection::open(db.path())?;
    conn.execute("DELETE FROM wallet_scoring_close_facts", [])?;
    drop(conn);

    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");
    let first = summary
        .wallets
        .iter()
        .find(|row| row.wallet_id == "w01")
        .expect("seeded wallet should be reported");

    assert_eq!(summary.correlation.eligible_wallets, 8);
    assert_eq!(first.leader_trades, 5);
    assert_eq!(first.leader_pnl_sol, 10.0);
    assert!(summary
        .caveats
        .iter()
        .any(|note| note.contains("observed_swaps")));
    Ok(())
}

fn test_cli(path: &std::path::Path) -> Cli {
    Cli {
        db_path: Some(path.to_path_buf()),
        since: Some(Utc.with_ymd_and_hms(2026, 6, 23, 0, 0, 0).unwrap()),
        until: Some(Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap()),
        limit: 50,
        per_wallet_limit: 50_000,
        min_leader_trades: 5,
        min_follower_trades: 5,
        deadline_seconds: 30,
        ..Cli::default()
    }
}

fn seed_db(rows: &[(&str, f64, f64, usize, usize)]) -> Result<NamedTempFile> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    for (rank, (wallet, leader_pnl, follower_pnl, leader_trades, follower_trades)) in
        rows.iter().enumerate()
    {
        seed_wallet(
            &conn,
            wallet,
            rank as i64,
            *leader_pnl,
            *follower_pnl,
            *leader_trades,
            *follower_trades,
        )?;
    }
    drop(conn);
    Ok(db)
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE followlist(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL,
            added_at TEXT NOT NULL,
            removed_at TEXT,
            reason TEXT,
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE UNIQUE INDEX idx_followlist_one_active_wallet
            ON followlist(wallet_id) WHERE active = 1;
        CREATE TABLE wallet_metrics(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL,
            window_start TEXT NOT NULL,
            pnl REAL NOT NULL DEFAULT 0,
            win_rate REAL NOT NULL DEFAULT 0,
            trades INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            hold_median_seconds INTEGER NOT NULL DEFAULT 0,
            score REAL NOT NULL DEFAULT 0,
            buy_total INTEGER NOT NULL DEFAULT 0,
            tradable_ratio REAL NOT NULL DEFAULT 0.0,
            rug_ratio REAL NOT NULL DEFAULT 1.0
        );
        CREATE INDEX idx_wallet_metrics_window_start ON wallet_metrics(window_start);
        CREATE TABLE wallet_scoring_close_facts(
            sell_signature TEXT NOT NULL,
            segment_index INTEGER NOT NULL,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            closed_ts TEXT NOT NULL,
            activity_day TEXT NOT NULL,
            pnl_sol REAL NOT NULL,
            hold_seconds INTEGER NOT NULL,
            win INTEGER NOT NULL,
            PRIMARY KEY(sell_signature, segment_index)
        );
        CREATE INDEX idx_wallet_scoring_close_facts_wallet_ts
            ON wallet_scoring_close_facts(wallet_id, closed_ts);
        CREATE TABLE observed_swaps(
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            dex TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            qty_in REAL NOT NULL,
            qty_out REAL NOT NULL,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX idx_observed_swaps_wallet_ts ON observed_swaps(wallet_id, ts);
        CREATE TABLE shadow_closed_trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            pnl_sol REAL NOT NULL,
            close_context TEXT NOT NULL,
            closed_ts TEXT NOT NULL
        );
        CREATE INDEX idx_shadow_closed_trades_wallet_closed_ts
            ON shadow_closed_trades(wallet_id, closed_ts);
        ",
    )?;
    Ok(())
}

fn seed_wallet(
    conn: &Connection,
    wallet: &str,
    rank: i64,
    leader_pnl: f64,
    follower_pnl: f64,
    leader_trades: usize,
    follower_trades: usize,
) -> Result<()> {
    conn.execute(
        "INSERT INTO followlist(wallet_id, added_at, active) VALUES (?1, ?2, 1)",
        params![wallet, "2026-06-23T00:00:00+00:00"],
    )?;
    conn.execute(
        "INSERT INTO wallet_metrics(
            wallet_id, window_start, pnl, win_rate, trades, closed_trades,
            hold_median_seconds, score, buy_total, tradable_ratio, rug_ratio
        ) VALUES (?1, ?2, ?3, 0.5, ?4, ?4, 120, ?5, ?4, 1.0, 0.0)",
        params![
            wallet,
            "2026-06-23T00:00:00+00:00",
            leader_pnl,
            leader_trades as i64,
            1000.0 - rank as f64
        ],
    )?;
    seed_leader_swaps(conn, wallet, leader_pnl, leader_trades)?;
    for idx in 0..follower_trades {
        conn.execute(
            "INSERT INTO shadow_closed_trades(wallet_id, token, pnl_sol, close_context, closed_ts)
             VALUES (?1, ?2, ?3, 'market', ?4)",
            params![
                wallet,
                format!("{wallet}:token:{idx}"),
                follower_pnl / follower_trades as f64,
                format!("2026-06-23T01:{idx:02}:00+00:00")
            ],
        )?;
    }
    Ok(())
}

fn seed_leader_swaps(
    conn: &Connection,
    wallet: &str,
    leader_pnl: f64,
    leader_trades: usize,
) -> Result<()> {
    if leader_trades == 0 {
        return Ok(());
    }
    let pnl_per_trade = leader_pnl / leader_trades as f64;
    for idx in 0..leader_trades {
        let token = format!("{wallet}:token:{idx}");
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, ?2, 'pump', ?3, ?4, 1.0, 1000.0, ?5, ?6)",
            params![
                format!("{wallet}:leader:buy:{idx}"),
                wallet,
                SOL_MINT,
                token,
                idx as i64 * 2,
                format!("2026-06-23T00:{idx:02}:00+00:00")
            ],
        )?;
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, ?2, 'pump', ?3, ?4, 1000.0, ?5, ?6, ?7)",
            params![
                format!("{wallet}:leader:sell:{idx}"),
                wallet,
                token,
                SOL_MINT,
                1.0 + pnl_per_trade,
                idx as i64 * 2 + 1,
                format!("2026-06-23T00:{idx:02}:30+00:00")
            ],
        )?;
    }
    Ok(())
}
