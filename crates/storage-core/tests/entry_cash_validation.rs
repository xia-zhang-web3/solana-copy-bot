#[path = "common/entry_cash_fixture.rs"]
mod cash;
#[path = "common/entry_cost_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

#[test]
fn entry_cash_bad_attribution_is_unavailable_without_partial_success_or_repair() -> Result<()> {
    for case in [
        "missing_id",
        "wrong_token",
        "wrong_bucket",
        "missing_position",
        "outside_day",
    ] {
        let mut db = fixed()?;
        closed(&db, "old-floor", Some(-20), None, "closed", db.now)?;
        complete(&db, ORDER, 3)?;
        cash::inventory(&db.conn()?, "cash-position", "cash-mint", 3, 9, db.now)?;
        cash::settle(
            &db.store,
            &db.conn()?,
            "exec-canary:cash",
            "cash-sig",
            WALLET,
            "cash-mint",
            1,
            -4,
            db.now,
        )?;
        let day = db
            .store
            .execution_canary_sell_cash_day(db.now + Duration::seconds(1))?;
        let conn = db.conn()?;
        match case {
            "missing_id" => {
                conn.execute("UPDATE fills SET position_id='missing'", [])?;
            }
            "wrong_token" => {
                conn.execute(
                    "UPDATE positions SET token='other' WHERE position_id='cash-position'",
                    [],
                )?;
            }
            "wrong_bucket" => {
                conn.execute("UPDATE positions SET accounting_bucket='legacy' WHERE position_id='cash-position'",[])?;
            }
            "missing_position" => {
                conn.execute(
                    "DELETE FROM positions WHERE position_id='cash-position'",
                    [],
                )?;
            }
            _ => {
                conn.execute(
                    "UPDATE fills SET settlement_ts=?1",
                    [(db.now + Duration::days(1)).to_rfc3339()],
                )?;
                conn.execute("UPDATE fills SET position_id='missing'", [])?;
            }
        }
        let before = snapshot(&db)?;
        for _ in 0..2 {
            db.reopen()?;
            let v = cost(&db)?;
            assert_eq!(v.closed_loss.loss_lamports, "20");
            assert_eq!(
                v.failed_expenses.known_wallet_fee_lamports.as_deref(),
                Some("3")
            );
            assert_eq!(v.partial_known_subtotal_lamports, "23");
            assert!(v.known_total_lamports.is_none());
            assert!(v.cash_loss.additional_loss_lamports.is_none());
            assert_eq!(
                v.cash_loss.unavailable_reason.as_deref(),
                Some("cash_loss_unavailable")
            );
            assert!(v.check_cap(1.0).is_err());
            assert!(!v.selected_cost_complete());
            assert!(serde_json::to_value(v)?["known_total_lamports"].is_null());
            // Public day API still reports events independently of current positions.
            if case != "outside_day" {
                assert_eq!(
                    db.store
                        .execution_canary_sell_cash_day(db.now + Duration::seconds(1))?,
                    day
                );
            }
            assert_eq!(snapshot(&db)?, before);
        }
    }
    Ok(())
}

#[test]
fn entry_cash_duplicate_receipts_and_corrupt_claims_cannot_use_closed_floor_as_fallback(
) -> Result<()> {
    for case in [
        "duplicate",
        "duplicate_across_positions",
        "outside_day_duplicate",
        "bad_date",
        "missing_facts",
        "duplicate_order",
    ] {
        let db = fixed()?;
        cash::inventory(&db.conn()?, "lot", "cash-mint", 3, 9, db.now)?;
        cash::settle(
            &db.store,
            &db.conn()?,
            "exec-canary:first",
            "shared",
            WALLET,
            "cash-mint",
            1,
            -4,
            db.now,
        )?;
        let mut conn = db.conn()?;
        match case {
            "duplicate" | "duplicate_across_positions" | "outside_day_duplicate" => {
                let token = if case == "duplicate_across_positions" {
                    cash::inventory(&conn, "lot2", "other-mint", 1, 0, db.now)?;
                    "other-mint"
                } else {
                    "cash-mint"
                };
                let at = if case == "outside_day_duplicate" {
                    db.now + Duration::days(1)
                } else {
                    db.now
                };
                cash::settle(
                    &db.store,
                    &conn,
                    "exec-canary:second",
                    "second-independent",
                    WALLET,
                    token,
                    1,
                    -4,
                    at,
                )?;
                // Both receipts must pass the canonical writer before constructing
                // historical corruption for the reader, without changing money.
                db.store
                    .execution_canary_sell_cash_day(at + Duration::seconds(1))?;
                let tx = conn.transaction()?;
                for table in [
                    "orders",
                    "execution_canary_receipt_proofs",
                    "execution_canary_receipt_facts",
                ] {
                    assert_eq!(
                        tx.execute(
                            &format!("UPDATE {table} SET tx_signature='shared' WHERE order_id='exec-canary:second' AND tx_signature='second-independent'"),
                            [],
                        )?,
                        1,
                        "{case}: {table} historical identity"
                    );
                }
                tx.commit()?;
            }
            "bad_date" => {
                conn.execute("UPDATE fills SET settlement_ts='bad'", [])?;
            }
            "missing_facts" => {
                conn.execute("DELETE FROM execution_canary_receipt_facts", [])?;
            }
            _ => {
                // Corruption fixture: duplicate one order under a different position sort key.
                conn.execute_batch("DROP INDEX idx_fills_order_id")?;
                let cols = conn
                    .prepare("PRAGMA table_info(fills)")?
                    .query_map([], |r| r.get::<_, String>(1))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let cols = cols.into_iter().filter(|s| s != "id").collect::<Vec<_>>();
                conn.execute(
                    &format!(
                        "INSERT INTO fills({}) SELECT {} FROM fills",
                        cols.join(","),
                        cols.iter()
                            .map(|s| if s == "position_id" { "'zz-other'" } else { s })
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                    [],
                )?;
            }
        }
        let before = snapshot(&db)?;
        let error = db
            .store
            .execution_canary_sell_cash_day(db.now + Duration::seconds(1))
            .expect_err(case);
        let reason = match case {
            "bad_date" => "invalid cash settlement timestamp",
            "missing_facts" => "cash day event missing receipt facts",
            "duplicate_order" => "duplicate cash day fill order",
            _ => "duplicate wallet cash receipt claimed by multiple canary orders",
        };
        assert!(format!("{error:#}").contains(reason), "{case}: {error:#}");
        let v = cost(&db)?;
        assert!(v.known_total_lamports.is_none(), "{case}");
        assert!(v.cash_loss.additional_loss_lamports.is_none(), "{case}");
        assert_eq!(
            v.cash_loss.unavailable_reason.as_deref(),
            Some("cash_loss_unavailable")
        );
        assert!(v.check_cap(1.0).is_err());
        assert_eq!(snapshot(&db)?, before);
        eprintln!(
            "B133_READER case={case} error={error:#} cash=unavailable cap=refused no_repair=true"
        );
    }
    Ok(())
}

#[test]
fn entry_cash_preserves_existing_fatal_sqlite_message_classification() -> Result<()> {
    for marker in [
        "disk I/O error",
        "database or disk is full",
        "ordinary_missing_fixture",
    ] {
        let db = fixed()?;
        // A synthetic broken view produces a SQLite error chain containing the existing
        // classifier's fatal marker. This is not a physical I/O fault injection.
        db.conn()?.execute_batch(&format!("DROP TABLE execution_canary_receipt_facts; CREATE VIEW execution_canary_receipt_facts AS SELECT * FROM \"{marker}\""))?;
        let result = cost(&db);
        if marker == "ordinary_missing_fixture" {
            assert!(result?.known_total_lamports.is_none());
        } else {
            let error = result.unwrap_err();
            assert!(copybot_storage_core::is_fatal_sqlite_anyhow_error(&error));
            assert!(format!("{error:#}").contains(marker));
        }
    }
    Ok(())
}
