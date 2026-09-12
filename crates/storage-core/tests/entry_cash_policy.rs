#[path = "common/entry_cash_fixture.rs"]
mod cash;
#[path = "common/entry_cost_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

fn sell(db: &Db, id: &str, token: &str, native: i128, yesterday: bool) -> Result<()> {
    let at = db.now
        - if yesterday {
            Duration::days(1)
        } else {
            Duration::zero()
        };
    cash::settle(
        &db.store,
        &db.conn()?,
        &format!("exec-canary:{id}"),
        id,
        "cash-wallet",
        token,
        1,
        native,
        at,
    )?;
    Ok(())
}

#[test]
fn entry_cash_five_per_position_floor_cases_with_fee_and_exact_cap_neighbors() -> Result<()> {
    for (case, expected, closed_expected, gross) in [
        ("open", 7, 0, 7),
        ("net", 9, 6, 9),
        ("larger_floor", 20, 20, 7),
        ("different_positions", 27, 20, 7),
        ("prior_day_floor", 25, 25, 5),
    ] {
        for fees in ["none", "known", "unknown", "mixed"] {
            let mut db = fixed()?;
            let c = db.conn()?;
            if matches!(case, "open" | "net") {
                cash::inventory(&c, "position", "cash-mint", 3, 9, db.now)?;
                sell(&db, "a", "cash-mint", -4, false)?;
                if case == "net" {
                    sell(&db, "b", "cash-mint", 6, false)?;
                    sell(&db, "c", "cash-mint", 1, false)?;
                }
            } else {
                cash::inventory(
                    &c,
                    "old-position",
                    "old-mint",
                    2,
                    0,
                    db.now - Duration::days(1),
                )?;
                sell(
                    &db,
                    "a",
                    "old-mint",
                    if case == "larger_floor" { -13 } else { -20 },
                    true,
                )?;
                sell(
                    &db,
                    "b",
                    "old-mint",
                    match case {
                        "larger_floor" => -7,
                        "prior_day_floor" => -5,
                        _ => 0,
                    },
                    false,
                )?;
                if case == "different_positions" {
                    cash::inventory(&c, "position", "cash-mint", 3, 9, db.now)?;
                    sell(&db, "c", "cash-mint", -4, false)?;
                }
            }
            if matches!(fees, "known" | "mixed") {
                complete(&db, ORDER, 3)?;
            }
            if matches!(fees, "unknown" | "mixed") {
                db.add(
                    "exec-canary:unknown-fee",
                    "unknown-signature",
                    "buy",
                    db.now,
                )?;
                db.detect("exec-canary:unknown-fee", "signature_status")?;
            }
            let total = expected
                + if matches!(fees, "known" | "mixed") {
                    3
                } else {
                    0
                };
            let before = snapshot(&db)?;
            for _ in 0..2 {
                db.reopen()?;
                let v = cost(&db)?;
                assert_eq!(v.known_total()?, total, "{case}/{fees}");
                assert_eq!(v.closed_loss.loss_lamports, closed_expected.to_string());
                assert_eq!(
                    v.cash_loss.day_gross_negative_lamports.as_deref(),
                    Some(gross.to_string().as_str())
                );
                assert_eq!(
                    v.cash_loss.additional_loss_lamports.as_deref(),
                    Some((expected - closed_expected).to_string().as_str())
                );
                assert!(v.check_cap(total as f64 / 1e9)?.exhausted);
                assert!(v.check_cap((total - 1) as f64 / 1e9)?.exhausted);
                assert!(!v.check_cap((total + 1) as f64 / 1e9)?.exhausted);
                let day = db
                    .store
                    .execution_canary_sell_cash_day(db.now + Duration::seconds(1))?;
                assert_eq!(
                    day.known_events.gross_negative_cash_result_lamports,
                    gross.to_string()
                );
                assert!(v.economic_pnl_lamports.is_none());
                assert!(serde_json::to_value(&v)?["known_total_lamports"].is_string());
                if matches!(fees, "unknown" | "mixed") {
                    assert!(!v.selected_cost_complete());
                }
                assert_eq!(snapshot(&db)?, before);
            }
        }
    }
    Ok(())
}

#[test]
fn entry_cash_recovery_orphan_events_and_wide_signed_loss_stay_exact() -> Result<()> {
    let db = fixed()?;
    for (i, delta) in [i64::MIN, i64::MIN].into_iter().enumerate() {
        let token = format!("wide-{i}");
        cash::inventory(
            &db.conn()?,
            &format!("exec-canary-pos:recovery-orphan:{i}"),
            &token,
            1,
            0,
            db.now,
        )?;
        sell(&db, &format!("wide-{i}"), &token, i128::from(delta), false)?;
    }
    let v = cost(&db)?;
    assert_eq!(v.closed_loss.positions, 0);
    assert_eq!(v.known_total()?, 1_u128 << 64);
    assert_eq!(
        v.cash_loss.additional_loss_lamports.as_deref(),
        Some("18446744073709551616")
    );
    assert_eq!(
        serde_json::to_value(v)?["known_total_lamports"],
        "18446744073709551616"
    );
    Ok(())
}

#[test]
fn entry_cash_overlap_preserves_legacy_rounding_null_and_closed_cutoff() -> Result<()> {
    for (raw, legacy, closed_at, expected_floor, expected_total) in [
        (None, Some(-20e-9), 0, 20, 20),
        (None, None, 0, 0, 7),
        (Some(-20), Some(1e20), 0, 20, 20),
        (Some(-20), None, 1, 20, 20),
        (Some(-20), None, -1, 0, 7),
    ] {
        let db = fixed()?;
        cash::inventory(&db.conn()?, "lot", "cash-mint", 1, 0, db.now)?;
        sell(&db, "loss", "cash-mint", -7, false)?;
        // Imported legacy/closed metadata control; the original cash fill stays intact.
        db.conn()?.execute(
            "UPDATE positions SET pnl_lamports=?1,pnl_sol=?2,closed_ts=?3",
            rusqlite::params![
                raw,
                legacy,
                (db.now + Duration::days(closed_at)).to_rfc3339()
            ],
        )?;
        let v = cost(&db)?;
        assert_eq!(v.closed_loss.loss_lamports, expected_floor.to_string());
        assert_eq!(v.known_total()?, expected_total);
    }
    Ok(())
}
