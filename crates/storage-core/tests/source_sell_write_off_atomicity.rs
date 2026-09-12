#[path = "common/source_write_off_db.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use fixture::*;

#[test]
fn mark_close_affected_rows_and_commit_errors_rollback_both_parts() -> Result<()> {
    for kind in kinds() {
        for (fault, sql, cause) in [
            ("mark-abort", "CREATE TRIGGER injected BEFORE UPDATE OF err_code ON orders BEGIN SELECT RAISE(ABORT,'mark_failure'); END;", "mark_failure"),
            ("mark-ignore", "CREATE TRIGGER injected BEFORE UPDATE OF err_code ON orders BEGIN SELECT RAISE(IGNORE); END;", "updated 0 rows"),
            ("close-abort", "CREATE TRIGGER injected BEFORE UPDATE OF state ON positions BEGIN SELECT RAISE(ABORT,'close_failure'); END;", "close_failure"),
            ("close-ignore", "CREATE TRIGGER injected BEFORE UPDATE OF state ON positions BEGIN SELECT RAISE(IGNORE); END;", "updated 0 rows"),
            ("between", "CREATE TRIGGER injected AFTER UPDATE OF err_code ON orders BEGIN UPDATE positions SET qty=qty+1; END;", "exact position changed"),
            ("post-mark", "CREATE TRIGGER injected AFTER UPDATE OF err_code ON orders BEGIN UPDATE orders SET client_order_id='wrong' WHERE order_id=NEW.order_id; END;", "terminal mark changed unexpectedly"),
            ("post-close", "CREATE TRIGGER injected AFTER UPDATE OF state ON positions BEGIN DELETE FROM positions WHERE position_id=NEW.position_id; END;", "position missing after close"),
            ("commit", "CREATE TABLE injected_parent(id TEXT PRIMARY KEY); CREATE TABLE injected_child(id TEXT REFERENCES injected_parent(id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TRIGGER injected AFTER UPDATE OF state ON positions BEGIN INSERT INTO injected_child VALUES('absent'); END;", "FOREIGN KEY constraint failed"),
        ] {
            let mut db=Db::new(kind,TokenQuantity::new(if matches!(kind,Kind::DustNoRoute) {1} else {7000},3))?;
            db.conn()?.execute_batch(sql)?;
            let before=snapshot(&db.conn()?, &[])?;
            let error=db.run(kind).expect_err(fault);
            assert!(format!("{error:#}").contains(cause),"{fault}: {error:#}");
            assert_eq!(snapshot(&db.conn()?, &[])?,before,"{fault}");
            db.reopen()?;
            assert_eq!(snapshot(&db.conn()?, &[])?,before,"{fault} reopen");
            db.conn()?.execute_batch("DROP TRIGGER injected;")?;
            assert!(matches!(db.run(kind)?,Outcome::WrittenOff {..}),"{fault} retry");
        }
    }
    Ok(())
}
