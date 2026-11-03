//! Read-only causal proof from the actual closed BOT child; no business row edits.
use anyhow::{ensure, Context, Result};
use copybot_storage_core::{PriorBotReceiptExitBinding, SqliteStore};
use sha2::{Digest, Sha256};

#[test]
#[ignore = "requires pinned current closed BOT snapshot; no network or signer"]
fn closed_child_witness_passes_and_unsigned_failed_parent_does_not() -> Result<()> {
    let input = std::env::var("COPYBOT_CLOSED_CHILD_DB")?;
    let bytes = std::fs::read(&input)?;
    ensure!(format!("{:x}", Sha256::digest(&bytes)) == std::env::var("COPYBOT_CLOSED_CHILD_DB_SHA256")?, "snapshot pin");
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("closed.sqlite");
    std::fs::write(&path, &bytes)?;
    let store = SqliteStore::open_read_only(&path)?;
    let source = "4msXfR3vwhiEhfNAAJDUzAW9Sz54xEWJ3yfajR48Z7mp64NJ5Dpe9EAqv6rEotCx3NDYcrmck82o8YxToNnmwmL1";
    let intent = store.load_owner_exit_intent(&format!("bot-receipt-child:{source}"))?.unwrap();
    let binding = PriorBotReceiptExitBinding {
        exit_intent_id: intent.intent_id, buy_order_id: intent.buy_order_id,
        buy_receipt_signature: intent.buy_receipt_signature, position_id: intent.position_id,
        wallet_pubkey: intent.wallet, mint: intent.mint, amount_raw: intent.amount_raw,
        decimals: intent.decimals,
    };
    assert!(store.prior_bot_receipt_exit_closed(&binding).context("closed child witness")?);
    let mut parent = binding.clone();
    parent.exit_intent_id = format!("bot-receipt-exit:{source}");
    assert!(!matches!(store.prior_bot_receipt_exit_closed(&parent), Ok(true)));
    let mut changed = binding.clone();
    changed.amount_raw += 1;
    assert!(!store.prior_bot_receipt_exit_closed(&changed)?);
    let mut foreign = binding.clone();
    foreign.exit_intent_id = "bot-receipt-child:foreign".into();
    assert!(!matches!(store.prior_bot_receipt_exit_closed(&foreign), Ok(true)));
    let order = format!("exec-canary:owner-exit:{}", binding.exit_intent_id);
    // Negative fixtures only: remove one proof from a private copy, then open RO.
    // No successful receipt/result is inserted or modified in any source DB.
    for (index, sql) in [
        "DELETE FROM fills WHERE order_id=?1",
        "UPDATE owner_exit_fee_reservations SET state='pending',actual_fee=NULL,reconciled_at=NULL WHERE order_id=?1",
        "UPDATE orders SET status='execution_canary_submitted',confirm_ts=NULL WHERE order_id=?1",
    ].into_iter().enumerate() {
        let negative = tmp.path().join(format!("refused-{index}.sqlite"));
        std::fs::write(&negative, &bytes)?;
        let db = rusqlite::Connection::open(&negative)?;
        assert_eq!(db.execute(sql, [&order])?, 1);
        db.close().map_err(|(_, error)| error)?;
        let reader = SqliteStore::open_read_only(&negative)?;
        assert!(!matches!(reader.prior_bot_receipt_exit_closed(&binding), Ok(true)), "{sql}");
    }
    assert!(store.is_read_only());
    assert_eq!(std::fs::read(path)?, bytes);
    assert_eq!(std::fs::read(input)?, bytes);
    Ok(())
}
