-- Only the protected runner may extend the immutable child ledger identity.
SELECT enabled FROM temp.bot_receipt_child_rebuild_guard;
