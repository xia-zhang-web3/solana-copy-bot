-- Only the protected runner may widen the owner EXIT raw quantity domain.
-- Existing USDC/decimals/identity/immutability checks and incoming FKs survive.
SELECT enabled FROM temp.owner_exit_amount_rebuild_guard;
