UPDATE orders
SET applied_tip_lamports = NULL
WHERE applied_tip_lamports IS NOT NULL
  AND applied_tip_lamports < 0;

UPDATE orders
SET ata_create_rent_lamports = NULL
WHERE ata_create_rent_lamports IS NOT NULL
  AND ata_create_rent_lamports < 0;

UPDATE orders
SET network_fee_lamports_hint = NULL
WHERE network_fee_lamports_hint IS NOT NULL
  AND network_fee_lamports_hint < 0;

UPDATE orders
SET base_fee_lamports_hint = NULL
WHERE base_fee_lamports_hint IS NOT NULL
  AND base_fee_lamports_hint < 0;

UPDATE orders
SET priority_fee_lamports_hint = NULL
WHERE priority_fee_lamports_hint IS NOT NULL
  AND priority_fee_lamports_hint < 0;

DROP TRIGGER IF EXISTS trg_orders_fee_breakdown_non_negative_insert;
DROP TRIGGER IF EXISTS trg_orders_fee_breakdown_non_negative_update;

CREATE TRIGGER IF NOT EXISTS trg_orders_fee_breakdown_non_negative_insert
BEFORE INSERT ON orders
FOR EACH ROW
WHEN (NEW.applied_tip_lamports IS NOT NULL AND NEW.applied_tip_lamports < 0)
   OR (NEW.ata_create_rent_lamports IS NOT NULL AND NEW.ata_create_rent_lamports < 0)
   OR (NEW.network_fee_lamports_hint IS NOT NULL AND NEW.network_fee_lamports_hint < 0)
   OR (NEW.base_fee_lamports_hint IS NOT NULL AND NEW.base_fee_lamports_hint < 0)
   OR (NEW.priority_fee_lamports_hint IS NOT NULL AND NEW.priority_fee_lamports_hint < 0)
BEGIN
    SELECT RAISE(ABORT, 'orders fee breakdown lamports must be non-negative');
END;

CREATE TRIGGER IF NOT EXISTS trg_orders_fee_breakdown_non_negative_update
BEFORE UPDATE OF
    applied_tip_lamports,
    ata_create_rent_lamports,
    network_fee_lamports_hint,
    base_fee_lamports_hint,
    priority_fee_lamports_hint ON orders
FOR EACH ROW
WHEN (NEW.applied_tip_lamports IS NOT NULL AND NEW.applied_tip_lamports < 0)
   OR (NEW.ata_create_rent_lamports IS NOT NULL AND NEW.ata_create_rent_lamports < 0)
   OR (NEW.network_fee_lamports_hint IS NOT NULL AND NEW.network_fee_lamports_hint < 0)
   OR (NEW.base_fee_lamports_hint IS NOT NULL AND NEW.base_fee_lamports_hint < 0)
   OR (NEW.priority_fee_lamports_hint IS NOT NULL AND NEW.priority_fee_lamports_hint < 0)
BEGIN
    SELECT RAISE(ABORT, 'orders fee breakdown lamports must be non-negative');
END;
