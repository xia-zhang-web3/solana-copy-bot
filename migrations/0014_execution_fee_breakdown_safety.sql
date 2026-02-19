UPDATE orders
SET applied_tip_lamports = NULL
WHERE applied_tip_lamports IS NOT NULL
  AND applied_tip_lamports < 0;

UPDATE orders
SET ata_create_rent_lamports = NULL
WHERE ata_create_rent_lamports IS NOT NULL
  AND ata_create_rent_lamports < 0;

CREATE TRIGGER IF NOT EXISTS trg_orders_fee_breakdown_non_negative_insert
BEFORE INSERT ON orders
FOR EACH ROW
WHEN (NEW.applied_tip_lamports IS NOT NULL AND NEW.applied_tip_lamports < 0)
   OR (NEW.ata_create_rent_lamports IS NOT NULL AND NEW.ata_create_rent_lamports < 0)
BEGIN
    SELECT RAISE(ABORT, 'orders fee breakdown lamports must be non-negative');
END;

CREATE TRIGGER IF NOT EXISTS trg_orders_fee_breakdown_non_negative_update
BEFORE UPDATE OF applied_tip_lamports, ata_create_rent_lamports ON orders
FOR EACH ROW
WHEN (NEW.applied_tip_lamports IS NOT NULL AND NEW.applied_tip_lamports < 0)
   OR (NEW.ata_create_rent_lamports IS NOT NULL AND NEW.ata_create_rent_lamports < 0)
BEGIN
    SELECT RAISE(ABORT, 'orders fee breakdown lamports must be non-negative');
END;
