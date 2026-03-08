ALTER TABLE fills ADD COLUMN qty_raw TEXT;
ALTER TABLE fills ADD COLUMN qty_decimals INTEGER;

ALTER TABLE positions ADD COLUMN qty_raw TEXT;
ALTER TABLE positions ADD COLUMN qty_decimals INTEGER;
