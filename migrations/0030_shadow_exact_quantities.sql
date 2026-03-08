ALTER TABLE shadow_lots ADD COLUMN qty_raw TEXT;
ALTER TABLE shadow_lots ADD COLUMN qty_decimals INTEGER;

ALTER TABLE shadow_closed_trades ADD COLUMN qty_raw TEXT;
ALTER TABLE shadow_closed_trades ADD COLUMN qty_decimals INTEGER;
