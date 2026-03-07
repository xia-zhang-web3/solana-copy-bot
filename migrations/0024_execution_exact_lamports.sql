ALTER TABLE fills ADD COLUMN notional_lamports INTEGER;
ALTER TABLE fills ADD COLUMN fee_lamports INTEGER;

ALTER TABLE positions ADD COLUMN cost_lamports INTEGER;
