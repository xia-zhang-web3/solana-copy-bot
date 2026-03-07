ALTER TABLE observed_swaps
    ADD COLUMN qty_in_raw TEXT;

ALTER TABLE observed_swaps
    ADD COLUMN qty_in_decimals INTEGER;

ALTER TABLE observed_swaps
    ADD COLUMN qty_out_raw TEXT;

ALTER TABLE observed_swaps
    ADD COLUMN qty_out_decimals INTEGER;
