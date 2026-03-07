ALTER TABLE shadow_lots ADD COLUMN cost_lamports INTEGER;

ALTER TABLE shadow_closed_trades ADD COLUMN entry_cost_lamports INTEGER;
ALTER TABLE shadow_closed_trades ADD COLUMN exit_value_lamports INTEGER;
ALTER TABLE shadow_closed_trades ADD COLUMN pnl_lamports INTEGER;
