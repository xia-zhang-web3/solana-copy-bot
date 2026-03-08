ALTER TABLE copy_signals
ADD COLUMN notional_origin TEXT NOT NULL DEFAULT 'leader_approximate';
