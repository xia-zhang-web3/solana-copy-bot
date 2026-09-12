-- Durable quantity provenance for unsigned SELL retries; NULL legacy rows stay unproven.
ALTER TABLE execution_canary_build_plan_metadata ADD COLUMN owned_sell_amount_proof_json TEXT;
