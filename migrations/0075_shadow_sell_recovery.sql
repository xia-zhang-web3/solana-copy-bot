-- Wallet/mint come from the same immutable SELL facts as the fresh Shadow scan.
-- Index existing 0074 preparations, including MissingOrigin; no metadata backfill.
-- object b102_shadow_sell_pair
CREATE INDEX b102_shadow_sell_pair ON association_sell_preparations(
    CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.wallet') END,
    CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.token_in') END,
    signature
);
-- object b102_shadow_sell_pair_bytes
CREATE INDEX b102_shadow_sell_pair_bytes ON association_sell_preparations(
    CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.wallet') END,
    CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.token_in') END,
    length(CAST(signature AS BLOB))
);
-- object association_shadow_sell_work
CREATE TABLE association_shadow_sell_work (
    wallet TEXT NOT NULL,
    token TEXT NOT NULL,
    after_signature TEXT NOT NULL,
    PRIMARY KEY(wallet,token)
);
