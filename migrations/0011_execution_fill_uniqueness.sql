DELETE FROM fills
WHERE id NOT IN (
    SELECT MIN(id)
    FROM fills
    GROUP BY order_id
);

DROP INDEX IF EXISTS idx_fills_order_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_order_id
    ON fills(order_id);
