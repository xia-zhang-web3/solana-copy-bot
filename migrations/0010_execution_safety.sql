UPDATE orders
SET client_order_id = order_id
WHERE client_order_id IS NULL OR TRIM(client_order_id) = '';

CREATE TRIGGER IF NOT EXISTS trg_orders_client_order_id_not_null_insert
BEFORE INSERT ON orders
FOR EACH ROW
WHEN NEW.client_order_id IS NULL OR TRIM(NEW.client_order_id) = ''
BEGIN
    SELECT RAISE(ABORT, 'orders.client_order_id must be non-empty');
END;

CREATE TRIGGER IF NOT EXISTS trg_orders_client_order_id_not_null_update
BEFORE UPDATE OF client_order_id ON orders
FOR EACH ROW
WHEN NEW.client_order_id IS NULL OR TRIM(NEW.client_order_id) = ''
BEGIN
    SELECT RAISE(ABORT, 'orders.client_order_id must be non-empty');
END;

CREATE INDEX IF NOT EXISTS idx_fills_order_id
    ON fills(order_id);
