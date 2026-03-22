CREATE TABLE IF NOT EXISTS purchase_order (
    id BIGSERIAL PRIMARY KEY,
    order_no VARCHAR(32) NOT NULL UNIQUE,
    customer_name VARCHAR(64) NOT NULL,
    customer_tier VARCHAR(32) NOT NULL,
    sku_code VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    shipping_fee NUMERIC(12, 2) NOT NULL DEFAULT 0,
    risk_level VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    remark VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_purchase_order_created_at ON purchase_order (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_purchase_order_customer_name ON purchase_order (customer_name);
