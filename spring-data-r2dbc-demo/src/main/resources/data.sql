INSERT INTO purchase_order (
    order_no,
    customer_name,
    customer_tier,
    sku_code,
    quantity,
    unit_price,
    shipping_fee,
    risk_level,
    status,
    remark,
    created_at,
    updated_at
) VALUES
    (
        'PO-DEMO-0001',
        'Alice Chen',
        'GOLD',
        'R2DBC-BOOK',
        2,
        129.00,
        18.00,
        'LOW',
        'CREATED',
        'seed data for list api',
        CURRENT_TIMESTAMP - INTERVAL '15 minutes',
        CURRENT_TIMESTAMP - INTERVAL '15 minutes'
    ),
    (
        'PO-DEMO-0002',
        'Bob Li',
        'SILVER',
        'WEBFLUX-COURSE',
        1,
        299.00,
        26.00,
        'MEDIUM',
        'PENDING_REVIEW',
        'seed data for topology demo',
        CURRENT_TIMESTAMP - INTERVAL '8 minutes',
        CURRENT_TIMESTAMP - INTERVAL '8 minutes'
    )
ON CONFLICT (order_no) DO NOTHING;
