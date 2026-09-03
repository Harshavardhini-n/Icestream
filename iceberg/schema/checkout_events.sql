CREATE TABLE IF NOT EXISTS checkout_events (
    event_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    user_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DOUBLE,
    subtotal DOUBLE,
    discount_amount DOUBLE,
    shipping_amount DOUBLE,
    tax_amount DOUBLE,
    total_amount DOUBLE,

    tax_was_null BOOLEAN,
    calculated_total_amount DOUBLE,
    amount_difference DOUBLE,
    amount_consistent BOOLEAN,
    has_discount BOOLEAN,

    processed BOOLEAN,
    processing_stage STRING,
    processed_at TIMESTAMP
);