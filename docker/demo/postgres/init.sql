-- paimon_heap demo — run after the container is healthy
\connect demo

CREATE TABLE IF NOT EXISTS orders (
    order_id    BIGINT PRIMARY KEY,
    customer    TEXT,
    product     TEXT,
    quantity    INT,
    unit_price  NUMERIC(12,2),
    status      TEXT,
    created_at  TIMESTAMP
) USING paimon_heap;

INSERT INTO orders (order_id, customer, product, quantity, unit_price, status, created_at) VALUES
    (1, 'Alice',   'Laptop',       1, 1299.99, 'shipped',   NOW()),
    (2, 'Bob',     'Keyboard',     2,   79.50, 'pending',   NOW()),
    (3, 'Charlie', 'Monitor',      1,  349.00, 'delivered', NOW()),
    (4, 'Diana',   'Headphones',   3,   59.99, 'shipped',   NOW()),
    (5, 'Eve',     'Webcam',       1,   89.00, 'pending',   NOW()),
    (6, 'Frank',   'USB-C Hub',    2,   49.95, 'shipped',   NOW()),
    (7, 'Grace',   'Desk Lamp',    1,   34.99, 'delivered', NOW());

SELECT 'Inserted ' || COUNT(*) || ' rows into paimon_heap orders table' AS result
FROM orders;
