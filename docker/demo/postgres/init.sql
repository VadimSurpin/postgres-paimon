-- paimon_heap demo — run after the container is healthy
\connect demo

CREATE TABLE IF NOT EXISTS orders (
    order_id    BIGINT,
    customer    TEXT,
    product     TEXT,
    quantity    INT,
    unit_price  NUMERIC(12,2),
    status      TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
) USING paimon_heap;

INSERT INTO orders (order_id, customer, product, quantity, unit_price, status) VALUES
    (1, 'Alice',   'Laptop',       1, 1299.99, 'shipped'),
    (2, 'Bob',     'Keyboard',     2,   79.50, 'pending'),
    (3, 'Charlie', 'Monitor',      1,  349.00, 'delivered'),
    (4, 'Diana',   'Headphones',   3,   59.99, 'shipped'),
    (5, 'Eve',     'Webcam',       1,   89.00, 'pending'),
    (6, 'Frank',   'USB-C Hub',    2,   49.95, 'shipped'),
    (7, 'Grace',   'Desk Lamp',    1,   34.99, 'delivered');

SELECT 'Inserted ' || COUNT(*) || ' rows into paimon_heap orders table' AS result
FROM orders;
