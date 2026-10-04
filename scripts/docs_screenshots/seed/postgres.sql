-- Synthetic demo data for the documentation screenshots.
-- Every value is derived from fixed lists and arithmetic on row numbers, so
-- two runs produce the same rows and the same images.

CREATE TABLE customers (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    email       text NOT NULL UNIQUE,
    country     text NOT NULL,
    created_at  timestamptz NOT NULL
);

CREATE TABLE products (
    id        integer PRIMARY KEY,
    sku       text NOT NULL UNIQUE,
    name      text NOT NULL,
    category  text NOT NULL,
    price     numeric(10, 2) NOT NULL,
    stock     integer NOT NULL
);

CREATE TABLE orders (
    id           integer PRIMARY KEY,
    customer_id  integer NOT NULL REFERENCES customers (id),
    status       text NOT NULL,
    ordered_at   timestamptz NOT NULL,
    total        numeric(12, 2) NOT NULL DEFAULT 0
);

CREATE TABLE order_items (
    order_id    integer NOT NULL REFERENCES orders (id),
    line        integer NOT NULL,
    product_id  integer NOT NULL REFERENCES products (id),
    quantity    integer NOT NULL,
    unit_price  numeric(10, 2) NOT NULL,
    PRIMARY KEY (order_id, line)
);

CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE INDEX orders_ordered_at_idx ON orders (ordered_at);

INSERT INTO customers (id, name, email, country, created_at)
SELECT
    i,
    first_name || ' ' || last_name,
    lower(first_name || '.' || last_name || i) || '@example.com',
    (ARRAY['Argentina', 'Brazil', 'Canada', 'Germany', 'Japan', 'Mexico', 'Spain', 'United States'])[1 + (i * 3) % 8],
    timestamptz '2024-01-15 09:00:00+00' + (i * interval '6 days 7 hours')
FROM generate_series(1, 40) AS i,
LATERAL (
    SELECT
        (ARRAY['Ada', 'Bruno', 'Carla', 'Diego', 'Elena', 'Felix', 'Grace', 'Hugo', 'Ines', 'Jonas',
               'Kenji', 'Lucia', 'Mateo', 'Nora', 'Omar', 'Paula', 'Quentin', 'Rosa', 'Samir', 'Tania'])[1 + (i - 1) % 20] AS first_name,
        (ARRAY['Alvarez', 'Becker', 'Costa', 'Dubois', 'Evans', 'Fischer', 'Garcia', 'Hayashi', 'Ivanova', 'Jensen',
               'Kowalski', 'Lopez', 'Moreau', 'Novak', 'Ortiz', 'Peralta', 'Quinn', 'Rossi', 'Silva', 'Tanaka'])[1 + (i * 7) % 20] AS last_name
) AS names;

INSERT INTO products (id, sku, name, category, price, stock) VALUES
    (1,  'KB-101', 'Mechanical Keyboard',        'Peripherals', 129.00, 84),
    (2,  'MS-210', 'Wireless Mouse',             'Peripherals',  49.90, 230),
    (3,  'MN-270', '27" 4K Monitor',             'Displays',    399.00, 41),
    (4,  'MN-340', '34" Ultrawide Monitor',      'Displays',    649.00, 17),
    (5,  'HD-500', 'Noise Cancelling Headset',   'Audio',       219.00, 66),
    (6,  'SP-120', 'Desk Speakers',              'Audio',        89.00, 120),
    (7,  'CM-080', '4K Webcam',                  'Video',       159.00, 58),
    (8,  'MC-030', 'USB Microphone',             'Audio',       119.00, 73),
    (9,  'DK-900', 'Thunderbolt Dock',           'Accessories', 279.00, 35),
    (10, 'HB-040', 'USB-C Hub',                  'Accessories',  59.00, 310),
    (11, 'SS-1TB', 'Portable SSD 1 TB',          'Storage',     109.00, 145),
    (12, 'SS-2TB', 'Portable SSD 2 TB',          'Storage',     189.00, 88),
    (13, 'LS-010', 'Laptop Stand',               'Accessories',  45.00, 190),
    (14, 'DM-160', 'Desk Mat',                   'Accessories',  29.00, 400),
    (15, 'CH-700', 'Ergonomic Chair',            'Furniture',   549.00, 12),
    (16, 'DS-140', 'Standing Desk',              'Furniture',   699.00, 9),
    (17, 'LT-050', 'Monitor Light Bar',          'Accessories',  69.00, 140),
    (18, 'CB-002', 'USB-C Cable 2 m',            'Cables',       19.00, 750),
    (19, 'CB-HD2', 'HDMI 2.1 Cable',             'Cables',       24.00, 520),
    (20, 'RT-600', 'Wi-Fi 6 Router',             'Networking',  179.00, 47),
    (21, 'SW-008', '8-Port Gigabit Switch',      'Networking',   39.00, 160),
    (22, 'TB-200', 'Graphics Tablet',            'Peripherals', 249.00, 28),
    (23, 'UP-150', 'UPS 1500 VA',                'Power',       229.00, 21),
    (24, 'PB-020', 'Power Bank 20000 mAh',       'Power',        59.00, 205);

INSERT INTO orders (id, customer_id, status, ordered_at)
SELECT
    i,
    1 + (i * 7) % 40,
    CASE
        WHEN i % 23 = 0 THEN 'refunded'
        WHEN i % 11 = 0 THEN 'cancelled'
        WHEN i > 285 THEN 'pending'
        WHEN i > 270 THEN 'shipped'
        ELSE 'delivered'
    END,
    timestamptz '2025-01-02 08:30:00+00' + (i * interval '29 hours 13 minutes')
FROM generate_series(1, 300) AS i;

INSERT INTO order_items (order_id, line, product_id, quantity, unit_price)
SELECT
    o.id,
    line,
    p.id,
    1 + (o.id + line) % 3,
    p.price
FROM orders AS o
CROSS JOIN LATERAL generate_series(1, 1 + o.id % 4) AS line
JOIN products AS p ON p.id = 1 + (o.id * 5 + line * 11) % 24;

UPDATE orders AS o
SET total = items.total
FROM (
    SELECT order_id, sum(quantity * unit_price) AS total
    FROM order_items
    GROUP BY order_id
) AS items
WHERE items.order_id = o.id;

CREATE VIEW monthly_revenue AS
SELECT
    date_trunc('month', ordered_at)::date AS month,
    count(*) AS orders,
    sum(total) AS revenue,
    round(avg(total), 2) AS average_order
FROM orders
WHERE status IN ('delivered', 'shipped')
GROUP BY 1
ORDER BY 1;

ANALYZE;
