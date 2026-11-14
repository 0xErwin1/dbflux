-- Synthetic demo data for the documentation screenshots: a small local
-- inventory database. All rows are fixed literals.

CREATE TABLE warehouses (
    id    INTEGER PRIMARY KEY,
    code  TEXT NOT NULL UNIQUE,
    city  TEXT NOT NULL
);

CREATE TABLE stock_levels (
    warehouse_id  INTEGER NOT NULL REFERENCES warehouses (id),
    sku           TEXT NOT NULL,
    quantity      INTEGER NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (warehouse_id, sku)
);

INSERT INTO warehouses (id, code, city) VALUES
    (1, 'EZE', 'Buenos Aires'),
    (2, 'BER', 'Berlin'),
    (3, 'YYZ', 'Toronto');

INSERT INTO stock_levels (warehouse_id, sku, quantity, updated_at) VALUES
    (1, 'KB-101', 31, '2025-03-01T09:00:00Z'),
    (1, 'MS-210', 88, '2025-03-01T09:00:00Z'),
    (1, 'MN-270', 12, '2025-03-01T09:00:00Z'),
    (2, 'KB-101', 27, '2025-03-01T10:30:00Z'),
    (2, 'HD-500', 40, '2025-03-01T10:30:00Z'),
    (2, 'DK-900', 15, '2025-03-01T10:30:00Z'),
    (3, 'MS-210', 142, '2025-03-01T12:15:00Z'),
    (3, 'SS-1TB', 63, '2025-03-01T12:15:00Z'),
    (3, 'CH-700', 6, '2025-03-01T12:15:00Z');

CREATE VIEW low_stock AS
SELECT w.code, s.sku, s.quantity
FROM stock_levels AS s
JOIN warehouses AS w ON w.id = s.warehouse_id
WHERE s.quantity < 20;
