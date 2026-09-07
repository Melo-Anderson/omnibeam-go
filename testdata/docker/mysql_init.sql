CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS products_mixed (
    id BIGINT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    in_stock BOOLEAN NOT NULL,
    description TEXT,
    created_at DATETIME NOT NULL
);

SET SESSION cte_max_recursion_depth = 15000;

-- Seed 10,000 product rows using a recursive CTE.
INSERT INTO products_mixed (id, name, price, in_stock, description, created_at)
WITH RECURSIVE seq (n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 10000
)
SELECT
    n,
    CONCAT('Product_', n),
    n * 0.99,
    (n % 2 = 0),
    CONCAT('Desc for product ', n),
    NOW()
FROM seq;
