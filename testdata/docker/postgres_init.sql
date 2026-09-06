CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table 1: Integer PK for arithmetic range slicing (10,000 rows)
CREATE TABLE orders_int (
    id BIGINT PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO orders_int (id, customer_id, amount, status, created_at)
SELECT
    i,
    'cust_' || (i % 500),
    (i * 1.5)::numeric(10,2),
    CASE WHEN i % 2 = 0 THEN 'COMPLETED' ELSE 'PENDING' END,
    NOW() - (i || ' minutes')::interval
FROM generate_series(1, 10000) AS i;

-- Table 2: UUID PK for NTILE window slicing (10,000 rows)
CREATE TABLE users_uuid (
    user_uuid VARCHAR(36) PRIMARY KEY,
    email VARCHAR(128) NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO users_uuid (user_uuid, email, score, created_at)
SELECT
    uuid_generate_v4()::text,
    'user_' || i || '@example.com',
    (i * 0.75),
    NOW() - (i || ' minutes')::interval
FROM generate_series(1, 10000) AS i;

-- Table 3: Corrupt table for DLQ quarantine testing (1,000 valid + 50 invalid nulls)
CREATE TABLE corrupt_payments (
    id BIGINT PRIMARY KEY,
    account_id VARCHAR(64),
    amount NUMERIC(10, 2)
);

-- 1,000 valid rows
INSERT INTO corrupt_payments (id, account_id, amount)
SELECT i, 'acc_' || i, (i * 2.0)::numeric(10,2)
FROM generate_series(1, 1000) AS i;

-- 50 corrupt rows (NULL account_id violating non-nullable schema)
INSERT INTO corrupt_payments (id, account_id, amount)
SELECT i, NULL, (i * 2.0)::numeric(10,2)
FROM generate_series(1001, 1050) AS i;
