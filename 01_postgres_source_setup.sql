-- ============================================================
-- 01_postgres_source_setup.sql
-- PostgreSQL Source Database: WAL / CDC Setup
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- STEP 1: Verify logical replication is enabled
--   On RDS: set rds.logical_replication = 1 in parameter group
--   Requires instance reboot after changing the parameter group
-- ─────────────────────────────────────────────────────────────
SHOW wal_level;            -- must return 'logical'
SHOW max_replication_slots; -- must be >= 1
SHOW max_wal_senders;      -- must be >= 1

-- ─────────────────────────────────────────────────────────────
-- STEP 2: Create source schema and tables
-- ─────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS public;

-- Customers table
CREATE TABLE IF NOT EXISTS public.customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) NOT NULL UNIQUE,
    phone           VARCHAR(20),
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    postal_code     VARCHAR(20),
    country         VARCHAR(50) DEFAULT 'US',
    customer_status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE | INACTIVE | SUSPENDED
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orders table
CREATE TABLE IF NOT EXISTS public.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT NOT NULL REFERENCES public.customers(customer_id),
    order_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    ship_date       DATE,
    status          VARCHAR(30) DEFAULT 'PENDING',  -- PENDING | PROCESSING | SHIPPED | DELIVERED | CANCELLED
    total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
    currency        VARCHAR(3)  DEFAULT 'USD',
    shipping_method VARCHAR(50),
    notes           TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Order items table (for richer dimensional model)
CREATE TABLE IF NOT EXISTS public.order_items (
    item_id         SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES public.orders(order_id),
    product_id      INT NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    quantity        INT NOT NULL DEFAULT 1,
    unit_price      NUMERIC(10,2) NOT NULL,
    discount_pct    NUMERIC(5,2) DEFAULT 0,
    line_total      NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price * (1 - discount_pct/100)) STORED,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────
-- STEP 3: Create a DMS replication user with minimal privileges
-- ─────────────────────────────────────────────────────────────
CREATE USER dms_replication_user WITH PASSWORD 'StrongP@ssw0rd!';

-- Grant replication role (required for logical replication)
GRANT rds_replication TO dms_replication_user;

-- Grant read access on source tables
GRANT CONNECT ON DATABASE postgres TO dms_replication_user;
GRANT USAGE ON SCHEMA public TO dms_replication_user;
GRANT SELECT ON public.customers   TO dms_replication_user;
GRANT SELECT ON public.orders      TO dms_replication_user;
GRANT SELECT ON public.order_items TO dms_replication_user;

-- ─────────────────────────────────────────────────────────────
-- STEP 4: Auto-update updated_at via trigger
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON public.orders
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ─────────────────────────────────────────────────────────────
-- STEP 5: Seed data — initial rows for full-load test
-- ─────────────────────────────────────────────────────────────
INSERT INTO public.customers (first_name, last_name, email, phone, city, state, country)
VALUES
    ('Alice',  'Johnson',  'alice.johnson@example.com',  '555-0101', 'New York',    'NY', 'US'),
    ('Bob',    'Smith',    'bob.smith@example.com',      '555-0102', 'Los Angeles', 'CA', 'US'),
    ('Carol',  'Williams', 'carol.w@example.com',        '555-0103', 'Chicago',     'IL', 'US'),
    ('David',  'Brown',    'david.b@example.com',        '555-0104', 'Houston',     'TX', 'US'),
    ('Eve',    'Davis',    'eve.davis@example.com',      '555-0105', 'Phoenix',     'AZ', 'US');

INSERT INTO public.orders (customer_id, order_date, status, total_amount, shipping_method)
VALUES
    (1, CURRENT_DATE - 10, 'DELIVERED',  125.50, 'STANDARD'),
    (1, CURRENT_DATE -  3, 'SHIPPED',    250.00, 'EXPRESS'),
    (2, CURRENT_DATE -  5, 'PROCESSING',  75.99, 'STANDARD'),
    (3, CURRENT_DATE -  1, 'PENDING',    340.00, 'OVERNIGHT'),
    (4, CURRENT_DATE,      'PENDING',     88.25, 'STANDARD');

INSERT INTO public.order_items (order_id, product_id, product_name, quantity, unit_price, discount_pct)
VALUES
    (1, 101, 'Wireless Mouse',       1,  45.00, 0),
    (1, 102, 'USB-C Hub',            1,  80.50, 0),
    (2, 103, 'Mechanical Keyboard',  1, 250.00, 0),
    (3, 104, 'Laptop Stand',         1,  75.99, 0),
    (4, 105, 'Monitor 27"',          1, 340.00, 0),
    (5, 106, 'Webcam HD',            1,  88.25, 0);

-- Verify seed data
SELECT 'customers'   AS tbl, COUNT(*) FROM public.customers   UNION ALL
SELECT 'orders'      AS tbl, COUNT(*) FROM public.orders      UNION ALL
SELECT 'order_items' AS tbl, COUNT(*) FROM public.order_items;
