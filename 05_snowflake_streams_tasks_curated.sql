-- ============================================================
-- 05_snowflake_streams_tasks_curated.sql
-- Snowflake: Streams, Tasks, and Curated Dimensional Models
-- Run as CDC_PIPELINE_ROLE
-- ============================================================

USE ROLE CDC_PIPELINE_ROLE;
USE DATABASE CDC_DB;
USE WAREHOUSE CDC_WH;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: CURATED TABLES (Dimensional Models)
-- ─────────────────────────────────────────────────────────────
USE SCHEMA CURATED;

-- DIM_CUSTOMERS: SCD Type 1 (upsert — latest state wins)
CREATE TABLE IF NOT EXISTS CDC_DB.CURATED.DIM_CUSTOMERS (
    customer_key        NUMBER AUTOINCREMENT PRIMARY KEY,  -- surrogate key
    customer_id         NUMBER        NOT NULL UNIQUE,      -- natural key from source
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(201)  AS (first_name || ' ' || last_name),
    email               VARCHAR(255),
    phone               VARCHAR(20),
    address_line1       VARCHAR(255),
    address_line2       VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    country             VARCHAR(50),
    customer_status     VARCHAR(20),
    src_created_at      TIMESTAMP_TZ,    -- when row was created in source
    src_updated_at      TIMESTAMP_TZ,    -- when row was last updated in source
    dw_first_seen_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  -- first time we saw this key
    dw_last_updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  -- last time we updated in DW
    is_deleted          BOOLEAN DEFAULT FALSE  -- soft-delete flag for DMS 'D' events
)
COMMENT = 'CURATED: DIM_CUSTOMERS — SCD1 customer dimension';

-- FACT_ORDERS: append+upsert order fact
CREATE TABLE IF NOT EXISTS CDC_DB.CURATED.FACT_ORDERS (
    order_key           NUMBER AUTOINCREMENT PRIMARY KEY,  -- surrogate key
    order_id            NUMBER        NOT NULL UNIQUE,
    customer_id         NUMBER        NOT NULL,  -- FK to DIM_CUSTOMERS.customer_id
    order_date          DATE,
    ship_date           DATE,
    order_status        VARCHAR(30),
    total_amount        NUMBER(12,2),
    currency            VARCHAR(3),
    shipping_method     VARCHAR(50),
    notes               TEXT,
    src_created_at      TIMESTAMP_TZ,
    src_updated_at      TIMESTAMP_TZ,
    dw_last_updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_deleted          BOOLEAN DEFAULT FALSE
)
COMMENT = 'CURATED: FACT_ORDERS — order fact table';

-- FACT_ORDER_ITEMS: line-level order details
CREATE TABLE IF NOT EXISTS CDC_DB.CURATED.FACT_ORDER_ITEMS (
    item_key            NUMBER AUTOINCREMENT PRIMARY KEY,
    item_id             NUMBER        NOT NULL UNIQUE,
    order_id            NUMBER        NOT NULL,
    product_id          NUMBER,
    product_name        VARCHAR(255),
    quantity            NUMBER,
    unit_price          NUMBER(10,2),
    discount_pct        NUMBER(5,2),
    line_total          NUMBER(12,2),
    src_created_at      TIMESTAMP_TZ,
    dw_last_updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_deleted          BOOLEAN DEFAULT FALSE
)
COMMENT = 'CURATED: FACT_ORDER_ITEMS — order line items';

-- ─────────────────────────────────────────────────────────────
-- SECTION B: STREAMS on RAW tables
--   Streams capture incremental changes (DML) in RAW tables
--   since the stream was last consumed by a Task.
--   APPEND_ONLY = FALSE → captures inserts, updates, deletes
-- ─────────────────────────────────────────────────────────────
USE SCHEMA RAW;

CREATE OR REPLACE STREAM CDC_DB.RAW.STREAM_CUSTOMERS_RAW
    ON TABLE CDC_DB.RAW.CUSTOMERS_RAW
    APPEND_ONLY = FALSE
    COMMENT     = 'Stream: incremental CDC events from CUSTOMERS_RAW';

CREATE OR REPLACE STREAM CDC_DB.RAW.STREAM_ORDERS_RAW
    ON TABLE CDC_DB.RAW.ORDERS_RAW
    APPEND_ONLY = FALSE
    COMMENT     = 'Stream: incremental CDC events from ORDERS_RAW';

CREATE OR REPLACE STREAM CDC_DB.RAW.STREAM_ORDER_ITEMS_RAW
    ON TABLE CDC_DB.RAW.ORDER_ITEMS_RAW
    APPEND_ONLY = FALSE
    COMMENT     = 'Stream: incremental CDC events from ORDER_ITEMS_RAW';

-- ─────────────────────────────────────────────────────────────
-- SECTION C: TASKS — Scheduled incremental MERGE into CURATED
--   Tasks run the MERGE statement on a schedule.
--   WHEN clause ensures task only runs if the stream has data.
--
--   MERGE strategy for CDC:
--     1. Deduplicate stream → latest record per key (Op priority: D > U > I)
--     2. MERGE into CURATED: MATCHED → UPDATE, NOT MATCHED → INSERT
--     3. 'D' ops set is_deleted = TRUE (soft delete)
-- ─────────────────────────────────────────────────────────────

-- TASK: Merge customers stream → DIM_CUSTOMERS
CREATE OR REPLACE TASK CDC_DB.RAW.TASK_MERGE_CUSTOMERS
    WAREHOUSE = CDC_WH
    SCHEDULE  = '5 MINUTE'         -- poll every 5 minutes
    WHEN SYSTEM$STREAM_HAS_DATA('CDC_DB.RAW.STREAM_CUSTOMERS_RAW')
AS
MERGE INTO CDC_DB.CURATED.DIM_CUSTOMERS AS tgt
USING (
    -- Deduplicate: for each customer_id keep only the last event in the stream
    -- QUALIFY ensures one row per customer_id (latest by _dms_timestamp)
    SELECT
        op,
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        address_line1,
        address_line2,
        city,
        state,
        postal_code,
        country,
        customer_status,
        created_at  AS src_created_at,
        updated_at  AS src_updated_at
    FROM CDC_DB.RAW.STREAM_CUSTOMERS_RAW
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY _dms_timestamp DESC, METADATA$ACTION DESC
    ) = 1
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND src.op = 'D' THEN
    UPDATE SET
        tgt.is_deleted         = TRUE,
        tgt.dw_last_updated_at = CURRENT_TIMESTAMP()
WHEN MATCHED AND src.op IN ('U', 'I') THEN
    UPDATE SET
        tgt.first_name         = src.first_name,
        tgt.last_name          = src.last_name,
        tgt.email              = src.email,
        tgt.phone              = src.phone,
        tgt.address_line1      = src.address_line1,
        tgt.address_line2      = src.address_line2,
        tgt.city               = src.city,
        tgt.state              = src.state,
        tgt.postal_code        = src.postal_code,
        tgt.country            = src.country,
        tgt.customer_status    = src.customer_status,
        tgt.src_updated_at     = src.src_updated_at,
        tgt.dw_last_updated_at = CURRENT_TIMESTAMP(),
        tgt.is_deleted         = FALSE
WHEN NOT MATCHED AND src.op != 'D' THEN
    INSERT (
        customer_id, first_name, last_name, email, phone,
        address_line1, address_line2, city, state, postal_code,
        country, customer_status, src_created_at, src_updated_at,
        dw_first_seen_at, dw_last_updated_at, is_deleted
    )
    VALUES (
        src.customer_id, src.first_name, src.last_name, src.email, src.phone,
        src.address_line1, src.address_line2, src.city, src.state, src.postal_code,
        src.country, src.customer_status, src.src_created_at, src.src_updated_at,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), FALSE
    );

-- TASK: Merge orders stream → FACT_ORDERS
CREATE OR REPLACE TASK CDC_DB.RAW.TASK_MERGE_ORDERS
    WAREHOUSE = CDC_WH
    SCHEDULE  = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CDC_DB.RAW.STREAM_ORDERS_RAW')
AS
MERGE INTO CDC_DB.CURATED.FACT_ORDERS AS tgt
USING (
    SELECT
        op,
        order_id,
        customer_id,
        order_date,
        ship_date,
        status          AS order_status,
        total_amount,
        currency,
        shipping_method,
        notes,
        created_at      AS src_created_at,
        updated_at      AS src_updated_at
    FROM CDC_DB.RAW.STREAM_ORDERS_RAW
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY _dms_timestamp DESC, METADATA$ACTION DESC
    ) = 1
) AS src
ON tgt.order_id = src.order_id
WHEN MATCHED AND src.op = 'D' THEN
    UPDATE SET
        tgt.is_deleted         = TRUE,
        tgt.dw_last_updated_at = CURRENT_TIMESTAMP()
WHEN MATCHED AND src.op IN ('U', 'I') THEN
    UPDATE SET
        tgt.customer_id        = src.customer_id,
        tgt.order_date         = src.order_date,
        tgt.ship_date          = src.ship_date,
        tgt.order_status       = src.order_status,
        tgt.total_amount       = src.total_amount,
        tgt.currency           = src.currency,
        tgt.shipping_method    = src.shipping_method,
        tgt.notes              = src.notes,
        tgt.src_updated_at     = src.src_updated_at,
        tgt.dw_last_updated_at = CURRENT_TIMESTAMP(),
        tgt.is_deleted         = FALSE
WHEN NOT MATCHED AND src.op != 'D' THEN
    INSERT (
        order_id, customer_id, order_date, ship_date, order_status,
        total_amount, currency, shipping_method, notes,
        src_created_at, src_updated_at, dw_last_updated_at, is_deleted
    )
    VALUES (
        src.order_id, src.customer_id, src.order_date, src.ship_date, src.order_status,
        src.total_amount, src.currency, src.shipping_method, src.notes,
        src.src_created_at, src.src_updated_at, CURRENT_TIMESTAMP(), FALSE
    );

-- TASK: Merge order_items stream → FACT_ORDER_ITEMS
CREATE OR REPLACE TASK CDC_DB.RAW.TASK_MERGE_ORDER_ITEMS
    WAREHOUSE = CDC_WH
    SCHEDULE  = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CDC_DB.RAW.STREAM_ORDER_ITEMS_RAW')
AS
MERGE INTO CDC_DB.CURATED.FACT_ORDER_ITEMS AS tgt
USING (
    SELECT
        op,
        item_id,
        order_id,
        product_id,
        product_name,
        quantity,
        unit_price,
        discount_pct,
        line_total,
        created_at AS src_created_at
    FROM CDC_DB.RAW.STREAM_ORDER_ITEMS_RAW
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY item_id
        ORDER BY _dms_timestamp DESC, METADATA$ACTION DESC
    ) = 1
) AS src
ON tgt.item_id = src.item_id
WHEN MATCHED AND src.op = 'D' THEN
    UPDATE SET tgt.is_deleted = TRUE, tgt.dw_last_updated_at = CURRENT_TIMESTAMP()
WHEN MATCHED AND src.op IN ('U', 'I') THEN
    UPDATE SET
        tgt.order_id           = src.order_id,
        tgt.product_id         = src.product_id,
        tgt.product_name       = src.product_name,
        tgt.quantity           = src.quantity,
        tgt.unit_price         = src.unit_price,
        tgt.discount_pct       = src.discount_pct,
        tgt.line_total         = src.line_total,
        tgt.dw_last_updated_at = CURRENT_TIMESTAMP(),
        tgt.is_deleted         = FALSE
WHEN NOT MATCHED AND src.op != 'D' THEN
    INSERT (item_id, order_id, product_id, product_name, quantity, unit_price,
            discount_pct, line_total, src_created_at, dw_last_updated_at, is_deleted)
    VALUES (src.item_id, src.order_id, src.product_id, src.product_name, src.quantity,
            src.unit_price, src.discount_pct, src.line_total, src.src_created_at,
            CURRENT_TIMESTAMP(), FALSE);

-- ─────────────────────────────────────────────────────────────
-- SECTION D: RESUME TASKS (Tasks are created in SUSPENDED state)
-- ─────────────────────────────────────────────────────────────
ALTER TASK CDC_DB.RAW.TASK_MERGE_CUSTOMERS   RESUME;
ALTER TASK CDC_DB.RAW.TASK_MERGE_ORDERS      RESUME;
ALTER TASK CDC_DB.RAW.TASK_MERGE_ORDER_ITEMS RESUME;

-- Verify task status
SHOW TASKS IN SCHEMA CDC_DB.RAW;

-- Monitor task run history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
ORDER BY SCHEDULED_TIME DESC;
