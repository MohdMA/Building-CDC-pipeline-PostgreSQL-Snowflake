-- ============================================================
-- 04_snowflake_raw_tables_and_snowpipe.sql
-- Snowflake: RAW Tables + Snowpipe AUTO_INGEST pipes
-- ============================================================

USE ROLE CDC_PIPELINE_ROLE;
USE DATABASE CDC_DB;
USE SCHEMA RAW;
USE WAREHOUSE CDC_WH;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: RAW TABLES
--   Store every CDC event from DMS as a raw Parquet record.
--   The Op column from DMS encodes: 'I' = Insert, 'U' = Update, 'D' = Delete
--   VARIANT column captures the full row as-is for audit/replay.
-- ─────────────────────────────────────────────────────────────

-- RAW Customers
CREATE TABLE IF NOT EXISTS CDC_DB.RAW.CUSTOMERS_RAW (
    -- DMS metadata columns
    op                  VARCHAR(1),     -- 'I', 'U', 'D'  (AWS DMS Op field)
    _dms_timestamp      TIMESTAMP_NTZ,  -- when DMS wrote the record

    -- Source columns (mirroring public.customers)
    customer_id         NUMBER,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    phone               VARCHAR(20),
    address_line1       VARCHAR(255),
    address_line2       VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    country             VARCHAR(50),
    customer_status     VARCHAR(20),
    created_at          TIMESTAMP_TZ,
    updated_at          TIMESTAMP_TZ,

    -- Snowpipe ingestion metadata
    _snowpipe_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _source_file_row    NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
)
COMMENT = 'RAW: CDC events for customers table, loaded via Snowpipe';

-- RAW Orders
CREATE TABLE IF NOT EXISTS CDC_DB.RAW.ORDERS_RAW (
    -- DMS metadata
    op                  VARCHAR(1),
    _dms_timestamp      TIMESTAMP_NTZ,

    -- Source columns
    order_id            NUMBER,
    customer_id         NUMBER,
    order_date          DATE,
    ship_date           DATE,
    status              VARCHAR(30),
    total_amount        NUMBER(12,2),
    currency            VARCHAR(3),
    shipping_method     VARCHAR(50),
    notes               TEXT,
    created_at          TIMESTAMP_TZ,
    updated_at          TIMESTAMP_TZ,

    -- Snowpipe metadata
    _snowpipe_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _source_file_row    NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
)
COMMENT = 'RAW: CDC events for orders table, loaded via Snowpipe';

-- RAW Order Items
CREATE TABLE IF NOT EXISTS CDC_DB.RAW.ORDER_ITEMS_RAW (
    -- DMS metadata
    op                  VARCHAR(1),
    _dms_timestamp      TIMESTAMP_NTZ,

    -- Source columns
    item_id             NUMBER,
    order_id            NUMBER,
    product_id          NUMBER,
    product_name        VARCHAR(255),
    quantity            NUMBER,
    unit_price          NUMBER(10,2),
    discount_pct        NUMBER(5,2),
    line_total          NUMBER(12,2),
    created_at          TIMESTAMP_TZ,

    -- Snowpipe metadata
    _snowpipe_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _source_file_row    NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
)
COMMENT = 'RAW: CDC events for order_items table, loaded via Snowpipe';

-- ─────────────────────────────────────────────────────────────
-- SECTION B: SNOWPIPE DEFINITIONS (AUTO_INGEST via SQS)
--   After creating each pipe, run SHOW PIPES to get the
--   notification_channel ARN, then configure S3 event
--   notifications to that SQS queue.
-- ─────────────────────────────────────────────────────────────

-- Pipe: customers
CREATE OR REPLACE PIPE CDC_DB.RAW.PIPE_CUSTOMERS_CDC
    AUTO_INGEST = TRUE
    COMMENT     = 'Snowpipe: auto-ingest customers CDC Parquet from S3'
AS
COPY INTO CDC_DB.RAW.CUSTOMERS_RAW (
    op,
    _dms_timestamp,
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
    created_at,
    updated_at
)
FROM (
    SELECT
        $1:Op::VARCHAR(1),
        $1:_timestamp::TIMESTAMP_NTZ,
        $1:customer_id::NUMBER,
        $1:first_name::VARCHAR(100),
        $1:last_name::VARCHAR(100),
        $1:email::VARCHAR(255),
        $1:phone::VARCHAR(20),
        $1:address_line1::VARCHAR(255),
        $1:address_line2::VARCHAR(255),
        $1:city::VARCHAR(100),
        $1:state::VARCHAR(50),
        $1:postal_code::VARCHAR(20),
        $1:country::VARCHAR(50),
        $1:customer_status::VARCHAR(20),
        $1:created_at::TIMESTAMP_TZ,
        $1:updated_at::TIMESTAMP_TZ
    FROM @CDC_DB.RAW.S3_CDC_STAGE/public/customers/
);

-- Pipe: orders
CREATE OR REPLACE PIPE CDC_DB.RAW.PIPE_ORDERS_CDC
    AUTO_INGEST = TRUE
    COMMENT     = 'Snowpipe: auto-ingest orders CDC Parquet from S3'
AS
COPY INTO CDC_DB.RAW.ORDERS_RAW (
    op,
    _dms_timestamp,
    order_id,
    customer_id,
    order_date,
    ship_date,
    status,
    total_amount,
    currency,
    shipping_method,
    notes,
    created_at,
    updated_at
)
FROM (
    SELECT
        $1:Op::VARCHAR(1),
        $1:_timestamp::TIMESTAMP_NTZ,
        $1:order_id::NUMBER,
        $1:customer_id::NUMBER,
        $1:order_date::DATE,
        $1:ship_date::DATE,
        $1:status::VARCHAR(30),
        $1:total_amount::NUMBER(12,2),
        $1:currency::VARCHAR(3),
        $1:shipping_method::VARCHAR(50),
        $1:notes::TEXT,
        $1:created_at::TIMESTAMP_TZ,
        $1:updated_at::TIMESTAMP_TZ
    FROM @CDC_DB.RAW.S3_CDC_STAGE/public/orders/
);

-- Pipe: order_items
CREATE OR REPLACE PIPE CDC_DB.RAW.PIPE_ORDER_ITEMS_CDC
    AUTO_INGEST = TRUE
    COMMENT     = 'Snowpipe: auto-ingest order_items CDC Parquet from S3'
AS
COPY INTO CDC_DB.RAW.ORDER_ITEMS_RAW (
    op,
    _dms_timestamp,
    item_id,
    order_id,
    product_id,
    product_name,
    quantity,
    unit_price,
    discount_pct,
    line_total,
    created_at
)
FROM (
    SELECT
        $1:Op::VARCHAR(1),
        $1:_timestamp::TIMESTAMP_NTZ,
        $1:item_id::NUMBER,
        $1:order_id::NUMBER,
        $1:product_id::NUMBER,
        $1:product_name::VARCHAR(255),
        $1:quantity::NUMBER,
        $1:unit_price::NUMBER(10,2),
        $1:discount_pct::NUMBER(5,2),
        $1:line_total::NUMBER(12,2),
        $1:created_at::TIMESTAMP_TZ
    FROM @CDC_DB.RAW.S3_CDC_STAGE/public/order_items/
);

-- ─────────────────────────────────────────────────────────────
-- STEP: Get SQS ARN for S3 event notification configuration
--   Copy the notification_channel value and paste into the
--   S3 bucket → Properties → Event Notifications → SQS ARN
-- ─────────────────────────────────────────────────────────────
SHOW PIPES IN SCHEMA CDC_DB.RAW;

-- Monitor pipe load history
SELECT SYSTEM$PIPE_STATUS('CDC_DB.RAW.PIPE_CUSTOMERS_CDC');
SELECT SYSTEM$PIPE_STATUS('CDC_DB.RAW.PIPE_ORDERS_CDC');
SELECT SYSTEM$PIPE_STATUS('CDC_DB.RAW.PIPE_ORDER_ITEMS_CDC');

-- Check ingestion history (last 24 hrs)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME    => 'CUSTOMERS_RAW',
    START_TIME    => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
