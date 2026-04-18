-- ============================================================
-- 02_snowflake_environment_setup.sql
-- Snowflake: Databases, Schemas, Warehouses, Roles

-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ─────────────────────────────────────────────────────────────
-- STEP 1: Create dedicated warehouse
-- ─────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS CDC_WH
    WITH WAREHOUSE_SIZE   = 'X-SMALL'
         AUTO_SUSPEND     = 60          -- seconds of inactivity
         AUTO_RESUME      = TRUE
         INITIALLY_SUSPENDED = TRUE
         COMMENT = 'Warehouse for CDC pipeline ingestion and transformation';

-- ─────────────────────────────────────────────────────────────
-- STEP 2: Create database and schemas (layered architecture)
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS CDC_DB
    COMMENT = 'End-to-end CDC pipeline: PostgreSQL → DMS → S3 → Snowpipe → Snowflake';

USE DATABASE CDC_DB;

-- RAW layer: immutable landing zone, preserves source fidelity
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Immutable RAW landing layer from Snowpipe';

-- CURATED layer: analytics-ready dimensional models
CREATE SCHEMA IF NOT EXISTS CURATED
    COMMENT = 'Curated dimensional models built from RAW via Streams + Tasks';

-- STAGING schema: intermediate work tables if needed
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Transient staging objects (optional)';

-- ─────────────────────────────────────────────────────────────
-- STEP 3: Role hierarchy
-- ─────────────────────────────────────────────────────────────
-- Pipeline role: owns Snowpipe, streams, tasks
CREATE ROLE IF NOT EXISTS CDC_PIPELINE_ROLE
    COMMENT = 'Role for CDC pipeline objects: Snowpipe, streams, tasks';

-- Analyst role: read-only on CURATED
CREATE ROLE IF NOT EXISTS CDC_ANALYST_ROLE
    COMMENT = 'Read-only access to CURATED dimensional models';

-- Grant hierarchy
GRANT ROLE CDC_PIPELINE_ROLE TO ROLE SYSADMIN;
GRANT ROLE CDC_ANALYST_ROLE  TO ROLE SYSADMIN;

-- Warehouse access
GRANT USAGE ON WAREHOUSE CDC_WH TO ROLE CDC_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE CDC_WH TO ROLE CDC_ANALYST_ROLE;

-- Database + schema access
GRANT USAGE ON DATABASE CDC_DB TO ROLE CDC_PIPELINE_ROLE;
GRANT USAGE ON DATABASE CDC_DB TO ROLE CDC_ANALYST_ROLE;

GRANT USAGE ON SCHEMA CDC_DB.RAW     TO ROLE CDC_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA CDC_DB.CURATED TO ROLE CDC_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA CDC_DB.CURATED TO ROLE CDC_ANALYST_ROLE;

-- Full DDL/DML on schemas for pipeline role
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CDC_DB.RAW     TO ROLE CDC_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CDC_DB.CURATED TO ROLE CDC_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CDC_DB.RAW     TO ROLE CDC_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CDC_DB.CURATED TO ROLE CDC_PIPELINE_ROLE;

-- Read-only on CURATED for analyst
GRANT SELECT ON ALL TABLES IN SCHEMA CDC_DB.CURATED TO ROLE CDC_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CDC_DB.CURATED TO ROLE CDC_ANALYST_ROLE;

USE ROLE SYSADMIN;
USE WAREHOUSE CDC_WH;
USE DATABASE CDC_DB;
