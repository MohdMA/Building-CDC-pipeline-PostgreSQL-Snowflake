-- ============================================================
-- 03_snowflake_storage_integration_and_stage.sql
-- Snowflake: S3 Storage Integration + External Stage + File Format

-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CDC_DB;
USE SCHEMA RAW;
USE WAREHOUSE CDC_WH;

-- ─────────────────────────────────────────────────────────────
-- STEP 1: Create Storage Integration
--   This creates a managed IAM identity Snowflake uses to
--   access your S3 bucket without embedding AWS credentials.
-- ─────────────────────────────────────────────────────────────
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_CDC_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-s3-cdc-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET_NAME>/dms-cdc/')
    COMMENT                   = 'Storage integration for CDC pipeline S3 bucket';

-- ─────────────────────────────────────────────────────────────
-- STEP 2: Retrieve the Snowflake-managed IAM values

-- ─────────────────────────────────────────────────────────────
DESC INTEGRATION S3_CDC_INTEGRATION;
-- Look for:
--   STORAGE_AWS_IAM_USER_ARN   → add as Principal in trust policy
--   STORAGE_AWS_EXTERNAL_ID    → add as ExternalId condition in trust policy

-- ─────────────────────────────────────────────────────────────
-- STEP 3: Grant integration to pipeline role
-- ─────────────────────────────────────────────────────────────
GRANT USAGE ON INTEGRATION S3_CDC_INTEGRATION TO ROLE CDC_PIPELINE_ROLE;

-- ─────────────────────────────────────────────────────────────
-- STEP 4: Parquet file format
--   DMS writes CDC data as Parquet; this tells Snowflake how to read it.
-- ─────────────────────────────────────────────────────────────
USE ROLE CDC_PIPELINE_ROLE;

CREATE OR REPLACE FILE FORMAT CDC_DB.RAW.PARQUET_CDC_FORMAT
    TYPE                       = 'PARQUET'
    SNAPPY_COMPRESSION         = TRUE      -- DMS default compression
    BINARY_AS_TEXT             = FALSE
    COMMENT                    = 'Parquet format for DMS CDC output files';

-- ─────────────────────────────────────────────────────────────
-- STEP 5: External stage pointing at the S3 DMS output path
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE STAGE CDC_DB.RAW.S3_CDC_STAGE
    STORAGE_INTEGRATION = S3_CDC_INTEGRATION
    URL                 = 's3://<YOUR_BUCKET_NAME>/dms-cdc/'
    FILE_FORMAT         = CDC_DB.RAW.PARQUET_CDC_FORMAT
    COMMENT             = 'External stage: S3 DMS Parquet CDC files';

-- ─────────────────────────────────────────────────────────────
-- STEP 6: Verify stage is accessible (should list S3 files)
-- ─────────────────────────────────────────────────────────────
LIST @CDC_DB.RAW.S3_CDC_STAGE;
