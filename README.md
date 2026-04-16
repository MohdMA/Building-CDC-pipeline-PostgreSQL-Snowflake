# Building-CDC-pipeline-from-PostgreSQL-Snowflake
A production-grade Change Data Capture pipeline that streams real-time  events from PostgreSQL into Snowflake dimensional models


# PostgreSQL → AWS DMS → S3 → Snowpipe → Snowflake
## End-to-End CDC Pipeline with Local Virtual Test Environment


A production-grade Change Data Capture pipeline that streams real-time
INSERT / UPDATE / DELETE events from PostgreSQL into Snowflake dimensional models —
with a **fully runnable local simulation** requiring zero cloud accounts or
paid services to test.

---

## What makes this different

| Capability | This project |
|---|---|
| Runnable code | 4 Python modules — fully executable end-to-end |
| Local test environment | SQLite mocks for PostgreSQL + Snowflake; local folder mocks S3 |
| Automated tests | 46+ checks across 7 layers, zero external dependencies |
| Tables covered | `customers`, `orders`, `order_items` with line-level detail |
| CDC scenarios | INSERT / UPDATE / DELETE / rapid-update dedup / soft-delete all tested |
| Soft-delete pattern | `is_deleted` flag on all curated tables (no hard deletes) |
| Terraform IaC | S3 bucket + both IAM roles reproducible as code |
| IAM policies | 3 least-privilege JSON policies (Snowflake read, DMS write, trust) |
| DMS config | Task settings JSON + table mapping rules with lowercase transforms |
| Architecture GIF | 
| GitHub Actions CI | Runs on Python 3.9–3.12 on every push |

---

## Pipeline architecture

```
PostgreSQL (RDS)
  │  WAL logical replication — every INSERT / UPDATE / DELETE
  ▼
AWS DMS  (Full load + ongoing CDC)
  │  Writes Parquet + Snappy to S3
  ▼
Amazon S3  →  dms-cdc/public/{customers,orders,order_items}/*.parquet
  │  S3 ObjectCreated → SQS notification
  ▼
Snowpipe  (AUTO_INGEST = TRUE)
  │  COPY INTO RAW tables; tracks source file + load timestamp
  ▼
Snowflake RAW Layer  (append-only, immutable)
  ├── CUSTOMERS_RAW      (Op, _dms_timestamp, all source columns)
  ├── ORDERS_RAW
  └── ORDER_ITEMS_RAW
  │  Streams track high-watermark since last Task run
  ▼
Snowflake Streams + Tasks  (every 5 minutes, WHEN stream has data)
  │  QUALIFY ROW_NUMBER() deduplication → MERGE into curated
  ▼
Snowflake Curated Layer  (analytics-ready)
  ├── DIM_CUSTOMERS      (SCD Type 1 upsert, is_deleted)
  ├── FACT_ORDERS        (upsert, is_deleted)
  └── FACT_ORDER_ITEMS   (upsert, is_deleted)
```

---

## Repository structure

```
├── sql/
│   ├── 01_postgres_source_setup.sql          # WAL config, tables, seed data, DMS user
│   ├── 02_snowflake_environment_setup.sql    # DB, schemas, warehouses, role hierarchy
│   ├── 03_snowflake_storage_integration_and_stage.sql  # S3 integration + Parquet stage
│   ├── 04_snowflake_raw_tables_and_snowpipe.sql        # RAW tables + 3 Snowpipe pipes
│   ├── 05_snowflake_streams_tasks_curated.sql          # Streams, Tasks, DIM/FACT tables
│   └── 06_validation_queries.sql            # 10 e2e test queries + analytics
│
├── local_test/                              # ← Run this to test without any cloud
│   ├── run_pipeline_demo.py                 # Step-by-step demo with printed output
│   ├── postgres_mock/pg_source.py           # SQLite PostgreSQL + WAL log simulation
│   ├── dms_mock/dms_replicator.py           # DMS: WAL → local CSV files (= S3 Parquet)
│   ├── snowflake_mock/snowflake_engine.py   # Snowflake: RAW + Streams + Tasks + Curated
│   └── tests/test_pipeline.py              # 46+ automated tests across 7 layers
│
├── iam/
│   ├── snowflake_s3_policy.json             # Snowflake → S3 read (GetObject + ListBucket)
│   ├── snowflake_trust_policy.json          # IAM trust relationship for Snowflake role
│   └── dms_s3_write_policy.json            # DMS → S3 write (PutObject + DeleteObject)
│
├── dms/
│   ├── dms_task_settings.json              # DMS replication task settings
│   └── dms_table_mapping.json             # Table selection + lowercase transformations
│
├── terraform/
│   └── main.tf                             # S3 bucket + both IAM roles as code
│
├── scripts/
│   └── generate_architecture_gif.py        # Generates images/architecture.gif via Pillow
│
├── architecture/
│   └── architecture.mmd                    # Mermaid source diagram
│
├── images/
│   └── architecture.gif                    # Animated pipeline diagram
│
├── .github/workflows/ci.yml               # GitHub Actions: tests on Python 3.9–3.12
├── Makefile                               # make demo | make test | make gif | make clean
├── requirements.txt                       # Production deps (snowflake, boto3, pyarrow)
└── requirements_local.txt                 # Local test deps (pandas, Pillow, pytest)
```

---

## Quick start — local testing (no cloud required)

The entire pipeline can be tested locally using Python stdlib + pandas.
No AWS account. No Snowflake account. No Docker. No paid services.


## Tech stack

| Layer | Technology |
|---|---|
| Source database | PostgreSQL 14+ (AWS RDS) |
| CDC capture | AWS DMS (logical replication) |
| Landing zone | Amazon S3 (Parquet + Snappy) |
| Auto-ingestion | Snowflake Snowpipe (AUTO_INGEST via SQS) |
| RAW layer | Snowflake tables (append-only) |
| Incremental tracking | Snowflake Streams |
| Processing | Snowflake Tasks (scheduled MERGE) |
| Curated models | Snowflake DIM + FACT tables |
| Infrastructure | Terraform (AWS S3 + IAM) |
| Local simulation | Python stdlib + SQLite |
| Testing | Python unittest (no external test framework required) |
| CI/CD | GitHub Actions (Python 3.9–3.12) |
| Diagram | Pillow (animated GIF), Mermaid (.mmd) |

---

## License

MIT — free to use, modify, and distribute.

## Author

Mujahed Anwar 

Senior Data Engineer
GitHub: https://github.com/MohdMA