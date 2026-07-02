# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An end-to-end on-premise **Lakehouse Customer 360** for a retail bank. It unifies two source systems (Oracle Core Banking + PostgreSQL CRM/Card) into a Medallion architecture (Bronze → Silver → Gold) on **Apache Iceberg + MinIO**, cataloged by **Nessie**, transformed by **Spark**, queried by **Trino**, orchestrated by **Airflow** — all in Docker Compose. The headline output is `nessie.gold.mart_customer_360`: one row per customer with 38 KPIs, RFM segmentation, cross-sell flags, and PII masking.

The codebase, comments, and `memory/` notes are written in Vietnamese. Match that language when editing files that are already Vietnamese.

## Environment & critical gotchas

- **Run `docker exec` commands from PowerShell, NOT Git Bash.** Git Bash rewrites `/opt/...` paths into Windows paths (`C:/Program Files/Git/opt/...`) and the job fails.
- **Spark jobs MUST receive `--packages` on the `spark-submit` CLI.** Setting `spark.jars.packages` in the SparkSession builder does NOT download jars — Ivy resolves them at JVM launch, before Python runs. `SPARK_PACKAGES` is defined in `src/common/spark_session.py` (and duplicated as a string literal in each DAG).
- **Nessie URI is `http://nessie:19120/api/v2`** (REST API v2 for the native `NessieCatalog` client) — NOT the `/iceberg` REST endpoint.
- **`io-impl` is `HadoopFileIO`, not `S3FileIO`.** S3FileIO needs AWS SDK v2; the pinned `aws-java-sdk-bundle:1.12.262` is SDK v1. HadoopFileIO reuses the working S3A stack.
- **Nessie runs with `gc.enabled=false`** (version management is catalog-level). Per-table `expire_snapshots` / `remove_orphan_files` are UNSUPPORTED; use `nessie-gc` at catalog level instead.

## How the project runs

### Start the stack
```bash
cp .env.example .env      # fill credentials
docker compose up -d      # postgres, oracle-xe, minio(+init), nessie, spark master/worker, trino, airflow init/scheduler/webserver, data-simulator
```

### Generate sample data (host Python, ~800 customers, 18 months of txns)
```bash
python -m venv venv && venv/Scripts/activate
pip install -r requirements.txt
python scripts/generate_data.py
```

### Run a Spark job manually
All Spark code is mounted at `/opt/spark/jobs` (← `./src`) and configs at `/opt/spark/config` (← `./config`). Pattern:
```powershell
$PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.postgresql:postgresql:42.7.3"
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/<layer>/<job>.py
```
Full pipeline order: `bronze/create_bronze_tables.py` → `bronze/full_snapshot.py` (or `incremental_load.py`) → `silver/create_silver_tables.py` → `silver/run_silver.py` → `gold/create_gold_tables.py` → `gold/run_gold.py`. Each layer has a `verify_*.py` quality-check job.

### Run via Airflow
UI at http://localhost:8080 (admin/admin). DAGs: `bronze_ingestion` (2 AM) → `silver_transform` (3 AM) → `gold_mart`, plus `maintenance` (weekly) and `daily_simulation`. DAGs use `BashOperator` + `docker exec lakehouse-spark-master spark-submit ...` (Airflow image has no Java/Spark; it mounts `/var/run/docker.sock`).
```powershell
docker exec lakehouse-airflow-scheduler airflow dags trigger silver_transform
```

### Query via Trino
Host port **8088** (8080 is taken by Airflow). DBeaver: Trino driver, `localhost:8088`.
```bash
docker exec lakehouse-trino trino --execute "SELECT rfm_segment, count(*) FROM iceberg.gold.mart_customer_360 GROUP BY rfm_segment"
```

## Architecture

### Layer flow & the three orchestrators
- **Bronze** (`src/bronze/`) — JDBC ingest from Oracle + Postgres into 8 Iceberg tables + `audit_log`. Two modes: `full_snapshot.py` (OVERWRITE, idempotent) and `incremental_load.py` (filter on `updated_at`/`created_at`, then `MERGE INTO` upsert).
- **Silver** (`src/silver/`) — `run_silver.py` loops `config/silver_tables.yaml` and dispatches each table **independently** by `type` to `transform_scd1` / `transform_scd2` / `transform_fact`. A failure logs and continues to the next table.
- **Gold** (`src/gold/`) — `run_gold.py` runs a **dependent** pipeline (`agg_customer_holdings` → `agg_customer_txn_12m` → `mart_customer_360`); a failed agg raises and stops the pipeline because the mart depends on both aggs. It resolves an `as_of` date (default `auto` = `max(txn_date)` across both silver facts) that anchors all time-window KPIs.

### YAML-driven design (the key extensibility point)
Adding or changing tables/logic means editing a config file, not writing new orchestration code:
- `config/bronze_tables.yaml` — the 8 source tables and their JDBC source config.
- `config/silver_tables.yaml` — per table: `type` (scd1/scd2/fact), `source` Bronze table, `natural_key`; for scd2 also `tracked_columns` (changes spawn a new version) vs `attributes` (carried but don't trigger a version); for fact a `partition` column.
- `config/gold_tables.yaml` — `as_of_date`, `lookback_months`, RFM `n_tiles`, and **cross-sell rules as SQL expression strings** interpolated directly into `build_mart_360`'s SQL (referenced columns must exist in the mart).
- `config/simulation.yaml` — drives the daily data simulator.

### Shared modules (`src/common/`) — read before writing any Spark job
- `spark_session.py` — `get_spark_session()` (the single source of Iceberg/Nessie/MinIO config), `get_jdbc_options()` (reads passwords from container ENV; raises clearly if missing), and `align_to_table()` — **always use this before `writeTo()`/`MERGE INTO`**. Oracle JDBC returns UPPERCASE column names and NUMBER→Decimal; Bronze tables are lowercase + DOUBLE. `align_to_table` maps by case-insensitive name and casts to the target schema, preventing schema/type-mismatch failures.
- `audit_logger.py` — `AuditLogger(spark, "nessie.<layer>.audit_log")`; every job logs batch_id, rows, status, run times. `get_last_run_time()` feeds incremental loads.
- `masking.py` — PII masking helpers applied **inside the Gold build** (`build_mart_360.py`), so masking happens on every run with no bypass. Silver keeps real PII (internal); Gold (widely queried) is PII-clean: `full_name` redacted, `phone_masked`, `email_masked`, `id_number` SHA-256 hashed.
- `cleansing.py` — null/dedupe/late-arrival helpers used by Silver transforms.

### Web dashboard (`dashboard/`) — downstream, decoupled from the stack
A static, serverless visualization of the Gold mart, deployable free to Vercel. It does **not** query the lakehouse live: Trino/MinIO run on `localhost` and cloud can't reach them, and the mart is only ~800 rows/snapshot, so the model is **export a snapshot to static Parquet, query it client-side**. `dashboard/export_mart.py` (host, in venv — needs the `trino` client from `requirements.txt`, stack must be up) reads `iceberg.gold.mart_customer_360` plus day-grain trend/active aggregates from the silver facts, writing `dashboard/public/data/*.parquet` + `meta.json`. `dashboard/public/` is the web root: `index.html` + `app.js` load the Parquet with **DuckDB-Wasm** and chart it with **ECharts** entirely in the browser. Refresh = re-run `export_mart.py` after a new Gold snapshot, then redeploy. The dashboard carries no PII because the mart is already masked.

### Governance (`src/governance/`)
Schema evolution via `ALTER TABLE ADD COLUMN` (no data rewrite), time-travel demos (`FOR VERSION/TIMESTAMP AS OF`), and `mask_legacy_partitions.py` (the Gold mart accumulates one `snapshot_date` partition per run — see the double-count follow-up noted in `memory/phase4_done.md`). Maintenance/compaction lives in `src/maintenance/maintain_tables.py` (`rewrite_data_files` + `rewrite_manifests`).

## Working with this repo

- **`memory/` is the project journal** — `MEMORY.md` indexes per-phase notes (`phase0_done.md` … `phase4_done.md`, `final_done.md`). Each records design decisions, real bugs hit, and fixes. Read the relevant phase note before changing that layer; the task files in `tasks/` are *drafts* and known to diverge from the implemented code (Phase 1 alone fixed 5 draft errors). Trust `src/` + `memory/` over `tasks/`.
- Naming: Spark/Iceberg catalog is `nessie` (`nessie.bronze.*`, `nessie.silver.*`, `nessie.gold.*`); the same tables are exposed in Trino under the `iceberg` catalog (`iceberg.gold.mart_customer_360`).
- The Airflow metadata DB is the `airflow` database inside the existing Postgres. If you reset the postgres volume, recreate it before starting Airflow: `docker exec lakehouse-postgres psql -U postgres -c "CREATE DATABASE airflow"`.
