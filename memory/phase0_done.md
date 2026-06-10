# Phase 0 Done — Foundation & Setup (Phase 0 theo plan_overall.md)

## Ngày hoàn thành: 2026-06-03

---

## Mục tiêu phase này
Dựng toàn bộ hạ tầng nguồn dữ liệu và storage layer, xác nhận Spark đọc/ghi được trước khi viết ETL thật.

---

## Những gì đã làm thành công

### 1. Cấu trúc thư mục dự án
Tạo đầy đủ toàn bộ cấu trúc:
```
lakehouse-customer360/
├── src/bronze, silver, gold, common
├── dags/
├── sql/bronze, silver, gold
├── scripts/
├── docker/trino/etc/catalog/
├── config/, notebooks/, tasks/, memory/
```

### 2. File cấu hình
| File | Nội dung |
|---|---|
| `.env` | Credentials: MinIO, PostgreSQL, Oracle, Spark, Nessie, Airflow |
| `.env.example` | Template không chứa giá trị thật (safe to commit) |
| `.gitignore` | Ignore `.env`, `venv/`, `__pycache__/` |
| `requirements.txt` | Local dev packages: pandas, pyarrow, psycopg2, oracledb, boto3, jupyterlab, pyiceberg, pynessie, loguru... |
| `requirements-docker.txt` | Tham khảo — pyspark và airflow chạy trong Docker, không cài local |

### 3. Python venv
- Tạo `venv/` bằng Python 3.12.5
- Cài đầy đủ 14 packages local (psycopg2-binary, oracledb, boto3, pandas, pyarrow, pyiceberg, pynessie, jupyterlab, python-dotenv, pyyaml, loguru, s3fs, numpy...)
- **Lý do tách khỏi airflow/pyspark:** Airflow và PySpark chạy 100% trong Docker — cài local gây dependency conflict với Python 3.12

### 4. Scripts SQL

**`scripts/init-postgres.sql`**
- 3 bảng: `customers`, `card_accounts`, `card_transactions`
- Index trên `updated_at` để hỗ trợ incremental load ở Phase 2
- Seed 5 customers (CIF001–CIF005), 4 card accounts, 5 card transactions

**`scripts/init-oracle.sql`**
- 5 bảng: `branches`, `products`, `bank_accounts`, `loans`, `bank_transactions`
- Index trên `updated_at` và `txn_date`
- Seed 2 branches, 3 products, 4 bank accounts, 2 loans, 2 transactions
- `customer_id` là FK nối xuyên suốt 2 hệ thống (Oracle chỉ lưu FK, không có bảng customers riêng)

### 5. Docker Compose — 6 services

| Container | Image thực tế dùng | Port | Ghi chú |
|---|---|---|---|
| `lakehouse-postgres` | `postgres:15-alpine` | 5432 | healthy |
| `lakehouse-oracle` | `gvenzl/oracle-xe:21-slim-faststart` | 1521 | healthy, ~7GB image |
| `lakehouse-minio` | `minio/minio:RELEASE.2024-05-10T01-41-38Z` | 9000, 9001 | healthy |
| `lakehouse-minio-init` | `minio/mc:latest` | — | one-shot, tạo bucket `lakehouse` |
| `lakehouse-nessie` | `ghcr.io/projectnessie/nessie:0.79.0` | 19120 | GHCR registry, không phải Docker Hub |
| `lakehouse-spark-master` | `apache/spark:3.5.1` | 8085, 7077 | official image, không phải bitnami |
| `lakehouse-spark-worker` | `apache/spark:3.5.1` | — | 2 cores, 3GB RAM, registered với master |

**Vấn đề gặp phải và cách fix:**
- `bitnami/spark:3.5.1` không tồn tại → đổi sang `apache/spark:3.5.1` (cần dùng command explicit thay vì SPARK_MODE env)
- `projectnessie/nessie:0.79.0` không có trên Docker Hub → đổi sang `ghcr.io/projectnessie/nessie:0.79.0`
- `minio/mc:RELEASE.2024-05-10T01-41-38Z` không tồn tại → đổi sang `minio/mc:latest`
- Oracle user `corebanking` không được tạo tự động (database volume đã tồn tại từ lần chạy trước) → tạo thủ công bằng SYSTEM, grant CONNECT + RESOURCE + UNLIMITED TABLESPACE, rồi chạy init script bằng `docker cp` + sqlplus

### 6. Kết nối DBeaver
- PostgreSQL: `localhost:5432` / `banking` / `postgres` → thấy 3 bảng ✅
- Oracle: `localhost:1521` / Service name `XEPDB1` / `corebanking` → thấy 5 bảng ✅

### 7. Test Spark end-to-end (`src/common/test_connections.py`)

Chạy bằng:
```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages 'org.postgresql:postgresql:42.7.3,com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262' \
  /opt/spark/jobs/common/test_connections.py
```

Kết quả:
```
PostgreSQL customers: 5 rows   ✅
Oracle bank_accounts: 4 rows   ✅
Ghi Parquet lên MinIO: OK      ✅
```

**Lưu ý:** `spark-submit` không có trong PATH của `apache/spark` image → phải dùng full path `/opt/spark/bin/spark-submit`

---

## Lệnh khởi động lại stack (khi tắt máy)

```powershell
cd "C:\Users\Administrator\Desktop\Myproject\Building Lakehouse for Customer bank 360"
docker compose up -d
```

Tất cả 6 container sẽ tự khởi động lại, data giữ nguyên trong Docker volumes.

---

## Phase tiếp theo

**Phase 2 — Bronze Layer** (`tasks/task_bronze_ingestion.md`)
- Viết Spark job full snapshot: Oracle → Bronze Iceberg table
- Viết Spark job full snapshot: PostgreSQL → Bronze Iceberg table
- Viết Spark job incremental load (theo `updated_at`)
- Audit Flag Table để track trạng thái mỗi lần chạy
- Airflow DAG Bronze với retry + idempotency
