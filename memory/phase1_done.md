# Phase 1 Done — Bronze Ingestion Layer

## Ngày hoàn thành: 2026-06-04

---

## Mục tiêu phase này
Đẩy data từ Oracle (Core Banking) + PostgreSQL (CRM + Card) vào Bronze layer trên MinIO
dưới dạng Apache Iceberg tables, qua Nessie catalog. 2 mode: full snapshot + incremental.
Orchestrate bằng Airflow DAG.

---

## Những gì đã làm thành công (đã test thật, không chỉ viết code)

### 1. File code tạo ra
| File | Vai trò |
|---|---|
| `config/bronze_tables.yaml` | "Bản đồ" 8 bảng Bronze — thêm bảng mới chỉ sửa file này |
| `src/common/spark_session.py` | SparkSession chuẩn (Iceberg + MinIO + Nessie) + `get_jdbc_options` + `align_to_table` |
| `src/common/audit_logger.py` | Ghi `nessie.bronze.audit_log` mỗi lần chạy + `get_last_run_time` |
| `src/bronze/create_bronze_tables.py` | Tạo 9 Iceberg tables (chạy 1 lần) |
| `src/bronze/full_snapshot.py` | Full load OVERWRITE, idempotent |
| `src/bronze/incremental_load.py` | Incremental theo updated_at/created_at + MERGE INTO upsert |
| `src/bronze/verify_bronze.py` | Verify bằng spark.sql count (thay cho Trino — Trino chưa dựng) |
| `dags/dag_bronze_ingestion.py` | Airflow DAG: full_snapshot → incremental_load → quality_check |
| `docker/spark/Dockerfile` | apache/spark:3.5.1 + pyyaml → image `lakehouse-spark:3.5.1` |
| `docker/airflow/Dockerfile` | apache/airflow:2.9.3 + docker CLI tĩnh → image `lakehouse-airflow:2.9.3` |

### 2. Kết quả test end-to-end
```
9 Iceberg tables tạo trong nessie.bronze (8 bảng + audit_log)   ✅
Full snapshot: branches 2, products 3, bank_accounts 4, loans 2,
               bank_transactions 2, customers 5, card_accounts 4,
               card_transactions 5                              ✅
Idempotency:   chạy full_snapshot 2 lần → row count KHÔNG đổi    ✅
Incremental:   thêm CIF006 vào Postgres → pg_customers 5 → 6,
               CIF006 mang _batch_id của batch incremental      ✅
Audit log:     mỗi lần chạy có entry SUCCESS + rows_ingested     ✅
MinIO:         warehouse/bronze/ có đủ 9 table dir + parquet     ✅
Airflow DAG:   trigger thật, cả 3 task SUCCESS                   ✅
```

---

## ⚠️ QUAN TRỌNG — 5 điểm task draft bị sai/thiếu, đã sửa khi làm

Task file `task_2_bronze_ingestion.md` là draft. Khi triển khai thật phát hiện 5 vấn đề,
đã fix (code thực tế KHÁC draft ở các điểm này):

1. **`config/` chưa mount vào Spark container.**
   → Thêm mount `./config:/opt/spark/config` vào spark-master + spark-worker.

2. **`ORACLE_PASSWORD` / `POSTGRES_PASSWORD` không truyền vào Spark container** (chỉ có AWS keys).
   → Thêm `env_file: .env` cho spark-master + spark-worker. `get_jdbc_options` raise lỗi rõ ràng
     nếu thiếu password.

3. **URI Nessie sai.** `.env` có `ICEBERG_CATALOG_URI=.../iceberg` (endpoint Iceberg-REST) nhưng code
   dùng `NessieCatalog` (client native, cần REST API v2).
   → Dùng `NESSIE_URI=http://nessie:19120/api/v2`. Đã thêm biến này vào `.env`.

4. **`io-impl = S3FileIO` không chạy được.** S3FileIO dùng AWS SDK v2, KHÔNG có trong
   `aws-java-sdk-bundle:1.12.262` (SDK v1).
   → Đổi sang `org.apache.iceberg.hadoop.HadoopFileIO` (tái dùng S3A stack đã chạy ổn Phase 1).

5. **`spark.jars.packages` đặt trong builder KHÔNG tải jar** (Ivy resolve lúc launch JVM, trước khi
   code Python chạy).
   → Mọi `spark-submit` PHẢI truyền `--packages` trên CLI. DAG (BashOperator) cũng truyền `--packages`.

**Bonus fix:** `align_to_table()` trong spark_session.py — căn chỉnh schema nguồn cho khớp bảng đích
(Oracle JDBC trả tên cột VIẾT HOA + kiểu NUMBER→Decimal; bảng Bronze là chữ thường + DOUBLE).
Nếu không có hàm này, `writeTo()` / `MERGE INTO` sẽ lỗi type/schema mismatch.

**Thiếu pyyaml:** image `apache/spark:3.5.1` không có module `yaml` → bake vào `docker/spark/Dockerfile`.

---

## Quyết định scope (user chọn)
- **Airflow**: dựng thật trong docker-compose (LocalExecutor) + chạy DAG thật. ✅
- **Trino**: CHƯA dựng. Verify bằng spark-submit count (`verify_bronze.py`) thay cho Trino. (để Phase sau)
- **Airflow → Spark**: dùng `BashOperator` + `docker exec lakehouse-spark-master spark-submit ...`
  (mount `/var/run/docker.sock`). KHÔNG cài Java/Spark trong Airflow.

---

## Hạ tầng mới thêm vào docker-compose (so với Phase 1)

| Container | Image | Port | Ghi chú |
|---|---|---|---|
| `lakehouse-spark-master` | `lakehouse-spark:3.5.1` (build) | 8085, 7077 | đổi từ apache/spark → +pyyaml, +mount config, +env_file |
| `lakehouse-spark-worker` | `lakehouse-spark:3.5.1` (build) | — | tương tự |
| `lakehouse-airflow-init` | `lakehouse-airflow:2.9.3` (build) | — | one-shot: db migrate + tạo user admin/admin |
| `lakehouse-airflow-scheduler` | `lakehouse-airflow:2.9.3` | — | LocalExecutor, mount docker.sock |
| `lakehouse-airflow-webserver` | `lakehouse-airflow:2.9.3` | 8080 | UI, login admin/admin |

- Airflow metadata DB = database `airflow` trong Postgres sẵn có.
  **LƯU Ý:** volume postgres đã init từ Phase 1 nên init-postgres.sql KHÔNG tạo lại DB airflow.
  Đã tạo thủ công: `docker exec lakehouse-postgres psql -U postgres -c "CREATE DATABASE airflow"`.
  (Nếu reset sạch volume thì cần tạo lại DB này trước khi up airflow.)
- Airflow services chạy `user: "0:0"` để truy cập được docker.sock.

---

## Lệnh chạy thủ công (cheat sheet)

Biến packages dùng chung (PowerShell):
```powershell
$PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.postgresql:postgresql:42.7.3"
```
```powershell
# Tạo bảng (1 lần)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/create_bronze_tables.py
# Full snapshot
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/full_snapshot.py
# Incremental
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/incremental_load.py
# Verify
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/verify_bronze.py
```
> LƯU Ý môi trường: chạy `docker exec` qua **PowerShell**, KHÔNG qua Git Bash — Git Bash convert
> `/opt/...` thành đường dẫn Windows (`C:/Program Files/Git/opt/...`) → lỗi.

Airflow:
```powershell
docker exec lakehouse-airflow-scheduler airflow dags trigger bronze_ingestion
# UI: http://localhost:8080  (admin / admin)
```

---

## Phase tiếp theo

**Phase 3 — Silver Layer** (`tasks/task_silver_scd.md` — cần tạo)
- Đọc từ Bronze (`nessie.bronze.*`), apply SCD Type 2 (`dim_customer`) + SCD Type 1
  (`dim_account`, `dim_product`, `dim_branch`)
- Cleansing: null, duplicate, late arrival
- YAML-driven SQL transform
- Airflow DAG Bronze → Silver
- Silver layer đọc `nessie.bronze.audit_log` để biết last_run_time của Bronze
