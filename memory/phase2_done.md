# Phase 2 Done — Silver Layer (Dimensional Model + SCD + Trino)

## Ngày hoàn thành: 2026-06-08

> Đánh số theo `plan_overall.md`: Foundation=0, Bronze=1, **Silver=2**, Gold=3, Governance=4.
> (Các file done cũ đánh lệch 1 — từ phase này thống nhất lại theo plan_overall.)

---

## Mục tiêu phase này
Đọc từ Bronze (`nessie.bronze.*`) → dựng dimensional model trên `nessie.silver.*`: dimension
SCD Type 1 & 2 + fact tables đã cleansing. YAML-driven, orchestrate bằng Airflow DAG
`silver_transform`, query được qua **Trino** (dựng mới phase này).

Quyết định scope (user chốt): **Dims + Facts** (không chỉ dims) · **dựng Trino ngay** ·
**tách riêng** dim_account / dim_card_account / dim_loan.

---

## File code tạo ra
| File | Vai trò |
|---|---|
| `config/silver_tables.yaml` | "Bản đồ" Silver — type(scd1/scd2/fact), source, natural_key, tracked_columns. Thêm bảng = sửa file này |
| `src/common/cleansing.py` | Helpers: clean_strings, upper_cols, dedup_latest, add_silver_meta, drop_null_keys |
| `src/common/audit_logger.py` | **REFACTOR**: `AuditLogger(spark, audit_table=...)` — param hoá; Silver dùng `nessie.silver.audit_log`, Bronze giữ default. Chữ ký log()/get_last_run_time() không đổi |
| `src/silver/create_silver_tables.py` | Tạo namespace + 9 bảng (6 dim + 2 fact + audit_log) |
| `src/silver/transform_scd1.py` | SCD1 generic — MERGE upsert (giá trị mới nhất, không lịch sử) |
| `src/silver/transform_scd2.py` | SCD2 `dim_customer` — 2-statement (đóng version cũ + append version mới) |
| `src/silver/transform_fact.py` | Fact — cleansing + MERGE insert (append idempotent) |
| `src/silver/run_silver.py` | Orchestrator: loop YAML, dispatch theo type, ghi silver.audit_log |
| `src/silver/verify_silver.py` | Count tất cả bảng + audit log + demo lịch sử SCD2 |
| `dags/dag_silver_transform.py` | DAG `silver_transform`: silver_transform → quality_check (3 AM) |
| `docker/trino/etc/{node,jvm,config}.properties` + `catalog/iceberg.properties` | Trino 446 config |
| `docker-compose.yml` | +service `lakehouse-trino` (port 8088) |

**Tái dùng Phase 1 (không viết lại):** `get_spark_session`, `align_to_table`, `SPARK_PACKAGES`,
pattern DAG BashOperator + `docker exec spark-submit`.

---

## Mô hình 9 bảng `nessie.silver.*`
| Bảng | Loại | Nguồn Bronze | Natural key |
|---|---|---|---|
| `dim_customer` | SCD2 | pg_customers | customer_id |
| `dim_account` | SCD1 | oracle_bank_accounts | account_id |
| `dim_card_account` | SCD1 | pg_card_accounts | account_id |
| `dim_product` | SCD1 | oracle_products | product_id |
| `dim_branch` | SCD1 | oracle_branches | branch_id |
| `dim_loan` | SCD1 | oracle_loans | loan_id |
| `fct_bank_transactions` | Fact | oracle_bank_transactions | txn_id (part. days(txn_date)) |
| `fct_card_transactions` | Fact | pg_card_transactions | txn_id (part. days(txn_date)) |
| `audit_log` | Audit | — | — (part. days(created_at)) |

---

## Kết quả test end-to-end (đã chạy thật)
```
9 Silver tables tạo trong nessie.silver                                ✅
Transform lần 1: dim_customer 6 (SCD2 NEW v1), dim_account 4,
   dim_card_account 4, dim_product 3, dim_branch 2, dim_loan 2,
   fct_bank_transactions 2, fct_card_transactions 5 — 8/8 SUCCESS      ✅
Idempotency: chạy run_silver lần 2 → dim_customer "khong co thay doi"
   (0 version mới), fact không nhân đôi                                ✅
SCD2 versioning: đổi CIF002 segment ở Postgres → incremental bronze →
   silver → CIF002 có 2 row: v1 (closed, is_current=false, effective_to
   set) + v2 (current, is_current=true, effective_to=NULL)            ✅
Trino: SHOW SCHEMAS thấy bronze+silver; query dim_customer SCD2 history
   + fact counts qua trino CLI OK                                      ✅
Airflow: DAG silver_transform trigger thật → run state SUCCESS         ✅
```

---

## ⚠️ QUAN TRỌNG — 3 điểm khác draft (đã fix khi làm thật)

1. **SCD2 versioning sai nếu chỉ `persist()`** (BUG correctness, đã sửa).
   Cơ chế 2-statement: Statement A (MERGE đóng version cũ) → Statement B (append version
   mới với `version = COALESCE(_old_version,0)+1`). DataFrame `changes` (chứa `_old_version`
   lấy từ join với current rows)派 sinh từ **bảng target**. MERGE ở Statement A **invalidate
   cache theo target** → Statement B tính lại `changes` trên current rows ĐÃ bị đóng →
   `_old_version=NULL` → `version` luôn = 1 (bản đổi cũng ra v1 thay vì v2).
   → **Fix:** `changes = changes.localCheckpoint(eager=True)` cắt lineage, materialize độc
   lập với target. (`persist()` KHÔNG đủ.) `transform_scd2.py` dùng localCheckpoint.

2. **Trino Nessie connector dùng API v1, KHÔNG phải v2.**
   Lỗi gặp: `API version mismatch, check URI prefix (expected: 1, actual: 2)`.
   → `iceberg.nessie-catalog.uri=http://nessie:19120/api/v1` trong Trino.
   **Khác Spark** (NessieCatalog của Spark dùng `/api/v2` — xem `spark_session.py` ghi chú #1).
   Cùng 1 Nessie server, 2 client trỏ 2 version endpoint khác nhau.

3. **Service Trino KHÔNG khai báo `networks`.** docker-compose.yml này không định nghĩa
   network nào → mọi service dùng default network `<project>_default`. Draft ghi
   `networks: - lakehouse` (không tồn tại) → đã bỏ.

**Cấu hình Trino 446 (filesystem mới):** `fs.native-s3.enabled=true` + nhóm `s3.*`
(`s3.endpoint`, `s3.path-style-access`, `s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}`),
KHÔNG dùng `hive.s3.*` cũ. `env_file: .env` cấp AWS keys cho `${ENV:...}`.

---

## ⚠️ Gotcha hạ tầng — Nessie IN_MEMORY mất catalog khi restart
`docker-compose.yml`: nessie `nessie.version.store.type=IN_MEMORY`. Khi container Nessie
restart (vd Docker Desktop khởi động lại), **toàn bộ table refs trong catalog bị mất** —
parquet vẫn còn trong MinIO nhưng `nessie.bronze.*` / `nessie.silver.*` báo TABLE_NOT_FOUND.
- Hệ quả phiên này: sau khi máy restart, phải chạy lại `create_bronze_tables.py` +
  `full_snapshot.py` rồi mới tới Silver. Postgres/Oracle data vẫn còn (named volume).
- **Khuyến nghị Phase sau:** đổi Nessie sang version store bền (`ROCKSDB` + volume, hoặc
  `JDBC` trỏ Postgres) để catalog sống sót qua restart. Hiện chưa làm.

---

## Trino — kết nối
- Container `lakehouse-trino` (image `trinodb/trino:446`), host port **8088** (8080 đã bị
  Airflow webserver chiếm).
- CLI test: `docker exec lakehouse-trino trino --execute "SHOW TABLES FROM iceberg.silver"`
- DBeaver: driver Trino, host `localhost`, port `8088`, user bất kỳ (vd `admin`), no password.
  Query `iceberg.silver.*` / `iceberg.bronze.*`.

---

## Lệnh chạy thủ công (cheat sheet) — PowerShell, KHÔNG Git Bash
`$PKGS` giống Phase 1 (xem phase1_done.md). Thêm `--conf spark.log.level=WARN` để bớt log INFO.
```powershell
# Tạo Silver tables (1 lần)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/create_silver_tables.py
# Transform Bronze -> Silver
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
# Verify (+ demo SCD2 history 1 customer)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/verify_silver.py CIF002
```
Airflow: `docker exec lakehouse-airflow-scheduler airflow dags trigger silver_transform`
(DAG tạo ra ở trạng thái paused do `DAGS_ARE_PAUSED_AT_CREATION=true` → cần `airflow dags unpause silver_transform` trước).

> **Exit code 255 lành tính:** mọi spark-submit trong phiên trả exit 255 lúc shutdown (netty
> đóng stream jar aws-sdk-bundle khi SparkContext stop). Job/commit vẫn thành công — kiểm tra
> bằng output `OK ...` / audit log, KHÔNG dựa vào exit code.

---

## Phase tiếp theo — Phase 3 (Gold)
Tạo `tasks/task_4_gold_mart.md`: star schema, `mart_customer_360` 25+ KPI, RFM segment,
cross-sell flags, DAG Silver → Gold. Gold đọc `nessie.silver.dim_*` + `fct_*` (đã sẵn nhờ
scope Dims+Facts). Cân nhắc làm Nessie bền trước (xem gotcha trên).
