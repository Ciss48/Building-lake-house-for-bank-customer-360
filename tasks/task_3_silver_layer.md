# Task: Silver Layer — Phase 2 (Dimensional Model + SCD + Trino)

## Mục tiêu
Đọc data từ Bronze (`nessie.bronze.*`) → xây **dimensional model** chuẩn ngân hàng trên
`nessie.silver.*`: dimension tables (SCD Type 1 & 2) + fact tables đã cleansing.
Toàn bộ YAML-driven, orchestrate bằng Airflow DAG `silver_transform`, và **queryable qua
Trino** (dựng mới trong phase này).

## Trạng thái
- [x] **ĐÃ XONG (2026-06-08)** — implement + test end-to-end thật. Chi tiết kết quả &
  2 fix khác draft xem `memory/phase2_done.md`.
  - Khác draft khi làm thật: (1) SCD2 phải `localCheckpoint(eager=True)` trên `changes`
    (persist không đủ — MERGE Statement A invalidate cache theo target → version sai);
    (2) Trino Nessie connector dùng **`/api/v1`** (KHÁC Spark dùng `/api/v2`); (3) service
    Trino KHÔNG khai báo `networks` (compose dùng default network).

> **Lưu ý đánh số phase:** theo `plan_overall.md` đây là **Phase 2 — Silver** (Foundation=0,
> Bronze=1, Silver=2, Gold=3, Governance=4). Các file `phaseX_done.md` cũ đánh lệch 1 số —
> khi viết `phase2_done.md` nên thống nhất lại theo `plan_overall.md`.

---

## Quyết định scope (đã chốt với user)
1. **Silver = Dims + Facts** (không chỉ dims) → đủ nguyên liệu cho Gold tính 25+ KPI.
2. **Dựng Trino ngay** trong phase này → verify Silver bằng SQL thật qua DBeaver.
3. **Tách riêng** `dim_account` (bank) / `dim_card_account` (card) / `dim_loan`
   (schema bank vs card khác nhau → không gộp).

---

## Checklist tổng

- [ ] Bước 1: Config YAML `config/silver_tables.yaml` (bản đồ Silver)
- [ ] Bước 2: Module cleansing dùng chung `src/common/cleansing.py`
- [ ] Bước 3: Refactor `src/common/audit_logger.py` → param hoá audit table (Silver dùng `nessie.silver.audit_log`)
- [ ] Bước 4: `src/silver/create_silver_tables.py` — tạo namespace + 9 bảng (chạy 1 lần)
- [ ] Bước 5: `src/silver/transform_scd1.py` — SCD1 generic (MERGE upsert)
- [ ] Bước 6: `src/silver/transform_scd2.py` — SCD2 cho `dim_customer` (2-statement)
- [ ] Bước 7: `src/silver/transform_fact.py` — cleansing + MERGE insert facts
- [ ] Bước 8: `src/silver/run_silver.py` — orchestrator loop YAML, dispatch theo type
- [ ] Bước 9: `src/silver/verify_silver.py` — count + demo lịch sử SCD2
- [ ] Bước 10: Dựng Trino (`docker/trino/etc/*` + service trong `docker-compose.yml`)
- [ ] Bước 11: `dags/dag_silver_transform.py` — Bronze → Silver DAG
- [ ] Bước 12: Test full → idempotency → SCD2 demo → Trino query → Airflow trigger

---

## Mô hình bảng `nessie.silver.*`

| Bảng Silver | Loại | Nguồn Bronze | Natural key | Partition |
|---|---|---|---|---|
| `dim_customer` | **SCD Type 2** | `bronze.pg_customers` | customer_id | — |
| `dim_account` | SCD Type 1 | `bronze.oracle_bank_accounts` | account_id | — |
| `dim_card_account` | SCD Type 1 | `bronze.pg_card_accounts` | account_id | — |
| `dim_product` | SCD Type 1 | `bronze.oracle_products` | product_id | — |
| `dim_branch` | SCD Type 1 | `bronze.oracle_branches` | branch_id | — |
| `dim_loan` | SCD Type 1 | `bronze.oracle_loans` | loan_id | — |
| `fct_bank_transactions` | Fact (append/dedup) | `bronze.oracle_bank_transactions` | txn_id | days(txn_date) |
| `fct_card_transactions` | Fact (append/dedup) | `bronze.pg_card_transactions` | txn_id | days(txn_date) |
| `audit_log` | Audit | — | — | days(created_at) |

---

## Cấu trúc file sẽ tạo ra

```
lakehouse-customer360/
├── config/
│   └── silver_tables.yaml             ← bản đồ Silver (type, source, keys, tracked cols)
├── src/
│   ├── common/
│   │   ├── cleansing.py               ← helpers cleansing dùng chung  (MỚI)
│   │   └── audit_logger.py            ← REFACTOR: param hoá audit table
│   └── silver/
│       ├── create_silver_tables.py    ← tạo 9 Iceberg tables (chạy 1 lần)
│       ├── transform_scd1.py          ← SCD1 generic
│       ├── transform_scd2.py          ← SCD2 dim_customer
│       ├── transform_fact.py          ← cleansing + append facts
│       ├── run_silver.py              ← orchestrator
│       └── verify_silver.py           ← verify + demo SCD2
├── docker/trino/etc/
│   ├── config.properties
│   ├── jvm.config
│   ├── node.properties
│   └── catalog/iceberg.properties     ← Iceberg + Nessie + MinIO
├── dags/
│   └── dag_silver_transform.py        ← Bronze → Silver DAG
└── docker-compose.yml                 ← thêm service lakehouse-trino
```

> **Tái dùng Phase 1 (KHÔNG viết lại):** `src/common/spark_session.py`
> (`get_spark_session`, `align_to_table`, `SPARK_PACKAGES`), pattern `--packages` trên CLI,
> pattern DAG BashOperator + `docker exec lakehouse-spark-master spark-submit`.

---

## Bước 1 — Config YAML `config/silver_tables.yaml`

"Bản đồ" toàn bộ Silver layer. Thêm bảng mới chỉ sửa file này.

```yaml
# config/silver_tables.yaml
# type: scd1 | scd2 | fact
# source: bảng Bronze nguồn (nessie.bronze.<source>)
# natural_key: khoá tự nhiên (PK nghiệp vụ)
# tracked_columns: (chỉ scd2) cột mà khi đổi → sinh version mới
# attributes: (scd2) cột giữ trong mỗi version nhưng KHÔNG trigger version mới
# partition: (fact) cột partition theo ngày

tables:

  dim_customer:
    type: scd2
    source: pg_customers
    natural_key: customer_id
    tracked_columns: [customer_segment, city, province, kyc_status]
    attributes: [full_name, date_of_birth, gender, id_number, phone, email]

  dim_account:
    type: scd1
    source: oracle_bank_accounts
    natural_key: account_id

  dim_card_account:
    type: scd1
    source: pg_card_accounts
    natural_key: account_id

  dim_product:
    type: scd1
    source: oracle_products
    natural_key: product_id

  dim_branch:
    type: scd1
    source: oracle_branches
    natural_key: branch_id

  dim_loan:
    type: scd1
    source: oracle_loans
    natural_key: loan_id

  fct_bank_transactions:
    type: fact
    source: oracle_bank_transactions
    natural_key: txn_id
    partition: txn_date

  fct_card_transactions:
    type: fact
    source: pg_card_transactions
    natural_key: txn_id
    partition: txn_date
```

---

## Bước 2 — Module cleansing `src/common/cleansing.py`

```python
# src/common/cleansing.py
"""Helpers cleansing dùng chung cho Silver layer."""
from datetime import datetime, timezone
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim mọi cột string; chuỗi rỗng '' → NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            c = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(c == "", None).otherwise(c))
    return df


def upper_cols(df: DataFrame, cols: list) -> DataFrame:
    """Chuẩn hoá UPPERCASE cho status/currency... (bỏ qua cột không tồn tại)."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.col(c)))
    return df


def dedup_latest(df: DataFrame, keys: list, order_col: str = "_ingested_at") -> DataFrame:
    """Giữ row mới nhất theo order_col cho mỗi natural key (xử lý late arrival/trùng PK)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def add_silver_meta(df: DataFrame, batch_id: str) -> DataFrame:
    """Thêm metadata Silver."""
    return (df
            .withColumn("_silver_loaded_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
            .withColumn("_silver_batch_id",  F.lit(batch_id)))


def drop_null_keys(df: DataFrame, keys: list) -> DataFrame:
    """Loại row có natural key NULL (data lỗi)."""
    cond = None
    for k in keys:
        c = F.col(k).isNotNull()
        cond = c if cond is None else (cond & c)
    return df.filter(cond) if cond is not None else df
```

---

## Bước 3 — Refactor `src/common/audit_logger.py`

Hiện `AuditLogger` hardcode `nessie.bronze.audit_log`. Param hoá để Silver dùng
`nessie.silver.audit_log`. **Giữ nguyên** chữ ký `log()` / `get_last_run_time()` để Bronze
không phải sửa.

```python
# Sửa __init__ + bỏ hằng số _AUDIT_TABLE cứng:
class AuditLogger:
    def __init__(self, spark, audit_table: str = "nessie.bronze.audit_log"):
        self.spark = spark
        self.audit_table = audit_table
    # ... trong log(): df.writeTo(self.audit_table).append()
    # ... trong get_last_run_time(): self.spark.table(self.audit_table)...
```

Silver khởi tạo: `AuditLogger(spark, "nessie.silver.audit_log")`.
Bronze giữ nguyên `AuditLogger(spark)` → vẫn trỏ bronze (mặc định).

---

## Bước 4 — `src/silver/create_silver_tables.py` (chạy 1 lần)

Tạo namespace `nessie.silver` + 9 bảng. Schema dim = cột nghiệp vụ đã cleansing +
metadata Silver; SCD2 thêm cột versioning.

```python
# src/silver/create_silver_tables.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session


def create(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    # ── dim_customer (SCD Type 2) ────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_customer (
            customer_sk      STRING,        -- surrogate key
            customer_id      STRING,        -- natural key
            full_name        STRING,
            date_of_birth    DATE,
            gender           STRING,
            id_number        STRING,
            phone            STRING,
            email            STRING,
            city             STRING,
            province         STRING,
            customer_segment STRING,
            kyc_status       STRING,
            row_hash         STRING,        -- hash các tracked column
            effective_from   TIMESTAMP,
            effective_to     TIMESTAMP,
            is_current       BOOLEAN,
            version          INT,
            _silver_loaded_at TIMESTAMP,
            _silver_batch_id  STRING
        ) USING iceberg
        TBLPROPERTIES ('write.format.default'='parquet',
                       'write.parquet.compression-codec'='snappy')
    """)

    # ── dim_account (SCD1) ───────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_account (
            account_id    STRING, customer_id STRING, product_id STRING, branch_id STRING,
            account_type  STRING, account_no STRING, balance DOUBLE, currency STRING,
            status STRING, opened_date DATE, closed_date DATE, interest_rate DOUBLE,
            updated_at TIMESTAMP, _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_card_account (SCD1) ──────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_card_account (
            account_id STRING, customer_id STRING, card_type STRING, card_number STRING,
            credit_limit DOUBLE, outstanding_balance DOUBLE, due_date DATE, status STRING,
            opened_date DATE, closed_date DATE, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_product (SCD1) ───────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_product (
            product_id STRING, product_name STRING, product_type STRING,
            interest_rate DOUBLE, status STRING, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_branch (SCD1) ────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_branch (
            branch_id STRING, branch_name STRING, region STRING, city STRING,
            branch_type STRING, status STRING, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_loan (SCD1) ──────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_loan (
            loan_id STRING, customer_id STRING, product_id STRING, branch_id STRING,
            loan_type STRING, principal_amount DOUBLE, outstanding_amount DOUBLE,
            interest_rate DOUBLE, disbursement_date DATE, maturity_date DATE,
            npl_status STRING, status STRING, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── fct_bank_transactions (Fact) ─────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.fct_bank_transactions (
            txn_id STRING, account_id STRING, txn_date DATE, txn_datetime TIMESTAMP,
            txn_type STRING, amount DOUBLE, balance_after DOUBLE, channel STRING,
            status STRING, created_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # ── fct_card_transactions (Fact) ─────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.fct_card_transactions (
            txn_id STRING, account_id STRING, txn_date DATE, txn_datetime TIMESTAMP,
            amount DOUBLE, currency STRING, txn_type STRING, merchant_name STRING,
            merchant_category STRING, channel STRING, status STRING, created_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # ── audit_log (giống bronze) ─────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.audit_log (
            batch_id STRING, table_name STRING, source_system STRING, run_mode STRING,
            status STRING, rows_ingested LONG, last_run_time TIMESTAMP,
            current_run_time TIMESTAMP, error_message STRING, created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    print("Tat ca Silver tables da tao xong trong nessie.silver")
    spark.sql("SHOW TABLES IN nessie.silver").show(truncate=False)


if __name__ == "__main__":
    spark = get_spark_session("create-silver-tables")
    create(spark)
    spark.stop()
```

---

## Bước 5 — SCD1 generic `src/silver/transform_scd1.py`

Đọc full Bronze → cleansing → dedup latest → MERGE upsert (giá trị mới nhất, không lịch sử).

```python
# src/silver/transform_scd1.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session, align_to_table
from common.cleansing import clean_strings, upper_cols, dedup_latest, add_silver_meta, drop_null_keys

# cột nên chuẩn hoá UPPERCASE nếu có
_UPPER = ["status", "currency", "npl_status", "account_type", "card_type",
          "product_type", "branch_type", "kyc_status"]


def transform_scd1(spark, table_name: str, cfg: dict, batch_id: str) -> int:
    source       = f"nessie.bronze.{cfg['source']}"
    target       = f"nessie.silver.{table_name}"
    nk           = cfg["natural_key"]

    df = spark.table(source)
    df = clean_strings(df)
    df = upper_cols(df, _UPPER)
    df = drop_null_keys(df, [nk])
    df = dedup_latest(df, [nk], "_ingested_at")
    df = add_silver_meta(df, batch_id)
    df = align_to_table(spark, df, target)   # cast khớp schema đích, bỏ cột thừa (_batch_id Bronze...)

    cnt = df.count()
    df.createOrReplaceTempView("scd1_src")
    spark.sql(f"""
        MERGE INTO {target} AS t USING scd1_src AS s
        ON t.{nk} = s.{nk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  OK SCD1 {table_name}: upsert {cnt} rows")
    return cnt
```

> `align_to_table` lấy schema bảng Silver đích → tự bỏ các cột Bronze-only
> (`_ingested_at`, `_source_system`, `_batch_id`) và thêm 2 cột `_silver_*`. Tiện lợi.

---

## Bước 6 — SCD2 `src/silver/transform_scd2.py` (dim_customer)

**Cơ chế 2-statement** (Iceberg MERGE không vừa đóng version cũ vừa mở version mới trong 1
lệnh). Idempotent nhờ `row_hash`.

```python
# src/silver/transform_scd2.py
import sys
from datetime import datetime, timezone
sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.cleansing import clean_strings, upper_cols, dedup_latest, drop_null_keys

_UPPER = ["customer_segment", "kyc_status"]


def transform_scd2(spark, table_name: str, cfg: dict, batch_id: str) -> int:
    source   = f"nessie.bronze.{cfg['source']}"
    target   = f"nessie.silver.{table_name}"
    nk       = cfg["natural_key"]
    tracked  = cfg["tracked_columns"]
    attrs    = cfg["attributes"]
    now      = datetime.now(timezone.utc)

    # 1) Snapshot mới nhất mỗi natural key + row_hash trên tracked cols
    src = spark.table(source)
    src = clean_strings(src)
    src = upper_cols(src, _UPPER)
    src = drop_null_keys(src, [nk])
    src = dedup_latest(src, [nk], "_ingested_at")
    src = src.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked])))

    src.createOrReplaceTempView("scd2_src")

    # 2) current version trong target
    cur = spark.table(target).filter("is_current = true").select(nk, "row_hash", "version")
    cur.createOrReplaceTempView("scd2_cur")

    # 3) Phân loại NEW (chưa có) + CHANGED (hash khác)
    changes = spark.sql(f"""
        SELECT s.*, c.version AS _old_version
        FROM scd2_src s LEFT JOIN scd2_cur c ON s.{nk} = c.{nk}
        WHERE c.{nk} IS NULL OR c.row_hash <> s.row_hash
    """)
    changes.persist()
    n_changes = changes.count()
    if n_changes == 0:
        print(f"  i SCD2 {table_name}: khong co thay doi")
        return 0
    changes.createOrReplaceTempView("scd2_changes")

    # 4) Statement A — đóng version cũ của những key CHANGED
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING (SELECT {nk}, row_hash FROM scd2_changes WHERE _old_version IS NOT NULL) AS s
        ON t.{nk} = s.{nk} AND t.is_current = true
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
            t.is_current = false,
            t.effective_to = TIMESTAMP '{now.strftime("%Y-%m-%d %H:%M:%S")}'
    """)

    # 5) Statement B — append version mới (NEW + CHANGED), version = old+1 (NEW→1)
    select_attrs = ", ".join(attrs + tracked)
    new_versions = spark.sql(f"""
        SELECT
            md5(concat({nk}, '|', '{now.strftime("%Y-%m-%d %H:%M:%S")}')) AS customer_sk,
            {nk}, {select_attrs}, row_hash,
            TIMESTAMP '{now.strftime("%Y-%m-%d %H:%M:%S")}' AS effective_from,
            CAST(NULL AS TIMESTAMP) AS effective_to,
            true AS is_current,
            COALESCE(_old_version, 0) + 1 AS version,
            TIMESTAMP '{now.strftime("%Y-%m-%d %H:%M:%S")}' AS _silver_loaded_at,
            '{batch_id}' AS _silver_batch_id
        FROM scd2_changes
    """)
    # align cột theo thứ tự bảng đích rồi append
    target_cols = [f.name for f in spark.table(target).schema.fields]
    new_versions.select(*target_cols).writeTo(target).append()

    changes.unpersist()
    print(f"  OK SCD2 {table_name}: {n_changes} version moi")
    return n_changes
```

> **Lưu ý implement:** kiểm tra thứ tự cột `select_attrs` khớp khai báo bảng; dùng
> `select(*target_cols)` trước khi `writeTo().append()` để tránh lệch cột. Surrogate key
> `customer_sk` = `md5(customer_id || '|' || effective_from)` đảm bảo duy nhất theo version.

---

## Bước 7 — Facts `src/silver/transform_fact.py`

Cleansing + MERGE insert (append idempotent — rerun không nhân đôi nhờ `ON txn_id`).

```python
# src/silver/transform_fact.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session, align_to_table
from common.cleansing import clean_strings, upper_cols, dedup_latest, add_silver_meta, drop_null_keys

_UPPER = ["status", "currency", "txn_type", "channel"]


def transform_fact(spark, table_name: str, cfg: dict, batch_id: str) -> int:
    source = f"nessie.bronze.{cfg['source']}"
    target = f"nessie.silver.{table_name}"
    nk     = cfg["natural_key"]

    df = spark.table(source)
    df = clean_strings(df)
    df = upper_cols(df, _UPPER)
    df = drop_null_keys(df, [nk])
    df = dedup_latest(df, [nk], "_ingested_at")     # phòng trùng txn_id trong Bronze
    df = add_silver_meta(df, batch_id)
    df = align_to_table(spark, df, target)

    cnt = df.count()
    df.createOrReplaceTempView("fact_src")
    spark.sql(f"""
        MERGE INTO {target} AS t USING fact_src AS s
        ON t.{nk} = s.{nk}
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  OK FACT {table_name}: +{cnt} (insert neu chua co)")
    return cnt
```

---

## Bước 8 — Orchestrator `src/silver/run_silver.py`

Loop `silver_tables.yaml`, dispatch theo `type`, ghi `nessie.silver.audit_log`.

```python
# src/silver/run_silver.py
import sys, uuid, yaml
from datetime import datetime, timezone
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session
from common.audit_logger import AuditLogger
from silver.transform_scd1 import transform_scd1
from silver.transform_scd2 import transform_scd2
from silver.transform_fact import transform_fact

CONFIG_PATH = "/opt/spark/config/silver_tables.yaml"
DISPATCH = {"scd1": transform_scd1, "scd2": transform_scd2, "fact": transform_fact}


def main():
    spark    = get_spark_session("silver-transform")
    logger   = AuditLogger(spark, "nessie.silver.audit_log")
    batch_id = str(uuid.uuid4())[:8]
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    for table_name, tcfg in cfg["tables"].items():
        ttype    = tcfg["type"]
        run_time = datetime.now(timezone.utc)
        try:
            rows = DISPATCH[ttype](spark, table_name, tcfg, batch_id)
            logger.log(batch_id=batch_id, table_name=table_name,
                       source_system="silver", run_mode=ttype, status="SUCCESS",
                       rows_ingested=rows, last_run_time=None, current_run_time=run_time)
        except Exception as e:
            print(f"  FAILED {table_name}: {e}")
            logger.log(batch_id=batch_id, table_name=table_name,
                       source_system="silver", run_mode=ttype, status="FAILED",
                       rows_ingested=0, last_run_time=None, current_run_time=run_time,
                       error_message=str(e))
    spark.stop()
    print(f"\nSilver transform hoan thanh. Batch ID: {batch_id}")


if __name__ == "__main__":
    main()
```

---

## Bước 9 — Verify `src/silver/verify_silver.py`

```python
# src/silver/verify_silver.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

SILVER_TABLES = ["dim_customer","dim_account","dim_card_account","dim_product",
                 "dim_branch","dim_loan","fct_bank_transactions","fct_card_transactions"]


def main():
    spark = get_spark_session("silver-verify")
    cust  = sys.argv[1] if len(sys.argv) > 1 else None

    print("\n==================== SILVER ROW COUNTS ====================")
    for t in SILVER_TABLES:
        try:
            print(f"  {t:<24}: {spark.table(f'nessie.silver.{t}').count()}")
        except Exception as e:
            print(f"  {t:<24}: ERROR {e}")

    if cust:
        print(f"\n========== SCD2 history dim_customer = {cust} ==========")
        (spark.table("nessie.silver.dim_customer")
            .filter(f"customer_id = '{cust}'")
            .select("customer_id","customer_segment","city","version",
                    "is_current","effective_from","effective_to")
            .orderBy("version").show(truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
```

---

## Bước 10 — Dựng Trino (Trino 446 + Iceberg + Nessie)

### `docker/trino/etc/node.properties`
```properties
node.environment=production
node.data-dir=/data/trino
```

### `docker/trino/etc/jvm.config`
```
-server
-Xmx2G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
```

### `docker/trino/etc/config.properties`
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
```

### `docker/trino/etc/catalog/iceberg.properties`
```properties
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://nessie:19120/api/v2
iceberg.nessie-catalog.ref=main
iceberg.nessie-catalog.default-warehouse-dir=s3a://lakehouse/warehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
```

> **Lưu ý:**
> - Trino 446 dùng filesystem mới: `fs.native-s3.enabled=true` + nhóm `s3.*` (KHÔNG
>   dùng `hive.s3.*` cũ).
> - `iceberg.nessie-catalog.uri` trỏ `/api/v2` — **giống** Spark (`spark_session.py` ghi
>   chú #1). Nếu fail thử connector `iceberg.catalog.type=rest` + endpoint `/iceberg`
>   (Iceberg-REST) như phương án B.
> - Host port **8088** (8080 đã bị Airflow webserver chiếm).

### Thêm vào `docker-compose.yml`
```yaml
  trino:
    image: trinodb/trino:446
    container_name: lakehouse-trino
    ports:
      - "8088:8080"
    env_file: .env                       # cung cấp AWS_ACCESS_KEY_ID / SECRET cho ${ENV:...}
    volumes:
      - ./docker/trino/etc:/etc/trino
    depends_on:
      - nessie
      - minio
    networks:
      - lakehouse          # dùng đúng network đang có trong compose
```

### Kết nối DBeaver
- Driver Trino, Host `localhost`, Port `8088`, user bất kỳ (vd `admin`), không password.
- Query: `SELECT * FROM iceberg.silver.dim_customer;`

---

## Bước 11 — Airflow DAG `dags/dag_silver_transform.py`

Mirror `dag_bronze_ingestion.py`: BashOperator + `docker exec lakehouse-spark-master
spark-submit`. Flow: `silver_transform → quality_check`.

```python
# dags/dag_silver_transform.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "lakehouse-spark-master"
SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
    "org.postgresql:postgresql:42.7.3"
)

def submit(job: str) -> str:
    return (f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 --packages '{SPARK_PACKAGES}' "
            f"/opt/spark/jobs/silver/{job}")

default_args = {"owner":"lakehouse","depends_on_past":False,
                "retries":2,"retry_delay":timedelta(minutes=5),"email_on_failure":False}

with DAG(
    dag_id="silver_transform",
    default_args=default_args,
    description="Bronze -> Silver: SCD1/SCD2 dims + cleansed facts",
    start_date=datetime(2024,11,1),
    schedule_interval="0 3 * * *",          # 3 AM, sau bronze (2 AM)
    catchup=False,
    tags=["silver","scd"],
) as dag:
    silver_transform = BashOperator(task_id="silver_transform",
                                    bash_command=submit("run_silver.py"))
    quality_check    = BashOperator(task_id="quality_check",
                                    bash_command=submit("verify_silver.py"))
    silver_transform >> quality_check
```

> **Optional (chain Bronze→Silver):** thêm `TriggerDagRunOperator(trigger_dag_id="silver_transform")`
> vào cuối `dag_bronze_ingestion.py` để Silver tự chạy sau Bronze. Mặc định để 2 DAG độc
> lập, lệch giờ (2 AM / 3 AM).

---

## Bước 12 — Test (chạy qua PowerShell, KHÔNG Git Bash — xem phase1_done.md)

Biến packages dùng chung (PowerShell):
```powershell
$PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.postgresql:postgresql:42.7.3"
```

```powershell
# 1) Tạo Silver tables (1 lần)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/create_silver_tables.py
# 2) Transform Bronze -> Silver
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
# 3) Verify
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/verify_silver.py CIF002
```

### Test idempotency
```powershell
# Chạy run_silver.py lần 2 → dim_customer KHÔNG sinh version mới, fct KHÔNG nhân đôi
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
```

### Test SCD2 (đổi segment 1 khách)
```powershell
docker exec lakehouse-postgres psql -U postgres -d banking -c "UPDATE customers SET customer_segment='PREMIER', updated_at=NOW() WHERE customer_id='CIF002';"
# Chạy lại bronze incremental rồi silver
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/incremental_load.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
# Verify: CIF002 có 2 version (is_current=false MASS + is_current=true PREMIER)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/verify_silver.py CIF002
```

### Test Trino (DBeaver `localhost:8088`)
```sql
SHOW TABLES FROM iceberg.silver;
SELECT customer_id, customer_segment, version, is_current, effective_from, effective_to
FROM iceberg.silver.dim_customer WHERE customer_id='CIF002' ORDER BY version;
SELECT COUNT(*) FROM iceberg.silver.fct_bank_transactions;
```

### Test Airflow
```powershell
docker exec lakehouse-airflow-scheduler airflow dags trigger silver_transform
# UI http://localhost:8080 (admin/admin) → cả 2 task SUCCESS
```

---

## Kết quả mong đợi khi hoàn thành
- [ ] 9 Silver tables trong `nessie.silver` (6 dim + 2 fact + audit_log)
- [ ] SCD1 dims upsert đúng giá trị mới nhất; SCD2 `dim_customer` chạy lần đầu version=1
- [ ] Idempotency: rerun → dim_customer 0 version mới, fact không nhân đôi
- [ ] SCD2 demo: đổi segment CIF002 → 2 version (cũ closed + mới current)
- [ ] Trino query `iceberg.silver.*` thành công qua DBeaver
- [ ] DAG `silver_transform` trigger thật, mọi task SUCCESS; `silver.audit_log` có entry
- [ ] MinIO `warehouse/silver/` có đủ table dir + parquet

---

## Lỗi có thể gặp (dự đoán — cập nhật thật vào phase2_done.md)

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| Trino `s3.aws-access-key` không nhận | `${ENV:...}` cần `env_file: .env` | Đảm bảo service trino có `env_file: .env` |
| Trino không thấy `silver` namespace | Nessie ref/branch sai | Đặt `iceberg.nessie-catalog.ref=main`; thử connector `rest` + `/iceberg` |
| SCD2 lệch cột khi `writeTo().append()` | thứ tự cột không khớp | `select(*target_cols)` theo schema bảng đích trước khi append |
| Port 8088 trùng | service khác chiếm | đổi host port khác (8089...) |
| `align_to_table` mất cột Bronze cần | cột không có trong bảng Silver đích | bổ sung cột vào DDL `create_silver_tables.py` |

---

## Task tiếp theo (Phase 3 — Gold)
Sau khi hoàn thành & cập nhật `memory/phase2_done.md` → tạo `tasks/task_4_gold_mart.md`:
star schema, 25+ KPI `mart_customer_360`, RFM segment, cross-sell flags, materialized views,
DAG Silver → Gold. Gold đọc `nessie.silver.dim_*` + `fct_*` (đã sẵn nhờ scope Dims+Facts).
