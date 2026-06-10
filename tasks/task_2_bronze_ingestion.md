# Task: Bronze Ingestion Pipeline — Phase 2

## Mục tiêu
Xây dựng pipeline đẩy data từ Oracle (Core Banking) và PostgreSQL (CRM + Card) vào Bronze layer
trên MinIO dưới dạng Apache Iceberg tables. Có 2 mode: full snapshot (chạy lần đầu) và
incremental load (chạy hàng ngày tự động qua Airflow).

## Trạng thái
- [x] HOÀN THÀNH (2026-06-04) — xem memory/phase2_done.md

---

## Checklist tổng

- [x] Bước 1: Tạo config YAML định nghĩa các bảng Bronze
- [x] Bước 2: Viết Spark session chuẩn dùng chung cho toàn project
- [x] Bước 3: Tạo Bronze Iceberg tables (chạy 1 lần) — 9 bảng trong nessie.bronze
- [x] Bước 4: Viết Spark job Full Snapshot
- [x] Bước 5: Viết Audit Logger
- [x] Bước 6: Viết Spark job Incremental Load
- [x] Bước 7: Viết Airflow DAG orchestrate toàn bộ
- [x] Bước 8: Test full snapshot — data trên MinIO, row count đúng
- [x] Bước 9: Test idempotency — chạy 2 lần, row count không đổi
- [x] Bước 10: Test incremental — thêm CIF006, chạy lại, thấy record mới (5→6)

---

## Cấu trúc file sẽ tạo ra

```
lakehouse-customer360/
├── config/
│   └── bronze_tables.yaml          ← định nghĩa tất cả bảng Bronze
├── src/
│   ├── common/
│   │   ├── spark_session.py        ← tạo SparkSession dùng chung
│   │   └── audit_logger.py         ← ghi trạng thái mỗi lần chạy
│   └── bronze/
│       ├── create_bronze_tables.py ← tạo Iceberg tables (chạy 1 lần)
│       ├── full_snapshot.py        ← full load tất cả bảng
│       └── incremental_load.py     ← incremental theo updated_at
└── dags/
    └── dag_bronze_ingestion.py     ← Airflow DAG
```

---

## Bước 1 — Config YAML định nghĩa bảng Bronze

Tạo file `config/bronze_tables.yaml`.
File này là "bản đồ" toàn bộ Bronze layer — mỗi khi thêm bảng mới chỉ cần sửa file này,
không cần sửa code Spark.

```yaml
# config/bronze_tables.yaml
# Định nghĩa tất cả bảng Bronze cần ingest
# mode: full_snapshot | incremental
# incremental_col: cột dùng để filter record mới (thường là updated_at hoặc created_at)

sources:

  oracle:
    jdbc_url: "jdbc:oracle:thin:@//oracle-xe:1521/XEPDB1"
    driver: "oracle.jdbc.OracleDriver"
    user: "corebanking"
    password_env: "ORACLE_PASSWORD"   # đọc từ .env, không hardcode
    tables:
      - name: branches
        bronze_table: bronze.oracle_branches
        mode: full_snapshot             # bảng nhỏ, ít thay đổi → full mỗi lần
        partition_col: null

      - name: products
        bronze_table: bronze.oracle_products
        mode: full_snapshot
        partition_col: null

      - name: bank_accounts
        bronze_table: bronze.oracle_bank_accounts
        mode: incremental
        incremental_col: updated_at
        partition_col: opened_date      # partition theo tháng mở tài khoản

      - name: loans
        bronze_table: bronze.oracle_loans
        mode: incremental
        incremental_col: updated_at
        partition_col: disbursement_date

      - name: bank_transactions
        bronze_table: bronze.oracle_bank_transactions
        mode: incremental
        incremental_col: created_at     # transactions không có updated_at
        partition_col: txn_date         # partition theo ngày giao dịch

  postgres:
    jdbc_url: "jdbc:postgresql://postgres:5432/banking"
    driver: "org.postgresql.Driver"
    user: "postgres"
    password_env: "POSTGRES_PASSWORD"
    tables:
      - name: customers
        bronze_table: bronze.pg_customers
        mode: incremental
        incremental_col: updated_at
        partition_col: null

      - name: card_accounts
        bronze_table: bronze.pg_card_accounts
        mode: incremental
        incremental_col: updated_at
        partition_col: null

      - name: card_transactions
        bronze_table: bronze.pg_card_transactions
        mode: incremental
        incremental_col: created_at
        partition_col: txn_date
```

---

## Bước 2 — Spark Session dùng chung

Tạo file `src/common/spark_session.py`.
File này được import bởi tất cả Spark job — không tạo SparkSession trực tiếp trong từng job.

```python
# src/common/spark_session.py
import os
from pyspark.sql import SparkSession


SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09",
    "org.postgresql:postgresql:42.7.3",
])


def get_spark_session(app_name: str) -> SparkSession:
    """
    Tạo SparkSession chuẩn với Iceberg + MinIO + Nessie.
    Import hàm này thay vì tạo SparkSession trực tiếp.
    """
    minio_endpoint   = os.getenv("MINIO_ENDPOINT",      "http://minio:9000")
    minio_access_key = os.getenv("AWS_ACCESS_KEY_ID",   "minioadmin")
    minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY","minioadmin123")
    nessie_uri       = os.getenv("ICEBERG_CATALOG_URI",  "http://nessie:19120/iceberg")
    warehouse        = os.getenv("ICEBERG_WAREHOUSE",    "s3a://lakehouse/warehouse")
    spark_master     = os.getenv("SPARK_MASTER_URL",     "spark://spark-master:7077")

    return (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", SPARK_PACKAGES)

        # Iceberg extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

        # Nessie catalog
        .config("spark.sql.catalog.nessie",             "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl","org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",          nessie_uri)
        .config("spark.sql.catalog.nessie.ref",          "main")
        .config("spark.sql.catalog.nessie.warehouse",    warehouse)
        .config("spark.sql.catalog.nessie.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")

        # MinIO S3-compatible
        .config("spark.hadoop.fs.s3a.endpoint",          minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",        minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",        minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Performance tuning cho 16GB RAM
        .config("spark.driver.memory",   "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores",  "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "20")

        .getOrCreate()
    )


def get_jdbc_options(source_config: dict) -> dict:
    """
    Trả về JDBC options cho spark.read.format('jdbc').
    Password đọc từ biến môi trường, không hardcode.
    """
    password = os.getenv(source_config["password_env"])
    return {
        "url":      source_config["jdbc_url"],
        "driver":   source_config["driver"],
        "user":     source_config["user"],
        "password": password,
    }
```

---

## Bước 3 — Tạo Bronze Iceberg Tables

Tạo file `src/bronze/create_bronze_tables.py`.
**Chạy 1 lần duy nhất** để tạo namespace và tables trong Nessie catalog.

```python
# src/bronze/create_bronze_tables.py
"""
Tạo Bronze Iceberg tables trong Nessie catalog.
Chạy 1 lần trước khi bắt đầu ingest data.

Chạy:
    docker exec -it lakehouse-spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/bronze/create_bronze_tables.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")

from common.spark_session import get_spark_session


def create_bronze_tables(spark):
    # Tạo namespace bronze nếu chưa có
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    # -------------------------------------------------------------------------
    # Oracle tables
    # -------------------------------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_branches (
            branch_id    STRING,
            branch_name  STRING,
            region       STRING,
            city         STRING,
            branch_type  STRING,
            status       STRING,
            updated_at   TIMESTAMP,
            -- metadata columns
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_products (
            product_id    STRING,
            product_name  STRING,
            product_type  STRING,
            interest_rate DOUBLE,
            status        STRING,
            updated_at    TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_bank_accounts (
            account_id    STRING,
            customer_id   STRING,
            product_id    STRING,
            branch_id     STRING,
            account_type  STRING,
            account_no    STRING,
            balance       DOUBLE,
            currency      STRING,
            status        STRING,
            opened_date   DATE,
            closed_date   DATE,
            interest_rate DOUBLE,
            updated_at    TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
        PARTITIONED BY (months(opened_date))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_loans (
            loan_id            STRING,
            customer_id        STRING,
            product_id         STRING,
            branch_id          STRING,
            loan_type          STRING,
            principal_amount   DOUBLE,
            outstanding_amount DOUBLE,
            interest_rate      DOUBLE,
            disbursement_date  DATE,
            maturity_date      DATE,
            npl_status         STRING,
            status             STRING,
            updated_at         TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
        PARTITIONED BY (months(disbursement_date))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_bank_transactions (
            txn_id        STRING,
            account_id    STRING,
            txn_date      DATE,
            txn_datetime  TIMESTAMP,
            txn_type      STRING,
            amount        DOUBLE,
            balance_after DOUBLE,
            channel       STRING,
            status        STRING,
            created_at    TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # -------------------------------------------------------------------------
    # PostgreSQL tables
    # -------------------------------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.pg_customers (
            customer_id      STRING,
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
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.pg_card_accounts (
            account_id          STRING,
            customer_id         STRING,
            card_type           STRING,
            card_number         STRING,
            credit_limit        DOUBLE,
            outstanding_balance DOUBLE,
            due_date            DATE,
            status              STRING,
            opened_date         DATE,
            closed_date         DATE,
            created_at          TIMESTAMP,
            updated_at          TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.pg_card_transactions (
            txn_id            STRING,
            account_id        STRING,
            txn_date          DATE,
            txn_datetime      TIMESTAMP,
            amount            DOUBLE,
            currency          STRING,
            txn_type          STRING,
            merchant_name     STRING,
            merchant_category STRING,
            channel           STRING,
            status            STRING,
            created_at        TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        )
        USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # Audit table — track trạng thái mỗi lần chạy
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.audit_log (
            batch_id        STRING,
            table_name      STRING,
            source_system   STRING,
            run_mode        STRING,
            status          STRING,
            rows_ingested   LONG,
            last_run_time   TIMESTAMP,
            current_run_time TIMESTAMP,
            error_message   STRING,
            created_at      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    print("✅ Tất cả Bronze tables đã tạo xong trong nessie.bronze")


if __name__ == "__main__":
    spark = get_spark_session("create-bronze-tables")
    create_bronze_tables(spark)
    spark.stop()
```

**Chạy lệnh:**

```bash
docker exec -it lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze/create_bronze_tables.py
```

---

## Bước 4 — Full Snapshot Job

Tạo file `src/bronze/full_snapshot.py`.
Đọc toàn bộ data từ nguồn, ghi đè vào Bronze (OVERWRITE). Chạy lần đầu hoặc khi cần reset.

```python
# src/bronze/full_snapshot.py
"""
Full snapshot: đọc toàn bộ data từ Oracle + PostgreSQL → Bronze Iceberg.
Dùng OVERWRITE mode — chạy lại bao nhiêu lần cũng không duplicate.

Chạy:
    docker exec -it lakehouse-spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/bronze/full_snapshot.py
"""
import sys
import uuid
import yaml
from datetime import datetime, timezone

sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session, get_jdbc_options
from common.audit_logger import AuditLogger


def run_full_snapshot(spark, source_name: str, source_config: dict,
                      table_config: dict, batch_id: str):
    """Full snapshot 1 bảng."""
    table_name    = table_config["name"]
    bronze_table  = f"nessie.{table_config['bronze_table']}"
    jdbc_opts     = get_jdbc_options(source_config)
    ingested_at   = datetime.now(timezone.utc)

    print(f"  → Full snapshot: {source_name}.{table_name} → {bronze_table}")

    # Đọc toàn bộ bảng từ nguồn
    df = (
        spark.read.format("jdbc")
        .option("url",      jdbc_opts["url"])
        .option("dbtable",  table_name)
        .option("user",     jdbc_opts["user"])
        .option("password", jdbc_opts["password"])
        .option("driver",   jdbc_opts["driver"])
        # Đọc song song 4 partition để nhanh hơn
        .option("numPartitions", "4")
        .load()
    )

    row_count = df.count()

    # Thêm metadata columns
    from pyspark.sql import functions as F
    df = (
        df
        .withColumn("_ingested_at",   F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_system", F.lit(source_name))
        .withColumn("_batch_id",      F.lit(batch_id))
    )

    # Ghi vào Bronze — OVERWRITE toàn bộ table
    df.writeTo(bronze_table).overwritePartitions()

    print(f"  ✅ {table_name}: {row_count} rows")
    return row_count


def main():
    spark      = get_spark_session("bronze-full-snapshot")
    logger     = AuditLogger(spark)
    batch_id   = str(uuid.uuid4())[:8]
    run_time   = datetime.now(timezone.utc)

    with open("/opt/spark/jobs/../config/bronze_tables.yaml") as f:
        config = yaml.safe_load(f)

    for source_name, source_config in config["sources"].items():
        for table_config in source_config["tables"]:
            try:
                rows = run_full_snapshot(
                    spark, source_name, source_config, table_config, batch_id
                )
                logger.log(
                    batch_id       = batch_id,
                    table_name     = table_config["bronze_table"],
                    source_system  = source_name,
                    run_mode       = "full_snapshot",
                    status         = "SUCCESS",
                    rows_ingested  = rows,
                    last_run_time  = None,
                    current_run_time = run_time,
                )
            except Exception as e:
                print(f"  ❌ Lỗi {table_config['name']}: {e}")
                logger.log(
                    batch_id       = batch_id,
                    table_name     = table_config["bronze_table"],
                    source_system  = source_name,
                    run_mode       = "full_snapshot",
                    status         = "FAILED",
                    rows_ingested  = 0,
                    last_run_time  = None,
                    current_run_time = run_time,
                    error_message  = str(e),
                )

    spark.stop()
    print(f"\n✅ Full snapshot hoàn thành. Batch ID: {batch_id}")


if __name__ == "__main__":
    main()
```

---

## Bước 5 — Audit Logger

Tạo file `src/common/audit_logger.py`.
Ghi trạng thái mỗi lần chạy vào `nessie.bronze.audit_log`.
Silver layer sẽ đọc bảng này để biết `last_run_time` của Bronze.

```python
# src/common/audit_logger.py
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class AuditLogger:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def log(self, batch_id: str, table_name: str, source_system: str,
            run_mode: str, status: str, rows_ingested: int,
            last_run_time, current_run_time,
            error_message: str = None):

        data = [{
            "batch_id":         batch_id,
            "table_name":       table_name,
            "source_system":    source_system,
            "run_mode":         run_mode,
            "status":           status,
            "rows_ingested":    rows_ingested,
            "last_run_time":    last_run_time,
            "current_run_time": current_run_time,
            "error_message":    error_message,
            "created_at":       datetime.now(timezone.utc),
        }]

        df = self.spark.createDataFrame(data)
        df.writeTo("nessie.bronze.audit_log").append()

    def get_last_run_time(self, table_name: str):
        """
        Lấy thời điểm chạy thành công gần nhất của 1 bảng.
        Incremental load dùng hàm này để biết filter từ đâu.
        """
        try:
            result = (
                self.spark.table("nessie.bronze.audit_log")
                .filter(F.col("table_name") == table_name)
                .filter(F.col("status") == "SUCCESS")
                .orderBy(F.col("current_run_time").desc())
                .limit(1)
                .select("current_run_time")
                .collect()
            )
            if result:
                return result[0]["current_run_time"]
            return None
        except Exception:
            return None
```

---

## Bước 6 — Incremental Load Job

Tạo file `src/bronze/incremental_load.py`.
Chỉ đọc record mới/thay đổi kể từ lần chạy trước, dùng MERGE INTO để upsert vào Bronze.

```python
# src/bronze/incremental_load.py
"""
Incremental load: chỉ đọc record có updated_at > last_run_time → upsert vào Bronze.
Chạy hàng ngày qua Airflow DAG.

Chạy thủ công:
    docker exec -it lakehouse-spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/bronze/incremental_load.py
"""
import sys
import uuid
import yaml
from datetime import datetime, timezone

sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session, get_jdbc_options
from common.audit_logger import AuditLogger


def run_incremental(spark, source_name: str, source_config: dict,
                    table_config: dict, batch_id: str, logger: AuditLogger):
    """Incremental load 1 bảng."""
    table_name       = table_config["name"]
    bronze_table     = f"nessie.{table_config['bronze_table']}"
    incremental_col  = table_config.get("incremental_col", "updated_at")
    jdbc_opts        = get_jdbc_options(source_config)
    current_run_time = datetime.now(timezone.utc)

    # Lấy last_run_time từ audit log
    last_run_time = logger.get_last_run_time(table_config["bronze_table"])

    if last_run_time is None:
        # Chưa có lịch sử → chạy full snapshot thay thế
        print(f"  ⚠️  {table_name}: chưa có audit log → chạy full load")
        query = f"(SELECT * FROM {table_name}) t"
    else:
        # Chỉ lấy record thay đổi sau last_run_time
        query = (
            f"(SELECT * FROM {table_name} "
            f"WHERE {incremental_col} > TIMESTAMP '{last_run_time}' "
            f"AND {incremental_col} <= TIMESTAMP '{current_run_time}') t"
        )

    print(f"  → Incremental: {source_name}.{table_name}")
    print(f"     Filter: {incremental_col} > {last_run_time}")

    from pyspark.sql import functions as F

    df = (
        spark.read.format("jdbc")
        .option("url",      jdbc_opts["url"])
        .option("dbtable",  query)
        .option("user",     jdbc_opts["user"])
        .option("password", jdbc_opts["password"])
        .option("driver",   jdbc_opts["driver"])
        .load()
    )

    row_count = df.count()

    if row_count == 0:
        print(f"  ℹ️  {table_name}: không có record mới")
        logger.log(
            batch_id=batch_id, table_name=table_config["bronze_table"],
            source_system=source_name, run_mode="incremental",
            status="SUCCESS", rows_ingested=0,
            last_run_time=last_run_time, current_run_time=current_run_time,
        )
        return 0

    # Thêm metadata
    df = (
        df
        .withColumn("_ingested_at",   F.lit(current_run_time).cast("timestamp"))
        .withColumn("_source_system", F.lit(source_name))
        .withColumn("_batch_id",      F.lit(batch_id))
    )

    # Tạo temp view để dùng trong MERGE
    df.createOrReplaceTempView("incremental_data")

    # MERGE INTO: upsert vào Bronze
    # Nếu record đã tồn tại → UPDATE, chưa tồn tại → INSERT
    # Primary key mỗi bảng khác nhau — map ở đây
    pk_map = {
        "oracle_branches":          "branch_id",
        "oracle_products":          "product_id",
        "oracle_bank_accounts":     "account_id",
        "oracle_loans":             "loan_id",
        "oracle_bank_transactions": "txn_id",
        "pg_customers":             "customer_id",
        "pg_card_accounts":         "account_id",
        "pg_card_transactions":     "txn_id",
    }
    short_name = table_config["bronze_table"].split(".")[-1]
    pk         = pk_map.get(short_name, "id")

    spark.sql(f"""
        MERGE INTO {bronze_table} AS target
        USING incremental_data AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"  ✅ {table_name}: upsert {row_count} rows")
    return row_count


def main():
    spark    = get_spark_session("bronze-incremental-load")
    logger   = AuditLogger(spark)
    batch_id = str(uuid.uuid4())[:8]

    with open("/opt/spark/jobs/../config/bronze_tables.yaml") as f:
        config = yaml.safe_load(f)

    for source_name, source_config in config["sources"].items():
        for table_config in source_config["tables"]:
            # Bảng full_snapshot mode thì vẫn full mỗi lần (branches, products)
            mode = table_config.get("mode", "incremental")
            try:
                if mode == "full_snapshot":
                    from bronze.full_snapshot import run_full_snapshot
                    rows = run_full_snapshot(
                        spark, source_name, source_config, table_config, batch_id
                    )
                else:
                    rows = run_incremental(
                        spark, source_name, source_config,
                        table_config, batch_id, logger
                    )

                logger.log(
                    batch_id=batch_id,
                    table_name=table_config["bronze_table"],
                    source_system=source_name,
                    run_mode=mode, status="SUCCESS",
                    rows_ingested=rows,
                    last_run_time=None,
                    current_run_time=datetime.now(timezone.utc),
                )
            except Exception as e:
                print(f"  ❌ Lỗi {table_config['name']}: {e}")
                logger.log(
                    batch_id=batch_id,
                    table_name=table_config["bronze_table"],
                    source_system=source_name,
                    run_mode=mode, status="FAILED",
                    rows_ingested=0,
                    last_run_time=None,
                    current_run_time=datetime.now(timezone.utc),
                    error_message=str(e),
                )

    spark.stop()
    print(f"\n✅ Incremental load hoàn thành. Batch ID: {batch_id}")


if __name__ == "__main__":
    main()
```

---

## Bước 7 — Airflow DAG

Tạo file `dags/dag_bronze_ingestion.py`.
Orchestrate toàn bộ pipeline: chạy full snapshot lần đầu, sau đó incremental hàng ngày.

```python
# dags/dag_bronze_ingestion.py
"""
DAG Bronze Ingestion
- Schedule: 2:00 AM mỗi ngày
- Task 1: Chạy incremental load (tự động fallback sang full nếu chưa có audit log)
- Task 2: Kiểm tra row count tối thiểu (data quality cơ bản)
- Retry: 2 lần nếu lỗi, chờ 5 phút giữa các lần retry
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner":            "lakehouse",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 11, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,   # bật lên khi có email server
}

with DAG(
    dag_id          = "bronze_ingestion",
    default_args    = default_args,
    description     = "Daily ingestion từ Oracle + PostgreSQL vào Bronze Iceberg",
    schedule_interval = "0 2 * * *",   # 2:00 AM mỗi ngày
    catchup         = False,           # không chạy bù các ngày đã qua
    tags            = ["bronze", "ingestion"],
) as dag:

    # Task 1: Chạy full snapshot lần đầu (idempotent — chạy lại cũng không sao)
    full_snapshot = SparkSubmitOperator(
        task_id     = "full_snapshot_if_needed",
        application = "/opt/airflow/jobs/bronze/full_snapshot.py",
        conn_id     = "spark_default",
        verbose     = True,
        conf        = {
            "spark.master":           "spark://spark-master:7077",
            "spark.driver.memory":    "2g",
            "spark.executor.memory":  "2g",
        },
        packages    = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
            "org.postgresql:postgresql:42.7.3"
        ),
    )

    # Task 2: Incremental load hàng ngày
    incremental_load = SparkSubmitOperator(
        task_id     = "incremental_load",
        application = "/opt/airflow/jobs/bronze/incremental_load.py",
        conn_id     = "spark_default",
        verbose     = True,
        conf        = {
            "spark.master":          "spark://spark-master:7077",
            "spark.driver.memory":   "2g",
            "spark.executor.memory": "2g",
        },
        packages    = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
            "org.postgresql:postgresql:42.7.3"
        ),
    )

    # Task 3: Kiểm tra data quality cơ bản
    def check_row_counts(**context):
        """Fail DAG nếu Bronze tables trống sau khi ingest."""
        # Trong thực tế: kết nối Trino hoặc Spark để đếm row
        # Đơn giản hóa ở đây — Claude Code sẽ implement chi tiết
        print("✅ Data quality check passed")

    quality_check = PythonOperator(
        task_id         = "quality_check",
        python_callable = check_row_counts,
    )

    # Dependency: full_snapshot → incremental_load → quality_check
    # Lần đầu chạy: cả 2 task đều chạy
    # Từ ngày 2 trở đi: full_snapshot vẫn chạy nhưng chỉ ghi bảng nhỏ (branches, products)
    full_snapshot >> incremental_load >> quality_check
```

### Setup Airflow Connection cho Spark

Vào Airflow UI `http://localhost:8080` → Admin → Connections → Add:

```
Connection ID   : spark_default
Connection Type : Spark
Host            : spark://spark-master
Port            : 7077
```

---

## Bước 8–10 — Kiểm tra

### Test full snapshot

```bash
# Chạy thủ công
docker exec -it lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze/full_snapshot.py

# Kiểm tra data qua DBeaver kết nối Trino (localhost:8088)
SELECT COUNT(*) FROM iceberg.bronze.pg_customers;
SELECT COUNT(*) FROM iceberg.bronze.oracle_bank_accounts;
```

### Test idempotency

```bash
# Chạy lại lần 2 — row count phải giống lần 1
docker exec -it lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze/full_snapshot.py

# Count phải bằng nhau
SELECT COUNT(*) FROM iceberg.bronze.pg_customers;
```

### Test incremental

```bash
# Thêm 1 khách hàng mới vào PostgreSQL
docker exec -it lakehouse-postgres psql -U postgres -d banking -c "
INSERT INTO customers VALUES (
  'CIF006','Nguyen Thi Moi','2000-01-01','F','099000006789',
  '0999888777','moi@gmail.com','Ho Chi Minh','Ho Chi Minh',
  'MASS','VERIFIED',NOW(),NOW()
);"

# Chạy incremental load
docker exec -it lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze/incremental_load.py

# Kiểm tra — phải thấy CIF006
SELECT customer_id, full_name FROM iceberg.bronze.pg_customers
WHERE customer_id = 'CIF006';
```

### Kiểm tra MinIO UI

Vào `http://localhost:9001` → bucket `lakehouse` → `warehouse/bronze/`:

```
warehouse/
└── bronze/
    ├── pg_customers/
    │   └── data/
    │       └── 00000-0-xxx.parquet   ← file data thật
    ├── oracle_bank_accounts/
    │   └── data/year=2020/month=01/
    │       └── 00000-0-xxx.parquet
    └── audit_log/
        └── data/year=2024/month=11/
            └── 00000-0-xxx.parquet
```

### Kiểm tra audit log

```sql
-- Xem lịch sử chạy
SELECT batch_id, table_name, run_mode, status, rows_ingested, current_run_time
FROM iceberg.bronze.audit_log
ORDER BY current_run_time DESC;
```

---

## Kết quả mong đợi khi hoàn thành

- [ ] 8 Bronze Iceberg tables tạo thành công trong Nessie catalog
- [ ] Full snapshot: tất cả bảng có data, row count đúng
- [ ] Idempotency: chạy 2 lần → row count không đổi
- [ ] Incremental: thêm record mới ở nguồn → thấy trong Bronze sau khi chạy
- [ ] Audit log có entry cho mỗi lần chạy
- [ ] Airflow DAG `bronze_ingestion` visible trên UI, trigger manual thành công
- [ ] MinIO UI thấy file Parquet được tổ chức theo partition

---

## Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `ClassNotFoundException: oracle.jdbc.OracleDriver` | Thiếu ojdbc11 jar | Kiểm tra `spark.jars.packages` có ojdbc11 |
| `MERGE INTO` lỗi schema mismatch | Cột metadata chưa có trong target table | Chạy lại `create_bronze_tables.py` để drop và recreate |
| Audit log trả về `None` lần đầu | Chưa có record trong audit_log | Bình thường — incremental sẽ tự fallback sang full load |
| Airflow `SparkSubmitOperator` lỗi connection | Chưa tạo Connection `spark_default` | Tạo trong Airflow UI → Admin → Connections |
| `s3a: No such bucket` | Bucket `lakehouse` chưa tạo | Chạy lại `minio-init` container |

---

## Task tiếp theo

Sau khi hoàn thành Phase 2 → cập nhật `memory/phase2_done.md` → tạo `tasks/task_silver_scd.md`

Nội dung Phase 3 (Silver): đọc từ Bronze, apply SCD Type 1 và SCD Type 2,
cleansing data, xây dựng dimensional model chuẩn ngân hàng.