# src/bronze/incremental_load.py
"""
Incremental load: chỉ đọc record có <incremental_col> > last_run_time → MERGE upsert vào Bronze.
Chạy hàng ngày qua Airflow DAG. Bảng mode=full_snapshot (branches, products) vẫn full mỗi lần.

Chạy thủ công:
    docker exec -it lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/bronze/incremental_load.py
"""
import sys
import uuid
import yaml
from datetime import datetime, timezone

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.spark_session import get_spark_session, get_jdbc_options, align_to_table
from common.audit_logger import AuditLogger
from bronze.full_snapshot import run_full_snapshot

CONFIG_PATH = "/opt/spark/config/bronze_tables.yaml"

# Primary key của mỗi bảng — dùng cho điều kiện MERGE ON.
PK_MAP = {
    "oracle_branches":          "branch_id",
    "oracle_products":          "product_id",
    "oracle_bank_accounts":     "account_id",
    "oracle_loans":             "loan_id",
    "oracle_bank_transactions": "txn_id",
    "pg_customers":             "customer_id",
    "pg_card_accounts":         "account_id",
    "pg_card_transactions":     "txn_id",
}


def _fmt(ts) -> str:
    """Format datetime → 'YYYY-MM-DD HH:MM:SS' (literal hợp lệ cho cả Oracle lẫn Postgres)."""
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def run_incremental(spark, source_name: str, source_config: dict,
                    table_config: dict, batch_id: str, logger: AuditLogger) -> int:
    """Incremental load 1 bảng. Trả về số dòng upsert."""
    table_name       = table_config["name"]
    bronze_table     = f"nessie.{table_config['bronze_table']}"
    short_name       = table_config["bronze_table"].split(".")[-1]
    incremental_col  = table_config.get("incremental_col", "updated_at")
    jdbc_opts        = get_jdbc_options(source_config)
    current_run_time = datetime.now(timezone.utc)

    last_run_time = logger.get_last_run_time(table_config["bronze_table"])

    if last_run_time is None:
        print(f"  ! {table_name}: chua co audit log -> full load")
        query = f"(SELECT * FROM {table_name}) t"
    else:
        query = (
            f"(SELECT * FROM {table_name} "
            f"WHERE {incremental_col} > TIMESTAMP '{_fmt(last_run_time)}' "
            f"AND {incremental_col} <= TIMESTAMP '{_fmt(current_run_time)}') t"
        )

    print(f"  -> Incremental: {source_name}.{table_name}")
    print(f"     Filter: {incremental_col} > {last_run_time}")

    df = (
        spark.read.format("jdbc")
        .option("url",      jdbc_opts["url"])
        .option("dbtable",  query)
        .option("user",     jdbc_opts["user"])
        .option("password", jdbc_opts["password"])
        .option("driver",   jdbc_opts["driver"])
        .load()
    )

    df = (
        df
        .withColumn("_ingested_at",   F.lit(current_run_time).cast("timestamp"))
        .withColumn("_source_system", F.lit(source_name))
        .withColumn("_batch_id",      F.lit(batch_id))
    )
    df = align_to_table(spark, df, bronze_table)
    row_count = df.count()

    if row_count == 0:
        print(f"  i {table_name}: khong co record moi")
        return 0

    df.createOrReplaceTempView("incremental_data")
    pk = PK_MAP.get(short_name, "id")

    spark.sql(f"""
        MERGE INTO {bronze_table} AS target
        USING incremental_data AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"  OK {table_name}: upsert {row_count} rows")
    return row_count


def main():
    spark    = get_spark_session("bronze-incremental-load")
    logger   = AuditLogger(spark)
    batch_id = str(uuid.uuid4())[:8]

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    for source_name, source_config in config["sources"].items():
        for table_config in source_config["tables"]:
            mode = table_config.get("mode", "incremental")
            run_time = datetime.now(timezone.utc)
            try:
                if mode == "full_snapshot":
                    rows = run_full_snapshot(
                        spark, source_name, source_config, table_config, batch_id
                    )
                else:
                    rows = run_incremental(
                        spark, source_name, source_config, table_config, batch_id, logger
                    )

                logger.log(
                    batch_id=batch_id, table_name=table_config["bronze_table"],
                    source_system=source_name, run_mode=mode,
                    status="SUCCESS", rows_ingested=rows,
                    last_run_time=None, current_run_time=run_time,
                )
            except Exception as e:
                print(f"  FAILED {table_config['name']}: {e}")
                logger.log(
                    batch_id=batch_id, table_name=table_config["bronze_table"],
                    source_system=source_name, run_mode=mode,
                    status="FAILED", rows_ingested=0,
                    last_run_time=None, current_run_time=run_time,
                    error_message=str(e),
                )

    spark.stop()
    print(f"\nIncremental load hoan thanh. Batch ID: {batch_id}")


if __name__ == "__main__":
    main()
