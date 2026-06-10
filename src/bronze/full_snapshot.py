# src/bronze/full_snapshot.py
"""
Full snapshot: đọc toàn bộ data từ Oracle + PostgreSQL → Bronze Iceberg.
Dùng overwritePartitions() → chạy lại bao nhiêu lần cũng không duplicate (idempotent).

Chạy:
    docker exec -it lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/bronze/full_snapshot.py
"""
import sys
import uuid
import yaml
from datetime import datetime, timezone

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.spark_session import get_spark_session, get_jdbc_options, align_to_table
from common.audit_logger import AuditLogger

CONFIG_PATH = "/opt/spark/config/bronze_tables.yaml"


def run_full_snapshot(spark, source_name: str, source_config: dict,
                      table_config: dict, batch_id: str) -> int:
    """Full snapshot 1 bảng. Trả về số dòng đã ghi."""
    table_name   = table_config["name"]
    bronze_table = f"nessie.{table_config['bronze_table']}"
    jdbc_opts    = get_jdbc_options(source_config)
    ingested_at  = datetime.now(timezone.utc)

    print(f"  -> Full snapshot: {source_name}.{table_name} -> {bronze_table}")

    df = (
        spark.read.format("jdbc")
        .option("url",           jdbc_opts["url"])
        .option("dbtable",       table_name)
        .option("user",          jdbc_opts["user"])
        .option("password",      jdbc_opts["password"])
        .option("driver",        jdbc_opts["driver"])
        .option("numPartitions", "4")
        .load()
    )

    # Thêm metadata columns
    df = (
        df
        .withColumn("_ingested_at",   F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_system", F.lit(source_name))
        .withColumn("_batch_id",      F.lit(batch_id))
    )

    # Căn chỉnh schema cho khớp bảng đích (xử lý Oracle UPPERCASE + Decimal→Double)
    df = align_to_table(spark, df, bronze_table)
    row_count = df.count()

    # OVERWRITE — idempotent
    df.writeTo(bronze_table).overwritePartitions()

    print(f"  OK {table_name}: {row_count} rows")
    return row_count


def main():
    spark    = get_spark_session("bronze-full-snapshot")
    logger   = AuditLogger(spark)
    batch_id = str(uuid.uuid4())[:8]

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    for source_name, source_config in config["sources"].items():
        for table_config in source_config["tables"]:
            run_time = datetime.now(timezone.utc)
            try:
                rows = run_full_snapshot(
                    spark, source_name, source_config, table_config, batch_id
                )
                logger.log(
                    batch_id=batch_id, table_name=table_config["bronze_table"],
                    source_system=source_name, run_mode="full_snapshot",
                    status="SUCCESS", rows_ingested=rows,
                    last_run_time=None, current_run_time=run_time,
                )
            except Exception as e:
                print(f"  FAILED {table_config['name']}: {e}")
                logger.log(
                    batch_id=batch_id, table_name=table_config["bronze_table"],
                    source_system=source_name, run_mode="full_snapshot",
                    status="FAILED", rows_ingested=0,
                    last_run_time=None, current_run_time=run_time,
                    error_message=str(e),
                )

    spark.stop()
    print(f"\nFull snapshot hoan thanh. Batch ID: {batch_id}")


if __name__ == "__main__":
    main()
