# src/gold/run_gold.py
"""
Orchestrator Gold: resolve as_of (auto = max txn_date), chay tuan tu agg -> agg -> mart,
ghi ket qua vao nessie.gold.audit_log.

Khac Silver (loop doc lap): Gold co thu tu phu thuoc (mart can 2 agg) -> chay tuan tu,
fail agg thi raise (dung pipeline).

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/gold/run_gold.py
"""
import sys
import uuid
import yaml
from datetime import datetime, timezone

sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session
from common.audit_logger import AuditLogger
from gold.build_agg_holdings import build_agg_holdings
from gold.build_agg_txn_12m import build_agg_txn_12m
from gold.build_mart_360 import build_mart_360

CONFIG_PATH = "/opt/spark/config/gold_tables.yaml"

PIPELINE = [
    ("agg_customer_holdings", build_agg_holdings),
    ("agg_customer_txn_12m",  build_agg_txn_12m),
    ("mart_customer_360",     build_mart_360),
]


def resolve_as_of(spark, cfg) -> str:
    cfg_val = cfg.get("as_of_date", "auto")
    if str(cfg_val) != "auto":
        return str(cfg_val)
    row = spark.sql("""
        SELECT MAX(d) AS m FROM (
            SELECT MAX(txn_date) AS d FROM nessie.silver.fct_bank_transactions
            UNION ALL
            SELECT MAX(txn_date) AS d FROM nessie.silver.fct_card_transactions
        )
    """).collect()[0]
    if row["m"] is None:
        raise RuntimeError("Khong tim thay txn_date trong fact silver -> chua co data Silver?")
    return str(row["m"])


def main():
    spark = get_spark_session("gold-build")
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    as_of = resolve_as_of(spark, cfg)
    logger = AuditLogger(spark, "nessie.gold.audit_log")
    batch_id = str(uuid.uuid4())[:8]
    print(f"Gold build as_of={as_of} batch={batch_id}")

    for name, fn in PIPELINE:
        run_time = datetime.now(timezone.utc)
        try:
            rows = fn(spark, cfg, as_of, batch_id)
            logger.log(batch_id=batch_id, table_name=name, source_system="gold",
                       run_mode="overwrite", status="SUCCESS", rows_ingested=rows,
                       last_run_time=None, current_run_time=run_time)
        except Exception as e:
            print(f"  FAILED {name}: {e}")
            logger.log(batch_id=batch_id, table_name=name, source_system="gold",
                       run_mode="overwrite", status="FAILED", rows_ingested=0,
                       last_run_time=None, current_run_time=run_time, error_message=str(e))
            raise   # mart phu thuoc agg -> dung pipeline neu agg fail

    spark.stop()
    print(f"\nGold build hoan thanh. Batch ID: {batch_id}")


if __name__ == "__main__":
    main()
