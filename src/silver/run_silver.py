# src/silver/run_silver.py
"""
Orchestrator Silver: loop config/silver_tables.yaml, dispatch theo type
(scd1/scd2/fact), ghi kết quả vào nessie.silver.audit_log.

Chạy:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/silver/run_silver.py
"""
import sys
import uuid
import yaml
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
