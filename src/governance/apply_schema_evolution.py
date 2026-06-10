# src/governance/apply_schema_evolution.py
"""
DEMO Schema Evolution (Phase 4): them cot masked vao mart_customer_360 bang
ALTER TABLE ADD COLUMN — Iceberg KHONG rewrite data file cu (chi doi metadata).

Chay 1 lan TRUOC khi rebuild mart (run_gold.py) de align_to_table nhan duoc 2 cot moi.
Sau ALTER, snapshot cu van query duoc: data cu o 2 cot moi = NULL (bang chung evolve
schema khong dung file cu).

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/governance/apply_schema_evolution.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

TARGET = "nessie.gold.mart_customer_360"
NEW_COLUMNS = [
    ("phone_masked", "STRING"),
    ("email_masked", "STRING"),
]


def main():
    spark = get_spark_session("schema-evolution")

    existing = {f.name.lower() for f in spark.table(TARGET).schema.fields}
    for name, dtype in NEW_COLUMNS:
        if name.lower() in existing:
            print(f"  - cot {name} da ton tai -> bo qua")
            continue
        spark.sql(f"ALTER TABLE {TARGET} ADD COLUMN {name} {dtype}")
        print(f"  + ADD COLUMN {name} {dtype} (KHONG rewrite data)")

    print(f"\nSchema evolution OK. Schema hien tai cua {TARGET}:")
    spark.sql(f"DESCRIBE {TARGET}").show(60, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
