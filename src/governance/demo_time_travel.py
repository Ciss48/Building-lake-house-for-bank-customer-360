# src/governance/demo_time_travel.py
"""
DEMO Time Travel (Phase 4): liet ke lich su snapshot cua mart_customer_360 va
query data tai snapshot cu bang Iceberg `VERSION AS OF <snapshot_id>`.

Moi lan build mart (overwritePartitions) tao 1 snapshot moi -> co nhieu snapshot
de du lieu hoa audit / rollback.

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/governance/demo_time_travel.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

TABLE = "nessie.gold.mart_customer_360"


def main():
    spark = get_spark_session("time-travel")

    print(f"== Lich su snapshot {TABLE} ==")
    snaps = spark.sql(f"""
        SELECT committed_at, snapshot_id, operation
        FROM {TABLE}.snapshots ORDER BY committed_at
    """)
    snaps.show(truncate=False)
    rows = snaps.collect()

    now = spark.table(TABLE).count()
    print(f"Row count HIEN TAI: {now}")

    if len(rows) >= 2:
        old_id = rows[0]["snapshot_id"]
        old_ts = rows[0]["committed_at"]
        old = spark.sql(
            f"SELECT count(*) c FROM {TABLE} VERSION AS OF {old_id}"
        ).collect()[0]["c"]
        print(f"\n== Time travel ve snapshot dau ({old_id} @ {old_ts}) ==")
        print(f"  snapshot dau: {old} rows | hien tai: {now} rows")

        # Demo 1 — PII: full_name o snapshot dau (THO, truoc masking) vs hien tai (CHE).
        print("\n== PII truoc/sau governance (cung vai customer_id) ==")
        spark.sql(f"""
            SELECT o.customer_id,
                   o.full_name AS full_name_snapshot_dau,
                   n.full_name AS full_name_hien_tai
            FROM (SELECT customer_id, full_name FROM {TABLE} VERSION AS OF {old_id}) o
            JOIN (SELECT customer_id, full_name FROM {TABLE}
                  WHERE snapshot_date = (SELECT max(snapshot_date) FROM {TABLE})) n
              ON o.customer_id = n.customer_id
            ORDER BY o.customer_id LIMIT 5
        """).show(truncate=False)

        # Demo 2 — Schema Evolution: cot phone_masked KHONG ton tai o snapshot truoc ALTER.
        print("== Schema Evolution: query phone_masked tai snapshot dau ==")
        try:
            spark.sql(f"SELECT phone_masked FROM {TABLE} VERSION AS OF {old_id} LIMIT 1").collect()
            print("  (cot phone_masked resolve duoc o snapshot dau)")
        except Exception as e:
            head = str(e).splitlines()[0][:120]
            print(f"  [MINH CHUNG] phone_masked KHONG resolve o snapshot dau: {head}")
            print("  -> cot mothem sau ALTER, Iceberg KHONG dung file/metadata cu (schema evolution that).")
    else:
        print("\n(Chi co 1 snapshot — build mart them 1 lan nua de thay time travel ro hon.)")

    spark.stop()


if __name__ == "__main__":
    main()
