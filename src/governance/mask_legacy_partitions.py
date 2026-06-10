# src/governance/mask_legacy_partitions.py
"""
Re-mask PII THO con sot lai trong mart_customer_360 (Phase 4 Governance).

Boi canh: mart partition theo snapshot_date. overwritePartitions() chi ghi de partition
trung snapshot_date -> cac partition build TRUOC khi co masking (vd 2025-12-31 tu Phase 3)
van con full_name THO. Script nay UPDATE tai cho (copy-on-write) cac dong chua che, GIU
nguyen lich su (so dong / snapshot_date khong doi).

phone_masked / email_masked KHONG dung duoc o partition cu (la cot moi them sau, data cu = NULL)
-> dung lam minh chung Schema Evolution: data cu co cot moi = NULL.

Idempotent: WHERE loai cac dong da che (full_name LIKE '% ****') -> chay lai khong doi gi.

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/governance/mask_legacy_partitions.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session
from common.masking import mask_name

TARGET = "nessie.gold.mart_customer_360"


def main():
    spark = get_spark_session("mask-legacy")

    before = spark.sql(f"""
        SELECT count(*) c FROM {TARGET}
        WHERE full_name IS NOT NULL AND full_name NOT LIKE '% ****'
    """).collect()[0]["c"]
    print(f"PII tho con lai TRUOC khi mask: {before} dong")

    if before == 0:
        print("Khong con PII tho -> bo qua (idempotent).")
        spark.stop()
        return

    # UPDATE tai cho: chi cac dong chua che. mask_name() = bieu thuc dung trong build_mart.
    spark.sql(f"""
        UPDATE {TARGET}
        SET full_name = {mask_name('full_name')}
        WHERE full_name IS NOT NULL AND full_name NOT LIKE '% ****'
    """)

    after = spark.sql(f"""
        SELECT count(*) c FROM {TARGET}
        WHERE full_name IS NOT NULL AND full_name NOT LIKE '% ****'
    """).collect()[0]["c"]
    print(f"PII tho con lai SAU khi mask:  {after} dong (ky vong 0)")
    print("Phan bo theo snapshot_date sau khi mask:")
    spark.sql(f"""
        SELECT snapshot_date, count(*) n,
               sum(CASE WHEN full_name LIKE '% ****' THEN 1 ELSE 0 END) masked
        FROM {TARGET} GROUP BY snapshot_date ORDER BY snapshot_date
    """).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
