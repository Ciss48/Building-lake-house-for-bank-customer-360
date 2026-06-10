# src/bronze/verify_bronze.py
"""
Verify Bronze layer bằng spark.sql (thay cho Trino).
- Đếm row count tất cả bảng Bronze
- In audit log gần nhất
- (tuỳ chọn) kiểm tra 1 customer_id cụ thể

Chạy:
    docker exec -it lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/bronze/verify_bronze.py [customer_id]
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

BRONZE_TABLES = [
    "oracle_branches",
    "oracle_products",
    "oracle_bank_accounts",
    "oracle_loans",
    "oracle_bank_transactions",
    "pg_customers",
    "pg_card_accounts",
    "pg_card_transactions",
]


def main():
    spark = get_spark_session("bronze-verify")
    check_customer = sys.argv[1] if len(sys.argv) > 1 else None

    print("\n==================== BRONZE ROW COUNTS ====================")
    for t in BRONZE_TABLES:
        try:
            cnt = spark.table(f"nessie.bronze.{t}").count()
            print(f"  {t:<28} : {cnt}")
        except Exception as e:
            print(f"  {t:<28} : ERROR {e}")

    print("\n==================== AUDIT LOG (20 gan nhat) ====================")
    try:
        (spark.table("nessie.bronze.audit_log")
            .orderBy("current_run_time", ascending=False)
            .select("batch_id", "table_name", "run_mode", "status",
                    "rows_ingested", "current_run_time")
            .show(20, truncate=False))
    except Exception as e:
        print(f"  audit_log ERROR: {e}")

    if check_customer:
        print(f"\n==================== CHECK customer_id = {check_customer} ====================")
        (spark.table("nessie.bronze.pg_customers")
            .filter(f"customer_id = '{check_customer}'")
            .select("customer_id", "full_name", "customer_segment", "_batch_id")
            .show(truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
