# src/silver/verify_silver.py
"""
Verify Silver layer bằng spark.sql:
- Đếm row count tất cả bảng Silver
- In audit log Silver gần nhất
- (tuỳ chọn) demo lịch sử SCD2 cho 1 customer_id

Chạy:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/silver/verify_silver.py [customer_id]
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

SILVER_TABLES = ["dim_customer", "dim_account", "dim_card_account", "dim_product",
                 "dim_branch", "dim_loan", "fct_bank_transactions", "fct_card_transactions"]


def main():
    spark = get_spark_session("silver-verify")
    cust  = sys.argv[1] if len(sys.argv) > 1 else None

    print("\n==================== SILVER ROW COUNTS ====================")
    for t in SILVER_TABLES:
        try:
            print(f"  {t:<24}: {spark.table(f'nessie.silver.{t}').count()}")
        except Exception as e:
            print(f"  {t:<24}: ERROR {e}")

    print("\n==================== SILVER AUDIT LOG (20 gan nhat) ====================")
    try:
        (spark.table("nessie.silver.audit_log")
            .orderBy("current_run_time", ascending=False)
            .select("batch_id", "table_name", "run_mode", "status",
                    "rows_ingested", "current_run_time")
            .show(20, truncate=False))
    except Exception as e:
        print(f"  audit_log ERROR: {e}")

    if cust:
        print(f"\n========== SCD2 history dim_customer = {cust} ==========")
        (spark.table("nessie.silver.dim_customer")
            .filter(f"customer_id = '{cust}'")
            .select("customer_id", "customer_segment", "city", "version",
                    "is_current", "effective_from", "effective_to")
            .orderBy("version").show(truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
