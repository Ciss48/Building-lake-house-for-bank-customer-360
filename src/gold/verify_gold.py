# src/gold/verify_gold.py
"""
Verify Gold: mart row count == dim_customer current; RFM segment distribution;
cross-sell counts; (optional) in chi tiet 1 KH neu truyen customer_id.

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/gold/verify_gold.py [CIF000001]
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session


def main():
    spark = get_spark_session("gold-verify")
    cust = sys.argv[1] if len(sys.argv) > 1 else None

    n_cust = spark.table("nessie.silver.dim_customer").filter("is_current = true").count()
    n_mart = spark.table("nessie.gold.mart_customer_360").count()
    print(f"\ndim_customer current = {n_cust} | mart_customer_360 = {n_mart}  (phai BANG nhau)")

    print("\n==================== RFM segment distribution ====================")
    spark.sql("""
        SELECT rfm_segment, COUNT(*) AS n,
               ROUND(AVG(monetary_12m), 0) AS avg_monetary,
               ROUND(AVG(recency_days), 0) AS avg_recency
        FROM nessie.gold.mart_customer_360
        GROUP BY rfm_segment ORDER BY n DESC
    """).show(truncate=False)

    print("==================== Cross-sell counts ====================")
    spark.sql("""
        SELECT SUM(CAST(cross_sell_loan_flag   AS INT)) AS loan_candidates,
               SUM(CAST(cross_sell_card_flag    AS INT)) AS card_candidates,
               SUM(CAST(cross_sell_invest_flag  AS INT)) AS invest_candidates,
               SUM(CAST(has_npl_loan            AS INT)) AS npl_customers
        FROM nessie.gold.mart_customer_360
    """).show(truncate=False)

    print("==================== Segment x avg net asset ====================")
    spark.sql("""
        SELECT customer_segment, COUNT(*) AS n,
               ROUND(AVG(net_asset_value), 0) AS avg_net_asset,
               ROUND(AVG(total_product_count), 2) AS avg_products
        FROM nessie.gold.mart_customer_360
        GROUP BY customer_segment ORDER BY avg_net_asset DESC
    """).show(truncate=False)

    if cust:
        print(f"\n==================== mart_customer_360 = {cust} ====================")
        spark.table("nessie.gold.mart_customer_360") \
             .filter(f"customer_id = '{cust}'").show(vertical=True, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
