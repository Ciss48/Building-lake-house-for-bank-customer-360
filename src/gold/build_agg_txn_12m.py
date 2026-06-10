# src/gold/build_agg_txn_12m.py
"""
agg_customer_txn_12m: aggregate giao dich (bank + card) trong cua so lookback_months
tinh tu as_of, theo customer_id. Gom luon recency/frequency/monetary cho RFM.
Ghi OVERWRITE. KH khong co txn trong cua so -> KHONG xuat hien (mart LEFT JOIN + COALESCE).
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.spark_session import align_to_table

TARGET = "nessie.gold.agg_customer_txn_12m"


def build_agg_txn_12m(spark, cfg: dict, as_of: str, batch_id: str) -> int:
    lookback = int(cfg.get("lookback_months", 12))
    digital = ", ".join("'%s'" % c for c in cfg["thresholds"]["digital_channels"])

    sql = f"""
        WITH txns AS (
            SELECT a.customer_id, f.txn_date, f.amount, f.channel,
                   CAST(NULL AS STRING) AS merchant_category
            FROM nessie.silver.fct_bank_transactions f
            JOIN nessie.silver.dim_account a ON f.account_id = a.account_id
            WHERE f.txn_date > add_months(DATE '{as_of}', -{lookback}) AND f.status = 'COMPLETED'
            UNION ALL
            SELECT ca.customer_id, f.txn_date, f.amount, f.channel, f.merchant_category
            FROM nessie.silver.fct_card_transactions f
            JOIN nessie.silver.dim_card_account ca ON f.account_id = ca.account_id
            WHERE f.txn_date > add_months(DATE '{as_of}', -{lookback}) AND f.status = 'COMPLETED'
        ),
        base AS (
            SELECT customer_id,
                   COUNT(*)    AS txn_count_12m,
                   SUM(amount) AS txn_amount_12m,
                   COUNT(DISTINCT date_format(txn_date, 'yyyy-MM')) AS active_months_12m,
                   MAX(txn_date) AS last_txn_date,
                   SUM(CASE WHEN channel IN ({digital}) THEN 1 ELSE 0 END) / COUNT(*) AS digital_txn_ratio
            FROM txns GROUP BY customer_id
        ),
        topcat AS (
            SELECT customer_id, merchant_category FROM (
                SELECT customer_id, merchant_category,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY SUM(amount) DESC) AS rn
                FROM txns WHERE merchant_category IS NOT NULL
                GROUP BY customer_id, merchant_category
            ) WHERE rn = 1
        )
        SELECT b.customer_id,
               CAST(b.txn_count_12m AS INT)      AS txn_count_12m,
               b.txn_amount_12m,
               b.txn_amount_12m / {lookback}      AS avg_monthly_spend,
               CAST(b.active_months_12m AS INT)  AS active_months_12m,
               t.merchant_category               AS top_spend_category,
               b.digital_txn_ratio,
               b.last_txn_date,
               CAST(DATEDIFF(DATE '{as_of}', b.last_txn_date) AS INT) AS recency_days,
               CAST(b.txn_count_12m AS INT)      AS frequency_12m,
               b.txn_amount_12m                  AS monetary_12m
        FROM base b
        LEFT JOIN topcat t ON b.customer_id = t.customer_id
    """
    df = spark.sql(sql)
    df = (df.withColumn("_gold_loaded_at", F.current_timestamp())
            .withColumn("_gold_batch_id", F.lit(batch_id)))
    df = align_to_table(spark, df, TARGET)
    cnt = df.count()
    df.writeTo(TARGET).overwritePartitions()
    print(f"  OK agg_customer_txn_12m: {cnt} rows")
    return cnt
