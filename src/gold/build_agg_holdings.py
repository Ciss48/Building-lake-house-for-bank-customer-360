# src/gold/build_agg_holdings.py
"""
agg_customer_holdings: gom product holding + balance + risk theo customer_id.
Nguon: silver.dim_account / dim_card_account / dim_loan + dim_customer(current).
Ghi OVERWRITE (snapshot tinh lai toan bo). 1 row / customer_id.
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.spark_session import align_to_table

TARGET = "nessie.gold.agg_customer_holdings"


def build_agg_holdings(spark, cfg: dict, as_of: str, batch_id: str) -> int:
    sql = f"""
        WITH cust AS (
            SELECT DISTINCT customer_id
            FROM nessie.silver.dim_customer WHERE is_current = true
        ),
        bank AS (
            SELECT customer_id,
                   COUNT(*) AS n_bank,
                   MAX(CASE WHEN account_type = 'TERM_DEPOSIT' THEN 1 ELSE 0 END) AS f_savings,
                   MAX(CASE WHEN account_type = 'CURRENT'      THEN 1 ELSE 0 END) AS f_current,
                   SUM(COALESCE(balance, 0)) AS deposit_balance,
                   MIN(opened_date) AS bank_first_open
            FROM nessie.silver.dim_account
            WHERE status = 'ACTIVE'
            GROUP BY customer_id
        ),
        card AS (
            SELECT customer_id,
                   COUNT(*) AS n_card,
                   MAX(CASE WHEN card_type LIKE '%CREDIT%' THEN 1 ELSE 0 END) AS f_credit,
                   SUM(COALESCE(credit_limit, 0))        AS credit_limit,
                   SUM(COALESCE(outstanding_balance, 0)) AS card_out,
                   MIN(opened_date) AS card_first_open
            FROM nessie.silver.dim_card_account
            WHERE status = 'ACTIVE'
            GROUP BY customer_id
        ),
        loan AS (
            SELECT customer_id,
                   COUNT(*) AS n_loan,
                   SUM(COALESCE(outstanding_amount, 0)) AS loan_out,
                   MAX(CASE WHEN npl_status <> 'NORMAL' THEN 1 ELSE 0 END) AS f_npl,
                   MAX(CASE WHEN npl_status <> 'NORMAL'
                            THEN GREATEST(DATEDIFF(DATE '{as_of}', maturity_date), 0)
                            ELSE 0 END) AS dpd
            FROM nessie.silver.dim_loan
            WHERE status = 'ACTIVE'
            GROUP BY customer_id
        )
        SELECT
            c.customer_id,
            CAST(COALESCE(b.n_bank,0) + COALESCE(cd.n_card,0) + COALESCE(l.n_loan,0) AS INT)
                AS total_product_count,
            COALESCE(b.f_savings,0) = 1 AS has_savings,
            COALESCE(b.f_current,0) = 1 AS has_current,
            COALESCE(cd.f_credit,0) = 1 AS has_credit_card,
            COALESCE(l.n_loan,0) > 0 AS has_loan,
            COALESCE(b.deposit_balance,0) AS total_deposit_balance,
            COALESCE(l.loan_out,0)        AS total_loan_outstanding,
            COALESCE(cd.credit_limit,0)   AS total_credit_limit,
            COALESCE(cd.card_out,0)       AS total_card_outstanding,
            CASE WHEN COALESCE(cd.credit_limit,0) > 0
                 THEN COALESCE(cd.card_out,0) / cd.credit_limit ELSE 0 END AS credit_utilization_rate,
            COALESCE(b.deposit_balance,0) - COALESCE(l.loan_out,0) - COALESCE(cd.card_out,0)
                AS net_asset_value,
            COALESCE(l.f_npl,0) = 1 AS has_npl_loan,
            CAST(COALESCE(l.dpd,0) AS INT) AS days_past_due,
            LEAST(COALESCE(b.bank_first_open, DATE '{as_of}'),
                  COALESCE(cd.card_first_open, DATE '{as_of}')) AS first_open_date
        FROM cust c
        LEFT JOIN bank b  ON c.customer_id = b.customer_id
        LEFT JOIN card cd ON c.customer_id = cd.customer_id
        LEFT JOIN loan l  ON c.customer_id = l.customer_id
    """
    df = spark.sql(sql)
    df = (df.withColumn("_gold_loaded_at", F.current_timestamp())
            .withColumn("_gold_batch_id", F.lit(batch_id)))
    df = align_to_table(spark, df, TARGET)
    cnt = df.count()
    df.writeTo(TARGET).overwritePartitions()
    print(f"  OK agg_customer_holdings: {cnt} rows")
    return cnt
