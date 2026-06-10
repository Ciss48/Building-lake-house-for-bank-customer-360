# src/gold/build_mart_360.py
"""
mart_customer_360: join dim_customer(current) + 2 agg gold -> tinh identity (age, tenure),
RFM (NTILE 5), cross-sell flags (rule tu gold_tables.yaml). 1 row / customer_id.
Ghi INSERT OVERWRITE partition snapshot_date = as_of (idempotent; giu lich su snapshot ngay khac).
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.spark_session import align_to_table
from common.masking import mask_name, mask_phone, mask_email

TARGET = "nessie.gold.mart_customer_360"


def build_mart_360(spark, cfg: dict, as_of: str, batch_id: str) -> int:
    cs = cfg["crosssell"]
    loan_rule   = cs["loan_flag"]
    card_rule   = cs["card_flag"]
    invest_rule = cs["invest_flag"]

    sql = f"""
        WITH cust AS (
            SELECT customer_id, full_name, gender, customer_segment, kyc_status,
                   city, province, date_of_birth, phone, email
            FROM nessie.silver.dim_customer WHERE is_current = true
        ),
        j AS (
            SELECT
                c.customer_id, c.full_name, c.phone, c.email,
                CAST(floor(months_between(DATE '{as_of}', c.date_of_birth) / 12) AS INT) AS age,
                c.gender, c.customer_segment, c.kyc_status, c.city, c.province,
                COALESCE(h.total_product_count, 0)     AS total_product_count,
                COALESCE(h.has_savings, false)         AS has_savings,
                COALESCE(h.has_current, false)         AS has_current,
                COALESCE(h.has_credit_card, false)     AS has_credit_card,
                COALESCE(h.has_loan, false)            AS has_loan,
                COALESCE(h.total_deposit_balance, 0)   AS total_deposit_balance,
                COALESCE(h.total_loan_outstanding, 0)  AS total_loan_outstanding,
                COALESCE(h.credit_utilization_rate, 0) AS credit_utilization_rate,
                COALESCE(h.net_asset_value, 0)         AS net_asset_value,
                COALESCE(t.txn_count_12m, 0)           AS txn_count_12m,
                COALESCE(t.txn_amount_12m, 0)          AS txn_amount_12m,
                COALESCE(t.avg_monthly_spend, 0)       AS avg_monthly_spend,
                t.top_spend_category,
                COALESCE(t.digital_txn_ratio, 0)       AS digital_txn_ratio,
                t.last_txn_date,
                COALESCE(t.active_months_12m, 0)       AS active_months_12m,
                t.recency_days,
                COALESCE(t.frequency_12m, 0)           AS frequency_12m,
                COALESCE(t.monetary_12m, 0)            AS monetary_12m,
                COALESCE(h.has_npl_loan, false)        AS has_npl_loan,
                COALESCE(h.days_past_due, 0)           AS days_past_due,
                CAST(DATEDIFF(DATE '{as_of}', h.first_open_date) AS INT) AS tenure_days
            FROM cust c
            LEFT JOIN nessie.gold.agg_customer_holdings h ON c.customer_id = h.customer_id
            LEFT JOIN nessie.gold.agg_customer_txn_12m  t ON c.customer_id = t.customer_id
        ),
        rfm AS (
            SELECT *,
                NTILE(5) OVER (ORDER BY COALESCE(recency_days, 999999) DESC) AS r_score,
                NTILE(5) OVER (ORDER BY frequency_12m ASC)                   AS f_score,
                NTILE(5) OVER (ORDER BY monetary_12m ASC)                    AS m_score
            FROM j
        )
        SELECT
            customer_id, {mask_name('full_name')} AS full_name, age, gender,
            customer_segment, kyc_status, city, province,
            total_product_count, has_savings, has_current, has_credit_card, has_loan,
            total_deposit_balance, total_loan_outstanding, credit_utilization_rate, net_asset_value,
            txn_count_12m, txn_amount_12m, avg_monthly_spend, top_spend_category, digital_txn_ratio,
            last_txn_date, active_months_12m,
            recency_days, frequency_12m, monetary_12m,
            r_score, f_score, m_score,
            CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_score,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                WHEN f_score >= 4                                   THEN 'Loyal'
                WHEN r_score >= 4 AND f_score >= 2                  THEN 'Potential_Loyal'
                WHEN r_score >= 4 AND f_score <  2                  THEN 'New'
                WHEN r_score <= 2 AND f_score >= 3                  THEN 'At_Risk'
                WHEN r_score <= 2 AND f_score <= 2                  THEN 'Hibernating'
                ELSE 'Need_Attention'
            END AS rfm_segment,
            has_npl_loan, days_past_due,
            ({loan_rule})   AS cross_sell_loan_flag,
            ({card_rule})   AS cross_sell_card_flag,
            ({invest_rule}) AS cross_sell_invest_flag,
            tenure_days,
            {mask_phone('phone')} AS phone_masked,
            {mask_email('email')} AS email_masked,
            DATE '{as_of}' AS snapshot_date
        FROM rfm
    """
    df = spark.sql(sql)
    df = (df.withColumn("_gold_loaded_at", F.current_timestamp())
            .withColumn("_gold_batch_id", F.lit(batch_id)))
    df = align_to_table(spark, df, TARGET)
    cnt = df.count()
    df.writeTo(TARGET).overwritePartitions()
    print(f"  OK mart_customer_360: {cnt} rows (snapshot_date={as_of})")
    return cnt
