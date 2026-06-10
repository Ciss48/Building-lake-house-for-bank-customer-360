# src/gold/create_gold_tables.py
"""
Tao namespace nessie.gold + 4 bang Iceberg (2 agg + mart_customer_360 + audit_log).
Chay 1 lan khi khoi tao Gold layer (idempotent nho CREATE TABLE IF NOT EXISTS).

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/gold/create_gold_tables.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session


def create(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    # ── agg_customer_holdings (product holding + balance + risk, 1 row/KH) ────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.agg_customer_holdings (
            customer_id            STRING,
            total_product_count    INT,
            has_savings            BOOLEAN,
            has_current            BOOLEAN,
            has_credit_card        BOOLEAN,
            has_loan               BOOLEAN,
            total_deposit_balance  DOUBLE,
            total_loan_outstanding DOUBLE,
            total_credit_limit     DOUBLE,
            total_card_outstanding DOUBLE,
            credit_utilization_rate DOUBLE,
            net_asset_value        DOUBLE,
            has_npl_loan           BOOLEAN,
            days_past_due          INT,
            first_open_date        DATE,
            _gold_loaded_at        TIMESTAMP,
            _gold_batch_id         STRING
        ) USING iceberg
    """)

    # ── agg_customer_txn_12m (transaction 12 thang, 1 row/KH) ─────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.agg_customer_txn_12m (
            customer_id        STRING,
            txn_count_12m      INT,
            txn_amount_12m     DOUBLE,
            avg_monthly_spend  DOUBLE,
            active_months_12m  INT,
            top_spend_category STRING,
            digital_txn_ratio  DOUBLE,
            last_txn_date      DATE,
            recency_days       INT,
            frequency_12m      INT,
            monetary_12m       DOUBLE,
            _gold_loaded_at    TIMESTAMP,
            _gold_batch_id     STRING
        ) USING iceberg
    """)

    # ── mart_customer_360 (1 row/KH, 38 cot KPI, partition snapshot_date) ─────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.mart_customer_360 (
            -- Identity
            customer_id STRING, full_name STRING, age INT, gender STRING,
            customer_segment STRING, kyc_status STRING, city STRING, province STRING,
            -- Product holding
            total_product_count INT, has_savings BOOLEAN, has_current BOOLEAN,
            has_credit_card BOOLEAN, has_loan BOOLEAN,
            -- Balance
            total_deposit_balance DOUBLE, total_loan_outstanding DOUBLE,
            credit_utilization_rate DOUBLE, net_asset_value DOUBLE,
            -- Transaction
            txn_count_12m INT, txn_amount_12m DOUBLE, avg_monthly_spend DOUBLE,
            top_spend_category STRING, digital_txn_ratio DOUBLE,
            last_txn_date DATE, active_months_12m INT,
            -- RFM
            recency_days INT, frequency_12m INT, monetary_12m DOUBLE,
            r_score INT, f_score INT, m_score INT, rfm_score STRING, rfm_segment STRING,
            -- Risk
            has_npl_loan BOOLEAN, days_past_due INT,
            -- Cross-sell
            cross_sell_loan_flag BOOLEAN, cross_sell_card_flag BOOLEAN,
            cross_sell_invest_flag BOOLEAN, tenure_days INT,
            -- meta
            snapshot_date DATE, _gold_loaded_at TIMESTAMP, _gold_batch_id STRING
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
    """)

    # ── audit_log (giong schema bronze/silver.audit_log) ─────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.audit_log (
            batch_id STRING, table_name STRING, source_system STRING, run_mode STRING,
            status STRING, rows_ingested LONG, last_run_time TIMESTAMP,
            current_run_time TIMESTAMP, error_message STRING, created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    print("Tat ca Gold tables da tao xong trong nessie.gold")
    spark.sql("SHOW TABLES IN nessie.gold").show(truncate=False)


if __name__ == "__main__":
    spark = get_spark_session("create-gold-tables")
    create(spark)
    spark.stop()
