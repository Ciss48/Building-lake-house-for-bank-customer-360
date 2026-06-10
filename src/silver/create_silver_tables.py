# src/silver/create_silver_tables.py
"""
Tạo namespace nessie.silver + 9 bảng Iceberg (6 dim + 2 fact + audit_log).
Chạy 1 lần khi khởi tạo Silver layer (idempotent nhờ CREATE TABLE IF NOT EXISTS).

Chạy:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/silver/create_silver_tables.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session


def create(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    # ── dim_customer (SCD Type 2) ────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_customer (
            customer_sk       STRING,        -- surrogate key (md5 customer_id|effective_from)
            customer_id       STRING,        -- natural key
            full_name         STRING,
            date_of_birth     DATE,
            gender            STRING,
            id_number         STRING,
            phone             STRING,
            email             STRING,
            city              STRING,
            province          STRING,
            customer_segment  STRING,
            kyc_status        STRING,
            row_hash          STRING,        -- hash các tracked column
            effective_from    TIMESTAMP,
            effective_to      TIMESTAMP,
            is_current        BOOLEAN,
            version           INT,
            _silver_loaded_at TIMESTAMP,
            _silver_batch_id  STRING
        ) USING iceberg
        TBLPROPERTIES ('write.format.default'='parquet',
                       'write.parquet.compression-codec'='snappy')
    """)

    # ── dim_account (SCD1) ───────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_account (
            account_id    STRING, customer_id STRING, product_id STRING, branch_id STRING,
            account_type  STRING, account_no STRING, balance DOUBLE, currency STRING,
            status STRING, opened_date DATE, closed_date DATE, interest_rate DOUBLE,
            updated_at TIMESTAMP, _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_card_account (SCD1) ──────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_card_account (
            account_id STRING, customer_id STRING, card_type STRING, card_number STRING,
            credit_limit DOUBLE, outstanding_balance DOUBLE, due_date DATE, status STRING,
            opened_date DATE, closed_date DATE, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_product (SCD1) ───────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_product (
            product_id STRING, product_name STRING, product_type STRING,
            interest_rate DOUBLE, status STRING, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_branch (SCD1) ────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_branch (
            branch_id STRING, branch_name STRING, region STRING, city STRING,
            branch_type STRING, status STRING, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── dim_loan (SCD1) ──────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.dim_loan (
            loan_id STRING, customer_id STRING, product_id STRING, branch_id STRING,
            loan_type STRING, principal_amount DOUBLE, outstanding_amount DOUBLE,
            interest_rate DOUBLE, disbursement_date DATE, maturity_date DATE,
            npl_status STRING, status STRING, updated_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
    """)

    # ── fct_bank_transactions (Fact) ─────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.fct_bank_transactions (
            txn_id STRING, account_id STRING, txn_date DATE, txn_datetime TIMESTAMP,
            txn_type STRING, amount DOUBLE, balance_after DOUBLE, channel STRING,
            status STRING, created_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # ── fct_card_transactions (Fact) ─────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.fct_card_transactions (
            txn_id STRING, account_id STRING, txn_date DATE, txn_datetime TIMESTAMP,
            amount DOUBLE, currency STRING, txn_type STRING, merchant_name STRING,
            merchant_category STRING, channel STRING, status STRING, created_at TIMESTAMP,
            _silver_loaded_at TIMESTAMP, _silver_batch_id STRING
        ) USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # ── audit_log (giống schema bronze.audit_log) ────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.audit_log (
            batch_id STRING, table_name STRING, source_system STRING, run_mode STRING,
            status STRING, rows_ingested LONG, last_run_time TIMESTAMP,
            current_run_time TIMESTAMP, error_message STRING, created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    print("Tat ca Silver tables da tao xong trong nessie.silver")
    spark.sql("SHOW TABLES IN nessie.silver").show(truncate=False)


if __name__ == "__main__":
    spark = get_spark_session("create-silver-tables")
    create(spark)
    spark.stop()
