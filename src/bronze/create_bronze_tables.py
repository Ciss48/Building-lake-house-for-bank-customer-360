# src/bronze/create_bronze_tables.py
"""
Tạo Bronze Iceberg tables trong Nessie catalog. CHẠY 1 LẦN trước khi ingest.

Chạy:
    docker exec -it lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/bronze/create_bronze_tables.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")

from common.spark_session import get_spark_session


def create_bronze_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    # ── Oracle tables ────────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_branches (
            branch_id    STRING,
            branch_name  STRING,
            region       STRING,
            city         STRING,
            branch_type  STRING,
            status       STRING,
            updated_at   TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_products (
            product_id    STRING,
            product_name  STRING,
            product_type  STRING,
            interest_rate DOUBLE,
            status        STRING,
            updated_at    TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_bank_accounts (
            account_id    STRING,
            customer_id   STRING,
            product_id    STRING,
            branch_id     STRING,
            account_type  STRING,
            account_no    STRING,
            balance       DOUBLE,
            currency      STRING,
            status        STRING,
            opened_date   DATE,
            closed_date   DATE,
            interest_rate DOUBLE,
            updated_at    TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
        PARTITIONED BY (months(opened_date))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_loans (
            loan_id            STRING,
            customer_id        STRING,
            product_id         STRING,
            branch_id          STRING,
            loan_type          STRING,
            principal_amount   DOUBLE,
            outstanding_amount DOUBLE,
            interest_rate      DOUBLE,
            disbursement_date  DATE,
            maturity_date      DATE,
            npl_status         STRING,
            status             STRING,
            updated_at         TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
        PARTITIONED BY (months(disbursement_date))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.oracle_bank_transactions (
            txn_id        STRING,
            account_id    STRING,
            txn_date      DATE,
            txn_datetime  TIMESTAMP,
            txn_type      STRING,
            amount        DOUBLE,
            balance_after DOUBLE,
            channel       STRING,
            status        STRING,
            created_at    TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # ── PostgreSQL tables ────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.pg_customers (
            customer_id      STRING,
            full_name        STRING,
            date_of_birth    DATE,
            gender           STRING,
            id_number        STRING,
            phone            STRING,
            email            STRING,
            city             STRING,
            province         STRING,
            customer_segment STRING,
            kyc_status       STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.pg_card_accounts (
            account_id          STRING,
            customer_id         STRING,
            card_type           STRING,
            card_number         STRING,
            credit_limit        DOUBLE,
            outstanding_balance DOUBLE,
            due_date            DATE,
            status              STRING,
            opened_date         DATE,
            closed_date         DATE,
            created_at          TIMESTAMP,
            updated_at          TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.pg_card_transactions (
            txn_id            STRING,
            account_id        STRING,
            txn_date          DATE,
            txn_datetime      TIMESTAMP,
            amount            DOUBLE,
            currency          STRING,
            txn_type          STRING,
            merchant_name     STRING,
            merchant_category STRING,
            channel           STRING,
            status            STRING,
            created_at        TIMESTAMP,
            _ingested_at    TIMESTAMP,
            _source_system  STRING,
            _batch_id       STRING
        ) USING iceberg
        PARTITIONED BY (days(txn_date))
    """)

    # ── Audit table ──────────────────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.audit_log (
            batch_id         STRING,
            table_name       STRING,
            source_system    STRING,
            run_mode         STRING,
            status           STRING,
            rows_ingested    LONG,
            last_run_time    TIMESTAMP,
            current_run_time TIMESTAMP,
            error_message    STRING,
            created_at       TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    print("Tat ca Bronze tables da tao xong trong nessie.bronze")
    spark.sql("SHOW TABLES IN nessie.bronze").show(truncate=False)


if __name__ == "__main__":
    spark = get_spark_session("create-bronze-tables")
    create_bronze_tables(spark)
    spark.stop()
