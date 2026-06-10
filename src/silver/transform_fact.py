# src/silver/transform_fact.py
"""
Fact transform: cleansing + MERGE insert (append idempotent).
Rerun không nhân đôi nhờ MERGE ON natural_key WHEN NOT MATCHED INSERT.

Dùng cho: fct_bank_transactions, fct_card_transactions.
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import align_to_table
from common.cleansing import (
    clean_strings, upper_cols, dedup_latest, add_silver_meta, drop_null_keys,
)

_UPPER = ["status", "currency", "txn_type", "channel"]


def transform_fact(spark, table_name: str, cfg: dict, batch_id: str) -> int:
    source = f"nessie.bronze.{cfg['source']}"
    target = f"nessie.silver.{table_name}"
    nk     = cfg["natural_key"]

    df = spark.table(source)
    df = clean_strings(df)
    df = upper_cols(df, _UPPER)
    df = drop_null_keys(df, [nk])
    df = dedup_latest(df, [nk], "_ingested_at")   # phòng trùng txn_id trong Bronze
    df = add_silver_meta(df, batch_id)
    df = align_to_table(spark, df, target)

    cnt = df.count()
    df.createOrReplaceTempView("fact_src")
    spark.sql(f"""
        MERGE INTO {target} AS t USING fact_src AS s
        ON t.{nk} = s.{nk}
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  OK FACT {table_name}: source {cnt} rows (insert neu chua co)")
    return cnt


if __name__ == "__main__":
    pass
