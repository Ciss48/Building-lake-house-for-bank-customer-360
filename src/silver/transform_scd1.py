# src/silver/transform_scd1.py
"""
SCD Type 1 generic: đọc full Bronze -> cleansing -> dedup latest -> MERGE upsert.
Chỉ giữ giá trị mới nhất (overwrite khi key đã tồn tại), KHÔNG giữ lịch sử.

Dùng cho: dim_account, dim_card_account, dim_product, dim_branch, dim_loan.
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import align_to_table
from common.cleansing import (
    clean_strings, upper_cols, dedup_latest, add_silver_meta, drop_null_keys,
)

# Cột nên chuẩn hoá UPPERCASE nếu tồn tại trong nguồn.
_UPPER = ["status", "currency", "npl_status", "account_type", "card_type",
          "product_type", "branch_type", "kyc_status"]


def transform_scd1(spark, table_name: str, cfg: dict, batch_id: str) -> int:
    source = f"nessie.bronze.{cfg['source']}"
    target = f"nessie.silver.{table_name}"
    nk     = cfg["natural_key"]

    df = spark.table(source)
    df = clean_strings(df)
    df = upper_cols(df, _UPPER)
    df = drop_null_keys(df, [nk])
    df = dedup_latest(df, [nk], "_ingested_at")
    df = add_silver_meta(df, batch_id)
    # align_to_table cast khớp schema đích + tự bỏ cột Bronze-only
    # (_ingested_at, _source_system, _batch_id) không có trong bảng Silver.
    df = align_to_table(spark, df, target)

    cnt = df.count()
    df.createOrReplaceTempView("scd1_src")
    spark.sql(f"""
        MERGE INTO {target} AS t USING scd1_src AS s
        ON t.{nk} = s.{nk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  OK SCD1 {table_name}: upsert {cnt} rows")
    return cnt
