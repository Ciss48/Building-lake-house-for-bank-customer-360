# src/common/cleansing.py
"""
Helpers cleansing dùng chung cho Silver layer.

Tách riêng khỏi từng transform để mọi job (scd1/scd2/fact) cùng áp một bộ quy tắc
làm sạch nhất quán: trim string, rỗng -> NULL, chuẩn hoá UPPERCASE, dedup theo natural
key (giữ bản mới nhất theo _ingested_at), gắn metadata Silver, loại row natural key NULL.
"""
from datetime import datetime, timezone
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim mọi cột string; chuỗi rỗng '' -> NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            c = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(c == "", None).otherwise(c))
    return df


def upper_cols(df: DataFrame, cols: list) -> DataFrame:
    """Chuẩn hoá UPPERCASE cho status/currency... (bỏ qua cột không tồn tại)."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.col(c)))
    return df


def dedup_latest(df: DataFrame, keys: list, order_col: str = "_ingested_at") -> DataFrame:
    """Giữ row mới nhất theo order_col cho mỗi natural key (xử lý late arrival / trùng PK)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def add_silver_meta(df: DataFrame, batch_id: str) -> DataFrame:
    """Thêm metadata Silver (thời điểm load + batch id)."""
    return (df
            .withColumn("_silver_loaded_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
            .withColumn("_silver_batch_id",  F.lit(batch_id)))


def drop_null_keys(df: DataFrame, keys: list) -> DataFrame:
    """Loại row có bất kỳ natural key nào NULL (data lỗi, không thể MERGE)."""
    cond = None
    for k in keys:
        c = F.col(k).isNotNull()
        cond = c if cond is None else (cond & c)
    return df.filter(cond) if cond is not None else df
