# src/common/audit_logger.py
"""
Ghi trạng thái mỗi lần chạy vào audit_log.

Mặc định trỏ nessie.bronze.audit_log (Bronze giữ nguyên `AuditLogger(spark)`).
Silver khởi tạo `AuditLogger(spark, "nessie.silver.audit_log")` để ghi audit riêng cho
layer Silver. Chữ ký log() / get_last_run_time() KHÔNG đổi nên Bronze không phải sửa.
"""
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)


# Schema cố định để createDataFrame không suy luận sai kiểu (vd. None → void).
_AUDIT_SCHEMA = StructType([
    StructField("batch_id",         StringType(),    True),
    StructField("table_name",       StringType(),    True),
    StructField("source_system",    StringType(),    True),
    StructField("run_mode",         StringType(),    True),
    StructField("status",           StringType(),    True),
    StructField("rows_ingested",    LongType(),      True),
    StructField("last_run_time",    TimestampType(), True),
    StructField("current_run_time", TimestampType(), True),
    StructField("error_message",    StringType(),    True),
    StructField("created_at",       TimestampType(), True),
])

_DEFAULT_AUDIT_TABLE = "nessie.bronze.audit_log"


class AuditLogger:
    def __init__(self, spark: SparkSession, audit_table: str = _DEFAULT_AUDIT_TABLE):
        self.spark = spark
        self.audit_table = audit_table

    def log(self, batch_id: str, table_name: str, source_system: str,
            run_mode: str, status: str, rows_ingested: int,
            last_run_time, current_run_time,
            error_message: str = None):

        row = (
            batch_id,
            table_name,
            source_system,
            run_mode,
            status,
            int(rows_ingested) if rows_ingested is not None else 0,
            last_run_time,
            current_run_time,
            error_message,
            datetime.now(timezone.utc),
        )
        df = self.spark.createDataFrame([row], schema=_AUDIT_SCHEMA)
        df.writeTo(self.audit_table).append()

    def get_last_run_time(self, table_name: str):
        """
        Thời điểm chạy thành công gần nhất của 1 bảng.
        Incremental load dùng hàm này để biết filter từ đâu.
        Trả None nếu chưa có lịch sử (lần đầu) → incremental tự fallback sang full.
        """
        try:
            result = (
                self.spark.table(self.audit_table)
                .filter(F.col("table_name") == table_name)
                .filter(F.col("status") == "SUCCESS")
                .orderBy(F.col("current_run_time").desc())
                .limit(1)
                .select("current_run_time")
                .collect()
            )
            if result:
                return result[0]["current_run_time"]
            return None
        except Exception:
            return None
