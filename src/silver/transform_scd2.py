# src/silver/transform_scd2.py
"""
SCD Type 2 cho dim_customer.

Iceberg MERGE không thể vừa ĐÓNG version cũ vừa MỞ version mới trong 1 lệnh, nên dùng
cơ chế 2-statement:
  - Statement A: MERGE đóng các current row có tracked-attr thay đổi (is_current=false,
    effective_to=now).
  - Statement B: append version mới (NEW + CHANGED) với is_current=true, version=old+1.

Idempotent: chạy lại mà nguồn không đổi -> row_hash trùng -> 0 version mới.
"""
import sys
from datetime import datetime, timezone
sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common.cleansing import clean_strings, upper_cols, dedup_latest, drop_null_keys

_UPPER = ["customer_segment", "kyc_status"]


def transform_scd2(spark, table_name: str, cfg: dict, batch_id: str) -> int:
    source  = f"nessie.bronze.{cfg['source']}"
    target  = f"nessie.silver.{table_name}"
    nk      = cfg["natural_key"]
    tracked = cfg["tracked_columns"]
    attrs   = cfg["attributes"]
    now     = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # 1) Snapshot mới nhất mỗi natural key + row_hash trên tracked cols
    src = spark.table(source)
    src = clean_strings(src)
    src = upper_cols(src, _UPPER)
    src = drop_null_keys(src, [nk])
    src = dedup_latest(src, [nk], "_ingested_at")
    src = src.withColumn(
        "row_hash",
        F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked])),
    )
    src.createOrReplaceTempView("scd2_src")

    # 2) current version trong target
    cur = spark.table(target).filter("is_current = true").select(nk, "row_hash", "version")
    cur.createOrReplaceTempView("scd2_cur")

    # 3) Phân loại NEW (chưa có) + CHANGED (hash khác)
    changes = spark.sql(f"""
        SELECT s.*, c.version AS _old_version
        FROM scd2_src s LEFT JOIN scd2_cur c ON s.{nk} = c.{nk}
        WHERE c.{nk} IS NULL OR c.row_hash <> s.row_hash
    """)
    # localCheckpoint(eager=True) cắt lineage -> 'changes' (gồm _old_version) được
    # materialize độc lập với bảng target. BẮT BUỘC: nếu chỉ persist(), Statement A
    # (MERGE) sẽ invalidate cache theo target và Statement B tính lại 'changes' trên
    # current rows ĐÃ bị đóng -> _old_version NULL -> version sai (luôn = 1).
    changes = changes.localCheckpoint(eager=True)
    n_changes = changes.count()
    if n_changes == 0:
        print(f"  i SCD2 {table_name}: khong co thay doi")
        return 0
    changes.createOrReplaceTempView("scd2_changes")

    # 4) Statement A — đóng version cũ của những key CHANGED (có _old_version)
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING (SELECT {nk}, row_hash FROM scd2_changes WHERE _old_version IS NOT NULL) AS s
        ON t.{nk} = s.{nk} AND t.is_current = true
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
            t.is_current   = false,
            t.effective_to = TIMESTAMP '{now_str}'
    """)

    # 5) Statement B — append version mới (NEW + CHANGED), version = old+1 (NEW -> 1)
    select_attrs = ", ".join(attrs + tracked)
    new_versions = spark.sql(f"""
        SELECT
            md5(concat({nk}, '|', '{now_str}'))      AS customer_sk,
            {nk}, {select_attrs}, row_hash,
            TIMESTAMP '{now_str}'                    AS effective_from,
            CAST(NULL AS TIMESTAMP)                  AS effective_to,
            true                                     AS is_current,
            COALESCE(_old_version, 0) + 1            AS version,
            TIMESTAMP '{now_str}'                    AS _silver_loaded_at,
            '{batch_id}'                             AS _silver_batch_id
        FROM scd2_changes
    """)
    # align cột theo đúng thứ tự bảng đích rồi append (tránh lệch cột)
    target_cols = [f.name for f in spark.table(target).schema.fields]
    new_versions.select(*target_cols).writeTo(target).append()

    print(f"  OK SCD2 {table_name}: {n_changes} version moi")
    return n_changes
