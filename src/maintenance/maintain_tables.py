# src/maintenance/maintain_tables.py
"""
Bao tri Iceberg (Phase 4 Governance/Ops) qua Iceberg stored procedures cua Nessie catalog.

CHAY (Nessie HO TRO):
  1) rewrite_data_files   — compaction: gop small files -> bang doc nhanh, it file
  2) rewrite_manifests    — gop manifest -> planning query nhanh

KHONG CHAY (Nessie KHONG ho tro — gc.enabled=false):
  - expire_snapshots / remove_orphan_files
    Loi neu goi: ValidationException "Cannot expire snapshots: GC is disabled
    (deleting files may corrupt other tables)". Nessie quan version GC o CAP CATALOG
    (snapshot co the duoc nhieu branch/tag tham chieu) -> Iceberg per-table GC bi tat
    co y. Don file rac dung `nessie-gc` tool (cap catalog), KHONG dung CALL per-table.
    (Da verify thuc te Phase 4 — xem memory/phase4_done.md.)

Moi procedure boc try/except rieng -> 1 bang loi khong chan cac bang con lai; cuoi in SUMMARY.

Chay:
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 --packages "<SPARK_PACKAGES>" \
        /opt/spark/jobs/maintenance/maintain_tables.py
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

CATALOG = "nessie"

# Bang lon / hay doi -> dang bao tri. (bo audit_log: nho, append-only)
TABLES = [
    "bronze.oracle_bank_transactions",
    "bronze.pg_card_transactions",
    "silver.fct_bank_transactions",
    "silver.fct_card_transactions",
    "silver.dim_customer",
    "gold.mart_customer_360",
]

# Procedure Nessie HO TRO (compaction). expire_snapshots/remove_orphan_files bi gc.enabled=false.
PROCEDURES = [
    ("rewrite_data_files", "CALL {cat}.system.rewrite_data_files(table => '{t}')"),
    ("rewrite_manifests",  "CALL {cat}.system.rewrite_manifests(table => '{t}')"),
]


def run_proc(spark, label: str, sql: str, summary: dict):
    try:
        rows = spark.sql(sql).collect()
        detail = rows[0].asDict() if rows else {}
        print(f"  [OK]   {label}: {detail}")
        summary[label]["ok"] += 1
    except Exception as e:
        msg = str(e).splitlines()[0][:200]
        print(f"  [FAIL] {label}: {msg}")
        summary[label]["fail"] += 1
        summary[label]["err"] = msg


def main():
    spark = get_spark_session("maintenance")
    summary = {label: {"ok": 0, "fail": 0, "err": ""} for label, _ in PROCEDURES}

    for t in TABLES:
        print(f"\n== nessie.{t} ==")
        for label, tmpl in PROCEDURES:
            run_proc(spark, label, tmpl.format(cat=CATALOG, t=t), summary)

    print("\n" + "=" * 60)
    print("SUMMARY compaction:")
    for label, s in summary.items():
        print(f"  {label:20s} ok={s['ok']} fail={s['fail']}"
              + (f"  | {s['err']}" if s["err"] else ""))
    print("NOTE: expire_snapshots/remove_orphan_files KHONG chay duoc voi Nessie "
          "(gc.enabled=false) -> dung nessie-gc tool o cap catalog.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
