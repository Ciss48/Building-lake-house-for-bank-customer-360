#!/usr/bin/env python
# dashboard/export_mart.py
"""
Export gold mart + aggregate xu huong thang -> public/data/*.parquet (+ meta.json).

Vi sao snapshot (khong query live):
  - Trino chay tren localhost:8088, Vercel (cloud) KHONG voi toi localhost duoc.
  - Mart ~800 dong/snapshot, la snapshot theo ngay -> export file tinh la toi uu nhat.

Output:
  - mart_customer_360.parquet : TAT CA snapshot (UI co bo loc ngay snapshot)
  - daily_trend.parquet       : NGAY x nguon(BANK/CARD) x channel x segment (count, amount)
                                -> UI cuon len thang bang date_trunc trong DuckDB-Wasm
  - monthly_active.parquet    : thang x segment (KH active distinct)
  - daily_active.parquet      : ngay  x segment (KH active distinct)
  - meta.json                 : danh sach snapshot, khoang thang/ngay, row counts

Cach chay (host, trong venv da pip install -r requirements.txt):
    python dashboard/export_mart.py
Refresh du lieu: chay lai script nay sau khi pipeline Gold tao snapshot moi, roi
redeploy / git push -> Vercel tu build lai.
"""
import json
import sys
from datetime import date, datetime
from pathlib import Path

OUT_DIR = Path(__file__).parent / "public" / "data"
PARQUET = OUT_DIR / "mart_customer_360.parquet"
DAILY_TREND_PARQUET = OUT_DIR / "daily_trend.parquet"
MONTHLY_ACTIVE_PARQUET = OUT_DIR / "monthly_active.parquet"
DAILY_ACTIVE_PARQUET = OUT_DIR / "daily_active.parquet"
LEGACY_MONTHLY_TREND = OUT_DIR / "monthly_trend.parquet"  # cu — xoa neu con
META = OUT_DIR / "meta.json"

# Chi export cot nghiep vu (bo cot _gold_* meta noi bo)
COLUMNS = [
    "customer_id", "full_name", "age", "gender", "customer_segment", "kyc_status",
    "city", "province",
    "total_product_count", "has_savings", "has_current", "has_credit_card", "has_loan",
    "total_deposit_balance", "total_loan_outstanding", "credit_utilization_rate", "net_asset_value",
    "txn_count_12m", "txn_amount_12m", "avg_monthly_spend", "top_spend_category", "digital_txn_ratio",
    "last_txn_date", "active_months_12m",
    "recency_days", "frequency_12m", "monetary_12m",
    "r_score", "f_score", "m_score", "rfm_score", "rfm_segment",
    "has_npl_loan", "days_past_due",
    "cross_sell_loan_flag", "cross_sell_card_flag", "cross_sell_invest_flag",
    "tenure_days", "snapshot_date",
]

# Export TAT CA snapshot — UI co bo loc "Ngay snapshot" (mac dinh = moi nhat)
QUERY = f"""
SELECT {", ".join(COLUMNS)}
FROM iceberg.gold.mart_customer_360
"""

# Xu huong giao dich theo NGAY tu silver facts (~700 ngay, 2 nguon, channel, segment).
# Join account -> customer (SCD2 current) de bo loc segment ap dung duoc cho trend.
# UI cuon len thang bang date_trunc('month', day) ngay trong DuckDB-Wasm.
DAILY_TREND_QUERY = """
WITH txn AS (
    SELECT f.txn_date AS day, 'BANK' AS source,
           f.channel, c.customer_segment, f.amount
    FROM iceberg.silver.fct_bank_transactions f
    JOIN iceberg.silver.dim_account a ON f.account_id = a.account_id
    JOIN iceberg.silver.dim_customer c
         ON a.customer_id = c.customer_id AND c.is_current = true
    WHERE f.status = 'COMPLETED'
    UNION ALL
    SELECT f.txn_date, 'CARD',
           f.channel, c.customer_segment, f.amount
    FROM iceberg.silver.fct_card_transactions f
    JOIN iceberg.silver.dim_card_account a ON f.account_id = a.account_id
    JOIN iceberg.silver.dim_customer c
         ON a.customer_id = c.customer_id AND c.is_current = true
    WHERE f.status = 'COMPLETED'
)
SELECT day, source, channel, customer_segment,
       count(*)    AS txn_count,
       sum(amount) AS txn_amount
FROM txn GROUP BY 1, 2, 3, 4 ORDER BY 1
"""

# KH active distinct — distinct KHONG cong duoc tu grain nho len grain lon,
# nen phai aggregate rieng o moi do phan giai (thang va ngay).
def _active_query(grain: str) -> str:
    bucket = "date_trunc('month', f.txn_date)" if grain == "month" else "f.txn_date"
    return f"""
WITH txn AS (
    SELECT {bucket} AS bucket, c.customer_segment, a.customer_id
    FROM iceberg.silver.fct_bank_transactions f
    JOIN iceberg.silver.dim_account a ON f.account_id = a.account_id
    JOIN iceberg.silver.dim_customer c
         ON a.customer_id = c.customer_id AND c.is_current = true
    WHERE f.status = 'COMPLETED'
    UNION ALL
    SELECT {bucket}, c.customer_segment, a.customer_id
    FROM iceberg.silver.fct_card_transactions f
    JOIN iceberg.silver.dim_card_account a ON f.account_id = a.account_id
    JOIN iceberg.silver.dim_customer c
         ON a.customer_id = c.customer_id AND c.is_current = true
    WHERE f.status = 'COMPLETED'
)
SELECT bucket AS {grain}, customer_segment,
       count(DISTINCT customer_id) AS active_customers
FROM txn GROUP BY 1, 2 ORDER BY 1
"""

MONTHLY_ACTIVE_QUERY = _active_query("month")
DAILY_ACTIVE_QUERY = _active_query("day")


def _connect():
    try:
        import trino  # type: ignore
    except ImportError:
        sys.exit(
            "Thieu package 'trino'. Cai bang:\n"
            "    pip install trino==0.329.0\n"
            "(da them vao requirements.txt)"
        )
    return trino.dbapi.connect(
        host="localhost", port=8088, user="dashboard",
        catalog="iceberg", schema="gold",
    )


def fetch_dataframe(conn, sql):
    """Query Trino qua DBAPI client (giu dung kieu du lieu)."""
    import pandas as pd

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=cols)


def _date_str(v):
    return v.isoformat() if isinstance(v, (date, datetime)) else str(v)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = _connect()

    print("-> Query mart_customer_360 (tat ca snapshot)...")
    df = fetch_dataframe(conn, QUERY)
    if df.empty:
        sys.exit("Mart rong — kiem tra pipeline Gold da chay chua.")

    # Ep kieu cho gon (booleans) — pyarrow se suy kieu dung cho DuckDB-Wasm
    for c in ["has_savings", "has_current", "has_credit_card", "has_loan",
              "has_npl_loan", "cross_sell_loan_flag", "cross_sell_card_flag",
              "cross_sell_invest_flag"]:
        if c in df.columns:
            df[c] = df[c].astype("boolean")
    df.to_parquet(PARQUET, engine="pyarrow", index=False, compression="zstd")

    print("-> Query xu huong giao dich theo NGAY (silver facts)...")
    trend = fetch_dataframe(conn, DAILY_TREND_QUERY)
    trend.to_parquet(DAILY_TREND_PARQUET, engine="pyarrow", index=False, compression="zstd")

    print("-> Query KH active theo thang + theo ngay...")
    m_active = fetch_dataframe(conn, MONTHLY_ACTIVE_QUERY)
    m_active.to_parquet(MONTHLY_ACTIVE_PARQUET, engine="pyarrow", index=False, compression="zstd")
    d_active = fetch_dataframe(conn, DAILY_ACTIVE_QUERY)
    d_active.to_parquet(DAILY_ACTIVE_PARQUET, engine="pyarrow", index=False, compression="zstd")
    conn.close()

    # Don file cu (truoc day la monthly_trend)
    if LEGACY_MONTHLY_TREND.exists():
        LEGACY_MONTHLY_TREND.unlink()

    snapshots = sorted({_date_str(s) for s in df["snapshot_date"]})
    days = sorted({_date_str(d) for d in trend["day"]})
    months = sorted({d[:7] for d in days})
    meta = {
        "snapshot_date": snapshots[-1],
        "snapshots": snapshots,
        "months": months,
        "days": [days[0], days[-1]],
        "row_count": int((df["snapshot_date"].apply(_date_str) == snapshots[-1]).sum()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "columns": list(df.columns),
    }
    META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    for p, d in [(PARQUET, df), (DAILY_TREND_PARQUET, trend),
                 (MONTHLY_ACTIVE_PARQUET, m_active), (DAILY_ACTIVE_PARQUET, d_active)]:
        print(f"[OK] {p.name}  ({len(d)} dong, {p.stat().st_size / 1024:.1f} KB)")
    print(f"[OK] meta.json  (snapshots={len(snapshots)}, months={months[0]}..{months[-1]}, "
          f"days={days[0]}..{days[-1]})")
    print("\nDeploy: xem dashboard/README.md")


if __name__ == "__main__":
    main()
