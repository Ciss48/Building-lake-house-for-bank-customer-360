# Phase 3 Done — Gold Layer (mart_customer_360 + RFM + Cross-sell)

## Ngày hoàn thành: 2026-06-08

> Đánh số theo `plan_overall.md`: Foundation=0, Bronze=1, Silver=2, **Gold=3**, Governance=4.

---

## Mục tiêu phase này
Đọc từ Silver (`nessie.silver.dim_* + fct_*`) → dựng Gold `nessie.gold.*`: 2 bảng aggregate
trung gian + `mart_customer_360` (1 row/KH, 38 cột KPI) gồm RFM segment + cross-sell flags.
YAML-driven, orchestrate bằng Airflow DAG `gold_mart`, query qua Trino.

3 quyết định scope (user chốt) — đều làm: **(1)** Gold = mart + agg tables · **(2)** sinh
data phong phú (~800 KH) · **(3)** Nessie bền (ROCKSDB) ngay đầu phase. 3 việc ăn khớp:
đổi Nessie store làm catalog rỗng → tận dụng regenerate data + chạy lại full pipeline 1 lượt.

---

## File code tạo ra
| File | Vai trò |
|---|---|
| `scripts/generate_data.py` | Sinh ~800 KH + giao dịch 18 tháng vào Postgres + Oracle (chạy HOST qua venv). seed=42, AS_OF=2025-12-31 |
| `config/gold_tables.yaml` | Bản đồ Gold: as_of, lookback_months, rule cross-sell (loan/card/invest), digital_channels |
| `src/gold/create_gold_tables.py` | Tạo namespace nessie.gold + 4 bảng (2 agg + mart + audit_log) |
| `src/gold/build_agg_holdings.py` | agg_customer_holdings: product holding + balance + NPL + first_open_date |
| `src/gold/build_agg_txn_12m.py` | agg_customer_txn_12m: txn 12m + recency/frequency/monetary + top category |
| `src/gold/build_mart_360.py` | mart: join + age/tenure + RFM (NTILE 5) + cross-sell (rule từ YAML) |
| `src/gold/run_gold.py` | Orchestrator: resolve as_of (auto=max txn_date), chạy tuần tự agg→agg→mart, ghi gold.audit_log |
| `src/gold/verify_gold.py` | Count + RFM distribution + cross-sell counts + segment×net_asset + 1 KH chi tiết |
| `dags/dag_gold_mart.py` | DAG `gold_mart`: gold_build → quality_check (4 AM) |
| `sql/gold/vw_marketing_rfm.sql`, `vw_sales_crosssell.sql` | **Saved query** (KHÔNG phải view — xem điểm #2) |
| `docker-compose.yml` | Nessie IN_MEMORY → ROCKSDB + named volume `nessie-data` |

**Tái dùng (KHÔNG viết lại):** `spark_session.py` (`get_spark_session`, `align_to_table`,
`SPARK_PACKAGES`), `AuditLogger(spark, "nessie.gold.audit_log")` (param hoá từ Phase 2),
`create_bronze_tables`+`full_snapshot`+`create_silver_tables`+`run_silver` chạy lại nguyên.

---

## Mô hình `nessie.gold.*`
| Bảng | Grain | Ghi | Rows |
|---|---|---|---|
| `agg_customer_holdings` | 1/KH | overwritePartitions | 800 |
| `agg_customer_txn_12m` | 1/KH | overwritePartitions | 798 (2 KH không có txn trong cửa sổ) |
| `mart_customer_360` | 1/KH | overwritePartitions, partition `snapshot_date` | 800 |
| `audit_log` | 1/build | append, part. days(created_at) | — |

**As-of date** = max(txn_date) 2 fact = `2025-12-31` (KHÔNG dùng current_date → recency có
nghĩa). age/tenure/recency tính theo mốc này.

---

## Kết quả test end-to-end (đã chạy thật)
```
Nessie ROCKSDB: docker restart lakehouse-nessie → query gold = 800 (catalog SỐNG SÓT)  ✅
generate_data:  customers 800, bank_accts 1287, bank_txns 25774, cards 441,
                card_txns 6547, loans 238                                              ✅
Bronze reload:  8 bảng full_snapshot khớp source                                       ✅
Silver reload:  dim_customer 800 (SCD2 v1), dim_account 1287, dim_card 441, dim_loan
                238, fct_bank 25774, fct_card 6547                                      ✅
Gold build:     agg_holdings 800, agg_txn 798, mart 800 (=số KH)                        ✅
RFM 7 segment:  Hibernating 184, Loyal 166, Champions 154, Potential_Loyal 124,
                Need_Attention 87, At_Risk 60, New 25 (recency/monetary track đúng:
                Champions recency~2d monetary cao nhất; Hibernating recency~53d)        ✅
Cross-sell:     loan 274, card 400, invest 87 | NPL 19 (~8% của 238 loans)             ✅
Segment×asset:  PREMIER net 389M > AFFLUENT 93M > MASS -18M (đòn bẩy/vay kéo âm)        ✅
Idempotency:    rerun run_gold → mart 800 total / 800 distinct (KHÔNG nhân đôi)         ✅
Trino:          query iceberg.gold.mart_customer_360 + 2 saved query OK                 ✅
Airflow:        DAG gold_mart trigger thật → gold_build + quality_check đều SUCCESS     ✅
```

---

## ⚠️ QUAN TRỌNG — 2 điểm khác draft (đã fix khi làm thật)

1. **Named volume `nessie-data` root-owned → Nessie (uid 185) không ghi được RocksDB.**
   Lỗi: `org.rocksdb.RocksDBException: While open a file for appending: /nessie/data/LOG:
   Permission denied`. Docker tạo named volume rỗng với owner root:root, nhưng image Nessie
   chạy uid 185.
   → **Fix:** chown volume cho 185 rồi restart:
   ```
   docker run --rm -v buildinglakehouseforcustomerbank360_nessie-data:/data alpine \
     chown -R 185:185 /data
   docker compose restart nessie
   ```
   Log xác nhận: `Using ROCKSDB version store (database path: /nessie/data)`. (Tên volume có
   prefix project = thư mục `buildinglakehouseforcustomerbank360`.)

2. **Trino + Iceberg Nessie catalog KHÔNG hỗ trợ `CREATE VIEW`.**
   Lỗi: `createView is not supported for Iceberg Nessie catalogs`.
   → **Fix:** 2 file `sql/gold/vw_*.sql` đổi từ `CREATE OR REPLACE VIEW` sang **saved query**
   (filtered SELECT chạy trực tiếp trên `mart_customer_360`). `mart_customer_360` chính là
   bản materialization rồi; marketing/sales chỉ là lát cắt WHERE. (Nếu cần view vật lý thật:
   build thành Iceberg table riêng bằng Spark — chưa làm, không cần thiết.)

**Lưu ý chung (giữ từ Phase 2):** mọi spark-submit chạy qua **PowerShell** (KHÔNG Git Bash);
exit 255 / `NativeCommandError` (Ivy stderr) lúc shutdown là lành tính — kiểm tra bằng
output `OK ...` / audit_log, KHÔNG dựa exit code.

---

## Gotcha Nessie IN_MEMORY (Phase 2) — ĐÃ GIẢI QUYẾT
Phase 2 ghi: Nessie IN_MEMORY mất catalog khi restart. Phase 3 đổi sang ROCKSDB + volume
`nessie-data` → test `docker restart lakehouse-nessie` rồi query gold vẫn 800 rows. Từ giờ
catalog bền qua restart máy. (Orphan parquet bronze/silver cũ trên MinIO chưa dọn → để Phase 4.)

---

## Thiết kế KPI đáng nhớ
- **RFM**: NTILE(5) — R `ORDER BY recency DESC` (ít ngày=tile cao), F/M `ASC` (lớn=tile cao).
  KH không txn → recency NULL→COALESCE 999999→R thấp, F/M=0. Segment map theo (R,F).
- **net_asset_value** = deposit − loan_outstanding − card_outstanding (MASS thường âm vì vay).
- **top_spend_category**: chỉ từ card txn (bank txn không có merchant_category) → KH chỉ có
  bank account → top_spend_category NULL (đúng, không phải bug).
- **days_past_due**: PROXY (source không có cột DPD) = GREATEST(DATEDIFF(as_of, maturity),0)
  khi npl_status≠NORMAL. generate_data set NPL loan có maturity < as_of để DPD>0.
- **has_savings** = có account_type TERM_DEPOSIT; **has_current** = CURRENT.

---

## Lệnh chạy (cheat sheet) — PowerShell, `$PKGS` như Phase 1/2
```powershell
# Sinh data (HOST qua venv)
venv/Scripts/python.exe scripts/generate_data.py
# Reload Bronze + Silver (sau khi Nessie đổi store / data mới)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/create_bronze_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/full_snapshot.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/create_silver_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
# Gold
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/create_gold_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/run_gold.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/verify_gold.py CIF000003
```
Airflow: `docker exec lakehouse-airflow-scheduler airflow dags unpause gold_mart` rồi
`... airflow dags trigger gold_mart`. (Unpause có thể tạo 1 scheduled run catch-up chạy
song song manual — chúng serialize ở Spark standalone, không xung đột commit.)

Trino: `docker exec lakehouse-trino trino --execute "SELECT ... FROM iceberg.gold.mart_customer_360"`

---

## Phase tiếp theo — Phase 4 (Governance & Ops)
`tasks/task_5_governance_ops.md`: PII masking (email/phone/full_name/id_number ở Silver/Gold),
Iceberg maintenance (compaction, expire_snapshots, **dọn orphan parquet bronze/silver cũ** từ
lần đổi Nessie store), Time Travel demo, Schema Evolution demo, DAG maintenance định kỳ.
