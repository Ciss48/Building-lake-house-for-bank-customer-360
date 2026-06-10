# Daily Data Simulator Done — sinh data tự động theo ngày (chen giữa Phase 3 và 4)

## Ngày hoàn thành: 2026-06-09

> Hạng mục riêng (`tasks/task_5_daily_simulation.md`), không đổi số phase trong plan_overall.

---

## Mục tiêu
Trình mô phỏng nguồn chạy mỗi ngày qua Airflow: sinh data mới theo **cadence 3 tier** vào
Oracle/Postgres → đẩy qua nhánh **incremental** (lần đầu dùng thật) → lakehouse "sống" tự cập
nhật. Lần đầu chứng minh: incremental load + SCD2 versioning từ thay đổi thật + tăng trưởng KH.

---

## File tạo/sửa
| File | Vai trò |
|---|---|
| `config/simulation.yaml` | Tham số cadence: new_customers_per_day, customer_change_rate, npl_transition_rate, account_txn_participation, txns_per_account |
| `scripts/generate_data.py` | **REFACTOR**: `--mode {bootstrap,daily}` + `--date` + host từ ENV (`PG_HOST`/`ORA_DSN`); tách `run_bootstrap()` (logic cũ) + thêm `run_daily()` |
| `docker/simulator/Dockerfile` | `python:3.12-slim` + psycopg2-binary + oracledb + pyyaml → image `lakehouse-simulator` |
| `docker-compose.yml` | +service `data-simulator` (sleep infinity, mount scripts+config, env_file .env) |
| `dags/dag_daily_simulation.py` | DAG khép kín: simulate → bronze incremental → silver → gold → verify |

**Tái dùng:** `incremental_load.py` (`run_incremental`), `run_silver.py`, `run_gold.py`,
`verify_gold.py`; helper `gen_*`/`rand_date`/`SEG_MULT` trong generate_data; pattern
`spark_job` BashOperator docker exec từ `dag_bronze_ingestion.py`.

---

## Cadence 3 tier
| Tier | Bảng | Daily |
|---|---|---|
| STATIC | branches, products | KHÔNG đụng (đã `full_snapshot` mode) |
| SLOW | customers, bank_accounts, card_accounts, loans | +0-3 KH mới/ngày; ~1% đổi segment (SCD2); ~1% loan→NPL |
| FAST | bank_transactions, card_transactions | ~30% account giao dịch → vài trăm dòng/ngày |

---

## ⚠️ QUAN TRỌNG — điểm khác draft (đã fix khi làm thật)

**CDC timestamp = giờ THỰC (`NOW()`/`SYSTIMESTAMP`), KHÔNG phải ngày mô phỏng `--date`.**
- Lý do: `incremental_load.run_incremental` lọc `incremental_col > last_run_time` (last_run =
  giờ thực UTC của lần bronze chạy trước). Nếu set `created_at`/`updated_at` = ngày mô phỏng
  (vd 2026-06-02, là QUÁ KHỨ so với last_run ~2026-06-08 của bootstrap) → **bị lọc bỏ hết**,
  incremental không bắt được gì.
- Đúng = cột **CDC** (`created_at`/`updated_at`) = `NOW()`/`SYSTIMESTAMP` (giờ ingest thực);
  chỉ cột **nghiệp vụ** (`txn_date`/`txn_datetime`/`opened_date`) = `--date` (= `{{ ds }}`).
  Đây cũng đúng cách CDC thật hoạt động.
- `run_gold` `as_of=auto=max(txn_date)` → tự bám `--date` (txn_date) → KHÔNG cần sửa gold.

**Idempotency = guard "ngày đã có data → skip"** (`SELECT COUNT(*) FROM bank_transactions
WHERE txn_date=:date`; >0 → return). Cộng thêm txn_id tiền tố ngày + Postgres
`ON CONFLICT DO NOTHING` / Oracle `executemany(batcherrors=True)`. → DAG retry cùng ngày an toàn.

**ID entity mới có tiền tố ngày** (KH `CIFN<yyyymmdd><k>`, account `BACN...`, card `CARN...`)
→ deterministic + không đụng dải bootstrap (CIF000xxx) + không cần đọc max.

---

## Kết quả test end-to-end (thật)
```
Build image lakehouse-simulator + up container data-simulator              ✅
generate_data --help trong container (argparse + drivers load)             ✅
Daily 2026-06-02: bank_txns=769 card_txns=196 new_cust=3 new_card=2
   seg_changes=9 npl_changes=2                                             ✅
Idempotency: re-run cùng ngày -> "da ton tai ... SKIP"                      ✅
Bronze incremental: upsert ĐÚNG delta — customers 12 (3 new+9 changed),
   bank_txn 769, card_txn 196, bank_accounts 3, card_accounts 2, loans 2;
   branches/products full 2/3 (static)                                     ✅
Silver: SCD2 dim_customer 12 version moi; dim_account 1287->1290;
   dim_card 441->443; fct_bank 25774->26543; fct_card 6547->6743           ✅
Gold: as_of TỰ ĐỘNG = 2026-06-02; mart 803 rows; 2 partition snapshot_date
   (2025-12-31=800 giữ nguyên + 2026-06-02=803)                           ✅
SCD2 verify: 9 KH có 2 version (vd CIF000001 v1 MASS closed -> v2 AFFLUENT
   current); 3 KH mới CIFN20260602* xuất hiện trong snapshot 06-02         ✅
Recency: very_recent(<=1d) 117 -> 344 ở snapshot 06-02 (KH giao dịch ngày đó)✅
DAG daily_simulation: parse OK, không import error; task `simulate` test
   (execution_date 2026-06-03) SUCCESS — docker exec data-simulator chạy   ✅
```

---

## Vận hành (QUAN TRỌNG khi bật daily thật)
- DAG `daily_simulation` để **PAUSED** sau khi làm (tránh catchup 8 ngày storm). Khi muốn chạy
  thật: `airflow dags unpause daily_simulation`. catchup=True + start_date=2026-06-01 →
  backfill tuần tự tới nay (max_active_runs=1). Muốn ít run hơn → dời start_date gần lại.
- **PHẢI pause** `bronze_ingestion` / `silver_transform` / `gold_mart` khi bật daily_simulation
  (chung `bronze.audit_log` → `last_run_time`; chạy đôi làm lệch cửa sổ incremental).
- Chạy host (bootstrap): `PG_HOST=localhost ORA_DSN=localhost:1521/XEPDB1 venv/Scripts/python.exe scripts/generate_data.py --mode bootstrap`.
- Chạy daily 1 ngày tay: `docker exec lakehouse-data-simulator python /app/scripts/generate_data.py --mode daily --date YYYY-MM-DD`.

## Trạng thái hiện tại của data
- Source: đã có data tới 2026-06-02 (ingested) + 2026-06-03 (sinh bởi `tasks test`, CHƯA
  ingest — sẽ vào ở lần bronze incremental kế tiếp).
- Lakehouse mart: 2 snapshot 2025-12-31 + 2026-06-02.

## Tiếp theo
Phase 4 — Governance & Ops (PII masking, Iceberg maintenance + dọn orphan, Time Travel,
Schema Evolution, DAG maintenance).
