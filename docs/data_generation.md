# Cơ chế sinh dữ liệu — `scripts/generate_data.py`

> Tài liệu mô tả chi tiết cách hệ thống sinh dữ liệu mẫu cho Lakehouse Customer 360.
> Tất cả nằm trong **một file** `scripts/generate_data.py` với **hai mode**.

---

## 0. Tổng quan kiến trúc

Một file, hai chế độ, chọn bằng tham số dòng lệnh:

| Mode | Mục đích | Hành vi | Tần suất |
|---|---|---|---|
| `--mode bootstrap` | Tạo nền ban đầu | **TRUNCATE** sạch rồi sinh lại toàn bộ | Một lần |
| `--mode daily --date <ngày>` | Tăng trưởng theo ngày | **APPEND/UPDATE** (không xoá) | Mỗi ngày |

Lệnh:
```bash
# bootstrap (chạy host)
PG_HOST=localhost ORA_DSN=localhost:1521/XEPDB1 python generate_data.py --mode bootstrap
# daily (chạy trong container, Airflow truyền {{ ds }})
docker exec lakehouse-data-simulator python /app/scripts/generate_data.py --mode daily --date 2026-06-02
```

---

## 1. Cơ chế kết nối DB

Script đọc địa chỉ DB từ **biến môi trường**, để chạy được cả hai nơi:
```python
PG_HOST = os.getenv("PG_HOST", "postgres")              # mặc định = service name (trong Docker)
ORA_DSN = os.getenv("ORA_DSN", "oracle-xe:1521/XEPDB1")
```

| Gọi từ đâu | Postgres | Oracle |
|---|---|---|
| Container (Airflow gọi) — mặc định | `postgres:5432` | `oracle-xe:1521` |
| Host (set `PG_HOST=localhost`) | `localhost:5432` | `localhost:1521` |

Container nói chuyện bằng **tên service** (Docker DNS nội bộ); host nối qua **port-mapping**.
Cùng một database, hai cách gọi tuỳ vị trí đứng.

---

## 2. Mode BOOTSTRAP — sinh từ con số 0

`random.seed(42)` → chạy lại ra **y hệt** (tái lập được). Sinh theo thứ tự phụ thuộc
(KH → tài khoản → giao dịch), mốc `AS_OF = 2025-12-31`, lịch sử trải 18 tháng.

| Bảng | Số lượng | Quy tắc sinh |
|---|---|---|
| **customers** | 800 | ID `CIF000001`→`CIF000800`; tên ghép họ/đệm/tên VN; DOB 1955–2003; segment 70/22/8% (MASS/AFFLUENT/PREMIER); id_number unique |
| **bank_accounts** | 1.287 | Mỗi KH 1–3 tài khoản (xác suất 50/35/15%); tài khoản đầu luôn CURRENT; số dư `uniform(1M–50M) × hệ số segment` (PREMIER ×12) |
| **bank_transactions** | 25.774 | Mỗi tài khoản `random(0–40)` giao dịch rải 18 tháng; amount 50k–20M; DEPOSIT/WITHDRAWAL/TRANSFER/PAYMENT |
| **card_accounts** | 441 | ~55% KH có thẻ; credit/debit; hạn mức theo segment; dư nợ < 60% hạn mức |
| **card_transactions** | 6.547 | Mỗi thẻ `random(0–30)` giao dịch; có `merchant_category` (GROCERY/DINING/TRAVEL…) |
| **loans** | 238 | ~30% KH có vay; gốc 20M–500M; dư nợ 20–100% gốc; ~8% NPL (maturity quá hạn → DPD > 0) |

Ghi: Postgres `TRUNCATE … CASCADE`; Oracle `DELETE` theo thứ tự con→cha (vì khoá ngoại).
**Không đụng** `branches` / `products` (dữ liệu tham chiếu tĩnh).

> ⚠️ Đây là **ảnh chụp khởi tạo một lần** — các con số trên cố định, KHÔNG phải dữ liệu hằng ngày.

---

## 3. Mode DAILY — sinh theo cadence 3 tier

Khác bootstrap về bản chất: **đọc trạng thái hiện có từ DB** rồi *thêm* lên, không tạo lại.
`random.seed(int(yyyymmdd))` → mỗi ngày một seed riêng, deterministic theo ngày.

### Bước 1 — Đọc state hiện có
```sql
SELECT customer_id, customer_segment FROM customers          -- để đổi segment
SELECT account_id FROM card_accounts WHERE status='ACTIVE'   -- để gắn giao dịch thẻ
SELECT account_id, balance FROM bank_accounts ...            -- để gắn giao dịch + balance_after
SELECT loan_id FROM loans ...                                -- để chuyển NPL
```

### Bước 2 — Sinh theo 3 tier cadence

| Tier | Bảng | Hành vi mỗi ngày |
|---|---|---|
| **STATIC** | branches, products | **Không làm gì** (reference data đứng yên) |
| **SLOW** | customers, bank_accounts, card_accounts, loans | +0–3 KH mới (mỗi người 1 tài khoản CURRENT, 40% có thẻ); 1% KH đổi segment (**kích hoạt SCD2**); 1% khoản vay → NPL |
| **FAST** | bank_transactions, card_transactions | 30% tài khoản "hoạt động" → mỗi cái `random(0–4)` giao dịch bank / `random(0–3)` giao dịch thẻ |

### Ước lượng lượng data sinh mỗi ngày

| Bảng | Công thức | TB/ngày | Thực tế quan sát |
|---|---|---|---|
| bank_transactions | 30% × 1.287 × ~2 | **~770** | 769 / 770 |
| card_transactions | 30% × 441 × ~1.5 | **~180–200** | 196 / 172 |
| customers (mới) | random 0–3 | **~1–2** | 3 / 1 |
| customers (đổi segment) | 1% × 800 | **~8** | 9 / 12 |
| bank_accounts (mới) | = số KH mới | **~1–2** | 3 |
| card_accounts (mới) | KH mới × 40% | **~0–1** | 2 / 1 |
| loans (NPL) | 1% × 238 | **~2–3** | 2 / 3 |
| branches / products | tĩnh | **0** | 0 |

→ Mỗi ngày incremental chỉ xử lý **~1.000 dòng mới** (chủ yếu giao dịch), thay vì 33.000 dòng bootstrap.
Số **biến thiên** vì mỗi đại lượng random trong một khoảng / theo xác suất. Data **cộng dồn** theo thời gian.

---

## 4. Bốn cơ chế cốt lõi

### (a) Determinism — seed theo ngày
`seed(int(yyyymmdd))` → ngày 2026-06-02 luôn sinh ra đúng cùng một tập data, dù chạy bao nhiêu lần.
Nền tảng cho idempotency và khả năng backfill quá khứ.

### (b) CDC timestamp — điểm tinh tế nhất ⚠️
Hai loại cột thời gian, set khác nhau:

| Loại cột | Set bằng | Ý nghĩa |
|---|---|---|
| **Nghiệp vụ** (`txn_date`, `opened_date`) | **Ngày mô phỏng** (`--date`) | ngày giao dịch logic |
| **Kỹ thuật/CDC** (`created_at`, `updated_at`) | **Giờ THỰC** (`NOW()`/`SYSTIMESTAMP`) | thời điểm ingest thật |

**Tại sao tách?** Pipeline incremental lọc `created_at > last_run_time` (giờ thực lần trước). Nếu set
`created_at` = ngày mô phỏng (quá khứ so với mốc bootstrap) → **bị lọc bỏ hết**, incremental không bắt
được gì. Đây cũng đúng cách CDC thật vận hành.

### (c) Idempotency — chạy lại không hỏng (3 lớp)
1. **Guard chính**: đầu mỗi lần daily, `SELECT COUNT(*) FROM bank_transactions WHERE txn_date=<ngày>`.
   Nếu > 0 → **SKIP** (đã chạy ngày này). Lớp bảo vệ mạnh nhất khi Airflow retry.
2. **ON CONFLICT DO NOTHING** (Postgres) / **`executemany(batcherrors=True)`** (Oracle): insert trùng
   khoá chính bị bỏ qua, không lỗi.
3. **ID có tiền tố ngày**: cùng một ngày luôn sinh ra cùng tập ID.

### (d) Sơ đồ đặt ID — tránh trùng
| Loại | Bootstrap | Daily |
|---|---|---|
| Giao dịch | `BTXN00000001` (tuần tự) | `BTXN<yyyymmdd><seq>` |
| KH mới | `CIF000001`–`CIF000800` | `CIFN<yyyymmdd><k>` |
| Tài khoản/thẻ mới | `BACC…`/`CARD…` | `BACN…`/`CARN<yyyymmdd>…` |

→ Tiền tố ngày đảm bảo ID **duy nhất xuyên ngày** + **deterministic** (không cần đọc max ID), không
đụng dải bootstrap.

---

## 5. Tham số điều khiển — `config/simulation.yaml`

Toàn bộ "nhịp" sinh data tách khỏi code (YAML-driven), đổi không cần sửa Python:
```yaml
slow:
  new_customers_per_day: [0, 3]      # range KH mới/ngày
  new_card_prob: 0.4                 # % KH mới có thẻ
  customer_change_rate: 0.01         # % KH đổi segment/ngày (→ SCD2)
  npl_transition_rate: 0.01          # % khoản vay → NPL/ngày
fast:
  account_txn_participation: 0.30    # % tài khoản giao dịch/ngày
  bank_txns_per_account: [0, 4]      # range giao dịch bank/tài khoản
  card_txns_per_account: [0, 3]      # range giao dịch thẻ/tài khoản
```

---

## 6. Data chảy vào pipeline thế nào

```
┌─────────────────────────────────────────────────────────────────────────┐
│  generate_data.py  --mode daily --date 2026-06-02                        │
│  (chạy trong container lakehouse-data-simulator, Airflow điều phối)       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                 │  ghi row MỚI vào nguồn:
                                 │   • txn_date       = 2026-06-02 (nghiệp vụ)
                                 │   • created_at/updated_at = NOW() (CDC, giờ thực)
                                 ▼
         ┌──────────────────────────────────────────────┐
         │  Oracle (bank_*) + Postgres (customers, card_*)│
         └───────────────────────────┬──────────────────┘
                                      ▼
        ┌─────────────────────────────────────────────────────┐
        │  BRONZE incremental_load.py                          │
        │  lọc  created_at/updated_at > last_run_time          │
        │  → chỉ lấy ~1.000 row mới (KHÔNG nạp lại 33.000)     │
        │  → MERGE upsert vào nessie.bronze.*                  │
        └───────────────────────────┬─────────────────────────┘
                                     ▼
        ┌─────────────────────────────────────────────────────┐
        │  SILVER run_silver.py                                │
        │  • SCD2 dim_customer → tạo VERSION MỚI cho KH đổi    │
        │    segment (giữ lịch sử)                             │
        │  • SCD1 dims upsert; facts append giao dịch mới      │
        └───────────────────────────┬─────────────────────────┘
                                     ▼
        ┌─────────────────────────────────────────────────────┐
        │  GOLD run_gold.py                                    │
        │  as_of = max(txn_date) → TỰ nhảy sang ngày mới       │
        │  → mart_customer_360 thêm partition snapshot_date    │
        │    mới (snapshot cũ giữ nguyên → time series)        │
        └─────────────────────────────────────────────────────┘
```

→ Simulator chỉ "đẻ" data ở **nguồn**; phần còn lại của lakehouse tự cuốn theo nhờ cơ chế
**incremental + CDC**. Đó là toàn bộ vòng đời của một ngày dữ liệu.

---

## 7. Vận hành nhanh (cheat sheet)

```bash
# Bootstrap một lần (host)
PG_HOST=localhost ORA_DSN=localhost:1521/XEPDB1 venv/Scripts/python.exe scripts/generate_data.py --mode bootstrap

# Daily một ngày thủ công (container)
docker exec lakehouse-data-simulator python /app/scripts/generate_data.py --mode daily --date 2026-06-02

# Tự động hằng ngày: bật DAG (đồng thời pause bronze_ingestion/silver_transform/gold_mart)
docker exec lakehouse-airflow-scheduler airflow dags unpause daily_simulation
```

> Đổi nhịp tăng trưởng: chỉ sửa `config/simulation.yaml`, không sửa code.
