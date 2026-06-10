# Task: Gold Layer — Phase 3 (mart_customer_360 + RFM + Cross-sell)

## Mục tiêu
Đọc từ Silver (`nessie.silver.dim_*` + `fct_*`) → xây **Gold layer** trên `nessie.gold.*`:
một vài **bảng aggregate trung gian** + bảng đích **`mart_customer_360`** (1 row/khách hàng,
25+ KPI), gồm **RFM segment** và **cross-sell flags**. YAML-driven, orchestrate bằng Airflow
DAG `gold_mart`, query qua **Trino** (đã dựng ở Phase 2).

## Trạng thái
- [x] **ĐÃ XONG (2026-06-08)** — implement + test end-to-end thật. Chi tiết kết quả &
  2 điểm khác draft xem `memory/phase3_done.md`.
  - Khác draft khi làm thật: (1) named volume `nessie-data` root-owned → Nessie (uid 185)
    không ghi được RocksDB (`Permission denied /nessie/data/LOG`) → phải `chown -R 185:185`
    volume rồi restart; (2) Trino + Iceberg Nessie catalog **KHÔNG hỗ trợ `CREATE VIEW`**
    (`createView is not supported for Iceberg Nessie catalogs`) → 2 file `sql/gold/vw_*.sql`
    đổi từ CREATE VIEW sang **saved query** (filtered SELECT chạy trực tiếp trên mart).
  - Kết quả: mart 800 rows = 800 KH; RFM trải 7 segment; cross-sell loan/card/invest =
    274/400/87; NPL 19; idempotency 800/800; Nessie restart → catalog còn nguyên; DAG
    `gold_mart` 2 task SUCCESS.

> **Đánh số phase** (theo `plan_overall.md`): Foundation=0, Bronze=1, Silver=2, **Gold=3**,
> Governance=4. File done sẽ là `memory/phase3_done.md`.

---

## Quyết định scope (đã chốt với user — 2026-06-08)
1. **Gold = mart + bảng aggregate** (không chỉ 1 bảng wide). Tách `agg_customer_holdings`
   (product holding) + `agg_customer_txn_12m` (transaction 12 tháng) làm "nguyên liệu" →
   `mart_customer_360` chỉ join lại + tính cross-sell. Đúng tinh thần "star schema Gold".
2. **Sinh thêm dữ liệu** (`scripts/generate_data.py`): ~800 khách + giao dịch trải nhiều
   tháng → RFM segment & 25 KPI có ý nghĩa thật (portfolio value). Seed cũ 5-6 KH quá ít,
   RFM degenerate (mọi KH cùng 1 bucket).
3. **Làm Nessie bền NGAY đầu Phase 3** (ROCKSDB + named volume) → catalog sống sót qua
   restart (fix gotcha `phase2_done.md`). Vì đổi store làm catalog rỗng nên gộp luôn:
   đổi store → regenerate data → chạy lại full Bronze→Silver→Gold.

---

## Thứ tự thực thi tổng (QUAN TRỌNG — có dependency)

```
Bước 0  Nessie bền (ROCKSDB + volume)   ← catalog sẽ RỖNG sau bước này
   ↓
Bước 1  generate_data.py → nạp ~800 KH + txn vào Postgres + Oracle (host, qua venv)
   ↓
Bước 2  Chạy lại Bronze: create_bronze_tables → full_snapshot   (tái dùng Phase 1)
   ↓
Bước 3  Chạy lại Silver: create_silver_tables → run_silver       (tái dùng Phase 2)
   ↓
Bước 4..10  Xây Gold (code mới của phase này)
   ↓
Bước 11  Test end-to-end + Trino + Airflow
```

> Bước 0 phải làm TRƯỚC, vì đổi version store xoá sạch refs catalog (`nessie.bronze.*`,
> `nessie.silver.*` mất). Data nguồn Postgres/Oracle vẫn còn (named volume) nhưng ta sẽ
> regenerate đè lên nên không sao.

---

## Mô hình bảng `nessie.gold.*`

| Bảng Gold | Grain | Nguồn Silver | Ghi/Refresh |
|---|---|---|---|
| `agg_customer_holdings` | 1 row / customer_id | dim_account, dim_card_account, dim_loan (+dim_product) | OVERWRITE (full snapshot) |
| `agg_customer_txn_12m` | 1 row / customer_id | fct_bank_transactions, fct_card_transactions (qua account→customer) | OVERWRITE |
| `mart_customer_360` | 1 row / customer_id | dim_customer(current) + 2 agg trên + RFM | OVERWRITE, partition `snapshot_date` |
| `audit_log` | 1 row / lần build | — | append (part. days(created_at)) |

> Gold là **snapshot tính lại toàn bộ mỗi lần chạy** (OVERWRITE) — KHÔNG incremental/SCD.
> Mỗi run ghi `snapshot_date` = as-of date → có thể giữ lịch sử snapshot nếu muốn (partition).
> Idempotent tự nhiên nhờ OVERWRITE (chạy 2 lần cùng as-of date → kết quả y hệt).

### As-of date (mốc tính KPI/RFM)
Định nghĩa `GOLD_AS_OF_DATE` = **max(txn_date)** trên 2 fact (mặc định), hoặc set cứng qua
ENV. Lý do: data sinh ra có thể không tới "hôm nay" → nếu lấy `current_date()` thì recency
của mọi KH đều rất lớn → RFM lệch. Lấy max txn_date làm "hiện tại của dữ liệu" → recency có
ý nghĩa. `age` cũng tính theo as-of date này.

---

## Cấu trúc file sẽ tạo ra

```
lakehouse-customer360/
├── scripts/
│   └── generate_data.py                ← MỚI: sinh ~800 KH + txn vào Postgres+Oracle
├── config/
│   └── gold_tables.yaml                ← MỚI: bản đồ Gold (as_of, ngưỡng RFM, cross-sell rules)
├── src/
│   └── gold/
│       ├── create_gold_tables.py       ← tạo namespace nessie.gold + 4 bảng (chạy 1 lần)
│       ├── build_agg_holdings.py        ← agg_customer_holdings
│       ├── build_agg_txn_12m.py         ← agg_customer_txn_12m (+ recency/frequency/monetary)
│       ├── build_mart_360.py            ← mart_customer_360 (join + RFM + cross-sell + identity)
│       ├── run_gold.py                  ← orchestrator (chạy tuần tự, ghi gold.audit_log)
│       └── verify_gold.py               ← count + sample 360 + RFM distribution + cross-sell counts
├── sql/
│   └── gold/
│       ├── vw_marketing_rfm.sql         ← (optional) Trino view: danh sách KH theo RFM segment
│       └── vw_sales_crosssell.sql       ← (optional) Trino view: danh sách KH cross-sell
├── dags/
│   └── dag_gold_mart.py                 ← Silver → Gold DAG (BashOperator)
└── docker-compose.yml                   ← SỬA service nessie (ROCKSDB + volume)
```

> **Tái dùng (KHÔNG viết lại):** `src/common/spark_session.py` (`get_spark_session`,
> `align_to_table`, `SPARK_PACKAGES`), `src/common/audit_logger.py` (param hoá — Gold dùng
> `nessie.gold.audit_log`), pattern `--packages` CLI, pattern DAG BashOperator +
> `docker exec lakehouse-spark-master spark-submit`. Bronze/Silver create+run scripts tái
> dùng nguyên ở Bước 2-3.

---

## Bước 0 — Nessie bền (ROCKSDB + volume)

Sửa service `nessie` trong `docker-compose.yml`: bỏ `IN_MEMORY`, dùng `ROCKSDB` + mount
named volume để dữ liệu catalog nằm trên disk.

```yaml
  nessie:
    image: ghcr.io/projectnessie/nessie:0.79.0
    container_name: lakehouse-nessie
    ports:
      - "19120:19120"
    environment:
      nessie.version.store.type: ROCKSDB
      nessie.version.store.persist.rocks.database-path: /nessie/data
    volumes:
      - nessie-data:/nessie/data          # MỚI: catalog bền qua restart

# ... cuối file, mục volumes:
volumes:
  nessie-data:                            # MỚI
```

> **Lưu ý:**
> - Tên property Nessie 0.79: `nessie.version.store.type` = `ROCKSDB`; database-path qua
>   `nessie.version.store.persist.rocks.database-path`. Nếu image báo property khác (một số
>   bản dùng `NESSIE_VERSION_STORE_TYPE` env in hoa) → thử dạng ENV in hoa tương ứng.
> - **Phương án B (production hơn):** `JDBC` trỏ Postgres (`nessie.version.store.type=JDBC`
>   + datasource trỏ `lakehouse-postgres`). Phức tạp hơn (cần tạo DB `nessie` + driver) →
>   chọn ROCKSDB cho đơn giản + đủ bền.
> - Sau khi `docker compose up -d nessie`, catalog RỖNG → bắt buộc làm Bước 2-3 (recreate
>   tables) trước khi có data.

Áp dụng:
```powershell
docker compose up -d nessie
docker logs lakehouse-nessie --tail 20      # xác nhận khởi động, dùng ROCKSDB
```

---

## Bước 1 — `scripts/generate_data.py` (sinh dữ liệu)

Chạy trên **host** qua venv Phase 0 (đã có `psycopg2-binary`, `oracledb`). Kết nối
`localhost:5432` (Postgres `banking`) + `localhost:1521` (Oracle `XEPDB1` / `corebanking`).
TRUNCATE bảng cũ rồi sinh mới, GIỮ NGUYÊN schema cột (`scripts/init-postgres.sql` /
`init-oracle.sql`).

**Tham số sinh (mặc định):**
| Thực thể | Số lượng | Ghi chú |
|---|---|---|
| customers (Postgres) | 800 | segment MASS/AFFLUENT/PREMIER ~ 70/22/8%; city/province VN; DOB 1955-2003 |
| bank_accounts (Oracle) | ~1.6/KH | product PRD001/PRD003 (savings/current) + 1 ít TERM_DEPOSIT; branch BR001/BR002; balance lệch theo segment |
| card_accounts (Postgres) | ~55% KH | VISA/MASTERCARD credit/debit; credit_limit theo segment; outstanding < limit |
| loans (Oracle) | ~30% KH | PRD002; principal/outstanding; ~8% NPL (npl_status≠NORMAL) |
| bank_transactions (Oracle) | ~12 tháng, 0-12 txn/account/tháng | DEPOSIT/WITHDRAWAL/TRANSFER/PAYMENT; channel ATM/ONLINE/POS/BRANCH; balance_after cộng dồn |
| card_transactions (Postgres) | ~12 tháng cho KH có thẻ | PURCHASE; merchant_category GROCERY/DINING/TRAVEL/SHOPPING/FUEL...; channel POS/ONLINE |

- Mốc thời gian txn: từ `as_of - 12 tháng` đến `as_of` (chọn `as_of = 2025-12-31` cho gọn).
- `customer_id` = `CIF` + số thứ tự zero-pad (CIF000001...). Dùng làm khoá xuyên 2 hệ thống.
- Set `created_at/updated_at = NOW()` để Bronze incremental vẫn hiểu được (dù phase này
  ta dùng full_snapshot).
- Tránh thêm dependency mới: dùng `random` + danh sách họ/tên/đệm tiếng Việt cho `full_name`
  (KHÔNG cần Faker). Seed `random.seed(42)` để tái lập.

**Skeleton:**
```python
# scripts/generate_data.py
"""Sinh dữ liệu giả lập phong phú cho Lakehouse Customer 360.
Chạy trên HOST qua venv:  python scripts/generate_data.py
Kết nối Postgres(localhost:5432/banking) + Oracle(localhost:1521/XEPDB1).
TRUNCATE bảng cũ rồi nạp mới. Idempotent: chạy lại → cùng kết quả (seed cố định)."""
import os, random, datetime as dt
from dotenv import load_dotenv
import psycopg2
import oracledb

load_dotenv()
random.seed(42)
AS_OF = dt.date(2025, 12, 31)
N_CUST = 800

HO   = ["Nguyen","Tran","Le","Pham","Hoang","Phan","Vu","Dang","Bui","Do","Ho","Ngo"]
DEM  = ["Van","Thi","Hoang","Minh","Thanh","Quoc","Huu","Ngoc","Gia","Duc"]
TEN  = ["An","Bich","Cuong","Dung","Em","Phuong","Giang","Hanh","Khanh","Linh","Mai","Nam"]
SEGMENTS = (["MASS"]*70) + (["AFFLUENT"]*22) + (["PREMIER"]*8)
CITIES   = [("Ho Chi Minh","Ho Chi Minh"),("Ha Noi","Ha Noi"),("Da Nang","Da Nang"),
            ("Can Tho","Can Tho"),("Hai Phong","Hai Phong")]
MCC = ["GROCERY","DINING","TRAVEL","SHOPPING","FUEL","ENTERTAINMENT","HEALTH","EDUCATION"]

def gen_customers():
    rows = []
    for i in range(1, N_CUST+1):
        cif = f"CIF{i:06d}"
        name = f"{random.choice(HO)} {random.choice(DEM)} {random.choice(TEN)}"
        dob  = dt.date(random.randint(1955,2003), random.randint(1,12), random.randint(1,28))
        seg  = random.choice(SEGMENTS)
        city, prov = random.choice(CITIES)
        rows.append((cif, name, dob, random.choice("MF"),
                     f"{random.randint(10**11,10**12-1)}", f"09{random.randint(10**8,10**9-1)}",
                     f"user{i}@example.com", city, prov, seg, "VERIFIED"))
    return rows

# ... tương tự gen_bank_accounts(custs), gen_card_accounts(custs), gen_loans(custs),
#     gen_bank_txns(bank_accounts), gen_card_txns(card_accounts)
#     -> balance theo segment (PREMIER lớn hơn), txn amount lognormal-ish bằng random.

def load_postgres(custs, cards, card_txns):
    cn = psycopg2.connect(host="localhost", port=5432, dbname="banking",
                          user="postgres", password=os.environ["POSTGRES_PASSWORD"])
    cur = cn.cursor()
    cur.execute("TRUNCATE card_transactions, card_accounts, customers CASCADE")
    cur.executemany("INSERT INTO customers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())", custs)
    cur.executemany("INSERT INTO card_accounts VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NOW(),NOW())", cards)
    cur.executemany("INSERT INTO card_transactions VALUES (%s,%s,%s,%s,%s,'VND',%s,%s,%s,%s,'COMPLETED',NOW())", card_txns)
    cn.commit(); cn.close()

def load_oracle(bank_accts, loans, bank_txns):
    cn = oracledb.connect(user="corebanking", password=os.environ["ORACLE_PASSWORD"],
                          dsn="localhost:1521/XEPDB1")
    cur = cn.cursor()
    for t in ("bank_transactions","loans","bank_accounts"):   # xoá theo thứ tự FK
        cur.execute(f"DELETE FROM {t}")
    cur.executemany("INSERT INTO bank_accounts VALUES (:1,:2,:3,:4,:5,:6,:7,'VND',:8,:9,NULL,:10,SYSTIMESTAMP)", bank_accts)
    cur.executemany("INSERT INTO loans VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,'ACTIVE',SYSTIMESTAMP)", loans)
    cur.executemany("INSERT INTO bank_transactions VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'COMPLETED',SYSTIMESTAMP)", bank_txns)
    cn.commit(); cn.close()

if __name__ == "__main__":
    # build các list rồi load. In ra số lượng từng bảng để kiểm chứng.
    ...
```

> **Lưu ý generate:**
> - Số cột & thứ tự INSERT phải KHỚP DDL gốc (Postgres `customers` 13 cột gồm created/updated;
>   Oracle `bank_accounts` có `currency`, `closed_date`, `interest_rate`...). Kiểm chứng lại
>   với `init-postgres.sql` / `init-oracle.sql` trước khi chạy.
> - Giữ tổng txn vừa phải (~80-150k mỗi loại) để Spark trên máy 16GB chạy nhẹ.
> - Oracle `oracledb` thin mode OK (Phase 0 đã kết nối được). Date → truyền `datetime.date`.

---

## Bước 2-3 — Chạy lại Bronze + Silver (tái dùng code cũ)

Sau khi có data mới + Nessie rỗng (do đổi store), dựng lại catalog:

```powershell
$PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.postgresql:postgresql:42.7.3"

# Bronze
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/create_bronze_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/full_snapshot.py
# Silver
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/create_silver_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
```

> MinIO `warehouse/bronze` & `warehouse/silver` cũ thành orphan (refs cũ đã mất theo store
> IN_MEMORY). Không bắt buộc dọn ngay — `create_*_tables` ghi vào path mới. Dọn small files
> để Phase 4 (Governance/maintenance).

---

## Bước 4 — `config/gold_tables.yaml` (bản đồ Gold)

Tham số hoá as-of date, ngưỡng RFM, rule cross-sell → đổi business logic chỉ sửa file này.

```yaml
# config/gold_tables.yaml
as_of_date: auto            # auto = max(txn_date) trên 2 fact; hoặc set 'YYYY-MM-DD'
lookback_months: 12         # cửa sổ tính txn (recency/frequency/monetary, txn_*_12m)

rfm:
  n_tiles: 5                # NTILE(5): R/F/M mỗi chiều 1..5
  # segment map theo (R,F) — đơn giản hoá RFM 11-segment cổ điển
  segments:
    Champions:        "r >= 4 AND f >= 4 AND m >= 4"
    Loyal:            "f >= 4"
    Potential_Loyal:  "r >= 4 AND f >= 2"
    New:              "r >= 4 AND f < 2"
    At_Risk:          "r <= 2 AND f >= 3"
    Hibernating:      "r <= 2 AND f <= 2"
    # còn lại -> Need_Attention

crosssell:
  # KH có tiền gửi tốt nhưng chưa có vay -> mời vay
  loan_flag:   "has_loan = false AND total_deposit_balance >= 50000000"
  # KH chưa có thẻ tín dụng nhưng chi tiêu/độ hoạt động tốt -> mời thẻ
  card_flag:   "has_credit_card = false AND txn_count_12m >= 12"
  # KH net asset cao, segment cao -> mời đầu tư (proxy, chưa có product invest)
  invest_flag: "net_asset_value >= 200000000 AND customer_segment IN ('AFFLUENT','PREMIER')"

thresholds:
  npl_statuses: ['NPL', 'SUB_STANDARD', 'DOUBTFUL', 'LOSS', 'OVERDUE']  # ≠ NORMAL = NPL
  digital_channels: ['ONLINE']     # phần còn lại (ATM/POS/BRANCH) = non-digital
```

---

## Bước 5 — `src/gold/create_gold_tables.py` (chạy 1 lần)

Tạo namespace `nessie.gold` + 4 bảng. Schema khớp KPI list.

```python
# src/gold/create_gold_tables.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

def create(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.agg_customer_holdings (
            customer_id STRING,
            total_product_count INT,
            has_savings BOOLEAN, has_current BOOLEAN,
            has_credit_card BOOLEAN, has_loan BOOLEAN,
            total_deposit_balance DOUBLE,
            total_loan_outstanding DOUBLE,
            total_credit_limit DOUBLE, total_card_outstanding DOUBLE,
            credit_utilization_rate DOUBLE,
            net_asset_value DOUBLE,
            has_npl_loan BOOLEAN, days_past_due INT,
            _gold_loaded_at TIMESTAMP, _gold_batch_id STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.agg_customer_txn_12m (
            customer_id STRING,
            txn_count_12m INT, txn_amount_12m DOUBLE,
            avg_monthly_spend DOUBLE, active_months_12m INT,
            top_spend_category STRING, digital_txn_ratio DOUBLE,
            last_txn_date DATE,
            recency_days INT, frequency_12m INT, monetary_12m DOUBLE,
            _gold_loaded_at TIMESTAMP, _gold_batch_id STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.mart_customer_360 (
            -- Identity
            customer_id STRING, full_name STRING, age INT, gender STRING,
            customer_segment STRING, kyc_status STRING, city STRING, province STRING,
            -- Product holding
            total_product_count INT, has_savings BOOLEAN, has_current BOOLEAN,
            has_credit_card BOOLEAN, has_loan BOOLEAN,
            -- Balance
            total_deposit_balance DOUBLE, total_loan_outstanding DOUBLE,
            credit_utilization_rate DOUBLE, net_asset_value DOUBLE,
            -- Transaction
            txn_count_12m INT, txn_amount_12m DOUBLE, avg_monthly_spend DOUBLE,
            top_spend_category STRING, digital_txn_ratio DOUBLE,
            last_txn_date DATE, active_months_12m INT,
            -- RFM
            recency_days INT, frequency_12m INT, monetary_12m DOUBLE,
            r_score INT, f_score INT, m_score INT, rfm_score STRING, rfm_segment STRING,
            -- Risk
            has_npl_loan BOOLEAN, days_past_due INT,
            -- Cross-sell
            cross_sell_loan_flag BOOLEAN, cross_sell_card_flag BOOLEAN,
            cross_sell_invest_flag BOOLEAN, tenure_days INT,
            -- meta
            snapshot_date DATE, _gold_loaded_at TIMESTAMP, _gold_batch_id STRING
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.audit_log (
            batch_id STRING, table_name STRING, source_system STRING, run_mode STRING,
            status STRING, rows_ingested LONG, last_run_time TIMESTAMP,
            current_run_time TIMESTAMP, error_message STRING, created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)
    print("Gold tables created in nessie.gold")
    spark.sql("SHOW TABLES IN nessie.gold").show(truncate=False)

if __name__ == "__main__":
    spark = get_spark_session("create-gold-tables")
    create(spark); spark.stop()
```

> 25+ KPI nằm rải ở `mart_customer_360` (đếm: 8 identity + 5 holding + 4 balance + 7 txn +
> 8 RFM + 2 risk + 4 cross-sell = **38 cột nghiệp vụ** > 25, đạt yêu cầu). `tenure_days` =
> as_of − min(opened_date) các account của KH.

---

## Bước 6 — `src/gold/build_agg_holdings.py`

Gom product holding + balance + NPL theo customer_id. Chỉ lấy **bản current** của dim
(SCD1 vốn là current; `dim_customer` SCD2 → filter `is_current=true`). `OVERWRITE`.

Logic chính (Spark SQL):
```sql
WITH bank AS (   -- tài khoản ngân hàng theo KH
  SELECT customer_id,
         COUNT(*) AS n_bank_acct,
         SUM(CASE WHEN account_type='TERM_DEPOSIT' THEN 1 ELSE 0 END) > 0 AS has_savings,
         SUM(CASE WHEN account_type='CURRENT'      THEN 1 ELSE 0 END) > 0 AS has_current,
         SUM(COALESCE(balance,0)) AS total_deposit_balance
  FROM nessie.silver.dim_account
  WHERE status='ACTIVE' GROUP BY customer_id
),
card AS (
  SELECT customer_id,
         MAX(CASE WHEN card_type LIKE '%CREDIT%' THEN true ELSE false END) AS has_credit_card,
         SUM(COALESCE(credit_limit,0))        AS total_credit_limit,
         SUM(COALESCE(outstanding_balance,0)) AS total_card_outstanding
  FROM nessie.silver.dim_card_account WHERE status='ACTIVE' GROUP BY customer_id
),
loan AS (
  SELECT customer_id,
         COUNT(*) > 0 AS has_loan,
         SUM(COALESCE(outstanding_amount,0)) AS total_loan_outstanding,
         MAX(CASE WHEN npl_status <> 'NORMAL' THEN true ELSE false END) AS has_npl_loan,
         -- days_past_due: PROXY (source không có cột DPD) = nếu NPL & quá hạn thì khoảng
         -- cách as_of - maturity_date, ngược lại 0
         MAX(CASE WHEN npl_status<>'NORMAL'
                  THEN GREATEST(DATEDIFF(DATE '<AS_OF>', maturity_date), 0) ELSE 0 END) AS days_past_due
  FROM nessie.silver.dim_loan WHERE status='ACTIVE' GROUP BY customer_id
)
SELECT c.customer_id,
       (COALESCE(b.n_bank_acct,0) + ...card... + ...loan...) AS total_product_count,
       COALESCE(b.has_savings,false), COALESCE(b.has_current,false),
       COALESCE(cd.has_credit_card,false), COALESCE(l.has_loan,false),
       COALESCE(b.total_deposit_balance,0), COALESCE(l.total_loan_outstanding,0),
       COALESCE(cd.total_credit_limit,0), COALESCE(cd.total_card_outstanding,0),
       CASE WHEN cd.total_credit_limit>0
            THEN cd.total_card_outstanding/cd.total_credit_limit ELSE 0 END AS credit_utilization_rate,
       COALESCE(b.total_deposit_balance,0) - COALESCE(l.total_loan_outstanding,0)
            - COALESCE(cd.total_card_outstanding,0) AS net_asset_value,
       COALESCE(l.has_npl_loan,false), COALESCE(l.days_past_due,0)
FROM (SELECT DISTINCT customer_id FROM nessie.silver.dim_customer WHERE is_current=true) c
LEFT JOIN bank b ON ... LEFT JOIN card cd ON ... LEFT JOIN loan l ON ...
```
Ghi: thêm `_gold_loaded_at/_gold_batch_id` → `df.writeTo("nessie.gold.agg_customer_holdings").overwritePartitions()`
(bảng không partition → dùng `.createOrReplace()` hoặc `INSERT OVERWRITE`). Trả `count()`.

> **`net_asset_value`** = deposit − loan_outstanding − card_outstanding (tài sản ròng tại bank).
> **`has_savings`**: map TERM_DEPOSIT (product SAVINGS) → savings; CURRENT → current.

---

## Bước 7 — `src/gold/build_agg_txn_12m.py`

Union 2 fact → map account→customer (qua dim_account / dim_card_account) → filter trong
cửa sổ `lookback_months` tính từ `as_of` → aggregate. `OVERWRITE`.

```sql
WITH txns AS (
  SELECT a.customer_id, f.txn_date, f.amount, f.channel,
         CAST(NULL AS STRING) AS merchant_category          -- bank txn không có category
  FROM nessie.silver.fct_bank_transactions f
  JOIN nessie.silver.dim_account a ON f.account_id = a.account_id
  WHERE f.txn_date > add_months(DATE '<AS_OF>', -<LOOKBACK>) AND f.status='COMPLETED'
  UNION ALL
  SELECT ca.customer_id, f.txn_date, f.amount, f.channel, f.merchant_category
  FROM nessie.silver.fct_card_transactions f
  JOIN nessie.silver.dim_card_account ca ON f.account_id = ca.account_id
  WHERE f.txn_date > add_months(DATE '<AS_OF>', -<LOOKBACK>) AND f.status='COMPLETED'
),
base AS (
  SELECT customer_id,
         COUNT(*)                                   AS txn_count_12m,
         SUM(amount)                                AS txn_amount_12m,
         COUNT(DISTINCT date_format(txn_date,'yyyy-MM')) AS active_months_12m,
         MAX(txn_date)                              AS last_txn_date,
         SUM(CASE WHEN channel IN ('ONLINE') THEN 1 ELSE 0 END)/COUNT(*) AS digital_txn_ratio
  FROM txns GROUP BY customer_id
),
topcat AS (   -- top_spend_category theo tổng amount (chỉ card txn có category)
  SELECT customer_id, merchant_category FROM (
    SELECT customer_id, merchant_category, SUM(amount) s,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY SUM(amount) DESC) rn
    FROM txns WHERE merchant_category IS NOT NULL GROUP BY customer_id, merchant_category
  ) WHERE rn=1
)
SELECT b.customer_id, b.txn_count_12m, b.txn_amount_12m,
       b.txn_amount_12m / <LOOKBACK>            AS avg_monthly_spend,
       b.active_months_12m, t.merchant_category AS top_spend_category, b.digital_txn_ratio,
       b.last_txn_date,
       DATEDIFF(DATE '<AS_OF>', b.last_txn_date) AS recency_days,   -- R
       b.txn_count_12m                          AS frequency_12m,    -- F
       b.txn_amount_12m                         AS monetary_12m      -- M
FROM base b LEFT JOIN topcat t ON b.customer_id=t.customer_id
```

> KH không có txn trong cửa sổ → KHÔNG xuất hiện ở bảng này → ở mart sẽ LEFT JOIN và
> COALESCE (recency=NULL→điểm thấp, frequency/monetary=0).

---

## Bước 8 — `src/gold/build_mart_360.py` (join + RFM + cross-sell + identity)

Join `dim_customer(current)` + 2 agg → tính identity (age, tenure) + **RFM (NTILE)** +
**cross-sell flags** (đọc rule từ YAML). `INSERT OVERWRITE` partition `snapshot_date=as_of`.

**RFM bằng window NTILE** (điểm 1..5; R: recent=cao, F/M: lớn=cao):
```sql
WITH j AS (
  SELECT cu.customer_id, cu.full_name, cu.gender, cu.customer_segment, cu.kyc_status,
         cu.city, cu.province, cu.date_of_birth,
         floor(months_between(DATE '<AS_OF>', cu.date_of_birth)/12) AS age,
         h.*, t.*
  FROM (SELECT * FROM nessie.silver.dim_customer WHERE is_current=true) cu
  LEFT JOIN nessie.gold.agg_customer_holdings h ON cu.customer_id=h.customer_id
  LEFT JOIN nessie.gold.agg_customer_txn_12m  t ON cu.customer_id=t.customer_id
),
rfm AS (
  SELECT *,
    NTILE(5) OVER (ORDER BY COALESCE(recency_days, 999999) DESC) AS r_score, -- ít ngày = tile cao
    NTILE(5) OVER (ORDER BY COALESCE(frequency_12m,0) ASC)       AS f_score,
    NTILE(5) OVER (ORDER BY COALESCE(monetary_12m,0)  ASC)       AS m_score
  FROM j
)
SELECT customer_id, full_name, age, gender, customer_segment, kyc_status, city, province,
       total_product_count, has_savings, has_current, has_credit_card, has_loan,
       total_deposit_balance, total_loan_outstanding, credit_utilization_rate, net_asset_value,
       COALESCE(txn_count_12m,0), COALESCE(txn_amount_12m,0), COALESCE(avg_monthly_spend,0),
       top_spend_category, COALESCE(digital_txn_ratio,0), last_txn_date, COALESCE(active_months_12m,0),
       recency_days, COALESCE(frequency_12m,0), COALESCE(monetary_12m,0),
       r_score, f_score, m_score,
       CONCAT(CAST(r_score AS STRING),CAST(f_score AS STRING),CAST(m_score AS STRING)) AS rfm_score,
       CASE
         WHEN r_score>=4 AND f_score>=4 AND m_score>=4 THEN 'Champions'
         WHEN f_score>=4                               THEN 'Loyal'
         WHEN r_score>=4 AND f_score>=2                THEN 'Potential_Loyal'
         WHEN r_score>=4 AND f_score<2                 THEN 'New'
         WHEN r_score<=2 AND f_score>=3                THEN 'At_Risk'
         WHEN r_score<=2 AND f_score<=2                THEN 'Hibernating'
         ELSE 'Need_Attention'
       END AS rfm_segment,
       COALESCE(has_npl_loan,false), COALESCE(days_past_due,0),
       -- cross-sell (rule từ YAML, hardcode tương đương ở đây)
       (NOT COALESCE(has_loan,false) AND COALESCE(total_deposit_balance,0)>=50000000) AS cross_sell_loan_flag,
       (NOT COALESCE(has_credit_card,false) AND COALESCE(txn_count_12m,0)>=12)        AS cross_sell_card_flag,
       (COALESCE(net_asset_value,0)>=200000000 AND customer_segment IN ('AFFLUENT','PREMIER')) AS cross_sell_invest_flag,
       <tenure_days_subquery> AS tenure_days,
       DATE '<AS_OF>' AS snapshot_date
FROM rfm
```
`tenure_days` = `DATEDIFF(as_of, min(opened_date))` gom từ dim_account + dim_card_account
theo customer_id (tính ở 1 CTE phụ rồi join, hoặc đưa sang agg_holdings cho gọn — **khuyến
nghị: thêm `first_open_date` vào `agg_customer_holdings`** để mart chỉ việc `DATEDIFF`).

Ghi: `df.writeTo("nessie.gold.mart_customer_360").overwritePartitions()` (ghi đè đúng
partition `snapshot_date` hiện tại; snapshot ngày khác giữ nguyên → lịch sử snapshot).

> **Đọc rule từ YAML thật:** build_mart đọc `gold_tables.yaml`, lấy chuỗi điều kiện
> `crosssell.*` và `rfm.segments` rồi nội suy vào SQL (f-string). Như vậy đổi rule không
> phải sửa Python. (Bản skeleton trên hardcode để minh hoạ logic.)

---

## Bước 9 — `src/gold/run_gold.py` (orchestrator)

Khác Silver (loop độc lập): Gold **chạy tuần tự có thứ tự phụ thuộc**. Resolve `as_of_date`
một lần (nếu `auto` → query max txn_date), rồi truyền xuống từng build. Ghi `gold.audit_log`.

```python
# src/gold/run_gold.py
import sys, uuid, yaml
from datetime import datetime, timezone
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session
from common.audit_logger import AuditLogger
from gold.build_agg_holdings import build_agg_holdings
from gold.build_agg_txn_12m  import build_agg_txn_12m
from gold.build_mart_360     import build_mart_360

CONFIG_PATH = "/opt/spark/config/gold_tables.yaml"

def resolve_as_of(spark, cfg):
    if cfg.get("as_of_date","auto") != "auto":
        return str(cfg["as_of_date"])
    row = spark.sql("""
        SELECT MAX(d) m FROM (
          SELECT MAX(txn_date) d FROM nessie.silver.fct_bank_transactions
          UNION ALL SELECT MAX(txn_date) FROM nessie.silver.fct_card_transactions)
    """).collect()[0]
    return str(row["m"])

PIPELINE = [("agg_customer_holdings", build_agg_holdings),
            ("agg_customer_txn_12m",  build_agg_txn_12m),
            ("mart_customer_360",     build_mart_360)]

def main():
    spark = get_spark_session("gold-build")
    with open(CONFIG_PATH) as f: cfg = yaml.safe_load(f)
    as_of = resolve_as_of(spark, cfg)
    logger = AuditLogger(spark, "nessie.gold.audit_log")
    batch_id = str(uuid.uuid4())[:8]
    print(f"Gold build as_of={as_of} batch={batch_id}")
    for name, fn in PIPELINE:
        rt = datetime.now(timezone.utc)
        try:
            rows = fn(spark, cfg, as_of, batch_id)
            logger.log(batch_id=batch_id, table_name=name, source_system="gold",
                       run_mode="overwrite", status="SUCCESS", rows_ingested=rows,
                       last_run_time=None, current_run_time=rt)
        except Exception as e:
            print(f"  FAILED {name}: {e}")
            logger.log(batch_id=batch_id, table_name=name, source_system="gold",
                       run_mode="overwrite", status="FAILED", rows_ingested=0,
                       last_run_time=None, current_run_time=rt, error_message=str(e))
            raise        # mart phụ thuộc agg → dừng nếu agg fail
    spark.stop()
    print(f"\nGold build done. Batch {batch_id}")

if __name__ == "__main__":
    main()
```

> `build_*` nhận signature `(spark, cfg, as_of, batch_id) -> int`. As-of được nội suy vào
> các `DATE '<AS_OF>'` trong SQL.

---

## Bước 10 — `src/gold/verify_gold.py`

```python
# src/gold/verify_gold.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

def main():
    spark = get_spark_session("gold-verify")
    cust = sys.argv[1] if len(sys.argv) > 1 else None

    n_cust = spark.table("nessie.silver.dim_customer").filter("is_current=true").count()
    n_mart = spark.table("nessie.gold.mart_customer_360").count()
    print(f"\ndim_customer current = {n_cust} | mart rows = {n_mart}  (phai BANG nhau)")

    print("\n== RFM segment distribution ==")
    spark.sql("""SELECT rfm_segment, COUNT(*) n, ROUND(AVG(monetary_12m),0) avg_m
                 FROM nessie.gold.mart_customer_360 GROUP BY rfm_segment ORDER BY n DESC""").show()

    print("\n== Cross-sell counts ==")
    spark.sql("""SELECT SUM(CAST(cross_sell_loan_flag AS INT)) loan,
                        SUM(CAST(cross_sell_card_flag AS INT)) card,
                        SUM(CAST(cross_sell_invest_flag AS INT)) invest
                 FROM nessie.gold.mart_customer_360""").show()

    if cust:
        spark.table("nessie.gold.mart_customer_360").filter(f"customer_id='{cust}'").show(vertical=True, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## Bước 11 — `dags/dag_gold_mart.py` (Silver → Gold)

Mirror `dag_silver_transform.py`. Flow `gold_build → quality_check`, schedule 4 AM (sau
Silver 3 AM).

```python
# dags/dag_gold_mart.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "lakehouse-spark-master"
SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.postgresql:postgresql:42.7.3")

def submit(job):
    return (f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 --packages '{SPARK_PACKAGES}' "
            f"/opt/spark/jobs/gold/{job}")

default_args = {"owner":"lakehouse","retries":2,"retry_delay":timedelta(minutes=5),
                "email_on_failure":False}

with DAG(dag_id="gold_mart", default_args=default_args,
         description="Silver -> Gold: mart_customer_360 + RFM + cross-sell",
         start_date=datetime(2024,11,1), schedule_interval="0 4 * * *",
         catchup=False, tags=["gold","mart","rfm"]) as dag:
    gold_build    = BashOperator(task_id="gold_build",    bash_command=submit("run_gold.py"))
    quality_check = BashOperator(task_id="quality_check", bash_command=submit("verify_gold.py"))
    gold_build >> quality_check
```

> **Optional chain:** thêm `TriggerDagRunOperator(trigger_dag_id="gold_mart")` cuối
> `dag_silver_transform.py` để Gold tự chạy sau Silver. Mặc định để độc lập, lệch giờ.

---

## Bước 12 — (optional) Trino views cho nghiệp vụ

`sql/gold/vw_marketing_rfm.sql`:
```sql
CREATE OR REPLACE VIEW iceberg.gold.vw_marketing_rfm AS
SELECT customer_id, full_name, customer_segment, rfm_segment, rfm_score,
       recency_days, frequency_12m, monetary_12m
FROM iceberg.gold.mart_customer_360
WHERE rfm_segment IN ('At_Risk','Hibernating','Champions');
```
`sql/gold/vw_sales_crosssell.sql`:
```sql
CREATE OR REPLACE VIEW iceberg.gold.vw_sales_crosssell AS
SELECT customer_id, full_name, customer_segment, net_asset_value,
       cross_sell_loan_flag, cross_sell_card_flag, cross_sell_invest_flag
FROM iceberg.gold.mart_customer_360
WHERE cross_sell_loan_flag OR cross_sell_card_flag OR cross_sell_invest_flag;
```
Chạy qua Trino CLI/DBeaver. (View là "materialized view" logic cho BI/marketing.)

---

## Test end-to-end (PowerShell — KHÔNG Git Bash; `$PKGS` như trên)

```powershell
# (sau khi đã xong Bước 0-3: Nessie bền + data mới + Bronze + Silver)

# 1) Tạo Gold tables (1 lần)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/create_gold_tables.py
# 2) Build Gold (agg -> agg -> mart)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/run_gold.py
# 3) Verify
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/verify_gold.py CIF000001
```

### Idempotency
```powershell
# Chạy run_gold.py lần 2 -> mart row count KHÔNG đổi (OVERWRITE cùng snapshot_date)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/run_gold.py
```

### Trino (DBeaver localhost:8088)
```sql
SHOW TABLES FROM iceberg.gold;
SELECT rfm_segment, COUNT(*) FROM iceberg.gold.mart_customer_360 GROUP BY rfm_segment;
SELECT * FROM iceberg.gold.mart_customer_360 WHERE customer_id='CIF000001';
SELECT COUNT(*) FROM iceberg.gold.vw_sales_crosssell;
```

### Airflow
```powershell
docker exec lakehouse-airflow-scheduler airflow dags unpause gold_mart
docker exec lakehouse-airflow-scheduler airflow dags trigger gold_mart
# UI http://localhost:8080 (admin/admin) -> 2 task SUCCESS
```

---

## Kết quả mong đợi khi hoàn thành
- [ ] Nessie ROCKSDB + volume → restart container catalog KHÔNG mất (test: `docker restart lakehouse-nessie` rồi query lại)
- [ ] `generate_data.py` nạp ~800 KH + txn nhiều tháng vào Postgres + Oracle
- [ ] 4 Gold tables trong `nessie.gold` (2 agg + mart + audit_log)
- [ ] `mart_customer_360` rows == số `dim_customer` current; mỗi KH đúng 1 row; 38 cột KPI đầy đủ
- [ ] RFM segment distribution có **nhiều bucket khác nhau** (không dồn 1 nhóm) — nhờ data mới
- [ ] Cross-sell flags có KH true/false hợp lý theo rule YAML
- [ ] Idempotency: rerun → mart không đổi
- [ ] Trino query `iceberg.gold.*` + 2 view OK qua DBeaver
- [ ] DAG `gold_mart` trigger thật, mọi task SUCCESS; `gold.audit_log` có entry
- [ ] MinIO `warehouse/gold/` có đủ table dir + parquet

---

## Lỗi có thể gặp (dự đoán — cập nhật thật vào phase3_done.md)

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| Nessie đổi store xong query báo TABLE_NOT_FOUND | Store mới rỗng (đúng như kỳ vọng) | Chạy lại create_bronze/silver + full_snapshot + run_silver (Bước 2-3) |
| `generate_data.py` lỗi số cột INSERT | thứ tự/đếm cột không khớp DDL | Đối chiếu `init-postgres.sql`/`init-oracle.sql`; chú ý cột default (created_at...) |
| Oracle `DELETE` chậm/lock | bảng lớn + FK | DELETE theo thứ tự con→cha (txn→loan/acct); hoặc `TRUNCATE` (cần disable FK) |
| RFM NTILE dồn 1 bucket | data vẫn ít / nhiều KH trùng giá trị | tăng N_CUST; NTILE chia đều theo thứ hạng — chấp nhận tie |
| `overwritePartitions` lỗi bảng không partition | agg không partition | dùng `INSERT OVERWRITE` / `.createOrReplace()` cho agg; `overwritePartitions` chỉ cho mart |
| mart thiếu KH không có txn | dùng INNER JOIN nhầm | phải LEFT JOIN từ `dim_customer` + COALESCE |
| Trino không thấy `gold` namespace | catalog refresh | `SHOW SCHEMAS FROM iceberg`; restart trino nếu cần |
| exit code 255 lúc spark-submit | netty đóng stream lúc shutdown (lành tính) | bỏ qua — kiểm tra bằng output/audit_log |

---

## Sau khi hoàn thành
- Cập nhật `memory/phase3_done.md` (mẫu như phase2_done): kết quả test thật + điểm khác draft.
- Phase 4 — Governance & Ops: PII masking (email/phone/full_name ở Silver/Gold), Iceberg
  maintenance (compaction/vacuum/small files — dọn cả orphan bronze/silver cũ), Time Travel
  + Schema Evolution demo, DAG maintenance định kỳ.
```
