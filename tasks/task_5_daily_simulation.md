# Task: Daily Data Simulator — sinh data tự động theo ngày (chen trước Phase 4)

## Mục tiêu
Một **trình mô phỏng nguồn** chạy tự động mỗi ngày qua Airflow: sinh dữ liệu mới **đúng theo
cadence thực tế của từng bảng** vào Oracle + Postgres, rồi đẩy qua nhánh **incremental** →
Bronze → Silver → Gold. Biến lakehouse thành hệ thống "sống" tự cập nhật — đồng thời lần đầu
chạy thật nhánh incremental + SCD2 + tăng trưởng khách hàng.

## Trạng thái
- [x] **ĐÃ XONG (2026-06-09)** — implement + test end-to-end thật. Chi tiết & 1 điểm khác
  draft xem `memory/daily_simulation_done.md`.
  - **Khác draft (quan trọng):** cột CDC `created_at`/`updated_at` dùng **`NOW()`/`SYSTIMESTAMP`
    (giờ thực)**, KHÔNG phải `--date`. Vì incremental lọc `> last_run_time` (giờ thực lần trước);
    nếu set = ngày mô phỏng (quá khứ) sẽ bị lọc bỏ. Chỉ cột nghiệp vụ `txn_date`/`txn_datetime`/
    `opened_date` = `--date`. Idempotency dùng **guard "ngày đã có data → skip"** + ON CONFLICT/
    batcherrors.
  - Kết quả: daily 2026-06-02 sinh 769 bank + 196 card txn, 3 KH mới, 9 đổi segment, 2 NPL →
    bronze incremental upsert ĐÚNG delta (12 customers, 769/196 txn...) → SCD2 12 version mới →
    mart 803 rows, 2 partition snapshot_date (2025-12-31 giữ nguyên + 2026-06-02 mới). DAG
    `daily_simulation` parse OK, task `simulate` test SUCCESS.

> Đánh số: đây là hạng mục **chen giữa Phase 3 (Gold) và Phase 4 (Governance)**. Không đổi
> số phase trong `plan_overall.md`; task này đứng riêng (`task_5`).

---

## Vấn đề đang tồn đọng (tại sao làm)
- `mart_customer_360` hiện tính từ **1 lần bootstrap tĩnh** (800 KH, giao dịch tới 2025-12-31).
- Pipeline incremental (`incremental_load.py`) **đã có sẵn** — lọc `incremental_col >
  last_run_time` rồi MERGE upsert — nhưng **chưa từng dùng thật**, mới chỉ `full_snapshot`.
- `generate_data.py` hiện chạy trên **host** (nối `localhost` nhờ port-mapping) và `TRUNCATE`
  toàn bộ → chỉ hợp bootstrap một lần.
- Để tự động hàng ngày phải: (1) chạy **trong Docker** (nối service name `postgres`/`oracle-xe`),
  (2) đổi logic sang **append/mutate** thay vì wipe, (3) **Airflow điều phối**.

---

## Quyết định scope (user chốt qua plan mode)
1. **Timeline = Airflow `execution_date`**: DAG `catchup=True`, mỗi run mang `{{ ds }}` = "ngày
   mô phỏng". Airflow tự backfill tuần tự; không cần lưu con trỏ ngày; deterministic theo ngày.
2. **Một DAG khép kín** `daily_simulation`: simulate → bronze **incremental** → silver → gold
   → verify (KHÔNG chạy full_snapshot trong nhánh daily).
3. **Đầy đủ 3 tier mutation** (STATIC / SLOW / FAST).

---

## Mô hình cadence 3 tier (bản chất thiết kế)

| Tier | Bảng | Hành vi daily |
|---|---|---|
| **STATIC** | `branches`, `products` | KHÔNG đụng. Đã là `full_snapshot` mode trong `bronze_tables.yaml` → incremental_load vẫn full chúng (rẻ, idempotent) |
| **SLOW** | `customers`, `bank_accounts`, `card_accounts`, `loans` | +0–3 KH mới/ngày (kèm 1 CURRENT account, đôi khi card); ~0.5% KH đổi `customer_segment`/`kyc_status`/`city` → **kích hoạt SCD2**; balance drift tài khoản có giao dịch; hiếm: trả nợ (giảm `outstanding_amount`) / chuyển NPL. Set `updated_at = <ds>` |
| **FAST** | `bank_transactions`, `card_transactions` | Subset tài khoản (vd 25–40%) giao dịch → vài trăm dòng/ngày, `txn_date`/`created_at = <ds>`, append-only |

**Ăn khớp pipeline có sẵn:** `incremental_load.run_incremental` lọc `updated_at`/`created_at >
last_run_time` → mọi dòng SLOW/FAST mới tự chảy qua Bronze→Silver→Gold. `run_gold` dùng
`as_of = auto = max(txn_date)`; vì catchup chạy **tuần tự** (`max_active_runs=1`) data nạp
đúng thứ tự → `as_of` tự bằng `{{ ds }}` → `snapshot_date` mart = ngày đó → **KHÔNG cần sửa gold**.

---

## Checklist tổng
- [ ] Bước 1: `config/simulation.yaml` — tham số cadence
- [ ] Bước 2: Refactor `scripts/generate_data.py` — `--mode {bootstrap,daily}` + `--date` + ENV host
- [ ] Bước 3: `run_daily()` — đọc state DB + sinh theo 3 tier + insert chống trùng
- [ ] Bước 4: `docker/simulator/Dockerfile` + service `data-simulator` trong compose
- [ ] Bước 5: `dags/dag_daily_simulation.py` — DAG khép kín
- [ ] Bước 6: Test bootstrap-không-hỏng → daily thủ công → pipeline → idempotency → DAG thật

---

## Cấu trúc file

```
lakehouse-customer360/
├── config/
│   └── simulation.yaml              ← MỚI: tham số cadence (YAML-driven)
├── scripts/
│   └── generate_data.py             ← SỬA: thêm --mode/--date, ENV host, run_daily()
├── docker/
│   └── simulator/
│       └── Dockerfile               ← MỚI: python:3.12-slim + psycopg2 + oracledb
├── dags/
│   └── dag_daily_simulation.py      ← MỚI: simulate→bronze inc→silver→gold→verify
└── docker-compose.yml               ← SỬA: thêm service data-simulator
```

> **Tái dùng (KHÔNG viết lại):** `incremental_load.py` (`run_incremental`), `run_silver.py`,
> `run_gold.py`, `verify_gold.py`; pattern `spark_submit_cmd` + BashOperator docker exec từ
> `dag_bronze_ingestion.py`; các helper `gen_*`, `rand_date`, `SEG_MULT`, danh sách HO/TEN/MCC...
> trong `generate_data.py`.

---

## Bước 1 — `config/simulation.yaml`

```yaml
# config/simulation.yaml — tham số cadence cho run_daily. Đổi nhịp = sửa file này.

slow:
  new_customers_per_day: [0, 3]        # range KH mới mỗi ngày
  customer_change_rate: 0.005          # % KH đổi segment/kyc/city mỗi ngày (kích hoạt SCD2)
  new_card_prob: 0.4                   # xác suất KH mới có thêm thẻ
  repayment_rate: 0.01                 # % khoản vay được trả bớt (giảm outstanding) mỗi ngày
  npl_transition_rate: 0.002           # % khoản vay chuyển NORMAL -> NPL mỗi ngày

fast:
  account_txn_participation: 0.30      # % tài khoản phát sinh giao dịch trong ngày
  bank_txns_per_account: [0, 4]        # range giao dịch bank / account hoạt động
  card_txns_per_account: [0, 3]        # range giao dịch card / card account hoạt động
  update_balance_on_txn: true          # cập nhật balance + updated_at account khi có giao dịch
```

---

## Bước 2 — Refactor `scripts/generate_data.py` (khung)

```python
import os, random, argparse, datetime as dt, yaml
from dotenv import load_dotenv
import psycopg2, oracledb

load_dotenv()

# Host từ ENV: mặc định service name (chạy trong Docker). Host thì set PG_HOST=localhost.
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
ORA_DSN = os.getenv("ORA_DSN", "oracle-xe:1521/XEPDB1")
SIM_CONFIG = os.getenv("SIM_CONFIG", "config/simulation.yaml")

def pg_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname="banking",
                            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"])
def ora_conn():
    return oracledb.connect(user=os.environ["ORACLE_USER"], password=os.environ["ORACLE_PASSWORD"], dsn=ORA_DSN)

# ... giữ nguyên HO/DEM/TEN/SEGMENTS/CITIES/MCC/SEG_MULT, rand_date(), gen_* ...

def run_bootstrap():
    """Logic hiện tại: TRUNCATE + 800 KH + lịch sử 18 tháng (đổi tên main() cũ thành đây)."""
    ...

def run_daily(date: dt.date, cfg: dict):
    """Sinh data 1 ngày: tier FAST (txn) + tier SLOW (KH mới, đổi segment, repay/NPL).
    Idempotent: seed theo ngày + insert chống trùng."""
    random.seed(int(date.strftime("%Y%m%d")))
    ...   # chi tiết Bước 3

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["bootstrap", "daily"], default="bootstrap")
    ap.add_argument("--date", default=dt.date.today().isoformat())  # Airflow truyền {{ ds }}
    args = ap.parse_args()
    if args.mode == "bootstrap":
        run_bootstrap()
    else:
        with open(SIM_CONFIG) as f:
            cfg = yaml.safe_load(f)
        run_daily(dt.date.fromisoformat(args.date), cfg)

if __name__ == "__main__":
    main()
```

> `run_bootstrap` đọc/ghi qua `pg_conn()`/`ora_conn()` (thay cho hardcode `localhost`) → chạy
> được cả host (set `PG_HOST=localhost ORA_DSN=localhost:1521/XEPDB1`) lẫn trong container.

---

## Bước 3 — `run_daily()` chi tiết

```python
def run_daily(date, cfg):
    random.seed(int(date.strftime("%Y%m%d")))
    yyyymmdd = date.strftime("%Y%m%d")
    pg, ora = pg_conn(), ora_conn()
    pgc, orac = pg.cursor(), ora.cursor()

    # 1) Đọc state hiện có (KHÔNG regenerate)
    pgc.execute("SELECT customer_id, customer_segment FROM customers")
    customers = pgc.fetchall()
    pgc.execute("SELECT account_id FROM card_accounts WHERE status='ACTIVE'")
    card_accts = [r[0] for r in pgc.fetchall()]
    orac.execute("SELECT account_id, balance FROM bank_accounts WHERE status='ACTIVE'")
    bank_accts = orac.fetchall()
    orac.execute("SELECT loan_id, outstanding_amount, npl_status FROM loans WHERE status='ACTIVE'")
    loans = orac.fetchall()
    # ID nối tiếp cho entity mới
    pgc.execute("SELECT COALESCE(MAX(CAST(SUBSTRING(customer_id,4) AS INT)),0) FROM customers")
    next_cif = pgc.fetchone()[0] + 1

    # 2) TIER FAST — giao dịch ngày (txn_id có tiền tố ngày -> unique xuyên ngày)
    bank_txns, card_txns, bal_updates = [], [], []
    seq = 0
    for acc_id, balance in bank_accts:
        if random.random() > cfg["fast"]["account_txn_participation"]:
            continue
        for _ in range(random.randint(*cfg["fast"]["bank_txns_per_account"])):
            seq += 1
            amount = round(random.uniform(50_000, 20_000_000), 2)
            balance = round(balance + random.uniform(-amount, amount), 2)
            bank_txns.append((f"BTXN{yyyymmdd}{seq:06d}", acc_id, date,
                              dt.datetime.combine(date, dt.time(random.randint(6,22), random.randint(0,59))),
                              random.choice(BANK_TXN_TYPES), amount, balance, random.choice(BANK_CHANNELS)))
        if cfg["fast"]["update_balance_on_txn"]:
            bal_updates.append((balance, acc_id))    # cập nhật balance + updated_at
    # ... tương tự card_txns cho card_accts (txn_id CTXN<yyyymmdd>...) ...

    # 3) TIER SLOW — KH mới + đổi segment + repay/NPL (đều set updated_at = date)
    new_custs, new_bank, seg_changes = [], [], []
    for _ in range(random.randint(*cfg["slow"]["new_customers_per_day"])):
        cif = f"CIF{next_cif:06d}"; next_cif += 1
        # ... build customer tuple + 1 CURRENT account ...
    for cid, seg in customers:
        if random.random() < cfg["slow"]["customer_change_rate"]:
            seg_changes.append((random.choice([s for s in ["MASS","AFFLUENT","PREMIER"] if s != seg]), date, cid))
    # repay/NPL: lặp loans theo repayment_rate / npl_transition_rate -> UPDATE outstanding/npl_status + updated_at

    # 4) GHI — insert chống trùng (idempotent khi DAG retry cùng ngày)
    #   Postgres:
    pgc.executemany("INSERT INTO card_transactions (...) VALUES (...) ON CONFLICT (txn_id) DO NOTHING", card_txns)
    pgc.executemany("INSERT INTO customers (...) VALUES (...) ON CONFLICT (customer_id) DO NOTHING", new_custs)
    pgc.executemany("UPDATE customers SET customer_segment=%s, updated_at=%s WHERE customer_id=%s", seg_changes)
    #   Oracle: executemany(..., batcherrors=True) -> bỏ qua ORA-00001 (duplicate)
    orac.executemany("INSERT INTO bank_transactions (...) VALUES (:1,...)", bank_txns, batcherrors=True)
    orac.executemany("UPDATE bank_accounts SET balance=:1, updated_at=SYSTIMESTAMP WHERE account_id=:2", bal_updates)
    # ... new_bank accounts, loan repay/NPL updates ...

    pg.commit(); ora.commit()
    print(f"[daily {date}] bank_txns={len(bank_txns)} card_txns={len(card_txns)} "
          f"new_cust={len(new_custs)} seg_changes={len(seg_changes)}")
```

> **Điểm then chốt:**
> - `updated_at = date` (KH/account đổi) và `created_at`/`txn_date = date` (txn) → incremental
>   của Bronze (lọc `> last_run_time`) bắt được. **Phải đảm bảo mốc thời gian = ngày mô phỏng**,
>   không dùng `NOW()` (để backfill đúng theo `{{ ds }}`). Cân nhắc dùng cột timestamp đầy đủ
>   `date + giờ` để so sánh `>` chính xác.
> - txn_id tiền tố ngày + `ON CONFLICT DO NOTHING` / `batcherrors=True` = **idempotent**.
> - seq reset mỗi ngày nhưng prefix ngày khác nhau → không đụng ID ngày khác.

---

## Bước 4 — `docker/simulator/Dockerfile` + service

```dockerfile
# docker/simulator/Dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir psycopg2-binary oracledb pyyaml python-dotenv
WORKDIR /app
# scripts + config mount qua volume (không COPY) để sửa nóng
CMD ["sleep", "infinity"]
```

Thêm vào `docker-compose.yml`:
```yaml
  data-simulator:
    build: ./docker/simulator
    image: lakehouse-simulator
    container_name: lakehouse-data-simulator
    env_file: .env
    working_dir: /app
    volumes:
      - ./scripts:/app/scripts
      - ./config:/app/config
    command: ["sleep", "infinity"]
    depends_on:
      - postgres
      - oracle-xe
```

> Theo đúng pattern dự án: Airflow KHÔNG làm việc nặng, chỉ `docker exec` vào container
> chuyên dụng đang chạy (giống spark-master). KHÔNG nhồi oracledb vào image Airflow.
> Container nối DB bằng **service name** (`postgres`/`oracle-xe`) vì cùng mạng compose default.

---

## Bước 5 — `dags/dag_daily_simulation.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK = "lakehouse-spark-master"
SIM   = "lakehouse-data-simulator"
PKGS = ("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,org.postgresql:postgresql:42.7.3")

def spark_job(layer, job):
    return (f"docker exec {SPARK} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 --packages '{PKGS}' "
            f"/opt/spark/jobs/{layer}/{job}")

default_args = {"owner":"lakehouse","retries":2,"retry_delay":timedelta(minutes=5),"email_on_failure":False}

with DAG("daily_simulation", default_args=default_args,
         description="Simulate nguon -> bronze incremental -> silver -> gold (daily)",
         start_date=datetime(2026,6,1),          # gần đây -> catchup nhẹ
         schedule_interval="0 1 * * *",           # 1 AM
         catchup=True, max_active_runs=1,         # tuần tự, đúng thứ tự ngày
         tags=["daily","simulation","incremental"]) as dag:

    simulate = BashOperator(task_id="simulate",
        bash_command=f"docker exec {SIM} python /app/scripts/generate_data.py --mode daily --date {{{{ ds }}}}")
    bronze   = BashOperator(task_id="bronze_incremental", bash_command=spark_job("bronze","incremental_load.py"))
    silver   = BashOperator(task_id="silver",  bash_command=spark_job("silver","run_silver.py"))
    gold     = BashOperator(task_id="gold",    bash_command=spark_job("gold","run_gold.py"))
    verify   = BashOperator(task_id="verify",  bash_command=spark_job("gold","verify_gold.py"))

    simulate >> bronze >> silver >> gold >> verify
```

> `{{{{ ds }}}}` trong f-string → render thành `{{ ds }}` cho Jinja Airflow = execution_date
> (ngày mô phỏng). `start_date` sớm hơn → nhiều lịch sử hơn nhưng nhiều pipeline run hơn.

---

## Bước 6 — Test (PowerShell, KHÔNG Git Bash)

```powershell
# 0) Bootstrap KHÔNG hỏng (host) — set ENV host
$env:PG_HOST="localhost"; $env:ORA_DSN="localhost:1521/XEPDB1"
venv/Scripts/python.exe scripts/generate_data.py --mode bootstrap

# 1) Build + up service simulator
docker compose build data-simulator
docker compose up -d data-simulator

# 2) Daily 1 ngày thủ công (trong container -> service name)
docker exec lakehouse-data-simulator python /app/scripts/generate_data.py --mode daily --date 2026-06-02
#   Kiểm tra nguồn:
docker exec lakehouse-postgres psql -U postgres -d banking -c "SELECT count(*) FROM card_transactions WHERE txn_date='2026-06-02'"
docker exec lakehouse-oracle sqlplus -s corebanking/oracle123@//localhost/XEPDB1 "@/dev/stdin" <<< "SELECT COUNT(*) FROM bank_transactions WHERE txn_date=DATE '2026-06-02';"

# 3) Đẩy qua pipeline thủ công ($PKGS như Phase 1-3)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/bronze/incremental_load.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/silver/run_silver.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/run_gold.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/verify_gold.py

# 4) Idempotency: chạy lại cùng ngày -> count txn KHÔNG đổi
docker exec lakehouse-data-simulator python /app/scripts/generate_data.py --mode daily --date 2026-06-02

# 5) DAG thật
docker exec lakehouse-airflow-scheduler airflow dags pause bronze_ingestion
docker exec lakehouse-airflow-scheduler airflow dags pause silver_transform
docker exec lakehouse-airflow-scheduler airflow dags pause gold_mart
docker exec lakehouse-airflow-scheduler airflow dags unpause daily_simulation
docker exec lakehouse-airflow-scheduler airflow dags trigger daily_simulation
```

### Kỳ vọng khi xong
- [ ] Bootstrap host vẫn nạp 800 KH (không hồi quy)
- [ ] `data-simulator` container chạy, `--mode daily` nạp txn mới + KH mới + đổi segment
- [ ] `bronze.audit_log` cho thấy incremental upsert **chỉ N dòng mới** (không full)
- [ ] `dim_customer` sinh **version SCD2 mới** cho KH đổi segment
- [ ] mart: `recency_days` KH vừa giao dịch giảm; KH mới xuất hiện; có partition `snapshot_date` theo ngày
- [ ] Idempotency: rerun cùng `--date` → count txn không đổi
- [ ] DAG `daily_simulation` mọi task SUCCESS; catchup tạo vài snapshot_date liên tiếp

---

## Ghi chú vận hành
- DAG `daily_simulation` là **driver daily duy nhất**. Pause schedule của `bronze_ingestion`/
  `silver_transform`/`gold_mart` để tránh chạy đôi (chung `bronze.audit_log` → `last_run_time`;
  2 DAG cùng ghi làm cửa sổ incremental lệch). Giữ chúng cho chạy tay/bootstrap.
- Khoảng trống lịch sử 1–5/2026 để trống mặc định (recency lớn dần — chấp nhận). Muốn lấp:
  vòng for gọi `--mode daily` qua dải ngày trước khi bật DAG.
- `agg_customer_holdings` = trạng thái hiện tại (SCD1, không point-in-time); `agg_customer_txn_12m`
  cửa sổ `(as_of-12m, as_of]` đúng theo ngày. Nuance chấp nhận được cho snapshot daily.

---

## Lỗi có thể gặp (dự đoán)
| Lỗi | Nguyên nhân | Xử lý |
|---|---|---|
| simulator không nối được DB | dùng `localhost` trong container | để mặc định ENV (service name `postgres`/`oracle-xe`); chỉ host mới set localhost |
| incremental không bắt dòng mới | mốc thời gian set `NOW()` ≠ `{{ ds }}`, hoặc tz lệch | set timestamp = `date` (ngày mô phỏng), so khớp cơ chế `_fmt` trong incremental_load |
| txn nhân đôi khi retry | thiếu chống trùng | Postgres `ON CONFLICT DO NOTHING`; Oracle `batcherrors=True` |
| catchup chạy quá nhiều ngày | `start_date` quá sớm | đặt `start_date` gần (vd 2026-06-01); `max_active_runs=1` |
| oracledb thiếu trong container | chưa cài trong Dockerfile | `pip install oracledb` trong `docker/simulator/Dockerfile` |
| `agg_customer_holdings` overwrite mỗi run nặng | bảng full 800+ mỗi ngày | chấp nhận (nhỏ); tối ưu để Phase 4 |

---

## Sau khi hoàn thành
- Cập nhật `memory/` (tạo `daily_simulation_done.md` hoặc ghi chú): kết quả thật + điểm khác draft.
- Tiếp tục **Phase 4 — Governance & Ops** (PII masking, Iceberg maintenance + dọn orphan
  bronze/silver cũ, Time Travel, Schema Evolution, DAG maintenance).
```
