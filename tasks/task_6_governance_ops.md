# Task: Governance & Ops — Phase 4 (PII masking + Iceberg maintenance + Time Travel + Schema Evolution)

## Mục tiêu
Bọc lớp **governance + vận hành** quanh toàn bộ lakehouse: che PII, tối ưu/bảo trì bảng
Iceberg, và 2 demo siêu năng lực Iceberg (Time Travel + Schema Evolution). Không thêm KPI mới —
mục tiêu là đạt **chuẩn production + an toàn dữ liệu + showcase**.

## Trạng thái
- [ ] **CHƯA LÀM** — bản kế hoạch (draft). Chưa code tới khi user cho phép.

> Theo `plan_overall.md`: đây là **Phase 4 — Governance & Ops** (Foundation=0, Bronze=1,
> Silver=2, Gold=3, **Governance=4**). File done: `memory/phase4_done.md`.

---

## 5 mảng công việc + vai trò

| # | Mảng | Tác dụng |
|---|---|---|
| 1 | **PII masking** | Che email/phone/full_name/id_number → tuân thủ bảo mật ngân hàng |
| 2 | **Iceberg maintenance** | Compaction + expire snapshots + dọn orphan → bảng nhanh, gọn |
| 3 | **Time Travel demo** | Query data tại thời điểm quá khứ → audit, rollback, showcase |
| 4 | **Schema Evolution demo** | Thêm cột không rewrite data → hệ thống mở rộng an toàn |
| 5 | **DAG Maintenance** | Tự động chạy bảo trì định kỳ (hàng tuần) |

---

## Quyết định thiết kế đề xuất (user confirm khi review)

1. **Masking ở lớp Gold (consumer), Silver giữ raw (internal).**
   - Lý do: Silver là source-of-truth nội bộ (truy cập hạn chế); Gold là nơi BI/Marketing/Sales
     query → **phải sạch PII**. Trino+Nessie KHÔNG hỗ trợ `CREATE VIEW` (đã biết từ Phase 3) →
     không dùng masked view được → **mask thẳng vào bảng Gold**.
2. **Kỹ thuật masking 2 loại:**
   - **Hash bất khả nghịch** (`SHA-256`) cho `id_number` → token để join/dedup mà không lộ.
   - **Redaction một phần** (hiển thị được) cho name/phone/email:
     `full_name` → `Nguyen ****`; `phone` → `090****901`; `email` → `u***@example.com`.
3. **Gắn masking với Schema Evolution demo:** thêm `phone_masked`, `email_masked` vào
   `mart_customer_360` bằng `ALTER TABLE ADD COLUMN` (chứng minh schema evolution thật — không
   rewrite), rồi `build_mart_360` populate. `full_name` đổi tại chỗ thành masked.
4. **Maintenance qua Iceberg stored procedures** của Nessie catalog (`CALL nessie.system.*`).
   ⚠️ Rủi ro: một số procedure (`expire_snapshots`, `remove_orphan_files`) có thể bị Nessie hạn
   chế (Nessie quản version qua ref/commit). **Cần verify khi implement**; nếu không hỗ trợ →
   ghi rõ + dùng phương án thay thế (Nessie GC tool / dọn tay MinIO).

---

## Checklist tổng
- [ ] Bước 1: `src/common/masking.py` — SQL expression builders (hash + redaction)
- [ ] Bước 2: Schema Evolution — `ALTER TABLE mart ADD COLUMN phone_masked/email_masked`
- [ ] Bước 3: Sửa `build_mart_360.py` — mask full_name + populate phone_masked/email_masked
- [ ] Bước 4: `src/maintenance/maintain_tables.py` — compaction/expire/orphan qua CALL
- [ ] Bước 5: `src/governance/demo_time_travel.py` — snapshots + query AS OF
- [ ] Bước 6: `dags/dag_maintenance.py` — DAG bảo trì hàng tuần
- [ ] Bước 7: Dọn orphan directories cũ (store-switch Phase 3) trên MinIO
- [ ] Bước 8: Test: masking qua Trino → maintenance → time travel → schema evolution → DAG

---

## Cấu trúc file
```
lakehouse-customer360/
├── src/
│   ├── common/
│   │   └── masking.py                ← MỚI: hàm build SQL expr masking (hash + redaction)
│   ├── gold/
│   │   └── build_mart_360.py         ← SỬA: mask full_name + thêm phone_masked/email_masked
│   ├── maintenance/
│   │   └── maintain_tables.py        ← MỚI: rewrite_data_files / expire_snapshots / remove_orphan_files
│   └── governance/
│       ├── apply_schema_evolution.py ← MỚI: ALTER TABLE ADD COLUMN (1 lần, demo + setup masking)
│       └── demo_time_travel.py       ← MỚI: liệt kê snapshots + query AS OF
├── dags/
│   └── dag_maintenance.py            ← MỚI: bảo trì định kỳ (weekly)
└── sql/governance/
    ├── time_travel.sql               ← saved query demo
    └── pii_check.sql                 ← saved query kiểm tra mart không còn PII thô
```

> **Tái dùng:** `get_spark_session`, `SPARK_PACKAGES`, pattern DAG BashOperator docker exec.
> Mart đã partition `snapshot_date` (Phase 3) → có sẵn nhiều snapshot cho Time Travel demo.

---

## Bước 1 — `src/common/masking.py`

SQL expression builders (dùng được cả Spark lẫn Trino — chuẩn ANSI/Spark functions).

```python
# src/common/masking.py
"""Helpers sinh SQL expression che PII. Dung trong build_mart_360 (Spark SQL)."""

def hash_col(col: str) -> str:
    """Hash bat kha nghich (token) cho id_number."""
    return f"sha2(CAST({col} AS STRING), 256)"

def mask_name(col: str) -> str:
    """Nguyen Van An -> 'Nguyen ****' (giu tu dau, che phan con lai)."""
    return (f"CASE WHEN {col} IS NULL THEN NULL "
            f"ELSE concat(split({col}, ' ')[0], ' ****') END")

def mask_phone(col: str) -> str:
    """09xxxxx901 -> 090****901 (giu 3 dau + 3 cuoi)."""
    return (f"CASE WHEN {col} IS NULL OR length({col}) < 6 THEN {col} "
            f"ELSE concat(substr({col},1,3), '****', substr({col}, length({col})-2, 3)) END")

def mask_email(col: str) -> str:
    """user12@example.com -> u***@example.com."""
    return (f"CASE WHEN {col} IS NULL OR instr({col},'@')=0 THEN {col} "
            f"ELSE concat(substr({col},1,1), '***', substr({col}, instr({col},'@'))) END")
```

---

## Bước 2 — Schema Evolution (`src/governance/apply_schema_evolution.py`)

Thêm cột masked vào mart **không rewrite data** — đây chính là demo Schema Evolution.

```python
# src/governance/apply_schema_evolution.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

def main():
    spark = get_spark_session("schema-evolution")
    for col in ["phone_masked STRING", "email_masked STRING"]:
        spark.sql(f"ALTER TABLE nessie.gold.mart_customer_360 ADD COLUMN IF NOT EXISTS {col.split()[0]} {col.split()[1]}")
    print("Schema evolution OK — them phone_masked/email_masked (KHONG rewrite data)")
    spark.sql("DESCRIBE nessie.gold.mart_customer_360").show(60, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
```

> Chạy 1 lần. Sau ALTER, snapshot cũ vẫn query được (cột mới = NULL với data cũ); chỉ build
> mới populate. Đây là bằng chứng Iceberg evolve schema không đụng file cũ.

---

## Bước 3 — Sửa `build_mart_360.py` (apply masking)

Trong CTE `cust`, thêm `phone`, `email` từ `dim_customer`; ở SELECT cuối:
- `full_name` → `mask_name("full_name")`
- thêm `phone_masked` = `mask_phone("phone")`, `email_masked` = `mask_email("email")`
- (id_number không có trong mart; nếu thêm thì `hash_col`)

```python
from common.masking import mask_name, mask_phone, mask_email
# ... trong cust CTE: SELECT ..., phone, email FROM nessie.silver.dim_customer WHERE is_current=true
# ... SELECT cuoi:
#   {mask_name('full_name')} AS full_name,
#   ... ,
#   {mask_phone('phone')}  AS phone_masked,
#   {mask_email('email')}  AS email_masked,
```

> Sau khi ALTER (Bước 2) + sửa build, chạy lại `run_gold.py` → mart có full_name đã che +
> 2 cột masked. `align_to_table` tự nhận 2 cột mới (đã có trong schema sau ALTER).

---

## Bước 4 — `src/maintenance/maintain_tables.py`

Bảo trì từng bảng qua Iceberg stored procedures (Nessie catalog).

```python
# src/maintenance/maintain_tables.py
import sys
from datetime import datetime, timedelta, timezone
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

TABLES = [  # bronze + silver + gold (bo audit_log neu muon)
    "bronze.oracle_bank_transactions", "bronze.pg_card_transactions",
    "silver.fct_bank_transactions", "silver.fct_card_transactions",
    "silver.dim_customer", "gold.mart_customer_360",
]

def main():
    spark = get_spark_session("maintenance")
    older = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    for t in TABLES:
        fq = f"nessie.{t}"
        print(f"== {fq} ==")
        try:
            # 1) Compaction — gop small files
            spark.sql(f"CALL nessie.system.rewrite_data_files(table => '{t}')").show(truncate=False)
            # 2) Rewrite manifests
            spark.sql(f"CALL nessie.system.rewrite_manifests('{t}')").show(truncate=False)
            # 3) Expire snapshots (giu 5 ban gan nhat) — ⚠️ verify Nessie support
            spark.sql(f"CALL nessie.system.expire_snapshots(table => '{t}', "
                      f"older_than => TIMESTAMP '{older}', retain_last => 5)").show(truncate=False)
            # 4) Remove orphan files — ⚠️ verify Nessie support
            spark.sql(f"CALL nessie.system.remove_orphan_files(table => '{t}', "
                      f"older_than => TIMESTAMP '{older}')").show(truncate=False)
        except Exception as e:
            print(f"  ! {fq}: {e}")
    spark.stop()

if __name__ == "__main__":
    main()
```

> ⚠️ **Verify đầu tiên khi implement:** chạy thử `CALL nessie.system.rewrite_data_files` trên 1
> bảng. Nếu procedure nào báo unsupported với Nessie → bỏ procedure đó, ghi vào phase4_done +
> dùng phương án thay thế. `rewrite_data_files`/`rewrite_manifests` thường OK; `expire_snapshots`/
> `remove_orphan_files` là phần rủi ro nhất với Nessie.

---

## Bước 5 — `src/governance/demo_time_travel.py`

```python
# src/governance/demo_time_travel.py
import sys
sys.path.insert(0, "/opt/spark/jobs")
from common.spark_session import get_spark_session

def main():
    spark = get_spark_session("time-travel")
    print("== Lich su snapshot mart_customer_360 ==")
    snaps = spark.sql("""SELECT committed_at, snapshot_id, operation
                         FROM nessie.gold.mart_customer_360.snapshots ORDER BY committed_at""")
    snaps.show(truncate=False)
    rows = snaps.collect()
    if len(rows) >= 2:
        old_id = rows[0]["snapshot_id"]
        print(f"== Row count tai snapshot dau ({old_id}) vs hien tai ==")
        old = spark.sql(f"SELECT count(*) c FROM nessie.gold.mart_customer_360 VERSION AS OF {old_id}").collect()[0]["c"]
        now = spark.table("nessie.gold.mart_customer_360").count()
        print(f"  snapshot dau: {old} rows | hien tai: {now} rows")
    spark.stop()

if __name__ == "__main__":
    main()
```

> Iceberg `VERSION AS OF <snapshot_id>` / `TIMESTAMP AS OF '...'`. Bảng `.snapshots`/`.history`
> là metadata table Iceberg. (Nessie cũng có cú pháp ref `table@main#<hash>` — không bắt buộc cho demo.)

---

## Bước 6 — `dags/dag_maintenance.py`

```python
# dags/dag_maintenance.py — chay weekly (Chu nhat 5 AM)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# ... SPARK_PACKAGES + spark_job() giong dag_gold_mart.py ...
with DAG("maintenance", start_date=datetime(2026,6,1),
         schedule_interval="0 5 * * 0", catchup=False, tags=["maintenance","ops"],
         default_args={"owner":"lakehouse","retries":1,"retry_delay":timedelta(minutes=5)}) as dag:
    maintain = BashOperator(task_id="maintain_tables",
        bash_command=spark_job("maintenance", "maintain_tables.py"))
```

---

## Bước 7 — Dọn orphan directories cũ (store-switch Phase 3)

`remove_orphan_files` chỉ dọn file rác **trong location của bảng hiện tại**. Đống parquet
bronze/silver cũ (mồ côi từ lúc đổi Nessie IN_MEMORY→ROCKSDB) là **thư mục bảng cũ không còn
ai tham chiếu** → procedure KHÔNG bắt. Dọn tay qua MinIO client:
```powershell
# Liet ke + xoa (CAN THAN — xac nhan path truoc khi xoa):
docker exec lakehouse-minio mc ls -r local/lakehouse/warehouse/bronze/
# So sanh voi bang hien tai; xoa dir orphan:
docker exec lakehouse-minio mc rm --recursive --force local/lakehouse/warehouse/<orphan-dir>
```
> ⚠️ Bước nguy hiểm: phải đối chiếu metadata-location của bảng hiện tại (qua
> `nessie.<t>.metadata_log_entries` / Nessie API) trước khi xoá, tránh xoá nhầm bảng đang sống.
> Có thể để cuối, làm cẩn thận hoặc bỏ qua nếu dung lượng không vấn đề.

---

## Bước 8 — Test (PowerShell, `$PKGS` như các phase trước)

```powershell
# Schema evolution (1 lan)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... /opt/spark/jobs/governance/apply_schema_evolution.py
# Rebuild mart voi masking
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... /opt/spark/jobs/gold/run_gold.py
# Maintenance
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... /opt/spark/jobs/maintenance/maintain_tables.py
# Time travel
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... /opt/spark/jobs/governance/demo_time_travel.py
```

Verify qua Trino:
```sql
-- PII da che?
SELECT customer_id, full_name, phone_masked, email_masked FROM iceberg.gold.mart_customer_360 LIMIT 5;
-- KHONG con ten that / email that
-- Snapshot history:
SELECT committed_at, snapshot_id FROM iceberg.gold."mart_customer_360$snapshots" ORDER BY committed_at;
```

### Kỳ vọng khi xong
- [ ] `mart_customer_360`: `full_name` đã che, có `phone_masked`/`email_masked`; KHÔNG còn PII thô
- [ ] Schema evolution: ALTER ADD COLUMN chạy, snapshot cũ vẫn query được (cột mới NULL)
- [ ] Maintenance: rewrite_data_files chạy OK; ghi rõ procedure nào Nessie hỗ trợ/không
- [ ] Time travel: liệt kê ≥2 snapshot, query AS OF snapshot cũ ra số row khác hiện tại
- [ ] DAG `maintenance` trigger thật SUCCESS
- [ ] (tuỳ) orphan dir cũ trên MinIO đã dọn

---

## Lỗi/rủi ro dự đoán
| Rủi ro | Khả năng | Xử lý |
|---|---|---|
| `expire_snapshots`/`remove_orphan_files` unsupported với Nessie | **Cao** | Verify sớm; nếu lỗi → bỏ procedure đó, dùng Nessie GC / dọn tay, ghi phase4_done |
| `align_to_table` chưa thấy cột masked mới | Trung bình | Chạy `apply_schema_evolution.py` TRƯỚC `run_gold.py` |
| Trino đọc cột mới ra NULL ở snapshot cũ | Đúng kỳ vọng | Đó chính là minh chứng schema evolution |
| Xoá nhầm dir bảng sống khi dọn orphan | Cao nếu ẩu | Đối chiếu metadata-location trước; bước 7 optional |
| Mart bị mask rồi, daily_simulation chạy lại ghi đè | Thấp | build_mart đã có masking → mọi lần build đều masked, nhất quán |

---

## Sau khi hoàn thành
- Viết `memory/phase4_done.md`: procedure nào Nessie hỗ trợ thật, kết quả masking/time-travel.
- **Final (Tuần 10):** README chuyên nghiệp + push GitHub + business value narrative cho CV.
  Đây là phase cuối theo `plan_overall.md` → dự án hoàn chỉnh.
```
