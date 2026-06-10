# Phase 4 Done — Governance & Ops (PII masking + Maintenance + Time Travel + Schema Evolution)

## Ngày hoàn thành: 2026-06-09

> Đánh số theo `plan_overall.md`: Foundation=0, Bronze=1, Silver=2, Gold=3, **Governance=4**.
> Đây là phase cuối về kỹ thuật. Còn lại: Final (README + push GitHub + business narrative).

---

## Mục tiêu phase này
Bọc lớp governance + vận hành quanh lakehouse: che PII ở Gold, bảo trì bảng Iceberg
(compaction), và 2 demo siêu năng lực Iceberg (Time Travel + Schema Evolution). Không thêm KPI.

Quyết định scope (user chốt): **mask tại chỗ partition cũ, GIỮ lịch sử** (không xoá, không để
nguyên) — xem điểm #2 bên dưới.

---

## File code tạo ra / sửa
| File | Vai trò |
|---|---|
| `src/common/masking.py` | **MỚI**: SQL expr builders — `hash_col` (SHA-256), `mask_name`, `mask_phone`, `mask_email` |
| `src/governance/apply_schema_evolution.py` | **MỚI**: ALTER TABLE mart ADD COLUMN phone_masked/email_masked (idempotent, KHÔNG rewrite) |
| `src/gold/build_mart_360.py` | **SỬA**: import masking; cust CTE thêm phone/email; SELECT cuối mask full_name + thêm phone_masked/email_masked |
| `src/governance/mask_legacy_partitions.py` | **MỚI (ngoài draft)**: UPDATE tại chỗ full_name các partition snapshot_date cũ còn PII thô. Idempotent. Xem #2 |
| `src/maintenance/maintain_tables.py` | **MỚI**: compaction qua `rewrite_data_files` + `rewrite_manifests` (2 procedure Nessie hỗ trợ) |
| `src/governance/demo_time_travel.py` | **MỚI**: liệt kê snapshots + VERSION AS OF + so sánh PII trước/sau + minh chứng schema evolution |
| `dags/dag_maintenance.py` | **MỚI**: DAG `maintenance` weekly (Chủ nhật 5 AM), BashOperator docker exec |
| `sql/governance/pii_check.sql`, `time_travel.sql` | **MỚI**: saved query Trino kiểm tra PII + demo time travel |

**Tái dùng (không viết lại):** `get_spark_session`, `align_to_table`, `SPARK_PACKAGES`,
`AuditLogger`, pattern DAG BashOperator + docker exec spark-submit.

---

## Kỹ thuật masking (đã test thật qua Trino)
| Cột | Kỹ thuật | Ví dụ |
|---|---|---|
| `full_name` | redaction giữ họ | `Ho Thi An` → `Ho ****` |
| `phone_masked` | giữ 3 đầu + 3 cuối | `0941234467` → `094****467` |
| `email_masked` | giữ 1 ký tự + domain | `user5@example.com` → `u***@example.com` |
| `id_number` | `hash_col` SHA-256 (BẤT khả nghịch) | — (id_number KHÔNG có trong mart → không populate; helper sẵn nếu cần) |

- Masked ở **Gold (consumer)**, Silver giữ raw (internal source-of-truth). Trino+Nessie không
  hỗ trợ CREATE VIEW (biết từ Phase 3) → mask thẳng vào bảng mart.
- `build_mart_360` đọc `phone`/`email` từ `dim_customer` nhưng mart **KHÔNG** có cột phone/email
  thô — `align_to_table` chỉ giữ cột trong schema đích (phone_masked/email_masked). Verify:
  `information_schema.columns ... IN ('phone','email','id_number')` = **0 cột**.

---

## ⚠️ QUAN TRỌNG — phát hiện khi làm thật (khác draft)

### 1. expire_snapshots / remove_orphan_files KHÔNG chạy được với Nessie (đúng như dự đoán)
Lỗi chính xác (đã verify):
```
org.apache.iceberg.exceptions.ValidationException:
Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)
```
Nessie set `gc.enabled=false` trên mọi bảng — vì 1 snapshot có thể được nhiều branch/tag tham
chiếu → Iceberg per-table GC bị **tắt cố ý**. Phương án đúng: **`nessie-gc` tool ở cấp catalog**
(chưa dựng — môi trường học tập, không cần).
- **rewrite_data_files** ✅ SUPPORTED (6/6 bảng)
- **rewrite_manifests** ✅ SUPPORTED (6/6 bảng)
- **expire_snapshots** ❌ UNSUPPORTED (gc.enabled=false)
- **remove_orphan_files** ❌ UNSUPPORTED (gc.enabled=false)
→ `maintain_tables.py` bản production chỉ chạy 2 procedure được hỗ trợ + in NOTE giải thích.

### 2. Mart TÍCH LŨY nhiều partition snapshot_date → partition cũ giữ PII thô (BUG governance)
`build_mart_360` ghi `overwritePartitions()` partition theo `snapshot_date`. `overwritePartitions`
**chỉ ghi đè partition trùng key**. Vì `as_of` (= max txn_date) đã dịch từ `2025-12-31` (Phase 3)
sang `2026-06-02` (data-simulator sinh thêm txn), build mới tạo partition MỚI `2026-06-02` (masked)
nhưng partition cũ `2025-12-31` (800 rows, **PII thô**, build trước khi có masking) **vẫn còn**.
- Hệ quả: `SELECT * FROM mart` (không lọc) = 1603 rows = 800 + 803 ≈ **2 row/KH**.
- **Fix masking:** viết `mask_legacy_partitions.py` — `UPDATE mart SET full_name = mask_name(...)
  WHERE full_name NOT LIKE '% ****'` (idempotent). Sau khi chạy: cả 2 partition đều masked,
  `raw_name_leak = 0`. phone_masked/email_masked ở partition cũ = NULL (cột mới, data cũ chưa có)
  → đồng thời minh chứng schema evolution.
- **User chốt:** mask tại chỗ + GIỮ lịch sử (không xoá partition). Time Travel vẫn xem được PII
  thô ở Iceberg snapshot cũ (trước UPDATE) — đó là "before governance".

### 3. ⚠️ FOLLOW-UP còn lại (chưa fix — cần lưu ý cho Final/consumer)
Mart giờ có 2 partition snapshot_date → **saved query marketing/sales (`vw_*` Phase 3) SELECT không
lọc snapshot_date sẽ double-count**. Consumer nên thêm `WHERE snapshot_date = (SELECT max(...))`.
Hoặc đổi grain mart về current-only. **Chưa làm** — ghi lại để Final xử lý.

---

## Kết quả test end-to-end (đã chạy thật)
```
Schema evolution: ALTER ADD phone_masked/email_masked OK (KHÔNG rewrite data cũ)        ✅
Rebuild mart:     run_gold as_of=2026-06-02 → mart partition moi 803 rows, full masked  ✅
Mask legacy:      UPDATE partition 2025-12-31 → raw_name_leak 800 → 0                    ✅
PII verify Trino: full_name 'Ho ****', phone '094****467', email 'u***@example.com';
                  raw phone/email/id_number columns = 0; email_leak = 0                  ✅
Maintenance:      rewrite_data_files 6/6 OK, rewrite_manifests 6/6 OK;
                  expire_snapshots/remove_orphan_files UNSUPPORTED (gc.enabled=false)    ✅
Time travel:      8 snapshots; VERSION AS OF snapshot đầu = 800 vs hiện tại 1603;
                  full_name snapshot đầu 'Ho Thi An' (thô) vs hiện tại 'Ho ****' (che)   ✅
Schema evo proof: query phone_masked tại snapshot đầu → UNRESOLVED_COLUMN
                  (cột thêm sau ALTER, Iceberg không đụng file cũ)                       ✅
DAG maintenance:  unpause + trigger thật → maintain_tables SUCCESS (return code 0),
                  log có đủ 12 [OK] + SUMMARY + NOTE                                     ✅
```

---

## Bước 7 (dọn orphan MinIO) — KHÔNG làm (có chủ đích)
- `remove_orphan_files` bị Nessie chặn (gc.enabled=false) → không dùng được.
- Dọn tay MinIO thư mục bảng cũ (mồ côi từ store-switch Phase 3) = rủi ro cao xoá nhầm bảng sống,
  và dung lượng không phải vấn đề (môi trường học) → **bỏ qua**. Nếu cần production thật: dùng
  `nessie-gc` tool (cấp catalog) chứ không xoá tay.

---

## Lệnh chạy (cheat sheet) — PowerShell, `$PKGS` như Phase 1/2/3
```powershell
# 1) Schema evolution (1 lần — TRƯỚC khi rebuild mart)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/governance/apply_schema_evolution.py
# 2) Rebuild mart với masking
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/gold/run_gold.py
# 3) Mask các partition snapshot_date cũ còn PII thô (idempotent)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/governance/mask_legacy_partitions.py
# 4) Maintenance (compaction)
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/maintenance/maintain_tables.py
# 5) Demo time travel + schema evolution
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $PKGS /opt/spark/jobs/governance/demo_time_travel.py
```
Airflow: `docker exec lakehouse-airflow-scheduler airflow dags unpause maintenance` rồi
`... airflow dags trigger maintenance`. (Unpause sinh thêm 1 scheduled catch-up run — vô hại.)

Trino verify: `sql/governance/pii_check.sql`, `sql/governance/time_travel.sql`.

> Giữ từ phase trước: chạy qua **PowerShell** (KHÔNG Git Bash); exit 255/NativeCommandError lúc
> shutdown là lành tính — kiểm tra bằng output/audit_log. Nessie ROCKSDB bền qua restart (Phase 3).

---

## Phase tiếp theo — Final (Tuần 10, theo plan_overall.md)
README chuyên nghiệp (Architecture, Tech stack, How to run, Business value) + push GitHub public +
business narrative cho CV/interview. Xử lý nốt follow-up #3 (double-count snapshot_date) nếu muốn
mart sạch tuyệt đối. → Dự án hoàn chỉnh.
