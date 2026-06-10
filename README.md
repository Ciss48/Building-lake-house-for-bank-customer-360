# 🏦 Lakehouse Customer 360 — Ngân hàng bán lẻ

> Hệ thống **Lakehouse end-to-end** hợp nhất dữ liệu Core Banking + CRM/Thẻ, dựng bảng
> **Customer 360** với 38 KPI mỗi khách hàng, tự động **phân khúc RFM** và **gợi ý cross-sell**,
> đạt chuẩn **governance** (che PII, time travel, bảo trì) — triển khai 100% on-premise bằng Docker.

Kiến trúc **Medallion 3 lớp** (Bronze → Silver → Gold) trên **Apache Iceberg + MinIO**, điều phối
bằng **Airflow**, truy vấn qua **Trino**.

---

## 📌 Bài toán nghiệp vụ

Ngân hàng bán lẻ có dữ liệu khách hàng **nằm rải rác** ở 2 hệ thống tách biệt:
- **Core Banking (Oracle)** — tài khoản, khoản vay, giao dịch, chi nhánh, sản phẩm
- **CRM + Thẻ (PostgreSQL)** — thông tin khách hàng, tài khoản thẻ, giao dịch thẻ

Hệ quả: **không có cái nhìn 360° về khách hàng** → không biết ai nên mời vay, ai sắp rời bỏ, ai
đáng chăm sóc. Marketing bắn campaign đại trà → lãng phí ngân sách, trải nghiệm kém.

**Giải pháp:** một Lakehouse hợp nhất 2 nguồn, sinh ra bảng `mart_customer_360` (1 dòng/khách)
để Marketing/Sales/BI **tự phục vụ** mà không đụng tới hệ thống production.

---

## 💰 Giá trị nghiệp vụ (Business Value)

| Thành phần kỹ thuật | Giá trị kinh doanh |
|---|---|
| `mart_customer_360` — 1 dòng/khách, 38 KPI | Marketing/Sales **tự lấy số**, không cần xin IT trích xuất |
| **Phân khúc RFM** (Champions, At_Risk, Hibernating…) | Nhắm đúng đối tượng: chăm Champions, kéo lại At_Risk → **tăng giữ chân** |
| **Cross-sell flags** (vay / thẻ / đầu tư) | Ra **danh sách khách đủ điều kiện** mời sản phẩm → **tăng doanh thu/khách** |
| **Cờ rủi ro** (NPL, days_past_due) | Cảnh báo sớm nợ xấu phục vụ quản trị rủi ro |
| **PII masking** | Tuân thủ bảo mật ngân hàng — dùng data mà không lộ thông tin nhạy cảm |
| **Time Travel + Audit log** | Phục vụ kiểm toán, rollback an toàn khi pipeline lỗi |

**Kết quả mẫu trên ~800 khách hàng:** phân loại thành **7 nhóm RFM**; xác định **~274 khách**
đủ điều kiện mời vay, **~400 khách** mời mở thẻ, **~87 khách** mời đầu tư; gắn cờ **~19 khoản nợ
xấu (NPL)**.

---

## 🏗️ Kiến trúc tổng thể

```
Nguồn dữ liệu
├── Oracle XE      → Core Banking: accounts, loans, transactions, branches, products
└── PostgreSQL     → CRM + Card: customers, card_accounts, card_transactions
        │
        │  Spark JDBC (full snapshot + incremental theo updated_at)
        ▼
┌──────────────────────────────────────────────────────────────┐
│   LAKEHOUSE — MinIO (S3) + Apache Iceberg + Nessie catalog    │
│                                                              │
│   🥉 BRONZE   Raw ingestion, giữ nguyên nguồn, + audit log    │
│   🥈 SILVER   SCD Type 2 (dim_customer) + SCD1 + Fact sạch    │
│   🥇 GOLD     mart_customer_360 (38 KPI) + RFM + cross-sell   │
│              + PII masking                                    │
└──────────────────────────────────────────────────────────────┘
        │
        │  SQL
        ▼
   Trino (query engine)  →  Dashboard / Marketing campaign / Sales

Vận hành:  Apache Airflow (orchestrate)  ·  Docker Compose (on-prem)
```

---

## 🛠️ Tech stack

| Lớp | Công nghệ | Vai trò |
|---|---|---|
| Nguồn | Oracle XE 21c | Core Banking |
| Nguồn | PostgreSQL 15 | CRM + Thẻ |
| Object storage | MinIO | S3-compatible, lưu Parquet |
| Table format | Apache Iceberg 1.5.2 | ACID, Time Travel, Schema Evolution |
| Catalog | Nessie 0.79 (ROCKSDB) | Iceberg catalog bền qua restart |
| Compute | Apache Spark 3.5.1 | ETL engine (batch) |
| Query | Trino 446 | SQL federated query |
| Orchestration | Apache Airflow 2.9 | Pipeline scheduling, retry |
| Hạ tầng | Docker Compose | Triển khai on-premise |

---

## 🥉🥈🥇 Ba lớp Medallion

### Bronze — Raw ingestion
- 8 bảng Iceberg sao y nguồn (Oracle + Postgres), partition theo `ingestion_date`.
- 2 chế độ: **full snapshot** (overwrite, idempotent) + **incremental** (theo `updated_at`, MERGE upsert).
- **Audit log** ghi mỗi lần chạy: số dòng, trạng thái, thời điểm → truy vết được.

### Silver — Dimensional model + cleansing
- **SCD Type 2** cho `dim_customer`: lưu **lịch sử thay đổi** segment/địa chỉ/KYC theo thời gian
  (mỗi lần đổi sinh version mới, `is_current` + `effective_from/to`).
- **SCD Type 1** cho `dim_account`, `dim_card_account`, `dim_product`, `dim_branch`, `dim_loan`.
- **Fact tables** giao dịch đã cleansing, partition theo ngày.
- YAML-driven: thêm/sửa bảng chỉ cần sửa `config/silver_tables.yaml`.

### Gold — mart_customer_360
- 2 bảng tổng hợp trung gian (holdings + giao dịch 12 tháng) → `mart_customer_360` (1 dòng/khách).
- **RFM** (Recency–Frequency–Monetary) bằng `NTILE(5)` → 7 phân khúc.
- **Cross-sell flags** theo rule cấu hình trong `config/gold_tables.yaml`.
- **PII masking** áp ngay trong lúc build (xem mục Governance).

---

## 📊 38 KPI trong `mart_customer_360`

| Nhóm | KPI |
|---|---|
| Identity | customer_id, age, customer_segment, kyc_status, city, province |
| Product holding | total_product_count, has_savings, has_current, has_credit_card, has_loan |
| Balance | total_deposit_balance, total_loan_outstanding, credit_utilization_rate, net_asset_value |
| Transaction | txn_count_12m, txn_amount_12m, avg_monthly_spend, top_spend_category, digital_txn_ratio, last_txn_date, active_months_12m |
| RFM | recency_days, frequency_12m, monetary_12m, r/f/m_score, rfm_score, rfm_segment |
| Risk | has_npl_loan, days_past_due |
| Cross-sell | cross_sell_loan_flag, cross_sell_card_flag, cross_sell_invest_flag, tenure_days |
| PII (đã che) | full_name (đã che), phone_masked, email_masked |

---

## 🔐 Governance & Ops

### PII Masking — che thông tin cá nhân
Áp **ngay trong job dựng Gold** (`build_mart_360.py`) → mọi lần pipeline chạy đều tự động che,
không có đường vòng. Silver giữ dữ liệu thật (nội bộ); Gold (nơi nhiều người query) **sạch PII**.

| Cột | Kỹ thuật | Ví dụ |
|---|---|---|
| `full_name` | Redaction giữ họ | `Nguyễn Thị An` → `Nguyễn ****` |
| `phone_masked` | Giữ 3 đầu + 3 cuối | `0941234467` → `094****467` |
| `email_masked` | Giữ 1 ký tự + domain | `user5@example.com` → `u***@example.com` |
| `id_number` | Hash SHA-256 (bất khả nghịch) | helper sẵn dùng |

### Schema Evolution
Thêm cột `phone_masked`/`email_masked` bằng `ALTER TABLE ADD COLUMN` — **không rewrite** dữ liệu cũ.
Bằng chứng: query cột mới ở snapshot cũ → báo `UNRESOLVED_COLUMN` (Iceberg không đụng file cũ).

### Time Travel
Mỗi lần ghi tạo 1 snapshot. Query trạng thái quá khứ bằng `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`
→ phục vụ **kiểm toán, rollback, tái dựng báo cáo**.

### Maintenance (bảo trì)
Job `maintain_tables.py` chạy **compaction** (`rewrite_data_files` gom file Parquet nhỏ +
`rewrite_manifests` gom metadata) → bảng nhanh gọn. Tự động hoá bằng DAG `maintenance` (hàng tuần).

> 📎 Ghi chú thực chiến: Nessie đặt `gc.enabled=false` (quản version cấp catalog) nên
> `expire_snapshots`/`remove_orphan_files` không chạy per-bảng — phải dùng `nessie-gc` cấp catalog.

---

## 🚀 Cách chạy (How to run)

### Yêu cầu
- Docker Desktop (Windows/Mac/Linux), tối thiểu ~12GB RAM cho stack.
- Python 3.12 (chỉ để chạy script sinh data trên host).

### 1. Khởi động stack
```bash
cp .env.example .env      # điền credentials (xem ghi chú bên dưới)
docker compose up -d      # 10 container: oracle, postgres, minio, nessie, spark x2, trino, airflow x3, simulator
```

### 2. Sinh dữ liệu mẫu (~800 khách hàng, 18 tháng giao dịch)
```bash
python -m venv venv && venv/Scripts/activate
pip install -r requirements.txt
python scripts/generate_data.py
```

### 3. Chạy pipeline (PowerShell — xem cheat sheet đầy đủ trong `memory/`)
```powershell
$PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,..."   # xem memory/phase1_done.md
# Bronze
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... bronze/create_bronze_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... bronze/full_snapshot.py
# Silver
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... silver/create_silver_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... silver/run_silver.py
# Gold
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... gold/create_gold_tables.py
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit ... gold/run_gold.py
```

### 4. Hoặc chạy tự động qua Airflow
- UI: http://localhost:8080 (admin/admin). Trigger DAG `bronze_ingestion` → `silver_transform`
  → `gold_mart`. DAG `maintenance` chạy bảo trì hàng tuần.

### 5. Truy vấn qua Trino
```bash
docker exec lakehouse-trino trino --execute "SELECT rfm_segment, count(*) FROM iceberg.gold.mart_customer_360 GROUP BY rfm_segment"
```
Hoặc kết nối **DBeaver**: driver Trino, host `localhost`, port `8088`.

---

## 📁 Cấu trúc dự án

```
lakehouse-customer360/
├── src/
│   ├── bronze/        # Ingestion (full snapshot + incremental)
│   ├── silver/        # SCD1/SCD2 + fact + cleansing
│   ├── gold/          # mart_customer_360 + RFM + cross-sell
│   ├── maintenance/   # Compaction Iceberg
│   ├── governance/    # PII masking, schema evolution, time travel
│   └── common/        # SparkSession, masking, audit logger
├── dags/              # Airflow DAGs (bronze, silver, gold, maintenance)
├── sql/               # Saved query (gold marts, governance checks)
├── config/            # YAML cấu hình mỗi layer (YAML-driven)
├── scripts/           # init SQL + generate_data.py
├── docker/            # Dockerfile spark/airflow/trino/simulator
├── memory/            # 📚 Nhật ký từng phase (quyết định, lỗi, cách fix)
├── docker-compose.yml
└── README.md
```

> 📚 Thư mục `memory/` lưu **nhật ký chi tiết từng phase** — quyết định thiết kế, các lỗi gặp phải
> và cách khắc phục thực tế (ví dụ: fix SCD2 versioning, gotcha Nessie catalog, PII rò rỉ ở partition cũ).

---

## 🧭 Lộ trình triển khai

| Phase | Nội dung | Trạng thái |
|---|---|---|
| 0 | Hạ tầng (Oracle, Postgres, MinIO, Nessie, Spark) | ✅ |
| 1 | Bronze ingestion + audit + Airflow DAG | ✅ |
| 2 | Silver (SCD1/SCD2 + fact) + Trino | ✅ |
| 3 | Gold (mart_customer_360 + RFM + cross-sell) | ✅ |
| 4 | Governance (PII masking, time travel, maintenance) | ✅ |

---

## 👤 Tác giả
Dự án portfolio Data Engineering — Lakehouse cho ngân hàng bán lẻ.
