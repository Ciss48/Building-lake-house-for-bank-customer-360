# Lakehouse Customer 360 — Project Plan

## Tổng quan

Xây dựng hệ thống Lakehouse end-to-end phục vụ bài toán Customer 360 và cross-sell trong ngân hàng bán lẻ.
Kiến trúc Medallion Architecture 3 lớp (Bronze → Silver → Gold) trên nền Apache Iceberg + MinIO, triển khai hoàn toàn on-premise bằng Docker Compose.

---

## Mục tiêu nghiệp vụ

- Xây dựng bảng `mart_customer_360` với 25+ KPI, mỗi khách hàng 1 row
- Tự động phân loại khách hàng theo RFM segment phục vụ marketing campaign
- Lưu toàn bộ lịch sử thay đổi customer / account / product / branch theo thời gian (SCD Type 2)
- Truy vấn trực tiếp qua Trino mà không ảnh hưởng production

---

## Kiến trúc tổng thể

```
Nguồn dữ liệu
├── Oracle XE (Docker)       → Core Banking: accounts, loans, transactions, branches, products
└── PostgreSQL (Docker)      → CRM + Card: customers, card_accounts, card_transactions

        ↓ Spark JDBC (full snapshot + incremental load)

Lakehouse — MinIO (S3-compatible) + Apache Iceberg
├── Bronze   → Raw ingestion, giữ nguyên format nguồn, partition by ingestion_date
├── Silver   → SCD Type 1 & 2, cleansing, dimensional model
└── Gold     → mart_customer_360, 25+ KPI, RFM segment, cross-sell flags

        ↓ SQL query

Trino (Query Engine)
├── Customer 360 dashboard
├── RFM segment → Marketing campaign
└── Cross-sell flags → Sales team

Vận hành
├── Apache Airflow   → Orchestrate toàn bộ pipeline (schedule, retry, alert)
├── Docker Compose   → Toàn bộ stack chạy on-premise
└── Git              → Version control, CI/CD
```

---

## Tech stack

| Layer | Technology | Vai trò |
|---|---|---|
| Source | Oracle XE 21c | Core Banking (accounts, loans) |
| Source | PostgreSQL 15 | CRM + Card system |
| Object Storage | MinIO | S3-compatible, lưu Parquet files |
| Table Format | Apache Iceberg | ACID, Time Travel, Schema Evolution |
| Catalog | Nessie | REST Iceberg catalog |
| Compute | Apache Spark 3.5 | ETL engine (batch only) |
| Query | Trino 446 | SQL federated query |
| Orchestration | Apache Airflow 2.9 | Pipeline scheduling |
| Notebook | JupyterLab | Development, exploration |
| GUI | DBeaver | Kết nối PostgreSQL, Oracle, Trino |
| Infrastructure | Docker Compose | On-premise deployment |

---

## Cấu trúc thư mục

```
lakehouse-customer360/
├── docker/
│   └── trino/
│       └── etc/
│           ├── config.properties
│           ├── jvm.config
│           ├── node.properties
│           └── catalog/
│               └── iceberg.properties
├── src/
│   ├── bronze/          → Spark jobs ingestion
│   ├── silver/          → Spark jobs SCD1, SCD2, cleansing
│   ├── gold/            → Spark jobs KPI, RFM
│   └── common/          → Shared utils (spark_session, logger...)
├── dags/                → Airflow DAGs
├── sql/
│   ├── bronze/          → SQL templates Bronze
│   ├── silver/          → SQL templates Silver
│   └── gold/            → SQL templates Gold
├── config/              → YAML config cho từng pipeline
├── scripts/
│   ├── init-postgres.sql
│   ├── init-oracle.sql
│   └── generate_data.py → Script sinh dữ liệu mẫu
├── notebooks/           → JupyterLab notebooks phát triển
├── tests/               → Unit tests
├── .env                 → Credentials (KHÔNG commit git)
├── .env.example         → Template credentials
├── .gitignore
├── docker-compose.yml
├── plan.md              → File này
└── tasks/
    ├── task_setup_databases.md
    ├── task_bronze_ingestion.md     → tạo sau
    ├── task_silver_scd.md           → tạo sau
    ├── task_gold_mart.md            → tạo sau
    ├── task_airflow_orchestration.md → tạo sau
    └── task_governance_ops.md       → tạo sau
```

---

## Roadmap theo phase

### Phase 0 — Foundation (Tuần 1)
**Task file:** `tasks/task_setup_databases.md`

- [ ] Cài DBeaver, kết nối được PostgreSQL và Oracle
- [ ] Chạy PostgreSQL trong Docker, seed data CRM + Card
- [ ] Chạy Oracle XE trong Docker, seed data Core Banking
- [ ] Chạy MinIO, tạo bucket `lakehouse`
- [ ] Chạy Nessie (Iceberg REST catalog)
- [ ] Chạy Spark cluster (master + worker)
- [ ] Test kết nối Spark → PostgreSQL và Oracle qua JDBC
- [ ] Test Spark ghi Parquet lên MinIO

**Output:** Toàn bộ stack chạy được, data seed sẵn, DBeaver kết nối thành công

---

### Phase 1 — Bronze Layer (Tuần 2–3)
**Task file:** `tasks/task_bronze_ingestion.md` *(tạo sau)*

- [ ] Viết Spark job full snapshot: Oracle → Bronze Iceberg
- [ ] Viết Spark job full snapshot: PostgreSQL → Bronze Iceberg
- [ ] Viết Spark job incremental load (theo `updated_at`)
- [ ] Thiết kế Audit Flag Table (track trạng thái mỗi lần chạy)
- [ ] Viết Airflow DAG Bronze với retry + idempotency
- [ ] Test chạy lại 2 lần → không duplicate

**Output:** DAG chạy tự động, data vào Bronze Iceberg, audit log đầy đủ

---

### Phase 2 — Silver Layer (Tuần 4–5)
**Task file:** `tasks/task_silver_scd.md` *(tạo sau)*

- [ ] SCD Type 2: `dim_customer` (track đổi segment, địa chỉ)
- [ ] SCD Type 1: `dim_account`, `dim_product`, `dim_branch`
- [ ] Cleansing: xử lý null, duplicate, late arrival data
- [ ] YAML-driven SQL transform template
- [ ] Viết Airflow DAG Bronze → Silver

**Output:** Dim tables chuẩn SCD, lịch sử thay đổi đầy đủ, queryable qua Trino

---

### Phase 3 — Gold Layer (Tuần 6–7)
**Task file:** `tasks/task_gold_mart.md` *(tạo sau)*

- [ ] Star schema Gold layer
- [ ] Tính 25+ KPI cho `mart_customer_360`
- [ ] RFM segment (Recency, Frequency, Monetary)
- [ ] Cross-sell flags (loan, card, investment)
- [ ] Materialized views cho BI/ML
- [ ] Viết Airflow DAG Silver → Gold
- [ ] Setup Trino + DBeaver query Gold layer

**Output:** `mart_customer_360` — 1 row/KH — 25+ KPI — RFM segment — query qua Trino

---

### Phase 4 — Governance & Ops (Tuần 8–9)
**Task file:** `tasks/task_governance_ops.md` *(tạo sau)*

- [ ] PII masking: email, phone, full_name trong Silver/Gold
- [ ] Iceberg maintenance: compaction, vacuum, small files
- [ ] Airflow DAG Maintenance chạy định kỳ
- [ ] Time Travel demo: query dữ liệu tại thời điểm cụ thể
- [ ] Schema Evolution demo: thêm cột không cần rewrite data

**Output:** PII ẩn, bảng tối ưu, governance đúng chuẩn production

---

### Final — Portfolio (Tuần 10)
- [ ] Push code lên GitHub public
- [ ] Viết README chuyên nghiệp (Architecture, Tech stack, How to run, Business value)
- [ ] Demo end-to-end pipeline
- [ ] Business value narrative cho CV/interview

---

## Data model nguồn

### Oracle XE — Core Banking
- `branches` — chi nhánh ngân hàng
- `products` — sản phẩm (tiết kiệm, vay, thanh toán)
- `bank_accounts` — tài khoản ngân hàng (join `customer_id` từ Postgres)
- `loans` — khoản vay
- `bank_transactions` — giao dịch ngân hàng

### PostgreSQL — CRM + Card
- `customers` — thông tin khách hàng (master)
- `card_accounts` — tài khoản thẻ
- `card_transactions` — giao dịch thẻ

> `customer_id` là khóa liên kết xuyên suốt 2 hệ thống.
> Oracle chỉ lưu `customer_id` dạng FK, không có bảng customers riêng.

---

## 25+ KPI trong mart_customer_360

| Nhóm | KPI |
|---|---|
| Identity | customer_id, age, customer_segment, kyc_status, city |
| Product holding | total_product_count, has_savings, has_current, has_credit_card, has_loan |
| Balance | total_deposit_balance, total_loan_outstanding, credit_utilization_rate, net_asset_value |
| Transaction | txn_count_12m, txn_amount_12m, avg_monthly_spend, top_spend_category, digital_txn_ratio, last_txn_date, active_months_12m |
| RFM | recency_days, frequency_12m, monetary_12m, rfm_score, rfm_segment |
| Risk | has_npl_loan, days_past_due |
| Cross-sell | cross_sell_loan_flag, cross_sell_card_flag, cross_sell_invest_flag, tenure_days |

---

## Ghi chú quan trọng

- Mọi credential chỉ lưu trong `.env`, không hardcode vào code
- Mọi Spark job chạy qua `spark-submit`, không chạy trong notebook
- Mọi pipeline production chạy qua Airflow DAG, không chạy tay
- Iceberg table dùng Nessie làm catalog, không dùng Hive Metastore
- Partition strategy: Bronze theo `ingestion_date`, Silver/Gold theo `snapshot_date`