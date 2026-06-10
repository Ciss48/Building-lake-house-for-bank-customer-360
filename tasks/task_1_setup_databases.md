# Task: Setup Databases — Phase 0

## Mục tiêu
Dựng toàn bộ hạ tầng nguồn dữ liệu và storage layer.
Kết thúc task này: DBeaver kết nối được cả PostgreSQL lẫn Oracle, MinIO chạy được, Spark đọc được từ cả 2 nguồn.

## Trạng thái
- [ ] Đang thực hiện

---

## Checklist tổng

- [ ] Bước 1: Tạo cấu trúc thư mục và file cấu hình
- [ ] Bước 2: Chạy PostgreSQL → kết nối DBeaver → xác nhận data
- [ ] Bước 3: Chạy Oracle XE → kết nối DBeaver → xác nhận data
- [ ] Bước 4: Chạy MinIO → vào Web UI → tạo bucket
- [ ] Bước 5: Chạy Nessie (Iceberg catalog)
- [ ] Bước 6: Chạy Spark cluster
- [ ] Bước 7: Test Spark đọc PostgreSQL qua JDBC
- [ ] Bước 8: Test Spark đọc Oracle qua JDBC
- [ ] Bước 9: Test Spark ghi Parquet lên MinIO

---

## Bước 1 — Cấu trúc thư mục và file cấu hình

### 1.1 Tạo thư mục (chạy trong PowerShell)

```powershell
mkdir lakehouse-customer360
cd lakehouse-customer360
mkdir tasks, scripts, config, notebooks
mkdir src\bronze, src\silver, src\gold, src\common
mkdir dags
mkdir docker\trino\etc\catalog
mkdir sql\bronze, sql\silver, sql\gold
```

### 1.2 Tạo .gitignore

```gitignore
.env
.env.*
!.env.example
venv/
__pycache__/
*.pyc
.pytest_cache/
.vscode/
*.iml
```

### 1.3 Tạo .env

Copy nội dung sau vào file `.env` (tạo tay bằng Notepad hoặc VSCode):

```env
# MinIO
MINIO_ROOT_USER=<minio-user>
MINIO_ROOT_PASSWORD=<minio-password>
AWS_ACCESS_KEY_ID=<minio-user>
AWS_SECRET_ACCESS_KEY=<minio-password>

# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<postgres-password>
POSTGRES_DB=banking
POSTGRES_AIRFLOW_DB=airflow

# Oracle
ORACLE_PASSWORD=<oracle-password>
ORACLE_USER=corebanking

# Spark
SPARK_MASTER_URL=spark://spark-master:7077

# Iceberg / Nessie
ICEBERG_CATALOG_URI=http://nessie:19120/iceberg
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse

# Airflow (dùng ở phase sau) — sinh key thật bằng:
#   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__FERNET_KEY=<your-fernet-key>
AIRFLOW__WEBSERVER__SECRET_KEY=<your-secret-key>
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW_UID=50000
```

### 1.4 Tạo .env.example (commit lên git, không chứa giá trị thật)

```env
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_AIRFLOW_DB=
ORACLE_PASSWORD=
ORACLE_USER=
SPARK_MASTER_URL=
ICEBERG_CATALOG_URI=
ICEBERG_WAREHOUSE=
AIRFLOW__CORE__FERNET_KEY=
AIRFLOW__WEBSERVER__SECRET_KEY=
AIRFLOW__CORE__EXECUTOR=
AIRFLOW_UID=
```

**Xác nhận xong:** `.env` tồn tại, `.gitignore` đã có `.env`

---

## Bước 2 — PostgreSQL

### 2.1 Tạo scripts/init-postgres.sql

```sql
CREATE DATABASE airflow;
\c banking;

CREATE TABLE customers (
    customer_id      VARCHAR(20) PRIMARY KEY,
    full_name        VARCHAR(200) NOT NULL,
    date_of_birth    DATE,
    gender           CHAR(1),
    id_number        VARCHAR(20) UNIQUE,
    phone            VARCHAR(15),
    email            VARCHAR(100),
    city             VARCHAR(100),
    province         VARCHAR(100),
    customer_segment VARCHAR(20),
    kyc_status       VARCHAR(20) DEFAULT 'VERIFIED',
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE card_accounts (
    account_id          VARCHAR(20) PRIMARY KEY,
    customer_id         VARCHAR(20) REFERENCES customers(customer_id),
    card_type           VARCHAR(30),
    card_number         VARCHAR(20),
    credit_limit        DECIMAL(18,2),
    outstanding_balance DECIMAL(18,2) DEFAULT 0,
    due_date            DATE,
    status              VARCHAR(20) DEFAULT 'ACTIVE',
    opened_date         DATE DEFAULT CURRENT_DATE,
    closed_date         DATE,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE card_transactions (
    txn_id            VARCHAR(30) PRIMARY KEY,
    account_id        VARCHAR(20) REFERENCES card_accounts(account_id),
    txn_date          DATE NOT NULL,
    txn_datetime      TIMESTAMP NOT NULL,
    amount            DECIMAL(18,2) NOT NULL,
    currency          CHAR(3) DEFAULT 'VND',
    txn_type          VARCHAR(30),
    merchant_name     VARCHAR(200),
    merchant_category VARCHAR(50),
    channel           VARCHAR(20),
    status            VARCHAR(20) DEFAULT 'COMPLETED',
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_customers_updated ON customers (updated_at);
CREATE INDEX idx_card_acct_updated ON card_accounts (updated_at);
CREATE INDEX idx_card_txn_date     ON card_transactions (txn_date);

INSERT INTO customers VALUES
('CIF001','Nguyen Van An',  '1985-03-15','M','079085001234','0901234501','an.nguyen@gmail.com',  'Ho Chi Minh','Ho Chi Minh','AFFLUENT','VERIFIED',NOW(),NOW()),
('CIF002','Tran Thi Bich',  '1990-07-22','F','038090002345','0912345602','bich.tran@gmail.com',  'Ho Chi Minh','Ho Chi Minh','MASS',    'VERIFIED',NOW(),NOW()),
('CIF003','Le Hoang Cuong', '1978-11-08','M','001078003456','0923456703','cuong.le@company.vn',  'Ha Noi',     'Ha Noi',     'PREMIER', 'VERIFIED',NOW(),NOW()),
('CIF004','Pham Thi Dung',  '1995-05-30','F','048095004567','0934567804','dung.pham@gmail.com',  'Da Nang',    'Da Nang',    'MASS',    'VERIFIED',NOW(),NOW()),
('CIF005','Hoang Minh Em',  '1982-09-14','M','074082005678','0945678905','em.hoang@business.vn', 'Ho Chi Minh','Ho Chi Minh','AFFLUENT','VERIFIED',NOW(),NOW());

INSERT INTO card_accounts VALUES
('ACC001','CIF001','VISA_CREDIT',      '4111****0001',50000000, 5000000, '2025-01-15','ACTIVE','2022-01-10',NULL,NOW(),NOW()),
('ACC002','CIF002','VISA_DEBIT',       '4111****0002',NULL,     0,       NULL,        'ACTIVE','2021-03-20',NULL,NOW(),NOW()),
('ACC003','CIF003','MASTERCARD_CREDIT','5200****0003',200000000,45000000,'2025-01-20','ACTIVE','2019-11-05',NULL,NOW(),NOW()),
('ACC004','CIF005','VISA_CREDIT',      '4111****0004',100000000,12000000,'2025-01-25','ACTIVE','2021-07-30',NULL,NOW(),NOW());

INSERT INTO card_transactions VALUES
('TXN001','ACC001','2024-11-01','2024-11-01 09:15:00',250000, 'VND','PURCHASE','Vinmart Q1',       'GROCERY', 'POS',   'COMPLETED',NOW()),
('TXN002','ACC001','2024-11-05','2024-11-05 19:30:00',850000, 'VND','PURCHASE','The Coffee House', 'DINING',  'POS',   'COMPLETED',NOW()),
('TXN003','ACC001','2024-11-10','2024-11-10 14:00:00',3200000,'VND','PURCHASE','Vietnam Airlines', 'TRAVEL',  'ONLINE','COMPLETED',NOW()),
('TXN004','ACC003','2024-11-02','2024-11-02 16:45:00',8500000,'VND','PURCHASE','Takashimaya',      'SHOPPING','POS',   'COMPLETED',NOW()),
('TXN005','ACC004','2024-11-04','2024-11-04 11:00:00',4500000,'VND','PURCHASE','VinPearl Travel',  'TRAVEL',  'ONLINE','COMPLETED',NOW());
```

### 2.2 Thêm PostgreSQL vào docker-compose.yml

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:15-alpine
    container_name: lakehouse-postgres
    env_file: .env
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./scripts/init-postgres.sql:/docker-entrypoint-initdb.d/01-init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres-data:
```

### 2.3 Chạy và kiểm tra

```bash
# Khởi động
docker compose up -d postgres

# Xem log (chờ dòng "ready to accept connections")
docker compose logs -f postgres

# Kiểm tra data
docker exec -it lakehouse-postgres psql -U postgres -d banking \
  -c "SELECT customer_id, full_name, customer_segment FROM customers;"
```

### 2.4 Kết nối DBeaver

```
Driver    : PostgreSQL
Host      : localhost
Port      : 5432
Database  : banking
Username  : postgres
Password  : postgres123
```

Sau khi kết nối: vào `banking → Schemas → public → Tables` → thấy 3 bảng là đúng.

**Xác nhận xong bước 2:**
- [ ] Container `lakehouse-postgres` status = running
- [ ] DBeaver kết nối thành công
- [ ] Thấy 5 rows trong bảng `customers`

---

## Bước 3 — Oracle XE

> Lưu ý: Image Oracle ~7GB, lần đầu download mất 10–20 phút tùy mạng.
> Sau khi download xong, Oracle cần thêm 2–3 phút để khởi tạo lần đầu.

### 3.1 Tạo scripts/init-oracle.sql

```sql
CREATE TABLE branches (
    branch_id   VARCHAR2(10) PRIMARY KEY,
    branch_name VARCHAR2(200) NOT NULL,
    region      VARCHAR2(50),
    city        VARCHAR2(100),
    branch_type VARCHAR2(30),
    status      VARCHAR2(10) DEFAULT 'ACTIVE',
    updated_at  TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE products (
    product_id    VARCHAR2(20) PRIMARY KEY,
    product_name  VARCHAR2(200) NOT NULL,
    product_type  VARCHAR2(30),
    interest_rate NUMBER(5,2),
    status        VARCHAR2(10) DEFAULT 'ACTIVE',
    updated_at    TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE bank_accounts (
    account_id    VARCHAR2(20) PRIMARY KEY,
    customer_id   VARCHAR2(20) NOT NULL,
    product_id    VARCHAR2(20) REFERENCES products(product_id),
    branch_id     VARCHAR2(10) REFERENCES branches(branch_id),
    account_type  VARCHAR2(20),
    account_no    VARCHAR2(20) UNIQUE,
    balance       NUMBER(18,2) DEFAULT 0,
    currency      CHAR(3) DEFAULT 'VND',
    status        VARCHAR2(20) DEFAULT 'ACTIVE',
    opened_date   DATE DEFAULT SYSDATE,
    closed_date   DATE,
    interest_rate NUMBER(5,2),
    updated_at    TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE loans (
    loan_id            VARCHAR2(20) PRIMARY KEY,
    customer_id        VARCHAR2(20) NOT NULL,
    product_id         VARCHAR2(20) REFERENCES products(product_id),
    branch_id          VARCHAR2(10) REFERENCES branches(branch_id),
    loan_type          VARCHAR2(30),
    principal_amount   NUMBER(18,2),
    outstanding_amount NUMBER(18,2),
    interest_rate      NUMBER(5,2),
    disbursement_date  DATE,
    maturity_date      DATE,
    npl_status         VARCHAR2(10) DEFAULT 'NORMAL',
    status             VARCHAR2(20) DEFAULT 'ACTIVE',
    updated_at         TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE bank_transactions (
    txn_id        VARCHAR2(30) PRIMARY KEY,
    account_id    VARCHAR2(20) REFERENCES bank_accounts(account_id),
    txn_date      DATE NOT NULL,
    txn_datetime  TIMESTAMP NOT NULL,
    txn_type      VARCHAR2(30),
    amount        NUMBER(18,2) NOT NULL,
    balance_after NUMBER(18,2),
    channel       VARCHAR2(20),
    status        VARCHAR2(20) DEFAULT 'COMPLETED',
    created_at    TIMESTAMP DEFAULT SYSTIMESTAMP
);

INSERT INTO branches VALUES ('BR001','Hoi So Chinh', 'South','Ho Chi Minh','HEAD_OFFICE','ACTIVE',SYSTIMESTAMP);
INSERT INTO branches VALUES ('BR002','Chi Nhanh Hanoi','North','Ha Noi',    'BRANCH',    'ACTIVE',SYSTIMESTAMP);

INSERT INTO products VALUES ('PRD001','Tiet Kiem Thuong',     'SAVINGS',5.5, 'ACTIVE',SYSTIMESTAMP);
INSERT INTO products VALUES ('PRD002','Vay Tieu Dung Ca Nhan','LOAN',   12.5,'ACTIVE',SYSTIMESTAMP);
INSERT INTO products VALUES ('PRD003','Tai Khoan Thanh Toan', 'SAVINGS',0,   'ACTIVE',SYSTIMESTAMP);

INSERT INTO bank_accounts VALUES ('BACC001','CIF001','PRD003','BR001','CURRENT',     '0011000001',85000000, 'VND','ACTIVE',DATE '2020-01-10',NULL,0,  SYSTIMESTAMP);
INSERT INTO bank_accounts VALUES ('BACC002','CIF001','PRD001','BR001','TERM_DEPOSIT','0011000002',200000000,'VND','ACTIVE',DATE '2024-06-01',NULL,5.5,SYSTIMESTAMP);
INSERT INTO bank_accounts VALUES ('BACC003','CIF002','PRD003','BR001','CURRENT',     '0011000003',12000000, 'VND','ACTIVE',DATE '2021-03-20',NULL,0,  SYSTIMESTAMP);
INSERT INTO bank_accounts VALUES ('BACC004','CIF003','PRD003','BR002','CURRENT',     '0011000004',350000000,'VND','ACTIVE',DATE '2019-11-05',NULL,0,  SYSTIMESTAMP);

INSERT INTO loans VALUES ('LOAN001','CIF002','PRD002','BR001','PERSONAL',50000000,38500000,12.5,DATE '2023-06-15',DATE '2026-06-15','NORMAL','ACTIVE',SYSTIMESTAMP);
INSERT INTO loans VALUES ('LOAN002','CIF003','PRD002','BR002','PERSONAL',30000000,25000000,12.5,DATE '2024-01-10',DATE '2026-01-10','NORMAL','ACTIVE',SYSTIMESTAMP);

INSERT INTO bank_transactions VALUES ('BTXN001','BACC001',DATE '2024-11-01',TIMESTAMP '2024-11-01 08:00:00','DEPOSIT',10000000,85000000, 'ONLINE','COMPLETED',SYSTIMESTAMP);
INSERT INTO bank_transactions VALUES ('BTXN002','BACC004',DATE '2024-11-01',TIMESTAMP '2024-11-01 09:00:00','DEPOSIT',50000000,350000000,'ONLINE','COMPLETED',SYSTIMESTAMP);

CREATE INDEX idx_bank_acct_updated ON bank_accounts (updated_at);
CREATE INDEX idx_loans_updated     ON loans (updated_at);
CREATE INDEX idx_btxn_date         ON bank_transactions (txn_date);

COMMIT;
```

### 3.2 Thêm Oracle vào docker-compose.yml

```yaml
  oracle-xe:
    image: gvenzl/oracle-xe:21-slim-faststart
    container_name: lakehouse-oracle
    env_file: .env
    environment:
      ORACLE_PASSWORD: ${ORACLE_PASSWORD}
      ORACLE_DATABASE: XEPDB1
      APP_USER: ${ORACLE_USER}
      APP_USER_PASSWORD: ${ORACLE_PASSWORD}
    ports:
      - "1521:1521"
    volumes:
      - oracle-data:/opt/oracle/oradata
      - ./scripts/init-oracle.sql:/container-entrypoint-initdb.d/01-init.sql
    healthcheck:
      test: ["CMD-SHELL", "echo 'SELECT 1 FROM DUAL;' | sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@//localhost/XEPDB1"]
      interval: 30s
      timeout: 10s
      retries: 10
      start_period: 120s

# Thêm vào volumes:
#   oracle-data:
```

### 3.3 Chạy và kiểm tra

```bash
docker compose up -d oracle-xe
docker compose logs -f oracle-xe
# Chờ dòng: DATABASE IS READY TO USE!

# Kiểm tra data
docker exec -it lakehouse-oracle sqlplus corebanking/oracle123@//localhost/XEPDB1 <<EOF
SELECT table_name FROM user_tables ORDER BY 1;
EXIT;
EOF
```

### 3.4 Kết nối DBeaver

```
Driver    : Oracle
Host      : localhost
Port      : 1521
Database  : XEPDB1  (chọn Service name, không phải SID)
Username  : corebanking
Password  : oracle123
```

> Lần đầu DBeaver sẽ hỏi download Oracle JDBC driver — chọn Download.

**Xác nhận xong bước 3:**
- [ ] Container `lakehouse-oracle` status = running
- [ ] DBeaver kết nối thành công
- [ ] Thấy 5 bảng: branches, products, bank_accounts, loans, bank_transactions

---

## Bước 4 — MinIO (Object Storage)

### 4.1 Thêm vào docker-compose.yml

```yaml
  minio:
    image: minio/minio:RELEASE.2024-05-10T01-41-38Z
    container_name: lakehouse-minio
    command: server /data --console-address ":9001"
    env_file: .env
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:RELEASE.2024-05-10T01-41-38Z
    container_name: lakehouse-minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin123;
      mc mb --ignore-existing local/lakehouse;
      echo 'Bucket lakehouse created';
      "

# Thêm vào volumes:
#   minio-data:
```

### 4.2 Chạy và kiểm tra

```bash
docker compose up -d minio minio-init
docker compose logs -f minio-init
# Chờ dòng: Bucket lakehouse created
```

Mở browser vào `http://localhost:9001`:
- Login: `minioadmin` / `minioadmin123`
- Thấy bucket `lakehouse` là đúng

**Xác nhận xong bước 4:**
- [ ] MinIO Web UI mở được tại `http://localhost:9001`
- [ ] Bucket `lakehouse` đã tạo

---

## Bước 5 — Nessie (Iceberg Catalog)

### 5.1 Thêm vào docker-compose.yml

```yaml
  nessie:
    image: projectnessie/nessie:0.79.0
    container_name: lakehouse-nessie
    ports:
      - "19120:19120"
    environment:
      - QUARKUS_PROFILE=prod
      - nessie.version.store.type=IN_MEMORY
```

```bash
docker compose up -d nessie
# Kiểm tra: http://localhost:19120/api/v2/config
```

---

## Bước 6 — Spark Cluster

### 6.1 Thêm vào docker-compose.yml

```yaml
  spark-master:
    image: bitnami/spark:3.5.1
    container_name: lakehouse-spark-master
    environment:
      - SPARK_MODE=master
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    ports:
      - "8085:8080"
      - "7077:7077"
    volumes:
      - ./src:/opt/spark/jobs

  spark-worker:
    image: bitnami/spark:3.5.1
    container_name: lakehouse-spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=3G
      - SPARK_WORKER_CORES=2
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    depends_on:
      - spark-master
    volumes:
      - ./src:/opt/spark/jobs
```

```bash
docker compose up -d spark-master spark-worker
# Kiểm tra Spark UI: http://localhost:8085
# Thấy 1 worker đang ALIVE là đúng
```

---

## Bước 7–9 — Test Spark kết nối

Tạo file `src/common/test_connections.py`:

```python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test-connections") \
    .config("spark.jars.packages",
        "org.postgresql:postgresql:42.7.3,"
        "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Test PostgreSQL
df_pg = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/banking") \
    .option("dbtable", "customers") \
    .option("user", "postgres") \
    .option("password", "postgres123") \
    .option("driver", "org.postgresql.Driver") \
    .load()
print(f"PostgreSQL customers: {df_pg.count()} rows")
df_pg.show(3)

# Test Oracle
df_ora = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//oracle-xe:1521/XEPDB1") \
    .option("dbtable", "bank_accounts") \
    .option("user", "corebanking") \
    .option("password", "oracle123") \
    .option("driver", "oracle.jdbc.OracleDriver") \
    .load()
print(f"Oracle bank_accounts: {df_ora.count()} rows")
df_ora.show(3)

# Test ghi lên MinIO
df_pg.write.mode("overwrite").parquet("s3a://lakehouse/test/customers")
print("Ghi Parquet lên MinIO: OK")

spark.stop()
```

Chạy từ container Spark:

```bash
docker exec -it lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/common/test_connections.py
```

**Xác nhận xong bước 7–9:**
- [ ] Log in thấy `PostgreSQL customers: 5 rows`
- [ ] Log in thấy `Oracle bank_accounts: 4 rows`
- [ ] Log in thấy `Ghi Parquet lên MinIO: OK`
- [ ] MinIO Web UI thấy file Parquet trong `lakehouse/test/customers/`

---

## Kết quả mong đợi khi hoàn thành

```bash
docker compose ps
```

```
NAME                      STATUS
lakehouse-postgres        running (healthy)
lakehouse-oracle          running (healthy)
lakehouse-minio           running (healthy)
lakehouse-nessie          running
lakehouse-spark-master    running
lakehouse-spark-worker    running
```

DBeaver kết nối được:
- `localhost:5432` → banking → 3 bảng Postgres
- `localhost:1521` → XEPDB1 → 5 bảng Oracle

MinIO UI `http://localhost:9001` → bucket `lakehouse` có folder `test/customers/`

---

## Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `port 5432 already in use` | PostgreSQL đã cài trên máy | Đổi port sang `5433:5432` trong compose |
| Oracle healthcheck fail | Oracle chưa khởi động xong | Chờ thêm 2–3 phút, check log |
| `No space left on device` | Docker Desktop hết disk | Tăng disk limit trong Docker Desktop Settings |
| Spark không connect được Oracle | Thiếu ojdbc11 jar | Đảm bảo `spark.jars.packages` có ojdbc11 |
| MinIO `Access Denied` | Sai credentials | Kiểm tra lại `.env` |

---

## Task tiếp theo

Sau khi hoàn thành task này → tạo `tasks/task_bronze_ingestion.md`

Nội dung: Viết Spark job đọc toàn bộ data từ Oracle + PostgreSQL, ghi vào Bronze Iceberg tables trên MinIO, orchestrate bằng Airflow DAG.