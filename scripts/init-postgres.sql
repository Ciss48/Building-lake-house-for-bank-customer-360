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
