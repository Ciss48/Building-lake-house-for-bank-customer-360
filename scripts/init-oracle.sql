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
