-- sql/governance/pii_check.sql
-- Saved query (Trino) — kiem tra mart_customer_360 KHONG con PII tho.
-- Chay qua Trino: iceberg.gold.mart_customer_360
-- Ky vong: full_name dang 'Ho ****', phone_masked dang '09x****xxx', email_masked 'u***@...'.

-- 1) Xem mau 10 dong da che
SELECT customer_id, full_name, phone_masked, email_masked
FROM iceberg.gold.mart_customer_360
LIMIT 10;

-- 2) Dam bao KHONG con full_name tho (full_name da che luon ket thuc ' ****')
--    => so dong vi pham phai = 0
SELECT count(*) AS raw_name_leak
FROM iceberg.gold.mart_customer_360
WHERE full_name IS NOT NULL AND full_name NOT LIKE '% ****';

-- 3) Dam bao KHONG con email tho (email da che luon chua '***@')
--    => so dong vi pham phai = 0 (bo qua NULL)
SELECT count(*) AS raw_email_leak
FROM iceberg.gold.mart_customer_360
WHERE email_masked IS NOT NULL AND email_masked NOT LIKE '%***@%';

-- 4) Dam bao mart KHONG ho cot phone/email tho (chi co *_masked).
--    Liet ke cot -> khong duoc thay 'phone' hay 'email' (khong _masked).
SELECT column_name
FROM iceberg.information_schema.columns
WHERE table_schema = 'gold' AND table_name = 'mart_customer_360'
  AND column_name IN ('phone', 'email', 'id_number');
-- Ky vong: 0 dong tra ve.
