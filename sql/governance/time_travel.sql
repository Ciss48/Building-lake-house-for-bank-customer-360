-- sql/governance/time_travel.sql
-- Saved query (Trino) — demo Iceberg Time Travel tren mart_customer_360.
-- Trino dung metadata table "<table>$snapshots" / "$history" va cu phap FOR VERSION AS OF.

-- 1) Lich su snapshot (moi lan build mart = 1 snapshot moi)
SELECT committed_at, snapshot_id, operation
FROM iceberg.gold."mart_customer_360$snapshots"
ORDER BY committed_at;

-- 2) Row count tai 1 snapshot cu (thay <SNAPSHOT_ID> bang gia tri tu query 1)
-- SELECT count(*) FROM iceberg.gold.mart_customer_360 FOR VERSION AS OF <SNAPSHOT_ID>;

-- 3) So sanh PII tai snapshot cu vs hien tai
--    Snapshot truoc khi rebuild voi masking -> full_name con dang tho (3 tu).
-- SELECT 'old' AS v, full_name FROM iceberg.gold.mart_customer_360 FOR VERSION AS OF <OLD_ID> LIMIT 5
-- UNION ALL
-- SELECT 'now' AS v, full_name FROM iceberg.gold.mart_customer_360 LIMIT 5;

-- 4) Time travel theo thoi gian (thay timestamp phu hop)
-- SELECT count(*) FROM iceberg.gold.mart_customer_360
-- FOR TIMESTAMP AS OF TIMESTAMP '2026-06-09 00:00:00 UTC';
