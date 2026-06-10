-- sql/gold/vw_marketing_rfm.sql
-- Saved query cho team Marketing: danh sach KH theo RFM segment uu tien chien dich.
--
-- LUU Y: Trino + Iceberg Nessie catalog KHONG ho tro CREATE VIEW
--   ("createView is not supported for Iceberg Nessie catalogs").
--   mart_customer_360 chinh la ban materialization; day chi la 1 lat cat (filtered SELECT)
--   de chay truc tiep trong DBeaver / Trino CLI. Doi tieu chi -> sua WHERE.
--
-- Chay: docker exec lakehouse-trino trino --execute "<dan noi dung ben duoi>"

SELECT customer_id, full_name, customer_segment, rfm_segment, rfm_score,
       recency_days, frequency_12m, monetary_12m, last_txn_date
FROM iceberg.gold.mart_customer_360
WHERE rfm_segment IN ('Champions', 'Loyal', 'At_Risk', 'Hibernating')
ORDER BY rfm_segment, monetary_12m DESC;
