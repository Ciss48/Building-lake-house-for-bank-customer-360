-- sql/gold/vw_sales_crosssell.sql
-- Saved query cho team Sales: danh sach KH co co hoi cross-sell (loan/card/invest).
--
-- LUU Y: Trino + Iceberg Nessie catalog KHONG ho tro CREATE VIEW -> day la saved query
--   (filtered SELECT) chay truc tiep tren mart_customer_360. Xem ghi chu vw_marketing_rfm.sql.

SELECT customer_id, full_name, customer_segment, net_asset_value, total_deposit_balance,
       cross_sell_loan_flag, cross_sell_card_flag, cross_sell_invest_flag
FROM iceberg.gold.mart_customer_360
WHERE cross_sell_loan_flag OR cross_sell_card_flag OR cross_sell_invest_flag
ORDER BY net_asset_value DESC;
