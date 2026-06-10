# MEMORY — Lakehouse Customer 360

Index các tài liệu trạng thái dự án (đọc khi bắt đầu session).

> Đánh số phase theo `plan_overall.md`: Foundation=0, Bronze=1, Silver=2, Gold=3, Governance=4.

- [plan_overall.md](plan_overall.md) — kế hoạch tổng thể, kiến trúc Medallion, roadmap 5 phase, 25+ KPI
- [phase0_done.md](phase0_done.md) — Phase 0: dựng hạ tầng (Postgres, Oracle, MinIO, Nessie, Spark), seed data, test Spark đọc/ghi
- [phase1_done.md](phase1_done.md) — Phase 1 (xong 2026-06-04): Bronze ingestion (8 Iceberg tables + audit), full/incremental, Airflow DAG. **Chứa 5 fix khác draft + cheat sheet lệnh**
- [phase2_done.md](phase2_done.md) — Phase 2 (xong 2026-06-08): Silver layer (6 dim SCD1/SCD2 + 2 fact + audit), YAML-driven, Trino 446, DAG silver_transform. **Fix SCD2 localCheckpoint + Trino /api/v1 + gotcha Nessie IN_MEMORY mất catalog khi restart**
- [phase3_done.md](phase3_done.md) — Phase 3 (xong 2026-06-08): Gold layer (2 agg + mart_customer_360 38 cột, RFM NTILE5, cross-sell), ~800 KH, **Nessie ROCKSDB bền qua restart**, DAG gold_mart. Fix volume chown uid 185 + Trino không CREATE VIEW
- [daily_simulation_done.md](daily_simulation_done.md) — container `lakehouse-data-simulator` (sleep infinity, công cụ thủ công) sinh thêm txn/KH theo thời gian → đẩy max(txn_date) tới (vd as_of nhảy 2025-12-31 → 2026-06-02)
- [phase4_done.md](phase4_done.md) — Phase 4 (xong 2026-06-09): Governance & Ops — PII masking ở Gold (full_name/phone/email), Schema Evolution (ALTER ADD COLUMN), Time Travel demo, compaction. **Nessie gc.enabled=false → expire_snapshots/remove_orphan_files UNSUPPORTED; mart tích lũy nhiều partition snapshot_date → mask_legacy_partitions.py + follow-up double-count**
