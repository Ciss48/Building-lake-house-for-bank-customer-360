# 💼 Business Value & Talking Points — Lakehouse Customer 360

Tài liệu này biến dự án kỹ thuật thành **câu chuyện giá trị** dùng cho CV, phỏng vấn và trình bày
với người không chuyên kỹ thuật (hiring manager, business stakeholder).

---

## 1. Câu chuyện 1 đoạn (elevator pitch)

> *Ngân hàng bán lẻ có dữ liệu khách hàng nằm rải rác ở Core Banking (Oracle) và CRM/Thẻ
> (PostgreSQL), không có cái nhìn 360° nên Marketing bắn campaign đại trà, lãng phí. Tôi xây dựng
> một Lakehouse end-to-end (Iceberg + Spark + Trino + Airflow) hợp nhất 2 nguồn, dựng bảng
> `mart_customer_360` với 38 KPI mỗi khách, tự động phân khúc RFM và gắn cờ cross-sell — giúp
> Marketing/Sales nhắm đúng đối tượng, kèm governance chuẩn ngân hàng (che PII, time travel, bảo trì
> tự động).*

---

## 2. Bullet point cho CV

Chọn 1–2 dòng đắt nhất tuỳ độ dài CV:

- **Xây dựng Lakehouse Customer 360 end-to-end** (Apache Iceberg, Spark, Trino, Airflow, MinIO,
  Nessie) theo kiến trúc Medallion 3 lớp, hợp nhất 2 hệ nguồn ngân hàng (Oracle Core Banking +
  PostgreSQL CRM/Thẻ), triển khai 100% on-premise bằng Docker Compose.
- Dựng bảng `mart_customer_360` (**38 KPI/khách**) với **phân khúc RFM** (NTILE-5, 7 nhóm) và
  **3 tín hiệu cross-sell** (vay/thẻ/đầu tư), phục vụ Marketing/Sales tự phục vụ qua Trino.
- Triển khai **SCD Type 2** theo dõi lịch sử thay đổi khách hàng; **PII masking** che tên/điện
  thoại/email ở lớp consumer đảm bảo tuân thủ bảo mật.
- Áp dụng **governance chuẩn production**: Time Travel (audit/rollback), Schema Evolution (thêm
  cột không rewrite), compaction Iceberg tự động hoá bằng Airflow DAG hàng tuần.

---

## 3. Số liệu "biết nói" (đưa vào CV/phỏng vấn cho cụ thể)

| Hạng mục | Con số |
|---|---|
| Khách hàng xử lý | ~800 |
| Giao dịch ngân hàng / thẻ | ~25.000 / ~6.500 (18 tháng) |
| KPI mỗi khách | 38 |
| Phân khúc RFM | 7 nhóm |
| Khách đủ điều kiện cross-sell vay / thẻ / đầu tư | ~274 / ~400 / ~87 |
| Khoản nợ xấu (NPL) được gắn cờ | ~19 |
| Bảng Iceberg toàn hệ thống | 8 Bronze + 9 Silver + 4 Gold |

---

## 4. Map: tính năng kỹ thuật → giá trị kinh doanh

| Tôi xây gì | "Để làm gì" cho ngân hàng |
|---|---|
| `mart_customer_360` 1 dòng/khách | Self-service analytics — Marketing không phải chờ IT trích số |
| RFM segment | Phân bổ ngân sách campaign đúng nhóm: giữ Champions, cứu At_Risk, đánh thức Hibernating |
| Cross-sell flags | Danh sách lead có sẵn → tăng số sản phẩm/khách (tăng doanh thu) |
| has_npl_loan / days_past_due | Tín hiệu rủi ro tín dụng sớm |
| net_asset_value, credit_utilization_rate | Đánh giá giá trị & hành vi tài chính của khách |
| PII masking | Tránh rủi ro pháp lý/bảo mật khi mở data cho nhiều phòng ban |
| Time Travel + audit log | Sẵn sàng cho kiểm toán nội bộ/ngoài; rollback an toàn |

---

## 5. Talking points cho phỏng vấn (câu hỏi sâu)

**"Tại sao Medallion 3 lớp?"**
> Tách trách nhiệm: Bronze trung thực với nguồn (truy vết), Silver chuẩn hoá + lịch sử (source of
> truth nội bộ), Gold tối ưu cho tiêu thụ (đã tổng hợp + che PII). Đổi logic nghiệp vụ chỉ chạm Gold,
> không phá Bronze/Silver.

**"Tại sao SCD Type 2 cho khách hàng?"**
> Phân khúc/địa chỉ/KYC của khách thay đổi theo thời gian. SCD2 giữ từng phiên bản (effective_from/to,
> is_current) → trả lời được "ngày X khách này thuộc nhóm nào", phục vụ phân tích lịch sử & kiểm toán.

**"Kể một vấn đề khó và cách bạn xử lý."** ⭐ *(câu chuyện đắt nhất)*
> Khi áp PII masking, tôi phát hiện bảng mart vẫn **rò rỉ tên thật ở partition cũ**: lệnh
> `overwritePartitions` chỉ ghi đè partition trùng `snapshot_date`, nên dữ liệu build trước khi có
> masking vẫn còn nguyên. Tôi viết job UPDATE tại chỗ (idempotent) để che nốt phần cũ mà vẫn giữ
> lịch sử, đưa số dòng rò rỉ về 0 — và time travel vẫn xem được trạng thái "trước governance" để audit.
> → Cho thấy tôi không chỉ code chạy được mà còn **bắt được lỗ hổng governance và xử lý tận gốc**.

**"Governance với Iceberg gặp giới hạn gì?"**
> Catalog Nessie tắt GC per-bảng (`gc.enabled=false`) vì quản version cấp catalog (giống Git nhiều
> nhánh). Nên `expire_snapshots`/`remove_orphan_files` không chạy được per-bảng — đúng phương án phải
> dùng `nessie-gc` cấp catalog. Biết giới hạn công cụ cũng là một phần của vận hành.

**"Vì sao chạy batch chứ không streaming?"**
> Bài toán Customer 360 / cross-sell là phân tích định kỳ (hàng ngày là đủ). Batch đơn giản hơn, rẻ
> hơn, dễ kiểm soát chất lượng. Kiến trúc vẫn mở để thêm CDC/streaming sau nếu cần real-time.

---

## 6. Hướng mở rộng (nếu được hỏi "next step")
- Thêm CDC real-time (Debezium) cho giao dịch nóng.
- Layer ML: churn prediction, next-best-offer dựa trên feature từ mart.
- Data quality framework (Great Expectations) gắn vào DAG.
- Chuẩn hoá mart về current-only hoặc daily-snapshot rõ ràng để tránh đếm trùng khi không lọc.
