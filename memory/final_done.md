# Final Done — Portfolio & GitHub (Tuần 10 theo plan_overall.md)

## Ngày hoàn thành: 2026-06-10

> Phase cuối theo `plan_overall.md`. Dự án Lakehouse Customer 360 đã **hoàn chỉnh** (Phase 0–4 + Final).

---

## Đã làm
- **`README.md`** (tiếng Việt): bài toán nghiệp vụ, business value, kiến trúc Medallion, tech stack,
  3 lớp Bronze/Silver/Gold, 38 KPI, governance, how-to-run, cấu trúc dự án, lộ trình 5 phase.
- **`docs/business_value.md`**: elevator pitch, bullet CV, số liệu "biết nói", map kỹ thuật→nghiệp vụ,
  **talking points phỏng vấn** (gồm câu chuyện đắt: phát hiện PII rò rỉ ở partition cũ & cách fix).
- **Git repo init** (`main`), commit đầu `a76cd68`, **70 file**.
- **Bảo mật trước khi public**: `.env` / `venv/` / `.claude/` đã gitignore; scrub password trong
  `tasks/task_1_setup_databases.md` thành placeholder; xoá `test.py` (rác) + `project_phase.md`
  (lỗi encoding mojibake) khỏi repo.

## GitHub
- Repo: **https://github.com/Ciss48/Building-lake-house-for-bank-customer-360**
- Remote `origin` đã set, `main` track `origin/main`, đã push thành công.
- Lệnh đẩy update sau này: `git add -A; git commit -m "..."; git push`.

## ⚠️ Còn lại (tuỳ chọn, CHƯA làm)
1. **Dev creds vẫn hardcode** dạng fallback (`minioadmin123`/`postgres123`/`oracle123`) trong
   `src/common/spark_session.py`, `src/common/test_connections.py`, `docker-compose.yml`. Rủi ro
   thấp (sandbox local) nhưng nếu muốn repo sạch tuyệt đối → thay bằng ENV bắt buộc.
2. **Follow-up Phase 4** (từ phase4_done.md): mart có 2 partition `snapshot_date` (2025-12-31 +
   2026-06-02) = 1603 rows → query không lọc snapshot_date sẽ double-count. Nên thêm
   `WHERE snapshot_date = (SELECT max(...))` ở saved query marketing/sales, hoặc đổi grain mart.
3. Hướng mở rộng (nếu phỏng vấn hỏi next step): CDC real-time, ML churn/next-best-offer,
   data quality (Great Expectations).
