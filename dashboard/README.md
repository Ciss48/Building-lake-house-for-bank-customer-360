# Customer 360 — Web Dashboard (static + DuckDB-Wasm → Vercel)

Dashboard web tĩnh visualize `gold.mart_customer_360`. **Không backend, không server.**
Toàn bộ truy vấn chạy *client-side trong trình duyệt* bằng **DuckDB-Wasm** trên 1 file
parquet (~80 KB); biểu đồ vẽ bằng **ECharts**. Deploy free lên **Vercel**.

## Vì sao kiến trúc này

Trino/Iceberg/MinIO chạy trên `localhost` (máy bạn) — **Vercel (cloud) không với tới localhost được**.
Giải pháp: **export snapshot** mart ra file tĩnh, dashboard đọc file đó. Hợp vì mart chỉ ~800 dòng,
là snapshot theo ngày. Performance: render tức thì; chi phí: $0 (Vercel free tier dư sức cho file tĩnh nhỏ).

```
[Trino @localhost:8088] --export_mart.py--> public/data/mart_customer_360.parquet
                                                     |
                              public/index.html + app.js (DuckDB-Wasm + ECharts)
                                                     |
                                              deploy --> Vercel (URL public)
```

## 1. Export dữ liệu (chạy trên host, trong venv)

```powershell
# stack Docker phải đang chạy (Trino healthy), pipeline Gold đã tạo snapshot
.\venv\Scripts\activate
pip install -r requirements.txt          # đã thêm 'trino' client
python dashboard\export_mart.py
```
Sinh ra `public/data/mart_customer_360.parquet` + `meta.json` (luôn lấy `max(snapshot_date)`).
**Refresh dữ liệu:** chạy lại lệnh này sau mỗi lần pipeline Gold tạo snapshot mới, rồi redeploy / `git push`.

## 2. Xem thử ở local

Phải chạy qua HTTP server (mở trực tiếp `file://` sẽ bị chặn fetch parquet & module):
```powershell
python -m http.server 5500 --directory dashboard\public
# mở http://localhost:5500
```

## 3. Deploy lên Vercel

**Cách A — CLI (nhanh nhất):**
```powershell
npm i -g vercel
cd dashboard\public
vercel            # lần đầu: link project; chọn thư mục hiện tại làm root
vercel --prod     # deploy production, nhận URL public
```

**Cách B — Git + Vercel UI:** push repo lên GitHub → vercel.com → New Project → import repo →
đặt **Root Directory = `dashboard/public`** → Framework Preset = *Other* → Deploy.

> `public/` đã là web root (chứa `index.html`, `app.js`, `style.css`, `vercel.json`, `data/`).
> Không cần build step — đây là site tĩnh thuần.

## Cấu trúc

```
dashboard/
├─ export_mart.py        # Trino -> parquet + meta.json
├─ README.md
└─ public/               # <-- web root deploy lên Vercel
   ├─ index.html         # layout + tabs + KPI + bộ lọc
   ├─ app.js             # DuckDB-Wasm load parquet, ECharts vẽ 20 chart
   ├─ style.css
   ├─ vercel.json        # cache header cho /data
   └─ data/
      ├─ mart_customer_360.parquet   # tất cả snapshot (1 dòng/KH/ngày snapshot)
      ├─ daily_trend.parquet         # giao dịch theo NGAY × nguồn × channel × segment
      ├─ monthly_active.parquet      # KH active distinct theo tháng × segment
      ├─ daily_active.parquet        # KH active distinct theo ngày × segment
      └─ meta.json
```

> **Grain theo ngày, cuộn lên tháng:** `daily_trend.parquet` lưu ở mức **ngày**; chart xem theo
> tháng được DuckDB-Wasm cuộn lên bằng `date_trunc('month', day)` — không cần file tháng riêng.
> Riêng "KH active distinct" không cộng được từ ngày lên tháng nên có 2 file (ngày + tháng).

## Các trang / biểu đồ (5 tab)

- **Tổng quan**: 8 KPI card · donut phân khúc · bar độ phủ SP · bar RFM segment · bar theo tỉnh.
- **Xu hướng**: giá trị+số GD theo thời gian (cột/đường) · KH giao dịch · cơ cấu kênh (stacked area) ·
  tăng trưởng KH+huy động theo snapshot. Có nút **Xu hướng theo: Ngày / Tháng** (chế độ Ngày bật
  thanh dataZoom để kéo-zoom theo khoảng ngày).
- **RFM**: heatmap R×F · treemap phân khúc · AUM theo phân khúc · scatter Recency–Monetary.
- **Sản phẩm**: phân bố số SP · band AUM · độ phủ SP theo phân khúc · danh mục chi tiêu.
- **Rủi ro & Cross-sell**: NPL/người vay theo phân khúc · số lead bán chéo · lead theo phân khúc · band sử dụng hạn mức.

## Bộ lọc (header, áp dụng toàn bộ chart, re-query DuckDB tại chỗ)

- **Phân khúc** — MASS / AFFLUENT / PREMIER (toàn bộ chart).
- **Ngày snapshot** — chọn 1 trong các snapshot của mart (mặc định mới nhất).
- **Từ tháng / Đến tháng** — khoảng thời gian cho tab Xu hướng.
- **Xu hướng theo** — Ngày / Tháng (chỉ hiện khi đã có `daily_trend` + `daily_active`).

> **Resilient:** app tải file nào có sẵn. Nếu chỉ có file tháng cũ (`monthly_trend.parquet`),
> dashboard vẫn chạy ở chế độ tháng và ẩn nút "Xu hướng theo"; chạy lại `export_mart.py`
> để sinh file ngày là nút tự hiện.

## Lưu ý

- Dashboard cố tình **không nhúng PII** — mart Gold đã masking sẵn (tên ẩn, phone/email mask, id hash).
- Muốn nhiều snapshot/so sánh theo thời gian: bỏ filter `max(snapshot_date)` trong `export_mart.py`
  (export hết) rồi thêm slicer `snapshot_date` ở UI.
