# Power BI — Thiết kế báo cáo Customer 360 (Gold)

Nguồn dữ liệu duy nhất: `iceberg.gold.mart_customer_360` (Trino, `localhost:8088`).
1 dòng = 1 khách hàng. Tất cả KPI khách hàng lấy từ bảng này — **không cần join thêm**.

> Quy ước số tiền: các cột `*_balance`, `*_outstanding`, `net_asset_value`, `txn_amount_12m`,
> `avg_monthly_spend`, `monetary_12m` đơn vị **VND**. Báo cáo hiển thị theo **Tỷ VND** (chia 1e9)
> hoặc **Triệu VND** (chia 1e6) bằng measure, không sửa dữ liệu gốc.

---

## 0. Kết nối Power BI Desktop ↔ Trino

Power BI không có connector Trino gốc → dùng **ODBC**:

1. Cài **Trino ODBC driver** (Starburst cung cấp bản miễn phí cho Trino open-source).
2. Tạo DSN (ODBC Data Sources 64-bit):
   - Host `localhost`, Port `8088`, Catalog `iceberg`, Schema `gold`
   - Auth: không SSL (môi trường on-prem Docker), user bất kỳ (vd `admin`).
3. Power BI Desktop → **Get Data → ODBC → chọn DSN** → chọn bảng `mart_customer_360`.
4. **Quan trọng — chọn chế độ Import** (KHÔNG DirectQuery). Dữ liệu ~800 KH rất nhỏ,
   Import cho phép dùng đầy đủ DAX và nhanh.

> Mẹo: đặt tên bảng trong Power BI là **`Mart360`** (Model view → rename). Toàn bộ measure dưới đây dùng tên này.

---

## 1. Xử lý `snapshot_date` (BẮT BUỘC đọc — tránh đếm trùng)

Mart ghi **1 partition `snapshot_date` mỗi lần chạy Gold**. Nếu pipeline chạy nhiều ngày, mỗi KH sẽ
có nhiều dòng (mỗi snapshot 1 dòng) → mọi phép `SUM/COUNT` sẽ **nhân đôi**.

**Giải pháp khuyến nghị (đơn giản nhất):** lọc về snapshot mới nhất ngay trong **Power Query**:

- Power Query Editor → cột `snapshot_date` → ... thực tế làm bằng 2 bước:
  1. Thêm bước: Group/“Keep” — hoặc đơn giản: tạo một query phụ `MaxSnap = List.Max(Mart360[snapshot_date])`.
  2. Lọc `Mart360`: giữ dòng có `snapshot_date = MaxSnap`.
- Hoặc gõ trực tiếp trong Advanced Editor:
  ```m
  let
      Source = Odbc.Query("dsn=Trino", "SELECT * FROM iceberg.gold.mart_customer_360
          WHERE snapshot_date = (SELECT max(snapshot_date) FROM iceberg.gold.mart_customer_360)")
  in
      Source
  ```
  → cách này đẩy filter xuống Trino, chỉ tải snapshot mới nhất.

**Nếu sau này muốn xem xu hướng theo snapshot:** tải hết các snapshot, thêm **Slicer `snapshot_date`**
ở mỗi trang và đặt mặc định = ngày mới nhất. Các measure bên dưới vẫn đúng vì chúng tự lọc theo context.

---

## 2. Calculated Columns (tạo trong Model view → New column)

Dùng cho histogram / nhóm. Đây là **column** (không phải measure).

```DAX
Age Band =
SWITCH( TRUE(),
    Mart360[age] < 25, "1. <25",
    Mart360[age] < 35, "2. 25-34",
    Mart360[age] < 45, "3. 35-44",
    Mart360[age] < 55, "4. 45-54",
    Mart360[age] < 65, "5. 55-64",
    "6. 65+" )
```
```DAX
Tenure Years = DIVIDE( Mart360[tenure_days], 365.25 )
```
```DAX
Tenure Band =
SWITCH( TRUE(),
    Mart360[tenure_days] < 365,  "1. <1 năm",
    Mart360[tenure_days] < 1095, "2. 1-3 năm",
    Mart360[tenure_days] < 1825, "3. 3-5 năm",
    "4. 5+ năm" )
```
```DAX
AUM Band =
SWITCH( TRUE(),
    Mart360[total_deposit_balance] = 0,          "0. Không gửi",
    Mart360[total_deposit_balance] < 50000000,   "1. <50tr",
    Mart360[total_deposit_balance] < 200000000,  "2. 50-200tr",
    Mart360[total_deposit_balance] < 500000000,  "3. 200-500tr",
    Mart360[total_deposit_balance] < 1000000000, "4. 500tr-1 tỷ",
    "5. 1 tỷ+" )
```
```DAX
Engagement Status =
SWITCH( TRUE(),
    ISBLANK(Mart360[recency_days]), "Chưa GD",
    Mart360[recency_days] <= 30,  "1. Rất tích cực (≤30d)",
    Mart360[recency_days] <= 90,  "2. Tích cực (31-90d)",
    Mart360[recency_days] <= 180, "3. Nguội (91-180d)",
    "4. Ngủ đông (>180d)" )
```
```DAX
Digital Persona =
SWITCH( TRUE(),
    Mart360[digital_txn_ratio] >= 0.8, "Digital-native",
    Mart360[digital_txn_ratio] >= 0.5, "Digital-first",
    Mart360[digital_txn_ratio] > 0,    "Hybrid",
    "Truyền thống" )
```
```DAX
RFM Sort =                      /* để sort segment đúng thứ tự giá trị */
SWITCH( Mart360[rfm_segment],
    "Champions", 1, "Loyal", 2, "Potential_Loyal", 3, "New", 4,
    "Need_Attention", 5, "At_Risk", 6, "Hibernating", 7, 99 )
```
> Sau khi tạo `RFM Sort`: chọn cột `rfm_segment` → **Sort by column → RFM Sort**.

---

## 3. Measures (tạo 1 bảng đo riêng — khuyến nghị gom vào display folder)

> Tạo 1 measure bất kỳ trước, rồi Model view gán **Display folder** để gọn:
> `01 KPI` / `02 Sản phẩm` / `03 RFM` / `04 Giao dịch` / `05 Rủi ro & Cross-sell`.

### 3.1 KPI nền (folder `01 KPI`)
```DAX
Total Customers = DISTINCTCOUNT( Mart360[customer_id] )

Total AUM = SUM( Mart360[total_deposit_balance] )
Total AUM (Tỷ) = DIVIDE( [Total AUM], 1000000000 )

Total Loan Outstanding = SUM( Mart360[total_loan_outstanding] )
Total Loan (Tỷ) = DIVIDE( [Total Loan Outstanding], 1000000000 )

Total NAV = SUM( Mart360[net_asset_value] )
Total NAV (Tỷ) = DIVIDE( [Total NAV], 1000000000 )

Avg Products / Customer = AVERAGE( Mart360[total_product_count] )

Total Txn Amount 12M (Tỷ) = DIVIDE( SUM(Mart360[txn_amount_12m]), 1000000000 )
Total Txn Count 12M = SUM( Mart360[txn_count_12m] )
Avg AUM / Customer (Tr) = DIVIDE( [Total AUM], [Total Customers] ) / 1000000
```

### 3.2 Sản phẩm & độ phủ (folder `02 Sản phẩm`)
```DAX
Savings Holders     = CALCULATE( [Total Customers], Mart360[has_savings]     = TRUE() )
Current Holders     = CALCULATE( [Total Customers], Mart360[has_current]     = TRUE() )
Card Holders        = CALCULATE( [Total Customers], Mart360[has_credit_card] = TRUE() )
Loan Holders        = CALCULATE( [Total Customers], Mart360[has_loan]        = TRUE() )

Savings Penetration % = DIVIDE( [Savings Holders], [Total Customers] )
Current Penetration % = DIVIDE( [Current Holders], [Total Customers] )
Card Penetration %    = DIVIDE( [Card Holders],    [Total Customers] )
Loan Penetration %    = DIVIDE( [Loan Holders],    [Total Customers] )

Single-Product Customers = CALCULATE( [Total Customers], Mart360[total_product_count] = 1 )
Multi-Product Customers  = CALCULATE( [Total Customers], Mart360[total_product_count] >= 3 )
Multi-Product %          = DIVIDE( [Multi-Product Customers], [Total Customers] )
```

### 3.3 RFM (folder `03 RFM`)
```DAX
Champions       = CALCULATE( [Total Customers], Mart360[rfm_segment] = "Champions" )
At Risk         = CALCULATE( [Total Customers], Mart360[rfm_segment] = "At_Risk" )
Hibernating     = CALCULATE( [Total Customers], Mart360[rfm_segment] = "Hibernating" )

Champions %     = DIVIDE( [Champions], [Total Customers] )
At Risk %       = DIVIDE( [At Risk], [Total Customers] )

Avg R = AVERAGE( Mart360[r_score] )
Avg F = AVERAGE( Mart360[f_score] )
Avg M = AVERAGE( Mart360[m_score] )

AUM by Segment (Tỷ) = [Total AUM (Tỷ)]     /* dùng với rfm_segment trên trục */
```

### 3.4 Giao dịch & số hóa (folder `04 Giao dịch`)
```DAX
Avg Digital Ratio = AVERAGE( Mart360[digital_txn_ratio] )

Digital-First Customers = CALCULATE( [Total Customers], Mart360[digital_txn_ratio] >= 0.5 )
Digital-First %         = DIVIDE( [Digital-First Customers], [Total Customers] )

Active Customers (90d) = CALCULATE( [Total Customers], Mart360[recency_days] <= 90 )
Activity Rate %        = DIVIDE( [Active Customers (90d)], [Total Customers] )

Dormant Customers = CALCULATE( [Total Customers],
                        OR( Mart360[recency_days] > 180, ISBLANK(Mart360[recency_days]) ) )

Avg Monthly Spend (Tr) = DIVIDE( AVERAGE(Mart360[avg_monthly_spend]), 1000000 )
Avg Active Months 12M  = AVERAGE( Mart360[active_months_12m] )
```

### 3.5 Rủi ro & Cross-sell (folder `05 Rủi ro & Cross-sell`)
```DAX
NPL Customers   = CALCULATE( [Total Customers], Mart360[has_npl_loan] = TRUE() )
NPL Rate % (KH) = DIVIDE( [NPL Customers], [Total Customers] )
NPL Rate % (Vay)= DIVIDE( [NPL Customers], [Loan Holders] )      /* tỷ lệ nợ xấu trên người vay */

Avg Days Past Due = CALCULATE( AVERAGE(Mart360[days_past_due]), Mart360[days_past_due] > 0 )
Avg Credit Utilization % = CALCULATE( AVERAGE(Mart360[credit_utilization_rate]),
                                      Mart360[has_credit_card] = TRUE() )

Loan Leads   = CALCULATE( [Total Customers], Mart360[cross_sell_loan_flag]   = TRUE() )
Card Leads   = CALCULATE( [Total Customers], Mart360[cross_sell_card_flag]   = TRUE() )
Invest Leads = CALCULATE( [Total Customers], Mart360[cross_sell_invest_flag] = TRUE() )

Loan Lead %   = DIVIDE( [Loan Leads],   [Total Customers] )
Card Lead %   = DIVIDE( [Card Leads],   [Total Customers] )
Invest Lead % = DIVIDE( [Invest Leads], [Total Customers] )

Any Cross-sell Lead = CALCULATE( [Total Customers],
    FILTER( Mart360,
        Mart360[cross_sell_loan_flag] || Mart360[cross_sell_card_flag] || Mart360[cross_sell_invest_flag] ) )
```

> Định dạng %: chọn measure `*%` → Measure tools → **Format = Percentage, 1 chữ số thập phân**.

---

## 4. Sáu trang báo cáo

Mỗi mục: **Loại visual** → *Fields* (Axis/Legend/Values) → **Measure/Column dùng**.

---

### 🟦 Trang 1 — Executive Overview (Tổng quan điều hành)

**Hàng KPI cards (Card visual):**
| Card | Measure |
|---|---|
| Tổng khách hàng | `Total Customers` |
| Tổng huy động (Tỷ VND) | `Total AUM (Tỷ)` |
| Tổng dư nợ (Tỷ VND) | `Total Loan (Tỷ)` |
| NAV ròng (Tỷ VND) | `Total NAV (Tỷ)` |
| SP/khách hàng | `Avg Products / Customer` |
| Tỷ lệ KH tích cực | `Activity Rate %` |

**Visuals:**
- **Donut chart** — Cơ cấu phân khúc: Legend `customer_segment`, Values `Total Customers`.
- **Bar chart (clustered)** — Độ phủ sản phẩm: Axis = 4 measure `Savings/Current/Card/Loan Penetration %`
  (đặt 4 measure vào Values của 1 bar; hoặc dùng visual “New ribbon”). Cách gọn: tạo bar với
  Values = `Savings Penetration %`, `Current Penetration %`, `Card Penetration %`, `Loan Penetration %`.
- **Stacked bar** — KH theo `RFM Sort/rfm_segment` (Axis) × `Total Customers` (Values), Legend `rfm_segment`.
- **Map / Filled map** — Phân bố theo `province`: Location `province`, Bubble size `Total Customers`.
- **Slicer** — `customer_segment`, `province`, (và `snapshot_date` nếu tải nhiều snapshot).

---

### 🟩 Trang 2 — Customer Demographics (Nhân khẩu học)

- **Column chart** — Phân bố tuổi: Axis `Age Band`, Values `Total Customers`.
- **Column chart (clustered)** — Tuổi × giới tính: Axis `Age Band`, Legend `gender`, Values `Total Customers`.
- **Bar chart** — Top tỉnh/thành: Axis `province` (Top N = 10 filter), Values `Total Customers`.
- **Column chart** — Thâm niên: Axis `Tenure Band`, Values `Total Customers`.
- **Donut** — Trạng thái KYC: Legend `kyc_status`, Values `Total Customers`.
- **Cards** — `Total Customers`, AVERAGE(`age`) (tạo measure `Avg Age = AVERAGE(Mart360[age])`),
  AVERAGE(`Tenure Years`).
- **Slicer** — `customer_segment`, `gender`.

---

### 🟨 Trang 3 — Product Holdings & Penetration (Sản phẩm)

**Cards:** `Savings Penetration %`, `Card Penetration %`, `Loan Penetration %`, `Multi-Product %`.

- **100% Stacked column** — Số sản phẩm nắm giữ: Axis `total_product_count`, Values `Total Customers`.
- **Clustered bar** — Độ phủ từng SP: Values = `Savings/Current/Card/Loan Penetration %`.
- **Matrix** — Rows `customer_segment`, Values: `Savings Penetration %`, `Card Penetration %`,
  `Loan Penetration %`, `Avg Products / Customer`. (bật conditional formatting heatmap).
- **Column** — Phân bố AUM: Axis `AUM Band`, Values `Total Customers`.
- **Scatter** — AUM vs số SP: X `Avg Products / Customer`? → thực ra dùng scatter cấp KH:
  Details `customer_id`, X `total_deposit_balance`, Y `total_product_count`, Legend `customer_segment`.
- **Slicer** — `customer_segment`, `AUM Band`.

---

### 🟧 Trang 4 — RFM Segmentation (Phân khúc giá trị)

**Cards:** `Champions %`, `At Risk %`, `Avg R`, `Avg F`, `Avg M`.

- **Treemap** — Quy mô phân khúc: Group `rfm_segment`, Values `Total Customers`.
- **Matrix (RFM heatmap)** — Rows `r_score`, Columns `f_score`, Values `Total Customers`,
  bật **Background color (conditional formatting)** → đây là “lưới RFM” kinh điển.
- **Clustered bar** — Giá trị theo phân khúc: Axis `rfm_segment`, Values `Total AUM (Tỷ)` và
  `Total Txn Amount 12M (Tỷ)`.
- **Scatter** — Recency vs Monetary: Details `customer_id`, X `recency_days`, Y `monetary_12m`,
  Size `frequency_12m`, Legend `rfm_segment`.
- **Table** — Danh sách Champions: cột `customer_id`, `customer_segment`, `total_deposit_balance`,
  `total_product_count`, lọc `rfm_segment = Champions`, sort theo AUM giảm dần (danh sách chăm sóc VIP).
- **Slicer** — `rfm_segment`, `customer_segment`.

---

### 🟪 Trang 5 — Transactions & Digital Behavior (Giao dịch & số hóa)

**Cards:** `Total Txn Count 12M`, `Total Txn Amount 12M (Tỷ)`, `Avg Digital Ratio` (format %),
`Digital-First %`, `Activity Rate %`.

- **Column** — KH theo trạng thái tương tác: Axis `Engagement Status`, Values `Total Customers`.
- **Donut** — Chân dung số: Legend `Digital Persona`, Values `Total Customers`.
- **Bar** — Danh mục chi tiêu phổ biến: Axis `top_spend_category`, Values `Total Customers`.
- **Column** — Số tháng hoạt động: Axis `active_months_12m`, Values `Total Customers`.
- **Histogram (column)** — Recency: Axis `recency_days` (nhóm bằng visual binning của Power BI,
  bin size ~30), Values `Total Customers`.
- **Gauge** — `Digital-First %` (target 0.5).
- **Slicer** — `customer_segment`, `Digital Persona`.

---

### 🟥 Trang 6 — Risk & Cross-sell (Rủi ro & cơ hội bán chéo)

**Cards:** `NPL Rate % (Vay)`, `NPL Customers`, `Avg Credit Utilization %`, `Avg Days Past Due`.

**Khối Rủi ro:**
- **KPI/Card + Bar** — NPL theo phân khúc: Axis `customer_segment`, Values `NPL Rate % (Vay)`.
- **Column** — Phân bố credit utilization: tạo column `Util Band` (tương tự bins) hoặc binning visual
  trên `credit_utilization_rate`, Values `Total Customers`.
- **Scatter** — Dư nợ vs DPD: Details `customer_id`, X `total_loan_outstanding`, Y `days_past_due`,
  Legend `has_npl_loan`.

**Khối Cross-sell (cơ hội):**
- **Cards** — `Loan Leads`, `Card Leads`, `Invest Leads`, `Any Cross-sell Lead`.
- **Clustered bar** — Tỷ lệ lead: Values `Loan Lead %`, `Card Lead %`, `Invest Lead %`.
- **Bar** — Lead theo phân khúc: Axis `customer_segment`, Values `Loan Leads`, `Card Leads`, `Invest Leads`.
- **Table (danh sách hành động)** — KH có cơ hội đầu tư: cột `customer_id`, `customer_segment`,
  `net_asset_value`, `total_deposit_balance`; lọc `cross_sell_invest_flag = True`; sort NAV giảm dần.
- **Slicer** — `customer_segment`, và 3 slicer flag `cross_sell_*_flag`.

---

## 5. Hoàn thiện (tùy chọn nhưng nên làm)

- **Theme**: View → Themes → chọn 1 theme ngân hàng (xanh dương). Đồng bộ màu phân khúc.
- **Drill-through**: tạo trang ẩn “Customer Detail”, drill-through theo `customer_id` để xem 360° 1 KH.
- **Tooltips**: bật để hover ra số chi tiết.
- **Sync slicers**: View → Sync slicers để slicer `customer_segment` áp dụng nhiều trang.
- **Format số tiền**: với measure `(Tỷ)` đặt 1 chữ số thập phân, suffix " tỷ".

> Khi pipeline Gold chạy lại tạo snapshot mới: chỉ cần **Refresh** trong Power BI; nếu lọc theo
> `max(snapshot_date)` ở Power Query thì báo cáo tự nhảy sang ngày mới nhất.
