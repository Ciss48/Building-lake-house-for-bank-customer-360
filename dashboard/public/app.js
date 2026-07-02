// dashboard/public/app.js
// Query parquet hoan toan client-side bang DuckDB-Wasm, ve bieu do bang ECharts.
// Khong co backend — chay tinh tren Vercel.
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const echarts = window.echarts;
const state = { segment: "ALL", snapshot: null, mFrom: null, mTo: null, grain: "month" };
const charts = {}; // id -> echarts instance
let conn = null;
// Bang du lieu xu huong co san sau khi load (resilient: thieu file thi tat tinh nang tuong ung)
const HAS = { dtrend: false, mtrend: false, dactive: false, mactive: false };

// ── Bang mau corporate (don sac xanh thep + xam; do tram chi cho rui ro) ──
const C = {
  blue:    "#5b8ec4", // primary — steel blue
  blueDk:  "#3c6390", // dam
  blueLt:  "#9ec0de", // nhat
  slate:   "#8395a7", // gray-blue
  slateLt: "#b6c2cd",
  navy:    "#2c4a6b",
  amber:   "#c4a468", // canh bao (tram)
  red:     "#b66a6a", // rui ro (tram)
  green:   "#79a08d", // tich cuc (sage tram)
};
const SEG_COLORS = { MASS: C.slate, AFFLUENT: C.blue, PREMIER: C.blueLt };
const PALETTE = [C.blue, C.slate, C.blueDk, C.slateLt, C.navy, C.blueLt, C.green, C.amber];
const CHANNEL_COLORS = { ONLINE: C.blue, ATM: C.slate, POS: C.blueDk, BRANCH: C.slateLt };
const AXIS = "#94a3b8", SPLIT = "#334155";
// Thu tu phan khuc RFM (gia tri cao -> thap)
const RFM_ORDER = ["Champions","Loyal","Potential_Loyal","New","Need_Attention","At_Risk","Hibernating"];

// ── Helpers ──────────────────────────────────────────────────────────────
const vn = (n, d = 0) => Number(n).toLocaleString("vi-VN", { maximumFractionDigits: d, minimumFractionDigits: d });
const pct = (n, d = 1) => (Number(n) * 100).toFixed(d) + "%";

// WHERE cho cac chart tu mart (snapshot dang chon + phan khuc)
function W(extra) {
  const parts = [`snapshot_date = DATE '${state.snapshot}'`];
  if (state.segment !== "ALL") parts.push(`customer_segment = '${state.segment}'`);
  if (extra) parts.push(extra);
  return "WHERE " + parts.join(" AND ");
}

// Co dang xem theo ngay khong (chi khi co du file daily)
const isDay = () => state.grain === "day" && HAS.dtrend && HAS.dactive;

// Inner subquery cho txn trend, chuan hoa ra: x (nhan hien thi), mkey (YYYY-MM de loc),
// channel, customer_segment, txn_count, txn_amount. Theo ngay -> tu dtrend; theo thang ->
// cuon tu dtrend bang date_trunc, hoac dung mtrend cu neu chua co daily.
function trendInner() {
  if (isDay())
    return `SELECT strftime(day,'%Y-%m-%d') x, strftime(day,'%Y-%m') mkey,
            channel, customer_segment, txn_count, txn_amount FROM dtrend`;
  if (HAS.dtrend)
    return `SELECT strftime(date_trunc('month', day),'%Y-%m') x, strftime(day,'%Y-%m') mkey,
            channel, customer_segment, txn_count, txn_amount FROM dtrend`;
  return `SELECT strftime(month,'%Y-%m') x, strftime(month,'%Y-%m') mkey,
          channel, customer_segment, txn_count, txn_amount FROM mtrend`;
}

function activeInner() {
  if (isDay())
    return `SELECT strftime(day,'%Y-%m-%d') x, strftime(day,'%Y-%m') mkey,
            customer_segment, active_customers FROM dactive`;
  return `SELECT strftime(month,'%Y-%m') x, strftime(month,'%Y-%m') mkey,
          customer_segment, active_customers FROM mactive`;
}

// WHERE cho cac chart xu huong (khoang thang mkey + phan khuc). Dung tren inner subquery.
function WT(extra) {
  const parts = [`mkey BETWEEN '${state.mFrom}' AND '${state.mTo}'`];
  if (state.segment !== "ALL") parts.push(`customer_segment = '${state.segment}'`);
  if (extra) parts.push(extra);
  return "WHERE " + parts.join(" AND ");
}

// dataZoom cho che do ngay (nhieu diem) — giu month view gon gang
const zoomDay = () => isDay()
  ? { dataZoom: [{ type: "inside" }, { type: "slider", height: 16, bottom: 8,
      borderColor: SPLIT, fillerColor: "rgba(91,142,196,.18)", textStyle: { color: AXIS } }] }
  : {};

// Layout chung cho cac chart xu huong: legend tren, chua cho slider khi xem theo ngay
function trendLayout(extraGrid) {
  const day = isDay();
  return {
    legend: { top: 0, textStyle: { color: AXIS } },
    grid: { left: 48, right: 20, top: 34, bottom: day ? 52 : 36, containLabel: true, ...extraGrid },
    ...zoomDay(),
  };
}

async function q(sql) {
  const res = await conn.query(sql);
  return res.toArray().map((r) => r.toJSON());
}

function getChart(id) {
  if (!charts[id]) charts[id] = echarts.init(document.getElementById(id), null, { renderer: "canvas" });
  return charts[id];
}

// Theme/grid mac dinh toi
const baseGrid = { left: 48, right: 20, top: 24, bottom: 36, containLabel: true };
const axisStyle = {
  axisLine: { lineStyle: { color: SPLIT } },
  axisLabel: { color: AXIS },
  splitLine: { lineStyle: { color: SPLIT, type: "dashed" } },
};
const tooltip = { trigger: "item", backgroundColor: "#0b1224", borderColor: SPLIT, textStyle: { color: "#e2e8f0" } };
const tooltipAxis = { ...tooltip, trigger: "axis", axisPointer: { type: "shadow" } };

// ── KPI cards ────────────────────────────────────────────────────────────
async function renderKpis() {
  const [k] = await q(`
    SELECT
      count(*) AS total,
      sum(total_deposit_balance)/1e9 AS aum_ty,
      sum(total_loan_outstanding)/1e9 AS loan_ty,
      sum(net_asset_value)/1e9 AS nav_ty,
      avg(total_product_count) AS avg_prod,
      avg(CASE WHEN recency_days <= 90 THEN 1.0 ELSE 0 END) AS activity,
      CAST(sum(CASE WHEN has_npl_loan THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(sum(CASE WHEN has_loan THEN 1 ELSE 0 END),0) AS npl_borrow,
      avg(CASE WHEN digital_txn_ratio >= 0.5 THEN 1.0 ELSE 0 END) AS digital
    FROM mart ${W()}`);

  const cards = [
    { label: "Tổng khách hàng", value: vn(k.total), cls: "accent" },
    { label: "Tổng huy động", value: vn(k.aum_ty, 1), unit: "tỷ", cls: "green" },
    { label: "Tổng dư nợ", value: vn(k.loan_ty, 1), unit: "tỷ", cls: "amber" },
    { label: "NAV ròng", value: vn(k.nav_ty, 1), unit: "tỷ", cls: "green" },
    { label: "SP / khách hàng", value: vn(k.avg_prod, 2), cls: "accent" },
    { label: "KH tích cực (≤90d)", value: pct(k.activity), cls: "green" },
    { label: "Nợ xấu / người vay", value: pct(k.npl_borrow), cls: "red" },
    { label: "Digital-first", value: pct(k.digital), cls: "accent" },
  ];
  document.getElementById("kpis").innerHTML = cards
    .map((c) => `<div class="kpi ${c.cls}"><div class="label">${c.label}</div>
      <div class="value">${c.value}${c.unit ? `<span class="unit">${c.unit}</span>` : ""}</div></div>`)
    .join("");
}

// ── TAB Tong quan ─────────────────────────────────────────────────────────
async function segDonut() {
  const d = await q(`SELECT customer_segment seg, count(*) n FROM mart ${W()} GROUP BY 1`);
  getChart("segDonut").setOption({
    tooltip, legend: { bottom: 0, textStyle: { color: AXIS } },
    series: [{
      type: "pie", radius: ["45%", "70%"], center: ["50%", "45%"],
      data: d.map((r) => ({ name: r.seg, value: Number(r.n), itemStyle: { color: SEG_COLORS[r.seg] } })),
      label: { color: "#e2e8f0", formatter: "{b}\n{d}%" },
    }],
  });
}

async function penBar() {
  const [r] = await q(`SELECT
      avg(CASE WHEN has_savings THEN 1.0 ELSE 0 END) sav,
      avg(CASE WHEN has_current THEN 1.0 ELSE 0 END) cur,
      avg(CASE WHEN has_credit_card THEN 1.0 ELSE 0 END) card,
      avg(CASE WHEN has_loan THEN 1.0 ELSE 0 END) loan
    FROM mart ${W()}`);
  const cats = ["Tiết kiệm", "Tài khoản", "Thẻ tín dụng", "Vay"];
  const vals = [r.sav, r.cur, r.card, r.loan].map(Number);
  getChart("penBar").setOption({
    tooltip: { ...tooltipAxis, valueFormatter: (v) => pct(v) },
    grid: baseGrid,
    xAxis: { type: "category", data: cats, ...axisStyle },
    yAxis: { type: "value", max: 1, axisLabel: { formatter: (v) => pct(v, 0), color: AXIS }, ...axisStyle },
    series: [{ type: "bar", data: vals, barWidth: "50%",
      itemStyle: { color: C.blue, borderRadius: [4, 4, 0, 0] },
      label: { show: true, position: "top", color: "#e2e8f0", formatter: (p) => pct(p.value) } }],
  });
}

async function rfmBar() {
  const d = await q(`SELECT rfm_segment seg, count(*) n FROM mart ${W()} GROUP BY 1`);
  const map = Object.fromEntries(d.map((r) => [r.seg, Number(r.n)]));
  const cats = RFM_ORDER.filter((s) => map[s] != null);
  getChart("rfmBar").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "value", ...axisStyle },
    yAxis: { type: "category", data: [...cats].reverse(), ...axisStyle },
    series: [{ type: "bar", data: [...cats].reverse().map((s) => map[s]), barWidth: "55%",
      itemStyle: { color: C.blueDk, borderRadius: [0, 4, 4, 0] },
      label: { show: true, position: "right", color: "#e2e8f0" } }],
  });
}

async function provBar() {
  const d = await q(`SELECT province p, count(*) n FROM mart ${W()} GROUP BY 1 ORDER BY n DESC`);
  getChart("provBar").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "category", data: d.map((r) => r.p), axisLabel: { color: AXIS, interval: 0, rotate: 20 }, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ type: "bar", data: d.map((r) => Number(r.n)), barWidth: "50%",
      itemStyle: { color: C.slate, borderRadius: [4, 4, 0, 0] } }],
  });
}

// ── TAB Xu huong (theo Ngay hoac Thang qua nut chuyen) ───────────────────────
async function txnTrend() {
  const day = isDay();
  const d = await q(`SELECT x, sum(txn_amount)/1e9 amt, sum(txn_count) cnt
    FROM (${trendInner()}) ${WT()} GROUP BY 1 ORDER BY 1`);
  getChart("txnTrend").setOption({
    tooltip: tooltipAxis,
    ...trendLayout({ right: 56 }),
    xAxis: { type: "category", data: d.map((r) => r.x), axisLabel: { color: AXIS, rotate: day ? 0 : 35 }, ...axisStyle },
    yAxis: [
      { type: "value", name: "Tỷ VND", nameTextStyle: { color: AXIS }, ...axisStyle },
      { type: "value", name: "Số GD", nameTextStyle: { color: AXIS }, splitLine: { show: false },
        axisLine: { lineStyle: { color: SPLIT } }, axisLabel: { color: AXIS } },
    ],
    series: [
      { name: "Giá trị GD (tỷ)", type: "bar", data: d.map((r) => Number(r.amt).toFixed(2)),
        itemStyle: { color: C.blue, borderRadius: [3, 3, 0, 0] } },
      { name: "Số giao dịch", type: "line", yAxisIndex: 1, smooth: true, symbol: day ? "none" : "circle", symbolSize: 5,
        data: d.map((r) => Number(r.cnt)),
        lineStyle: { color: C.amber, width: day ? 1.2 : 2 }, itemStyle: { color: C.amber } },
    ],
  });
}

async function activeTrend() {
  const day = isDay();
  const d = await q(`SELECT x, sum(active_customers) n
    FROM (${activeInner()}) ${WT()} GROUP BY 1 ORDER BY 1`);
  getChart("activeTrend").setOption({
    tooltip: tooltipAxis,
    ...trendLayout(),
    xAxis: { type: "category", data: d.map((r) => r.x), axisLabel: { color: AXIS, rotate: day ? 0 : 35 }, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ name: "KH giao dịch", type: "line", smooth: true, symbol: day ? "none" : "circle", symbolSize: 5,
      data: d.map((r) => Number(r.n)),
      lineStyle: { color: C.blue, width: day ? 1.5 : 2.5 }, itemStyle: { color: C.blue },
      areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1,
        colorStops: [{ offset: 0, color: "rgba(91,142,196,.30)" }, { offset: 1, color: "rgba(91,142,196,0)" }] } } }],
  });
}

async function channelTrend() {
  const day = isDay();
  const d = await q(`SELECT x, channel, sum(txn_count) n
    FROM (${trendInner()}) ${WT()} GROUP BY 1, 2 ORDER BY 1`);
  const xs = [...new Set(d.map((r) => r.x))];
  const channels = ["ONLINE", "ATM", "POS", "BRANCH"].filter((c) => d.some((r) => r.channel === c));
  const byCh = {};
  for (const r of d) (byCh[r.channel] ||= {})[r.x] = Number(r.n);
  getChart("channelTrend").setOption({
    tooltip: tooltipAxis,
    ...trendLayout(),
    xAxis: { type: "category", data: xs, axisLabel: { color: AXIS, rotate: day ? 0 : 35 }, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: channels.map((ch) => ({
      name: ch, type: "line", stack: "total", smooth: true, symbol: "none",
      lineStyle: { width: 0 }, areaStyle: { color: CHANNEL_COLORS[ch], opacity: 0.85 },
      emphasis: { focus: "series" },
      data: xs.map((x) => byCh[ch]?.[x] ?? 0),
    })),
  });
}

async function snapTrend() {
  // Xu huong KPI qua cac snapshot cua mart (khong loc theo snapshot dang chon)
  const seg = state.segment !== "ALL" ? `WHERE customer_segment = '${state.segment}'` : "";
  const d = await q(`SELECT strftime(snapshot_date, '%Y-%m-%d') s,
      count(*) n, sum(total_deposit_balance)/1e9 aum
    FROM mart ${seg} GROUP BY 1 ORDER BY 1`);
  getChart("snapTrend").setOption({
    tooltip: tooltipAxis,
    legend: { bottom: 0, textStyle: { color: AXIS } },
    grid: { ...baseGrid, right: 56 },
    xAxis: { type: "category", data: d.map((r) => r.s), axisLabel: { color: AXIS, rotate: 25 }, ...axisStyle },
    yAxis: [
      { type: "value", name: "KH", nameTextStyle: { color: AXIS }, min: "dataMin", ...axisStyle },
      { type: "value", name: "Tỷ VND", nameTextStyle: { color: AXIS }, min: "dataMin", splitLine: { show: false },
        axisLine: { lineStyle: { color: SPLIT } }, axisLabel: { color: AXIS } },
    ],
    series: [
      { name: "Khách hàng", type: "line", smooth: true, symbolSize: 6,
        data: d.map((r) => Number(r.n)), lineStyle: { color: C.blue, width: 2.5 }, itemStyle: { color: C.blue } },
      { name: "Huy động (tỷ)", type: "line", yAxisIndex: 1, smooth: true, symbolSize: 6,
        data: d.map((r) => Number(r.aum).toFixed(1)),
        lineStyle: { color: C.green, width: 2.5 }, itemStyle: { color: C.green } },
    ],
  });
}

// ── TAB RFM ───────────────────────────────────────────────────────────────
async function rfmHeat() {
  const d = await q(`SELECT f_score f, r_score r, count(*) n FROM mart ${W()} GROUP BY 1,2`);
  const data = d.map((x) => [Number(x.f) - 1, Number(x.r) - 1, Number(x.n)]);
  const maxN = Math.max(...data.map((x) => x[2]), 1);
  getChart("rfmHeat").setOption({
    tooltip: { ...tooltip, formatter: (p) => `F=${p.value[0] + 1}, R=${p.value[1] + 1}<br/>${p.value[2]} KH` },
    grid: { left: 40, right: 16, top: 16, bottom: 60, containLabel: true },
    xAxis: { type: "category", name: "Frequency", nameLocation: "middle", nameGap: 28,
      data: [1, 2, 3, 4, 5], ...axisStyle, splitLine: { show: false } },
    yAxis: { type: "category", name: "Recency", data: [1, 2, 3, 4, 5], ...axisStyle, splitLine: { show: false } },
    visualMap: { min: 0, max: maxN, calculable: true, orient: "horizontal", left: "center", bottom: 0,
      textStyle: { color: AXIS },
      inRange: { color: ["#16243a", "#2c4a6b", "#3c6390", "#5b8ec4", "#9ec0de"] } },
    series: [{ type: "heatmap", data, label: { show: true, color: "#e2e8f0", fontWeight: 500 } }],
  });
}

async function rfmTree() {
  const d = await q(`SELECT rfm_segment seg, count(*) n FROM mart ${W()} GROUP BY 1`);
  const map = Object.fromEntries(d.map((r) => [r.seg, Number(r.n)]));
  const cats = RFM_ORDER.filter((s) => map[s] != null);
  getChart("rfmTree").setOption({
    tooltip,
    series: [{ type: "treemap", roam: false, nodeClick: false, breadcrumb: { show: false },
      label: { color: "#0b1224", fontWeight: 600, formatter: "{b}\n{c}" },
      data: cats.map((s, i) => ({ name: s, value: map[s], itemStyle: { color: PALETTE[i % PALETTE.length] } })) }],
  });
}

async function aumSeg() {
  const d = await q(`SELECT rfm_segment seg, sum(total_deposit_balance)/1e9 ty FROM mart ${W()} GROUP BY 1`);
  const map = Object.fromEntries(d.map((r) => [r.seg, Number(r.ty)]));
  const cats = RFM_ORDER.filter((s) => map[s] != null);
  getChart("aumSeg").setOption({
    tooltip: { ...tooltipAxis, valueFormatter: (v) => vn(v, 1) + " tỷ" }, grid: baseGrid,
    xAxis: { type: "category", data: cats, axisLabel: { color: AXIS, interval: 0, rotate: 25 }, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ type: "bar", data: cats.map((s) => map[s].toFixed(1)), barWidth: "55%",
      itemStyle: { color: C.blue, borderRadius: [4, 4, 0, 0] } }],
  });
}

async function rmScatter() {
  const d = await q(`SELECT recency_days rec, monetary_12m/1e6 mon, frequency_12m freq, customer_segment seg
    FROM mart ${W("recency_days IS NOT NULL")}`);
  const bySeg = {};
  for (const r of d) (bySeg[r.seg] ||= []).push([Number(r.rec), Number(r.mon), Number(r.freq)]);
  getChart("rmScatter").setOption({
    tooltip: { ...tooltip, formatter: (p) => `Recency ${p.value[0]}d<br/>Monetary ${vn(p.value[1], 1)} tr<br/>Freq ${p.value[2]}` },
    legend: { bottom: 0, textStyle: { color: AXIS } },
    grid: baseGrid,
    xAxis: { type: "value", name: "Recency (ngày)", nameLocation: "middle", nameGap: 26, ...axisStyle },
    yAxis: { type: "value", name: "Monetary (triệu)", ...axisStyle },
    series: Object.entries(bySeg).map(([seg, pts]) => ({
      name: seg, type: "scatter", symbolSize: 7, data: pts,
      itemStyle: { color: SEG_COLORS[seg], opacity: 0.7 } })),
  });
}

// ── TAB San pham ────────────────────────────────────────────────────────────
async function prodCount() {
  const d = await q(`SELECT total_product_count k, count(*) n FROM mart ${W()} GROUP BY 1 ORDER BY 1`);
  getChart("prodCount").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "category", data: d.map((r) => r.k), name: "Số SP", nameLocation: "middle", nameGap: 26, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ type: "bar", data: d.map((r) => Number(r.n)), barWidth: "55%",
      itemStyle: { color: C.blue, borderRadius: [4, 4, 0, 0] },
      label: { show: true, position: "top", color: "#e2e8f0" } }],
  });
}

async function aumBand() {
  const d = await q(`
    SELECT band, count(*) n FROM (
      SELECT CASE
        WHEN total_deposit_balance = 0 THEN '0. Không gửi'
        WHEN total_deposit_balance < 5e7 THEN '1. <50tr'
        WHEN total_deposit_balance < 2e8 THEN '2. 50-200tr'
        WHEN total_deposit_balance < 5e8 THEN '3. 200-500tr'
        WHEN total_deposit_balance < 1e9 THEN '4. 500tr-1 tỷ'
        ELSE '5. 1 tỷ+' END AS band
      FROM mart ${W()}
    ) GROUP BY band ORDER BY band`);
  getChart("aumBand").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "category", data: d.map((r) => r.band), axisLabel: { color: AXIS, interval: 0, rotate: 20 }, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ type: "bar", data: d.map((r) => Number(r.n)), barWidth: "55%",
      itemStyle: { color: C.blueDk, borderRadius: [4, 4, 0, 0] } }],
  });
}

async function penSeg() {
  const d = await q(`SELECT customer_segment seg,
      avg(CASE WHEN has_savings THEN 1.0 ELSE 0 END) sav,
      avg(CASE WHEN has_credit_card THEN 1.0 ELSE 0 END) card,
      avg(CASE WHEN has_loan THEN 1.0 ELSE 0 END) loan
    FROM mart ${W()} GROUP BY 1`);
  const segs = d.map((r) => r.seg);
  const series = [
    { name: "Tiết kiệm", key: "sav", color: C.blue },
    { name: "Thẻ TD", key: "card", color: C.slate },
    { name: "Vay", key: "loan", color: C.navy },
  ].map((s) => ({ name: s.name, type: "bar", itemStyle: { color: s.color },
    data: d.map((r) => Number(r[s.key])) }));
  getChart("penSeg").setOption({
    tooltip: { ...tooltipAxis, valueFormatter: (v) => pct(v) },
    legend: { bottom: 0, textStyle: { color: AXIS } }, grid: baseGrid,
    xAxis: { type: "category", data: segs, ...axisStyle },
    yAxis: { type: "value", max: 1, axisLabel: { formatter: (v) => pct(v, 0), color: AXIS }, ...axisStyle },
    series,
  });
}

async function catBar() {
  const d = await q(`SELECT top_spend_category c, count(*) n FROM mart ${W("top_spend_category IS NOT NULL")}
    GROUP BY 1 ORDER BY n DESC`);
  getChart("catBar").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "value", ...axisStyle },
    yAxis: { type: "category", data: d.map((r) => r.c).reverse(), ...axisStyle },
    series: [{ type: "bar", data: d.map((r) => Number(r.n)).reverse(), barWidth: "60%",
      itemStyle: { color: C.slate, borderRadius: [0, 4, 4, 0] },
      label: { show: true, position: "right", color: "#e2e8f0" } }],
  });
}

// ── TAB Rui ro & Cross-sell ─────────────────────────────────────────────────
async function nplSeg() {
  const d = await q(`SELECT customer_segment seg,
      CAST(sum(CASE WHEN has_npl_loan THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(sum(CASE WHEN has_loan THEN 1 ELSE 0 END),0) AS rate
    FROM mart ${W()} GROUP BY 1`);
  getChart("nplSeg").setOption({
    tooltip: { ...tooltipAxis, valueFormatter: (v) => pct(v) }, grid: baseGrid,
    xAxis: { type: "category", data: d.map((r) => r.seg), ...axisStyle },
    yAxis: { type: "value", axisLabel: { formatter: (v) => pct(v, 0), color: AXIS }, ...axisStyle },
    series: [{ type: "bar", data: d.map((r) => Number(r.rate || 0)), barWidth: "45%",
      itemStyle: { color: C.red, borderRadius: [4, 4, 0, 0] },
      label: { show: true, position: "top", color: "#e2e8f0", formatter: (p) => pct(p.value) } }],
  });
}

async function leadBar() {
  const [r] = await q(`SELECT
      sum(CASE WHEN cross_sell_loan_flag THEN 1 ELSE 0 END) loan,
      sum(CASE WHEN cross_sell_card_flag THEN 1 ELSE 0 END) card,
      sum(CASE WHEN cross_sell_invest_flag THEN 1 ELSE 0 END) invest
    FROM mart ${W()}`);
  getChart("leadBar").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "category", data: ["Vay", "Thẻ TD", "Đầu tư"], ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ type: "bar", barWidth: "45%",
      data: [
        { value: Number(r.loan), itemStyle: { color: C.blue } },
        { value: Number(r.card), itemStyle: { color: C.slate } },
        { value: Number(r.invest), itemStyle: { color: C.blueDk } },
      ],
      itemStyle: { borderRadius: [4, 4, 0, 0] },
      label: { show: true, position: "top", color: "#e2e8f0" } }],
  });
}

async function leadSeg() {
  const d = await q(`SELECT customer_segment seg,
      sum(CASE WHEN cross_sell_loan_flag THEN 1 ELSE 0 END) loan,
      sum(CASE WHEN cross_sell_card_flag THEN 1 ELSE 0 END) card,
      sum(CASE WHEN cross_sell_invest_flag THEN 1 ELSE 0 END) invest
    FROM mart ${W()} GROUP BY 1`);
  const segs = d.map((r) => r.seg);
  const series = [
    { name: "Vay", key: "loan", color: C.blue },
    { name: "Thẻ TD", key: "card", color: C.slate },
    { name: "Đầu tư", key: "invest", color: C.blueDk },
  ].map((s) => ({ name: s.name, type: "bar", itemStyle: { color: s.color }, data: d.map((r) => Number(r[s.key])) }));
  getChart("leadSeg").setOption({
    tooltip: tooltipAxis, legend: { bottom: 0, textStyle: { color: AXIS } }, grid: baseGrid,
    xAxis: { type: "category", data: segs, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series,
  });
}

async function utilBand() {
  const d = await q(`
    SELECT band, count(*) n FROM (
      SELECT CASE
        WHEN credit_utilization_rate = 0 THEN '0%'
        WHEN credit_utilization_rate < 0.3 THEN '1. <30%'
        WHEN credit_utilization_rate < 0.6 THEN '2. 30-60%'
        WHEN credit_utilization_rate < 0.9 THEN '3. 60-90%'
        ELSE '4. ≥90%' END AS band
      FROM mart ${W("has_credit_card = true")}
    ) GROUP BY band ORDER BY band`);
  getChart("utilBand").setOption({
    tooltip: tooltipAxis, grid: baseGrid,
    xAxis: { type: "category", data: d.map((r) => r.band), axisLabel: { color: AXIS, interval: 0, rotate: 15 }, ...axisStyle },
    yAxis: { type: "value", ...axisStyle },
    series: [{ type: "bar", data: d.map((r) => Number(r.n)), barWidth: "50%",
      itemStyle: { color: C.amber, borderRadius: [4, 4, 0, 0] } }],
  });
}

// ── Orchestration ───────────────────────────────────────────────────────────
const RENDERERS = [
  renderKpis, segDonut, penBar, rfmBar, provBar,
  txnTrend, activeTrend, channelTrend, snapTrend,
  rfmHeat, rfmTree, aumSeg, rmScatter,
  prodCount, aumBand, penSeg, catBar,
  nplSeg, leadBar, leadSeg, utilBand,
];

async function renderAll() {
  await Promise.all(RENDERERS.map((fn) => fn()));
  // resize cac chart o tab dang hien
  Object.values(charts).forEach((c) => c.resize());
}

// Tai 1 parquet -> tao table. Tra ve true neu thanh cong (file co that).
async function tryLoad(db, file, table) {
  try {
    const res = await fetch(`./data/${file}`);
    if (!res.ok) return false;
    const buf = new Uint8Array(await res.arrayBuffer());
    await db.registerFileBuffer(file, buf);
    await conn.query(`CREATE TABLE ${table} AS SELECT * FROM read_parquet('${file}')`);
    return true;
  } catch { return false; }
}

async function initDB() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
  );
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();

  // mart bat buoc; cac bang xu huong tuy chon (resilient voi du lieu cu/moi)
  if (!await tryLoad(db, "mart_customer_360.parquet", "mart"))
    throw new Error("Khong tai duoc mart_customer_360.parquet");
  HAS.dtrend  = await tryLoad(db, "daily_trend.parquet", "dtrend");
  HAS.mtrend  = await tryLoad(db, "monthly_trend.parquet", "mtrend");   // file cu (neu con)
  HAS.dactive = await tryLoad(db, "daily_active.parquet", "dactive");
  HAS.mactive = await tryLoad(db, "monthly_active.parquet", "mactive");
}

function fillSelect(id, values, selected) {
  const el = document.getElementById(id);
  el.innerHTML = values.map((v) => `<option value="${v}"${v === selected ? " selected" : ""}>${v}</option>`).join("");
}

async function distinctMonths() {
  if (HAS.dtrend) return (await q(`SELECT DISTINCT strftime(day,'%Y-%m') m FROM dtrend ORDER BY 1`)).map((r) => r.m);
  if (HAS.mtrend) return (await q(`SELECT DISTINCT strftime(month,'%Y-%m') m FROM mtrend ORDER BY 1`)).map((r) => r.m);
  if (HAS.mactive) return (await q(`SELECT DISTINCT strftime(month,'%Y-%m') m FROM mactive ORDER BY 1`)).map((r) => r.m);
  return [];
}

async function initFilters() {
  // Snapshot tu mart; khoang thang tu bang xu huong co san
  const snaps = (await q(`SELECT DISTINCT strftime(snapshot_date, '%Y-%m-%d') s FROM mart ORDER BY 1`)).map((r) => r.s);
  const months = await distinctMonths();
  state.snapshot = snaps[snaps.length - 1];
  state.mFrom = months[0] ?? null;
  state.mTo = months[months.length - 1] ?? null;
  fillSelect("snapFilter", snaps, state.snapshot);
  fillSelect("monthFrom", months, state.mFrom);
  fillSelect("monthTo", months, state.mTo);

  // Nut do phan giai Ngay/Thang — chi cho "Ngay" khi co du file daily
  const canDay = HAS.dtrend && HAS.dactive;
  const opts = canDay ? [["month", "Tháng"], ["day", "Ngày"]] : [["month", "Tháng"]];
  document.getElementById("granFilter").innerHTML =
    opts.map(([v, l]) => `<option value="${v}">${l}</option>`).join("");
  document.getElementById("granWrap").style.display = canDay ? "" : "none";
}

async function loadMeta() {
  try {
    const m = await (await fetch("./data/meta.json")).json();
    document.getElementById("meta").textContent =
      `Snapshot ${m.snapshot_date} · ${vn(m.row_count)} KH`;
  } catch { /* optional */ }
}

function wireUI() {
  const bind = (id, key) => document.getElementById(id).addEventListener("change", (e) => {
    state[key] = e.target.value;
    // Khoang thang hop le: from <= to
    if (state.mFrom > state.mTo) {
      if (key === "mFrom") { state.mTo = state.mFrom; document.getElementById("monthTo").value = state.mTo; }
      else { state.mFrom = state.mTo; document.getElementById("monthFrom").value = state.mFrom; }
    }
    renderAll();
  });
  bind("segFilter", "segment");
  bind("snapFilter", "snapshot");
  bind("monthFrom", "mFrom");
  bind("monthTo", "mTo");
  bind("granFilter", "grain");

  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      const tab = btn.dataset.tab;
      document.querySelectorAll(".panel").forEach((p) =>
        p.classList.toggle("hidden", p.dataset.panel !== tab));
      Object.values(charts).forEach((c) => c.resize());
    });
  });
  window.addEventListener("resize", () => Object.values(charts).forEach((c) => c.resize()));
}

(async function main() {
  try {
    await initDB();
    await initFilters();
    await loadMeta();
    wireUI();
    await renderAll();
    document.getElementById("loading").classList.add("hidden");
  } catch (err) {
    document.getElementById("loading").innerHTML =
      `<p style="color:#b66a6a">Lỗi nạp dashboard:<br/>${err.message}</p>`;
    console.error(err);
  }
})();
