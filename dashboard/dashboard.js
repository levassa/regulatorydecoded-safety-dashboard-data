/**
 * Product Safety Dashboard — Frontend
 * Fetches pre-aggregated JSON from jsDelivr CDN and renders:
 *   - KPI cards (total alerts, per-source counts, last updated)
 *   - Multi-line trend chart (Chart.js)
 *   - Horizontal bar chart: top product categories
 *   - Doughnut chart: risk type breakdown
 *   - Sortable recent alerts table
 *   - Filter chips: region and category
 *   - CSV export button
 *
 * SETUP: Replace the two constants below with your GitHub details.
 */

const GITHUB_USER = "YOUR_GITHUB_USERNAME";
const REPO_NAME   = "YOUR_REPO_NAME";

// ---------------------------------------------------------------------------
// CDN base URL — all JSON files are served from here
// ---------------------------------------------------------------------------
const CDN_BASE = `https://cdn.jsdelivr.net/gh/${GITHUB_USER}/${REPO_NAME}@main/data`;

const URLS = {
  meta:           `${CDN_BASE}/meta.json`,
  byCategory:     `${CDN_BASE}/by_category.json`,
  byRegion:       `${CDN_BASE}/by_region.json`,
  byCountry:      `${CDN_BASE}/by_country_origin.json`,
  trendsMonthly:  `${CDN_BASE}/trends_monthly.json`,
  recentAlerts:   `${CDN_BASE}/recent_alerts.json`,
  riskBreakdown:  `${CDN_BASE}/risk_breakdown.json`,
};

// Source colour palette
const SOURCE_COLOURS = {
  "EU Safety Gate": "#003399",
  "CPSC":           "#BF0A30",
  "Health Canada":  "#FF0000",
};

const REGION_COLOURS = {
  "EU / EEA": "#003399",
  "USA":      "#BF0A30",
  "Canada":   "#FF0000",
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let state = {
  activeRegion:   "All",
  activeCategory: "All",
  recentAlerts:   [],
  sortCol:        "date",
  sortAsc:        false,
  charts:         {},
};

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------
async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} — ${url}`);
  return res.json();
}

async function fetchAll() {
  const [meta, byCategory, byRegion, byCountry, trends, recentAlerts, riskBreakdown] =
    await Promise.all([
      fetchJSON(URLS.meta),
      fetchJSON(URLS.byCategory),
      fetchJSON(URLS.byRegion),
      fetchJSON(URLS.byCountry),
      fetchJSON(URLS.trendsMonthly),
      fetchJSON(URLS.recentAlerts),
      fetchJSON(URLS.riskBreakdown),
    ]);
  return { meta, byCategory, byRegion, byCountry, trends, recentAlerts, riskBreakdown };
}

// ---------------------------------------------------------------------------
// KPI cards
// ---------------------------------------------------------------------------
function renderKPIs(meta) {
  const container = document.getElementById("sd-kpi-row");
  if (!container) return;

  const sources = meta.by_source || {};
  const lastUpdated = meta.last_updated
    ? new Date(meta.last_updated).toLocaleDateString("en-GB", {
        day: "numeric", month: "short", year: "numeric",
      })
    : "—";

  const cards = [
    { label: "Total Alerts", value: (meta.total_records || 0).toLocaleString(), accent: "#1a1a2e" },
    { label: "EU Safety Gate", value: (sources["EU Safety Gate"] || 0).toLocaleString(), accent: "#003399" },
    { label: "CPSC (USA)", value: (sources["CPSC"] || 0).toLocaleString(), accent: "#BF0A30" },
    { label: "Health Canada", value: (sources["Health Canada"] || 0).toLocaleString(), accent: "#CC0000" },
    { label: "Last Updated", value: lastUpdated, accent: "#2d6a4f" },
  ];

  container.innerHTML = cards.map(c => `
    <div class="sd-kpi-card" style="border-top: 4px solid ${c.accent}">
      <div class="sd-kpi-value">${c.value}</div>
      <div class="sd-kpi-label">${c.label}</div>
    </div>
  `).join("");
}

// ---------------------------------------------------------------------------
// Trend chart (multi-line, monthly)
// ---------------------------------------------------------------------------
function renderTrendChart(trends) {
  const ctx = document.getElementById("sd-trend-chart");
  if (!ctx) return;
  if (state.charts.trend) state.charts.trend.destroy();

  const labels = trends.map(t => t.month);
  const sources = Object.keys(trends[0] || {}).filter(k => k !== "month");

  const datasets = sources.map(src => ({
    label: src,
    data: trends.map(t => t[src] || 0),
    borderColor: SOURCE_COLOURS[src] || "#888",
    backgroundColor: (SOURCE_COLOURS[src] || "#888") + "22",
    borderWidth: 2,
    pointRadius: 3,
    tension: 0.3,
    fill: false,
  }));

  state.charts.trend = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "top" },
        title: { display: false },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 12 } },
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Category bar chart
// ---------------------------------------------------------------------------
function renderCategoryChart(byCategory) {
  const ctx = document.getElementById("sd-category-chart");
  if (!ctx) return;
  if (state.charts.category) state.charts.category.destroy();

  const top = byCategory.slice(0, 12);
  const labels = top.map(c => c.category);
  const values = top.map(c => c.total);

  state.charts.category = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Alerts",
        data: values,
        backgroundColor: "#003399cc",
        borderColor: "#003399",
        borderWidth: 1,
      }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Risk doughnut chart
// ---------------------------------------------------------------------------
function renderRiskChart(riskBreakdown) {
  const ctx = document.getElementById("sd-risk-chart");
  if (!ctx) return;
  if (state.charts.risk) state.charts.risk.destroy();

  const top = riskBreakdown.slice(0, 8);
  const labels = top.map(r => `${r.risk_type} (${r.percentage}%)`);
  const values = top.map(r => r.count);
  const colours = [
    "#003399", "#BF0A30", "#FF6B35", "#2d6a4f",
    "#8338ec", "#fb8500", "#023e8a", "#e63946",
  ];

  state.charts.risk = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: colours,
        borderWidth: 2,
        borderColor: "#fff",
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "right", labels: { font: { size: 11 } } },
      },
      cutout: "60%",
    },
  });
}

// ---------------------------------------------------------------------------
// Filter chips
// ---------------------------------------------------------------------------
function renderFilterChips(byRegion, byCategory) {
  const regionEl = document.getElementById("sd-region-filters");
  const categoryEl = document.getElementById("sd-category-filters");
  if (!regionEl || !categoryEl) return;

  const regions = ["All", ...byRegion.map(r => r.region)];
  const categories = ["All", ...byCategory.slice(0, 10).map(c => c.category)];

  regionEl.innerHTML = regions.map(r => `
    <button class="sd-chip ${state.activeRegion === r ? "sd-chip--active" : ""}"
            data-filter-type="region" data-value="${r}">${r}</button>
  `).join("");

  categoryEl.innerHTML = categories.map(c => `
    <button class="sd-chip ${state.activeCategory === c ? "sd-chip--active" : ""}"
            data-filter-type="category" data-value="${c}">${c}</button>
  `).join("");

  document.querySelectorAll(".sd-chip").forEach(btn => {
    btn.addEventListener("click", () => {
      const type = btn.dataset.filterType;
      const value = btn.dataset.value;
      if (type === "region") state.activeRegion = value;
      if (type === "category") state.activeCategory = value;
      renderTable(state.recentAlerts);
      document.querySelectorAll(`[data-filter-type="${type}"]`).forEach(b =>
        b.classList.toggle("sd-chip--active", b.dataset.value === value)
      );
    });
  });
}

// ---------------------------------------------------------------------------
// Recent alerts table
// ---------------------------------------------------------------------------
function filteredAlerts(alerts) {
  return alerts.filter(a => {
    const regionOk = state.activeRegion === "All" || a.region === state.activeRegion;
    const catOk = state.activeCategory === "All" || a.product_category === state.activeCategory;
    return regionOk && catOk;
  });
}

function renderTable(alerts) {
  const tbody = document.getElementById("sd-table-body");
  const countEl = document.getElementById("sd-table-count");
  if (!tbody) return;

  const rows = filteredAlerts(alerts);

  // Sort
  rows.sort((a, b) => {
    let av = a[state.sortCol] ?? "";
    let bv = b[state.sortCol] ?? "";
    if (typeof av === "string") av = av.toLowerCase();
    if (typeof bv === "string") bv = bv.toLowerCase();
    if (av < bv) return state.sortAsc ? -1 : 1;
    if (av > bv) return state.sortAsc ? 1 : -1;
    return 0;
  });

  const display = rows.slice(0, 25);

  if (countEl) countEl.textContent = `Showing ${display.length} of ${rows.length} matching alerts`;

  const riskBadgeColour = {
    "Chemical": "#8338ec",
    "Fire": "#e63946",
    "Electric Shock": "#003399",
    "Injury": "#fb8500",
    "Choking": "#023e8a",
    "Burns": "#BF0A30",
  };

  tbody.innerHTML = display.map(a => {
    const riskColour = riskBadgeColour[a.risk_type] || "#888";
    const srcColour = SOURCE_COLOURS[a.source] || "#555";
    return `
      <tr>
        <td>${a.date || "—"}</td>
        <td><span class="sd-badge" style="background:${srcColour}">${a.source}</span></td>
        <td>${escHtml(a.product_desc || "—")}</td>
        <td>${escHtml(a.product_category || "—")}</td>
        <td><span class="sd-badge" style="background:${riskColour}">${escHtml(a.risk_type || "—")}</span></td>
        <td>${escHtml(a.notifying_country || "—")}</td>
        <td>${escHtml(a.corrective_action || "—")}</td>
      </tr>`;
  }).join("");
}

function setupTableSort() {
  document.querySelectorAll("[data-sort]").forEach(th => {
    th.style.cursor = "pointer";
    th.addEventListener("click", () => {
      const col = th.dataset.sort;
      if (state.sortCol === col) {
        state.sortAsc = !state.sortAsc;
      } else {
        state.sortCol = col;
        state.sortAsc = false;
      }
      renderTable(state.recentAlerts);
    });
  });
}

// ---------------------------------------------------------------------------
// CSV export
// ---------------------------------------------------------------------------
function exportCSV(alerts) {
  const rows = filteredAlerts(alerts);
  const cols = ["date","source","region","product_category","product_desc",
                "risk_type","notifying_country","country_of_origin",
                "corrective_action","reference"];
  const header = cols.join(",");
  const lines = rows.map(r =>
    cols.map(c => `"${(r[c] || "").toString().replace(/"/g, '""')}"`).join(",")
  );
  const csv = [header, ...lines].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `safety-alerts-${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function showError(msg) {
  const el = document.getElementById("sd-error");
  if (el) { el.textContent = msg; el.style.display = "block"; }
  console.error("[Safety Dashboard]", msg);
}

function showLoading(show) {
  const el = document.getElementById("sd-loading");
  if (el) el.style.display = show ? "block" : "none";
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------
async function init() {
  showLoading(true);
  try {
    const data = await fetchAll();
    showLoading(false);

    renderKPIs(data.meta);
    renderTrendChart(data.trends);
    renderCategoryChart(data.byCategory);
    renderRiskChart(data.riskBreakdown);
    renderFilterChips(data.byRegion, data.byCategory);

    state.recentAlerts = data.recentAlerts;
    renderTable(state.recentAlerts);
    setupTableSort();

    const exportBtn = document.getElementById("sd-export-btn");
    if (exportBtn) {
      exportBtn.addEventListener("click", () => exportCSV(state.recentAlerts));
    }
  } catch (err) {
    showLoading(false);
    showError(`Failed to load dashboard data: ${err.message}. Check the browser console for details.`);
  }
}

document.addEventListener("DOMContentLoaded", init);
