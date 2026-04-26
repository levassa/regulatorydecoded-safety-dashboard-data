/**
 * Product Safety Dashboard — Frontend
 * regulatorydecoded.com
 *
 * Fetches pre-aggregated JSON from jsDelivr CDN and renders:
 *   - KPI cards  (total alerts, injury reports, fatal count, sources)
 *   - Monthly trend chart by source
 *   - Severity trend chart  (Fatal / Serious / Moderate / Minor over time)
 *   - Hazard type bar chart (root cause analysis)
 *   - Product category bar chart
 *   - Injury type doughnut chart
 *   - Filter chips: region, category, severity
 *   - Sortable alerts table with hazard / injury / severity columns
 *   - CSV export
 *
 * SETUP: set GITHUB_USER and REPO_NAME below to your values.
 */

const GITHUB_USER = "levassa";
const REPO_NAME   = "regulatorydecoded-safety-dashboard-data";

const CDN_BASE = `https://cdn.jsdelivr.net/gh/${GITHUB_USER}/${REPO_NAME}@main/data`;

const URLS = {
  meta:             `${CDN_BASE}/meta.json`,
  byCategory:       `${CDN_BASE}/by_category.json`,
  byRegion:         `${CDN_BASE}/by_region.json`,
  byCountry:        `${CDN_BASE}/by_country_origin.json`,
  trendsMonthly:    `${CDN_BASE}/trends_monthly.json`,
  recentAlerts:     `${CDN_BASE}/recent_alerts.json`,
  riskBreakdown:    `${CDN_BASE}/risk_breakdown.json`,
  byHazardType:     `${CDN_BASE}/by_hazard_type.json`,
  byInjuryType:     `${CDN_BASE}/by_injury_type.json`,
  bySeverity:       `${CDN_BASE}/by_severity.json`,
  severityTrends:   `${CDN_BASE}/severity_trends_monthly.json`,
  hazardTrends:     `${CDN_BASE}/hazard_trends_monthly.json`,
};

// ---------------------------------------------------------------------------
// Colour palettes
// ---------------------------------------------------------------------------

const SOURCE_COLOURS = {
  "EU Safety Gate": "#003399",
  "CPSC":           "#BF0A30",
  "Health Canada":  "#CC0000",
  "RappelConso":    "#0055a4",
  "FDA":            "#1a6faf",
  "NHTSA":          "#c1121f",
  "OPSS":           "#012169",
  "ACCC":           "#00843d",
};

const SEVERITY_COLOURS = {
  "Fatal":        "#e63946",
  "Serious":      "#fb8500",
  "Moderate":     "#ffb703",
  "Minor":        "#2d6a4f",
  "Not Reported": "#adb5bd",
};

const HAZARD_COLOURS = [
  "#003399","#e63946","#fb8500","#2d6a4f",
  "#8338ec","#023e8a","#0055a4","#c1121f",
  "#ffb703","#00843d","#012169","#6c757d",
];

const CHART_PALETTE = [
  "#003399","#BF0A30","#fb8500","#2d6a4f",
  "#8338ec","#023e8a","#0055a4","#c1121f",
  "#ffb703","#00843d","#012169","#6c757d",
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let state = {
  activeRegion:   "All",
  activeCategory: "All",
  activeSeverity: "All",
  recentAlerts:   [],
  sortCol:        "date",
  sortAsc:        false,
  charts:         {},
};

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} — ${url}`);
  return res.json();
}

async function fetchAll() {
  const keys = Object.keys(URLS);
  const values = await Promise.all(keys.map(k => fetchJSON(URLS[k])));
  return Object.fromEntries(keys.map((k, i) => [k, values[i]]));
}

// ---------------------------------------------------------------------------
// KPI cards
// ---------------------------------------------------------------------------

function renderKPIs(meta) {
  const container = document.getElementById("sd-kpi-row");
  if (!container) return;

  const sources = meta.by_source || {};
  const sev     = meta.by_severity || {};
  const lastUpdated = meta.last_updated
    ? new Date(meta.last_updated).toLocaleDateString("en-GB", {
        day: "numeric", month: "short", year: "numeric",
      })
    : "—";

  const cards = [
    { label: "Total Alerts",    value: (meta.total_records  || 0).toLocaleString(), accent: "#1a1a2e" },
    { label: "Injury Reports",  value: (meta.injury_reports || 0).toLocaleString(), accent: "#fb8500" },
    { label: "Fatal",           value: (sev["Fatal"]        || 0).toLocaleString(), accent: "#e63946" },
    { label: "Serious",         value: (sev["Serious"]      || 0).toLocaleString(), accent: "#c1121f" },
    { label: "EU Safety Gate",  value: (sources["EU Safety Gate"] || 0).toLocaleString(), accent: "#003399" },
    { label: "CPSC (USA)",      value: (sources["CPSC"]     || 0).toLocaleString(), accent: "#BF0A30" },
    { label: "FDA",             value: (sources["FDA"]      || 0).toLocaleString(), accent: "#1a6faf" },
    { label: "Last Updated",    value: lastUpdated,                                 accent: "#2d6a4f" },
  ];

  container.innerHTML = cards.map(c => `
    <div class="sd-kpi-card" style="border-top:4px solid ${c.accent}">
      <div class="sd-kpi-value">${c.value}</div>
      <div class="sd-kpi-label">${c.label}</div>
    </div>
  `).join("");
}

// ---------------------------------------------------------------------------
// Monthly trend by source
// ---------------------------------------------------------------------------

function renderTrendChart(trends) {
  const ctx = document.getElementById("sd-trend-chart");
  if (!ctx || !trends.length) return;
  if (state.charts.trend) state.charts.trend.destroy();

  const labels  = trends.map(t => t.month);
  const sources = Object.keys(trends[0]).filter(k => k !== "month");

  state.charts.trend = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: sources.map(src => ({
        label: src,
        data: trends.map(t => t[src] || 0),
        borderColor: SOURCE_COLOURS[src] || "#888",
        backgroundColor: (SOURCE_COLOURS[src] || "#888") + "22",
        borderWidth: 2,
        pointRadius: 2,
        tension: 0.3,
        fill: false,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "top" } },
      scales: {
        x: { ticks: { maxTicksLimit: 12 } },
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Severity trend chart
// ---------------------------------------------------------------------------

function renderSeverityTrendChart(trends) {
  const ctx = document.getElementById("sd-severity-trend-chart");
  if (!ctx || !trends.length) return;
  if (state.charts.severityTrend) state.charts.severityTrend.destroy();

  const labels   = trends.map(t => t.month);
  const sevKeys  = Object.keys(trends[0]).filter(k => k !== "month");
  const order    = ["Fatal", "Serious", "Moderate", "Minor", "Not Reported"];
  const sorted   = [...sevKeys].sort(
    (a, b) => (order.indexOf(a) - order.indexOf(b))
  );

  state.charts.severityTrend = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: sorted.map(sev => ({
        label: sev,
        data: trends.map(t => t[sev] || 0),
        borderColor: SEVERITY_COLOURS[sev] || "#888",
        backgroundColor: (SEVERITY_COLOURS[sev] || "#888") + "22",
        borderWidth: 2,
        pointRadius: 2,
        tension: 0.3,
        fill: false,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "top" } },
      scales: {
        x: { ticks: { maxTicksLimit: 12 } },
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Hazard type bar chart (root cause)
// ---------------------------------------------------------------------------

function renderHazardChart(byHazardType) {
  const ctx = document.getElementById("sd-hazard-chart");
  if (!ctx || !byHazardType.length) return;
  if (state.charts.hazard) state.charts.hazard.destroy();

  const top    = byHazardType.filter(h => h.hazard_type !== "Other").slice(0, 12);
  const labels = top.map(h => h.hazard_type);
  const values = top.map(h => h.total);

  state.charts.hazard = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Alerts",
        data: values,
        backgroundColor: HAZARD_COLOURS.slice(0, labels.length),
        borderWidth: 0,
      }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { beginAtZero: true, ticks: { precision: 0 } },
        y: { ticks: { font: { size: 11 } } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Category bar chart
// ---------------------------------------------------------------------------

function renderCategoryChart(byCategory) {
  const ctx = document.getElementById("sd-category-chart");
  if (!ctx || !byCategory.length) return;
  if (state.charts.category) state.charts.category.destroy();

  const top    = byCategory.slice(0, 12);
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
// Injury type doughnut
// ---------------------------------------------------------------------------

function renderInjuryChart(byInjuryType) {
  const ctx = document.getElementById("sd-injury-chart");
  if (!ctx || !byInjuryType.length) return;
  if (state.charts.injury) state.charts.injury.destroy();

  const top    = byInjuryType.filter(i => i.injury_type !== "Not Specified").slice(0, 8);
  const labels = top.map(i => i.injury_type);
  const values = top.map(i => i.total);

  state.charts.injury = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: CHART_PALETTE.slice(0, labels.length),
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
// Severity doughnut
// ---------------------------------------------------------------------------

function renderSeverityChart(bySeverity) {
  const ctx = document.getElementById("sd-severity-chart");
  if (!ctx || !bySeverity.length) return;
  if (state.charts.severity) state.charts.severity.destroy();

  const order = ["Fatal", "Serious", "Moderate", "Minor", "Not Reported"];
  const sorted = [...bySeverity].sort(
    (a, b) => order.indexOf(a.severity) - order.indexOf(b.severity)
  );
  const labels = sorted.map(s => s.severity);
  const values = sorted.map(s => s.total);
  const colours = sorted.map(s => SEVERITY_COLOURS[s.severity] || "#888");

  state.charts.severity = new Chart(ctx, {
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
  const regionEl   = document.getElementById("sd-region-filters");
  const categoryEl = document.getElementById("sd-category-filters");
  const severityEl = document.getElementById("sd-severity-filters");
  if (!regionEl || !categoryEl) return;

  const regions    = ["All", ...byRegion.map(r => r.region)];
  const categories = ["All", ...byCategory.slice(0, 12).map(c => c.category)];
  const severities = ["All", "Fatal", "Serious", "Moderate", "Minor", "Not Reported"];

  regionEl.innerHTML = regions.map(r => `
    <button class="sd-chip ${state.activeRegion === r ? "sd-chip--active" : ""}"
            data-filter-type="region" data-value="${r}">${r}</button>
  `).join("");

  categoryEl.innerHTML = categories.map(c => `
    <button class="sd-chip ${state.activeCategory === c ? "sd-chip--active" : ""}"
            data-filter-type="category" data-value="${c}">${c}</button>
  `).join("");

  if (severityEl) {
    severityEl.innerHTML = severities.map(s => {
      const col = s !== "All" ? `style="--chip-active:${SEVERITY_COLOURS[s]}"` : "";
      return `<button class="sd-chip ${state.activeSeverity === s ? "sd-chip--active" : ""}"
              data-filter-type="severity" data-value="${s}" ${col}>${s}</button>`;
    }).join("");
  }

  document.querySelectorAll(".sd-chip").forEach(btn => {
    btn.addEventListener("click", () => {
      const type  = btn.dataset.filterType;
      const value = btn.dataset.value;
      if (type === "region")   state.activeRegion   = value;
      if (type === "category") state.activeCategory = value;
      if (type === "severity") state.activeSeverity = value;
      renderTable(state.recentAlerts);
      document.querySelectorAll(`[data-filter-type="${type}"]`).forEach(b =>
        b.classList.toggle("sd-chip--active", b.dataset.value === value)
      );
    });
  });
}

// ---------------------------------------------------------------------------
// Alerts table
// ---------------------------------------------------------------------------

function filteredAlerts(alerts) {
  return alerts.filter(a => {
    const regionOk   = state.activeRegion   === "All" || a.region           === state.activeRegion;
    const catOk      = state.activeCategory === "All" || a.product_category === state.activeCategory;
    const severityOk = state.activeSeverity === "All" || a.severity         === state.activeSeverity;
    return regionOk && catOk && severityOk;
  });
}

function renderTable(alerts) {
  const tbody   = document.getElementById("sd-table-body");
  const countEl = document.getElementById("sd-table-count");
  if (!tbody) return;

  const rows = filteredAlerts(alerts);

  rows.sort((a, b) => {
    let av = a[state.sortCol] ?? "";
    let bv = b[state.sortCol] ?? "";
    if (typeof av === "string") av = av.toLowerCase();
    if (typeof bv === "string") bv = bv.toLowerCase();
    if (av < bv) return state.sortAsc ? -1 : 1;
    if (av > bv) return state.sortAsc ? 1 : -1;
    return 0;
  });

  const display = rows.slice(0, 50);
  if (countEl) countEl.textContent =
    `Showing ${display.length} of ${rows.length} matching alerts`;

  tbody.innerHTML = display.map(a => {
    const srcColour = SOURCE_COLOURS[a.source]   || "#555";
    const sevColour = SEVERITY_COLOURS[a.severity] || "#adb5bd";
    const injFlag   = a.injury_flag
      ? `<span class="sd-badge" style="background:#fb8500">⚠ Injury</span>`
      : "";
    return `
      <tr>
        <td>${a.date || "—"}</td>
        <td><span class="sd-badge" style="background:${srcColour}">${escHtml(a.source)}</span></td>
        <td>${escHtml(a.product_desc || "—")}</td>
        <td>${escHtml(a.product_category || "—")}</td>
        <td>${escHtml(a.hazard_type || "—")}</td>
        <td>${escHtml(a.injury_type || "—")}</td>
        <td><span class="sd-badge" style="background:${sevColour}">${escHtml(a.severity || "—")}</span> ${injFlag}</td>
        <td>${escHtml(a.notifying_country || "—")}</td>
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
  const cols = [
    "date", "source", "region", "product_category", "product_desc",
    "hazard_type", "injury_type", "severity", "injury_flag", "injury_count",
    "injury_description", "notifying_country", "country_of_origin",
    "corrective_action", "reference",
  ];
  const header = cols.join(",");
  const lines  = rows.map(r =>
    cols.map(c => `"${(r[c] ?? "").toString().replace(/"/g, '""')}"`).join(",")
  );
  const csv  = [header, ...lines].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href     = url;
  a.download = `safety-alerts-${new Date().toISOString().slice(0, 10)}.csv`;
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
    renderTrendChart(data.trendsMonthly);
    renderSeverityTrendChart(data.severityTrends);
    renderHazardChart(data.byHazardType);
    renderCategoryChart(data.byCategory);
    renderInjuryChart(data.byInjuryType);
    renderSeverityChart(data.bySeverity);
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
    showError(`Failed to load dashboard data: ${err.message}`);
  }
}

document.addEventListener("DOMContentLoaded", init);
