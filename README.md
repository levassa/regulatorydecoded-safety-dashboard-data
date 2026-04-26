<<<<<<< HEAD
# Product Safety Dashboard

Automated pipeline that collects product recall and safety alert data from three public
open-data sources, normalises it to a unified schema, and serves it as static JSON via
jsDelivr CDN to a Chart.js-powered WordPress dashboard.

---

## Repository structure

```
.github/workflows/update_data.yml   GitHub Actions scheduler
pipeline/collect_and_normalise.py   Python data pipeline
dashboard/dashboard.js              Frontend Chart.js logic
dashboard/wordpress-block.html      WordPress Custom HTML block
data/                               Auto-generated JSON (created on first run)
README.md                           This file
```

---

## Data sources

| Tier | Source | Access | Schedule |
|------|--------|--------|----------|
| Primary | EU Safety Gate (RAPEX) | OpenDataSoft REST API | Friday 09:00 UTC |
| Primary | CPSC (USA) | SaferProducts.gov REST API | Friday 09:00 UTC |
| Primary | Health Canada | Open Government JSON feed | Sunday 06:00 UTC |

All sources are free, public, and require no API key.

---

## Setup — step by step

### 1. Create a public GitHub repository

Go to [github.com/new](https://github.com/new). Name it (e.g. `safety-dashboard-data`).
Set visibility to **Public** — jsDelivr CDN only mirrors public repos.

### 2. Edit the two constants in `dashboard/dashboard.js`

Open `dashboard/dashboard.js` and replace the placeholders at the top:

```js
const GITHUB_USER = "YOUR_GITHUB_USERNAME";   // ← your GitHub username
const REPO_NAME   = "YOUR_REPO_NAME";         // ← your repository name
```

### 3. Edit the script tag in `dashboard/wordpress-block.html`

Near the bottom of the file, replace the same two placeholders in the `<script src>` tag:

```html
<script src="https://cdn.jsdelivr.net/gh/YOUR_GITHUB_USERNAME/YOUR_REPO_NAME@main/dashboard/dashboard.js"></script>
```

### 4. Commit and push all files

```bash
git init
git remote add origin https://github.com/YOUR_GITHUB_USERNAME/YOUR_REPO_NAME.git
git add .
git commit -m "chore: initial dashboard setup"
git push -u origin main
```

### 5. Trigger the first pipeline run manually

Go to your repo on GitHub → **Actions** tab → **Update Safety Dashboard Data** → **Run workflow**.

The pipeline takes approximately 2–3 minutes to run. When it completes, a `/data/` folder
will appear in the repo containing all seven JSON files:

```
data/meta.json
data/by_category.json
data/by_region.json
data/by_country_origin.json
data/trends_monthly.json
data/recent_alerts.json
data/risk_breakdown.json
```

### 6. Verify CDN delivery

Open this URL in your browser (replace with your details):

```
https://cdn.jsdelivr.net/gh/YOUR_GITHUB_USERNAME/YOUR_REPO_NAME@main/data/meta.json
```

You should see JSON with `last_updated`, `total_records`, and `by_source` fields.

> **Note:** jsDelivr CDN has a cache TTL of up to 24 hours after the first request.
> To force an immediate cache purge, use the tool at https://www.jsdelivr.com/tools/purge

### 7. Add the dashboard to WordPress

1. Create or edit the page where you want the dashboard to appear.
2. Add a **Custom HTML** block.
3. Paste the entire contents of `dashboard/wordpress-block.html` into it.
4. Publish or update the page.

---

## Automated schedule

The pipeline runs automatically via GitHub Actions on the free tier — no server required.

| Day | Time (UTC) | Reason |
|-----|-----------|--------|
| Friday | 09:00 | EU Safety Gate publishes its weekly CSV on Fridays |
| Sunday | 06:00 | Health Canada daily feed sweep |

You can also trigger a manual run at any time from the Actions tab.

---

## Output JSON schema

All seven files use only normalised, deduplicated data. The per-record schema is:

| Field | Description |
|-------|-------------|
| `id` | SHA-1 hash of source + reference — deduplication key |
| `source` | `"EU Safety Gate"` \| `"CPSC"` \| `"Health Canada"` |
| `date` | ISO date string `YYYY-MM-DD` |
| `region` | `"EU / EEA"` \| `"USA"` \| `"Canada"` |
| `notifying_country` | Country that issued the alert |
| `country_of_origin` | Manufacturing origin country |
| `product_category` | Normalised category taxonomy |
| `product_desc` | Free-text product description |
| `risk_type` | Normalised risk taxonomy |
| `corrective_action` | Recall / withdrawal / ban description |
| `reference` | Source-specific alert number |

---

## Adding a new data source

Follow this pattern in `collect_and_normalise.py`:

1. Add a `fetch_SOURCENAME()` function using `requests.get()`.
2. Add a `normalise_SOURCENAME()` function that maps raw fields to the unified schema.
3. Call both in `run()` and append the resulting DataFrame to `frames`.
4. Add the source colour in `dashboard.js` under `SOURCE_COLOURS`.
5. Add the source URL and schedule to the sources table in this README.

Category and risk type normalisation maps are the `CATEGORY_MAP` and `RISK_MAP` dicts
in `collect_and_normalise.py` — extend them when new raw values appear in the pipeline logs.

---

## Known limitations

- The EU Safety Gate OpenDataSoft mirror may lag the official source by a variable amount.
- The CPSC API occasionally returns HTTP 5xx errors. The pipeline catches these, logs a
  warning, and leaves the previous data intact.
- Health Canada's JSON feed mixes consumer products with food, drugs, and medical devices.
  The pipeline filters to consumer product records only.
- jsDelivr CDN TTL is up to 24 hours. Check `meta.json → last_updated` to verify freshness.
- The choropleth map (D3.js) is a Phase 2 item. `by_country_origin.json` and
  `by_region.json` are already generated and ready to feed it.

---

## Phase 2 — planned extensions

- D3.js choropleth world map (country-of-origin alerts heatmap)
- UK OPSS and ACCC Australia as additional weekly sources
- Severity tagging (`severity_tag`) and injury flag (`injury_flag`) fields
- NEISS / FDA MAUDE overlay for injury count data

---

*Built for [regulatorydecoded.com](https://regulatorydecoded.com) — regulatory compliance and product safety intelligence.*
=======
# regulatorydecoded-safety-dashboard-data
database for regulatorydecoded product safety metrics
>>>>>>> 29575f5d3057f5eca668cc0a1d70b777acd753f8
