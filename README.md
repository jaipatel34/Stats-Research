# Federal Workforce DOGE Study

This project analyzes U.S. federal workforce data to investigate how the Department of Government Efficiency (DOGE) restructuring of 2025–2026 affected federal employees. It examines whether specific agencies and employee profiles were disproportionately impacted, and whether pre-restructuring workforce characteristics can predict which groups experienced the highest separation rates.

Data is ingested from the U.S. Office of Personnel Management (OPM) and stored in a Cloudflare R2 bucket. Notebooks pull directly from the bucket — no local data files required.

---

## Research Question

> Did the 2025 federal workforce restructuring driven by DOGE disproportionately affect specific employee profiles and agencies, and did it produce a chilling effect on voluntary departures beyond the explicit involuntary removals?

### Analysis Design

Three-period natural experiment using OPM administrative data (Jan 2024–Feb 2026, 558 agency subelements):
- **Pre-DOGE**: Jan–Dec 2024 — clean baseline before EO 14210
- **DOGE Active**: Jan–Nov 2025 — EO 14210 in effect
- **Post-DOGE**: Dec 2025–Feb 2026 — 3-month recovery window

**Models**: Aggregate ITS, Panel ITS (agency FE + seasonal controls), event study, agency-specific linear trends (Frisch-Waugh), negative binomial count model, and chi-square demographic disparity tests. Both involuntary and voluntary separation streams analyzed separately.

### Key Findings

**Voluntary separations are the dominant signal**
- Voluntary exits outnumbered involuntary 6-to-1 during DOGE (269,260 vs 43,881)
- Voluntary rate: 0.64%/month baseline → 1.11% DOGE → 1.27% post-DOGE (still elevated)
- Agency-specific trends model: voluntary `doge_active` = **+1.03pp above trend, p = 0.009**; `post_doge` = **+1.48pp, p = 0.007**
- Kruskal-Wallis for voluntary rates across three periods: **p = 0.049** (significant); involuntary: p = 0.132 (not significant)
- Firings stopped when DOGE ended; voluntary departures continued — consistent with a lasting chilling effect

**Involuntary targeting was legally concentrated, not aggregate-significant**
- Aggregate rate 0.115% → 0.180%/month, but does not survive agency-trend controls (Panel ITS `doge_level` p = 0.184)
- The post-DOGE *fall* is significant (Panel ITS `post_level` p = 0.016)
- Involuntary targeting was concentrated in workers with the fewest civil service protections

**Demographic targeting was systematic — two distinct profiles (all chi-square p ≈ 0)**

| Stream | Who was overrepresented |
|---|---|
| Involuntary | No-tenure-group (+46.7pp), nonpermanent competitive (+21.4pp), ages 20–29 (+8–10pp), intermittent schedule (+17.9pp), HS or less (+7pp) |
| Voluntary | Ages 65+ (+7.4pp), 60–64 (+5.7pp), career-conditional (+4.7pp) — consistent with early retirement pressure |

Career permanent workers: −54.1pp involuntary overrepresentation (protected)

**22 agencies experienced workforce collapse (≥20% headcount loss)**
- USAID: **−93.8%** (4,752 → 294) — effectively eliminated
- Defense Human Resources Activity: −49.2%; Federal Student Aid: −47.1%
- CDC: −24.9%; FDA: −20.4%; IRS: −22.1%; Forest Service: −27.2%; National Park Service: −27.5%
- Total federal headcount: ~2.30M → ~2.05M (**−11%, ~250,000 workers**)

**Compositional shift appears permanent**
- Career permanent (tenure group 1): 96.4% retained post-DOGE
- No-tenure-group: 65.8% retained; probationary: 64.9% retained; workers 20–24: 70.1% retained
- The workforce that remains is measurably older and more tenured than the 2024 baseline

---

## Structure

```
federal-doge-study/
├── scripts/
│   ├── data_loader.py       # R2 + local data loaders
│   └── opm_to_r2.py         # Pipeline: OPM website → Cloudflare R2
├── notebooks/
│   ├── info.ipynb           # Data dictionary — columns, types, code lookups
│   ├── separations.ipynb    # Separations analysis (voluntary vs involuntary, agency, profile)
│   ├── employment.ipynb     # Workforce snapshot analysis
│   └── doge_analysis.ipynb  # Combined regression analysis
├── pyproject.toml
├── requirements.txt
└── .env                     # Cloudflare R2 credentials (not committed)
```

---

## Data Source

Data is obtained from the U.S. Office of Personnel Management (OPM):

[https://data.opm.gov/explore-data/data/data-downloads](https://data.opm.gov/explore-data/data/data-downloads)

Datasets covered:

- **Employment** — full workforce snapshots (~1.5–1.7 GB/month, ~2M rows per file)
- **Separations** — departure events per month, with separation category codes

Coverage: **January 2024 – February 2026**

---

## Setup

### Using uv (recommended)

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create venv and install dependencies
uv sync

# Install Playwright browser
uv run playwright install chromium
```

### Using pip

```bash
python3 -m venv .venv
source .venv/bin/activate      # Mac/Linux
.venv\Scripts\activate         # Windows

pip install -r requirements.txt
playwright install chromium
```

Create a `.env` file in the project root:
```
account_id        = <cloudflare account id>
access_key_id     = <r2 s3-compat access key>
secret_access_key = <r2 s3-compat secret key>
bucket_name       = opm-data
```

Contact a teammate for credentials.

---

## R2 Bucket Layout

```
opm-data/
  employment/
    2024/
      employment_202401.txt
      ...
  separations/
    2024/
      separations_202401.txt
      ...
```

Files are pipe-delimited (`.txt`), one file per dataset per month.

---

## Loading Data in a Notebook

```python
from scripts.data_loader import load_r2_data

# All separations (2024-2026)
df = load_r2_data("separations")

# Filter by year or month
df = load_r2_data("separations", year=2025)

# Aggregate on load to reduce memory (recommended for employment)
df = load_r2_data("employment", agg_cols=[
    "agency_subelement_code", "agency_subelement",
    "age_bracket", "tenure_code", "tenure",
    "appointment_type_code", "appointment_type",
    "pay_plan", "work_schedule", "education_level_bracket",
])
```

`load_r2_data` streams each matching file from R2, optionally aggregates by `agg_cols` to keep memory flat, and returns a single concatenated DataFrame. Columns `year`, `month`, and `data_type` are added automatically.

The employment notebook caches the aggregated result to `employment.pkl` — subsequent runs load from cache instantly.

---

## Refreshing / Extending the Data Pipeline

`scripts/opm_to_r2.py` downloads data from the OPM website and uploads it to R2.

**Scan what's available on OPM:**
```bash
python scripts/opm_to_r2.py scan --start 2024-01 --end 2026-02
```

**Run the pipeline (Mac):**
```bash
caffeinate -i python scripts/opm_to_r2.py run --start 2024-01 --end 2026-02
```

**Run the pipeline (Windows):**
```powershell
powercfg /change standby-timeout-ac 0
python scripts/opm_to_r2.py run --start 2024-01 --end 2026-02
powercfg /change standby-timeout-ac 30
```

**Options:**
```
--types    employment,separations  (default: all)
--start    YYYY-MM  start month
--end      YYYY-MM  end month
--no-skip  re-upload files that already exist in R2
```

---

## Dependencies

Managed via `pyproject.toml` (uv) or `requirements.txt` (pip):

- pandas, numpy, statsmodels, scikit-learn, linearmodels
- jupyter, boto3, pyarrow, playwright
