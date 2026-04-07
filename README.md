# Federal Workforce Attrition Study

This project analyzes U.S. federal workforce data to study attrition across agencies and time. Data is ingested from OPM and stored in a Cloudflare R2 bucket; notebooks pull directly from the bucket — no local data files required.

---

## Structure

```
federal-doge-study/
├── scripts/
│   ├── data_loader.py     # R2 + local data loaders
│   └── opm_to_r2.py       # Pipeline: OPM website → Cloudflare R2
├── notebooks/
│   └── separations.ipynb  # Separations analysis
├── requirements.txt
└── .env                   # Cloudflare R2 credentials (not committed)
```

---

## Data Source

Data is obtained from the U.S. Office of Personnel Management (OPM):

[https://data.opm.gov/explore-data/data/data-downloads](https://data.opm.gov/explore-data/data/data-downloads)

Datasets covered:

- Employment (workforce snapshots, ~1.5 GB/month)
- Accessions (new hires)
- Separations (departures)

Coverage: **January 2024 – February 2026**, all three dataset types.

---

## Setup

Create and activate a virtual environment:

**Mac / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows**
```bash
python3 -m venv .venv
.venv\Scripts\activate
```

Install dependencies:
```bash
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

Data lives in a Cloudflare R2 bucket named `opm-data`:

```
opm-data/
  accessions/
    2024/
      accessions_202401.txt
      accessions_202402.txt
      ...
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
df = load_r2_data("employment",  year=2024, month=6)
```

`load_r2_data` streams each matching file from R2 into memory and returns a single concatenated DataFrame. Columns `year`, `month`, and `data_type` are added automatically.

---

## Refreshing / Extending the Data Pipeline

`scripts/opm_to_r2.py` downloads data from the OPM website and uploads it to R2. The pipeline uses Playwright to automate the browser (the OPM site is Blazor Server — no direct download URLs exist).

**Scan what's available on OPM:**
```bash
python scripts/opm_to_r2.py scan --start 2024-01 --end 2026-02
```

**Run the pipeline:**

Mac (use `caffeinate` to prevent sleep):
```bash
caffeinate -i python scripts/opm_to_r2.py run --start 2024-01 --end 2026-02
```

Windows (prevent sleep by running this first, then reset after):
```powershell
powercfg /change standby-timeout-ac 0
python scripts/opm_to_r2.py run --start 2024-01 --end 2026-02
powercfg /change standby-timeout-ac 30
```

**Options:**
```
--types    employment,accessions,separations  (default: all three)
--start    YYYY-MM  start month (default: 2024-01)
--end      YYYY-MM  end month   (default: 2026-02)
--no-skip  re-upload files that already exist in R2
```

The pipeline resumes safely — already-uploaded files are skipped by default. Large employment files (~1.5 GB) upload via multipart with automatic retry on transient network errors.

---

## Dependencies

```
pandas
numpy
statsmodels
scikit-learn
jupyter
boto3
playwright
```

---

## Status

- [x] Automated OPM → R2 pipeline (Jan 2024 – Feb 2026)
- [x] R2-backed data loader for notebooks
- [x] Separations analysis notebook

Planned:
- Time-series attrition rate computation
- Agency-level regression and causal modeling
- Accessions and employment notebooks
