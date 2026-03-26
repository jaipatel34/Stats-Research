# Federal Workforce Attrition Study

This project analyzes U.S. federal workforce data to study attrition across agencies and time. The current implementation provides a data ingestion layer that loads raw OPM datasets into pandas DataFrames.

---

## Structure

```
federal-doge-study/
├── data/
│   ├── separations/
│   ├── employment/
│   └── accessions/
├── scripts/
│   └── data_loader.py
├── notebooks/
├── requirements.txt
└── README.md
```

---

## Data Source

Data is obtained from the U.S. Office of Personnel Management (OPM):

[https://data.opm.gov/explore-data/data/data-downloads](https://data.opm.gov/explore-data/data/data-downloads)

Relevant datasets:

* FedScope Employment
* Separations
* Accessions

Download the `.txt` files and place them into the corresponding folders under `data/`.

---

## File Naming Convention

Files must follow this format:

```
<data_type>_<YYYYMM>_<day>_<access_date>.txt
```

Example:

```
separations_202601_1_2026-03-25.txt
```

---

## Setup

Create and activate a virtual environment:

## Mac & Linux

```
python3 -m venv .venv
source .venv/bin/activate
```

## Windows

```
python3 -m venv .venv
.venv\bin\activate
```

Install dependencies:

```
python -m pip install -r requirements.txt
```

---

## Usage

Import the loader:

```python
from scripts.data_loader import load_federal_data
```

Load a dataset:

```python
df = load_federal_data("separations", year=2026, month=1)
```

---

## Function Behavior

`load_federal_data(data_type, year=None, month=None, day=None)`

* Scans the corresponding folder under `data/`
* Parses metadata from filenames
* Filters files by input parameters
* If multiple matches exist, selects the most recent `access_date`
* Loads the file into a pandas DataFrame
* Appends metadata columns:

  * `year`
  * `month`
  * `day`
  * `data_type`
  * `access_date`

---

## Assumptions

* Input files are pipe-delimited (`|`)
* Directory structure is consistent with `data/<data_type>/`
* Filenames follow the specified convention

---

## Dependencies

```
pandas
numpy
statsmodels
scikit-learn
jupyter
```

---

## Status

Current scope:

* Dynamic loading of raw federal workforce datasets

Planned:

* Time-series dataset construction
* Attrition rate computation
* Regression and causal modeling

---


