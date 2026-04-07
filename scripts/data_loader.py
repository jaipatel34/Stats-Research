from pathlib import Path
from io import BytesIO
import re
import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError  # noqa: F401

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENV_PATH = Path(__file__).parent.parent / ".env"
BASE_PATH = Path(__file__).resolve().parent.parent / "data"

def _load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env

_env = _load_env(ENV_PATH)

BUCKET      = _env.get("bucket_name", "opm-data")
R2_ENDPOINT = f"https://{_env.get('account_id', '')}.r2.cloudflarestorage.com"


def _r2():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=_env.get("access_key_id"),
        aws_secret_access_key=_env.get("secret_access_key"),
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


# ---------------------------------------------------------------------------
# R2 loader  (primary — reads from your bucket)
# ---------------------------------------------------------------------------

def load_r2_data(data_type: str, year: int = None, month: int = None) -> pd.DataFrame:
    """
    Load federal workforce data directly from R2.

    Parameters
    ----------
    data_type : "employment" | "accessions" | "separations"
    year      : filter to a specific year (None = all years)
    month     : filter to a specific month (None = all months)

    Returns a concatenated DataFrame with added year/month columns.

    R2 key format: {data_type}/{year}/{data_type}_{year}{month:02d}.txt
    """
    client = _r2()
    prefix = f"{data_type}/"
    if year is not None:
        prefix += f"{year}/"

    # List all matching keys
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    if not keys:
        raise ValueError(f"No files found in R2 for {data_type!r} "
                         f"year={year} month={month}")

    # Parse year/month from key name and filter
    pattern = re.compile(rf"{data_type}_(\d{{4}})(\d{{2}})\.txt$")
    matched = []
    for key in keys:
        m = pattern.search(key)
        if not m:
            continue
        ky, km = int(m.group(1)), int(m.group(2))
        if year  is not None and ky != year:
            continue
        if month is not None and km != month:
            continue
        matched.append((key, ky, km))

    if not matched:
        raise ValueError(f"No files matched filters: data_type={data_type!r} "
                         f"year={year} month={month}")

    matched.sort(key=lambda x: (x[1], x[2]))

    dfs = []
    for key, ky, km in matched:
        print(f"  loading {key} …")
        obj = client.get_object(Bucket=BUCKET, Key=key)
        buf = BytesIO(obj["Body"].read())
        df = pd.read_csv(buf, sep="|", low_memory=False)
        df["year"]      = ky
        df["month"]     = km
        df["data_type"] = data_type
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Local loader  (legacy — reads from data/ folder)
# ---------------------------------------------------------------------------

def load_federal_data(data_type, year=None, month=None, day=None):
    folder = BASE_PATH / data_type

    if not folder.exists():
        raise ValueError(f"{data_type} folder does not exist: {folder}")

    files = list(folder.glob("*.txt"))

    if not files:
        raise ValueError(f"No files found in {folder}")

    def parse_filename(file):
        parts = file.stem.split("_")
        return {
            "file": file,
            "data_type": parts[0],
            "year": int(parts[1][:4]),
            "month": int(parts[1][4:]),
            "day": int(parts[2]),
            "access_date": parts[3]
        }

    metadata = [parse_filename(f) for f in files]

    filtered = [
        m for m in metadata
        if (year is None or m["year"] == year)
        and (month is None or m["month"] == month)
        and (day is None or m["day"] == day)
    ]

    if not filtered:
        raise ValueError("No matching files found")

    filtered.sort(key=lambda x: x["access_date"], reverse=True)
    selected = filtered[0]

    df = pd.read_csv(selected["file"], sep="|")
    df["year"]        = selected["year"]
    df["month"]       = selected["month"]
    df["day"]         = selected["day"]
    df["data_type"]   = selected["data_type"]
    df["access_date"] = pd.to_datetime(selected["access_date"])

    return df
