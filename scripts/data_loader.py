from pathlib import Path
import pandas as pd

BASE_PATH = Path(__file__).resolve().parent.parent / "data"


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

    df["year"] = selected["year"]
    df["month"] = selected["month"]
    df["day"] = selected["day"]
    df["data_type"] = selected["data_type"]
    df["access_date"] = pd.to_datetime(selected["access_date"])

    return df