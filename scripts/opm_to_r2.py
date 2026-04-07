#!/usr/bin/env python3
"""
OPM Data → Cloudflare R2 pipeline.

The OPM site is Blazor Server (SignalR) — there are no direct download URLs.
Downloads are triggered through the browser and intercepted by Playwright.

Flow per file:
  Browser click → Blazor/SignalR → downloadFileInMemory() →
  Playwright download event → temp file → R2 multipart upload → delete temp

Peak disk usage = one file at a time (~1–2 GB), not the full 50–70 GB.

Commands
--------
  scan   List every available download on the page (dataset / date / version).
  run    Download everything in the requested range and upload to R2.

Usage
-----
  python scripts/opm_to_r2.py scan
  python scripts/opm_to_r2.py run --start 2024-01 --end 2026-02
  python scripts/opm_to_r2.py run --types separations --start 2025-01 --end 2025-12

Environment (.env)
------------------
  account_id        = <cloudflare account id>
  access_key_id     = <r2 s3-compat access key>
  secret_access_key = <r2 s3-compat secret key>
  bucket_name       = <r2 bucket>   # default: opm-data
"""

import argparse
import asyncio
import io
import json
import time
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENV_PATH = Path(__file__).parent.parent / ".env"

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

ACCOUNT_ID  = _env.get("account_id", "")
ACCESS_KEY  = _env.get("access_key_id", "")
SECRET_KEY  = _env.get("secret_access_key", "")
BUCKET      = _env.get("bucket_name", "opm-data")
R2_ENDPOINT = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com"
OPM_URL     = "https://data.opm.gov/explore-data/data/data-downloads"

# 5 MB — minimum part size for S3/R2 multipart upload
CHUNK_SIZE = 5 * 1024 * 1024

# Dataset label → R2 key prefix  (lowercase, no spaces)
DATASET_LABELS = {
    "employment":  ["employment", "status"],
    "accessions":  ["accession"],
    "separations": ["separation"],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# R2 helpers
# ---------------------------------------------------------------------------

def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def ensure_bucket(client):
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=BUCKET)
            log.info("Created bucket: %s", BUCKET)
        else:
            raise


def object_exists(client, key: str) -> bool:
    try:
        client.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False


def _upload_part(client, key: str, upload_id: str, part_num: int, data: bytes,
                 max_retries: int = 5) -> dict:
    """Upload a single multipart part, retrying on transient errors."""
    for attempt in range(max_retries):
        try:
            part = client.upload_part(
                Bucket=BUCKET,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=data,
            )
            return {"PartNumber": part_num, "ETag": part["ETag"]}
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt   # 1s, 2s, 4s, 8s, 16s
            log.warning("Part %d failed (attempt %d/%d): %s — retrying in %ds",
                        part_num, attempt + 1, max_retries, e, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def upload_file_to_r2(client, path: Path, key: str):
    """Stream a local file to R2 via multipart upload, then delete it."""
    size = path.stat().st_size
    log.info("Uploading %s (%d MB) → %s", path.name, size // 1024 // 1024, key)

    mpu = client.create_multipart_upload(Bucket=BUCKET, Key=key)
    upload_id = mpu["UploadId"]
    parts = []
    part_num = 1

    try:
        with path.open("rb") as fh:
            while True:
                data = fh.read(CHUNK_SIZE)
                if not data:
                    break
                parts.append(_upload_part(client, key, upload_id, part_num, data))
                if part_num % 50 == 0:
                    log.info("  … %d parts uploaded (%d MB so far)",
                             part_num, part_num * CHUNK_SIZE // 1024 // 1024)
                part_num += 1

        if not parts:
            raise ValueError("Downloaded file was empty")

        client.complete_multipart_upload(
            Bucket=BUCKET,
            Key=key,
            MultipartUpload={"Parts": parts},
            UploadId=upload_id,
        )
        log.info("  uploaded (%d parts, %d MB)", len(parts), size // 1024 // 1024)

    except Exception:
        client.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)
        raise
    finally:
        path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def parse_ym_from_text(text: str) -> Optional[tuple[int, int]]:
    """Extract (year, month) from a string like 'February 2026' or '2025-03'."""
    text = text.strip()
    # "Month YYYY"
    m = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})", text, re.IGNORECASE)
    if m:
        return int(m.group(2)), MONTH_NAMES[m.group(1).lower()]
    # "YYYY-MM" or "YYYYMM"
    m = re.search(r"(\d{4})[_\-]?(\d{2})\b", text)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 2000 <= y <= 2030 and 1 <= mo <= 12:
            return y, mo
    return None


def classify_dataset(text: str) -> Optional[str]:
    """Map row text to a dataset key (employment / accessions / separations)."""
    lower = text.lower()
    for ds, keywords in DATASET_LABELS.items():
        if any(kw in lower for kw in keywords):
            return ds
    return None


def r2_key(dataset: str, year: int, month: int, filename: str) -> str:
    ext = Path(filename).suffix or ".txt"
    if ext not in (".txt", ".csv"):
        ext = ".txt"
    return f"{dataset}/{year}/{dataset}_{year}{month:02d}{ext}"


# ---------------------------------------------------------------------------
# Browser helpers
# ---------------------------------------------------------------------------

async def _wait_for_blazor(page, extra_seconds: float = 2.0):
    """Wait for Blazor's SignalR connection and initial render to settle."""
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(extra_seconds)


async def _set_date_filter(page, from_ym: tuple[int, int], to_ym: tuple[int, int]):
    """
    Fill the From / To date range inputs on the OPM downloads page.

    The inputs are <input type="date"> (ids: fromDateInput / toDateInput).
    Native date inputs require YYYY-MM-DD format.
    """
    import calendar as _cal
    last_day = _cal.monthrange(to_ym[0], to_ym[1])[1]
    from_str = f"{from_ym[0]}-{from_ym[1]:02d}-01"
    to_str   = f"{to_ym[0]}-{to_ym[1]:02d}-{last_day:02d}"
    log.info("Setting date filter  From=%s  To=%s", from_str, to_str)

    # Playwright's fill() with YYYY-MM-DD is the correct way to set <input type="date">.
    # After each fill, Tab away so Blazor's @bind fires its change handler.
    await page.locator("#fromDateInput").fill(from_str)
    await page.keyboard.press("Tab")
    await asyncio.sleep(2)   # let Blazor re-render after first date

    await page.locator("#toDateInput").fill(to_str)
    await page.keyboard.press("Tab")

    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(2)

    count = await page.locator("button:has-text('Download'), a:has-text('Download')").count()
    log.info("Download buttons visible after filter: %d (expect ~78 for full 2024-2026 range)", count)


async def _collect_page_rows(page, page_offset: int) -> list[dict]:
    """Collect all download rows visible on the current paginated page."""
    rows = []
    # Match only the top-level DOWNLOAD button, not the dropdown sub-items
    # (.TXT / .JSON sub-options contain a period so we exclude those)
    buttons = page.locator(
        "button:has-text('DOWNLOAD'):not(:has-text('.')), "
        "a:has-text('DOWNLOAD'):not(:has-text('.'))"
    )
    count = await buttons.count()

    for i in range(count):
        btn = buttons.nth(i)
        row_text = await btn.evaluate("""el => {
            let node = el;
            for (let depth = 0; depth < 8; depth++) {
                node = node.parentElement;
                if (!node) break;
                const t = node.innerText || '';
                if (t.length > 20) return t;
            }
            return '';
        }""")
        ym = parse_ym_from_text(row_text)
        ds = classify_dataset(row_text)
        rows.append({
            "index":   page_offset + i,
            "dataset": ds,
            "year":    ym[0] if ym else None,
            "month":   ym[1] if ym else None,
            "label":   row_text[:120].replace("\n", " ").strip(),
        })
    return rows


async def _set_rows_per_page(page, target: int = 100):
    """Try to increase rows-per-page so we need fewer paginated requests."""
    # Find the rows-per-page select/dropdown (shows "10" by default)
    select = page.locator("select").filter(has_text=re.compile(r"^\d+$"))
    if await select.count() == 0:
        # Try a combobox-style element near "Rows per page" text
        select = page.locator("[aria-label*='rows' i], [aria-label*='per page' i]")
    if await select.count() > 0:
        try:
            await select.first.select_option(str(target))
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)
            log.info("Set rows per page to %d", target)
        except Exception as e:
            log.debug("Could not set rows per page: %s", e)


async def _get_download_rows(
    page,
    date_filter: Optional[tuple[tuple[int, int], tuple[int, int]]] = None,
) -> list[dict]:
    """
    Return all download rows across all paginated pages.
    Iterates Next → until disabled, collecting rows from each page.
    """
    if date_filter:
        await _set_date_filter(page, date_filter[0], date_filter[1])

    await _set_rows_per_page(page, 100)

    all_rows: list[dict] = []
    page_num = 0

    while True:
        rows = await _collect_page_rows(page, page_offset=len(all_rows))
        all_rows.extend(rows)
        log.info("Page %d: collected %d entries (total so far: %d)", page_num + 1, len(rows), len(all_rows))

        # Find the "Next page" button — aria-label or the ">" symbol
        next_btn = page.locator(
            "button[aria-label='Next page'], "
            "button[aria-label='next'], "
            "button:has-text('›'), "
            "button:has-text('>')"
        ).last  # use .last to avoid matching << < > >> — we want the single >

        if await next_btn.count() == 0:
            break
        disabled = await next_btn.get_attribute("disabled")
        aria_disabled = await next_btn.get_attribute("aria-disabled")
        if disabled is not None or aria_disabled == "true":
            break

        await next_btn.click()
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1.5)
        page_num += 1

        if page_num > 100:   # safety cap
            log.warning("Stopped after 100 pages — something may be wrong")
            break

    return all_rows


# ---------------------------------------------------------------------------
# Scan command
# ---------------------------------------------------------------------------

async def cmd_scan(start_ym: tuple[int, int], end_ym: tuple[int, int]):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        log.info("Loading %s …", OPM_URL)
        await page.goto(OPM_URL)
        await _wait_for_blazor(page)

        rows = await _get_download_rows(page, date_filter=(start_ym, end_ym))
        await browser.close()

    out = Path(__file__).parent / "available_downloads.json"
    out.write_text(json.dumps(rows, indent=2))

    print(f"\n{'#':>3}  {'Dataset':<14} {'Year':>4}  {'Mo':>2}  Label")
    print("-" * 72)
    for r in rows:
        ds  = r["dataset"] or "?"
        yr  = str(r["year"])  if r["year"]  else "?"
        mo  = str(r["month"]) if r["month"] else "?"
        print(f"{r['index']:>3}  {ds:<14} {yr:>4}  {mo:>2}  {r['label'][:40]}")

    print(f"\n{len(rows)} entries found. Full details → {out}")


# ---------------------------------------------------------------------------
# Run command
# ---------------------------------------------------------------------------

async def cmd_run(
    types: list[str],
    start_ym: tuple[int, int],
    end_ym: tuple[int, int],
    skip_existing: bool,
):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    client = r2_client()
    ensure_bucket(client)

    uploaded = skipped = errors = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        async def load_and_scan():
            await page.goto(OPM_URL)
            await _wait_for_blazor(page)
            all_rows = await _get_download_rows(page, date_filter=(start_ym, end_ym))
            valid = [
                r for r in all_rows
                if r["dataset"] in types
                and r["year"] is not None
                and r["month"] is not None
                and start_ym <= (r["year"], r["month"]) <= end_ym
            ]
            return valid

        log.info("Loading %s …", OPM_URL)
        want = await load_and_scan()

        if not want:
            log.warning("No matching entries found. Run 'scan' to see what's available.")
            await browser.close()
            return

        log.info("%d files to process", len(want))
        # Track which (dataset, year, month) tuples still need uploading
        remaining = {(r["dataset"], r["year"], r["month"]): r for r in want}

        while remaining:
            # Check skip-existing up front for this pass
            to_do = {}
            for key_tuple, r in remaining.items():
                ds, year, month = key_tuple
                placeholder = f"{ds}/{year}/{ds}_{year}{month:02d}.txt"
                if skip_existing and object_exists(client, placeholder):
                    log.info("Skip (exists): %s", placeholder)
                    skipped += 1
                else:
                    to_do[key_tuple] = r

            if not to_do:
                break

            # Process one file; on error reload and re-scan to refresh indices
            key_tuple, r = next(iter(to_do.items()))
            ds, year, month = key_tuple
            log.info("Downloading %s %d-%02d (button index %d) …", ds, year, month, r["index"])

            try:
                buttons = page.locator(
                    "button:has-text('DOWNLOAD'):not(:has-text('.')), "
                    "a:has-text('DOWNLOAD'):not(:has-text('.'))"
                )
                btn = buttons.nth(r["index"])

                # Click the main DOWNLOAD button to open the format dropdown
                await btn.click()
                await asyncio.sleep(1.0)  # wait for dropdown to render

                # Select "Download .TXT" — use get_by_text to avoid CSS selector
                # issues with the period in ".TXT"; matches any visible element
                txt_btn = page.get_by_text("Download .TXT", exact=True)
                if await txt_btn.count() == 0:
                    # Fallback: any visible element containing "TXT"
                    txt_btn = page.locator("li, span, a, button").filter(
                        has_text=re.compile(r"\.TXT", re.IGNORECASE)
                    )

                async with page.expect_download(timeout=300_000) as dl_info:
                    await txt_btn.first.click()

                download = await dl_info.value
                temp_path = Path(await download.path())
                filename  = download.suggested_filename or f"{ds}_{year}{month:02d}.csv"
                upload_file_to_r2(client, temp_path, r2_key(ds, year, month, filename))
                uploaded += 1
                del remaining[key_tuple]

            except Exception as e:
                log.error("Failed %s %d-%02d: %s", ds, year, month, e)
                errors += 1
                del remaining[key_tuple]  # don't retry same file endlessly
                # Reload + re-scan to get fresh button indices
                log.info("Reloading page to recover …")
                fresh = await load_and_scan()
                # Update indices for remaining items
                fresh_map = {(r2["dataset"], r2["year"], r2["month"]): r2 for r2 in fresh}
                remaining = {k: fresh_map[k] for k in remaining if k in fresh_map}

        await browser.close()

    log.info("Done — uploaded: %d  skipped: %d  errors: %d", uploaded, skipped, errors)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_ym(s: str) -> tuple[int, int]:
    dt = datetime.strptime(s, "%Y-%m")
    return dt.year, dt.month


def main():
    ap = argparse.ArgumentParser(description="OPM federal workforce data → Cloudflare R2")
    sub = ap.add_subparsers(dest="cmd")

    scan_p = sub.add_parser("scan", help="List available downloads on the OPM page")
    scan_p.add_argument("--start", default="2024-01", metavar="YYYY-MM")
    scan_p.add_argument("--end",   default="2026-02", metavar="YYYY-MM")

    run_p = sub.add_parser("run", help="Download files and upload to R2")
    run_p.add_argument(
        "--types",
        default="employment,accessions,separations",
        metavar="TYPES",
        help="Comma-separated dataset types (default: all)",
    )
    run_p.add_argument("--start", default="2024-01", metavar="YYYY-MM", help="Start month")
    run_p.add_argument("--end",   default="2026-02", metavar="YYYY-MM", help="End month")
    run_p.add_argument("--no-skip", action="store_true", help="Re-upload existing files")

    args = ap.parse_args()

    if args.cmd == "scan":
        asyncio.run(cmd_scan(
            start_ym=_parse_ym(args.start),
            end_ym=_parse_ym(args.end),
        ))

    elif args.cmd == "run":
        types = [t.strip() for t in args.types.split(",")]
        invalid = [t for t in types if t not in DATASET_LABELS]
        if invalid:
            ap.error(f"Unknown dataset types: {invalid}. Valid: {list(DATASET_LABELS)}")
        asyncio.run(cmd_run(
            types=types,
            start_ym=_parse_ym(args.start),
            end_ym=_parse_ym(args.end),
            skip_existing=not args.no_skip,
        ))

    else:
        ap.print_help()


if __name__ == "__main__":
    main()
