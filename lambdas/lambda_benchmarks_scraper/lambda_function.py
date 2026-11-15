import gzip
import json
import os
import re
import urllib.request
from datetime import datetime, timezone

import boto3
from bs4 import BeautifulSoup

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]
LIVE_URL = "https://www.bls.gov/ooh/highest-paying.htm"
SNAPSHOT_KEY = "reference/bls_highest_paying.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def fetch_html(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", errors="replace")


def parse_money(text):
    nums = re.findall(r"\d[\d,]*", text)
    return int(nums[0].replace(",", "")) if nums else None


def parse_table(html):
    soup = BeautifulSoup(html, "html.parser")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
        pay_idx = next((i for i, h in enumerate(headers) if "median" in h.lower()), None)
        if pay_idx is None:
            continue

        records = []
        for row in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
            if len(cells) != len(headers):
                continue
            pay = parse_money(cells[pay_idx])
            if pay is None:
                continue
            records.append({
                "occupation": cells[0],
                "median_annual_pay": pay,
                "median_pay_raw": cells[pay_idx],
                "details": dict(zip(headers, cells)),
            })

        if records:
            records.sort(key=lambda r: r["median_annual_pay"], reverse=True)
            top = records[:10]
            for rank, r in enumerate(top, start=1):
                r["rank"] = rank
            return top

    raise ValueError("Median pay table not found on page")


def get_records():
    """Try live BLS first; on failure use the stored snapshot. Returns (records, fetch_mode)."""
    try:
        return parse_table(fetch_html(LIVE_URL)), "live"
    except Exception as e:
        print(f"WARN: live failed: {e}")
    obj = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
    html = obj["Body"].read().decode("utf-8", errors="replace")
    return parse_table(html), "snapshot"


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    status, records, fetch_mode = "success", [], "none"

    try:
        records, fetch_mode = get_records()
    except Exception as e:
        status = "error"
        print(f"ERROR: {e}")

    contract = {
        "source": "benchmarks",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "record_count": len(records),
        "status": status,
        "fetch_mode": fetch_mode,
    }

    key = f"raw/benchmarks/{now:%Y/%m/%d}/{now:%H%M}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps({"contract": contract, "data": records}),
        ContentType="application/json",
        Metadata={k: str(v) for k, v in contract.items()},
    )

    print(f"Saved s3://{BUCKET}/{key} ({len(records)} records, mode={fetch_mode})")
    return {"statusCode": 200, "key": key, **contract}