import json
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]
APP_ID = os.environ["OXR_APP_ID"]

SYMBOLS = ["PKR", "SAR", "AED", "GBP", "EUR", "INR"]
URL = "https://openexchangerates.org/api/latest.json"


def fetch_rates():
    params = urllib.parse.urlencode({
        "app_id": APP_ID,
        "base": "USD",
        "symbols": ",".join(SYMBOLS),
    })
    with urllib.request.urlopen(f"{URL}?{params}", timeout=20) as resp:
        return json.loads(resp.read())


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    status, records = "success", []

    try:
        payload = fetch_rates()
        rate_ts = datetime.fromtimestamp(payload["timestamp"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for cur, rate in payload["rates"].items():
            records.append({
                "base": payload.get("base", "USD"),
                "currency": cur,
                "rate": rate,
                "rate_timestamp": rate_ts,
            })
    except Exception as e:
        status = "error"
        print(f"ERROR: {e}")

    contract = {
        "source": "fx",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "record_count": len(records),
        "status": status,
    }

    key = f"raw/fx/{now:%Y/%m/%d}/{now:%H%M}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps({"contract": contract, "data": records}),
        ContentType="application/json",
        Metadata={k: str(v) for k, v in contract.items()},
    )

    print(f"Saved s3://{BUCKET}/{key} ({len(records)} records)")
    return {"statusCode": 200, "key": key, **contract}