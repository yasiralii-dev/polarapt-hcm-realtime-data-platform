import json
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]
API_KEY = os.environ["USAJOBS_API_KEY"]
EMAIL = os.environ["USAJOBS_EMAIL"]

URL = "https://data.usajobs.gov/api/search"


def fetch_jobs():
    params = urllib.parse.urlencode({"ResultsPerPage": 500, "Page": 1})
    req = urllib.request.Request(
        f"{URL}?{params}",
        headers={
            "Host": "data.usajobs.gov",
            "User-Agent": EMAIL,
            "Authorization-Key": API_KEY,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, json.loads(resp.read())


def parse_jobs(payload):
    items = payload["SearchResult"]["SearchResultItems"]
    jobs = []
    for item in items:
        d = item["MatchedObjectDescriptor"]
        pay = d.get("PositionRemuneration", [{}])[0]
        jobs.append({
            "job_id": item.get("MatchedObjectId"),
            "title": d.get("PositionTitle"),
            "organization": d.get("OrganizationName"),
            "location": d.get("PositionLocationDisplay"),
            "salary_min": pay.get("MinimumRange"),
            "salary_max": pay.get("MaximumRange"),
            "salary_period": pay.get("Description"),
            "open_date": d.get("PositionStartDate"),
            "close_date": d.get("PositionEndDate"),
        })
    return jobs


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    status, jobs = "success", []

    try:
        http_status, payload = fetch_jobs()
        jobs = parse_jobs(payload)
    except Exception as e:
        status = "error"
        print(f"ERROR: {e}")

    contract = {
        "source": "jobs",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "record_count": len(jobs),
        "status": status,
    }

    key = f"raw/jobs/{now:%Y/%m/%d}/{now:%H%M}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps({"contract": contract, "data": jobs}),
        ContentType="application/json",
        Metadata={k: str(v) for k, v in contract.items()},
    )

    print(f"Saved s3://{BUCKET}/{key} ({len(jobs)} records)")
    return {"statusCode": 200, "key": key, **contract}