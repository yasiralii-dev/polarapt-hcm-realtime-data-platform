import csv
import io
import json
import os
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

BUCKET = os.environ["BUCKET_NAME"]
QUEUE_URL = os.environ["QUEUE_URL"]
MAX_BATCHES = 5  # up to 50 messages per run


def transform(raw):
    contract = raw["contract"]
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for r in raw["data"]:
        pay = r["median_annual_pay"]
        rows.append({
            "rank": r["rank"],
            "occupation": r["occupation"].strip(),
            "median_annual_pay_usd": pay,
            "median_monthly_pay_usd": round(pay / 12, 2),
            "median_hourly_pay_usd": round(pay / 2080, 2),
            "source_timestamp": contract["timestamp"],
            "fetch_mode": contract.get("fetch_mode", "unknown"),
            "processed_at": ingested_at,
        })
    return rows


def process_message(body):
    raw_key = body["key"]
    if body.get("status") != "success" or body.get("record_count", 0) == 0:
        print(f"SKIP {raw_key}: status={body.get('status')}")
        return None

    obj = s3.get_object(Bucket=body["bucket"], Key=raw_key)
    rows = transform(json.loads(obj["Body"].read()))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    out_key = raw_key.replace("raw/", "processed/", 1).replace(".json", ".csv")
    s3.put_object(
        Bucket=BUCKET,
        Key=out_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
        Metadata={"source": "benchmarks", "record_count": str(len(rows)), "stage": "processed"},
    )
    print(f"WROTE s3://{BUCKET}/{out_key} ({len(rows)} rows)")
    return out_key


def lambda_handler(event, context):
    processed, failed = 0, 0
    for _ in range(MAX_BATCHES):
        resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            try:
                process_message(json.loads(m["Body"]))
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
                processed += 1
            except Exception as e:
                failed += 1
                print(f"ERROR processing message: {e}")
    print(f"DONE processed={processed} failed={failed}")
    return {"processed": processed, "failed": failed}