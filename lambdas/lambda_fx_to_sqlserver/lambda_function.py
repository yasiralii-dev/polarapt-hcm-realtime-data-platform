import json
import os
from datetime import datetime

import boto3
import pytds

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

QUEUE_URL = os.environ["QUEUE_URL"]
DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]
MAX_BATCHES = 5

INSERT_SQL = """
IF NOT EXISTS (SELECT 1 FROM dbo.fx_rates WHERE source_key = %s AND currency = %s)
INSERT INTO dbo.fx_rates (base_currency, currency, rate, rate_timestamp, source_key, ingested_at)
VALUES (%s, %s, %s, %s, %s, SYSUTCDATETIME())
"""


def process_message(conn, body):
    key = body["key"]
    if body.get("status") != "success" or body.get("record_count", 0) == 0:
        print(f"SKIP {key}: status={body.get('status')}")
        return 0

    raw = json.loads(s3.get_object(Bucket=body["bucket"], Key=key)["Body"].read())
    cur = conn.cursor()
    for r in raw["data"]:
        ts = datetime.strptime(r["rate_timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        cur.execute(INSERT_SQL, (key, r["currency"],
                                 r["base"], r["currency"], r["rate"], ts, key))
    conn.commit()
    print(f"INSERTED {len(raw['data'])} rows from {key}")
    return len(raw["data"])


def lambda_handler(event, context):
    conn = pytds.connect(DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
                         autocommit=False, login_timeout=15, timeout=30)
    processed, failed, rows = 0, 0, 0
    try:
        for _ in range(MAX_BATCHES):
            resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=1)
            msgs = resp.get("Messages", [])
            if not msgs:
                break
            for m in msgs:
                try:
                    rows += process_message(conn, json.loads(m["Body"]))
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
                    processed += 1
                except Exception as e:
                    conn.rollback()
                    failed += 1
                    print(f"ERROR processing message: {e}")
    finally:
        conn.close()
    print(f"DONE processed={processed} failed={failed} rows={rows}")
    return {"processed": processed, "failed": failed, "rows": rows}