import json
import sys

import boto3
import snowflake.connector
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    "QUEUE_URL", "SF_ACCOUNT", "SF_USER", "SF_PASS",
])

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
MAX_BATCHES = 10


def to_date(v):
    return v[:10] if v else None


def to_num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def process_message(cur, body, existing_ids):
    key = body["key"]
    if body.get("status") != "success" or body.get("record_count", 0) == 0:
        print(f"SKIP {key}: status={body.get('status')}")
        return 0

    raw = json.loads(s3.get_object(Bucket=body["bucket"], Key=key)["Body"].read())

    rows = []
    for j in raw["data"]:
        jid = j.get("job_id")
        if not jid or jid in existing_ids:
            continue
        existing_ids.add(jid)
        rows.append((
            jid, j.get("title"), j.get("organization"), j.get("location"),
            to_num(j.get("salary_min")), to_num(j.get("salary_max")),
            j.get("salary_period"), to_date(j.get("open_date")),
            to_date(j.get("close_date")), key,
        ))

    if rows:
        cur.executemany(
            "INSERT INTO JOB_POSTINGS (JOB_ID, TITLE, ORGANIZATION, LOCATION, SALARY_MIN, "
            "SALARY_MAX, SALARY_PERIOD, OPEN_DATE, CLOSE_DATE, SOURCE_KEY) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            rows,
        )
    print(f"INSERTED {len(rows)} new rows from {key} ({len(raw['data'])} in file)")
    return len(rows)


def main():
    conn = snowflake.connector.connect(
        account=args["SF_ACCOUNT"], user=args["SF_USER"], password=args["SF_PASS"],
        role="HCM_ROLE", warehouse="HCM_WH", database="HCM_DB", schema="JOBS",
    )
    cur = conn.cursor()
    cur.execute("SELECT JOB_ID FROM JOB_POSTINGS")
    existing_ids = {r[0] for r in cur.fetchall()}

    processed = failed = inserted = 0
    for _ in range(MAX_BATCHES):
        resp = sqs.receive_message(QueueUrl=args["QUEUE_URL"], MaxNumberOfMessages=10, WaitTimeSeconds=2)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            try:
                inserted += process_message(cur, json.loads(m["Body"]), existing_ids)
                sqs.delete_message(QueueUrl=args["QUEUE_URL"], ReceiptHandle=m["ReceiptHandle"])
                processed += 1
            except Exception as e:
                failed += 1
                print(f"ERROR processing message: {e}")
    conn.close()
    print(f"DONE processed={processed} failed={failed} inserted={inserted}")


main()