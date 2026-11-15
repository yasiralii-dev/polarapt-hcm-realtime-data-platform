import json
import os
import urllib.parse

import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")

FIFO_TOPIC_ARN = os.environ["FIFO_TOPIC_ARN"]
VALID_SOURCES = {"jobs", "benchmarks", "fx"}


def lambda_handler(event, context):
    routed = 0
    for rec in event["Records"]:
        s3_event = json.loads(rec["Sns"]["Message"])

        # S3 sends a test event (no "Records") when notifications are configured
        for r in s3_event.get("Records", []):
            bucket = r["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(r["s3"]["object"]["key"])

            head = s3.head_object(Bucket=bucket, Key=key)
            meta = head.get("Metadata", {})
            source = meta.get("source")

            if source not in VALID_SOURCES:
                print(f"SKIP {key}: unknown source in metadata: {source}")
                continue

            message = {
                "bucket": bucket,
                "key": key,
                "source": source,
                "timestamp": meta.get("timestamp"),
                "record_count": int(meta.get("record_count", 0)),
                "status": meta.get("status"),
                "fetch_mode": meta.get("fetch_mode"),
            }

            etag = head["ETag"].strip('"')
            sns.publish(
                TopicArn=FIFO_TOPIC_ARN,
                Message=json.dumps(message),
                MessageGroupId=source,
                MessageDeduplicationId=f"{key}:{etag}",
                MessageAttributes={
                    "source": {"DataType": "String", "StringValue": source},
                    "status": {"DataType": "String", "StringValue": str(meta.get("status"))},
                },
            )
            routed += 1
            print(f"ROUTED {key} -> source={source}")

    return {"routed": routed}