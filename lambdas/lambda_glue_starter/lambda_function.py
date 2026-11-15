import boto3

glue = boto3.client("glue")
JOB_NAME = "jobs_to_snowflake"


def lambda_handler(event, context):
    try:
        run = glue.start_job_run(JobName=JOB_NAME)
        print(f"Started Glue job {JOB_NAME}: {run['JobRunId']}")
        return {"started": True, "run_id": run["JobRunId"]}
    except glue.exceptions.ConcurrentRunsExceededException:
        print(f"Glue job {JOB_NAME} is already running, skipping")
        return {"started": False, "reason": "already running"}
