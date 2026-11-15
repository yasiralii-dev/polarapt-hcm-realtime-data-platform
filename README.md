# Polarapt HCM Real-Time Data Platform

Real-time ingestion platform for three HCM feeds: job postings, salary benchmarks and FX rates.
Each feed lands in S3 every minute, is routed by source into its own FIFO queue, and is processed into its own target store.

## Architecture

```
EventBridge (rate 1 min)
   ├─ lambda_jobs_ingest        (USAJOBS API)
   ├─ lambda_benchmarks_scraper (BLS top-paying occupations)
   └─ lambda_fx_ingest          (Open Exchange Rates, USD base)
              │
              ▼
   S3  raw/{source}/YYYY/MM/DD/HHMM.json  (+ contract in object metadata)
              │  ObjectCreated (prefix raw/)
              ▼
   SNS  hcm-raw-events (standard)
              │
              ▼
   lambda_contract_router  (reads S3 metadata)
              │  publish with `source` attribute
              ▼
   SNS FIFO  hcm-routed-events.fifo   (filter policy on `source`)
      ├─► jobs-queue.fifo
      ├─► benchmarks-queue.fifo
      └─► fx-queue.fifo

EventBridge (rate 5 min)
   ├─ lambda_benchmarks_transform ─► S3 processed/benchmarks/*.csv
   ├─ lambda_fx_to_sqlserver      ─► SQL Server (RDS) dbo.fx_rates
   └─ lambda_glue_starter ─► Glue job jobs_to_snowflake ─► Snowflake HCM_DB.JOBS.JOB_POSTINGS
```

## Data contract

Every raw file has the same contract, stored both inside the JSON and as S3 object metadata:

| Field | Meaning |
|---|---|
| `source` | `jobs`, `fx` or `benchmarks` |
| `timestamp` | UTC ingestion time |
| `record_count` | number of records in the file |
| `status` | `success` / `error` |
| `fetch_mode` | benchmarks only: `live` or `snapshot` |

S3 path: `raw/{source}/YYYY/MM/DD/HHMM.json`

## Resources

| Type | Name |
|---|---|
| Region | us-east-1 |
| Bucket | `data-hackathon-smit-yasir-ali` |
| Ingest Lambdas | `lambda_jobs_ingest`, `lambda_benchmarks_scraper`, `lambda_fx_ingest` |
| Router | `lambda_contract_router` |
| Processors | `lambda_benchmarks_transform`, `lambda_fx_to_sqlserver`, Glue `jobs_to_snowflake` (started by `lambda_glue_starter`) |
| SNS | `hcm-raw-events`, `hcm-routed-events.fifo` |
| SQS | `jobs-queue.fifo`, `benchmarks-queue.fifo`, `fx-queue.fifo` |
| EventBridge | `*-ingest-every-minute` (3), `*-process-every-5min` / `jobs-glue-every-5min` (3) |
| SQL Server | RDS `hcm-sqlserver`, db `hcm_payroll`, table `dbo.fx_rates` (deleted after demo) |
| Snowflake | `HCM_DB.JOBS.JOB_POSTINGS`, warehouse `HCM_WH` |

## Design decisions

- **BLS blocks Lambda IPs (HTTP 403, Akamai).** The scraper tries the live page with browser headers first; on failure it falls back to an S3 snapshot (`reference/bls_highest_paying.html`) and records `fetch_mode: snapshot` in the contract so the data origin is always visible.
- **Routing by metadata, not by parsing payloads.** The router only reads S3 object metadata and publishes to the FIFO topic with a `source` attribute; SNS filter policies deliver to the right queue.
- **FIFO ordering and dedup.** `MessageGroupId = source`, `MessageDeduplicationId = key:etag`.
- **Idempotent targets.**
  - Jobs: Glue dedupes by `JOB_ID` before loading Snowflake.
  - FX: `UNIQUE(source_key, currency)` in SQL Server.
- **Free-tier quota.** Open Exchange Rates free plan allows 1,000 requests/month, so EventBridge rules are DISABLED by default and enabled only for tests and demo (`scripts/schedule.sh <rule> ENABLED|DISABLED`).
- **Cost control.** Lifecycle rule expires `raw/` after 3 days; RDS deleted after the demo.

## Run it

```bash
cp .env.example .env      # fill in values, then:
source .env
bash scripts/setup_task2_infra.sh
python scripts/init_sqlserver.py      # needs a running SQL Server

# enable ingest rules
bash scripts/schedule.sh jobs-ingest-every-minute ENABLED
bash scripts/schedule.sh fx-ingest-every-minute ENABLED
bash scripts/schedule.sh benchmarks-ingest-every-minute ENABLED
# after ~5 min each queue holds ~5 messages; disable, then enable the process-* rules
```

## Results

- Ingest: jobs 500 records, fx 6 currencies, benchmarks top-10 occupations per run.
- After 5 minutes each FIFO queue accumulated ~5 messages. TODO: add counts from your run.
- Benchmarks → CSVs under `processed/benchmarks/`.
- Jobs → Snowflake `JOB_POSTINGS`: 500 rows / 500 distinct `JOB_ID`.
- FX → SQL Server `fx_rates`: 48 rows inserted (8 messages × 6 currencies) according to the processor logs. TODO: screenshot or log evidence.

## Screenshots

### Ingest, 5 messages per queue
![Queue counts](docs/01-queue-counts-terminal.png)
![SQS console](docs/02-sqs-console.png)
![S3 raw jobs](docs/03-s3-raw-jobs.png)
![S3 raw fx](docs/04-s3-raw-fx.png)
![S3 raw benchmarks](docs/05-s3-raw-benchmarks.png)
![Contract metadata](docs/06-raw-file-metadata.png)

## Security notes

- Secrets live in `.env` (gitignored) and in Lambda/Glue environment variables. Production should use AWS Secrets Manager.
- SQL Server port 1433 was open to the internet for the hackathon only. Production should use a VPC with private access.
- IAM policies are scoped per function (S3, SNS publish, SQS consume, Glue start).

## Cleanup

All rules DISABLED, RDS deleted with `aws rds delete-db-instance --skip-final-snapshot`.
