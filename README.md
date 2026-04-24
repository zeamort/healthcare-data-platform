# Open-Source Healthcare Data Engineering Framework

An end-to-end cloud-native data platform built on AWS using [Synthea](https://github.com/synthetichealth/synthea) synthetic patient data. Demonstrates batch ingestion, streaming, OLTP/OLAP separation, in-warehouse ML, and analytics — all reproducible via Terraform.

## Architecture

```
Data Sources          Ingestion Layer        Storage Layer              Application Layer
┌──────────────┐      ┌───────────────┐      ┌────────────────────┐     ┌──────────────────┐
│ Synthea CSV  │─────▶│  S3 Trigger   │─────▶│  S3 (Data Lake)    │     │ FastAPI REST API │
│ (Batch)      │      │  → Lambda ETL │─────▶│  RDS PostgreSQL    │────▶│ (Operational)    │
└──────────────┘      └───────────────┘      │  (OLTP, OMOP CDM)  │     └──────────────────┘
                                             └────────┬───────────┘
┌──────────────┐      ┌───────────────┐               │              ┌──────────────────┐
│ Stream       │─────▶│  Kinesis      │─────▶┌────────▼───────────┐  │ Streamlit         │
│ Simulator    │      │  → Lambda     │      │  Redshift          │──▶│ Dashboard         │
│ (post-cutoff)│      └───────────────┘      │  (OLAP, Star Schema│  │ (Fargate)         │
└──────────────┘                             │   + Redshift ML)   │  └──────────────────┘
                                             └────────┬───────────┘
                                                      │
                                             ┌────────▼──────────────────┐
                                             │ ML: K-Means clustering,    │
                                             │ XGBoost risk scoring,      │
                                             │ comorbidity analysis       │
                                             └────────────────────────────┘
```

Data model: [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/) in RDS, star schema in Redshift (see `docs/adr/`).

## Project Structure

```
├── terraform/       # Infrastructure as Code (AWS resources)
├── etl/             # ETL + ML Python modules (deployed as Lambdas)
├── lambda/handlers/ # Lambda entry points
├── api/             # FastAPI REST API
├── dashboard/       # Streamlit analytics dashboard (Fargate)
├── sql/             # Database schemas (RDS OMOP + Redshift star schema)
├── scripts/         # deployment, demo, and operational helpers
├── tests/           # pytest suite
└── docs/            # Architecture decision records, guides
```

## Key Features

- **Batch ETL**: S3 manifest → Lambda → RDS (OMOP CDM) → Redshift (star schema), chained by S3 events and EventBridge
- **Streaming**: Kinesis Data Streams for post-cutoff clinical events, consumed by a Lambda that writes back to RDS
- **OLTP/OLAP separation**: RDS for point queries, Redshift for analytics
- **In-warehouse ML**: Redshift ML (SageMaker-backed) — K-means patient clustering, XGBoost 30-day readmission risk
- **Comorbidity analysis**: SQL-based disease co-occurrence pairs
- **REST API**: FastAPI with API-key auth and role-based access
- **Security**: Private subnets for RDS/Redshift, NAT for Lambda egress, SageMaker VPC endpoints, de-identified analytics layer with surrogate keys
- **Infrastructure as Code**: Full Terraform deployment

## Prerequisites

- AWS account + AWS CLI configured
- Terraform >= 1.0
- Python 3.12
- Docker (for building the psycopg2 Lambda layer)

## Quick Start

```bash
# 1. Package Lambda deployment zips + psycopg2 layer
./scripts/package_lambdas.sh

# 2. Deploy infrastructure (VPC, RDS, Redshift, Kinesis, Lambdas,
#    API Gateway, ECS Fargate dashboard, ECR, ALB, IAM, ...)
cd terraform && terraform init && terraform apply && cd ..

# 3. Source helper vars (API URL, Kinesis stream, Redshift cluster, etc.)
source scripts/load_env.sh

# 4. Kick off the batch pipeline. S3 → RDS → Redshift → ML jobs
#    run automatically via S3 events + in-lambda invocations.
./scripts/setup_data.sh

# 5. Push the dashboard image to ECR and force an ECS redeploy (once per
#    fresh deploy; ECR is empty until this runs).
./scripts/deploy_dashboard.sh

# 6. (Optional) live-tail every pipeline Lambda in one stream:
./scripts/tail_all.sh batch

# 7. (Optional) stream post-cutoff events into Kinesis:
./scripts/stream_demo.sh        # or: MAX_EVENTS=2000 EVENTS_PER_SECOND=200 ./scripts/stream_demo.sh

# 8. Explore the running system:
./scripts/show_api.sh           # hit the REST API
./scripts/show_data.sh          # snapshot RDS row counts + stream totals
echo "$API/docs"                # Swagger UI
echo "$DASHBOARD_URL"           # Streamlit dashboard

# 9. Reset data without destroying infra (re-invokes schema_init + clears S3 state):
./scripts/reset_data.sh && ./scripts/setup_data.sh

# 10. Tear down cheaply (deletes orphan ENIs before terraform destroy):
./scripts/fast_destroy.sh
```

### Scripts reference

| Script | Purpose |
|---|---|
| `package_lambdas.sh` | Build Lambda zips and the psycopg2 layer |
| `setup_data.sh` | Copy Synthea CSVs to S3 + upload manifest (triggers the pipeline) |
| `load_env.sh` | `source`-able — exports `API`, `S3_BUCKET`, `KINESIS_STREAM`, `DASHBOARD_URL`, `REDSHIFT_CLUSTER`, Lambda function names |
| `show_api.sh` | Hit representative REST endpoints |
| `show_data.sh` | Snapshot current RDS row counts + stream consumer totals |
| `stream_demo.sh` | End-to-end streaming demo (reset → simulate → drain → snapshot) |
| `tail_all.sh` | Live-tail every pipeline Lambda in one stream. `batch` / `stream` / `api` / `all` |
| `reset_data.sh` | Wipe RDS + Redshift data (without destroying infra) |
| `redshift_query.sh` | `source`-able — gives you an `rs "<SQL>"` helper via the Redshift Data API |
| `deploy_dashboard.sh` | Build dashboard container, push to ECR, force ECS redeploy |
| `fast_destroy.sh` | Delete orphan Lambda ENIs then `terraform destroy` (~10 min vs ~30 min) |

## Data

Uses [Synthea](https://github.com/synthetichealth/synthea) synthetic patient data. No real PHI. See `docs/data_guide.md`.

## License

MIT
