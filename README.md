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
├── scripts/         # package_lambdas.sh, setup_data.sh
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

# 2. Deploy infrastructure
cd terraform
terraform init
terraform apply
cd ..

# 3. Kick off the batch pipeline
#    (copies Synthea OMOP CSVs into S3, which triggers the chain:
#     S3 → RDS → Redshift → ML jobs)
./scripts/setup_data.sh

# 4. After ~5–10 min, apply trained ML model results back to fact tables
aws lambda invoke --function-name healthcare-dev-ml-redshift \
  --payload '{"action":"apply"}' --cli-binary-format raw-in-base64-out /tmp/out.json

# 5. Stream post-cutoff events into Kinesis (optional)
python3 etl/stream_simulator.py

# 6. Tear down when done (RDS + Redshift + NAT accrue cost)
cd terraform && terraform destroy
```

## Data

Uses [Synthea](https://github.com/synthetichealth/synthea) synthetic patient data. No real PHI. See `docs/data_guide.md`.

## License

MIT
