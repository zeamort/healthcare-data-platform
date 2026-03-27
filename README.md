# Open-Source Healthcare Data Engineering Framework

An end-to-end cloud-native data engineering platform using [Synthea](https://github.com/synthetichealth/synthea) synthetic patient data on AWS. Built as a reference implementation for healthcare data pipeline architecture — demonstrating batch ingestion, ETL, OLTP/OLAP separation, analytics, and machine learning with fully reproducible Infrastructure as Code.

## Architecture

```
Data Sources          Ingestion Layer        Storage Layer              Application Layer
┌──────────────┐      ┌───────────────┐      ┌────────────────────┐     ┌──────────────────┐
│ Synthea CSV  │─────▶│  AWS Lambda   │─────▶│  S3 (Data Lake)    │     │  FastAPI REST API │
│ (Batch)      │      │  (ETL)        │─────▶│  RDS PostgreSQL    │────▶│  (Operational)    │
└──────────────┘      └───────────────┘      │  (OLTP)            │     └──────────────────┘
                                             └────────┬───────────┘
┌──────────────┐      ┌───────────────┐               │              ┌──────────────────┐
│ Synthea      │─────▶│  Kinesis      │─────▶┌────────▼───────────┐  │  Analytics        │
│ Simulator    │      │  (Streaming)  │      │  Redshift          │──▶│  Dashboard        │
│ (Real-time)  │      └───────────────┘      │  (OLAP/Warehouse)  │  │  (Visualization)  │
└──────────────┘                             └────────────────────┘  └──────────────────┘
                                                      │
                                             ┌────────▼───────────┐
                                             │  ML Pipeline       │
                                             │  (K-means Patient  │
                                             │   Segmentation)    │
                                             └────────────────────┘
```

## Project Structure

```
├── terraform/       # Infrastructure as Code (AWS resources)
├── etl/             # ETL pipeline scripts (Lambda functions)
├── api/             # FastAPI REST API application
├── sql/             # Database schemas (RDS + Redshift)
├── scripts/         # Utility scripts (data generation, deployment helpers)
├── tests/           # Test suite
│   └── fixtures/    # Test data files
└── docs/            # Documentation and architecture diagrams
```

## Key Features

- **Batch ETL Pipeline**: S3 → Lambda → RDS PostgreSQL → Redshift
- **Real-time Streaming**: Kinesis Data Streams for continuous ingestion
- **OLTP/OLAP Separation**: RDS for operational queries, Redshift for analytics
- **Healthcare Analytics**: Patient segmentation, disease prevalence, medication patterns
- **ML Pipeline**: K-means clustering for patient population segmentation
- **REST API**: FastAPI endpoints for operational data access
- **Infrastructure as Code**: Full Terraform deployment

## Prerequisites

- AWS Account with appropriate permissions
- Terraform >= 1.0
- Python 3.9+
- AWS CLI configured

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd healthcare-data-platform

# 2. Deploy infrastructure
cd terraform
terraform init && terraform apply

# 3. Run ETL pipeline
cd ../etl
pip install -r requirements.txt
python run_pipeline.py

# 4. Start the API
cd ../api
pip install -r requirements.txt
uvicorn main:app --reload
```

## Data

This project uses [Synthea](https://github.com/synthetichealth/synthea) synthetic patient data. No real patient data is used. See `docs/data_guide.md` for instructions on generating or downloading datasets.

## License

MIT
