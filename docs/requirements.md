# Requirements Specifications

**Project:** Open-Source Healthcare Data Engineering Framework

**Author:** Morteza Rahimi (A00981311)

**Date:** January 23, 2026

---

## Table of Contents

1. [User Stories & Functional Requirements](#1-user-stories--functional-requirements)
2. [Non-Functional Requirements](#2-non-functional-requirements)
3. [Sprint Plan](#3-sprint-plan)
4. [Backlog](#4-backlog)

---

## 1. User Stories & Functional Requirements

Requirements are prioritized using MoSCoW: **Must Have**, **Should Have**, **Could Have**.

### Personas

| Persona | Description |
|---------|-------------|
| **Data Engineer** | Deploys infrastructure, configures pipelines, runs ETL jobs, monitors system health |
| **Healthcare Analyst** | Queries patient data, views dashboards, explores analytics, interprets ML results |
| **Developer** | Integrates with the platform via REST API for external applications |

---

### US-1: Infrastructure Deployment via Terraform

**Priority:** Must Have
**Sprint:** 1
**Persona:** Data Engineer

> As a **Data Engineer**, I want to deploy the complete AWS infrastructure using a single Terraform configuration, so that the entire platform can be reproducibly provisioned without manual setup.

**Acceptance Test:**
1. Run `terraform init` followed by `terraform apply` from the `terraform/` directory.
2. Verify the following resources are created in AWS:
   - VPC with public and private subnets across 2 availability zones
   - S3 bucket for raw data storage
   - RDS PostgreSQL instance in a private subnet
   - IAM roles with least-privilege policies for Lambda functions
   - Security groups restricting access between components
3. Run `terraform output` and confirm all endpoint values are populated.
4. Run `terraform destroy` to confirm clean teardown of all resources.

---

### US-2: Batch Data Ingestion (S3 Upload)

**Priority:** Must Have
**Sprint:** 1
**Persona:** Data Engineer

> As a **Data Engineer**, I want to upload Synthea CSV files to an S3 bucket, so that raw healthcare data is stored in the data lake ready for ETL processing.

**Acceptance Test:**
1. Generate or download a Synthea dataset (patients.csv, encounters.csv, conditions.csv, medications.csv).
2. Upload CSV files to the designated S3 bucket using AWS CLI or console.
3. Verify files are stored in S3 with correct paths (e.g., `s3://bucket/raw/patients.csv`).
4. Confirm S3 bucket versioning is enabled and public access is blocked.

---

### US-3: ETL Pipeline — S3 to RDS (Ingestion ETL)

**Priority:** Must Have
**Sprint:** 1
**Persona:** Data Engineer

> As a **Data Engineer**, I want to run an ETL pipeline that extracts Synthea CSV data from S3, validates data quality, transforms data types, and loads it into RDS PostgreSQL, so that operational data is available for transactional queries.

**Acceptance Test:**
1. Trigger the ETL Lambda function (manually or via S3 event).
2. Verify the following transformations occur:
   - Date strings (e.g., `'1956-08-23'`) are converted to proper `TIMESTAMP` types.
   - Required fields are validated (records with missing `patient_id` or `encounter_id` are rejected and logged).
   - Foreign key relationships are checked (encounters reference valid patients).
3. Query RDS PostgreSQL and confirm:
   - `patients` table contains the expected row count matching the source CSV.
   - `encounters`, `conditions`, `medications` tables are populated with correct foreign key references.
4. Verify the data quality report is logged (record counts, rejection counts, validation summary).
5. Re-run ETL on the same data and confirm no duplicate records are created.

---

### US-4: RDS Operational Database Schema

**Priority:** Must Have
**Sprint:** 1
**Persona:** Data Engineer

> As a **Data Engineer**, I want a normalized relational schema in RDS PostgreSQL with proper indexes, constraints, and views, so that operational queries perform efficiently and data integrity is maintained.

**Acceptance Test:**
1. Connect to RDS PostgreSQL and verify tables exist: `patients`, `encounters`, `conditions`, `medications`.
2. Confirm primary keys, foreign keys, and NOT NULL constraints are enforced.
3. Confirm indexes exist on frequently queried columns (`patient_id`, `encounter_id`, `birthdate`, `code`).
4. Run the `data_quality_check` view and verify it returns zero orphaned records.
5. Execute a sample transactional query (e.g., "retrieve all encounters for patient X") and confirm it returns correct results.

---

### US-5: Redshift Data Warehouse Deployment

**Priority:** Must Have
**Sprint:** 2
**Persona:** Data Engineer

> As a **Data Engineer**, I want to deploy a Redshift cluster via Terraform with a dimensional model (fact and dimension tables), so that analytical workloads are separated from operational queries.

**Acceptance Test:**
1. Run `terraform apply` and confirm Redshift cluster is provisioned.
2. Connect to Redshift and verify dimensional model tables exist:
   - Dimension tables: `dim_patient`, `dim_time`
   - Fact tables: `fact_encounters`, `fact_conditions`, `fact_medications`, `fact_patient_metrics`
3. Verify distribution keys and sort keys are configured for query performance.
4. Confirm the cluster is accessible only from within the VPC security group.

---

### US-6: ETL Pipeline — RDS to Redshift (Analytics ETL)

**Priority:** Must Have
**Sprint:** 2
**Persona:** Data Engineer

> As a **Data Engineer**, I want an ETL pipeline that transforms normalized RDS data into denormalized dimensional models in Redshift, including data enrichment and aggregation, so that the data warehouse is optimized for analytical queries.

**Acceptance Test:**
1. Trigger the RDS-to-Redshift ETL Lambda function.
2. Verify enrichment transformations:
   - Patient age is calculated from birthdate.
   - Encounters are categorized by type (wellness, emergency, inpatient, outpatient).
   - Patient metrics are aggregated (total encounters, total conditions, healthcare costs).
3. Query `fact_patient_metrics` in Redshift and confirm aggregated values match source data in RDS.
4. Verify dimension tables contain no duplicate keys.
5. Run a sample analytical query (e.g., "average healthcare cost by age group") and confirm it returns results in under 10 seconds.

---

### US-7: REST API for Operational Data Access

**Priority:** Must Have
**Sprint:** 2
**Persona:** Developer

> As a **Developer**, I want a REST API with documented endpoints for querying patient, encounter, condition, and medication data from RDS, so that I can integrate healthcare data into external applications.

**Acceptance Test:**
1. Start the FastAPI application and navigate to the auto-generated `/docs` (Swagger UI).
2. Test the following endpoints and verify correct JSON responses:
   - `GET /patients` — returns paginated list of patients
   - `GET /patients/{id}` — returns a single patient's demographics
   - `GET /patients/{id}/encounters` — returns encounters for a specific patient
   - `GET /conditions?code={snomed_code}` — searches conditions by SNOMED code
   - `GET /medications?patient_id={id}` — returns medications for a patient
   - `GET /health` — returns API health status
3. Verify response times are under 1 second for all endpoints.
4. Verify error handling: requesting a non-existent patient returns `404` with a descriptive message.

---

### US-8: Patient Segmentation via K-Means Clustering

**Priority:** Must Have
**Sprint:** 2
**Persona:** Healthcare Analyst

> As a **Healthcare Analyst**, I want to run a machine learning pipeline that segments patients into clusters based on healthcare utilization patterns, so that I can identify distinct patient populations (e.g., healthy/low-utilization, chronic care, complex/high-cost).

**Acceptance Test:**
1. Run the ML clustering pipeline against `fact_patient_metrics` in Redshift.
2. Verify K-means produces 3 clusters with meaningful separation:
   - Cluster labels are assigned (e.g., "Healthy & Young", "Chronic Care", "Complex/Elderly").
   - Within-cluster variance is lower than between-cluster variance.
3. Confirm `cluster_id` and `cluster_label` are written back to `fact_patient_metrics` in Redshift.
4. Verify results are reproducible with a fixed random seed.
5. Query cluster statistics (average age, average cost, average encounters per cluster) and confirm they show meaningful differentiation.

---

### US-9: Real-Time Data Streaming via Kinesis

**Priority:** Should Have
**Sprint:** 3
**Persona:** Data Engineer

> As a **Data Engineer**, I want to ingest simulated real-time patient events through Kinesis Data Streams and process them via a Lambda consumer, so that the platform demonstrates both batch and streaming ingestion patterns.

**Acceptance Test:**
1. Run the Synthea event simulator script, which publishes synthetic patient events to a Kinesis Data Stream.
2. Verify the Lambda stream consumer processes events and inserts them into RDS.
3. Confirm events are also archived to S3 via Kinesis Firehose.
4. Verify processing latency is under 5 seconds from event publish to RDS availability.
5. Stop the simulator and confirm no data loss (published event count matches processed count).

---

### US-10: Interactive Analytics Dashboard

**Priority:** Must Have
**Sprint:** 3
**Persona:** Healthcare Analyst

> As a **Healthcare Analyst**, I want an interactive dashboard that visualizes key healthcare metrics from the Redshift data warehouse, so that I can explore population health trends, disease prevalence, and patient segments without writing SQL.

**Acceptance Test:**
1. Open the dashboard application and verify the following views are available:
   - **Population Health**: Patient demographics by age group, gender, and geographic distribution.
   - **Disease Analytics**: Top diagnoses by frequency, condition prevalence trends.
   - **Medication Insights**: Most prescribed medications, cost analysis by medication.
   - **Healthcare Utilization**: Encounter volume by type, cost distribution.
   - **Patient Segments**: K-means cluster visualization with summary statistics per segment.
2. Verify interactive filtering works (e.g., filter by gender, age group, or condition).
3. Confirm dashboard loads within 5 seconds and updates within 3 seconds when filters are applied.

---

### US-11: Security Best Practices Implementation

**Priority:** Must Have
**Sprint:** 3
**Persona:** Data Engineer

> As a **Data Engineer**, I want the platform to implement AWS security best practices (IAM least-privilege, VPC isolation, encryption at rest and in transit), so that the architecture demonstrates production-grade security patterns suitable for healthcare data.

**Acceptance Test:**
1. Verify IAM roles follow least-privilege: ETL Lambda can read S3 and write RDS/Redshift but cannot modify IAM or networking resources.
2. Verify RDS and Redshift are in private subnets, not publicly accessible.
3. Verify encryption at rest is enabled (KMS) for S3 buckets, RDS, and Redshift.
4. Verify all connections use TLS/SSL (database connections, API traffic).
5. Verify security groups restrict traffic: only the API can connect to RDS on port 5432; only ETL functions can connect to Redshift on port 5439.
6. Verify no hardcoded credentials in source code — all secrets use environment variables or AWS Secrets Manager.

---

### US-12: Comprehensive Technical Documentation

**Priority:** Must Have
**Sprint:** 3
**Persona:** Data Engineer

> As a **Data Engineer**, I want comprehensive documentation including a deployment guide, architecture diagrams, data flow descriptions, and API reference, so that I can independently reproduce and understand the entire platform.

**Acceptance Test:**
1. Follow the deployment guide from a clean AWS account and successfully deploy all infrastructure via Terraform.
2. Verify the architecture diagram accurately reflects the deployed system (all components, data flows, and service connections).
3. Verify the API reference (auto-generated Swagger + supplementary docs) covers all endpoints with example requests and responses.
4. Verify the ETL documentation describes each pipeline stage, data transformations, and error handling behavior.

---

### US-13: Data Quality Monitoring and Error Handling

**Priority:** Should Have
**Sprint:** 2
**Persona:** Data Engineer

> As a **Data Engineer**, I want the ETL pipeline to log data quality metrics and handle errors gracefully (rejecting bad records without failing the entire job), so that I can monitor pipeline health and investigate data issues.

**Acceptance Test:**
1. Submit a CSV file with intentionally malformed records (missing patient_id, invalid dates, negative costs).
2. Verify the pipeline completes successfully, loading valid records and rejecting invalid ones.
3. Verify a data quality report is generated with: total records processed, records accepted, records rejected, and rejection reasons.
4. Verify CloudWatch logs capture pipeline execution details and errors.

---

### US-14: Scalability Validation

**Priority:** Could Have
**Sprint:** 5
**Persona:** Data Engineer

> As a **Data Engineer**, I want to validate that the platform processes datasets of varying sizes (1K, 100K patients) without architectural changes, so that scalability characteristics are documented.

**Acceptance Test:**
1. Run the full pipeline with a 1,000-patient dataset and record processing time.
2. Run the full pipeline with a 100,000-patient dataset and record processing time.
3. Verify no architectural changes are required between dataset sizes.
4. Document processing time, resource utilization, and any bottlenecks observed.

---

## 2. Non-Functional Requirements

### NFR-1: Performance

**Requirement:** ETL pipeline processes a 100,000-patient Synthea dataset within 60 minutes end-to-end. Redshift analytical queries return results in under 10 seconds for standard aggregations. FastAPI endpoints respond in under 1 second.

**Test:**
- Time the full ETL pipeline (S3 → RDS → Redshift) on a 100K-patient dataset. Record total duration.
- Execute 5 representative analytical queries on Redshift (demographic aggregation, diagnosis counts, cost analysis, cluster statistics, encounter trends). Record each query's execution time.
- Use `curl` or a load testing tool to measure API response times across 100 sequential requests to each endpoint. Calculate p50, p95, and p99 latencies.

---

### NFR-2: Data Integrity

**Requirement:** Zero data loss or corruption throughout the pipeline. Record counts at each stage (S3 source → RDS → Redshift) must be reconcilable. All foreign key relationships must be valid.

**Test:**
- Compare row counts at each pipeline stage: source CSV, RDS tables, Redshift fact tables.
- Run referential integrity checks: all `patient_id` values in `encounters`, `conditions`, and `medications` exist in `patients`.
- Run the `data_quality_check` view and confirm zero orphaned records.
- Verify aggregate values (total cost, total encounters) match between RDS and Redshift.

---

### NFR-3: Reproducibility

**Requirement:** The entire platform can be deployed from scratch using only the repository contents and documented prerequisites. No manual AWS console steps required beyond initial account setup.

**Test:**
- From a clean AWS account with only CLI credentials configured, follow the deployment guide.
- Execute `terraform apply` and verify all resources are created.
- Run the ETL pipeline and API, confirming functionality matches documentation.
- Have a second person (e.g., instructor) follow the same steps and confirm success.

---

### NFR-4: Security

**Requirement:** No hardcoded credentials in source code. All data encrypted at rest and in transit. Network access follows least-privilege principle. Architecture aligns with AWS Well-Architected Framework healthcare lens.

**Test:**
- Scan the repository for secrets using `git secrets` or `truffleHog`. Confirm zero findings.
- Verify AWS KMS encryption is enabled on S3, RDS, and Redshift via AWS CLI or Terraform state.
- Verify TLS is enforced on all database connections and API endpoints.
- Attempt to connect to RDS/Redshift from outside the VPC and confirm connection is refused.

---

### NFR-5: Availability and Error Recovery

**Requirement:** ETL pipeline failures are logged and do not leave the system in an inconsistent state. Failed jobs can be re-run without creating duplicates. The API remains available even if the ETL pipeline is running.

**Test:**
- Simulate an ETL failure mid-pipeline (e.g., disconnect database during load) and verify partial data is rolled back.
- Re-run a successful ETL job and confirm no duplicate records.
- Start an ETL job and simultaneously query the API — confirm API responds normally.

---

### NFR-6: Maintainability

**Requirement:** Code follows consistent style, is modular, and includes sufficient inline documentation for a new developer to understand each component. Terraform configurations are modular and parameterized.

**Test:**
- Run a Python linter (`flake8` or `ruff`) on all Python code with zero errors.
- Verify Terraform variables are used for all configurable values (no hardcoded resource names, regions, or sizes).
- Verify each ETL script, API module, and Terraform file has a header comment describing its purpose.

---

## 3. Sprint Plan

### Sprint 1: Core Infrastructure & Batch ETL Pipeline
**Duration:** Jan 23 – Jan 30, 2026
**Demo Date:** Jan 30, 2026 (Submission 2)

**Goal:** Deploy AWS infrastructure via Terraform and demonstrate a working batch ETL pipeline that loads Synthea CSV data into RDS PostgreSQL.

**User Stories:** US-1, US-2, US-3, US-4
**Demo:** Run `terraform apply` → upload Synthea CSVs to S3 → trigger ETL → query patient data in RDS.

---

### Sprint 2: Data Warehouse + API + ML
**Duration:** Jan 30 – Feb 13, 2026
**Demo Date:** Feb 13, 2026 (Submission 3)

**Goal:** Extend the platform with a Redshift data warehouse, FastAPI REST API, and ML patient segmentation.

**User Stories:** US-5, US-6, US-7, US-8, US-13
**Demo:** Show full pipeline S3 → RDS → Redshift → query analytical data. Demo API endpoints in Swagger UI. Show K-means clustering results.

---

### Sprint 3: Streaming + Dashboard + Security = MVP
**Duration:** Feb 13 – Feb 27, 2026
**Demo Date:** Feb 27, 2026 (Submission 4)

**Goal:** Complete the MVP with real-time streaming, interactive analytics dashboard, security hardening, and documentation.

**User Stories:** US-9, US-10, US-11, US-12
**Demo:** Full end-to-end platform — batch pipeline, streaming ingestion, analytics dashboard, API, ML results. Show security controls.

---

### Sprint 4: QA & Testing
**Duration:** Feb 27 – Mar 13, 2026
**Demo Date:** Mar 13, 2026 (Submission 5)

**Goal:** Comprehensive testing across all components. Document test results.

**Focus Areas:**
- Data quality & validation tests (NFR-2)
- ETL pipeline tests (error handling, idempotency)
- Performance benchmarking (NFR-1)
- Security validation (NFR-4)
- Integration tests (end-to-end pipeline)
- API tests (endpoint correctness, error handling)

---

### Sprint 5: Refactoring + Final Polish
**Duration:** Mar 13 – Mar 27, 2026
**Demo Dates:** Mar 20 (Submission 6), Mar 27 (Submission 7)

**Goal:** Code refactoring based on QA findings, scalability validation, documentation completion, and final demo preparation.

**User Stories:** US-14
**Focus Areas:**
- Refactor code based on test findings (NFR-6)
- Scalability testing with larger datasets
- Final documentation review
- Video demo recording for final submission

---

## 4. Backlog

Each subtask is estimated at 4–5 hours of effort.

### Sprint 1: Core Infrastructure & Batch ETL Pipeline

| ID | Subtask | User Story | Est. Hours |
|----|---------|-----------|------------|
| B-1.1 | Create Terraform modules: VPC, subnets, internet gateway, route tables, security groups | US-1 | 5 |
| B-1.2 | Create Terraform modules: S3 buckets (data + lambda packages) with versioning and access policies | US-1, US-2 | 4 |
| B-1.3 | Create Terraform modules: RDS PostgreSQL instance with parameter groups and subnet groups | US-1 | 5 |
| B-1.4 | Create Terraform modules: IAM roles and policies for Lambda execution | US-1 | 4 |
| B-1.5 | Write RDS schema SQL: tables (patients, encounters, conditions, medications), indexes, constraints, views | US-4 | 5 |
| B-1.6 | Develop ETL Lambda: S3 CSV reader with schema validation and data type conversion | US-3 | 5 |
| B-1.7 | Develop ETL Lambda: Data quality validation (required fields, foreign keys, value ranges) and error logging | US-3 | 5 |
| B-1.8 | Develop ETL Lambda: RDS PostgreSQL loader with upsert logic (idempotent loads) | US-3 | 4 |
| B-1.9 | Integration testing: end-to-end batch pipeline (upload → ETL → query RDS) and Sprint 1 demo prep | US-1–4 | 5 |

**Sprint 1 Total: ~42 hours**

---

### Sprint 2: Data Warehouse + API + ML

| ID | Subtask | User Story | Est. Hours |
|----|---------|-----------|------------|
| B-2.1 | Create Terraform module: Redshift cluster with subnet group and security group | US-5 | 4 |
| B-2.2 | Write Redshift schema SQL: dimension tables (dim_patient, dim_time) and fact tables (fact_encounters, fact_conditions, fact_medications, fact_patient_metrics) | US-5 | 5 |
| B-2.3 | Develop ETL Lambda: RDS → Redshift transformer with data enrichment (age calculation, encounter categorization, metric aggregation) | US-6 | 5 |
| B-2.4 | Develop ETL Lambda: Redshift loader for dimension and fact tables with data validation | US-6 | 5 |
| B-2.5 | Build FastAPI application: project setup, database connection, patient and encounter endpoints | US-7 | 5 |
| B-2.6 | Build FastAPI application: condition and medication endpoints, error handling, pagination | US-7 | 4 |
| B-2.7 | Develop ML pipeline: K-means clustering on patient metrics, cluster labeling, write-back to Redshift | US-8 | 5 |
| B-2.8 | Develop data quality monitoring: validation reports, CloudWatch logging integration | US-13 | 4 |
| B-2.9 | Integration testing: full pipeline (S3 → RDS → Redshift → API) and Sprint 2 demo prep | US-5–8 | 5 |

**Sprint 2 Total: ~42 hours**

---

### Sprint 3: Streaming + Dashboard + Security (MVP)

| ID | Subtask | User Story | Est. Hours |
|----|---------|-----------|------------|
| B-3.1 | Create Terraform modules: Kinesis Data Stream, Kinesis Firehose, Lambda consumer | US-9 | 5 |
| B-3.2 | Develop Synthea event simulator script (publishes synthetic events to Kinesis) | US-9 | 4 |
| B-3.3 | Develop Lambda stream consumer: process Kinesis events, insert into RDS | US-9 | 5 |
| B-3.4 | Build analytics dashboard: population health and disease analytics views | US-10 | 5 |
| B-3.5 | Build analytics dashboard: medication insights, healthcare utilization, and patient segment views | US-10 | 5 |
| B-3.6 | Security hardening: move databases to private subnets, enable KMS encryption, enforce TLS | US-11 | 5 |
| B-3.7 | Security hardening: remove hardcoded credentials, implement Secrets Manager, tighten IAM policies | US-11 | 4 |
| B-3.8 | Write technical documentation: deployment guide, architecture diagrams, API reference | US-12 | 5 |
| B-3.9 | MVP integration testing: end-to-end validation and Sprint 3 demo prep | US-9–12 | 5 |

**Sprint 3 Total: ~43 hours**

---

### Sprint 4: QA & Testing

| ID | Subtask | User Story | Est. Hours |
|----|---------|-----------|------------|
| B-4.1 | Data quality tests: schema validation, completeness checks, referential integrity, range validation | NFR-2 | 5 |
| B-4.2 | ETL pipeline tests: error handling, idempotency, malformed input, partial failure recovery | US-3, US-6 | 5 |
| B-4.3 | API tests: endpoint correctness, error responses, pagination, response time validation | US-7, NFR-1 | 4 |
| B-4.4 | Performance benchmarking: ETL timing, Redshift query timing, API latency measurement | NFR-1 | 5 |
| B-4.5 | Security testing: credential scanning, encryption verification, network access validation | NFR-4 | 4 |
| B-4.6 | Integration tests: end-to-end pipeline, streaming pipeline, dashboard data accuracy | All | 5 |
| B-4.7 | Document all test results with evidence (screenshots, logs, metrics) | Submission 5 | 5 |

**Sprint 4 Total: ~33 hours**

---

### Sprint 5: Refactoring + Final Polish

| ID | Subtask | User Story | Est. Hours |
|----|---------|-----------|------------|
| B-5.1 | Code refactoring: address issues found during QA, improve modularity and error handling | NFR-6 | 5 |
| B-5.2 | Code quality: linting, consistent style, add header comments to all modules | NFR-6 | 4 |
| B-5.3 | Scalability validation: test with 100K patient dataset, document performance characteristics | US-14 | 5 |
| B-5.4 | Documentation review: verify deployment guide, update architecture diagrams, finalize README | US-12 | 5 |
| B-5.5 | Final demo preparation: record video demonstration of complete platform capabilities | Submission 7 | 5 |

**Sprint 5 Total: ~24 hours**

---

**Grand Total: ~184 hours** (~37 hours/week across 5 sprints)
