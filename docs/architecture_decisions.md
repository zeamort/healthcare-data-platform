# Architecture Decision Log

A running record of design decisions, what was considered, and why we chose what we did. Referenced during development and useful for the final project report.

---

## ADR-001: Lambda over AWS Glue for ETL

**Date:** 2026-01-23
**Status:** Accepted

**Context:** The proposal specified AWS Glue for ETL jobs. Glue provides managed Spark infrastructure but has a minimum billing of 1 DPU (~$0.44/min) and cold-start times of 1-2 minutes.

**Options:**
1. AWS Glue — managed Spark, good for TB-scale, expensive for small datasets
2. AWS Lambda — serverless, sub-second cold start, 15-min max runtime, cheap
3. ECS/Fargate tasks — containerized, flexible, more operational overhead

**Decision:** Use Lambda. Synthea datasets at our target scale (1K–100K patients) produce CSV files well under Lambda's memory/time limits. The ETL logic is straightforward row-by-row transformation, not distributed Spark workloads. Lambda is ~10x cheaper for our use case and demonstrates the same serverless ETL pattern.

**Trade-off:** Lambda has a 15-minute timeout and 10GB memory limit. For the 2.8M patient stretch goal, we may need to split files or switch to Step Functions orchestrating multiple Lambda invocations. This is acceptable — the architecture supports it without redesign.

---

## ADR-002: Sensitive variables via tfvars, not hardcoded

**Date:** 2026-01-23
**Status:** Accepted

**Context:** The prototype hardcoded `DemoPass123!` in `main.tf` locals and in every Python script. This is a security anti-pattern — credentials in git history are effectively permanent.

**Decision:** Database credentials are Terraform input variables marked `sensitive = true`. They must be provided via `terraform.tfvars` (gitignored) or `TF_VAR_db_password` environment variable. ETL scripts read all configuration from environment variables.

**Future improvement:** For production, use AWS Secrets Manager with automatic rotation. Lambda can retrieve secrets at runtime via the Secrets Manager SDK. This is noted in the security documentation but not implemented in the educational version to keep the deployment simple.

---

## ADR-003: Private subnets for databases

**Date:** 2026-01-23
**Status:** Accepted

**Context:** The prototype placed RDS and Redshift in public subnets with `publicly_accessible = true` and `0.0.0.0/0` security groups. This was expedient for development but violates healthcare security best practices.

**Decision:** Databases are in private subnets with no public IP. A NAT gateway in the public subnet provides outbound internet access for private resources. Security groups allow inbound access only from the Lambda security group and explicitly allowed CIDR blocks (developer IP).

**Trade-off:** Adds ~$32/month for the NAT gateway. For development, you need to either: (a) connect via a bastion host, (b) use VPN, or (c) temporarily add your IP to `allowed_cidr_blocks`. This is the correct trade-off for a healthcare platform — inconvenience over insecurity.

---

## ADR-004: Kimball star schema for Redshift

**Date:** 2026-01-28
**Status:** Accepted

**Context:** The prototype used a flat `fact_patient_metrics` table in Redshift that combined dimensions and facts. This made some queries easy but violated dimensional modeling principles, couldn't support drill-down analytics, and would perform poorly at scale.

**Options:**
1. Flat denormalized table — simple, fast for pre-defined queries, inflexible
2. Star schema (Kimball methodology) — dimension + fact tables, flexible analytics, industry standard
3. OMOP CDM — healthcare-specific standard, very complex, overkill for educational scope

**Decision:** Implement a proper Kimball star schema with:
- **Dimension tables:** `dim_patient`, `dim_condition`, `dim_medication`, `dim_date`
- **Fact tables:** `fact_encounters`, `fact_conditions`, `fact_medications`
- **Aggregate table:** `fact_patient_metrics` (pre-computed for ML and dashboards)

Dimensions use surrogate keys and include descriptive attributes for filtering and grouping. Fact tables are at the grain of one event per row with foreign keys to all relevant dimensions and numeric measures.

**Rationale:** This is the pattern used in real healthcare data warehouses. OHDSI/OMOP follows similar dimensional principles. It enables ad-hoc analytics, supports drill-down, and performs well with Redshift's columnar storage and distribution/sort keys. It also demonstrates a valuable skill (dimensional modeling) in the capstone.

---

## ADR-005: Multiple ML approaches beyond K-means

**Date:** 2026-01-28
**Status:** Accepted

**Context:** The prototype only implemented K-means clustering with 3 hardcoded clusters. While functional, this doesn't demonstrate depth in healthcare analytics or ML methodology.

**Decision:** Implement three ML components:
1. **Patient segmentation (K-means)** — retained from prototype but improved with proper feature normalization, elbow method for K selection, and silhouette scoring for validation
2. **Patient risk scoring** — logistic regression or random forest to predict high-utilization patients based on demographics, condition count, and encounter history
3. **Disease co-occurrence analysis** — association analysis of which conditions appear together, relevant for comorbidity research

**Rationale:** These three approaches demonstrate different ML paradigms (unsupervised clustering, supervised classification, association analysis) and each has clear healthcare relevance. They all operate on the same `fact_patient_metrics` and fact table data, so the data pipeline supports them without modification.

---

## ADR-006: Data minimization in API responses

**Date:** 2026-01-28
**Status:** Accepted

**Context:** Even though Synthea data is synthetic, the API should demonstrate healthcare data access patterns. Real healthcare APIs must not expose unnecessary PII.

**Decision:** The FastAPI application will:
- Never return SSN, drivers license, or passport in API responses
- Separate endpoints for demographic data vs clinical data
- Include only the fields needed for each use case
- Document which fields are considered PII and why they're excluded

**Rationale:** This demonstrates the data minimization principle from HIPAA's minimum necessary standard. It's a design pattern, not a compliance requirement for synthetic data, but it shows the architecture is production-aware.

---

## ADR-007: Dashboard technology selection

**Date:** 2026-01-28
**Status:** Pending (evaluate during Sprint 3)

**Context:** The proposal specified Apache Superset. Superset is powerful but requires Docker deployment, has a complex configuration, and is heavy for demo purposes.

**Options:**
1. Apache Superset — full-featured BI tool, SQL-based, complex setup
2. Streamlit — Python-native, fast to build, interactive, easy to demo
3. Grafana — strong for time-series, connects to Postgres/Redshift natively, moderate setup

**Leaning toward:** Streamlit for speed of development and demo-ability, with the option to add Grafana for infrastructure monitoring views. Final decision deferred to Sprint 3 when we know what data is available and what looks best in a demo video.

---

*New decisions are appended below as they arise.*
