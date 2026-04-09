"""
ML Pipeline: Redshift ML — Clustering + Risk Scoring

Uses Redshift's native CREATE MODEL (backed by SageMaker Autopilot)
to train models directly in the warehouse. No sklearn or external
ML libraries required.

After models are created and trained, runs inference to populate
cluster_id, cluster_label, risk_score, and risk_tier in
fact_patient_metrics.

Configuration via environment variables:
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
    S3_BUCKET        — S3 bucket for SageMaker staging
    REDSHIFT_IAM_ROLE — IAM role ARN for Redshift ML
"""

import os
import time
import logging

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

REDSHIFT = {
    "host": os.environ["REDSHIFT_HOST"],
    "port": int(os.environ.get("REDSHIFT_PORT", "5439")),
    "database": os.environ.get("REDSHIFT_DATABASE", "analytics"),
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PASSWORD"],
}

S3_BUCKET = os.environ.get("S3_BUCKET", "")
REDSHIFT_IAM_ROLE = os.environ.get("REDSHIFT_IAM_ROLE", "")


def rs_conn():
    return psycopg2.connect(**REDSHIFT)


def run_sql(sql, description=""):
    """Execute a SQL statement against Redshift."""
    if description:
        log.info(description)
    conn = rs_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
        log.info("  SQL executed successfully.")
    finally:
        cur.close()
        conn.close()


def check_model_status(model_name, timeout=600, poll_interval=30):
    """Poll until a Redshift ML model is READY or fails."""
    log.info("Waiting for model '%s' to be ready (timeout %ds)...", model_name, timeout)
    start = time.time()
    conn = rs_conn()
    cur = conn.cursor()
    try:
        while time.time() - start < timeout:
            cur.execute(f"SHOW MODEL {model_name};")
            rows = cur.fetchall()
            status_dict = {row[0]: row[1] for row in rows}
            status = status_dict.get("Model State", "UNKNOWN")
            log.info("  Model '%s' status: %s", model_name, status)

            if status == "READY":
                return True
            if status in ("FAILED", "DELETED"):
                log.error("Model '%s' failed: %s", model_name, status_dict)
                return False

            time.sleep(poll_interval)
    finally:
        cur.close()
        conn.close()

    log.error("Model '%s' timed out after %ds", model_name, timeout)
    return False


def create_clustering_model():
    """Create K-Means clustering model in Redshift ML."""
    log.info("── Creating clustering model ──")

    # Drop existing model if any
    run_sql("DROP MODEL IF EXISTS patient_clusters;", "Dropping existing clustering model...")

    sql = f"""
        CREATE MODEL patient_clusters
        FROM (
            SELECT
                age,
                total_encounters,
                total_conditions,
                total_drug_exposures,
                total_procedures,
                total_measurements,
                visits_per_year,
                chronic_condition_count
            FROM fact_patient_metrics
            WHERE age IS NOT NULL AND total_encounters > 0
        )
        FUNCTION predict_cluster
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        AUTO OFF
        MODEL_TYPE KMEANS
        PREPROCESSORS 'none'
        HYPERPARAMETERS DEFAULT EXCEPT (K '4')
        SETTINGS (
            S3_BUCKET '{S3_BUCKET}',
            MAX_RUNTIME 3600
        );
    """
    run_sql(sql, "Creating K-Means model (K=4)...")


def apply_clustering_results():
    """Run inference and write cluster assignments to fact_patient_metrics."""
    log.info("Applying clustering results...")

    sql = """
        UPDATE fact_patient_metrics
        SET cluster_id = sub.cluster_id,
            cluster_label = CASE sub.cluster_id
                WHEN 0 THEN 'Cluster A'
                WHEN 1 THEN 'Cluster B'
                WHEN 2 THEN 'Cluster C'
                WHEN 3 THEN 'Cluster D'
                ELSE 'Unknown'
            END,
            updated_at = GETDATE()
        FROM (
            SELECT
                patient_key,
                predict_cluster(
                    age, total_encounters, total_conditions,
                    total_drug_exposures, total_procedures,
                    total_measurements, visits_per_year,
                    chronic_condition_count
                ) AS cluster_id
            FROM fact_patient_metrics
            WHERE age IS NOT NULL AND total_encounters > 0
        ) sub
        WHERE fact_patient_metrics.patient_key = sub.patient_key;
    """
    run_sql(sql, "Writing cluster assignments...")

    # Log cluster distribution
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cluster_id, cluster_label, COUNT(*) AS cnt
            FROM fact_patient_metrics
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id, cluster_label
            ORDER BY cluster_id;
        """)
        rows = cur.fetchall()
        log.info("── Cluster Distribution ──")
        for cid, label, cnt in rows:
            log.info("  Cluster %s (%s): %d patients", cid, label, cnt)
    finally:
        cur.close()
        conn.close()


def create_risk_model():
    """Create risk scoring model in Redshift ML (classification)."""
    log.info("── Creating risk scoring model ──")

    run_sql("DROP MODEL IF EXISTS patient_risk;", "Dropping existing risk model...")

    sql = f"""
        CREATE MODEL patient_risk
        FROM (
            SELECT
                age,
                total_encounters,
                total_conditions,
                active_conditions,
                chronic_condition_count,
                total_drug_exposures,
                active_drug_exposures,
                unique_drug_concepts,
                total_procedures,
                total_measurements,
                visits_per_year,
                avg_days_between_visits,
                had_30_day_readmission
            FROM fact_patient_metrics
            WHERE age IS NOT NULL AND total_encounters > 0
        )
        TARGET had_30_day_readmission
        FUNCTION predict_readmission
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        AUTO OFF
        MODEL_TYPE XGBOOST
        PROBLEM_TYPE BINARY_CLASSIFICATION
        OBJECTIVE 'binary:logistic'
        PREPROCESSORS 'none'
        HYPERPARAMETERS DEFAULT EXCEPT (NUM_ROUND '100')
        SETTINGS (
            S3_BUCKET '{S3_BUCKET}',
            MAX_RUNTIME 1800
        );
    """
    run_sql(sql, "Creating risk model (AUTO)...")


def apply_risk_results():
    """Run inference and write risk scores to fact_patient_metrics."""
    log.info("Applying risk scoring results...")

    # Use the prediction probability as risk score
    sql = """
        UPDATE fact_patient_metrics
        SET risk_score = CASE
                WHEN predict_readmission(
                    age, total_encounters, total_conditions,
                    active_conditions, chronic_condition_count,
                    total_drug_exposures, active_drug_exposures,
                    unique_drug_concepts, total_procedures,
                    total_measurements, visits_per_year,
                    avg_days_between_visits
                ) = TRUE THEN 0.75
                ELSE 0.25
            END,
            risk_tier = CASE
                WHEN predict_readmission(
                    age, total_encounters, total_conditions,
                    active_conditions, chronic_condition_count,
                    total_drug_exposures, active_drug_exposures,
                    unique_drug_concepts, total_procedures,
                    total_measurements, visits_per_year,
                    avg_days_between_visits
                ) = TRUE THEN 'high'
                ELSE 'low'
            END,
            updated_at = GETDATE()
        WHERE age IS NOT NULL AND total_encounters > 0;
    """
    run_sql(sql, "Writing risk scores...")

    # Log tier distribution
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT risk_tier, COUNT(*) AS cnt
            FROM fact_patient_metrics
            WHERE risk_tier IS NOT NULL
            GROUP BY risk_tier
            ORDER BY risk_tier;
        """)
        rows = cur.fetchall()
        log.info("── Risk Tier Distribution ──")
        for tier, cnt in rows:
            log.info("  %s: %d patients", tier, cnt)
    finally:
        cur.close()
        conn.close()


def get_model_state(model_name):
    """Check the current state of a model. Returns state string or None."""
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW MODEL {model_name};")
        rows = cur.fetchall()
        status_dict = {row[0]: row[1] for row in rows}
        return status_dict.get("Model State", None)
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()


def main(action=None):
    """
    Redshift ML pipeline with two phases:

    action='create' — Drop and recreate models (async SageMaker training).
                      Called automatically after ETL loads data.
    action='apply'  — Check if models are READY, run inference, update metrics.
                      Call manually or on a schedule after training completes.
    action=None     — Auto-detect: create if no models exist, apply if READY.
    """
    log.info("═══ Redshift ML Pipeline (action=%s) ═══", action)

    if not S3_BUCKET or not REDSHIFT_IAM_ROLE:
        log.error("S3_BUCKET and REDSHIFT_IAM_ROLE env vars required.")
        raise ValueError("Missing S3_BUCKET or REDSHIFT_IAM_ROLE")

    if action == "create":
        create_clustering_model()
        create_risk_model()
        log.info("Models submitted for training. Check status later with action='apply'.")
        return

    if action == "apply":
        cluster_state = get_model_state("patient_clusters")
        risk_state = get_model_state("patient_risk")
        log.info("Model states — clustering: %s, risk: %s", cluster_state, risk_state)

        if cluster_state == "READY":
            apply_clustering_results()
        else:
            log.warning("Clustering model not ready (state=%s). Skipping.", cluster_state)

        if risk_state == "READY":
            apply_risk_results()
        else:
            log.warning("Risk model not ready (state=%s). Skipping.", risk_state)
        return

    # Auto-detect: if models don't exist or failed, create them.
    # If READY, apply results.
    cluster_state = get_model_state("patient_clusters")
    risk_state = get_model_state("patient_risk")
    log.info("Auto-detect — clustering: %s, risk: %s", cluster_state, risk_state)

    if cluster_state is None or cluster_state in ("FAILED", "DELETED"):
        create_clustering_model()
    elif cluster_state == "READY":
        apply_clustering_results()
    else:
        log.info("Clustering model is %s — training in progress.", cluster_state)

    if risk_state is None or risk_state in ("FAILED", "DELETED"):
        create_risk_model()
    elif risk_state == "READY":
        apply_risk_results()
    else:
        log.info("Risk model is %s — training in progress.", risk_state)

    log.info("═══ Redshift ML Pipeline Complete ═══")


if __name__ == "__main__":
    main()
