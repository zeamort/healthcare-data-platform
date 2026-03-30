"""
ML Pipeline: Patient Risk Scoring

Trains a Random Forest classifier to predict high-utilization patients
using the 30-day readmission flag as the target variable. Outputs a
continuous risk score (0-1) and categorical risk tier per patient.

Configuration via environment variables:
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""

import os
import logging

import numpy as np
import psycopg2
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report

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

FEATURE_COLUMNS = [
    "age",
    "total_encounters",
    "total_conditions",
    "active_conditions",
    "chronic_condition_count",
    "total_drug_exposures",
    "active_drug_exposures",
    "unique_drug_concepts",
    "total_procedures",
    "total_measurements",
    "visits_per_year",
    "avg_days_between_visits",
]

TARGET_COLUMN = "had_30_day_readmission"
RANDOM_STATE = 42


def rs_conn():
    return psycopg2.connect(**REDSHIFT)


def fetch_data():
    """Fetch features and target from Redshift."""
    cols = ", ".join(FEATURE_COLUMNS)
    sql = f"""
        SELECT patient_key, {cols}, {TARGET_COLUMN}
        FROM fact_patient_metrics
        WHERE age IS NOT NULL AND total_encounters > 0
    """
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    patient_keys = [r[0] for r in rows]
    features = np.array([
        [float(v) if v is not None else 0.0 for v in r[1:-1]]
        for r in rows
    ])
    targets = np.array([1 if r[-1] else 0 for r in rows])

    log.info("Fetched %d patients (%d positive, %d negative)",
             len(patient_keys), int(targets.sum()), int(len(targets) - targets.sum()))
    return patient_keys, features, targets


def risk_tier(score):
    if score >= 0.75:
        return "very_high"
    if score >= 0.50:
        return "high"
    if score >= 0.25:
        return "medium"
    return "low"


def write_results(patient_keys, scores):
    """Write risk scores and tiers back to Redshift."""
    conn = rs_conn()
    cur = conn.cursor()
    try:
        for pk, score in zip(patient_keys, scores):
            tier = risk_tier(score)
            cur.execute("""
                UPDATE fact_patient_metrics
                SET risk_score = %s, risk_tier = %s, updated_at = GETDATE()
                WHERE patient_key = %s
            """, (round(float(score), 4), tier, pk))
        conn.commit()
        log.info("Wrote risk scores for %d patients", len(patient_keys))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    log.info("ML Pipeline: Patient Risk Scoring (Random Forest)")

    patient_keys, features, targets = fetch_data()

    if len(patient_keys) < 20:
        log.warning("Too few patients (%d) for risk modeling. Skipping.", len(patient_keys))
        return

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    # Handle class imbalance with class_weight='balanced'
    clf = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )

    # Cross-validation
    if targets.sum() >= 5:
        cv_folds = min(5, int(targets.sum()))
        cv_scores = cross_val_score(clf, X_scaled, targets, cv=cv_folds, scoring="roc_auc")
        log.info("Cross-validation ROC-AUC: %.4f (+/- %.4f)", cv_scores.mean(), cv_scores.std())
    else:
        log.warning("Too few positive samples for cross-validation (only %d)", int(targets.sum()))

    # Train on full dataset for scoring
    clf.fit(X_scaled, targets)

    # Probability of positive class as risk score
    risk_scores = clf.predict_proba(X_scaled)[:, 1]

    # Feature importance
    log.info("── Feature Importance ──")
    importances = sorted(
        zip(FEATURE_COLUMNS, clf.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )
    for feat, imp in importances:
        log.info("  %-35s %.4f", feat, imp)

    # Classification report on training set
    predictions = clf.predict(X_scaled)
    log.info("Training set classification report:\n%s",
             classification_report(targets, predictions, target_names=["No Readmit", "Readmit"]))

    # Write results
    write_results(patient_keys, risk_scores)

    # Tier distribution
    log.info("── Risk Tier Distribution ──")
    tiers = [risk_tier(s) for s in risk_scores]
    for tier_name in ["low", "medium", "high", "very_high"]:
        count = tiers.count(tier_name)
        log.info("  %-12s %d patients (%.1f%%)", tier_name, count, 100 * count / len(tiers))

    log.info("Risk scoring complete.")


if __name__ == "__main__":
    main()
