"""
ML Pipeline: Patient Segmentation via K-Means Clustering

Reads patient metrics from Redshift, runs K-means with automatic K selection
(elbow method + silhouette scoring), and writes cluster assignments back.

Configuration via environment variables:
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""

import os
import logging

import numpy as np
import psycopg2
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

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

# Features used for clustering
FEATURE_COLUMNS = [
    "age",
    "total_encounters",
    "total_conditions",
    "total_drug_exposures",
    "total_procedures",
    "total_measurements",
    "visits_per_year",
    "chronic_condition_count",
]

# K range to evaluate
K_MIN = 2
K_MAX = 6
RANDOM_STATE = 42


def rs_conn():
    return psycopg2.connect(**REDSHIFT)


def fetch_features():
    """Fetch patient metrics for clustering."""
    cols = ", ".join(FEATURE_COLUMNS)
    sql = f"""
        SELECT person_id, {cols}
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

    person_ids = [r[0] for r in rows]
    features = np.array([[float(v) if v is not None else 0.0 for v in r[1:]] for r in rows])
    log.info("Fetched %d patients with %d features", len(person_ids), len(FEATURE_COLUMNS))
    return person_ids, features


def find_optimal_k(X_scaled):
    """Use elbow method + silhouette scoring to select K."""
    results = []
    for k in range(K_MIN, K_MAX + 1):
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X_scaled)
        inertia = km.inertia_
        sil = silhouette_score(X_scaled, labels)
        results.append({"k": k, "inertia": inertia, "silhouette": sil})
        log.info("  K=%d  inertia=%.1f  silhouette=%.4f", k, inertia, sil)

    # Pick K with highest silhouette score
    best = max(results, key=lambda r: r["silhouette"])
    log.info("Optimal K=%d (silhouette=%.4f)", best["k"], best["silhouette"])
    return best["k"], results


def assign_cluster_labels(km, scaler, feature_names):
    """Generate descriptive labels based on cluster centroids."""
    centroids = scaler.inverse_transform(km.cluster_centers_)
    labels = {}

    for i, centroid in enumerate(centroids):
        profile = dict(zip(feature_names, centroid))
        age = profile["age"]
        encounters = profile["total_encounters"]
        chronic = profile["chronic_condition_count"]
        procedures = profile["total_procedures"]

        if age < 35 and encounters < 10:
            label = "Young & Healthy"
        elif chronic >= 3 or (encounters > 30 and procedures > 20):
            label = "Complex / High-Utilization"
        elif age >= 65:
            label = "Senior Care"
        elif encounters > 15:
            label = "Moderate Utilization"
        else:
            label = "Low Utilization"

        labels[i] = label
        log.info("  Cluster %d: '%s' (age=%.0f, encounters=%.0f, chronic=%.0f, procedures=%.0f)",
                 i, label, age, encounters, chronic, procedures)

    return labels


def write_results(person_ids, cluster_ids, cluster_labels_map):
    """Write cluster assignments back to Redshift."""
    conn = rs_conn()
    cur = conn.cursor()
    try:
        for pid, cid in zip(person_ids, cluster_ids):
            label = cluster_labels_map.get(cid, f"Cluster {cid}")
            cur.execute("""
                UPDATE fact_patient_metrics
                SET cluster_id = %s, cluster_label = %s, updated_at = GETDATE()
                WHERE person_id = %s
            """, (int(cid), label, pid))
        conn.commit()
        log.info("Wrote cluster assignments for %d patients", len(person_ids))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    log.info("ML Pipeline: Patient Segmentation (K-Means)")

    person_ids, features = fetch_features()
    if len(person_ids) < K_MAX:
        log.warning("Too few patients (%d) for clustering. Skipping.", len(person_ids))
        return

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    # Find optimal K
    log.info("Evaluating K from %d to %d:", K_MIN, K_MAX)
    optimal_k, eval_results = find_optimal_k(X_scaled)

    # Final clustering
    km = KMeans(n_clusters=optimal_k, random_state=RANDOM_STATE, n_init=10)
    cluster_ids = km.fit_predict(X_scaled)

    # Label clusters
    log.info("Cluster profiles:")
    labels_map = assign_cluster_labels(km, scaler, FEATURE_COLUMNS)

    # Write back
    write_results(person_ids, cluster_ids, labels_map)

    # Summary
    log.info("── Cluster Summary ──")
    for cid in range(optimal_k):
        count = int(np.sum(cluster_ids == cid))
        label = labels_map.get(cid, f"Cluster {cid}")
        log.info("  %s: %d patients (%.1f%%)", label, count, 100 * count / len(person_ids))

    log.info("Clustering complete.")


if __name__ == "__main__":
    main()
