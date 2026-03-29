"""
ML Pipeline: Disease Co-occurrence Analysis

Analyzes which conditions appear together in patients, computes co-occurrence
frequency and lift scores, and writes top comorbidity pairs to Redshift.

This is an association analysis approach — not clustering or classification,
demonstrating a third ML paradigm relevant to healthcare.

Configuration via environment variables:
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""

import os
import logging
from collections import defaultdict
from itertools import combinations

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

MIN_SUPPORT = 3  # minimum shared patients for a pair to be reported


def rs_conn():
    return psycopg2.connect(**REDSHIFT)


def fetch_patient_conditions():
    """Fetch each patient's set of condition codes."""
    sql = """
        SELECT patient_id, condition_code
        FROM fact_conditions
        WHERE condition_code IS NOT NULL
    """
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    patient_conditions = defaultdict(set)
    for pid, code in rows:
        patient_conditions[pid].add(code)

    log.info("Fetched conditions for %d patients", len(patient_conditions))
    return patient_conditions


def fetch_condition_descriptions():
    """Fetch condition code → description mapping."""
    sql = "SELECT code, description FROM dim_condition"
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return {code: desc for code, desc in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def compute_comorbidities(patient_conditions):
    """Compute co-occurrence counts, support, and lift for condition pairs."""
    total_patients = len(patient_conditions)

    # Count individual condition frequencies
    condition_freq = defaultdict(int)
    for conditions in patient_conditions.values():
        for code in conditions:
            condition_freq[code] += 1

    # Count pair co-occurrences
    pair_counts = defaultdict(int)
    for conditions in patient_conditions.values():
        sorted_codes = sorted(conditions)
        for pair in combinations(sorted_codes, 2):
            pair_counts[pair] += 1

    # Compute metrics for each pair
    results = []
    for (c1, c2), shared in pair_counts.items():
        if shared < MIN_SUPPORT:
            continue

        support = shared / total_patients
        freq_c1 = condition_freq[c1] / total_patients
        freq_c2 = condition_freq[c2] / total_patients

        # Lift: how much more likely they co-occur vs independence
        expected = freq_c1 * freq_c2
        lift = support / expected if expected > 0 else 0

        results.append({
            "condition_1": c1,
            "condition_2": c2,
            "shared_patients": shared,
            "support": round(support, 6),
            "lift": round(lift, 4),
            "freq_c1": condition_freq[c1],
            "freq_c2": condition_freq[c2],
        })

    results.sort(key=lambda r: r["shared_patients"], reverse=True)
    log.info("Found %d comorbidity pairs with >= %d shared patients", len(results), MIN_SUPPORT)
    return results


def ensure_table_exists():
    """Create the comorbidity results table if it doesn't exist."""
    sql = """
    CREATE TABLE IF NOT EXISTS comorbidity_analysis (
        condition_1       VARCHAR(50),
        condition_2       VARCHAR(50),
        description_1     VARCHAR(500),
        description_2     VARCHAR(500),
        shared_patients   INTEGER,
        support           DECIMAL(10,6),
        lift              DECIMAL(10,4),
        freq_condition_1  INTEGER,
        freq_condition_2  INTEGER,
        loaded_at         TIMESTAMP DEFAULT GETDATE(),
        PRIMARY KEY (condition_1, condition_2)
    )
    DISTSTYLE ALL
    SORTKEY (shared_patients);
    """
    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def write_results(results, descriptions):
    """Write comorbidity pairs to Redshift."""
    ensure_table_exists()

    conn = rs_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM comorbidity_analysis")

        for r in results[:200]:  # top 200 pairs
            cur.execute("""
                INSERT INTO comorbidity_analysis (
                    condition_1, condition_2, description_1, description_2,
                    shared_patients, support, lift, freq_condition_1, freq_condition_2
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                r["condition_1"], r["condition_2"],
                descriptions.get(r["condition_1"], "Unknown"),
                descriptions.get(r["condition_2"], "Unknown"),
                r["shared_patients"], r["support"], r["lift"],
                r["freq_c1"], r["freq_c2"],
            ))

        conn.commit()
        log.info("Wrote %d comorbidity pairs to Redshift", min(len(results), 200))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    log.info("ML Pipeline: Disease Co-occurrence Analysis")

    patient_conditions = fetch_patient_conditions()
    descriptions = fetch_condition_descriptions()

    if len(patient_conditions) < 10:
        log.warning("Too few patients (%d) for comorbidity analysis. Skipping.",
                     len(patient_conditions))
        return

    results = compute_comorbidities(patient_conditions)

    if not results:
        log.warning("No comorbidity pairs found above minimum support (%d). Skipping.", MIN_SUPPORT)
        return

    # Log top 10
    log.info("── Top 10 Comorbidity Pairs ──")
    for r in results[:10]:
        d1 = descriptions.get(r["condition_1"], r["condition_1"])[:40]
        d2 = descriptions.get(r["condition_2"], r["condition_2"])[:40]
        log.info("  %s + %s  (n=%d, lift=%.2f)",
                 d1, d2, r["shared_patients"], r["lift"])

    write_results(results, descriptions)
    log.info("Comorbidity analysis complete.")


if __name__ == "__main__":
    main()
