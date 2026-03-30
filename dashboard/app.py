"""
Healthcare Data Platform — Analytics Dashboard

Streamlit dashboard that visualizes Kimball star-schema analytics data
from Amazon Redshift. All queries target the Redshift data warehouse;
no RDS connections are used.

Configuration via environment variables:
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
    S3_BUCKET, S3_PREFIX (optional, for Streaming Monitor page)
"""

import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import redshift_connector

# ── Page Config ──────────────────────────────────────────

st.set_page_config(
    page_title="Healthcare Data Platform",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database Connection ──────────────────────────────────


@st.cache_resource
def get_redshift_connection():
    """Return a Redshift connection, or None on failure."""
    try:
        return redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "healthcare_dw"),
            user=os.environ["REDSHIFT_USER"],
            password=os.environ["REDSHIFT_PASSWORD"],
        )
    except Exception as exc:
        st.error(f"Redshift connection failed: {exc}")
        return None


def query_redshift(sql, params=None):
    """Execute *sql* against Redshift and return a DataFrame."""
    conn = get_redshift_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception as exc:
        st.error(f"Query error: {exc}")
        # Reset connection on failure so next attempt reconnects
        st.cache_resource.clear()
        return pd.DataFrame()


def has_redshift():
    return get_redshift_connection() is not None


# ── Sidebar Navigation ──────────────────────────────────

st.sidebar.title("Healthcare Data Platform")
st.sidebar.caption("Redshift Star-Schema Analytics Dashboard")

page = st.sidebar.radio(
    "Navigate",
    [
        "Overview",
        "Demographics",
        "Visits & Encounters",
        "Conditions",
        "Medications",
        "Patient Segments",
        "Risk Analysis",
        "Data Quality",
        "Streaming Monitor",
    ],
)

# ── Helper ───────────────────────────────────────────────


def metric_row(metrics):
    """Display a row of st.metric cards."""
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics):
        col.metric(label, f"{value:,}" if isinstance(value, (int, float)) else value)


# ════════════════════════════════════════════════════════
#  PAGE: Overview
# ════════════════════════════════════════════════════════

if page == "Overview":
    st.title("Platform Overview")

    if not has_redshift():
        st.warning("Redshift connection not available. Configure REDSHIFT_* environment variables.")
        st.stop()

    # Record counts from fact / dimension tables
    counts_df = query_redshift("""
        SELECT
            (SELECT COUNT(*) FROM dim_patient)       AS patients,
            (SELECT COUNT(*) FROM fact_encounters)    AS encounters,
            (SELECT COUNT(*) FROM fact_conditions)    AS conditions,
            (SELECT COUNT(*) FROM fact_medications)   AS medications,
            (SELECT COUNT(*) FROM fact_procedures)    AS procedures,
            (SELECT COUNT(*) FROM dim_condition)      AS unique_conditions,
            (SELECT COUNT(*) FROM dim_medication)     AS unique_medications
    """)

    if not counts_df.empty:
        r = counts_df.iloc[0]
        metric_row([
            ("Patients", int(r["patients"])),
            ("Encounters", int(r["encounters"])),
            ("Conditions", int(r["conditions"])),
            ("Medications", int(r["medications"])),
        ])
        metric_row([
            ("Procedures", int(r["procedures"])),
            ("Unique Condition Concepts", int(r["unique_conditions"])),
            ("Unique Medication Concepts", int(r["unique_medications"])),
        ])

    st.divider()

    # Bar chart of fact table sizes
    chart_data = {
        "Table": ["fact_encounters", "fact_conditions", "fact_medications", "fact_procedures"],
        "Records": [
            int(counts_df.iloc[0]["encounters"]),
            int(counts_df.iloc[0]["conditions"]),
            int(counts_df.iloc[0]["medications"]),
            int(counts_df.iloc[0]["procedures"]),
        ],
    } if not counts_df.empty else {"Table": [], "Records": []}
    chart_df = pd.DataFrame(chart_data)
    fig = px.bar(
        chart_df, x="Table", y="Records",
        title="Record Counts by Fact Table",
        color="Records", color_continuous_scale="Blues",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Demographics snapshot from dim_patient
    st.subheader("Patient Demographics Snapshot")
    demo = query_redshift("""
        SELECT
            MIN(year_of_birth)                     AS oldest_birth_year,
            MAX(year_of_birth)                     AS youngest_birth_year,
            COUNT(DISTINCT gender)                 AS distinct_genders,
            COUNT(DISTINCT race)                   AS distinct_races,
            ROUND(AVG(age), 1)                     AS avg_age
        FROM dim_patient
    """)
    if not demo.empty:
        r = demo.iloc[0]
        metric_row([
            ("Oldest Birth Year", int(r["oldest_birth_year"])),
            ("Youngest Birth Year", int(r["youngest_birth_year"])),
            ("Distinct Genders", int(r["distinct_genders"])),
            ("Distinct Races", int(r["distinct_races"])),
            ("Average Age", float(r["avg_age"])),
        ])


# ════════════════════════════════════════════════════════
#  PAGE: Demographics
# ════════════════════════════════════════════════════════

elif page == "Demographics":
    st.title("Patient Demographics")

    if not has_redshift():
        st.warning("Redshift connection not available.")
        st.stop()

    col1, col2 = st.columns(2)

    # Gender
    gender_df = query_redshift("""
        SELECT gender, COUNT(*) AS count
        FROM dim_patient
        GROUP BY gender ORDER BY count DESC
    """)
    with col1:
        fig = px.pie(gender_df, names="gender", values="count", title="Gender Distribution")
        st.plotly_chart(fig, use_container_width=True)

    # Race
    race_df = query_redshift("""
        SELECT race, COUNT(*) AS count
        FROM dim_patient
        GROUP BY race ORDER BY count DESC
    """)
    with col2:
        fig = px.pie(race_df, names="race", values="count", title="Race Distribution")
        st.plotly_chart(fig, use_container_width=True)

    # Age group distribution
    age_df = query_redshift("""
        SELECT age_group, COUNT(*) AS count
        FROM dim_patient
        WHERE age_group IS NOT NULL
        GROUP BY age_group
        ORDER BY
            CASE age_group
                WHEN '0-17'  THEN 1
                WHEN '18-34' THEN 2
                WHEN '35-49' THEN 3
                WHEN '50-64' THEN 4
                WHEN '65-79' THEN 5
                WHEN '80+'   THEN 6
                ELSE 7
            END
    """)
    fig = px.bar(
        age_df, x="age_group", y="count",
        title="Age Group Distribution",
        color="count", color_continuous_scale="Teal",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Ethnicity
    eth_df = query_redshift("""
        SELECT ethnicity, COUNT(*) AS count
        FROM dim_patient
        GROUP BY ethnicity ORDER BY count DESC
    """)
    fig = px.bar(eth_df, x="ethnicity", y="count", title="Ethnicity Distribution")
    st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Visits & Encounters
# ════════════════════════════════════════════════════════

elif page == "Visits & Encounters":
    st.title("Visits & Encounters")

    if not has_redshift():
        st.warning("Redshift connection not available.")
        st.stop()

    # Visit type breakdown
    visit_df = query_redshift("""
        SELECT
            visit_class                        AS visit_type,
            COUNT(*)                           AS count,
            COUNT(DISTINCT patient_key)         AS unique_patients
        FROM fact_encounters
        GROUP BY visit_class
        ORDER BY count DESC
    """)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.pie(visit_df, names="visit_type", values="count", title="Visit Type Distribution")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(
            visit_df, x="visit_type", y="unique_patients",
            title="Unique Patients by Visit Type", color="unique_patients",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Visit volume over time (using the view)
    st.subheader("Visit Volume Over Time")
    trend_df = query_redshift("""
        SELECT year, month, month_name, visit_class,
               encounter_count, unique_patients
        FROM vw_encounter_trends
        ORDER BY year, month
    """)
    if not trend_df.empty:
        trend_df["period"] = trend_df["year"].astype(str) + "-" + trend_df["month"].astype(str).str.zfill(2)
        # Aggregate across visit classes for the line chart
        monthly = trend_df.groupby("period", as_index=False).agg(
            encounters=("encounter_count", "sum"),
            unique_patients=("unique_patients", "sum"),
        ).sort_values("period")
        fig = px.line(monthly, x="period", y="encounters", title="Monthly Encounter Volume")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        # Breakdown by visit class
        fig = px.bar(
            trend_df, x="period", y="encounter_count", color="visit_class",
            title="Monthly Encounters by Visit Class",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # Average visit duration
    dur_df = query_redshift("""
        SELECT visit_class                     AS visit_type,
               ROUND(AVG(duration_days), 1)    AS avg_days
        FROM fact_encounters
        WHERE duration_days IS NOT NULL
        GROUP BY visit_class
        ORDER BY avg_days DESC
    """)
    if not dur_df.empty:
        fig = px.bar(dur_df, x="visit_type", y="avg_days", title="Average Visit Duration (Days)")
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Conditions
# ════════════════════════════════════════════════════════

elif page == "Conditions":
    st.title("Condition Analysis")

    if not has_redshift():
        st.warning("Redshift connection not available.")
        st.stop()

    top_n = st.slider("Top N conditions", 10, 50, 20)

    # Use the analytical view
    cond_df = query_redshift("""
        SELECT concept_name  AS condition,
               occurrence_count AS occurrences,
               unique_patients,
               body_system,
               chronicity,
               pct_active
        FROM vw_top_conditions
        ORDER BY occurrences DESC
        LIMIT %s
    """, (top_n,))

    fig = px.bar(
        cond_df, x="occurrences", y="condition", orientation="h",
        title=f"Top {top_n} Conditions by Occurrence",
        color="unique_patients", color_continuous_scale="Reds",
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=max(400, top_n * 25))
    st.plotly_chart(fig, use_container_width=True)

    # Body system breakdown
    st.subheader("Conditions by Body System")
    body_df = query_redshift("""
        SELECT body_system, COUNT(*) AS condition_concepts,
               SUM(occurrence_count) AS total_occurrences
        FROM vw_top_conditions
        WHERE body_system IS NOT NULL
        GROUP BY body_system
        ORDER BY total_occurrences DESC
    """)
    if not body_df.empty:
        fig = px.bar(body_df, x="body_system", y="total_occurrences",
                     title="Total Occurrences by Body System", color="condition_concepts")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # Chronicity breakdown
    st.subheader("Chronic vs Acute Conditions")
    chron_df = query_redshift("""
        SELECT chronicity, SUM(occurrence_count) AS total_occurrences,
               SUM(unique_patients) AS total_patients
        FROM vw_top_conditions
        WHERE chronicity IS NOT NULL
        GROUP BY chronicity
    """)
    if not chron_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(chron_df, names="chronicity", values="total_occurrences",
                         title="Occurrences: Chronic vs Acute")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.pie(chron_df, names="chronicity", values="total_patients",
                         title="Patients: Chronic vs Acute")
            st.plotly_chart(fig, use_container_width=True)

    # Conditions over time
    st.subheader("Condition Occurrences Over Time")
    cond_trend = query_redshift("""
        SELECT d.year, d.month, d.month_name,
               COUNT(*) AS new_conditions
        FROM fact_conditions fc
        JOIN dim_date d ON fc.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """)
    if not cond_trend.empty:
        cond_trend["period"] = cond_trend["year"].astype(str) + "-" + cond_trend["month"].astype(str).str.zfill(2)
        fig = px.area(cond_trend, x="period", y="new_conditions",
                      title="Monthly Condition Occurrences")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Medications
# ════════════════════════════════════════════════════════

elif page == "Medications":
    st.title("Medication Analysis")

    if not has_redshift():
        st.warning("Redshift connection not available.")
        st.stop()

    top_n = st.slider("Top N medications", 10, 50, 20)

    drug_df = query_redshift("""
        SELECT dm.concept_name                 AS medication,
               dm.therapeutic_class,
               COUNT(*)                        AS prescriptions,
               COUNT(DISTINCT fm.patient_key)   AS unique_patients
        FROM fact_medications fm
        JOIN dim_medication dm ON fm.drug_concept_id = dm.drug_concept_id
        GROUP BY dm.concept_name, dm.therapeutic_class
        ORDER BY prescriptions DESC
        LIMIT %s
    """, (top_n,))

    fig = px.bar(
        drug_df, x="prescriptions", y="medication", orientation="h",
        title=f"Top {top_n} Medications by Prescription Count",
        color="unique_patients", color_continuous_scale="Purples",
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=max(400, top_n * 25))
    st.plotly_chart(fig, use_container_width=True)

    # Therapeutic class breakdown
    st.subheader("Prescriptions by Therapeutic Class")
    tc_df = query_redshift("""
        SELECT dm.therapeutic_class,
               COUNT(*)                        AS prescriptions,
               COUNT(DISTINCT fm.patient_key)   AS unique_patients
        FROM fact_medications fm
        JOIN dim_medication dm ON fm.drug_concept_id = dm.drug_concept_id
        WHERE dm.therapeutic_class IS NOT NULL
        GROUP BY dm.therapeutic_class
        ORDER BY prescriptions DESC
        LIMIT 15
    """)
    if not tc_df.empty:
        fig = px.bar(tc_df, x="therapeutic_class", y="prescriptions",
                     title="Top 15 Therapeutic Classes", color="unique_patients")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # Average days supply
    st.subheader("Average Days Supply by Medication")
    dur_df = query_redshift("""
        SELECT dm.concept_name                   AS medication,
               ROUND(AVG(fm.days_supply), 1)     AS avg_days_supply,
               COUNT(*)                          AS total
        FROM fact_medications fm
        JOIN dim_medication dm ON fm.drug_concept_id = dm.drug_concept_id
        WHERE fm.days_supply IS NOT NULL AND fm.days_supply > 0
        GROUP BY dm.concept_name
        HAVING COUNT(*) >= 10
        ORDER BY avg_days_supply DESC
        LIMIT 20
    """)
    if not dur_df.empty:
        fig = px.bar(dur_df, x="medication", y="avg_days_supply", title="Avg Days Supply (Top 20)")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # Polypharmacy patients
    st.subheader("Polypharmacy Overview")
    poly_df = query_redshift("""
        SELECT risk_tier,
               COUNT(*)                             AS patients,
               ROUND(AVG(active_drug_exposures), 1) AS avg_active_drugs,
               ROUND(AVG(active_conditions), 1)     AS avg_active_conditions
        FROM vw_polypharmacy
        GROUP BY risk_tier
        ORDER BY avg_active_drugs DESC
    """)
    if not poly_df.empty:
        fig = px.bar(poly_df, x="risk_tier", y="patients",
                     title="Polypharmacy Patients by Risk Tier", color="avg_active_drugs")
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Patient Segments (Redshift)
# ════════════════════════════════════════════════════════

elif page == "Patient Segments":
    st.title("Patient Segments (ML Clustering)")

    if not has_redshift():
        st.warning("Redshift connection not available. Configure REDSHIFT_* environment variables.")
        st.stop()

    seg_df = query_redshift("SELECT * FROM vw_patient_segments")
    if seg_df.empty:
        st.info("No clustering results yet. Run the ML clustering pipeline first.")
    else:
        metric_row([
            ("Total Segments", len(seg_df)),
            ("Total Patients", int(seg_df["patient_count"].sum())),
        ])

        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                seg_df, names="cluster_label", values="patient_count",
                title="Patient Distribution by Segment",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                seg_df, x="cluster_label", y="avg_risk_score",
                title="Average Risk Score by Segment",
                color="avg_risk_score", color_continuous_scale="RdYlGn_r",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Segment profile radar
        st.subheader("Segment Profiles")
        categories = ["avg_age", "avg_encounters", "avg_conditions", "avg_drug_exposures"]
        fig = go.Figure()
        for _, row in seg_df.iterrows():
            fig.add_trace(go.Scatterpolar(
                r=[row[c] for c in categories],
                theta=[c.replace("avg_", "").replace("_", " ").title() for c in categories],
                fill="toself",
                name=row["cluster_label"],
            ))
        fig.update_layout(title="Segment Profile Comparison", polar=dict(radialaxis=dict(visible=True)))
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(seg_df, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Risk Analysis (Redshift)
# ════════════════════════════════════════════════════════

elif page == "Risk Analysis":
    st.title("Patient Risk Analysis")

    if not has_redshift():
        st.warning("Redshift connection not available. Configure REDSHIFT_* environment variables.")
        st.stop()

    risk_df = query_redshift("SELECT * FROM vw_risk_distribution")
    if risk_df.empty:
        st.info("No risk scoring results yet. Run the ML risk scoring pipeline first.")
    else:
        metric_row([
            ("Risk Tiers", len(risk_df)),
            ("Total Assessed", int(risk_df["patient_count"].sum())),
        ])

        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                risk_df, names="risk_tier", values="patient_count",
                title="Patients by Risk Tier",
                color="risk_tier",
                color_discrete_map={
                    "Low": "#2ecc71", "Medium": "#f39c12",
                    "High": "#e74c3c", "Critical": "#8e44ad",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                risk_df, x="risk_tier", y="avg_chronic_conditions",
                title="Avg Chronic Conditions by Risk Tier",
                color="risk_tier",
                color_discrete_map={
                    "Low": "#2ecc71", "Medium": "#f39c12",
                    "High": "#e74c3c", "Critical": "#8e44ad",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

        # Risk tier details table
        st.dataframe(risk_df, use_container_width=True)

    # Comorbidity network
    st.subheader("Top Condition Comorbidities")
    comorb_df = query_redshift("SELECT * FROM vw_condition_comorbidity")
    if not comorb_df.empty:
        fig = px.scatter(
            comorb_df, x="co_occurrence_count", y="lift",
            size="support", hover_name="concept_name_1",
            hover_data=["concept_name_2"],
            title="Comorbidity Pairs (size = support, y = lift)",
            color="lift", color_continuous_scale="Viridis",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(comorb_df, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Data Quality
# ════════════════════════════════════════════════════════

elif page == "Data Quality":
    st.title("Data Quality Checks")

    if not has_redshift():
        st.warning("Redshift connection not available.")
        st.stop()

    # ── Null checks on key columns ──
    st.subheader("Null Value Checks")

    null_checks_sql = """
        SELECT 'dim_patient: NULL gender'          AS check_name,
               COUNT(*) AS issue_count
        FROM dim_patient WHERE gender IS NULL
      UNION ALL
        SELECT 'dim_patient: NULL race',
               COUNT(*)
        FROM dim_patient WHERE race IS NULL
      UNION ALL
        SELECT 'dim_patient: NULL year_of_birth',
               COUNT(*)
        FROM dim_patient WHERE year_of_birth IS NULL
      UNION ALL
        SELECT 'dim_patient: NULL age_group',
               COUNT(*)
        FROM dim_patient WHERE age_group IS NULL
      UNION ALL
        SELECT 'dim_condition: NULL concept_name',
               COUNT(*)
        FROM dim_condition WHERE concept_name IS NULL
      UNION ALL
        SELECT 'dim_medication: NULL concept_name',
               COUNT(*)
        FROM dim_medication WHERE concept_name IS NULL
      UNION ALL
        SELECT 'fact_encounters: NULL visit_class',
               COUNT(*)
        FROM fact_encounters WHERE visit_class IS NULL
      UNION ALL
        SELECT 'fact_encounters: NULL date_key',
               COUNT(*)
        FROM fact_encounters WHERE date_key IS NULL
      UNION ALL
        SELECT 'fact_conditions: NULL date_key',
               COUNT(*)
        FROM fact_conditions WHERE date_key IS NULL
      UNION ALL
        SELECT 'fact_medications: NULL date_key',
               COUNT(*)
        FROM fact_medications WHERE date_key IS NULL
      UNION ALL
        SELECT 'fact_procedures: NULL date_key',
               COUNT(*)
        FROM fact_procedures WHERE date_key IS NULL
        ORDER BY issue_count DESC
    """
    dq_df = query_redshift(null_checks_sql)
    if not dq_df.empty:
        all_pass = (dq_df["issue_count"] == 0).all()
        if all_pass:
            st.success("All null-value checks passed — no missing key columns.")
        else:
            st.warning("Some checks found NULL values in key columns.")

        fig = px.bar(
            dq_df, x="check_name", y="issue_count",
            title="Null Value Issues by Check",
            color="issue_count", color_continuous_scale="RdYlGn_r",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(dq_df, use_container_width=True)

    # ── Dimension coverage (orphan fact keys) ──
    st.subheader("Dimension Coverage (Orphan Key Checks)")
    orphan_sql = """
        SELECT 'fact_encounters: orphan patient_key' AS check_name,
               COUNT(*) AS issue_count
        FROM fact_encounters fe
        WHERE NOT EXISTS (SELECT 1 FROM dim_patient dp WHERE dp.patient_key = fe.patient_key)
      UNION ALL
        SELECT 'fact_conditions: orphan patient_key',
               COUNT(*)
        FROM fact_conditions fc
        WHERE NOT EXISTS (SELECT 1 FROM dim_patient dp WHERE dp.patient_key = fc.patient_key)
      UNION ALL
        SELECT 'fact_medications: orphan patient_key',
               COUNT(*)
        FROM fact_medications fm
        WHERE NOT EXISTS (SELECT 1 FROM dim_patient dp WHERE dp.patient_key = fm.patient_key)
      UNION ALL
        SELECT 'fact_procedures: orphan patient_key',
               COUNT(*)
        FROM fact_procedures fp
        WHERE NOT EXISTS (SELECT 1 FROM dim_patient dp WHERE dp.patient_key = fp.patient_key)
      UNION ALL
        SELECT 'fact_encounters: orphan date_key',
               COUNT(*)
        FROM fact_encounters fe
        WHERE fe.date_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM dim_date dd WHERE dd.date_key = fe.date_key)
        ORDER BY issue_count DESC
    """
    orphan_df = query_redshift(orphan_sql)
    if not orphan_df.empty:
        all_pass = (orphan_df["issue_count"] == 0).all()
        if all_pass:
            st.success("All dimension coverage checks passed — no orphan keys.")
        else:
            st.warning("Some fact rows reference missing dimension keys.")

        fig = px.bar(
            orphan_df, x="check_name", y="issue_count",
            title="Orphan Key Issues",
            color="issue_count", color_continuous_scale="RdYlGn_r",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(orphan_df, use_container_width=True)

    # ── Table row counts ──
    st.subheader("Table Row Counts")
    rowcount_sql = """
        SELECT 'dim_patient'       AS table_name, COUNT(*) AS row_count FROM dim_patient
      UNION ALL
        SELECT 'dim_condition',    COUNT(*) FROM dim_condition
      UNION ALL
        SELECT 'dim_medication',   COUNT(*) FROM dim_medication
      UNION ALL
        SELECT 'dim_procedure',    COUNT(*) FROM dim_procedure
      UNION ALL
        SELECT 'dim_date',         COUNT(*) FROM dim_date
      UNION ALL
        SELECT 'fact_encounters',  COUNT(*) FROM fact_encounters
      UNION ALL
        SELECT 'fact_conditions',  COUNT(*) FROM fact_conditions
      UNION ALL
        SELECT 'fact_medications', COUNT(*) FROM fact_medications
      UNION ALL
        SELECT 'fact_procedures',  COUNT(*) FROM fact_procedures
      UNION ALL
        SELECT 'fact_patient_metrics', COUNT(*) FROM fact_patient_metrics
      UNION ALL
        SELECT 'comorbidity_analysis', COUNT(*) FROM comorbidity_analysis
        ORDER BY table_name
    """
    rc_df = query_redshift(rowcount_sql)
    if not rc_df.empty:
        fig = px.bar(rc_df, x="table_name", y="row_count",
                     title="Row Counts Across Star Schema", color="row_count",
                     color_continuous_scale="Blues")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(rc_df, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Streaming Monitor
# ════════════════════════════════════════════════════════

elif page == "Streaming Monitor":
    st.title("Real-Time Streaming Monitor")

    st.info(
        "This page monitors streaming ingestion from Kinesis. "
        "Data shown reflects the S3 simulation state and Redshift fact table counts."
    )

    # ── S3 simulation state ──
    st.subheader("Batch / Streaming Split")
    try:
        import boto3
        s3 = boto3.client("s3")
        bucket = os.environ.get("S3_BUCKET", "")
        prefix = os.environ.get("S3_PREFIX", "omop/")

        if bucket:
            try:
                state_obj = s3.get_object(Bucket=bucket, Key=f"{prefix}simulation_state.json")
                state = json.loads(state_obj["Body"].read().decode("utf-8"))
                col1, col2, col3 = st.columns(3)
                col1.metric("Batch Cutoff Date", state.get("batch_cutoff_date", "N/A"))
                col2.metric("Batch Records Loaded", f"{state.get('total_batch_records', 0):,}")
                col3.metric("Streaming Records Available", f"{state.get('total_streaming_records', 0):,}")
            except Exception:
                st.caption("No simulation state found. Run the batch ETL first.")

            try:
                prog_obj = s3.get_object(Bucket=bucket, Key=f"{prefix}streaming_progress.json")
                progress = json.loads(prog_obj["Body"].read().decode("utf-8"))
                col1, col2, col3 = st.columns(3)
                col1.metric("Last Streamed Date", progress.get("last_streamed_date", "N/A"))
                col2.metric("Total Events Sent", f"{progress.get('total_events_sent', 0):,}")
                col3.metric("Last Updated", progress.get("updated_at", "N/A")[:19])
            except Exception:
                st.caption("No streaming progress found. Stream simulator has not run yet.")
        else:
            st.caption("S3_BUCKET not configured.")
    except ImportError:
        st.caption("boto3 not available — S3 state cannot be loaded.")

    # ── Fact table record counts from Redshift ──
    st.subheader("Fact Table Record Counts")
    if has_redshift():
        fact_counts = query_redshift("""
            SELECT 'fact_encounters'  AS fact_table, COUNT(*) AS records FROM fact_encounters
          UNION ALL
            SELECT 'fact_conditions',  COUNT(*) FROM fact_conditions
          UNION ALL
            SELECT 'fact_medications', COUNT(*) FROM fact_medications
          UNION ALL
            SELECT 'fact_procedures',  COUNT(*) FROM fact_procedures
            ORDER BY fact_table
        """)
        if not fact_counts.empty:
            fig = px.bar(fact_counts, x="fact_table", y="records",
                         title="Current Fact Table Sizes in Redshift",
                         color="records", color_continuous_scale="Blues")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(fact_counts, use_container_width=True)

        # Latest records by date
        st.subheader("Most Recent Records by Fact Table")
        for table, date_col, label in [
            ("fact_encounters",  "visit_start_date",          "Encounters"),
            ("fact_conditions",  "condition_start_date",      "Conditions"),
            ("fact_medications", "drug_exposure_start_date",  "Medications"),
            ("fact_procedures",  "procedure_date",            "Procedures"),
        ]:
            recent_df = query_redshift(f"""
                SELECT {date_col}::DATE AS event_date, COUNT(*) AS count
                FROM {table}
                WHERE {date_col} IS NOT NULL
                GROUP BY event_date
                ORDER BY event_date DESC
                LIMIT 30
            """)
            if not recent_df.empty:
                recent_df = recent_df.sort_values("event_date")
                with st.expander(f"{label} — last 30 date buckets"):
                    fig = px.bar(recent_df, x="event_date", y="count", title=label)
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Redshift connection not available.")

    if st.button("Refresh"):
        st.cache_resource.clear()
        st.rerun()


# ── Footer ───────────────────────────────────────────────

st.sidebar.divider()
st.sidebar.caption("Healthcare Data Platform v2.0 — Redshift Star Schema")
if has_redshift():
    st.sidebar.success("Redshift: Connected")
else:
    st.sidebar.warning("Redshift: Not connected")
