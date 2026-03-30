"""
Healthcare Data Platform — Analytics Dashboard

Streamlit dashboard that visualizes OMOP CDM operational data (RDS)
and Kimball star-schema analytics data (Redshift).

Configuration via environment variables:
    RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2

# ── Page Config ──────────────────────────────────────────

st.set_page_config(
    page_title="Healthcare Data Platform",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database Connections ─────────────────────────────────

@st.cache_resource
def get_rds_connection():
    return psycopg2.connect(
        host=os.environ["RDS_HOST"],
        port=int(os.environ.get("RDS_PORT", "5432")),
        database=os.environ.get("RDS_DATABASE", "healthcare"),
        user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"],
    )


@st.cache_resource
def get_redshift_connection():
    try:
        import redshift_connector
        return redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "healthcare_dw"),
            user=os.environ["REDSHIFT_USER"],
            password=os.environ["REDSHIFT_PASSWORD"],
        )
    except Exception:
        return None


def query_rds(sql, params=None):
    conn = get_rds_connection()
    return pd.read_sql(sql, conn, params=params)


def query_redshift(sql, params=None):
    conn = get_redshift_connection()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql(sql, conn, params=params)


def has_redshift():
    return get_redshift_connection() is not None


# ── Sidebar Navigation ──────────────────────────────────

st.sidebar.title("Healthcare Data Platform")
st.sidebar.caption("OMOP CDM Analytics Dashboard")

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

    counts = {}
    for table in [
        "person", "visit_occurrence", "condition_occurrence",
        "drug_exposure", "procedure_occurrence", "measurement", "observation",
    ]:
        df = query_rds(f"SELECT COUNT(*) AS cnt FROM {table}")
        counts[table] = int(df["cnt"].iloc[0])

    metric_row([
        ("Persons", counts["person"]),
        ("Visits", counts["visit_occurrence"]),
        ("Conditions", counts["condition_occurrence"]),
        ("Drug Exposures", counts["drug_exposure"]),
    ])
    metric_row([
        ("Procedures", counts["procedure_occurrence"]),
        ("Measurements", counts["measurement"]),
        ("Observations", counts["observation"]),
    ])

    st.divider()

    # Table breakdown bar chart
    chart_df = pd.DataFrame(
        {"Table": list(counts.keys()), "Records": list(counts.values())}
    )
    fig = px.bar(
        chart_df, x="Table", y="Records",
        title="Record Counts by OMOP Table",
        color="Records", color_continuous_scale="Blues",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Person demographics snapshot
    st.subheader("Person Demographics Snapshot")
    demo = query_rds("""
        SELECT
            MIN(year_of_birth) AS oldest_birth_year,
            MAX(year_of_birth) AS youngest_birth_year,
            COUNT(DISTINCT gender_source_value) AS distinct_genders,
            COUNT(DISTINCT race_source_value) AS distinct_races
        FROM person
    """)
    if not demo.empty:
        r = demo.iloc[0]
        metric_row([
            ("Oldest Birth Year", int(r["oldest_birth_year"])),
            ("Youngest Birth Year", int(r["youngest_birth_year"])),
            ("Distinct Genders", int(r["distinct_genders"])),
            ("Distinct Races", int(r["distinct_races"])),
        ])


# ════════════════════════════════════════════════════════
#  PAGE: Demographics
# ════════════════════════════════════════════════════════

elif page == "Demographics":
    st.title("Patient Demographics")

    col1, col2 = st.columns(2)

    # Gender
    gender_df = query_rds("""
        SELECT gender_source_value AS gender, COUNT(*) AS count
        FROM person GROUP BY gender_source_value ORDER BY count DESC
    """)
    with col1:
        fig = px.pie(gender_df, names="gender", values="count", title="Gender Distribution")
        st.plotly_chart(fig, use_container_width=True)

    # Race
    race_df = query_rds("""
        SELECT race_source_value AS race, COUNT(*) AS count
        FROM person GROUP BY race_source_value ORDER BY count DESC
    """)
    with col2:
        fig = px.pie(race_df, names="race", values="count", title="Race Distribution")
        st.plotly_chart(fig, use_container_width=True)

    # Age distribution
    age_df = query_rds("""
        SELECT
            CASE
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 17 THEN '0-17'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 34 THEN '18-34'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 49 THEN '35-49'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 64 THEN '50-64'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 79 THEN '65-79'
                ELSE '80+'
            END AS age_group,
            COUNT(*) AS count
        FROM person
        WHERE birth_datetime IS NOT NULL
        GROUP BY age_group
        ORDER BY age_group
    """)
    fig = px.bar(
        age_df, x="age_group", y="count",
        title="Age Group Distribution",
        color="count", color_continuous_scale="Teal",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Ethnicity
    eth_df = query_rds("""
        SELECT ethnicity_source_value AS ethnicity, COUNT(*) AS count
        FROM person GROUP BY ethnicity_source_value ORDER BY count DESC
    """)
    fig = px.bar(eth_df, x="ethnicity", y="count", title="Ethnicity Distribution")
    st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Visits & Encounters
# ════════════════════════════════════════════════════════

elif page == "Visits & Encounters":
    st.title("Visits & Encounters")

    # Visit type breakdown
    visit_df = query_rds("""
        SELECT
            COALESCE(c.concept_name, vo.visit_source_value, 'Unknown') AS visit_type,
            COUNT(*) AS count,
            COUNT(DISTINCT vo.person_id) AS unique_patients
        FROM visit_occurrence vo
        LEFT JOIN concept c ON vo.visit_concept_id = c.concept_id
        GROUP BY visit_type
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

    # Visit volume over time
    st.subheader("Visit Volume Over Time")
    trend_df = query_rds("""
        SELECT
            DATE_TRUNC('month', visit_start_date)::DATE AS month,
            COUNT(*) AS visits
        FROM visit_occurrence
        WHERE visit_start_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)
    if not trend_df.empty:
        fig = px.line(trend_df, x="month", y="visits", title="Monthly Visit Volume")
        st.plotly_chart(fig, use_container_width=True)

    # Average visit duration
    dur_df = query_rds("""
        SELECT
            COALESCE(c.concept_name, 'Unknown') AS visit_type,
            ROUND(AVG(visit_end_date - visit_start_date), 1) AS avg_days
        FROM visit_occurrence vo
        LEFT JOIN concept c ON vo.visit_concept_id = c.concept_id
        WHERE visit_end_date IS NOT NULL AND visit_start_date IS NOT NULL
        GROUP BY visit_type
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

    top_n = st.slider("Top N conditions", 10, 50, 20)

    cond_df = query_rds("""
        SELECT
            COALESCE(c.concept_name, co.condition_source_value, 'Unknown') AS condition,
            COUNT(*) AS occurrences,
            COUNT(DISTINCT co.person_id) AS unique_patients
        FROM condition_occurrence co
        LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
        GROUP BY condition
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

    # Conditions over time
    st.subheader("New Conditions Over Time")
    cond_trend = query_rds("""
        SELECT
            DATE_TRUNC('month', condition_start_date)::DATE AS month,
            COUNT(*) AS new_conditions
        FROM condition_occurrence
        WHERE condition_start_date IS NOT NULL
        GROUP BY month ORDER BY month
    """)
    if not cond_trend.empty:
        fig = px.area(cond_trend, x="month", y="new_conditions", title="Monthly New Condition Occurrences")
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Medications
# ════════════════════════════════════════════════════════

elif page == "Medications":
    st.title("Medication Analysis")

    top_n = st.slider("Top N medications", 10, 50, 20)

    drug_df = query_rds("""
        SELECT
            COALESCE(c.concept_name, de.drug_source_value, 'Unknown') AS medication,
            COUNT(*) AS prescriptions,
            COUNT(DISTINCT de.person_id) AS unique_patients
        FROM drug_exposure de
        LEFT JOIN concept c ON de.drug_concept_id = c.concept_id
        GROUP BY medication
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

    # Drug exposure duration
    st.subheader("Average Drug Exposure Duration")
    dur_df = query_rds("""
        SELECT
            COALESCE(c.concept_name, de.drug_source_value, 'Unknown') AS medication,
            ROUND(AVG(days_supply), 1) AS avg_days_supply,
            COUNT(*) AS total
        FROM drug_exposure de
        LEFT JOIN concept c ON de.drug_concept_id = c.concept_id
        WHERE days_supply IS NOT NULL AND days_supply > 0
        GROUP BY medication
        HAVING COUNT(*) >= 10
        ORDER BY avg_days_supply DESC
        LIMIT 20
    """)
    if not dur_df.empty:
        fig = px.bar(dur_df, x="medication", y="avg_days_supply", title="Avg Days Supply (Top 20)")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Patient Segments (Redshift)
# ════════════════════════════════════════════════════════

elif page == "Patient Segments":
    st.title("Patient Segments (ML Clustering)")

    if not has_redshift():
        st.warning("Redshift connection not available. Configure REDSHIFT_* environment variables.")
    else:
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
    else:
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

    try:
        dq_df = query_rds("SELECT * FROM data_quality_check")
        dq_df.columns = ["check_name", "issue_count"]

        all_pass = (dq_df["issue_count"] == 0).all()
        if all_pass:
            st.success("All data quality checks passed.")
        else:
            st.warning("Some data quality checks have issues.")

        fig = px.bar(
            dq_df, x="check_name", y="issue_count",
            title="Data Quality Issues by Check",
            color="issue_count", color_continuous_scale="RdYlGn_r",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(dq_df, use_container_width=True)
    except Exception as e:
        st.error(f"Could not run data quality checks: {e}")

    # Table completeness
    st.subheader("Table Completeness")
    completeness = []
    for table in ["person", "visit_occurrence", "condition_occurrence",
                   "drug_exposure", "procedure_occurrence", "measurement", "observation"]:
        try:
            df = query_rds(f"""
                SELECT
                    '{table}' AS table_name,
                    COUNT(*) AS total_rows,
                    COUNT(*) - COUNT(person_id) AS missing_person_id
                FROM {table}
            """)
            completeness.append(df.iloc[0].to_dict())
        except Exception:
            pass

    if completeness:
        comp_df = pd.DataFrame(completeness)
        st.dataframe(comp_df, use_container_width=True)


# ════════════════════════════════════════════════════════
#  PAGE: Streaming Monitor
# ════════════════════════════════════════════════════════

elif page == "Streaming Monitor":
    st.title("Real-Time Streaming Monitor")

    st.info(
        "This page monitors streaming ingestion from Kinesis. "
        "Data shown reflects post-cutoff events inserted by the stream consumer Lambda."
    )

    # Show simulation state
    st.subheader("Batch/Streaming Split")
    try:
        import boto3
        s3 = boto3.client("s3")
        bucket = os.environ.get("S3_BUCKET", "")
        prefix = os.environ.get("S3_PREFIX", "omop/")

        if bucket:
            try:
                state_obj = s3.get_object(Bucket=bucket, Key=f"{prefix}simulation_state.json")
                import json
                state = json.loads(state_obj["Body"].read().decode("utf-8"))
                col1, col2, col3 = st.columns(3)
                col1.metric("Batch Cutoff Date", state.get("batch_cutoff_date", "N/A"))
                col2.metric("Batch Records Loaded", f"{state.get('total_batch_records', 0):,}")
                col3.metric("Streaming Records Available", f"{state.get('total_streaming_records', 0):,}")
            except Exception:
                st.caption("No simulation state found. Run the batch ETL first.")

            try:
                prog_obj = s3.get_object(Bucket=bucket, Key=f"{prefix}streaming_progress.json")
                import json
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

    # Recent records by table
    st.subheader("Recent Records by Table")
    for table, date_col in [
        ("visit_occurrence", "visit_start_date"),
        ("condition_occurrence", "condition_start_date"),
        ("drug_exposure", "drug_exposure_start_date"),
        ("procedure_occurrence", "procedure_date"),
        ("measurement", "measurement_date"),
        ("observation", "observation_date"),
    ]:
        try:
            recent_df = query_rds(f"""
                SELECT {date_col}::DATE AS event_date, COUNT(*) AS count
                FROM {table}
                WHERE {date_col} IS NOT NULL
                GROUP BY event_date
                ORDER BY event_date DESC
                LIMIT 30
            """)
            if not recent_df.empty:
                recent_df = recent_df.sort_values("event_date")
                with st.expander(f"{table} — recent 30 days"):
                    fig = px.bar(recent_df, x="event_date", y="count", title=table)
                    st.plotly_chart(fig, use_container_width=True)
        except Exception:
            pass

    if st.button("Refresh"):
        st.cache_resource.clear()
        st.rerun()


# ── Footer ───────────────────────────────────────────────

st.sidebar.divider()
st.sidebar.caption("Healthcare Data Platform v2.0 — OMOP CDM")
if has_redshift():
    st.sidebar.success("Redshift: Connected")
else:
    st.sidebar.warning("Redshift: Not connected")
