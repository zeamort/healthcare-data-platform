"""
Streaming Simulator: Push post-cutoff OMOP events to Kinesis

Reads the simulation_state.json from S3 to determine the batch cutoff date,
then reads OMOP CSV files from S3 and pushes records dated after the cutoff
to a Kinesis stream in chronological order, simulating real-time data arrival.

Supports resumable streaming — tracks progress so you can pause and resume
across demo sessions.

Configuration via environment variables:
    S3_BUCKET         — S3 bucket containing OMOP CSVs
    S3_PREFIX         — key prefix (default: omop/)
    KINESIS_STREAM    — Kinesis stream name
    AWS_REGION        — AWS region (default: us-east-1)
    EVENTS_PER_SECOND — throttle rate (default: 10)
    MAX_EVENTS        — stop after N events (default: 0 = unlimited)
"""

import os
import sys
import csv
import json
import time
import logging
from io import StringIO
from datetime import datetime

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "omop/")
KINESIS_STREAM = os.environ["KINESIS_STREAM"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "10"))
MAX_EVENTS = int(os.environ.get("MAX_EVENTS", "0"))

s3 = boto3.client("s3")
kinesis = boto3.client("kinesis", region_name=AWS_REGION)

# Clinical event tables and their date columns
EVENT_TABLES = {
    # visit_occurrence is loaded in full during batch so FKs from child events
    # always resolve. Streaming only carries the child clinical events.
    "condition_occurrence": "condition_start_date",
    "drug_exposure": "drug_exposure_start_date",
    "procedure_occurrence": "procedure_date",
    "measurement": "measurement_date",
    "observation": "observation_date",
}


# ── Helpers ──────────────────────────────────────────────

def load_simulation_state():
    """Load the batch cutoff date from S3."""
    state_key = f"{S3_PREFIX}simulation_state.json"
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=state_key)
        state = json.loads(response["Body"].read().decode("utf-8"))
        log.info("Loaded simulation state: cutoff=%s", state["batch_cutoff_date"])
        return state
    except s3.exceptions.NoSuchKey:
        log.error("No simulation_state.json found. Run batch ETL with CUTOFF_DATE first.")
        sys.exit(1)


def save_streaming_progress(last_date, total_sent):
    """Save streaming progress to S3 for resume capability."""
    progress_key = f"{S3_PREFIX}streaming_progress.json"
    progress = {
        "last_streamed_date": last_date,
        "total_events_sent": total_sent,
        "updated_at": datetime.utcnow().isoformat(),
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=progress_key,
        Body=json.dumps(progress, indent=2),
        ContentType="application/json",
    )


def load_streaming_progress():
    """Load previous streaming progress if it exists."""
    progress_key = f"{S3_PREFIX}streaming_progress.json"
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=progress_key)
        progress = json.loads(response["Body"].read().decode("utf-8"))
        log.info("Resuming from: last_date=%s, total_sent=%d",
                 progress["last_streamed_date"], progress["total_events_sent"])
        return progress
    except Exception:
        return None


def read_csv_from_s3(filename):
    """Download a CSV from S3 and return a DictReader."""
    s3_key = f"{S3_PREFIX}{filename}"
    log.info("Reading s3://%s/%s", S3_BUCKET, s3_key)
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    body = response["Body"].read().decode("utf-8")
    return csv.DictReader(StringIO(body))


def collect_post_cutoff_events(cutoff_date, resume_date=None):
    """Collect all clinical events after the cutoff date, sorted chronologically."""
    events = []
    effective_cutoff = resume_date if resume_date else cutoff_date

    for table, date_col in EVENT_TABLES.items():
        try:
            reader = read_csv_from_s3(f"{table}.csv")
        except Exception as e:
            log.warning("Could not read %s.csv: %s", table, e)
            continue

        for row in reader:
            date_str = row.get(date_col, "")
            if not date_str or len(date_str) < 10:
                continue
            event_date = date_str[:10]
            if event_date > effective_cutoff:
                events.append({
                    "event_type": table,
                    "event_date": event_date,
                    "data": dict(row),
                })

    events.sort(key=lambda e: e["event_date"])
    log.info("Collected %d post-cutoff events across %d tables",
             len(events), len(EVENT_TABLES))
    return events


def send_to_kinesis(events, total_previously_sent=0):
    """Send events to Kinesis with throttling."""
    total_sent = total_previously_sent
    batch = []
    last_date = None

    for event in events:
        record = {
            "Data": json.dumps(event, default=str).encode("utf-8"),
            "PartitionKey": str(event["data"].get("person_id", "unknown")),
        }
        batch.append(record)

        # Send in batches of up to 500 (Kinesis limit)
        if len(batch) >= 500:
            kinesis.put_records(StreamName=KINESIS_STREAM, Records=batch)
            total_sent += len(batch)
            last_date = event["event_date"]
            batch = []

            # Progress update every 1000 events
            if total_sent % 1000 == 0:
                log.info("  Sent %d events (current date: %s)", total_sent, last_date)
                save_streaming_progress(last_date, total_sent)

            # Throttle
            if EVENTS_PER_SECOND > 0:
                time.sleep(len(batch) / EVENTS_PER_SECOND)

        # Check max events limit
        if MAX_EVENTS > 0 and total_sent >= MAX_EVENTS:
            log.info("Reached MAX_EVENTS limit (%d)", MAX_EVENTS)
            break

    # Send remaining batch
    if batch:
        kinesis.put_records(StreamName=KINESIS_STREAM, Records=batch)
        total_sent += len(batch)
        last_date = event["event_date"]

    return total_sent, last_date


# ── Main ─────────────────────────────────────────────────

def main():
    log.info("Streaming Simulator: OMOP Events → Kinesis")
    log.info("Stream: %s", KINESIS_STREAM)
    log.info("Source: s3://%s/%s", S3_BUCKET, S3_PREFIX)

    # Load cutoff from batch ETL
    state = load_simulation_state()
    cutoff_date = state["batch_cutoff_date"]

    # Check for resume
    progress = load_streaming_progress()
    resume_date = None
    total_previously_sent = 0
    if progress:
        resume_date = progress["last_streamed_date"]
        total_previously_sent = progress["total_events_sent"]

    # Collect events
    events = collect_post_cutoff_events(cutoff_date, resume_date)
    if not events:
        log.info("No events to stream. All data is before cutoff (%s).", cutoff_date)
        return

    log.info("Streaming %d events at %d events/sec", len(events), EVENTS_PER_SECOND)

    # Stream to Kinesis
    total_sent, last_date = send_to_kinesis(events, total_previously_sent)

    # Save final progress
    if last_date:
        save_streaming_progress(last_date, total_sent)

    log.info("── Streaming Summary ──")
    log.info("Total events sent: %d", total_sent)
    log.info("Last event date: %s", last_date)
    log.info("Streaming complete.")


if __name__ == "__main__":
    main()
