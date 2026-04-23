"""
Probe the date range of the public Synthea OMOP dataset.

For each clinical event table, streams the CSV from the public S3 bucket
and reports min / max date, per-year counts, and suggested cutoffs for
an ~80/20 batch/streaming split.

Usage:
    python3 etl/probe_data_range.py            # defaults to synthea1k
    python3 etl/probe_data_range.py synthea100k
"""

import sys
import csv
import io
from collections import Counter

import boto3

SOURCE_BUCKET = "synthea-omop"

EVENT_TABLES = {
    "visit_occurrence": "visit_start_date",
    "condition_occurrence": "condition_start_date",
    "drug_exposure": "drug_exposure_start_date",
    "procedure_occurrence": "procedure_date",
    "measurement": "measurement_date",
    "observation": "observation_date",
}


def stream_dates(s3, prefix, filename, date_col):
    """Yield YYYY-MM-DD strings from a CSV in S3."""
    key = f"{prefix}/{filename}"
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=key)
    # Stream line-by-line to avoid loading the full CSV into memory
    body = io.TextIOWrapper(obj["Body"], encoding="utf-8", newline="")
    reader = csv.DictReader(body)
    for row in reader:
        d = row.get(date_col, "")
        if len(d) >= 10:
            yield d[:10]


def percentile_cutoff(year_counts, fraction):
    """Return the year at which cumulative fraction of rows passes `fraction`."""
    total = sum(year_counts.values())
    if total == 0:
        return None
    running = 0
    for year in sorted(year_counts):
        running += year_counts[year]
        if running / total >= fraction:
            return year
    return max(year_counts)


def main():
    dataset = sys.argv[1] if len(sys.argv) > 1 else "synthea1k"
    s3 = boto3.client("s3")

    print(f"Dataset: s3://{SOURCE_BUCKET}/{dataset}/")
    print()

    combined_years = Counter()

    for table, date_col in EVENT_TABLES.items():
        print(f"── {table} ({date_col}) ──")
        min_date = None
        max_date = None
        year_counts = Counter()
        total = 0

        try:
            for d in stream_dates(s3, dataset, f"{table}.csv", date_col):
                total += 1
                year_counts[d[:4]] += 1
                if min_date is None or d < min_date:
                    min_date = d
                if max_date is None or d > max_date:
                    max_date = d
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        print(f"  rows:  {total:,}")
        print(f"  range: {min_date} → {max_date}")
        print(f"  by year (top 10 most recent):")
        for y in sorted(year_counts, reverse=True)[:10]:
            print(f"    {y}: {year_counts[y]:,}")
        print()

        combined_years.update(year_counts)

    print("══ Combined across tables ══")
    total = sum(combined_years.values())
    print(f"total events: {total:,}")
    if total:
        p50 = percentile_cutoff(combined_years, 0.5)
        p80 = percentile_cutoff(combined_years, 0.8)
        p90 = percentile_cutoff(combined_years, 0.9)
        print()
        print("Suggested cutoffs (year where cumulative % of events crosses threshold):")
        print(f"  50% batch / 50% stream  → cutoff at end of year {p50}  (CUTOFF_DATE={p50}-12-31)")
        print(f"  80% batch / 20% stream  → cutoff at end of year {p80}  (CUTOFF_DATE={p80}-12-31)")
        print(f"  90% batch / 10% stream  → cutoff at end of year {p90}  (CUTOFF_DATE={p90}-12-31)")


if __name__ == "__main__":
    main()
