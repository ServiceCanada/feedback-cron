#!/usr/bin/env python3
"""
Export MongoDB feedback entries for the archive quarter to a CSV file,
then save it to the archives/ directory.

Archive quarter = two calendar quarters before the current one:
  - Trigger on Jan 1 (CQ1)  → archive CQ3 of previous year  (Jul 1 – Sep 30)
  - Trigger on Apr 1 (CQ2)  → archive CQ4 of previous year  (Oct 1 – Dec 31)
  - Trigger on Jul 1 (CQ3)  → archive CQ1 of current year   (Jan 1 – Mar 31)
  - Trigger on Oct 1 (CQ4)  → archive CQ2 of current year   (Apr 1 – Jun 30)

Required environment variable:
  MONGO_DB_WRITE  — MongoDB connection string (stored in GitHub Secrets)
"""

import csv
import os
import sys
from datetime import date

from pymongo import MongoClient


def get_archive_quarter_range():
    """Return (start_str, end_str, label) for the quarter to archive."""
    today = date.today()
    month = today.month
    year = today.year

    if month in (1, 2, 3):       # CQ1 running → archive CQ3 of prev year
        start = date(year - 1, 7, 1)
        end   = date(year - 1, 9, 30)
        label = f"{year - 1}-CQ3"
    elif month in (4, 5, 6):     # CQ2 running → archive CQ4 of prev year
        start = date(year - 1, 10, 1)
        end   = date(year - 1, 12, 31)
        label = f"{year - 1}-CQ4"
    elif month in (7, 8, 9):     # CQ3 running → archive CQ1 of current year
        start = date(year, 1, 1)
        end   = date(year, 3, 31)
        label = f"{year}-CQ1"
    else:                         # CQ4 running → archive CQ2 of current year
        start = date(year, 4, 1)
        end   = date(year, 6, 30)
        label = f"{year}-CQ2"

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), label


def main():
    mongo_uri = os.environ.get("MONGO_DB_WRITE")
    if not mongo_uri:
        print("ERROR: MONGO_DB_WRITE environment variable not set.", file=sys.stderr)
        sys.exit(1)

    start_str, end_str, label = get_archive_quarter_range()
    print(f"Archive quarter : {label}")
    print(f"Date range      : {start_str} → {end_str}")

    client = MongoClient(mongo_uri)
    problem = client.pagesuccess.problem

    query = {"problemDate": {"$gte": start_str, "$lte": end_str}}
    documents = list(problem.find(query))
    print(f"Documents found : {len(documents)}")

    if not documents:
        print("No documents found for this quarter — nothing to export.")
        # Write the output vars so downstream steps still have valid values
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            with open(github_output, "a") as f:
                f.write(f"archive_label={label}\n")
                f.write(f"archive_start={start_str}\n")
                f.write(f"archive_end={end_str}\n")
                f.write("doc_count=0\n")
        sys.exit(0)

    # Fixed field order matching the viewer CSV export format
    # Maps CSV header → MongoDB field name
    FIELD_MAP = [
        ("Problem Date",     "problemDate"),
        ("Time Stamp (UTC)", "timeStamp"),
        ("Problem Details",  "problemDetails"),
        ("Language",         "language"),
        ("Title",            "title"),
        ("URL",              "url"),
        ("Institution",      "institution"),
        ("Section",          "section"),
        ("Theme",            "theme"),
        ("Device Type",      "deviceType"),
        ("Browser",          "browser"),
    ]
    headers    = [h for h, _ in FIELD_MAP]
    mongo_keys = [k for _, k in FIELD_MAP]

    os.makedirs("/tmp/feedback-archives", exist_ok=True)
    filename = f"/tmp/feedback-archives/feedback_archive_{label}_{start_str}_{end_str}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for doc in documents:
            writer.writerow([doc.get(k, "") for k in mongo_keys])

    print(f"CSV saved to    : {filename}")

    # Expose values to subsequent workflow steps via GITHUB_OUTPUT
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"archive_label={label}\n")
            f.write(f"archive_start={start_str}\n")
            f.write(f"archive_end={end_str}\n")
            f.write(f"doc_count={len(documents)}\n")
            f.write(f"csv_file={filename}\n")


if __name__ == "__main__":
    main()
