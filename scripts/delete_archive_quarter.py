#!/usr/bin/env python3
"""
Delete MongoDB feedback entries for the archive quarter.

This script is intended to run AFTER export_archive_quarter.py has already
uploaded a CSV backup to Azure Blob Storage. It is called by the
archive-delete workflow which only runs when the export workflow succeeds,
providing the safety guarantee that a backup exists before deletion.

Archive quarter = two calendar quarters before the current one:
  - Trigger on Jan 1 (CQ1)  → delete CQ3 of previous year  (Jul 1 – Sep 30)
  - Trigger on Apr 1 (CQ2)  → delete CQ4 of previous year  (Oct 1 – Dec 31)
  - Trigger on Jul 1 (CQ3)  → delete CQ1 of current year   (Jan 1 – Mar 31)
  - Trigger on Oct 1 (CQ4)  → delete CQ2 of current year   (Apr 1 – Jun 30)

Required environment variable:
  MONGO_DB_WRITE  — MongoDB connection string (stored in GitHub Secrets)
"""

import os
import sys
from datetime import date

from pymongo import MongoClient


def get_archive_quarter_range():
    """Return (start_str, end_str, label) for the quarter to archive."""
    today = date.today()
    month = today.month
    year = today.year

    if month in (1, 2, 3):
        start = date(year - 1, 7, 1)
        end   = date(year - 1, 9, 30)
        label = f"{year - 1}-CQ3"
    elif month in (4, 5, 6):
        start = date(year - 1, 10, 1)
        end   = date(year - 1, 12, 31)
        label = f"{year - 1}-CQ4"
    elif month in (7, 8, 9):
        start = date(year, 1, 1)
        end   = date(year, 3, 31)
        label = f"{year}-CQ1"
    else:
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

    count_before = problem.count_documents({})
    match_count  = problem.count_documents(query)
    print(f"Total documents : {count_before}")
    print(f"Matching quarter: {match_count}")

    if match_count == 0:
        print("No documents found for this quarter — nothing to delete.")
        sys.exit(0)

    # Batch deletion (mirrors the original manual script)
    batch_size    = 1000
    total_deleted = 0

    while True:
        batch = list(problem.find(query, {"_id": 1}).limit(batch_size))
        if not batch:
            break
        for doc in batch:
            problem.delete_one({"_id": doc["_id"]})
            total_deleted += 1
            if total_deleted % 100 == 0:
                print(f"  Deleted so far: {total_deleted}")

    print(f"Total deleted   : {total_deleted}")
    print(f"Remaining docs  : {problem.count_documents({})}")


if __name__ == "__main__":
    main()
