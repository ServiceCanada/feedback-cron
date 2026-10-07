#!/usr/bin/env python3
"""
Delete MongoDB feedback entries for the archive quarter.

This script is intended to run AFTER export_archive_quarter.py has uploaded a
CSV backup to Azure Blob Storage. The archive-delete workflow looks up that
backup blob and passes the number of documents it contains in
BACKUP_DOC_COUNT. Deletion is refused if:
  - no backup exists for the quarter (BACKUP_DOC_COUNT empty), or
  - the database now holds MORE documents for the quarter than were backed up
    (those extra documents would be lost).
Fewer documents than backed up is allowed, so a partially completed run can
be safely re-run.

See archive_quarter.py for how the archive quarter is chosen.

Required environment variables:
  MONGO_DB_WRITE    — MongoDB connection string (stored in GitHub Secrets)
  BACKUP_DOC_COUNT  — doc_count metadata from the backup blob (set by workflow)
"""

import os
import sys
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from archive_quarter import get_archive_quarter_range

# CosmosDB returns error 16500 when a request exceeds the provisioned RU/s.
COSMOS_THROTTLED = 16500
MAX_ATTEMPTS     = 6


def with_retry(fn):
    """Call fn(), backing off exponentially while CosmosDB is throttling."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn()
        except OperationFailure as e:
            if e.code != COSMOS_THROTTLED or attempt == MAX_ATTEMPTS:
                raise
            delay = 2 ** attempt
            print(f"  Throttled by CosmosDB — retrying in {delay}s (attempt {attempt}/{MAX_ATTEMPTS})")
            time.sleep(delay)


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

    backup_count = os.environ.get("BACKUP_DOC_COUNT", "").strip()
    if not backup_count.isdigit():
        print(f"ERROR: No backup found in Azure Blob for {label} — refusing to delete.", file=sys.stderr)
        sys.exit(1)
    backup_count = int(backup_count)
    print(f"Backed up docs  : {backup_count}")

    if match_count > backup_count:
        print(
            f"ERROR: {match_count - backup_count} document(s) for {label} are not in the backup "
            f"— refusing to delete. Re-run the export or investigate.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Batch deletion — batch_size kept small (100) to avoid CosmosDB per-request
    # timeouts and RU limits that larger $in arrays can trigger.
    batch_size    = 100
    total_deleted = 0

    print(f"Starting deletion of {match_count} documents in batches of {batch_size}...")

    while True:
        batch = with_retry(lambda: list(problem.find(query, {"_id": 1}).limit(batch_size)))
        if not batch:
            break
        ids = [doc["_id"] for doc in batch]
        result = with_retry(lambda: problem.delete_many({"_id": {"$in": ids}}))
        if result.deleted_count == 0:
            print("ERROR: Batch deleted 0 documents — aborting to avoid an infinite loop.", file=sys.stderr)
            sys.exit(1)
        total_deleted += result.deleted_count
        pct = total_deleted / match_count * 100
        print(f"  Deleting... {total_deleted:,} / {match_count:,} ({pct:.1f}%)")

    print(f"Total deleted   : {total_deleted}")
    print(f"Remaining docs  : {problem.count_documents({})}")


if __name__ == "__main__":
    main()
