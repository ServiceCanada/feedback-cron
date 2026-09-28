#!/usr/bin/env python3
"""
Shared archive-quarter logic for the export and delete scripts.

Archive quarter = two calendar quarters before the current one:
  - Trigger on Jan 1 (CQ1)  → archive CQ3 of previous year  (Jul 1 – Sep 30)
  - Trigger on Apr 1 (CQ2)  → archive CQ4 of previous year  (Oct 1 – Dec 31)
  - Trigger on Jul 1 (CQ3)  → archive CQ1 of current year   (Jan 1 – Mar 31)
  - Trigger on Oct 1 (CQ4)  → archive CQ2 of current year   (Apr 1 – Jun 30)

Run directly, it writes archive_label / archive_start / archive_end / blob_name
to $GITHUB_OUTPUT so workflow steps can locate the backup blob.
"""

import os
from datetime import date


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


def archive_blob_name(start_str, end_str, label):
    """Name of the CSV backup, both on disk and in Azure Blob Storage."""
    return f"feedback_archive_{label}_{start_str}_{end_str}.csv"


def main():
    start_str, end_str, label = get_archive_quarter_range()
    blob_name = archive_blob_name(start_str, end_str, label)
    print(f"Archive quarter : {label} ({start_str} → {end_str})")
    print(f"Backup blob     : {blob_name}")

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"archive_label={label}\n")
            f.write(f"archive_start={start_str}\n")
            f.write(f"archive_end={end_str}\n")
            f.write(f"blob_name={blob_name}\n")


if __name__ == "__main__":
    main()
