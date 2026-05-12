#!/usr/bin/env python3
"""
Cleanup script: delete the duplicate HubSpot deals identified by check_duplicate_deals.py.
Reads duplicate_deals_report.json and deletes only the newer duplicate(s) for each group,
keeping the oldest deal (the original, created 2026-04-30).

DRY RUN by default. Pass --live to actually delete.
"""

import os, json, sys, time
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("CustomCode")
if not TOKEN:
    sys.exit("CustomCode (HubSpot token) not found in .env")

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
DRY_RUN = "--live" not in sys.argv

REPORT_FILE = os.path.join(os.path.dirname(__file__), "duplicate_deals_report.json")

if not os.path.exists(REPORT_FILE):
    sys.exit(f"Report file not found: {REPORT_FILE}\nRun check_duplicate_deals.py first.")

with open(REPORT_FILE) as f:
    report = json.load(f)

print(f"\n{'='*60}")
print(f"DM Cvent Integration — Duplicate Deal Cleanup")
print(f"Mode: {'DRY RUN (pass --live to actually delete)' if DRY_RUN else '*** LIVE — will delete deals ***'}")
print(f"{'='*60}\n")

total_to_delete = sum(len(item["duplicate_deal_ids"]) for item in report)
print(f"Report: {len(report)} duplicate group(s), {total_to_delete} deal(s) to delete.\n")

deleted = 0
failed = 0

for item in report:
    contact_label = item.get("contact_label") or item.get("contact_id")
    event_key = item.get("event_key", "")
    keep_id = item["keep_deal_id"]
    keep_name = item.get("keep_dealname", keep_id)
    dup_ids = item["duplicate_deal_ids"]

    print(f"Contact: {contact_label}")
    print(f"  Deal:  {keep_name}")
    print(f"  Keep:  {keep_id}")
    for dup_id in dup_ids:
        print(f"  {'[DRY RUN] Would delete' if DRY_RUN else 'Deleting'}: {dup_id}")
        if not DRY_RUN:
            try:
                r = requests.delete(
                    f"https://api.hubapi.com/crm/v3/objects/deals/{dup_id}",
                    headers=HEADERS,
                    timeout=15,
                )
                if r.status_code in (200, 204):
                    print(f"    Deleted {dup_id}")
                    deleted += 1
                else:
                    print(f"    FAILED {dup_id}: HTTP {r.status_code} — {r.text[:200]}")
                    failed += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"    ERROR {dup_id}: {e}")
                failed += 1
        else:
            deleted += 1
    print()

print(f"{'='*60}")
if DRY_RUN:
    print(f"DRY RUN complete. {deleted} deal(s) would be deleted.")
    print("Run with --live to actually delete them.")
else:
    print(f"Done. Deleted: {deleted}  Failed: {failed}")
print(f"{'='*60}\n")
