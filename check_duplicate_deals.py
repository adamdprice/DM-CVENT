#!/usr/bin/env python3
"""
Diagnostic script: find duplicate HubSpot deals caused by the pagination fix re-run.
Scans all deals in the DM pipeline, groups them by (contact_id, event_name_key),
and reports any contact that has more than one deal for the same event.
"""

import os, json, sys, time
import requests
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("CustomCode")
if not TOKEN:
    sys.exit("CustomCode (HubSpot token) not found in .env")

PIPELINE = "726721932"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
ATTENDEE_OBJECT = "2-44005420"

def hs_get(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def hs_post(url, body):
    r = requests.post(url, headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    return r.json()

def search_deals_in_pipeline(pipeline_id: str) -> list:
    """Page through all deals in the given pipeline, returning list of deal dicts."""
    all_deals = []
    after = None
    page = 0
    while True:
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": pipeline_id,
                }]
            }],
            "properties": ["dealname", "amount", "pipeline", "dealstage", "createdate", "hs_lastmodifieddate"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        data = hs_post("https://api.hubapi.com/crm/v3/objects/deals/search", body)
        results = data.get("results", [])
        all_deals.extend(results)
        paging = data.get("paging", {})
        after = (paging.get("next") or {}).get("after")
        page += 1
        print(f"  Fetched page {page} — {len(results)} deals (total so far: {len(all_deals)})", flush=True)
        if not after:
            break
        time.sleep(0.2)  # be gentle with rate limits
    return all_deals

def get_contact_for_deal(deal_id: str):
    """Return the primary contact ID associated to a deal, or None."""
    try:
        data = hs_get(f"https://api.hubapi.com/crm/v4/objects/deals/{deal_id}/associations/contacts")
        results = data.get("results", [])
        if results:
            return str(results[0].get("toObjectId") or results[0].get("id") or "")
        return None
    except Exception:
        return None

def get_attendee_for_deal(deal_id: str):
    """Return the primary attendee (custom object) ID associated to a deal, or None."""
    try:
        data = hs_get(f"https://api.hubapi.com/crm/v4/objects/deals/{deal_id}/associations/{ATTENDEE_OBJECT}")
        results = data.get("results", [])
        if results:
            return str(results[0].get("toObjectId") or results[0].get("id") or "")
        return None
    except Exception:
        return None

def get_contact_email(contact_id: str) -> str:
    """Return email for a contact ID."""
    try:
        data = hs_get(
            f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}",
            params={"properties": "email,firstname,lastname"},
        )
        props = data.get("properties") or {}
        email = props.get("email", "")
        first = props.get("firstname", "")
        last = props.get("lastname", "")
        name = f"{first} {last}".strip()
        return f"{name} <{email}>" if name else email
    except Exception:
        return contact_id

def event_key_from_dealname(dealname: str) -> str:
    """
    Normalise deal name for deduplication.
    Deals have names like "LFEU26 - Sona Asset Management - Delegate".
    We use the FULL normalised name so that deals for genuinely different events
    (e.g. LFEU26 vs IPEU26) are NOT flagged as duplicates of each other.
    """
    return dealname.strip().lower()

def main():
    print(f"\n{'='*60}")
    print("DM Cvent Integration — Duplicate Deal Checker")
    print(f"{'='*60}\n")

    print(f"Fetching all deals in pipeline {PIPELINE}...")
    deals = search_deals_in_pipeline(PIPELINE)
    print(f"\nTotal deals found: {len(deals)}\n")

    if not deals:
        print("No deals found in this pipeline.")
        return

    # Group deals by contact (fetching contact associations in batches)
    print("Resolving contact associations for each deal...")
    deal_contact_map = {}  # deal_id -> contact_id
    deal_attendee_map = {}  # deal_id -> attendee_id

    for i, deal in enumerate(deals):
        deal_id = str(deal["id"])
        contact_id = get_contact_for_deal(deal_id)
        deal_contact_map[deal_id] = contact_id
        if (i + 1) % 20 == 0:
            print(f"  Resolved {i + 1}/{len(deals)} deals...", flush=True)
        time.sleep(0.05)

    print(f"  Done resolving {len(deals)} deals.\n")

    # Group: contact_id -> list of (deal_id, dealname, event_key, amount, createdate)
    contact_deals = defaultdict(list)
    no_contact = []

    for deal in deals:
        deal_id = str(deal["id"])
        props = deal.get("properties") or {}
        dealname = props.get("dealname", "") or ""
        amount = props.get("amount", "") or ""
        createdate = (props.get("createdate") or "")[:10]
        modified = (props.get("hs_lastmodifieddate") or "")[:10]
        contact_id = deal_contact_map.get(deal_id)
        ekey = event_key_from_dealname(dealname)

        entry = {
            "deal_id": deal_id,
            "dealname": dealname,
            "event_key": ekey,
            "amount": amount,
            "createdate": createdate,
            "modified": modified,
            "contact_id": contact_id,
        }

        if contact_id:
            contact_deals[contact_id].append(entry)
        else:
            no_contact.append(entry)

    # Find duplicates: contacts that have >1 deal with the same event_key
    duplicates = {}
    for contact_id, deal_list in contact_deals.items():
        event_groups = defaultdict(list)
        for d in deal_list:
            event_groups[d["event_key"]].append(d)
        for ekey, group in event_groups.items():
            if len(group) > 1:
                if contact_id not in duplicates:
                    duplicates[contact_id] = []
                duplicates[contact_id].append(group)

    # Report
    print(f"{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"Total deals in pipeline:       {len(deals)}")
    print(f"Deals with no contact linked:  {len(no_contact)}")
    print(f"Unique contacts with deals:    {len(contact_deals)}")
    print(f"Contacts with duplicate deals: {len(duplicates)}")
    print()

    if not duplicates:
        print("No duplicate deals detected.")
    else:
        print(f"DUPLICATE DEALS FOUND ({len(duplicates)} contacts affected):\n")
        print(f"{'─'*60}")
        total_extra = 0
        rows = []
        for contact_id, dup_groups in duplicates.items():
            contact_label = get_contact_email(contact_id)
            time.sleep(0.1)
            for group in dup_groups:
                event_name = group[0]["event_key"]
                extra = len(group) - 1
                total_extra += extra
                rows.append((contact_label, contact_id, event_name, group))

        rows.sort(key=lambda x: x[0])

        for contact_label, contact_id, event_name, group in rows:
            print(f"Contact:     {contact_label}")
            print(f"  Contact ID:  {contact_id}")
            print(f"  Event:       {event_name}")
            print(f"  Deals ({len(group)}):")
            for d in sorted(group, key=lambda x: x["createdate"]):
                print(f"    ID: {d['deal_id']}  name: {d['dealname']!r}  amount: {d['amount']}  created: {d['createdate']}  modified: {d['modified']}")
            print()

        print(f"{'─'*60}")
        print(f"Total extra (duplicate) deals to remove: {total_extra}")
        print()

        # Save results to JSON for follow-up cleanup
        out_file = os.path.join(os.path.dirname(__file__), "duplicate_deals_report.json")
        report = []
        for contact_label, contact_id, event_name, group in rows:
            # The oldest deal is the "original"; everything else is a duplicate
            sorted_group = sorted(group, key=lambda x: x["createdate"])
            report.append({
                "contact_id": contact_id,
                "contact_label": contact_label,
                "event_key": event_name,
                "keep_deal_id": sorted_group[0]["deal_id"],
                "keep_dealname": sorted_group[0]["dealname"],
                "duplicate_deal_ids": [d["deal_id"] for d in sorted_group[1:]],
                "all_deals": sorted_group,
            })
        with open(out_file, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Full report saved to: {out_file}")
        print("Review the report, then run delete_duplicate_deals.py to clean up.\n")

    if no_contact:
        print(f"\nDeals with no contact association ({len(no_contact)}):")
        for d in no_contact[:20]:
            print(f"  Deal {d['deal_id']}: {d['dealname']!r}  created: {d['createdate']}")
        if len(no_contact) > 20:
            print(f"  ... and {len(no_contact)-20} more")

if __name__ == "__main__":
    main()
