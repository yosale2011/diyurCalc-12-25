"""
Payroll Snapshot Tool - יצירת תמונת מצב לבדיקת רגרסיה

שימוש:
1. לפני תיקון: python payroll_snapshot.py --create
2. אחרי תיקון: python payroll_snapshot.py --compare

יוצר קובץ JSON עם כל החישובים לכל העובדים בחודש הנבחר,
ואז משווה אחרי התיקון לזהות שינויים.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn, set_demo_mode
from core.logic import get_shabbat_times_cache

# Import the function we want to test
from app_utils import get_daily_segments_data


def get_all_active_workers(conn, year: int, month: int):
    """Get all workers with time reports in the given month."""
    cursor = conn.execute("""
        SELECT DISTINCT p.id, p.name
        FROM people p
        JOIN time_reports tr ON p.id = tr.person_id
        WHERE tr.date >= %s AND tr.date <= %s
        AND p.is_active = true
        ORDER BY p.name
    """, (f"{year}-{month:02d}-01", f"{year}-{month:02d}-31"))
    return cursor.fetchall()


def calculate_worker_month(conn, person_id: int, year: int, month: int, shabbat_cache) -> dict:
    """Calculate all payroll data for a worker in a given month."""
    try:
        segments, chain_list = get_daily_segments_data(
            conn, person_id, year, month, shabbat_cache, 34.40
        )

        # Extract key metrics
        result = {
            "person_id": person_id,
            "year": year,
            "month": month,
            "total_days": len(segments),
            "total_payment": 0,
            "total_standby": 0,
            "total_work_minutes": 0,
            "total_calc100": 0,
            "total_calc125": 0,
            "total_calc150": 0,
            "days": []
        }

        for day in segments:
            day_data = {
                "date": str(day.get("date", "")),
                "payment": round(day.get("payment", 0), 2),
                "standby_payment": round(day.get("standby_payment", 0), 2),
                "work_minutes": day.get("work_minutes", 0),
                "calc100": day.get("calc100", 0),
                "calc125": day.get("calc125", 0),
                "calc150": day.get("calc150", 0),
            }
            result["days"].append(day_data)
            result["total_payment"] += day_data["payment"]
            result["total_standby"] += day_data["standby_payment"]
            result["total_work_minutes"] += day_data["work_minutes"]
            result["total_calc100"] += day_data["calc100"]
            result["total_calc125"] += day_data["calc125"]
            result["total_calc150"] += day_data["calc150"]

        # Round totals
        result["total_payment"] = round(result["total_payment"], 2)
        result["total_standby"] = round(result["total_standby"], 2)

        return result

    except Exception as e:
        return {
            "person_id": person_id,
            "error": str(e)
        }


def create_snapshot(year: int, month: int, output_file: str):
    """Create a snapshot of all payroll calculations."""
    print(f"Creating snapshot for {month}/{year}...")

    conn = get_conn()
    shabbat_cache = get_shabbat_times_cache(conn.conn)

    workers = get_all_active_workers(conn, year, month)
    print(f"Found {len(workers)} active workers")

    snapshot = {
        "created_at": datetime.now().isoformat(),
        "year": year,
        "month": month,
        "workers": []
    }

    for i, worker in enumerate(workers):
        person_id = worker["id"]
        name = worker['name']
        print(f"  [{i+1}/{len(workers)}] Processing {name} (id={person_id})...")

        result = calculate_worker_month(conn, person_id, year, month, shabbat_cache)
        result["name"] = name
        snapshot["workers"].append(result)

    conn.close()

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    print(f"\nSnapshot saved to: {output_file}")
    print(f"Total workers: {len(snapshot['workers'])}")

    # Summary
    total_payment = sum(w.get("total_payment", 0) for w in snapshot["workers"])
    total_standby = sum(w.get("total_standby", 0) for w in snapshot["workers"])
    print(f"Total payments: {total_payment:,.2f} NIS")
    print(f"Total standby: {total_standby:,.2f} NIS")


def compare_snapshots(before_file: str, after_file: str):
    """Compare two snapshots and report differences."""
    print(f"Comparing snapshots...")
    print(f"  Before: {before_file}")
    print(f"  After: {after_file}")

    with open(before_file, 'r', encoding='utf-8') as f:
        before = json.load(f)

    with open(after_file, 'r', encoding='utf-8') as f:
        after = json.load(f)

    # Build lookup by person_id
    before_workers = {w["person_id"]: w for w in before["workers"]}
    after_workers = {w["person_id"]: w for w in after["workers"]}

    differences = []

    for person_id, before_data in before_workers.items():
        after_data = after_workers.get(person_id)

        if not after_data:
            differences.append({
                "person_id": person_id,
                "name": before_data.get("name", "Unknown"),
                "type": "MISSING_IN_AFTER"
            })
            continue

        # Compare totals
        fields_to_compare = [
            "total_payment", "total_standby", "total_work_minutes",
            "total_calc100", "total_calc125", "total_calc150"
        ]

        worker_diffs = []
        for field in fields_to_compare:
            before_val = before_data.get(field, 0)
            after_val = after_data.get(field, 0)

            if abs(before_val - after_val) > 0.01:  # Allow small rounding differences
                worker_diffs.append({
                    "field": field,
                    "before": before_val,
                    "after": after_val,
                    "diff": round(after_val - before_val, 2)
                })

        # Compare daily data
        before_days = {d["date"]: d for d in before_data.get("days", [])}
        after_days = {d["date"]: d for d in after_data.get("days", [])}

        day_diffs = []
        all_dates = set(before_days.keys()) | set(after_days.keys())

        for date in sorted(all_dates):
            b_day = before_days.get(date, {})
            a_day = after_days.get(date, {})

            for field in ["payment", "standby_payment", "work_minutes"]:
                b_val = b_day.get(field, 0)
                a_val = a_day.get(field, 0)

                if abs(b_val - a_val) > 0.01:
                    day_diffs.append({
                        "date": date,
                        "field": field,
                        "before": b_val,
                        "after": a_val,
                        "diff": round(a_val - b_val, 2)
                    })

        if worker_diffs or day_diffs:
            differences.append({
                "person_id": person_id,
                "name": before_data.get("name", "Unknown"),
                "type": "CHANGED",
                "total_changes": worker_diffs,
                "daily_changes": day_diffs[:10]  # Limit to first 10 days
            })

    # Report
    print(f"\n{'='*60}")
    print(f"COMPARISON RESULTS")
    print(f"{'='*60}")

    if not differences:
        print("\n[OK] No differences found! All calculations match.")
    else:
        print(f"\n[!!] Found {len(differences)} workers with differences:\n")

        for diff in differences:
            print(f"\n--- {diff['name']} (id={diff['person_id']}) ---")

            if diff["type"] == "MISSING_IN_AFTER":
                print("  Worker missing in after snapshot!")
                continue

            if diff.get("total_changes"):
                print("  Total changes:")
                for tc in diff["total_changes"]:
                    sign = "+" if tc["diff"] > 0 else ""
                    print(f"    {tc['field']}: {tc['before']} -> {tc['after']} ({sign}{tc['diff']})")

            if diff.get("daily_changes"):
                print("  Daily changes (first 10):")
                for dc in diff["daily_changes"]:
                    sign = "+" if dc["diff"] > 0 else ""
                    print(f"    {dc['date']} {dc['field']}: {dc['before']} -> {dc['after']} ({sign}{dc['diff']})")

    # Save comparison report
    report_file = before_file.replace("_before.json", "_comparison.json")
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "compared_at": datetime.now().isoformat(),
            "before_file": before_file,
            "after_file": after_file,
            "total_differences": len(differences),
            "differences": differences
        }, f, ensure_ascii=False, indent=2)

    print(f"\nComparison saved to: {report_file}")

    return len(differences) == 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Payroll Snapshot Tool")
    parser.add_argument("--create", action="store_true", help="Create a new snapshot")
    parser.add_argument("--compare", action="store_true", help="Compare before/after snapshots")
    parser.add_argument("--year", type=int, default=2025, help="Year (default: 2025)")
    parser.add_argument("--month", type=int, default=12, help="Month (default: 12)")
    parser.add_argument("--before", action="store_true", help="Create 'before' snapshot")
    parser.add_argument("--after", action="store_true", help="Create 'after' snapshot")

    args = parser.parse_args()

    base_dir = Path(__file__).parent
    base_name = f"snapshot_{args.year}_{args.month:02d}"

    if args.create or args.before:
        output_file = base_dir / f"{base_name}_before.json"
        create_snapshot(args.year, args.month, str(output_file))

    elif args.after:
        output_file = base_dir / f"{base_name}_after.json"
        create_snapshot(args.year, args.month, str(output_file))

    elif args.compare:
        before_file = base_dir / f"{base_name}_before.json"
        after_file = base_dir / f"{base_name}_after.json"

        if not before_file.exists():
            print(f"Error: Before snapshot not found: {before_file}")
            print("Run with --before first")
            sys.exit(1)

        if not after_file.exists():
            print(f"Error: After snapshot not found: {after_file}")
            print("Run with --after first")
            sys.exit(1)

        success = compare_snapshots(str(before_file), str(after_file))
        sys.exit(0 if success else 1)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
