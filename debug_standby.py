#!/usr/bin/env python3
"""Debug script to check standby segments for a specific person and month"""

from database import get_conn
from datetime import datetime
import psycopg2.extras

def check_person_standby():
    person_id = 78  # The person from the screenshot
    year = 2025
    month = 11  # November
    
    with get_conn() as conn:
        # Get the person's time reports for the month
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        start_date = datetime(year, month, 1).date()
        end_date = datetime(year, month + 1, 1).date() if month < 12 else datetime(year + 1, 1, 1).date()
        
        cursor.execute("""
            SELECT tr.*, st.name as shift_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON st.id = tr.shift_type_id
            WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
            ORDER BY tr.date
        """, (person_id, start_date, end_date))
        
        reports = cursor.fetchall()
        
        print(f"Reports for person {person_id} in {month}/{year}:")
        print("-" * 80)
        
        shift_ids = set()
        for r in reports:
            if r['shift_type_id']:
                shift_ids.add(r['shift_type_id'])
                print(f"Date: {r['date']}, Shift ID: {r['shift_type_id']}, Shift Name: {r['shift_name']}")
        
        if shift_ids:
            print("\n" + "=" * 80)
            print("Shift segments:")
            placeholders = ",".join(["%s"] * len(shift_ids))
            cursor.execute(f"""
                SELECT sts.*, st.name as shift_name
                FROM shift_time_segments sts
                LEFT JOIN shift_types st ON st.id = sts.shift_type_id
                WHERE sts.shift_type_id IN ({placeholders})
                ORDER BY sts.shift_type_id, sts.order_index
            """, tuple(shift_ids))
            
            segments = cursor.fetchall()
            for s in segments:
                print(f"Shift: {s['shift_name']} (ID: {s['shift_type_id']})")
                print(f"  Segment {s['order_index']}: {s['start_time']}-{s['end_time']}, Type: {s['segment_type']}, Wage: {s['wage_percent']}%")
            
            print("\n" + "=" * 80)
            print("Standby count by shift:")
            for shift_id in shift_ids:
                standby_count = sum(1 for s in segments if s['shift_type_id'] == shift_id and s['segment_type'] == 'standby')
                shift_name = next((s['shift_name'] for s in segments if s['shift_type_id'] == shift_id), 'Unknown')
                print(f"  {shift_name} (ID: {shift_id}): {standby_count} standby segments")
        
        cursor.close()

if __name__ == "__main__":
    check_person_standby()
